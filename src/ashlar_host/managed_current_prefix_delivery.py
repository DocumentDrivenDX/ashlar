# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure delivery boundary for exact pages in a managed-current R04 prefix.

The selected anchors and page records are caller-provided evidence. This module
checks byte correspondence and ordering; it does not authenticate Truss, select
source or publication authority, perform native effects, or acknowledge pages.
"""
from __future__ import annotations

from dataclasses import dataclass
import base64
import hashlib
import json

from ashlar.truss_native_feed import native_feed_batch
from ashlar.staging import batch_row
from ashlar.source_checkpoint import truss_checkpoint
from .managed_current_prefix_custody_v2 import (
    ManagedCurrentPrefixCustodyCorrespondence,
    validate_selected_managed_current_prefix_v2,
)
from .managed_current_truss_cursor import validate_selected_truss_cursor
from .r04_schedule import R04Schedule, R04CustodyError


@dataclass(frozen=True)
class ManagedCurrentPrefixPage:
    """Exact public Truss page and the original cursors returned with it."""
    ordinal: int
    source: str
    page_bytes: bytes
    prior_cursor_bytes: bytes
    next_cursor_bytes: bytes


@dataclass(frozen=True)
class ManagedCurrentPrefixDelivery:
    """Verified original prefix bytes, summary, and ordered exact page digests."""
    prefix_bytes: bytes
    prefix_sha256: str
    correspondence: ManagedCurrentPrefixCustodyCorrespondence
    page_sha256: tuple[str, ...]


def _decode(raw: bytes):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate prefix delivery JSON member")
            result[key] = value
        return result

    return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(
                          ValueError("Non-finite prefix delivery JSON value")))


def validate_managed_current_prefix_delivery(prefix_bytes, *, prefix_sha256,
        selected, schedule_bytes, pages):
    """Bind exact ordered Truss pages to the retained `/0.2` publication prefix.

    ``selected`` remains independently sourced custody, as required by the
    `/0.2` validator. ``pages`` must be the exact public page/cursor bytes from
    the selected Truss observations, in global R04 order. The result grants no
    source, publication, native-runtime, or ACK authority.
    """
    try:
        schedule = R04Schedule(schedule_bytes)
    except (R04CustodyError, TypeError) as error:
        raise ValueError("Exact canonical original R04 schedule required") from error

    correspondence = validate_selected_managed_current_prefix_v2(
        prefix_bytes, prefix_sha256=prefix_sha256, selected=selected)
    package = _decode(prefix_bytes)
    schedule_text = schedule.raw.decode("utf-8")
    if package["scheduleJson"] != schedule_text:
        raise ValueError("Delivery schedule must equal the separately supplied original R04 bytes")
    if type(pages) is not tuple or len(pages) != package["scheduleStep"]:
        raise ValueError("Exact ordered Truss page for every global prefix slot required")

    schedule_doc = schedule.document()
    digests = []
    for index, (page, entry, slot) in enumerate(
            zip(pages, package["entries"], schedule_doc["slots"]), start=1):
        if (type(page) is not ManagedCurrentPrefixPage
                or type(page.ordinal) is not int or page.ordinal != index
                or page.source != slot["source"]
                or type(page.page_bytes) is not bytes
                or type(page.prior_cursor_bytes) is not bytes
                or type(page.next_cursor_bytes) is not bytes):
            raise ValueError("Exact Truss page and cursor must match the original global schedule slot")
        identity = selected["sources"][page.source]["identity"]
        feed = identity["feed"]
        try:
            batch = native_feed_batch(page.page_bytes,
                sha256=hashlib.sha256(page.page_bytes).hexdigest(),
                prior=page.prior_cursor_bytes, next=page.next_cursor_bytes,
                feed=feed)
            row = batch_row(batch)
            request = _decode(entry["requestJson"].encode("utf-8"))
        except (KeyError, TypeError, UnicodeError, ValueError) as error:
            raise ValueError("Exact public Truss page, cursors, and retained request required") from error
        if (request.get("source_batch_json") != row["batch_json"]
                or request.get("source_batch_digest") != row["batch_digest"]
                or request.get("source_checkpoint_json") != truss_checkpoint(batch)
                or request.get("batch_id") != batch.batch_id
                or batch.feed != feed or batch.epoch != identity["epoch"]):
            raise ValueError("Exact Truss page/cursor bytes differ from the retained publication request")

        cursor_identity = {
            "installation_id": identity["installation_id"],
            "epoch": identity["epoch"],
            "consumer_id": identity["consumer_id"],
            "registration_id": identity["registration_id"],
        }
        try:
            before = validate_selected_truss_cursor(page.prior_cursor_bytes,
                cursor_sha256=hashlib.sha256(page.prior_cursor_bytes).hexdigest(),
                selected_identity=cursor_identity)
            after = validate_selected_truss_cursor(page.next_cursor_bytes,
                cursor_sha256=hashlib.sha256(page.next_cursor_bytes).hexdigest(),
                selected_identity=cursor_identity)
        except ValueError as error:
            raise ValueError("Exact page cursors differ from separately selected source identity") from error
        if (base64.b64encode(page.prior_cursor_bytes).decode("ascii") != batch.cursor_before
                or base64.b64encode(page.next_cursor_bytes).decode("ascii") != batch.cursor_after
                or before.next_xid != _decode(page.prior_cursor_bytes).get("nextXid")
                or after.next_xid != _decode(page.next_cursor_bytes).get("nextXid")):
            raise ValueError("Exact Truss page cursor transition differs from retained request")
        digests.append(hashlib.sha256(page.page_bytes).hexdigest())

    return ManagedCurrentPrefixDelivery(prefix_bytes, prefix_sha256,
                                        correspondence, tuple(digests))
