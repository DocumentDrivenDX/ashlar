# SPDX-License-Identifier: MIT OR Apache-2.0
"""Closed detached evidence codec for a pre-publication Truss A/B page.

Decoded values are plain evidence. They do not issue a Truss source selection,
hold, ACK capability, or publisher admission.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import json
from types import MappingProxyType

from .r04_schedule import canonical
from .truss_source_custody_selection import (
    TrussSourceCustodyAnchor, _ANCHORS, _issued,
)


PROFILE = "ashlar-truss-detached-a1-source-page/0.1"
MAX_PAGE_BYTES = 8 * 1024 * 1024
MAX_OPAQUE_BYTES = 4 * 1024 * 1024
MAX_BINDING_BYTES = 1024 * 1024
MAX_CODEC_BYTES = 20 * 1024 * 1024
_TOP_KEYS = {"profile", "source", "schedule_sha256", "publication_id",
    "destination_id", "request_bytes_base64", "request_sha256",
    "destination_registration_bytes_base64", "ack_scope_bytes_base64",
    "identity", "maximum_facts", "maximum_bytes", "page", "binding_sha256"}
_PAGE_KEYS = {"consumer_id", "registration_id", "prior_bytes_base64",
    "next_bytes_base64", "content_base64", "sha256", "writer_xid",
    "safe_watermark_xid", "outcome"}


class DetachedSourceCodecError(ValueError):
    """Detached source evidence is malformed, changed, or outside its profile."""


def _b64(raw):
    return base64.b64encode(raw).decode("ascii")


def _decode64(value, label, maximum):
    if type(value) is not str or len(value) > 4 * ((maximum + 2) // 3):
        raise DetachedSourceCodecError(f"Bounded {label} bytes required")
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, TypeError) as error:
        raise DetachedSourceCodecError(f"Exact {label} base64 required") from error
    if len(raw) > maximum or _b64(raw) != value:
        raise DetachedSourceCodecError(f"Canonical bounded {label} bytes required")
    return raw


def _decode_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate member")
            result[key] = value
        return result
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (UnicodeError, ValueError, json.JSONDecodeError) as error:
        raise DetachedSourceCodecError("Closed detached source JSON required") from error
    if type(value) is not dict:
        raise DetachedSourceCodecError("Detached source object required")
    return value


@dataclass(frozen=True)
class DetachedTrussSourcePage:
    """Detached exact inputs for parent-side revalidation; carries no issuer."""

    source: str
    schedule_sha256: str
    publication_id: str
    destination_id: str
    request_bytes: bytes
    request_sha256: str
    destination_registration_bytes: bytes
    ack_scope_bytes: bytes
    identity: dict
    maximum_facts: int
    maximum_bytes: int
    page_fields: dict
    page_content: bytes
    prior_bytes: bytes
    next_bytes: bytes
    binding_sha256: str

    def materialize_page(self):
        """Recreate only Truss's public plain FeedPage result for semantic checks."""
        try:
            from truss.feed import FeedCursor, FeedPage
        except ImportError as error:
            raise DetachedSourceCodecError("Public Truss FeedPage types required") from error
        fields = self.page_fields
        return FeedPage(
            consumer_id=fields["consumer_id"],
            registration_id=fields["registration_id"],
            prior=FeedCursor(content=self.prior_bytes),
            next=FeedCursor(content=self.next_bytes),
            content=self.page_content, sha256=fields["sha256"],
            writer_xid=fields["writer_xid"],
            safe_watermark_xid=fields["safe_watermark_xid"],
            outcome=fields["outcome"])


def encode_detached_source_page(anchor: TrussSourceCustodyAnchor) -> bytes:
    """Encode only an issued, pre-publication A/B anchor as closed IPC evidence."""
    if (type(anchor) is not TrussSourceCustodyAnchor or not _issued(_ANCHORS, anchor)
            or anchor.authority_profile != "r04-scheduled"
            or anchor.source not in ("A", "B")
            or type(anchor.preselected_publication_id) is not str
            or not anchor.preselected_publication_id):
        raise DetachedSourceCodecError("Issued pre-publication A/B source anchor required")
    page = anchor.page
    try:
        content, prior, following = page.content, page.prior.content, page.next.content
        page_fields = {
            "consumer_id": page.consumer_id,
            "registration_id": page.registration_id,
            "prior_bytes_base64": _b64(prior),
            "next_bytes_base64": _b64(following),
            "content_base64": _b64(content),
            "sha256": page.sha256,
            "writer_xid": page.writer_xid,
            "safe_watermark_xid": page.safe_watermark_xid,
            "outcome": page.outcome,
        }
    except (AttributeError, TypeError) as error:
        raise DetachedSourceCodecError("Complete Truss page fields required") from error
    identity = dict(anchor.identity)
    if (type(identity) is not dict or not identity
            or any(type(key) is not str or not key
                   or type(value) is not str or not value
                   for key, value in identity.items())):
        raise DetachedSourceCodecError("Closed source identity strings required")
    if (type(content) is not bytes or not 0 < len(content) <= MAX_PAGE_BYTES
            or type(prior) is not bytes or not prior
            or type(following) is not bytes or not following
            or type(page.sha256) is not str
            or hashlib.sha256(content).hexdigest() != page.sha256
            or type(page_fields["consumer_id"]) is not str
            or type(page_fields["registration_id"]) is not str
            or type(page_fields["writer_xid"]) not in (str, type(None))
            or (page_fields["writer_xid"] is not None and not page_fields["writer_xid"])
            or type(page_fields["safe_watermark_xid"]) is not str
            or not page_fields["safe_watermark_xid"]
            or page_fields["outcome"] not in ("transaction", "empty_interval")):
        raise DetachedSourceCodecError("Bounded exact Truss page required")
    if (type(anchor.request_bytes) is not bytes
            or not 0 < len(anchor.request_bytes) <= MAX_OPAQUE_BYTES
            or type(anchor.destination_registration_bytes) is not bytes
            or not 0 < len(anchor.destination_registration_bytes) <= MAX_BINDING_BYTES
            or type(anchor.ack_scope_bytes) is not bytes
            or not 0 < len(anchor.ack_scope_bytes) <= MAX_BINDING_BYTES
            or type(anchor.maximum_facts) is not int or anchor.maximum_facts < 1
            or type(anchor.maximum_bytes) is not int or anchor.maximum_bytes < 1
            or hashlib.sha256(anchor.request_bytes).hexdigest() != anchor.request_sha256):
        raise DetachedSourceCodecError("Bounded exact source anchor inputs required")
    body = {
        "profile": PROFILE, "source": anchor.source,
        "schedule_sha256": anchor.schedule_sha256,
        "publication_id": anchor.preselected_publication_id,
        "destination_id": anchor.destination_id,
        "request_bytes_base64": _b64(anchor.request_bytes),
        "request_sha256": anchor.request_sha256,
        "destination_registration_bytes_base64": _b64(
            anchor.destination_registration_bytes),
        "ack_scope_bytes_base64": _b64(anchor.ack_scope_bytes),
        "identity": identity, "maximum_facts": anchor.maximum_facts,
        "maximum_bytes": anchor.maximum_bytes, "page": page_fields,
    }
    raw_body = canonical(body)
    envelope = {**body, "binding_sha256": hashlib.sha256(raw_body).hexdigest()}
    encoded = canonical(envelope)
    if len(encoded) > MAX_CODEC_BYTES:
        raise DetachedSourceCodecError("Detached source page exceeds codec bound")
    return encoded


def decode_detached_source_page(raw: bytes) -> DetachedTrussSourcePage:
    """Decode exact IPC bytes to plain evidence without issuing source authority."""
    if type(raw) is not bytes or not 0 < len(raw) <= MAX_CODEC_BYTES:
        raise DetachedSourceCodecError("Bounded exact detached source bytes required")
    value = _decode_json(raw)
    if set(value) != _TOP_KEYS or canonical(value) != raw:
        raise DetachedSourceCodecError("Canonical closed detached source profile required")
    binding = value.pop("binding_sha256")
    if (type(binding) is not str or len(binding) != 64
            or any(c not in "0123456789abcdef" for c in binding)
            or hashlib.sha256(canonical(value)).hexdigest() != binding):
        raise DetachedSourceCodecError("Detached source binding digest differs")
    if (value["profile"] != PROFILE or value["source"] not in ("A", "B")
            or type(value["schedule_sha256"]) is not str
            or len(value["schedule_sha256"]) != 64
            or type(value["publication_id"]) is not str or not value["publication_id"]
            or type(value["destination_id"]) is not str or not value["destination_id"]
            or type(value["request_sha256"]) is not str
            or type(value["maximum_facts"]) is not int or value["maximum_facts"] < 1
            or type(value["maximum_bytes"]) is not int or value["maximum_bytes"] < 1
            or type(value["identity"]) is not dict
            or any(type(k) is not str or not k or type(v) is not str or not v
                   for k, v in value["identity"].items())):
        raise DetachedSourceCodecError("Exact detached source identity required")
    request = _decode64(value["request_bytes_base64"], "request", MAX_OPAQUE_BYTES)
    registration = _decode64(value["destination_registration_bytes_base64"],
                              "destination registration", MAX_BINDING_BYTES)
    scope = _decode64(value["ack_scope_bytes_base64"], "ACK scope", MAX_BINDING_BYTES)
    if (not request or not registration or not scope
            or hashlib.sha256(request).hexdigest() != value["request_sha256"]):
        raise DetachedSourceCodecError("Detached request digest differs")
    page = value["page"]
    if type(page) is not dict or set(page) != _PAGE_KEYS:
        raise DetachedSourceCodecError("Closed detached Truss page fields required")
    content = _decode64(page["content_base64"], "page content", MAX_PAGE_BYTES)
    prior = _decode64(page["prior_bytes_base64"], "prior cursor", MAX_OPAQUE_BYTES)
    following = _decode64(page["next_bytes_base64"], "next cursor", MAX_OPAQUE_BYTES)
    if (not content or not prior or not following or type(page["sha256"]) is not str
            or hashlib.sha256(content).hexdigest() != page["sha256"]
            or type(page["consumer_id"]) is not str or not page["consumer_id"]
            or type(page["registration_id"]) is not str or not page["registration_id"]
            or type(page["writer_xid"]) not in (str, type(None))
            or (page["writer_xid"] is not None and not page["writer_xid"])
            or type(page["safe_watermark_xid"]) is not str
            or not page["safe_watermark_xid"]
            or page["outcome"] not in ("transaction", "empty_interval")):
        raise DetachedSourceCodecError("Detached Truss page integrity differs")
    return DetachedTrussSourcePage(
        source=value["source"], schedule_sha256=value["schedule_sha256"],
        publication_id=value["publication_id"], destination_id=value["destination_id"],
        request_bytes=request, request_sha256=value["request_sha256"],
        destination_registration_bytes=registration, ack_scope_bytes=scope,
        identity=MappingProxyType(dict(value["identity"])),
        maximum_facts=value["maximum_facts"],
        maximum_bytes=value["maximum_bytes"], page_fields=MappingProxyType(dict(page)),
        page_content=content, prior_bytes=prior, next_bytes=following,
        binding_sha256=binding)
