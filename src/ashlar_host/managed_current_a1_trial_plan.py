# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure composition of selected .52/.53 A1 inputs for one managed trial.

This is a preparation record, not source, publication, native-runtime, or ACK
authority. It only composes outputs from the Truss source owner and the
create-once genesis manifest owner; callers still own each effectful boundary.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import base64
import hashlib


@dataclass(frozen=True)
class ManagedCurrentA1TrialPlan:
    """Exact source/genesis/request inputs for one first A-page trial."""
    selection: object = field(repr=False, compare=False)
    predecessor: object = field(repr=False, compare=False)
    request: dict
    driver_inputs: dict = field(repr=False, compare=False)
    delivery_page: object = field(repr=False, compare=False)
    phase: str = "a1_genesis"

    def verify_delivery(self, prefix_bytes, *, prefix_sha256, selected,
                        schedule_bytes, pages):
        """Verify a later retained /0.2 package against this exact A page.

        The package and independent ACK/registration anchors must be supplied
        by their owners after publication. This method confers no authority.
        """
        from .managed_current_prefix_delivery import (
            validate_managed_current_prefix_delivery, ManagedCurrentPrefixPage,
            decode_prefix_payload,
        )
        if schedule_bytes != self.selection.schedule_bytes:
            raise ValueError("Original selected R04 schedule bytes must be retained")
        if type(pages) is not tuple or not pages or pages[0] != self.delivery_page:
            raise ValueError("First retained /0.2 page must be the selected original A page")
        if self.phase == "a0_metadata":
            from .managed_current_prefix_custody_v2 import (
                validate_selected_managed_current_prefix_v2,
            )
            from ashlar.truss_native_feed import native_feed_batch
            from ashlar.staging import batch_row
            from ashlar.source_checkpoint import truss_checkpoint
            if schedule_bytes != self.selection.schedule_bytes:
                raise ValueError("A0 verifier must retain the exact original R04 schedule")
            if type(pages) is not tuple or len(pages) != 1:
                raise ValueError("A0 metadata verifier requires the exact held A0 source page")
            page = pages[0]
            if (type(page) is not ManagedCurrentPrefixPage or page.source != "A"
                    or page.ordinal != 0):
                raise ValueError("A0 metadata page is outside the semantic R04 slot sequence")
            correspondence = validate_selected_managed_current_prefix_v2(
                prefix_bytes, prefix_sha256=prefix_sha256, selected=selected)
            package = decode_prefix_payload(prefix_bytes)
            if (package.get("phase") != "a0_metadata" or package.get("scheduleStep") != 0
                    or package.get("entries") != []
                    or package.get("scheduleJson", "").encode("utf-8") != schedule_bytes):
                raise ValueError("A0 zero-step prefix must not consume an R04 semantic slot")
            batch = native_feed_batch(page.page_bytes,
                sha256=hashlib.sha256(page.page_bytes).hexdigest(),
                prior=page.prior_cursor_bytes, next=page.next_cursor_bytes,
                feed=self.selection.source_progress["A"].feed)
            selected_batch = self.selection.source_owner_admitted_batch
            if (batch.batch_id != selected_batch.batch_id
                    or batch.begin != selected_batch.begin
                    or batch.cursor_before != selected_batch.cursor_before
                    or batch.cursor_after != selected_batch.cursor_after
                    or batch.cursor_before != self.selection.source_progress["A"].position
                    or self.request.get("source_batch_json") != batch_row(batch)["batch_json"]
                    or self.request.get("source_checkpoint_json") != truss_checkpoint(batch)
                    or self.selection.admission.changes):
                raise ValueError("A0 metadata-only page and empty graph admission must match exactly")
            return correspondence
        if self.phase == "a1_continuation":
            from .managed_current_prefix_custody_v2 import (
                validate_selected_managed_current_prefix_a0_continuation,
            )
            from ashlar.truss_native_feed import native_feed_batch
            correspondence = validate_selected_managed_current_prefix_a0_continuation(
                prefix_bytes, prefix_sha256=prefix_sha256, selected=selected)
            package = decode_prefix_payload(prefix_bytes)
            if package["scheduleJson"].encode("utf-8") != schedule_bytes:
                raise ValueError("A1 continuation must retain the original R04 schedule bytes")
            if type(pages) is not tuple or len(pages) != 1:
                raise ValueError("A1 continuation requires exactly its original source page")
            page = pages[0]
            if (type(page) is not ManagedCurrentPrefixPage or page.ordinal != 1
                    or page.source != "A"):
                raise ValueError("A1 continuation page must occupy semantic slot one for A")
            identity = selected["sources"]["A"]["identity"]
            batch = native_feed_batch(page.page_bytes,
                sha256=hashlib.sha256(page.page_bytes).hexdigest(),
                prior=page.prior_cursor_bytes, next=page.next_cursor_bytes,
                feed=identity["feed"])
            request = decode_prefix_payload(package["entries"][0]["requestJson"].encode("utf-8"))
            if (request["source_batch_json"] != __import__("ashlar.staging",
                    fromlist=["batch_row"]).batch_row(batch)["batch_json"]
                    or request["source_checkpoint_json"] != __import__(
                        "ashlar.source_checkpoint", fromlist=["truss_checkpoint"]
                        ).truss_checkpoint(batch)):
                raise ValueError("A1 continuation source page bytes differ from its exact request")
            return correspondence
        return validate_managed_current_prefix_delivery(
            prefix_bytes, prefix_sha256=prefix_sha256, selected=selected,
            schedule_bytes=schedule_bytes, pages=pages)


def prepare_managed_current_a1_trial(selection, genesis_manifest, *,
        stream, materialized_at, context):
    """Compose exact source-owner and durable genesis-owner outputs.

    ``genesis_manifest`` must be the row returned by the six-role manifest
    owner's create-once/readback path. The function verifies correspondence,
    prepares the existing driver's exact request, and retains the public A
    page bytes. It performs no native, source, cloud, or ACK action.
    """
    from .managed_current_genesis import (
        ManagedCurrentGenesisPredecessor,
        verify_managed_current_genesis_predecessor,
    )
    from .managed_current_truss052_genesis_owner import (
        ManagedCurrentTruss052GenesisSelection,
    )
    from .managed_current_truss053_genesis_owner import (
        ManagedCurrentTruss053GenesisSelection,
    )
    from .managed_current_prefix_delivery import ManagedCurrentPrefixPage
    from .publication_planning import truss_request_for

    if type(selection) not in (ManagedCurrentTruss052GenesisSelection,
                               ManagedCurrentTruss053GenesisSelection):
        raise ValueError("Independently selected Truss .52, .53 or .54 A/B source-owner output required")
    if type(stream) is not str or not stream or "\0" in stream:
        raise ValueError("Exact managed source stream required")
    if (type(materialized_at) is not str or not materialized_at.isascii()
            or not materialized_at.isdecimal()
            or str(int(materialized_at)) != materialized_at
            or int(materialized_at) >= 2**63):
        raise ValueError("Canonical bounded materialization timestamp required")
    if selection.admission.batch is not selection.source_owner_admitted_batch:
        raise ValueError("A batch must be the exact source-owner admitted graph batch")
    observations = selection.source_observations
    if (type(observations) is not dict or set(observations) != {"A", "B"}
            or observations["A"].batch is not selection.source_owner_admitted_batch
            or observations["B"].batch is not None):
        raise ValueError("Complete original A/B observations and exact A admitted batch required")
    if (type(selection.schedule_bytes) is not bytes
            or selection.schedule_sha256 != hashlib.sha256(
                selection.schedule_bytes).hexdigest()):
        raise ValueError("Selected original schedule digest differs from retained bytes")
    from .r04_schedule import R04Schedule
    schedule = R04Schedule(selection.schedule_bytes)
    if schedule.document()["slots"][0]["source"] != "A":
        raise ValueError("A1 first page must occupy the original first A schedule slot")

    selected_sources = selection.source_progress
    predecessor = verify_managed_current_genesis_predecessor(
        genesis_manifest, selected_sources=selected_sources)
    if type(predecessor) is not ManagedCurrentGenesisPredecessor:
        raise ValueError("Exact verified six-role genesis manifest predecessor required")
    batch = selection.source_owner_admitted_batch
    request = truss_request_for(stream, batch, genesis_manifest["publication_id"],
                                selection.schema_revisions)
    page = observations["A"].page
    try:
        delivery_page = ManagedCurrentPrefixPage(
            ordinal=1, source="A", page_bytes=page.content,
            prior_cursor_bytes=page.prior.content,
            next_cursor_bytes=page.next.content)
    except AttributeError as error:
        raise ValueError("Original public Truss .52 or .53 A page and cursors required") from error
    try:
        original_prior = base64.b64decode(batch.cursor_before, validate=True)
        original_next = base64.b64decode(batch.cursor_after, validate=True)
    except (ValueError, TypeError) as error:
        raise ValueError("Admitted source batch must retain canonical base64 cursors") from error
    if (base64.b64encode(original_prior).decode("ascii") != batch.cursor_before
            or base64.b64encode(original_next).decode("ascii") != batch.cursor_after
            or delivery_page.page_bytes != batch.begin
            or delivery_page.prior_cursor_bytes != original_prior
            or delivery_page.next_cursor_bytes != original_next):
        raise ValueError("Retained A page bytes differ from admitted native-feed batch")
    driver_inputs = {
        "admission": selection.admission,
        "stream": stream,
        "materialized_at": materialized_at,
        "context": context,
        "managed_genesis_predecessor": predecessor,
        "source_owner_admitted_batch": batch,
    }
    return ManagedCurrentA1TrialPlan(selection, predecessor, request,
                                    driver_inputs, delivery_page)


@dataclass(frozen=True)
class ManagedCurrentA1ContinuationPredecessor:
    """Exact previously published A0 manifest for the A1 continuation."""
    manifest: dict


def prepare_managed_current_a1_continuation(selection, predecessor_manifest, *,
        stream, materialized_at, context):
    """Prepare A1 against the exact committed A0 publication.

    Unlike the initial A0 publication, this path deliberately supplies no
    genesis predecessor pair to the publisher driver. The driver must reopen
    and validate A0's committed publication attempt and its current six-role
    table vector before applying this request.
    """
    from .managed_current_truss052_genesis_owner import ManagedCurrentTruss052GenesisSelection
    from .managed_current_truss053_genesis_owner import ManagedCurrentTruss053GenesisSelection
    from .managed_current_prefix_delivery import ManagedCurrentPrefixPage
    from .publication_planning import truss_request_for
    from .managed_current_core import CARRIER_PROFILE_XXHASH64

    if type(selection) not in (ManagedCurrentTruss052GenesisSelection,
                               ManagedCurrentTruss053GenesisSelection):
        raise ValueError("Exact detached A/B source selection required")
    if type(predecessor_manifest) is not dict:
        raise ValueError("Exact read-back A0 manifest required")
    if (predecessor_manifest.get("profile_version") != CARRIER_PROFILE_XXHASH64
            or type(predecessor_manifest.get("publication_id")) is not str
            or not predecessor_manifest["publication_id"]
            or type(predecessor_manifest.get("source_progress_json")) is not str):
        raise ValueError("A0 publication identity and physical profile are required")
    try:
        previous = __import__("json").loads(predecessor_manifest["source_progress_json"])
        request_predecessor = predecessor_manifest["publication_id"]
        batch = selection.source_owner_admitted_batch
        progress_a = selection.source_progress
        feed_a = progress_a["A"].feed
        feed_b = progress_a["B"].feed
        a0 = previous[feed_a]
        b0 = previous[feed_b]
        cursor_before = __import__("base64").b64decode(batch.cursor_before, validate=True)
        cursor_after = __import__("base64").b64decode(batch.cursor_after, validate=True)
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        raise ValueError("A0 full source progress and exact A1 page cursors required") from error
    if (type(request_predecessor) is not str or not request_predecessor
            or type(previous) is not dict or set(previous) != {feed_a, feed_b}
            or type(a0) is not dict or a0.get("epoch") != progress_a["A"].epoch
            or a0.get("position") != __import__("base64").b64encode(cursor_before).decode("ascii")
            or a0.get("profile") != "truss-native-feed/0.1"
            or a0.get("previous") is None
            or type(b0) is not dict or b0 != progress_a["B"].document()
            or cursor_before != progress_a["A"].cursor_bytes
            or selection.admission.batch is not batch):
        raise ValueError("A1 prior cursor must equal A0 committed A progress while B remains genesis")
    request = truss_request_for(stream, batch, request_predecessor,
                                selection.schema_revisions)
    page = selection.source_observations["A"].page
    delivery_page = ManagedCurrentPrefixPage(ordinal=1, source="A",
        page_bytes=page.content, prior_cursor_bytes=page.prior.content,
        next_cursor_bytes=page.next.content)
    if delivery_page.prior_cursor_bytes != cursor_before or delivery_page.next_cursor_bytes != cursor_after:
        raise ValueError("A1 page differs from the exact A0 predecessor cursor")
    predecessor = ManagedCurrentA1ContinuationPredecessor(dict(predecessor_manifest))
    driver_inputs = {"admission": selection.admission, "stream": stream,
        "materialized_at": materialized_at, "context": context,
        "managed_genesis_predecessor": None, "source_owner_admitted_batch": None}
    return ManagedCurrentA1TrialPlan(selection, predecessor, request,
        driver_inputs, delivery_page, phase="a1_continuation")
