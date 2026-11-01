# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure composition of the selected .52 A1 inputs for one managed trial.

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

    def verify_delivery(self, prefix_bytes, *, prefix_sha256, selected,
                        schedule_bytes, pages):
        """Verify a later retained /0.2 package against this exact A page.

        The package and independent ACK/registration anchors must be supplied
        by their owners after publication. This method confers no authority.
        """
        from .managed_current_prefix_delivery import (
            validate_managed_current_prefix_delivery,
        )
        if schedule_bytes != self.selection.schedule_bytes:
            raise ValueError("Original selected R04 schedule bytes must be retained")
        if type(pages) is not tuple or not pages or pages[0] != self.delivery_page:
            raise ValueError("First retained /0.2 page must be the selected original A page")
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
    from .managed_current_prefix_delivery import ManagedCurrentPrefixPage
    from .publication_planning import truss_request_for

    if type(selection) is not ManagedCurrentTruss052GenesisSelection:
        raise ValueError("Independently selected Truss .52 A/B source-owner output required")
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
        raise ValueError("Original public Truss .52 A page and cursors required") from error
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
