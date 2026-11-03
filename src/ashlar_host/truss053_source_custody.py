# SPDX-License-Identifier: MIT OR Apache-2.0
"""Current-fresh Truss A1 custody and read-only ACK reconciliation adapters."""
from __future__ import annotations

from dataclasses import dataclass
import base64
import hashlib
import importlib
import json
import os
from pathlib import Path
import socket
import struct

from .truss052_source_custody import (
    Truss052AckUncertain, Truss052CustodyError, Truss052SourceCustodySession,
)


class Truss053CustodyError(Truss052CustodyError):
    """The current-fresh source-custody interval refused or became uncertain."""


def detached_source_custody_digests(snapshot) -> dict:
    """Digest frozen A1 source/page/ACK bindings for managed observers."""
    from .truss_detached_source_snapshot_codec import DetachedTrussSourceSnapshot
    from .r04_schedule import canonical
    if type(snapshot) is not DetachedTrussSourceSnapshot:
        raise Truss053CustodyError("Exact detached A1 source snapshot required")
    page = snapshot.source_page
    identity = dict(page.identity)
    fields = dict(page.page_fields)
    values = {
        "source_registration": canonical({key: identity[key] for key in (
            "feed", "epoch", "installation_id")}),
        "consumer_registration": canonical({key: identity[key] for key in (
            "consumer_id", "registration_id")}),
        "cursor": page.prior_bytes,
        "destination_registration": page.destination_registration_bytes,
        "ack_scope": page.ack_scope_bytes,
        "checkpoint": page.request_bytes,
        "current_page": page.page_content,
        "retained_page": canonical({
            "source": page.source, "schedule_sha256": page.schedule_sha256,
            "publication_id": page.publication_id, "destination_id": page.destination_id,
            "request_sha256": page.request_sha256, "identity": identity,
            "page": fields, "prior_bytes_base64": base64.b64encode(page.prior_bytes).decode("ascii"),
            "next_bytes_base64": base64.b64encode(page.next_bytes).decode("ascii"),
        }),
    }
    return {name: hashlib.sha256(raw if type(raw) is bytes else raw).hexdigest()
            for name, raw in values.items()}


def _genesis_matches_with_monotonic_watermarks(original: bytes, refreshed: bytes, *,
                                                expected_a_prior_cursor="0") -> bool:
    """Keep frozen source bytes exact while admitting only monotonic safe-frontier drift."""
    from .truss_detached_a1_genesis_codec import decode_detached_a1_genesis_snapshot

    # Both decoders enforce the complete closed schema, binding digest, and
    # source/page/cursor correspondence before this narrowly scoped comparison.
    decode_detached_a1_genesis_snapshot(
        original, expected_a_prior_cursor=expected_a_prior_cursor)
    decode_detached_a1_genesis_snapshot(
        refreshed, expected_a_prior_cursor=expected_a_prior_cursor)
    before, after = json.loads(original), json.loads(refreshed)
    prior_sources = {item["source"]: item for item in before["sources"]}
    current_sources = {item["source"]: item for item in after["sources"]}
    if set(prior_sources) != {"A", "B"} or set(current_sources) != {"A", "B"}:
        return False
    for source in ("A", "B"):
        old = prior_sources[source]["page"]["safe_watermark_xid"]
        new = current_sources[source]["page"]["safe_watermark_xid"]
        if (type(old) is not str or not old.isascii() or not old.isdecimal()
                or len(old) > 20 or str(int(old)) != old
                or int(old) > 18446744073709551615
                or type(new) is not str or not new.isascii() or not new.isdecimal()
                or len(new) > 20 or str(int(new)) != new
                or int(new) > 18446744073709551615 or int(new) < int(old)):
            return False
        prior_sources[source]["page"]["safe_watermark_xid"] = "<frontier>"
        current_sources[source]["page"]["safe_watermark_xid"] = "<frontier>"
    # Each envelope digest necessarily changes when the admitted monotonic
    # frontier changes; both original digests were independently verified by
    # the closed decoder above.
    before["binding_sha256"] = "<verified-binding>"
    after["binding_sha256"] = "<verified-binding>"
    return before == after


@dataclass
class Truss053SourceCustodySession(Truss052SourceCustodySession):
    """Same-session feed custody gated to exact installed fresh-runtime pins."""

    def __post_init__(self):
        from .truss_current_catalog import installed_fresh_truss_runtime_pins
        try:
            installed_fresh_truss_runtime_pins()
            super().__post_init__()
        except (ImportError, ValueError, TypeError) as error:
            raise Truss053CustodyError(
                "Exact installed current-fresh Truss source-custody runtime required") from error


class Truss053A1SourceHolder:
    """Open a fresh A1 page hold before publication and retain its issuer in-process.

    The supplied A/B selections are connection-owner inputs, not Truss authority.
    The holder independently reads them with the public fresh-runtime owner and issues the
    prepublication anchor/open selection in this process. Only the detached
    snapshot crosses the IPC boundary.
    """

    def __init__(self, *, schedule, source_selections, context):
        from .managed_current_truss053_genesis_owner import (
            ManagedCurrentTruss053SourceSelection,
        )
        if (type(source_selections) is not dict or set(source_selections) != {"A", "B"}
                or any(type(item) is not ManagedCurrentTruss053SourceSelection
                       for item in source_selections.values())
                or context is None):
            raise Truss053CustodyError(
                "Fresh public A/B source owners and holder context required")
        from .r04_schedule import R04Schedule
        if type(schedule) is not R04Schedule:
            raise Truss053CustodyError("Exact frozen original R04 schedule required")
        self.schedule = schedule
        self.source_selections = dict(source_selections)
        self.request_bytes = None
        self.destination_id = None
        self.publication_id = None
        self.context = context
        self.genesis = self.anchor = self.session = None
        self.genesis_bytes = None
        self.snapshot_bytes = None
        self.bound_selection = None
        self.custody_digests = None
        self.phase = "a0"
        self._a0_settlement = None
        self._a0_page_next = None
        self._matched_ack = None
        self._advance_a1_callback = None
        self._resumed_a1_genesis_event_pending = False
        self._closed = False

    def select_genesis(self) -> bytes:
        from .managed_current_truss053_genesis_owner import (
            ManagedCurrentTruss053GenesisSourceOwner,
        )
        from .truss_detached_a1_genesis_codec import encode_detached_a1_genesis_snapshot
        from .truss_current_catalog import installed_fresh_truss_runtime_pins
        if self.genesis is not None:
            if (self.phase == "a1" and self._resumed_a1_genesis_event_pending
                    and type(self.genesis_bytes) is bytes):
                self._resumed_a1_genesis_event_pending = False
                return self.genesis_bytes
            raise Truss053CustodyError("A/B genesis selection is one-shot")
        installed_fresh_truss_runtime_pins()
        expected = (None if self.phase == "a0" else
                    json.loads(self._a0_page_next.decode("utf-8"))["nextXid"])
        self.genesis = ManagedCurrentTruss053GenesisSourceOwner(
            self.schedule, self.source_selections).admit_a1(
                expected_a_prior_cursor=expected)
        self.genesis_bytes = encode_detached_a1_genesis_snapshot(self.genesis)
        return self.genesis_bytes

    def resume_a1_after_matched_a0(self, attempt, settlement):
        """Resume an already-committed A1 page without replaying its seed group.

        The durable A0 attempt and settlement are revalidated against a fresh
        read-only Truss reconciliation. The exact A0 next cursor then gates a
        new public A/B selection for the still-pending A1 page.
        """
        from .a1_ack_attempt import (
            A1AckAttempt, A1AckSettlement, DurableA1AckAttemptStore,
            MAX_SETTLEMENT_RECORD_BYTES,
        )
        from .managed_current_truss053_genesis_owner import (
            ManagedCurrentTruss053GenesisSourceOwner,
        )
        from .truss_detached_a1_genesis_codec import encode_detached_a1_genesis_snapshot
        from .truss_detached_source_snapshot_codec import decode_detached_source_snapshot
        from .managed_current_truss_cursor import validate_selected_truss_cursor

        if (self._closed or self.phase != "a0" or self.genesis is not None
                or self.genesis_bytes is not None or self.session is not None
                or self.snapshot_bytes is not None or self.anchor is not None
                or self.bound_selection is not None or self._a0_settlement is not None
                or type(attempt) is not A1AckAttempt
                or type(settlement) is not A1AckSettlement):
            raise Truss053CustodyError(
                "Fresh unopened source holder and exact A0 attempt/settlement required")
        if set(self.source_selections) != {"A", "B"}:
            raise Truss053CustodyError("Exact selected A/B source owners required")
        try:
            store = DurableA1AckAttemptStore(attempt.path.parent)
            persisted = store.load()
            dispatch = store._read_record("ack-command-dispatched.json")
            worker = store._read_record("worker-settlement.json",
                                        MAX_SETTLEMENT_RECORD_BYTES)
            snapshot = decode_detached_source_snapshot(attempt.selection_bytes)
            page = snapshot.source_page
            selected_a = self.source_selections["A"]
            expected_identity = self.schedule.document()["source_bindings"]["A"]
            if (persisted != attempt or page.source != "A"
                    or page.schedule_sha256 != self.schedule.sha256
                    or page.identity != expected_identity
                    or (selected_a.feed, selected_a.epoch,
                        selected_a.installation_id, selected_a.consumer_id,
                        selected_a.consumer_registration_id)
                       != (expected_identity["feed"], expected_identity["epoch"],
                           expected_identity["installation_id"],
                           expected_identity["consumer_id"], expected_identity["registration_id"])
                    or snapshot.progress.feed != selected_a.feed
                    or snapshot.progress.epoch != selected_a.epoch
                    or snapshot.progress.installation_id != selected_a.installation_id
                    or snapshot.progress.consumer_id != selected_a.consumer_id
                    or snapshot.progress.consumer_registration_id
                       != selected_a.consumer_registration_id
                    or dispatch.get("attempt_id") != attempt.attempt_id
                    or dispatch.get("status") != "ack_command_may_have_been_sent"
                    or dispatch.get("ack_retry_permitted") is not False
                    or worker.get("attempt_id") != attempt.attempt_id
                    or worker.get("status") != "matched"
                    or worker.get("outcome") != "matched"
                    or worker.get("ack_retry_permitted") is not False):
                raise Truss053CustodyError(
                    "Persisted matched A0 attempt and selected source identity required")
            prior_cursor = validate_selected_truss_cursor(
                attempt.prior_checkpoint_bytes,
                cursor_sha256=hashlib.sha256(attempt.prior_checkpoint_bytes).hexdigest(),
                selected_identity=expected_identity)
            if (prior_cursor.next_xid != "0"
                    or page.prior_bytes != attempt.prior_checkpoint_bytes
                    or page.next_bytes != attempt.next_checkpoint_bytes
                    or page.request_bytes != attempt.request_bytes
                    or page.publication_id != attempt.publication_id
                    or page.destination_id != attempt.destination_id
                    or page.page_fields.get("sha256") != attempt.page_sha256):
                raise Truss053CustodyError(
                    "Original zero-cursor A0 attempt bindings required for A1 resume")
            cursor = validate_selected_truss_cursor(
                attempt.next_checkpoint_bytes,
                cursor_sha256=hashlib.sha256(attempt.next_checkpoint_bytes).hexdigest(),
                selected_identity=expected_identity)
            expected_settlement = A1AckSettlement(
                "matched", attempt.next_checkpoint_bytes, attempt.receipt_bytes,
                attempt.request_sha256, attempt.manifest_sha256,
                attempt.selection_sha256)
            if settlement != expected_settlement:
                raise Truss053CustodyError(
                    "Matched A0 settlement differs from exact durable attempt")
            encoded_settlement = {
                "outcome": settlement.outcome,
                "checkpoint_bytes_base64": base64.b64encode(
                    settlement.checkpoint_bytes).decode("ascii"),
                "receipt_bytes_base64": base64.b64encode(
                    settlement.receipt_bytes).decode("ascii"),
                "request_sha256": settlement.request_sha256,
                "manifest_sha256": settlement.manifest_sha256,
                "selection_sha256": settlement.selection_sha256,
            }
            if any(worker.get(key) != value for key, value in encoded_settlement.items()):
                raise Truss053CustodyError(
                    "Worker's durable A0 settlement differs from supplied bytes")
            reconciled = Truss053ReadOnlyAckReconciler(
                lambda _context: selected_a.connection_factory(), context=self.context
            ).reconcile(attempt)
            if reconciled != settlement:
                raise Truss053CustodyError(
                    "Fresh public A0 ACK reconciliation differs from retained settlement")

            # No callback is retained or invoked: the public A1 transaction is
            # already committed. Re-admit that pending page at the exact A0
            # checkpoint and let the ordinary open path establish its hold.
            genesis = ManagedCurrentTruss053GenesisSourceOwner(
                self.schedule, self.source_selections).admit_a1(
                    expected_a_prior_cursor=cursor.next_xid)
            page_a = genesis.source_observations["A"].page
            if (page_a.outcome != "transaction"
                    or page_a.prior.content != settlement.checkpoint_bytes):
                raise Truss053CustodyError(
                    "Exact pending A1 transaction after the reconciled A0 cursor required")
            genesis_bytes = encode_detached_a1_genesis_snapshot(genesis)
        except Truss053CustodyError:
            raise
        except Exception as error:
            raise Truss053CustodyError(
                "Could not verify the matched A0 attempt and pending A1 source") from error

        self._a0_settlement = settlement
        self._a0_page_next = settlement.checkpoint_bytes
        self._advance_a1_callback = None
        self.genesis = genesis
        self.genesis_bytes = genesis_bytes
        self.phase = "a1"
        self._resumed_a1_genesis_event_pending = True
        return genesis_bytes

    def open_prepublication(self, *, request_bytes: bytes, destination_id: str,
                             publication_id: str) -> bytes:
        from .managed_current_truss053_genesis_owner import (
            ManagedCurrentTruss053GenesisSourceOwner,
        )
        from .truss_detached_a1_genesis_codec import encode_detached_a1_genesis_snapshot
        from .truss_detached_source_snapshot_codec import encode_detached_source_snapshot
        from .truss_detached_source_snapshot_codec import decode_detached_source_snapshot
        from .truss_source_custody_selection import select_truss_source_custody_anchor
        from .truss_current_catalog import installed_fresh_truss_runtime_pins
        if self.session is not None or self.genesis is None or self.genesis_bytes is None:
            raise Truss053CustodyError("Prepublication source holder opens once")
        if self.phase not in ("a0", "a1"):
            raise Truss053CustodyError("Exact A0 or A1 source phase required before custody open")
        if (type(request_bytes) is not bytes or not request_bytes
                or type(destination_id) is not str or not destination_id
                or type(publication_id) is not str or not publication_id):
            raise Truss053CustodyError("Exact frozen A1 request and publication binding required")
        installed_fresh_truss_runtime_pins()
        expected = (None if self.phase == "a0" else
                    json.loads(self._a0_page_next.decode("utf-8"))["nextXid"])
        refreshed = ManagedCurrentTruss053GenesisSourceOwner(
            self.schedule, self.source_selections).admit_a1(
                expected_a_prior_cursor=expected)
        refreshed_bytes = encode_detached_a1_genesis_snapshot(refreshed)
        if not _genesis_matches_with_monotonic_watermarks(self.genesis_bytes,
                refreshed_bytes, expected_a_prior_cursor=("0" if expected is None else expected)):
            raise Truss053CustodyError(
                "A/B page, cursor, catalog, accepted document, or model changed since genesis admission")
        self.request_bytes = request_bytes
        self.destination_id = destination_id
        self.publication_id = publication_id
        observation = self.genesis.source_observations["A"]
        self.anchor = select_truss_source_custody_anchor(
            schedule=self.schedule, source="A", page=observation.page,
            request_bytes=self.request_bytes, destination_id=self.destination_id,
            preselected_publication_id=self.publication_id,
            incremental=self.phase in ("a0", "a1"))
        open_selection = self.anchor.open_selection()
        selected_a = self.source_selections["A"]
        # The public owner selection stores a no-argument, independently
        # admitted connection factory. The custody API receives holder context.
        def connect(_context):
            return selected_a.connection_factory()
        self.session = Truss053SourceCustodySession(
            connection_factory=connect, selection=open_selection,
            context=self.context)
        self.session.open()
        page = self.session.observe_selected_page()
        fields = ("content", "sha256", "prior", "next", "consumer_id",
                  "registration_id", "writer_xid", "outcome")
        if any(getattr(page, item) != getattr(observation.page, item) for item in fields):
            self.close_before_ack()
            raise Truss053CustodyError(
                "Held source page differs from the public A/B selection snapshot")
        self.snapshot_bytes = encode_detached_source_snapshot(self.anchor, observation)
        from .a1_ack_attempt import MAX_SELECTION_BYTES
        if len(self.snapshot_bytes) > MAX_SELECTION_BYTES:
            self.close_before_ack()
            raise Truss053CustodyError(
                "Detached source snapshot exceeds durable one-use ACK record selection bound")
        self.custody_digests = detached_source_custody_digests(
            decode_detached_source_snapshot(self.snapshot_bytes))
        return self.snapshot_bytes

    def renew(self):
        if self.session is None or self._closed:
            raise Truss053CustodyError("Live original fresh-Truss source holder required")
        observation = self.session.renew(
            phase="ack_unresolved" if self._matched_ack is not None else "before_ack")
        return observation

    def bind_publication(self, *, manifest_bytes: bytes, vector_payload: dict,
                         receipt_bytes: bytes):
        from .managed_current_spark_receipt_read import (
            PROFILE as MANAGED_READ_PROFILE,
            ManagedCurrentSparkReceiptReopenObservation,
        )
        from ashlar.schema import decode_retained_json
        from ashlar.pins import PinVector
        if self.session is None or self._closed or self.bound_selection is not None:
            raise Truss053CustodyError("One live post-verifier bind to the original source hold required")
        if (type(vector_payload) is not dict
                or set(vector_payload) != {"authority", "scope_kind", "scope_id",
                    "custody_digest", "targets"}
                or type(vector_payload.get("targets")) is not dict):
            raise Truss053CustodyError("Exact closed four-role vector payload required")
        try:
            vector = PinVector(
                vector_payload["authority"], vector_payload["scope_kind"],
                vector_payload["scope_id"], vector_payload["custody_digest"],
                {name: tuple(target) for name, target in vector_payload["targets"].items()})
            manifest = decode_retained_json(manifest_bytes)
            publication_id = manifest["publication_id"]
            observation = ManagedCurrentSparkReceiptReopenObservation(
                MANAGED_READ_PROFILE, publication_id, manifest,
                hashlib.sha256(manifest_bytes).hexdigest(), receipt_bytes,
                hashlib.sha256(receipt_bytes).hexdigest(), vector, {})
        except (KeyError, TypeError, ValueError) as error:
            raise Truss053CustodyError("Exact managed publication binding could not be decoded") from error
        selection = self.anchor.bind_publication(
            manifest_bytes=manifest_bytes, vector=vector,
            receipt_bytes=receipt_bytes, observation=observation)
        self.session.bind_publication(selection)
        self.bound_selection = selection
        return True

    def acknowledge_once(self, *, attempt_directory, generation: int):
        from .a1_ack_attempt import DurableA1AckAttemptStore, A1AckSettlement
        if (self.session is None or self._closed or self.bound_selection is None
                or self.snapshot_bytes is None or type(generation) is not int):
            raise Truss053CustodyError("Bound original fresh-Truss holder required for one ACK command")
        store = DurableA1AckAttemptStore(attempt_directory)
        attempt = store.load()
        dispatch = store._read_record("ack-command-dispatched.json")
        if (attempt.selection_bytes != self.snapshot_bytes
                or dispatch.get("attempt_id") != attempt.attempt_id
                or dispatch.get("generation") != generation
                or dispatch.get("process_group_id") != os.getpgrp()
                or dispatch.get("status") != "ack_command_may_have_been_sent"
                or dispatch.get("ack_retry_permitted") is not False
                or attempt.request_bytes != self.request_bytes
                or attempt.manifest_bytes != self.bound_selection.manifest_bytes
                or attempt.receipt_bytes != self.bound_selection.receipt_bytes):
            raise Truss053CustodyError(
                "Exact dispatched original source selection and publication bytes required")
        try:
            self.session.acknowledge()
        except BaseException:
            # A possibly committed ACK has only the exact public read-only
            # reconciliation path on this retained source session.
            if not self.session._ack_attempted:
                raise
        value = self.session.reconcile()
        receipt = value.receipt.receipt_bytes if value.outcome == "matched" else None
        settlement = A1AckSettlement(value.outcome, value.checkpoint.content,
            receipt, attempt.request_sha256, attempt.manifest_sha256,
            attempt.selection_sha256)
        if value.outcome == "matched":
            self._matched_ack = (settlement, Path(attempt_directory).resolve(),
                                 attempt.attempt_id, generation)
        else:
            self.session.dispose()
            self.session = None
            self._closed = True
            self._close_bootstrap_runtimes()
        return settlement

    def finish_ack(self, *, settlement, attempt_directory, generation: int):
        """Close a matched source interval only after the caller's post-ACK observation."""
        from .a1_ack_attempt import (
            DurableA1AckAttemptStore, A1AckSettlement, MAX_SETTLEMENT_RECORD_BYTES,
        )
        expected = self._matched_ack
        if (self._closed or self.session is None or expected is None
                or type(settlement) is not A1AckSettlement or settlement != expected[0]
                or Path(attempt_directory).resolve() != expected[1]
                or type(generation) is not int or generation != expected[3]
                or settlement.outcome != "matched"):
            raise Truss053CustodyError(
                "Exact matched ACK and original durable attempt required to close source hold")
        store = DurableA1AckAttemptStore(attempt_directory)
        attempt = store.load()
        record = store._read_record("worker-settlement.json", MAX_SETTLEMENT_RECORD_BYTES)
        dispatch = store._read_record("ack-command-dispatched.json")
        if (attempt.attempt_id != expected[2] or record.get("attempt_id") != expected[2]
                or dispatch.get("attempt_id") != expected[2]
                or dispatch.get("generation") != generation
                or dispatch.get("process_group_id") != os.getpgrp()
                or dispatch.get("status") != "ack_command_may_have_been_sent"
                or dispatch.get("ack_retry_permitted") is not False
                or record.get("outcome") != "matched"
                or record.get("checkpoint_bytes_base64") != base64.b64encode(
                    settlement.checkpoint_bytes).decode("ascii")
                or record.get("receipt_bytes_base64") != (None if settlement.receipt_bytes is None
                    else base64.b64encode(settlement.receipt_bytes).decode("ascii"))
                or record.get("request_sha256") != settlement.request_sha256
                or record.get("manifest_sha256") != settlement.manifest_sha256
                or record.get("selection_sha256") != settlement.selection_sha256):
            raise Truss053CustodyError("Durable matched settlement changed before source close")
        self.session.close()
        self.session.dispose()
        self.session = None
        self.bound_selection = None
        self.request_bytes = None
        self.snapshot_bytes = None
        self.anchor = None
        self._matched_ack = None
        if self.phase == "a0":
            self._a0_settlement = settlement
            self._a0_page_next = settlement.checkpoint_bytes
            self.genesis = None
            self.genesis_bytes = None
            self.phase = "a0_closed"
        else:
            self._closed = True
            self._close_bootstrap_runtimes()
        return True

    def advance_after_a0_ack(self, *, settlement):
        """Apply the already-frozen A1 group only after exact matched A0 ACK."""
        from .truss_detached_a1_genesis_codec import encode_detached_a1_genesis_snapshot
        from .a1_ack_attempt import A1AckSettlement
        from .managed_current_truss053_genesis_owner import (
            ManagedCurrentTruss053GenesisSourceOwner,
        )
        settled = self._a0_settlement
        if (self.phase != "a0_closed" or self._closed or settled is None
                or type(settlement) is not A1AckSettlement
                or settlement != settled or settled.outcome != "matched"
                or settlement.checkpoint_bytes != self._a0_page_next
                or self._advance_a1_callback is None):
            raise Truss053CustodyError(
                "Exact matched A0 settlement and next cursor required before A1 source effect")
        self._advance_a1_callback(settlement.checkpoint_bytes)
        self.phase = "a1"
        self._a0_settlement = None
        self.genesis = None
        self.genesis_bytes = None
        self.genesis = ManagedCurrentTruss053GenesisSourceOwner(
            self.schedule, self.source_selections).admit_a1(
                expected_a_prior_cursor=json.loads(
                    settlement.checkpoint_bytes.decode("utf-8"))["nextXid"])
        self.genesis_bytes = encode_detached_a1_genesis_snapshot(self.genesis)
        return self.genesis_bytes

    def _close_bootstrap_runtimes(self):
        owner = getattr(self, "_bootstrap_runtimes", None)
        if owner is not None:
            self._bootstrap_runtimes = None
            owner.close()

    def close_before_ack(self):
        if self.session is None or self._closed:
            self._close_bootstrap_runtimes()
            return
        primary = None
        try:
            if not self.session._ack_attempted:
                self.session.close()
            self.session.dispose()
        except BaseException as error:
            primary = error
        self._closed = True
        try:
            self._close_bootstrap_runtimes()
        except BaseException as error:
            if primary is None:
                primary = error
        if primary is not None:
            raise primary


class Truss053ReadOnlyAckReconciler:
    """Fresh public source read that settles an original dispatched ACK attempt.

    The reconciler never writes an ACK. Its connection and transaction are new;
    the original exact page and durable attempt bytes supply the query identity.
    """

    def __init__(self, connection_factory, *, context, statement_timeout_ms=10000):
        if (not callable(connection_factory) or context is None
                or type(statement_timeout_ms) is not int
                or not 1 <= statement_timeout_ms <= 60000):
            raise Truss053CustodyError(
                "Fresh Truss reconciliation connection owner and bounded timeout required")
        self.connection_factory = connection_factory
        self.context = context
        self.statement_timeout_ms = statement_timeout_ms

    def reconcile(self, attempt):
        from .a1_ack_attempt import A1AckAttempt, A1AckSettlement
        from .truss_detached_source_snapshot_codec import (
            decode_detached_source_snapshot,
        )
        from .truss_current_catalog import installed_fresh_truss_runtime_pins
        from truss import (CatalogRuntimeConfiguration, CatalogRuntimeStatus,
                           FeedAckReconciled, FeedUnavailable,
                           adopt_psycopg_host, create_reference_assembly)

        if type(attempt) is not A1AckAttempt:
            raise Truss053CustodyError("Exact durable original A1 ACK attempt required")
        installed_fresh_truss_runtime_pins()
        snapshot = decode_detached_source_snapshot(attempt.selection_bytes)
        if (attempt.request_bytes != snapshot.source_page.request_bytes
                or attempt.page_sha256 != snapshot.source_page.page_fields["sha256"]
                or attempt.destination_id != snapshot.source_page.destination_id
                or attempt.publication_id != snapshot.source_page.publication_id
                or attempt.prior_checkpoint_bytes != snapshot.source_page.prior_bytes
                or attempt.next_checkpoint_bytes != snapshot.source_page.next_bytes
                or hashlib.sha256(attempt.request_bytes).hexdigest()
                   != snapshot.source_page.request_sha256):
            raise Truss053CustodyError(
                "Durable ACK attempt differs from detached original Truss page")
        page = snapshot.source_page.materialize_page()
        connection = self.connection_factory(self.context)
        if (getattr(connection, "autocommit", True)
                or getattr(getattr(connection, "info", None), "transaction_status", None) != 0):
            try:
                connection.close()
            finally:
                raise Truss053CustodyError(
                    "Fresh idle non-autocommit reconciliation connection required")
        host = assembly = None
        primary = None
        value = None
        try:
            connection.execute(
                "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY")
            connection.execute(
                f"SET LOCAL statement_timeout = '{self.statement_timeout_ms}ms'")
            host = adopt_psycopg_host(connection,
                statement_timeout_ms=self.statement_timeout_ms,
                operation_timeout_ms=self.statement_timeout_ms)
            transaction = host.adopt_transaction(access_mode="read_only")
            from .truss052_source_custody import _value
            transaction = _value(transaction)
            assembly = _value(create_reference_assembly(
                CatalogRuntimeConfiguration(), host))
            status = _value(assembly.status_in_transaction(transaction))
            if (type(status) is not CatalogRuntimeStatus or status.installed is not True
                    or status.installation_id != snapshot.progress.installation_id):
                raise Truss053CustodyError(
                    "Fresh public Truss status differs from original selected installation")
            value = _value(assembly.reconcile_feed_ack_in_transaction(
                transaction, page, destination_id=attempt.destination_id,
                request_sha256=attempt.request_sha256,
                publication_id=attempt.publication_id,
                receipt_bytes=attempt.receipt_bytes))
            if type(value) is not FeedAckReconciled:
                if isinstance(value, FeedUnavailable):
                    raise Truss053CustodyError(
                        "Fresh public Truss ACK reconciliation is unavailable")
                raise Truss053CustodyError(
                    "Exact public Truss ACK reconciliation result required")
            if value.outcome not in ("matched", "missing", "conflicting"):
                raise Truss053CustodyError("Unknown public Truss ACK reconciliation outcome")
            checkpoint = value.checkpoint.content
            if value.outcome == "matched":
                receipt = value.receipt
                if (checkpoint != attempt.next_checkpoint_bytes
                        or receipt is None
                        or receipt.installation_id != snapshot.progress.installation_id
                        or receipt.source_epoch != snapshot.progress.epoch
                        or receipt.consumer_id != page.consumer_id
                        or receipt.registration_id != page.registration_id
                        or receipt.prior.content != page.prior.content
                        or receipt.next.content != page.next.content
                        or receipt.feed_sha256 != page.sha256
                        or receipt.destination_id != attempt.destination_id
                        or receipt.request_sha256 != attempt.request_sha256
                        or receipt.publication_id != attempt.publication_id
                        or receipt.receipt_bytes != attempt.receipt_bytes
                        or receipt.receipt_sha256 != hashlib.sha256(
                            attempt.receipt_bytes).hexdigest()):
                    raise Truss053CustodyError(
                        "Fresh Truss receipt differs from exact dispatched attempt")
                receipt_bytes = receipt.receipt_bytes
            else:
                if checkpoint not in (attempt.prior_checkpoint_bytes,
                                      attempt.next_checkpoint_bytes):
                    raise Truss053CustodyError(
                        "Unmatched reconciliation checkpoint differs from original attempt")
                receipt_bytes = None
        except BaseException as error:
            primary = error
            raise
        finally:
            failures = []
            if assembly is not None:
                try:
                    assembly.dispose()
                except BaseException as error:
                    failures.append(error)
            if host is not None:
                try:
                    if not host.detach():
                        raise Truss053CustodyError(
                            "Fresh Truss reconciliation host detach was not confirmed")
                except BaseException as error:
                    failures.append(error)
            if not getattr(connection, "closed", False):
                try:
                    connection.rollback()
                except BaseException as error:
                    failures.append(error)
                try:
                    connection.close()
                except BaseException as error:
                    failures.append(error)
            if failures and primary is None:
                raise Truss053CustodyError(
                    "Fresh Truss reconciliation cleanup failed") from failures[0]
        return A1AckSettlement(value.outcome, checkpoint, receipt_bytes,
            attempt.request_sha256, attempt.manifest_sha256,
            attempt.selection_sha256)
