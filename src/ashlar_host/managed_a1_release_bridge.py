# SPDX-License-Identifier: MIT OR Apache-2.0
"""Post-publication A1 bridge for independently admitted live owner ports.

This bridge coordinates one selected Truss .52 A1 source page with the existing
managed read/pin port and a source-owned protected receipt ACK port. It does not
create compute, publish, create pins, select source authority, or mint an ACK
capability. Each supplied owner must authenticate and renew its own authority.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import time
from typing import Protocol

from ashlar.pins import PinVector
from ashlar.staging import batch_row
from ashlar.source_checkpoint import truss_checkpoint


PROFILE = "ashlar-managed-a1-release-bridge/0.1"
MAX_INTERVAL_SECONDS = 900
ACK_RECONCILE_WINDOW_SECONDS = 30


class ManagedA1BridgeError(ValueError):
    """A1 source, publication, owner, or closing correspondence refused."""


class ManagedA1AckUncertain(ManagedA1BridgeError):
    """The one ACK attempt could not be settled from exact source readback."""


class ManagedA1AckIncomplete(ManagedA1BridgeError):
    """ACK was exactly reconciled, but an independent owner failed to close."""


class RenewableA1OwnerHandle(Protocol):
    def renew(self) -> object: ...
    def remaining_lifetime_seconds(self) -> float: ...
    def close(self) -> object: ...


class ManagedA1SourceCustodyPort(Protocol):
    """Live Truss .52 A/B holder; must reread source and ACK-scope custody."""

    def open(self, *, selection: object, request_bytes: bytes,
             manifest_bytes: bytes, receipt_bytes: bytes,
             vector: PinVector, context: object) -> RenewableA1OwnerHandle: ...


class ManagedA1ProtectedReceiptAckPort(Protocol):
    """Source-owned one-shot receipt ACK plus fresh exact reconciliation."""

    def commit(self, request_bytes: bytes, manifest_bytes: bytes,
               receipt_bytes: bytes, context: object, *,
               deadline_monotonic: float) -> object: ...

    def reconcile(self, request_bytes: bytes, manifest_bytes: bytes,
                  receipt_bytes: bytes, context: object, *,
                  deadline_monotonic: float) -> tuple[bytes, bytes, bytes]: ...


@dataclass(frozen=True)
class ManagedA1VerifierTerminalSuccess:
    """Fresh verifier-run readback bound to the exact A1 publication bytes."""
    profile: str
    run_id: str
    task_id: str
    terminal: bool
    status: str
    outcome: str
    request_sha256: str
    manifest_sha256: str
    destination_receipt_sha256: str
    vector_sha256: str
    result_sha256: str


class ManagedA1VerifierCompletionPort(Protocol):
    """Independently observe durable terminal verifier success before open."""

    def verify_terminal_success(self, *, selection: object,
            request_bytes: bytes, manifest_bytes: bytes, receipt_bytes: bytes,
            vector: PinVector, context: object) -> ManagedA1VerifierTerminalSuccess: ...


@dataclass(frozen=True)
class ManagedA1AckReceipt:
    profile: str
    source: str
    publication_id: str
    request_sha256: str
    manifest_sha256: str
    destination_receipt_sha256: str
    outcome: str
    commit_response_received: bool
    elapsed_seconds: float


def _decode_object(raw: bytes, label: str) -> dict:
    if type(raw) is not bytes or not 0 < len(raw) <= 4 * 1024 * 1024:
        raise ManagedA1BridgeError(f"Bounded exact {label} bytes required")

    def unique_members(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate member")
            result[key] = value
        return result

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_members,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (UnicodeError, ValueError, json.JSONDecodeError) as error:
        raise ManagedA1BridgeError(f"Closed exact {label} JSON required") from error
    if type(value) is not dict:
        raise ManagedA1BridgeError(f"Closed exact {label} object required")
    return value


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _vector_sha(vector: PinVector) -> str:
    value = {"authority": vector.authority, "scope_kind": vector.scope_kind,
             "scope_id": vector.scope_id, "custody_digest": vector.custody_digest,
             "targets": {table: list(target) for table, target in vector.targets.items()}}
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True,
                     separators=(",", ":"), allow_nan=False).encode("utf-8")
    return _sha(raw)


def _validate_managed_selection(port, request_bytes, manifest_bytes,
                                receipt_bytes, vector, context) -> None:
    from .managed_operational_read_port import (
        ManagedOperationalReadPort,
        ManagedOperationalReadPinPort,
    )
    from .managed_operational_retention_interval import (
        ManagedOperationalRetentionSelection,
    )

    if (type(port) is not ManagedOperationalReadPinPort
            or type(getattr(port, "read_port", None)) is not ManagedOperationalReadPort
            or context is not getattr(port, "context", None)
            or context is not getattr(port.read_port, "context", None)):
        raise ManagedA1BridgeError(
            "Original managed operational read/pin owner and exact holder context required")
    selected = getattr(port.read_port, "selection", None)
    if (type(selected) is not ManagedOperationalRetentionSelection
            or getattr(selected, "request_bytes", None) != request_bytes
            or getattr(selected, "manifest_bytes", None) != manifest_bytes
            or getattr(selected, "receipt_bytes", None) != receipt_bytes
            or getattr(selected, "vector", None) != vector):
        raise ManagedA1BridgeError(
            "Exact original request, manifest, receipt and vector bytes must match the managed selection")


def _require_coverage(role, handle, minimum_seconds: float) -> None:
    remaining = getattr(handle, "remaining_lifetime_seconds", None)
    if not callable(remaining):
        raise ManagedA1BridgeError(f"{role} live bounded renewal interval required")
    value = remaining()
    if (type(value) not in (int, float) or not math.isfinite(value)
            or value < minimum_seconds):
        raise ManagedA1BridgeError(
            f"{role} live renewal interval does not cover bounded ACK/reconciliation window")


def _require_native_pin_coverage(handle, minimum_seconds: float) -> None:
    remaining = getattr(handle, "native_pin_remaining_lifetime_seconds", None)
    if not callable(remaining):
        raise ManagedA1BridgeError(
            "managed native pin transaction lifetime admission required")
    value = remaining()
    if (type(value) not in (int, float) or not math.isfinite(value)
            or value < minimum_seconds):
        raise ManagedA1BridgeError(
            "managed native pin transaction lifetime does not cover bounded ACK/reconciliation window")


def _elapsed(clock, started) -> float:
    current = clock()
    if (type(current) not in (int, float) or not math.isfinite(current)
            or type(started) not in (int, float) or not math.isfinite(started)
            or current < started):
        raise ManagedA1BridgeError("Finite nondecreasing monotonic holder clock required")
    return float(current - started)


def _validate_plan(plan, request_bytes: bytes, manifest_bytes: bytes,
                   receipt_bytes: bytes, vector: PinVector) -> tuple[str, str]:
    from .managed_current_a1_trial_plan import ManagedCurrentA1TrialPlan
    from .managed_current_truss052_genesis_owner import (
        ManagedCurrentTruss052GenesisSelection,
    )

    if type(plan) is not ManagedCurrentA1TrialPlan:
        raise ManagedA1BridgeError("Exact prepared managed A1 trial plan required")
    selection = plan.selection
    if type(selection) is not ManagedCurrentTruss052GenesisSelection:
        raise ManagedA1BridgeError("Original Truss .52 A/B source-owner selection required")
    if (type(selection.source_observations) is not dict
            or set(selection.source_observations) != {"A", "B"}
            or selection.source_observations["A"].batch is not selection.source_owner_admitted_batch
            or selection.source_observations["B"].batch is not None
            or selection.admission.batch is not selection.source_owner_admitted_batch):
        raise ManagedA1BridgeError("Original A1 page and both selected source observations required")
    if type(vector) is not PinVector:
        raise ManagedA1BridgeError("Complete original managed publication vector required")
    if (len(vector.targets) != 4
            or {name.rsplit(".", 1)[-1] for name in vector.targets}
                != {"object_current", "edge_current", "tombstone", "whole_source_history"}):
        raise ManagedA1BridgeError("Exact four-role A1 graph-data vector required")

    request = _decode_object(request_bytes, "A1 request")
    manifest = _decode_object(manifest_bytes, "A1 publication manifest")
    if request != plan.request:
        raise ManagedA1BridgeError("Exact prepared A1 request bytes differ")
    inputs = plan.driver_inputs
    expected_inputs = {"admission", "stream", "materialized_at", "context",
                       "managed_genesis_predecessor", "source_owner_admitted_batch"}
    if type(inputs) is not dict or set(inputs) != expected_inputs:
        raise ManagedA1BridgeError("Exact prepared A1 task inputs required")
    from .managed_current_a1_trial_plan import prepare_managed_current_a1_trial
    try:
        rebuilt = prepare_managed_current_a1_trial(
            selection, plan.predecessor.manifest, stream=inputs["stream"],
            materialized_at=inputs["materialized_at"], context=inputs["context"])
    except (TypeError, ValueError, KeyError, AttributeError) as error:
        raise ManagedA1BridgeError("Original A1 plan and genesis predecessor failed revalidation") from error
    if (rebuilt.request != plan.request
            or rebuilt.delivery_page != plan.delivery_page
            or rebuilt.predecessor.manifest != plan.predecessor.manifest
            or inputs["admission"] is not selection.admission
            or inputs["managed_genesis_predecessor"] is not plan.predecessor
            or inputs["source_owner_admitted_batch"] is not selection.source_owner_admitted_batch):
        raise ManagedA1BridgeError("Prepared A1 plan differs from its original owners")
    batch = selection.source_owner_admitted_batch
    batch_row_value = batch_row(batch)
    if (request.get("source_checkpoint_json") != truss_checkpoint(batch)
            or request.get("source_batch_json") != batch_row_value["batch_json"]
            or request.get("source_batch_digest") != batch_row_value["batch_digest"]
            or type(manifest.get("publication_id")) is not str
            or not manifest["publication_id"]
            or request.get("predecessor") != plan.predecessor.manifest.get("publication_id")):
        raise ManagedA1BridgeError("A1 request, admitted source page, or manifest differs")
    try:
        from .driver import validate_union
        validate_union(plan.predecessor.source_progress, request, manifest)
        versions = _decode_object(manifest["table_versions_json"].encode("utf-8"),
                                  "A1 manifest vector")
    except (TypeError, ValueError, KeyError, AttributeError) as error:
        raise ManagedA1BridgeError("Complete A/B progress and original publication vector required") from error
    expected_versions = {table: target[1] for table, target in vector.targets.items()}
    if (set(versions) != set(expected_versions)
            or any(type(versions[table]) is not int
                   or versions[table] != version
                   for table, version in expected_versions.items())):
        raise ManagedA1BridgeError("A1 manifest four-role versions differ from the pinned vector")
    try:
        from .truss_feed_sessions import encode_truss_destination_receipt
        expected_receipt = encode_truss_destination_receipt(
            request_bytes, manifest_bytes, vector)
    except (TypeError, ValueError) as error:
        raise ManagedA1BridgeError("Original bounded Truss destination receipt cannot be reconstructed") from error
    if receipt_bytes != expected_receipt:
        raise ManagedA1BridgeError("Exact source-retained receipt differs from request, manifest and vector")
    return "A", manifest["publication_id"]


class ManagedA1ReleaseBridge:
    """Run the bounded holder sequence over concrete separately owned ports.

    ``managed_read_pin_port`` is the existing composed managed operational
    read plus native Postgres pin owner. Its renewal performs the pre-ACK
    four-role source-derived read and native pin check. The source and ACK
    ports remain distinct and source-owned. A test double can exercise order,
    but only the owners supplied by the live task can establish authority.
    """

    def __init__(self, managed_read_pin_port, source_custody_port,
                 protected_ack_port, verifier_completion_port, *, clock=time.monotonic,
                 ack_reconcile_window_seconds=ACK_RECONCILE_WINDOW_SECONDS,
                 maximum_interval_seconds=MAX_INTERVAL_SECONDS):
        if (not callable(getattr(managed_read_pin_port, "open", None))
                or not callable(getattr(source_custody_port, "open", None))
                or not callable(getattr(protected_ack_port, "commit", None))
                or not callable(getattr(protected_ack_port, "reconcile", None))
                or not callable(getattr(verifier_completion_port,
                                        "verify_terminal_success", None))
                or not callable(clock)
                or type(ack_reconcile_window_seconds) not in (int, float)
                or not math.isfinite(ack_reconcile_window_seconds)
                or not 1 <= ack_reconcile_window_seconds <= MAX_INTERVAL_SECONDS
                or type(maximum_interval_seconds) is not int
                or not 1 <= maximum_interval_seconds <= MAX_INTERVAL_SECONDS
                or ack_reconcile_window_seconds > maximum_interval_seconds):
            raise ManagedA1BridgeError("Managed read/pin, Truss source, ACK and bounded clock owners required")
        self._managed = managed_read_pin_port
        self._source = source_custody_port
        self._ack = protected_ack_port
        self._verifier = verifier_completion_port
        self._clock = clock
        self._ack_reconcile_window_seconds = float(ack_reconcile_window_seconds)
        self._maximum_interval_seconds = maximum_interval_seconds
        self._used = False

    def acknowledge(self, *, plan, request_bytes: bytes, manifest_bytes: bytes,
                    receipt_bytes: bytes, vector: PinVector, context: object):
        if self._used:
            raise ManagedA1BridgeError("One A1 bridge instance admits one source ACK attempt")
        if context is None:
            raise ManagedA1BridgeError("Original live holder context required")
        if type(receipt_bytes) is not bytes or not 0 < len(receipt_bytes) <= 1024 * 1024:
            raise ManagedA1BridgeError("Exact bounded original destination receipt bytes required")
        _validate_managed_selection(self._managed, request_bytes, manifest_bytes,
                                    receipt_bytes, vector, context)
        source_slot, publication_id = _validate_plan(
            plan, request_bytes, manifest_bytes, receipt_bytes, vector)
        selection = plan.selection
        completion = self._verifier.verify_terminal_success(
            selection=selection, request_bytes=request_bytes,
            manifest_bytes=manifest_bytes, receipt_bytes=receipt_bytes,
            vector=vector, context=context)
        if (type(completion) is not ManagedA1VerifierTerminalSuccess
                or completion.profile != "ashlar-managed-a1-verifier-terminal-success/0.1"
                or type(completion.run_id) is not str or not completion.run_id
                or type(completion.task_id) is not str or not completion.task_id
                or completion.terminal is not True
                or completion.status != "SUCCEEDED"
                or completion.outcome != "durably_verified"
                or any(type(value) is not str for value in (
                    completion.request_sha256, completion.manifest_sha256,
                    completion.destination_receipt_sha256, completion.vector_sha256,
                    completion.result_sha256))
                or completion.request_sha256 != _sha(request_bytes)
                or completion.manifest_sha256 != _sha(manifest_bytes)
                or completion.destination_receipt_sha256 != _sha(receipt_bytes)
                or completion.vector_sha256 != _vector_sha(vector)
                or len(completion.result_sha256) != 64
                or any(character not in "0123456789abcdef"
                       for character in completion.result_sha256)):
            raise ManagedA1BridgeError(
                "Fresh durable terminal verifier success for the exact A1 publication required")
        opened: list[tuple[str, object]] = []
        primary: BaseException | None = None
        primary_tb = None
        started = self._clock()
        reconciled = False
        commit_response_received = False
        try:
            self._used = True
            managed = self._managed.open(receipt_bytes, vector, context)
            opened.append(("managed_read_pin", managed))
            source = self._source.open(selection=selection, request_bytes=request_bytes,
                manifest_bytes=manifest_bytes, receipt_bytes=receipt_bytes,
                vector=vector, context=context)
            if source is managed:
                raise ManagedA1BridgeError("Distinct managed and Truss owner handles required")
            opened.append(("truss_source", source))
            if any(not callable(getattr(handle, method, None))
                   for _, handle in opened for method in ("renew", "close")):
                raise ManagedA1BridgeError("Renewable and independently closable live owner handles required")
            for role, handle in opened:
                _require_coverage(role, handle, self._ack_reconcile_window_seconds)
            _require_native_pin_coverage(
                managed, self._ack_reconcile_window_seconds)
            if _elapsed(self._clock, started) > self._maximum_interval_seconds:
                raise ManagedA1BridgeError("Managed A1 holder interval exceeded its selected limit")

            # Both owners independently reobserve their original custody just
            # before the only source-side effect.
            managed.renew()
            source.renew()
            for role, handle in opened:
                _require_coverage(role, handle, self._ack_reconcile_window_seconds)
            _require_native_pin_coverage(
                managed, self._ack_reconcile_window_seconds)
            if _elapsed(self._clock, started) > self._maximum_interval_seconds:
                raise ManagedA1BridgeError("Managed A1 holder interval expired before protected ACK")

            ack_started = self._clock()
            if _elapsed(self._clock, started) > self._maximum_interval_seconds:
                raise ManagedA1BridgeError("Managed A1 holder interval expired before protected ACK")
            ack_deadline = ack_started + self._ack_reconcile_window_seconds
            commit_error = None
            try:
                self._ack.commit(request_bytes, manifest_bytes, receipt_bytes, context,
                                 deadline_monotonic=ack_deadline)
                commit_response_received = True
            except BaseException as error:
                # The source may have committed despite a lost response. Never
                # retry; only a fresh exact receipt readback can settle it.
                commit_error = error
            try:
                readback = self._ack.reconcile(
                    request_bytes, manifest_bytes, receipt_bytes, context,
                    deadline_monotonic=ack_deadline)
            except BaseException as error:
                uncertain = ManagedA1AckUncertain(
                    "Protected A1 ACK requires fresh exact reconciliation")
                if commit_error is not None:
                    raise uncertain from commit_error
                raise uncertain from error
            if (type(readback) is not tuple or len(readback) != 3
                    or readback != (request_bytes, manifest_bytes, receipt_bytes)):
                uncertain = ManagedA1AckUncertain(
                    "Protected A1 ACK exact source readback differs")
                if commit_error is not None:
                    raise uncertain from commit_error
                raise uncertain
            if _elapsed(self._clock, ack_started) > self._ack_reconcile_window_seconds:
                raise ManagedA1AckIncomplete(
                    "A1 ACK and reconciliation exceeded the admitted bounded window")
            reconciled = True
        except BaseException as error:
            primary, primary_tb = error, error.__traceback__

        failures: list[tuple[str, BaseException]] = []
        # Keep Truss source custody live through the managed composite's final
        # read and pin checks; source closes only after those checks finish.
        for role, handle in opened:
            try:
                handle.close()
            except BaseException as error:
                failures.append((role, error))
        if primary is not None:
            add_note = getattr(primary, "add_note", None)
            if failures and callable(add_note):
                add_note("Managed A1 owner close failures: " + ", ".join(
                    f"{role}:{type(error).__name__}" for role, error in failures))
            raise primary.with_traceback(primary_tb)
        if failures:
            if reconciled:
                raise ManagedA1AckIncomplete(
                    "A1 protected ACK reconciled, but independent owner close failed") from failures[0][1]
            raise ManagedA1BridgeError("Managed A1 owner close failed") from failures[0][1]
        elapsed = _elapsed(self._clock, started)
        if elapsed > self._maximum_interval_seconds:
            raise ManagedA1AckIncomplete("Managed A1 interval exceeded its selected limit at close")
        return ManagedA1AckReceipt(PROFILE, source_slot, publication_id,
            _sha(request_bytes), _sha(manifest_bytes), _sha(receipt_bytes),
            "acknowledged_exactly_reconciled", commit_response_received, elapsed)
