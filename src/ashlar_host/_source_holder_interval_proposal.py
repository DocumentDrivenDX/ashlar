# SPDX-License-Identifier: MIT OR Apache-2.0
"""Proposal-only source-holder handle lifetime choreography.

This private helper mirrors the draft ports in CONTRACT-001 and TD-009. It
exposes injected-port choreography for exact-byte ACK commit and reconciliation.
It is not a native adapter and establishes no source authority. No campaign or
Truss entrypoint imports it.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import math
from typing import Protocol


class SourceHolderIntervalError(ValueError):
    """A draft source-holder port or lifetime operation refused."""


@dataclass(frozen=True)
class SourceHolderCloseFailure:
    handle: str
    error_type: str


class SourceHolderCloseError(SourceHolderIntervalError):
    """Closing failed after the body completed; no interval completion is implied."""

    def __init__(self, failures: tuple[SourceHolderCloseFailure, ...]):
        self.failures = failures
        names = ", ".join(f"{item.handle}:{item.error_type}" for item in failures)
        super().__init__("Draft source-holder close failure(s): " + names)


class RenewableHandle(Protocol):
    def renew(self) -> object: ...
    def close(self) -> object: ...
    def remaining_lifetime_seconds(self) -> float: ...


class ManagedPublicationReadPort(Protocol):
    def open(self, receipt_bytes: bytes, vector: object, context: object) -> RenewableHandle: ...


class NativePinPort(Protocol):
    def open(self, vector: object, source_selection: object, context: object) -> RenewableHandle: ...


class TrussSourceCustodyPort(Protocol):
    def open(self, source_selection: object, request_bytes: bytes, context: object) -> RenewableHandle: ...


class TrussProtectedReceiptAckPort(Protocol):
    def commit(self, request_bytes: bytes, manifest_bytes: bytes, receipt_bytes: bytes, context: object) -> object: ...
    # Reconcile returns exact original (request, manifest, receipt) bytes.
    def reconcile(self, request_bytes: bytes, manifest_bytes: bytes, receipt_bytes: bytes, context: object) -> tuple[bytes, bytes, bytes]: ...


class SourceHolderAckUncertain(SourceHolderIntervalError):
    """ACK was attempted but exact settlement could not be established."""


class SourceHolderAckIncomplete(SourceHolderIntervalError):
    """ACK may be settled, but required close checks did not complete."""


@dataclass(frozen=True)
class SourceHolderIntervalHandles:
    """The three opened handles; their presence proves no authority or duration."""

    managed_publication: RenewableHandle
    native_pin: RenewableHandle
    truss_source: RenewableHandle
    _request_bytes: bytes = field(repr=False, compare=False)
    _receipt_bytes: bytes = field(repr=False, compare=False)
    _context: object = field(repr=False, compare=False)
    _ack_window_seconds: float = field(repr=False, compare=False)
    _active: bool = field(default=True, init=False, repr=False, compare=False)
    _ack_attempted: bool = field(default=False, init=False, repr=False, compare=False)
    _ack_reconciled: bool = field(default=False, init=False, repr=False, compare=False)

    def renew_all(self) -> None:
        """Recheck in the fixed order; callers must not infer a lease duration."""
        if not self._active:
            raise SourceHolderIntervalError("Draft source-holder interval is closed")
        for role, handle in self.ordered():
            try:
                handle.renew()
                _require_coverage(role, handle, self._ack_window_seconds)
            except BaseException as error:
                add_note = getattr(error, "add_note", None)
                if callable(add_note):
                    add_note(f"Draft source-holder renewal failed: {role}")
                raise

    def _expire(self) -> None:
        object.__setattr__(self, "_active", False)

    def ordered(self) -> tuple[tuple[str, RenewableHandle], ...]:
        return (
            ("managed_publication", self.managed_publication),
            ("native_pin", self.native_pin),
            ("truss_source", self.truss_source),
        )


def _record_close_failures(primary: BaseException, failures: tuple[SourceHolderCloseFailure, ...]) -> None:
    if not failures:
        return
    try:
        setattr(primary, "source_holder_close_failures", failures)
    except BaseException:
        pass
    add_note = getattr(primary, "add_note", None)
    if callable(add_note):
        add_note("Draft source-holder close failures: " + ", ".join(
            f"{item.handle}:{item.error_type}" for item in failures
        ))


def _close_reverse(handles: list[tuple[str, object]]) -> tuple[tuple[SourceHolderCloseFailure, ...], BaseException | None]:
    failures = []
    first_error = None
    for role, handle in reversed(handles):
        close = getattr(handle, "close", None)
        if not callable(close):
            failures.append(SourceHolderCloseFailure(role, "MissingCloseMethod"))
            continue
        try:
            close()
        except BaseException as error:
            if first_error is None:
                first_error = error
            failures.append(SourceHolderCloseFailure(role, type(error).__name__))
    return tuple(failures), first_error


def _require_coverage(role: str, handle: object, minimum: float) -> None:
    coverage = getattr(handle, "remaining_lifetime_seconds", None)
    remaining = coverage() if callable(coverage) else None
    if (type(remaining) not in (int, float) or not math.isfinite(remaining)
            or remaining < minimum):
        raise SourceHolderIntervalError(f"Draft {role} does not cover ACK window")


@contextmanager
def hold_source_holder_interval_proposal(
    managed_publication_port: ManagedPublicationReadPort,
    native_pin_port: NativePinPort,
    truss_source_port: TrussSourceCustodyPort,
    *,
    receipt_bytes: bytes,
    vector: object,
    source_selection: object,
    request_bytes: bytes,
    context: object,
    ack_window_seconds: float = 30.0,
):
    """Open, renew-all, yield and reverse-close the draft source-holder interval.

    The API is deliberately disconnected from campaign code. An open/renew
    success says only that injected ports returned without error at that
    observation; it does not prove retention, authority, or continuous lease
    coverage while the caller body runs.
    """
    if (type(receipt_bytes) is not bytes or not receipt_bytes
            or type(request_bytes) is not bytes or not request_bytes
            or vector is None or source_selection is None or context is None
            or type(ack_window_seconds) not in (int, float)
            or not math.isfinite(ack_window_seconds) or ack_window_seconds <= 0):
        raise SourceHolderIntervalError("Exact draft source-holder inputs required")
    ports = (
        ("managed_publication", managed_publication_port,
         (receipt_bytes, vector, context)),
        ("native_pin", native_pin_port, (vector, source_selection, context)),
        ("truss_source", truss_source_port,
         (source_selection, request_bytes, context)),
    )
    handles: list[tuple[str, object]] = []
    interval = None
    primary: BaseException | None = None
    primary_traceback = None
    try:
        opened = {}
        for role, port, arguments in ports:
            open_handle = getattr(port, "open", None)
            if not callable(open_handle):
                raise SourceHolderIntervalError(f"Draft {role} open port required")
            handle = open_handle(*arguments)
            if handle is not None:
                if any(handle is acquired for _, acquired in handles):
                    raise SourceHolderIntervalError("Three distinct draft source-holder handles required")
                handles.append((role, handle))
            if (handle is None or not callable(getattr(handle, "renew", None))
                    or not callable(getattr(handle, "close", None))):
                raise SourceHolderIntervalError(f"Draft {role} renewable closable handle required")
            opened[role] = handle
        interval = SourceHolderIntervalHandles(
            opened["managed_publication"], opened["native_pin"], opened["truss_source"],
            request_bytes, receipt_bytes, context, ack_window_seconds,
        )
        interval.renew_all()
        yield interval
    except BaseException as error:
        primary = error
        primary_traceback = error.__traceback__

    if interval is not None:
        interval._expire()
    failures, first_close_error = _close_reverse(handles)
    if primary is not None:
        _record_close_failures(primary, failures)
        raise primary.with_traceback(primary_traceback)
    if failures:
        if interval is not None and interval._ack_reconciled:
            raise SourceHolderAckIncomplete(
                "Protected ACK reconciled but source-holder closes were incomplete"
            ) from first_close_error
        raise SourceHolderCloseError(failures) from first_close_error


def commit_and_reconcile_protected_ack_proposal(
    interval: SourceHolderIntervalHandles,
    ack_port: TrussProtectedReceiptAckPort,
    *, request_bytes: bytes, manifest_bytes: bytes, receipt_bytes: bytes,
    context: object,
) -> None:
    """Renew immediately before one exact-byte commit and exact-byte readback.

    Reconciliation must return the exact original request, manifest and receipt
    bytes. The call is one-shot even after an uncertain commit outcome; retry
    requires original-operation reconciliation outside this helper.
    """
    if (type(request_bytes) is not bytes or not request_bytes
            or type(manifest_bytes) is not bytes or not manifest_bytes
            or type(receipt_bytes) is not bytes or not receipt_bytes
            or context is None or type(interval) is not SourceHolderIntervalHandles):
        raise SourceHolderIntervalError("Exact protected ACK inputs and live interval required")
    if request_bytes != interval._request_bytes or receipt_bytes != interval._receipt_bytes:
        raise SourceHolderIntervalError("Protected ACK bytes differ from source-holder opening custody")
    if context is not interval._context:
        raise SourceHolderIntervalError("Protected ACK context differs from source-holder opening context")
    if interval._ack_attempted:
        raise SourceHolderIntervalError("Protected ACK already attempted in this source-holder interval")
    interval.renew_all()
    commit = getattr(ack_port, "commit", None)
    reconcile = getattr(ack_port, "reconcile", None)
    if not callable(commit) or not callable(reconcile):
        raise SourceHolderIntervalError("Protected exact-byte ACK commit/reconcile port required")
    object.__setattr__(interval, "_ack_attempted", True)
    try:
        commit(request_bytes, manifest_bytes, receipt_bytes, context)
    except BaseException as error:
        raise SourceHolderAckUncertain("Protected ACK commit outcome is uncertain") from error
    try:
        readback = reconcile(request_bytes, manifest_bytes, receipt_bytes, context)
    except BaseException as error:
        raise SourceHolderAckUncertain("Protected ACK exact reconciliation failed") from error
    if (type(readback) is not tuple or len(readback) != 3
            or any(type(item) is not bytes for item in readback)
            or readback != (request_bytes, manifest_bytes, receipt_bytes)):
        raise SourceHolderAckUncertain("Protected ACK readback does not match exact original bytes")
    object.__setattr__(interval, "_ack_reconciled", True)
