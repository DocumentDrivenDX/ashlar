# SPDX-License-Identifier: MIT OR Apache-2.0
"""Proposal-only source-holder handle lifetime choreography.

This private helper mirrors the draft ports in CONTRACT-001 and TD-009. It
does not implement a native adapter, establish authority or lease coverage,
produce an ACK-ready result, or expose commit/reconcile operations. No campaign
or Truss entrypoint imports it.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
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


class ManagedPublicationReadPort(Protocol):
    def open(self, receipt_bytes: bytes, vector: object, context: object) -> RenewableHandle: ...


class NativePinPort(Protocol):
    def open(self, vector: object, source_selection: object, context: object) -> RenewableHandle: ...


class TrussSourceCustodyPort(Protocol):
    def open(self, source_selection: object, request_bytes: bytes, context: object) -> RenewableHandle: ...


@dataclass(frozen=True)
class SourceHolderIntervalHandles:
    """The three opened handles; their presence proves no authority or duration."""

    managed_publication: RenewableHandle
    native_pin: RenewableHandle
    truss_source: RenewableHandle
    _active: bool = field(default=True, init=False, repr=False, compare=False)

    def renew_all(self) -> None:
        """Recheck in the fixed order; callers must not infer a lease duration."""
        if not self._active:
            raise SourceHolderIntervalError("Draft source-holder interval is closed")
        for role, handle in self.ordered():
            try:
                handle.renew()
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
):
    """Open, renew-all, yield and reverse-close draft handles without ACK.

    The API is deliberately disconnected from campaign code. An open/renew
    success says only that injected ports returned without error at that
    observation; it does not prove retention, authority, or continuous lease
    coverage while the caller body runs.
    """
    if (type(receipt_bytes) is not bytes or not receipt_bytes
            or type(request_bytes) is not bytes or not request_bytes
            or vector is None or source_selection is None or context is None):
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
            opened["managed_publication"], opened["native_pin"], opened["truss_source"]
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
        raise SourceHolderCloseError(failures) from first_close_error
