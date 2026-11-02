# SPDX-License-Identifier: MIT OR Apache-2.0
"""Source-holder adapter for the bounded managed-read observation profile.

The handle coordinates substantive reads at open, pre-ACK renewal and close.
Its deadline is only the local 15-minute observation window; it is not a Delta
file lease, pin lease, source authority, or protected-ACK capability. The
injected session, source and policy owners remain mandatory authority boundaries.
"""
from __future__ import annotations

import time

from ashlar.pins import PinVector, PostgresPins
from .managed_operational_retention_interval import (
    ManagedOperationalRetentionSelection,
    OperationalRetentionInterval,
    OperationalRetentionIntervalError,
)


class ManagedOperationalReadPortError(ValueError):
    """The selected receipt, vector, context or interval state did not match."""


class ManagedOperationalReadHandle:
    """One local observation interval; never represents continuous file custody."""

    def __init__(self, interval, *, clock):
        self._interval = interval
        self._clock = clock
        self._closed = False

    def _require_open(self):
        if self._closed:
            raise ManagedOperationalReadPortError("Managed observation handle is closed")

    def renew(self):
        """Perform the single substantive four-role pre-ACK observation."""
        self._require_open()
        try:
            return self._interval.renew_pre_ack()
        except OperationalRetentionIntervalError as error:
            raise ManagedOperationalReadPortError(
                "Managed pre-ACK observation refused") from error

    def remaining_lifetime_seconds(self):
        """Return time to the profile deadline, not a retention or read lease."""
        self._require_open()
        return max(0.0, self._interval.deadline - self._clock())

    def close(self):
        self._require_open()
        self._closed = True
        try:
            return self._interval.close()
        except OperationalRetentionIntervalError as error:
            raise ManagedOperationalReadPortError(
                "Managed closing observation incomplete") from error


class ManagedOperationalReadPort:
    """Open a read-only interval bound to exact selected receipt/vector bytes.

    Construct this only after selecting the original request, manifest,
    destination receipt, complete PinVector and caller context. The injected
    owners must independently authenticate and renew their observations; this
    adapter does not turn protocol fakes or saved report values into authority.
    """

    def __init__(self, selection, session_factory, source_observer,
                 policy_observer, *, context, clock=time.monotonic):
        if (type(selection) is not ManagedOperationalRetentionSelection
                or context is None or not callable(clock)):
            raise ManagedOperationalReadPortError(
                "Selected operational profile and caller context required")
        # These are capability boundaries, not defaults the adapter may infer.
        if (not callable(getattr(session_factory, "open", None))
                or not callable(getattr(source_observer, "observe", None))
                or not callable(getattr(policy_observer, "observe", None))):
            raise ManagedOperationalReadPortError(
                "Owned session, source and policy observers required")
        self.selection = selection
        self.session_factory = session_factory
        self.source_observer = source_observer
        self.policy_observer = policy_observer
        self.context = context
        self.clock = clock
        self._opened = False

    def open(self, receipt_bytes: bytes, vector: PinVector, context: object):
        """Read and admit the exact original vector before returning a handle."""
        if (self._opened or type(receipt_bytes) is not bytes
                or receipt_bytes != self.selection.receipt_bytes
                or type(vector) is not PinVector or vector != self.selection.vector
                or context is not self.context):
            raise ManagedOperationalReadPortError(
                "Exact selected receipt bytes, PinVector and caller context required")
        self._opened = True
        try:
            interval = OperationalRetentionInterval.open(
                self.selection, self.session_factory, self.source_observer,
                self.policy_observer, clock=self.clock,
            )
        except OperationalRetentionIntervalError as error:
            raise ManagedOperationalReadPortError(
                "Managed opening observation refused") from error
        return ManagedOperationalReadHandle(interval, clock=self.clock)


class ManagedOperationalReadPinHandle:
    """Coordinate read observations with live native pin custody.

    The PostgreSQL transaction protects pin-row coordination only. It does not
    prevent Delta maintenance from deleting files, and this handle has no ACK
    capability.
    """

    def __init__(self, read_handle, pin_handle):
        self._read_handle = read_handle
        self._pin_handle = pin_handle
        self._pin_active = True
        self._closed = False

    def _require_open(self):
        if self._closed:
            raise ManagedOperationalReadPortError("Managed observation handle is closed")

    def renew(self):
        """Renew the original pin scope, then reopen all four native roles."""
        self._require_open()
        try:
            self._pin_handle.renew()
        except BaseException as error:
            # PostgresPins poisons and exits the transaction on renewal failure.
            self._pin_active = False
            if not isinstance(error, Exception):
                raise
            raise ManagedOperationalReadPortError(
                "Native pin renewal refused") from error
        return self._read_handle.renew()

    def remaining_lifetime_seconds(self):
        self._require_open()
        return self._read_handle.remaining_lifetime_seconds()

    def native_pin_remaining_lifetime_seconds(self):
        """Return native pin-owner coverage, never the managed read deadline.

        ``PostgresPins.open_live`` currently returns a transaction handle with
        no duration admission. In that case the composed bridge must fail
        closed before ACK; the read interval deadline is not a substitute.
        """
        self._require_open()
        remaining = getattr(
            self._pin_handle,
            "native_pin_transaction_remaining_lifetime_seconds", None)
        if not callable(remaining):
            raise ManagedOperationalReadPortError(
                "Native pin owner does not expose bounded live transaction coverage")
        return remaining()

    def close(self):
        """Attempt final managed read and pin checks, preserving first failure."""
        self._require_open()
        self._closed = True
        result = None
        primary = None
        try:
            result = self._read_handle.close()
        except BaseException as error:
            primary = error
        if self._pin_active:
            try:
                self._pin_handle.close()
            except BaseException as error:
                if primary is None:
                    primary = ManagedOperationalReadPortError(
                        "Native pin closing check incomplete")
                    primary.__cause__ = error
                else:
                    add_note = getattr(primary, "add_note", None)
                    if callable(add_note):
                        add_note("Native pin closing check also failed: "
                                 + type(error).__name__)
            finally:
                self._pin_active = False
        if primary is not None:
            raise primary
        return result


class ManagedOperationalReadPinPort:
    """Compose the read-only 15-minute interval with exact live pin custody.

    The caller must supply the real ``PostgresPins`` owner and its independently
    admitted source-bound policy. This does not provide production source or
    UC session authority by itself.
    """

    def __init__(self, read_port, pins, *, context):
        if (type(read_port) is not ManagedOperationalReadPort
                or type(pins) is not PostgresPins or context is not read_port.context
                or not callable(getattr(pins.policy, "authorize_read", None))
                or not callable(getattr(pins.policy, "authorize_source_bound_read", None))):
            raise ManagedOperationalReadPortError(
                "Exact read owner, native pin owner, source-bound policy and caller context required")
        self.read_port = read_port
        self.pins = pins
        self.context = context
        self._opened = False

    def open(self, receipt_bytes: bytes, vector: PinVector, context: object):
        if (self._opened or type(receipt_bytes) is not bytes
                or receipt_bytes != self.read_port.selection.receipt_bytes
                or type(vector) is not PinVector
                or vector != self.read_port.selection.vector or context is not self.context):
            raise ManagedOperationalReadPortError(
                "Exact selected receipt bytes, PinVector and caller context required")
        self._opened = True
        try:
            pin_handle = self.pins.open_live(
                vector, self.read_port.selection, context=context)
        except Exception as error:
            raise ManagedOperationalReadPortError(
                "Native source-bound pin opening refused") from error
        try:
            read_handle = self.read_port.open(receipt_bytes, vector, context)
        except BaseException as error:
            try:
                pin_handle.close()
            except BaseException as cleanup:
                add_note = getattr(error, "add_note", None)
                if callable(add_note):
                    add_note("Native pin open cleanup also failed: "
                             + type(cleanup).__name__)
            raise
        return ManagedOperationalReadPinHandle(read_handle, pin_handle)
