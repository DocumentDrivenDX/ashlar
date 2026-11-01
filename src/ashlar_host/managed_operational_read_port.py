# SPDX-License-Identifier: MIT OR Apache-2.0
"""Source-holder adapter for the bounded managed-read observation profile.

The handle coordinates substantive reads at open, pre-ACK renewal and close.
Its deadline is only the local 15-minute observation window; it is not a Delta
file lease, pin lease, source authority, or protected-ACK capability. The
injected session, source and policy owners remain mandatory authority boundaries.
"""
from __future__ import annotations

import time

from ashlar.pins import PinVector
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
