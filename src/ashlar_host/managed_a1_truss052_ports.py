# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fail-closed gate for the proposed managed A1 Truss .52 ports.

Truss .52 holds custody with a same-session advisory lock, not a TTL lease.
Its public source methods do not accept one aggregate ACK/reconciliation
deadline. Until Ashlar can enforce that interval and native PostgreSQL
transaction setup is qualified, this owner refuses before session open/ACK.
"""
from __future__ import annotations

from .truss_source_custody_selection import TrussSourceCustodySelection


PROFILE = "ashlar-managed-a1-truss052-ports/0.1"


class ManagedA1Truss052PortError(RuntimeError):
    """A1 same-session deadline or native transaction admission is absent."""


_REFUSAL = (
    "Managed A1 Truss source custody is unavailable: aggregate cancellable "
    "ACK/reconciliation deadline and native PostgreSQL qualification are required")


class _SourcePort:
    def __init__(self, owner):
        self._owner = owner

    def open(self, *, selection, request_bytes, manifest_bytes, receipt_bytes,
             vector, context):
        # Check capability ownership before inspecting inputs or constructing a
        # public Truss session, so a foreign caller causes no source-side work.
        if context is not self._owner.context:
            raise ManagedA1Truss052PortError("Original Truss owner context required")
        if (type(self._owner.selection) is not TrussSourceCustodySelection
                or selection is None or type(request_bytes) is not bytes
                or type(manifest_bytes) is not bytes or type(receipt_bytes) is not bytes
                or vector is None):
            raise ManagedA1Truss052PortError(
                "Exact selected A1 publication and source inputs required")
        raise ManagedA1Truss052PortError(_REFUSAL)


class _AckPort:
    def __init__(self, owner):
        self._owner = owner

    def commit(self, request_bytes, manifest_bytes, receipt_bytes, context, *,
               deadline_monotonic):
        if context is not self._owner.context:
            raise ManagedA1Truss052PortError("Original Truss owner context required")
        raise ManagedA1Truss052PortError(_REFUSAL)

    def reconcile(self, request_bytes, manifest_bytes, receipt_bytes, context, *,
                  deadline_monotonic):
        if context is not self._owner.context:
            raise ManagedA1Truss052PortError("Original Truss owner context required")
        raise ManagedA1Truss052PortError(_REFUSAL)


class ManagedA1Truss052PortOwner:
    """Fail-closed source/ACK pair pending deadline and PG qualification."""

    def __init__(self, selection, *, context):
        if type(selection) is not TrussSourceCustodySelection or context is None:
            raise ManagedA1Truss052PortError(
                "Issuer-bound Truss source selection and original owner context required")
        self.selection = selection
        self.context = context
        self.source_custody_port = _SourcePort(self)
        self.protected_ack_port = _AckPort(self)
