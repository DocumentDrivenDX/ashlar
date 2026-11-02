# SPDX-License-Identifier: MIT OR Apache-2.0
"""Same-session adapter for the public Truss 0.0.52 source-custody API.

This adapter owns one borrowed psycopg connection, one Truss host and one
reference assembly for the entire source interval. It commits each public
Truss operation in caller-owned transactions. Ashlar's destination registration
and ACK-scope bytes remain opaque, exact bytes; this module does not grant or
perform external destination ACK authority.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
from typing import Literal

from .truss_source_custody_selection import (
    TrussSourceCustodyOpenSelection, TrussSourceCustodySelection,
    TrussSourceSelectionError, _OPEN_SELECTIONS, _SELECTIONS, _issued,
)


class Truss052CustodyError(RuntimeError):
    """The selected .52 source-custody interval refused or became uncertain."""


class Truss052AckUncertain(Truss052CustodyError):
    """Source ACK commit was attempted; only exact reconciliation is allowed."""


def _sdk():
    try:
        from truss import (Ok, CatalogRuntimeConfiguration, create_reference_assembly,
                           adopt_psycopg_host, FeedSourceCustodyHandle,
                           FeedSourceObservation, FeedReceiptAcknowledged,
                           FeedAckReconciled, FeedSourceClosed)
        from truss.feed import FeedPage, FeedUnavailable
        from truss import CatalogRuntimeStatus
    except ImportError as error:
        raise Truss052CustodyError("Public Truss 0.0.52 custody API is unavailable") from error
    return (Ok, CatalogRuntimeConfiguration, create_reference_assembly,
            adopt_psycopg_host, FeedSourceCustodyHandle, FeedSourceObservation,
            FeedReceiptAcknowledged, FeedAckReconciled, FeedSourceClosed,
            FeedUnavailable, CatalogRuntimeStatus, FeedPage)


def _value(result):
    if not isinstance(result, _sdk()[0]):
        raise Truss052CustodyError("Public Truss source-custody operation refused")
    return result.value


@dataclass
class Truss052SourceCustodySession:
    """One immutable selection and one same-session Truss handle lifetime.

    ``connection_factory`` is called once. The returned connection is retained
    until ``dispose``; reconnecting cannot inherit this handle or its private
    PostgreSQL session marker. Callers must invoke ``close`` after ACK
    reconciliation, then ``dispose`` (or use this object as a context manager).
    """

    connection_factory: object = field(repr=False, compare=False)
    selection: TrussSourceCustodySelection = field(repr=False, compare=False)
    context: object = field(repr=False, compare=False)
    runtime_status_provider: object = field(default=None, repr=False, compare=False)
    statement_timeout_ms: int = 10000
    _connection: object = field(default=None, init=False, repr=False, compare=False)
    _host: object = field(default=None, init=False, repr=False, compare=False)
    _assembly: object = field(default=None, init=False, repr=False, compare=False)
    _sdk_selection: object = field(default=None, init=False, repr=False, compare=False)
    _selected: object = field(default=None, init=False, repr=False, compare=False)
    _prepublication: bool = field(default=False, init=False, repr=False, compare=False)
    _bound_selection: object = field(default=None, init=False, repr=False, compare=False)
    _handle: object = field(default=None, init=False, repr=False, compare=False)
    _runtime_status: object = field(default=None, init=False, repr=False, compare=False)
    _phase: str = field(default="new", init=False, repr=False)
    _ack_attempted: bool = field(default=False, init=False, repr=False)
    _ack_matched: bool = field(default=False, init=False, repr=False)
    _disposed: bool = field(default=False, init=False, repr=False)

    def __post_init__(self):
        valid_post = (type(self.selection) is TrussSourceCustodySelection
                      and _issued(_SELECTIONS, self.selection))
        valid_pre = (type(self.selection) is TrussSourceCustodyOpenSelection
                     and _issued(_OPEN_SELECTIONS, self.selection))
        if (not (valid_post or valid_pre)
                or type(self.context) is type(None)
                or not callable(self.connection_factory)
                or (self.runtime_status_provider is not None
                    and not callable(self.runtime_status_provider))
                or type(self.statement_timeout_ms) is not int
                or not 1 <= self.statement_timeout_ms <= 60000):
            raise Truss052CustodyError("Exact .52 selection, context and bounded connection factory required")
        object.__setattr__(self, "_selected", self.selection)
        object.__setattr__(self, "_prepublication", valid_pre)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.dispose()
        return False

    def _live(self):
        if self._disposed:
            raise Truss052CustodyError("Truss source-custody session is disposed")
        if self._connection is None:
            raise Truss052CustodyError("Source custody has not opened its original connection")
        if getattr(self._connection, "closed", False):
            self._phase = "connection_lost"
            raise Truss052CustodyError("Original Truss source-custody connection was lost")
        if getattr(self._connection, "broken", False):
            self._phase = "connection_lost"
            raise Truss052CustodyError("Original Truss source-custody connection is broken")

    def _selection(self):
        if self.selection is not self._selected:
            raise Truss052CustodyError("Source/destination/ACK byte selection changed during custody")
        return self._selected

    def bind_publication(self, selection: TrussSourceCustodySelection):
        """Add receipt authority after publication without replacing the held selection."""
        early = self._selection()
        if (not self._prepublication or self._phase not in ("open", "renewed")
                or type(selection) is not TrussSourceCustodySelection
                or not _issued(_SELECTIONS, selection)
                or self._bound_selection is not None):
            raise Truss052CustodyError(
                "One issuer-bound publication selection required after pre-publication open")
        same = (
            selection.page is early.page
            and selection.maximum_facts == early.maximum_facts
            and selection.maximum_bytes == early.maximum_bytes
            and selection.request_bytes == early.request_bytes
            and selection.destination_registration_bytes == early.destination_registration_bytes
            and selection.ack_scope_bytes == early.ack_scope_bytes
            and selection.destination_id == early.destination_id
            and selection.request_sha256 == early.request_sha256
            and selection.publication_id == early.publication_id
            and selection.schedule_sha256 == early.schedule_sha256
            and selection.source == early.source
            and dict(selection.identity) == dict(early.identity))
        if not same:
            raise Truss052CustodyError(
                "Post-publication ACK binding differs from retained pre-publication source")
        self._bound_selection = selection
        return True

    def _ack_selection(self):
        self._selection()
        if not self._prepublication:
            return self._selection()
        if (self._bound_selection is None
                or not _issued(_SELECTIONS, self._bound_selection)):
            raise Truss052CustodyError(
                "Exact post-publication managed/native observation must bind before ACK")
        return self._bound_selection

    def _ensure_host(self):
        if self._connection is None:
            connection = self.connection_factory(self.context)
            if (getattr(connection, "autocommit", True)
                    or getattr(getattr(connection, "info", None), "transaction_status", None) != 0):
                try:
                    connection.close()
                finally:
                    raise Truss052CustodyError("Fresh idle non-autocommit Truss connection required")
            Ok, Configuration, create, adopt, *_ = _sdk()
            self._connection = connection
            try:
                self._host = adopt(connection, statement_timeout_ms=self.statement_timeout_ms,
                                   operation_timeout_ms=self.statement_timeout_ms)
                self._assembly = _value(create(Configuration(), self._host))
            except BaseException:
                self._phase = "connection_lost"
                raise
        self._live()

    @contextmanager
    def _transaction(self, access_mode: Literal["read_only", "read_write"]):
        self._ensure_host()
        connection = self._connection
        try:
            mode = {"read_only": "READ ONLY", "read_write": "READ WRITE"}.get(access_mode)
            if mode is None:
                raise Truss052CustodyError("Exact Truss transaction access mode required")
            # Set access mode together with isolation in the first statement
            # of every transaction. A session's default_transaction_read_only
            # may otherwise make later host adoption too late to enable writes.
            connection.execute(
                f"SET TRANSACTION ISOLATION LEVEL READ COMMITTED, {mode}")
            # The timeout is an integer bounded in __post_init__; PostgreSQL's
            # SET grammar does not accept a bind parameter in this position.
            connection.execute(
                f"SET LOCAL statement_timeout = '{self.statement_timeout_ms}ms'")
            transaction = _value(self._host.adopt_transaction(access_mode=access_mode))
            yield transaction
        except BaseException:
            try:
                if not getattr(connection, "closed", False):
                    connection.rollback()
            except BaseException:
                self._phase = "connection_lost"
            raise

    def _commit(self, *, uncertain: bool = False):
        try:
            self._connection.commit()
        except BaseException as error:
            if uncertain:
                self._phase = "ack_unresolved"
                raise Truss052AckUncertain(
                    "Truss source ACK commit outcome is unknown; reconcile only") from error
            self._phase = "connection_lost" if getattr(self._connection, "closed", False) else "uncertain"
            raise Truss052CustodyError(
                "Truss source-custody transaction commit outcome is unknown") from error

    def open(self):
        """Observe the installed manifest and open/commit the exact source hold."""
        if self._phase != "new":
            raise Truss052CustodyError("Source custody can be opened only once")
        selected = self._selection()
        status = None
        if self.runtime_status_provider is not None:
            # Query through the caller's registrar connection immediately
            # before the retained feed-reader session starts its open call.
            status = self.runtime_status_provider()
            CatalogRuntimeStatus = _sdk()[10]
            if (type(status) is not CatalogRuntimeStatus or status.installed is not True
                    or status.installation_id != selected.identity.get("installation_id")):
                self._phase = "refused"
                raise Truss052CustodyError(
                    "Fresh public Truss runtime status differs from selected installation")
        self._ensure_host()
        try:
            with self._transaction("read_write") as transaction:
                if self.runtime_status_provider is None:
                    status = _value(self._assembly.status_in_transaction(transaction))
                    CatalogRuntimeStatus = _sdk()[10]
                    if (type(status) is not CatalogRuntimeStatus or status.installed is not True
                            or status.installation_id != selected.identity.get("installation_id")):
                        self._phase = "refused"
                        raise Truss052CustodyError(
                            "Public Truss runtime status differs from selected installation")
                self._runtime_status = status
                self._sdk_selection = selected.to_candidate_sdk(runtime_status=status)
                handle = _value(self._assembly.open_feed_source_custody_in_transaction(
                    transaction, self._sdk_selection))
                if type(handle) is not _sdk()[4]:
                    raise Truss052CustodyError("Exact .52 source custody handle required")
                self._handle = handle
            self._commit()
        except TrussSourceSelectionError:
            self._phase = "refused"
            raise
        except BaseException:
            if self._phase == "new":
                self._phase = "refused"
            raise
        self._phase = "open"
        return handle

    def renew(self, *, phase: Literal["before_ack", "ack_unresolved"] = "before_ack"):
        """Renew on the original connection; renewal is a writable transaction."""
        self._require_handle()
        self._selection()
        if phase == "before_ack" and self._ack_attempted:
            raise Truss052CustodyError("After ACK attempt only unresolved renewal/reconciliation is allowed")
        if phase == "ack_unresolved" and not self._ack_attempted:
            raise Truss052CustodyError("Unresolved phase requires an attempted source ACK")
        with self._transaction("read_write") as transaction:
            value = _value(self._assembly.renew_feed_source_custody_in_transaction(
                transaction, self._handle, phase=phase))
            self._validate_renewal_observation(value, phase)
        self._commit()
        self._live()
        return value

    def _validate_renewal_observation(self, value, phase):
        """Bind successful server lock/row validation to the original page."""
        selected = self._selection()
        page = selected.page
        identity = selected.identity
        if type(value) is not _sdk()[5] or value.phase != phase:
            raise Truss052CustodyError("Exact source custody renewal observation required")
        expected_checkpoint = None
        expected_receipt = None
        if phase == "before_ack":
            expected_checkpoint = page.prior.content
            expected_receipt = "missing"
        elif phase == "ack_unresolved":
            if value.checkpoint.content == page.prior.content:
                expected_checkpoint = page.prior.content
                expected_receipt = "missing"
            elif value.checkpoint.content == page.next.content:
                expected_checkpoint = page.next.content
                expected_receipt = "matched"
        status = self._runtime_status
        if (status is None or expected_checkpoint is None
                or value.checkpoint.content != expected_checkpoint
                or value.receipt_outcome != expected_receipt
                or value.consumer_id != page.consumer_id
                or value.registration_id != page.registration_id
                or value.installation_id != identity.get("installation_id")
                or value.source_epoch != identity.get("epoch")
                or value.release_id != status.release_id
                or value.release_sha256 != status.release_sha256
                or value.prior.content != page.prior.content
                or value.next.content != page.next.content
                or value.feed_sha256 != page.sha256):
            raise Truss052CustodyError(
                "Server-verified Truss source custody differs from original selection")

    def observe_selected_page(self):
        """Reread the selected page on the held source session before effects.

        This is one-page source observation only. It does not select catalog
        registrations, establish genesis for another feed, or authenticate a
        caller's original selection. A/B genesis admission remains external.
        """
        self._require_handle()
        selected = self._selection()
        expected = selected.page
        try:
            with self._transaction("read_only") as transaction:
                page = _value(self._assembly.read_feed_in_transaction(
                    transaction, consumer_id=expected.consumer_id,
                    maximum_facts=selected.maximum_facts,
                    maximum_bytes=selected.maximum_bytes))
                if type(page) is not _sdk()[11]:
                    raise Truss052CustodyError("Exact public Truss FeedPage observation required")
                fields = ("content", "sha256", "prior", "next", "consumer_id",
                          "registration_id", "writer_xid", "outcome")
                if any(getattr(page, field, None) != getattr(expected, field, None)
                       for field in fields):
                    raise Truss052CustodyError(
                        "Current Truss page differs from the selected original page")
        finally:
            # Ensure the read transaction ends on this same held connection,
            # including when source comparison refuses. _transaction already
            # rolls back exceptions; the repeated rollback is idempotent.
            try:
                if not getattr(self._connection, "closed", False):
                    self._connection.rollback()
            except BaseException as error:
                self._phase = "connection_lost"
                raise Truss052CustodyError(
                    "Original Truss page reread transaction cleanup failed") from error
        self._phase = "renewed"
        return page

    def acknowledge(self):
        """Attempt the one exact-byte Truss source receipt ACK on the held page."""
        self._require_handle()
        selected = self._ack_selection()
        if self._ack_attempted or self._phase not in ("open", "renewed"):
            raise Truss052CustodyError("Source ACK is one-shot and requires a live pre-ACK hold")
        self.renew(phase="before_ack")
        # Exact destination receipt bytes are fixed by the publication binder;
        # no caller-provided replacement is accepted here.
        self._ack_attempted = True
        try:
            with self._transaction("read_write") as transaction:
                value = _value(self._assembly.acknowledge_feed_under_source_custody_in_transaction(
                    transaction, self._handle, self._sdk_selection,
                    receipt_bytes=selected.receipt_bytes))
                if (type(value) is not _sdk()[6]
                        or value.checkpoint.content != selected.page.next.content
                        or value.outcome not in ("advanced", "equal")
                        or value.durability != "pending"):
                    raise Truss052CustodyError("Truss ACK result differs from exact retained receipt")
                self._check_receipt(value.receipt, selected)
            self._commit(uncertain=True)
        except Truss052AckUncertain:
            raise
        except BaseException:
            self._phase = "ack_unresolved"
            raise
        self._phase = "ack_committed"
        return value

    def reconcile(self):
        """Read exact ACK status in a new read-only transaction on same session."""
        self._require_handle()
        selected = self._ack_selection()
        if not self._ack_attempted or self._phase not in ("ack_unresolved", "ack_committed"):
            raise Truss052CustodyError("Only an attempted source ACK may be reconciled")
        self._live()
        with self._transaction("read_only") as transaction:
            value = _value(self._assembly.reconcile_feed_ack_under_source_custody_in_transaction(
                transaction, self._handle, self._sdk_selection,
                receipt_bytes=selected.receipt_bytes))
            if type(value) is not _sdk()[7]:
                raise Truss052CustodyError("Exact public Truss ACK reconciliation required")
            if value.outcome == "matched":
                receipt = value.receipt
                if value.checkpoint.content != selected.page.next.content:
                    raise Truss052CustodyError("Reconciled Truss checkpoint differs from selected page")
                self._check_receipt(receipt, selected)
            elif value.outcome not in ("missing", "conflicting"):
                raise Truss052CustodyError("Unknown Truss ACK reconciliation outcome")
        # Reconciliation is intentionally read-only and is rolled back to leave
        # the caller connection idle without assigning any new source state.
        self._connection.rollback()
        self._ack_matched = value.outcome == "matched"
        self._phase = "ack_reconciled" if self._ack_matched else "ack_unresolved"
        return value

    @staticmethod
    def _check_receipt(receipt, selection):
        page = selection.page
        identity = selection.identity
        if (receipt is None
                or receipt.installation_id != identity["installation_id"]
                or receipt.source_epoch != identity["epoch"]
                or receipt.consumer_id != page.consumer_id
                or receipt.registration_id != page.registration_id
                or receipt.prior != page.prior or receipt.next != page.next
                or receipt.feed_sha256 != page.sha256
                or receipt.destination_id != selection.destination_id
                or receipt.request_sha256 != selection.request_sha256
                or receipt.publication_id != selection.publication_id
                or receipt.receipt_bytes != selection.receipt_bytes
                or receipt.receipt_sha256 != hashlib.sha256(selection.receipt_bytes).hexdigest()):
            raise Truss052CustodyError("Truss receipt differs from exact source/destination/ACK binding")

    def close(self):
        """Close the source fence on the original connection after exact outcome."""
        self._require_handle()
        selected = self._selection()
        if self._ack_attempted and self._phase != "ack_reconciled":
            raise Truss052CustodyError("Source ACK must be reconciled before custody close")
        if self._phase not in ("open", "renewed", "ack_reconciled"):
            raise Truss052CustodyError("Source custody is not closable in this phase")
        committed = self._ack_matched
        with self._transaction("read_write") as transaction:
            value = _value(self._assembly.close_feed_source_custody_in_transaction(
                transaction, self._handle, self._sdk_selection, committed=committed))
            if type(value) is not _sdk()[8] or value.durability != "pending":
                raise Truss052CustodyError("Exact Truss source custody close required")
        self._commit()
        self._phase = "closed"
        return value

    def _require_handle(self):
        self._live()
        if self._handle is None or self._phase not in (
                "open", "renewed", "ack_unresolved", "ack_committed", "ack_reconciled"):
            raise Truss052CustodyError("Original committed Truss source handle required")

    def dispose(self):
        """Detach and close the original connection; never reconnect or ACK."""
        if self._disposed:
            return
        self._disposed = True
        failures = []
        if self._assembly is not None:
            try:
                self._assembly.dispose()
            except BaseException as error:
                failures.append(error)
        if self._host is not None:
            try:
                if not self._host.detach():
                    raise Truss052CustodyError("Truss host detach was not confirmed")
            except BaseException as error:
                failures.append(error)
        if self._connection is not None:
            try:
                if not getattr(self._connection, "closed", False):
                    self._connection.rollback()
                    self._connection.close()
            except BaseException as error:
                failures.append(error)
        if failures:
            raise Truss052CustodyError("Original Truss custody connection cleanup failed") from failures[0]
