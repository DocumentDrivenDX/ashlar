# SPDX-License-Identifier: MIT OR Apache-2.0
"""Focused fake-port coverage for the .52 same-connection adapter."""
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from ashlar_host.truss_source_custody_selection import (
    TrussSourceCustodyAnchor, TrussSourceCustodyOpenSelection,
    TrussSourceCustodySelection, _ANCHORS, _SELECTIONS, _issue)
from ashlar_host.truss052_source_custody import (
    Truss052AckUncertain, Truss052CustodyError, Truss052SourceCustodySession,
)


class _Ok:
    def __init__(self, value): self.value = value


class _Handle: pass
class _RuntimeStatus:
    def __init__(self, installation_id="install", installed=True):
        self.installation_id = installation_id
        self.installed = installed
        self.release_id = "release"
        self.release_sha256 = "d" * 64

class _Observation:
    def __init__(self, phase, selected, status):
        page = selected.page
        self.phase = phase
        self.checkpoint = SimpleNamespace(content=(page.prior.content
            if phase == "before_ack" else page.next.content))
        self.receipt_outcome = "missing" if phase == "before_ack" else "matched"
        self.consumer_id = page.consumer_id
        self.registration_id = page.registration_id
        self.installation_id = selected.identity["installation_id"]
        self.source_epoch = selected.identity["epoch"]
        self.release_id = status.release_id
        self.release_sha256 = status.release_sha256
        self.prior = page.prior
        self.next = page.next
        self.feed_sha256 = page.sha256
class _Closed:
    durability = "pending"
class _Unavailable: pass


class _Connection:
    def __init__(self):
        self.autocommit = False
        self.closed = False
        self.broken = False
        self.info = SimpleNamespace(transaction_status=0)
        self.calls = []
        self.fail_commit_after = None
        self.last_op = None

    def execute(self, statement, *args):
        self.calls.append(statement)
        self.info.transaction_status = 2

    def commit(self):
        if self.fail_commit_after == self.last_op:
            self.fail_commit_after = None
            raise OSError("commit acknowledgment lost")
        self.info.transaction_status = 0

    def rollback(self): self.info.transaction_status = 0
    def close(self): self.closed = True


class _Host:
    def __init__(self, connection): self.connection = connection; self.modes = []
    def adopt_transaction(self, *, access_mode):
        self.modes.append(access_mode)
        return _Ok(object())
    def detach(self): return True


class _Ack:
    def __init__(self, receipt, checkpoint):
        self.receipt = receipt
        self.checkpoint = checkpoint
        self.outcome = "advanced"
        self.durability = "pending"


class _Reconciled:
    def __init__(self, receipt, checkpoint, outcome="matched"):
        self.receipt = receipt if outcome == "matched" else None
        self.checkpoint = checkpoint
        self.outcome = outcome


class _Receipt:
    def __init__(self, selected):
        import hashlib
        page = selected.page
        self.installation_id = selected.identity["installation_id"]
        self.source_epoch = selected.identity["epoch"]
        self.consumer_id = page.consumer_id
        self.registration_id = page.registration_id
        self.prior = page.prior
        self.next = page.next
        self.feed_sha256 = page.sha256
        self.receipt_bytes = selected.receipt_bytes
        self.receipt_sha256 = hashlib.sha256(selected.receipt_bytes).hexdigest()
        self.request_sha256 = selected.request_sha256
        self.publication_id = selected.publication_id
        self.destination_id = selected.destination_id


class _Assembly:
    def __init__(self, connection, host, selected, *, reconcile_outcome="matched"):
        self.connection, self.host = connection, host
        self.selected = selected
        self.reconcile_outcome = reconcile_outcome
        self.disposed = False
    def status_in_transaction(self, transaction):
        raise AssertionError("catalog status must use the separately authorized provider")
    def open_feed_source_custody_in_transaction(self, transaction, selection):
        self.connection.last_op = "open"
        self.open_selection = selection
        return _Ok(_Handle())
    def read_feed_in_transaction(self, transaction, *, consumer_id, maximum_facts, maximum_bytes):
        self.connection.last_op = "read"
        self.read_arguments = (consumer_id, maximum_facts, maximum_bytes)
        return _Ok(self.selected.page)
    def renew_feed_source_custody_in_transaction(self, transaction, handle, *, phase):
        self.connection.last_op = "renew"
        value = _Observation(phase, self.selected, self.runtime_status)
        if getattr(self, "observation_mutation", None) is not None:
            self.observation_mutation(value)
        return _Ok(value)
    def acknowledge_feed_under_source_custody_in_transaction(
            self, transaction, handle, selection, *, receipt_bytes):
        self.connection.last_op = "ack"
        if receipt_bytes != self.selected.receipt_bytes:
            raise AssertionError("adapter changed the exact receipt bytes")
        return _Ok(_Ack(_Receipt(self.selected), self.selected.page.next))
    def reconcile_feed_ack_under_source_custody_in_transaction(
            self, transaction, handle, selection, *, receipt_bytes):
        self.connection.last_op = "reconcile"
        return _Ok(_Reconciled(_Receipt(self.selected), self.selected.page.next,
                               self.reconcile_outcome))
    def close_feed_source_custody_in_transaction(self, transaction, handle, selection, *, committed):
        self.connection.last_op = "close"
        self.closed_committed = committed
        return _Ok(_Closed())
    def dispose(self): self.disposed = True


class _Page(SimpleNamespace):
    pass


def _selection():
    # Direct construction is acceptable at this adapter seam only in tests;
    # production materialization is additionally guarded by the issuer registry.
    page = _Page(consumer_id="consumer", registration_id="registration-id",
        prior=SimpleNamespace(content=b"prior-cursor"),
        next=SimpleNamespace(content=b"next-cursor"), sha256="c" * 64)
    selection = TrussSourceCustodySelection(
        page=page, maximum_facts=10, maximum_bytes=1000,
        request_bytes=b"request", destination_registration_bytes=b"registration",
        ack_scope_bytes=b"scope", destination_id="destination",
        request_sha256="a" * 64, publication_id="publication",
        manifest_bytes=b"manifest", vector=object(), receipt_bytes=b"exact-receipt",
        schedule_sha256="b" * 64, source="A",
        identity={"installation_id": "install", "epoch": "epoch"})
    _issue(_SELECTIONS, selection)
    return selection


def _open_selection(selection):
    anchor = _issue(_ANCHORS, TrussSourceCustodyAnchor(
        schedule_sha256=selection.schedule_sha256, source=selection.source,
        identity=selection.identity, page=selection.page,
        destination_id=selection.destination_id,
        request_bytes=selection.request_bytes,
        request_sha256=selection.request_sha256,
        destination_registration_bytes=selection.destination_registration_bytes,
        ack_scope_bytes=selection.ack_scope_bytes,
        maximum_facts=selection.maximum_facts,
        maximum_bytes=selection.maximum_bytes,
        preselected_publication_id=selection.publication_id))
    return anchor.open_selection()


class Truss052SourceCustodyTests(unittest.TestCase):
    def setUp(self):
        self.selection = _selection()
        self.connection = _Connection()
        self.host = _Host(self.connection)
        self.assembly = _Assembly(self.connection, self.host, self.selection)
        self.calls = []
        self.order = []
        self.runtime_status = _RuntimeStatus()
        self.assembly.runtime_status = self.runtime_status
        def factory(context):
            self.calls.append(context)
            self.order.append("reader_connection")
            return self.connection
        def adopt(connection, **kwargs):
            self.assertIs(connection, self.connection)
            self.assertEqual(kwargs["statement_timeout_ms"], 10000)
            return self.host
        def create(configuration, host):
            self.assertIs(host, self.host)
            return _Ok(self.assembly)
        self.sdk = (_Ok, object, create, adopt, _Handle, _Observation,
                    _Ack, _Reconciled, _Closed, _Unavailable, _RuntimeStatus, _Page)
        def materialize(selection, *, runtime_status):
            self.materialized_status = runtime_status
            return SimpleNamespace(selection=selection,
                destination_registration_bytes=selection.destination_registration_bytes,
                ack_scope_bytes=selection.ack_scope_bytes)
        self.patches = [
            patch("ashlar_host.truss052_source_custody._sdk", return_value=self.sdk),
            patch.object(TrussSourceCustodySelection, "to_candidate_sdk",
                autospec=True, side_effect=materialize),
            patch.object(TrussSourceCustodyOpenSelection, "to_candidate_sdk",
                autospec=True, side_effect=materialize),
        ]
        for p in self.patches: p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(self.patches)])
        self.context = object()
        def status_provider():
            self.order.append("registrar_status")
            return self.runtime_status
        self.session = Truss052SourceCustodySession(
            connection_factory=factory, runtime_status_provider=status_provider,
            selection=self.selection, context=self.context)

    def test_lifecycle_uses_one_connection_and_exact_registration_scope(self):
        self.session.open()
        self.assertEqual(self.connection.calls[0],
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ WRITE")
        self.assertEqual(self.connection.calls[1],
            "SET LOCAL statement_timeout = '10000ms'")
        self.assertEqual(self.order[:2], ["registrar_status", "reader_connection"])
        self.assertIs(self.materialized_status, self.runtime_status)
        self.assertEqual(self.assembly.open_selection.destination_registration_bytes, b"registration")
        self.assertEqual(self.assembly.open_selection.ack_scope_bytes, b"scope")
        page = self.session.observe_selected_page()
        self.assertEqual(self.connection.calls[2],
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY")
        self.assertIs(page, self.selection.page)
        self.assertEqual(self.assembly.read_arguments, ("consumer", 10, 1000))
        self.assertEqual(self.connection.info.transaction_status, 0)
        self.assertIs(self.session._connection, self.connection)
        self.session.renew()
        self.session.acknowledge()
        reconciled = self.session.reconcile()
        self.assertEqual(reconciled.outcome, "matched")
        self.session.close()
        self.assertEqual(self.calls, [self.context])
        self.assertEqual(self.host.modes,
            ["read_write", "read_only", "read_write", "read_write", "read_write",
             "read_only", "read_write"])
        self.assertTrue(self.assembly.closed_committed)

    def test_live_page_must_match_the_selected_original_page(self):
        from dataclasses import replace
        self.session.open()
        original = self.selection.page
        self.assembly.selected = replace(self.selection, page=_Page(consumer_id=original.consumer_id,
            registration_id=original.registration_id, prior=original.prior,
            next=original.next, sha256="d" * 64))
        with self.assertRaisesRegex(Truss052CustodyError, "differs from the selected original page"):
            self.session.observe_selected_page()

    def test_lost_connection_is_not_replaced(self):
        self.session.open()
        self.connection.closed = True
        with self.assertRaisesRegex(Truss052CustodyError, "connection was lost"):
            self.session.renew()
        self.assertEqual(self.calls, [self.context])

    def test_mismatched_public_runtime_status_refuses_before_reader_connection(self):
        session = Truss052SourceCustodySession(
            connection_factory=lambda _context: self.fail("reader connection must not open"),
            runtime_status_provider=lambda: _RuntimeStatus(installation_id="other-install"),
            selection=self.selection, context=self.context)
        with self.assertRaisesRegex(Truss052CustodyError, "differs from selected installation"):
            session.open()

    def test_unknown_ack_is_one_shot_and_allows_only_reconciliation(self):
        self.session.open()
        self.connection.fail_commit_after = "ack"
        with self.assertRaises(Truss052AckUncertain):
            self.session.acknowledge()
        with self.assertRaisesRegex(Truss052CustodyError, "one-shot"):
            self.session.acknowledge()
        self.assertEqual(self.session.reconcile().outcome, "matched")
        self.session.close()

    def test_prepublication_open_binds_publication_later_without_replacing_handle(self):
        from dataclasses import replace
        early = _open_selection(self.selection)
        session = Truss052SourceCustodySession(
            connection_factory=lambda _context: self.connection,
            runtime_status_provider=lambda: self.runtime_status,
            selection=early, context=self.context)
        session.open()
        held_handle = session._handle
        with self.assertRaisesRegex(Truss052CustodyError, "must bind before ACK"):
            session.acknowledge()
        bad = replace(self.selection, publication_id="other-publication")
        from ashlar_host.truss_source_custody_selection import _issue
        _issue(_SELECTIONS, bad)
        with self.assertRaisesRegex(Truss052CustodyError, "differs from retained"):
            session.bind_publication(bad)
        session.bind_publication(self.selection)
        session.acknowledge()
        self.assertIs(session._handle, held_handle)
        self.assertIs(session._sdk_selection.selection, early)
        self.assertEqual(session.reconcile().outcome, "matched")
        session.close()

    def test_prepublication_hold_closes_uncommitted_when_publication_fails(self):
        early = _open_selection(self.selection)
        session = Truss052SourceCustodySession(
            connection_factory=lambda _context: self.connection,
            runtime_status_provider=lambda: self.runtime_status,
            selection=early, context=self.context)
        session.open()
        session.close()
        self.assertFalse(self.assembly.closed_committed)

    def test_selection_scope_cannot_be_replaced_after_open(self):
        from dataclasses import replace
        self.session.open()
        self.session.selection = replace(self.selection, ack_scope_bytes=b"foreign-scope")
        with self.assertRaisesRegex(Truss052CustodyError, "selection changed"):
            self.session.renew()

    def test_renewal_observation_is_bound_to_exact_source_page_and_runtime(self):
        self.session.open()
        self.assembly.observation_mutation = lambda value: setattr(
            value, "feed_sha256", "e" * 64)
        with self.assertRaisesRegex(Truss052CustodyError,
                                   "differs from original selection"):
            self.session.renew()


if __name__ == "__main__":
    unittest.main()
