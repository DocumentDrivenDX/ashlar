"""Public protected ACK invocation with inert source/Delta ports; no native claim."""
import unittest
from contextlib import closing
import json
import sqlite3
import tempfile
from pathlib import Path
from types import SimpleNamespace
from ashlar_host.driver import NativeDriver, encoded
from unittest.mock import patch
from ashlar_host.ack import AckScope, AckError, AckOutcomeUncertain, receipt_bytes
from ashlar_host.ack_sessions import OutboxAckRegistration, RegisteredOutboxAcks
from ashlar_host.source_sessions import OutboxSourceRegistration, RegisteredOutboxSources
from test_protected_outbox_ack import Harness, FakeSession


class Tests(unittest.TestCase):
    def owner(self, h):
        h.scope = AckScope(h.scope.service_schema, h.scope.scope_id, h.scope.installation_id,
                           h.scope.consumer, h.scope.feed, h.scope.epoch)
        source = OutboxSourceRegistration(h.scope, 'ashlar_ack_source_test', 'a' * 64, h.factory)
        return RegisteredOutboxAcks((OutboxAckRegistration(source, h.factory, h.port.policy),))

    def call(self, owner, h, method='acknowledge'):
        with patch('ashlar_host.ack.Session', FakeSession):
            return getattr(owner, method)(h.request_bytes, h.manifest_bytes, h.vector,
                context=h.context, pins=h.port.pins, publication_backend=h.backend,
                supported_profiles=['ashlar-delta/0.3'], supported_revisions={'source': ['r1']})

    def test_actual_public_ack_reads_original_under_pins_and_closes(self):
        h = Harness(); owner = self.owner(h)
        self.assertEqual(self.call(owner, h), receipt_bytes(h.scope, h.request_bytes, h.manifest_bytes)[3])
        self.assertEqual(h.connections[0].writes, 1)
        self.assertTrue(h.connections[0].closed); self.assertFalse(h.held)
        self.assertEqual(h.policy_calls, 2)

    def test_uncertain_commit_requires_fresh_original_reconcile_no_second_write(self):
        h = Harness(); owner = self.owner(h); h.unknown_commit = True
        with self.assertRaises(AckOutcomeUncertain): self.call(owner, h)
        h.unknown_commit = False
        self.assertEqual(self.call(owner, h, 'reconcile'), receipt_bytes(h.scope, h.request_bytes, h.manifest_bytes)[3])
        self.assertEqual([c.writes for c in h.connections], [1, 0])
        self.assertTrue(all(c.closed for c in h.connections))

    def test_missing_ack_reconcile_is_absent_not_success(self):
        h = Harness(); owner = self.owner(h)
        self.assertIsNone(self.call(owner, h, 'reconcile'))
        self.assertEqual(h.connections[0].writes, 0)

    def test_changed_registration_and_false_native_policy_withhold(self):
        h = Harness(); owner = self.owner(h)
        object.__setattr__(owner.registrations[0].source, 'source_signature_sha256', 'b' * 64)
        with self.assertRaises(AckError): self.call(owner, h)
        self.assertEqual(h.connections[0].writes, 0); self.assertTrue(h.connections[0].closed)
        h = Harness(); h.policy_false = True; owner = self.owner(h)
        with self.assertRaises(AckError): self.call(owner, h)
        self.assertEqual(h.connections[0].writes, 0)

    def test_invalid_bytes_and_unknown_epoch_never_acquire_connection(self):
        h = Harness(); owner = self.owner(h); h.request_bytes = b'{}'
        with self.assertRaises((AckError, KeyError)): self.call(owner, h)
        self.assertEqual(h.connections, [])
        h = Harness(); owner = self.owner(h)
        object.__setattr__(owner.registrations[0].source.scope, 'epoch', 'replacement')
        with self.assertRaises(AckError): self.call(owner, h)
        self.assertEqual(h.connections, [])

    def test_cancellation_identity_before_after_commit_and_during_cleanup(self):
        for stage, cancellation_type in ((stage, kind) for stage in
                ('before_commit', 'after_commit', 'close', 'rollback')
                for kind in (KeyboardInterrupt, SystemExit, GeneratorExit)):
            h = Harness(); owner = self.owner(h); primary = cancellation_type(stage)
            item = owner.registrations[0]; original_factory = item.connection_factory
            def factory(context):
                connection = original_factory(context)
                original_commit = connection.commit
                original_close = connection.close
                if stage == 'before_commit':
                    def commit(): raise primary
                    connection.commit = commit
                elif stage == 'after_commit':
                    def commit():
                        original_commit()
                        raise primary
                    connection.commit = commit
                elif stage == 'close':
                    def close():
                        original_close()
                        raise primary
                    connection.close = close
                else:
                    h.unknown_commit = True
                    def rollback(): raise primary
                    connection.rollback = rollback
                return connection
            object.__setattr__(item, 'connection_factory', factory)
            with self.assertRaises(cancellation_type) as observed: self.call(owner, h)
            self.assertIs(observed.exception, primary)
            self.assertIsInstance(primary.ack_outcome_uncertain, AckOutcomeUncertain)
            self.assertEqual(primary.ack_outcome_uncertain.original_request, h.request_bytes)
            self.assertEqual(len(h.connections), 1)
            self.assertTrue(h.connections[0].closed)

    def test_original_body_cancellation_outranks_different_cleanup_cancellation(self):
        h = Harness(); owner = self.owner(h)
        primary = KeyboardInterrupt('commit'); cleanup = SystemExit('close')
        item = owner.registrations[0]; original_factory = item.connection_factory
        def factory(context):
            connection = original_factory(context)
            def commit(): raise primary
            def close():
                connection.closed = True
                raise cleanup
            connection.commit = commit; connection.close = close
            return connection
        object.__setattr__(item, 'connection_factory', factory)
        with self.assertRaises(KeyboardInterrupt) as observed: self.call(owner, h)
        self.assertIs(observed.exception, primary)
        self.assertTrue(h.connections[0].closed)
        self.assertEqual(primary.ack_outcome_uncertain.original_manifest, h.manifest_bytes)

    def test_status_inspection_cancellation_during_ordinary_failure_is_not_lost(self):
        for kind in (KeyboardInterrupt, SystemExit, GeneratorExit):
            h = Harness(); owner = self.owner(h); primary = kind('status inspection')
            item = owner.registrations[0]; original_factory = item.connection_factory
            def factory(context):
                connection = original_factory(context)
                class FailedInfo:
                    @property
                    def transaction_status(self): raise primary
                    @transaction_status.setter
                    def transaction_status(self, value): pass
                def commit():
                    connection.info = FailedInfo()
                    raise OSError('commit response')
                connection.commit = commit
                return connection
            object.__setattr__(item, 'connection_factory', factory)
            with self.assertRaises(kind) as observed: self.call(owner, h)
            self.assertIs(observed.exception, primary)
            self.assertTrue(h.connections[0].closed)
            self.assertEqual(primary.ack_outcome_uncertain.original_request, h.request_bytes)

    def driver(self, h, owner):
        driver = object.__new__(NativeDriver)
        db = sqlite3.connect(':memory:'); self.addCleanup(db.close)
        db.execute('CREATE TABLE local_publication_artifact(request_digest TEXT, request TEXT, artifact TEXT)')
        db.execute('INSERT INTO local_publication_artifact VALUES(?,?,?)',
            (h.request['request_digest'], encoded(h.request), encoded({'manifest': h.manifest})))
        versions = json.loads(h.manifest['table_versions_json'])
        driver.transport = SimpleNamespace(db=db, targets={table: SimpleNamespace(uuid='trusted-uuid') for table in versions})
        driver.policy = SimpleNamespace(active={})
        driver.require = lambda context: None
        driver.admission_facts = lambda: {'profile': 'ashlar-commerce-evolution-source-set/0.1'}
        driver.ack_sessions = owner; driver.original_ack_sessions = encoded(owner.metadata()) if owner else None
        driver.lose_manifest = False; driver.scope_ports = {}; driver.acks = []
        return driver, SimpleNamespace(raw=h.manifest, versions=versions)

    def test_driver_missing_ack_and_changed_original_artifact_refuse(self):
        h = Harness(); owner = self.owner(h); driver, descriptor = self.driver(h, None)
        with self.assertRaises(PermissionError): driver.acknowledge(h.request, descriptor, h.context)
        driver, descriptor = self.driver(h, owner)
        driver.transport.db.execute('UPDATE local_publication_artifact SET request=?', ('replacement',))
        with self.assertRaises(ValueError): driver.acknowledge(h.request, descriptor, h.context)
        self.assertEqual(h.connections, [])

    def test_driver_injected_dispatch_never_discovers_and_never_reconciles_cancellation(self):
        h = Harness(); owner = self.owner(h); driver, descriptor = self.driver(h, owner)
        primary = KeyboardInterrupt('original ACK')
        with patch('ashlar_host.driver.connect', side_effect=AssertionError('discovery')):
            with patch.object(RegisteredOutboxAcks, 'acknowledge', side_effect=primary):
                with patch.object(RegisteredOutboxAcks, 'reconcile', side_effect=AssertionError('cancel reconcile')):
                    with self.assertRaises(KeyboardInterrupt) as observed:
                        driver.acknowledge(h.request, descriptor, h.context)
                    self.assertIs(observed.exception, primary)
        self.assertEqual(driver.acks, [])

    def test_driver_constructor_rejects_mismatched_source_ack_inventory(self):
        h = Harness(); owner = self.owner(h)
        source = owner.registrations[0].source
        altered = OutboxSourceRegistration(source.scope, 'replacement_namespace', 'a' * 64, h.factory)
        sources = RegisteredOutboxSources((altered,), SimpleNamespace(admit_source=lambda *args: None))
        facts = {'profile': 'ashlar-commerce-evolution-source-set/0.1',
            'qualification': 'inert constructor routing only',
            'sources': [{'source_system': 'feed', 'epoch': 'epoch'}]}
        admission = SimpleNamespace(metadata=lambda: facts)
        with tempfile.TemporaryDirectory() as directory:
            with closing(sqlite3.connect(':memory:')) as db:
                transport = SimpleNamespace(db=db, journal_path=Path(directory) / 'original.sqlite')
                with self.assertRaises(PermissionError):
                    NativeDriver(transport, object(), object(), {}, {}, (), {},
                        source_admission=admission, source_sessions=sources, ack_sessions=owner)
                self.assertEqual(db.execute("SELECT count(*) FROM sqlite_master WHERE type='table'").fetchone()[0], 0)

    def test_registration_copy_and_mandatory_policy(self):
        h = Harness(); self.owner(h); source = OutboxSourceRegistration(h.scope, 'ashlar_ack_source_test', 'a' * 64, h.factory)
        with self.assertRaises(AckError): OutboxAckRegistration(source, h.factory, object())
        item = OutboxAckRegistration(source, h.factory, h.port.policy)
        owner = RegisteredOutboxAcks((item,))
        object.__setattr__(item.source.scope, 'epoch', 'borrowed')
        self.assertEqual(owner.metadata()['registrations'][0]['scope']['epoch'], 'epoch')
        with self.assertRaises(AckError): RegisteredOutboxAcks((item, item))


if __name__ == '__main__': unittest.main()
