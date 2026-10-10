"""Ordinary-session wiring with actual source reader and inert DB-API; no native claim."""
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from ashlar.outbox import OutboxTransaction
from ashlar.source import jsonl_batches, records_digest
from ashlar_host.ack import AckScope
from ashlar_host.driver import NativeDriver, request_for
from ashlar_host.source_sessions import OutboxSourceRegistration, RegisteredOutboxSources, SourceSessionError


SCOPE = AckScope('ashlar_ack_existing', '11111111-1111-1111-1111-111111111111',
    '22222222-2222-2222-2222-222222222222', 'ordinary-existing', 'A', 'original-epoch')


def transaction(label='original'):
    def line(value): return (json.dumps(value, separators=(',', ':')) + '\n').encode()
    record = line({'kind': 'event', 'delivery_id': '1', 'value': label})
    begin = line({'kind': 'begin', 'batch_id': 'original-group'})
    commit = line({'kind': 'commit', 'batch_id': 'original-group', 'record_count': 1, 'records_sha256': records_digest([record])})
    batch, = jsonl_batches((begin, record, commit), feed=SCOPE.feed, epoch=SCOPE.epoch)
    raw = begin + record + commit
    return OutboxTransaction('ashlar-postgresql-outbox/0.1', SCOPE.feed, SCOPE.epoch,
        '0', '1', hashlib.sha256(raw).hexdigest(), batch), raw


class Connection:
    def __init__(self):
        self.calls = []; self.queries = []; self.reads = 0; self.drift = False
        self.rollback_error = None; self.close_error = None
    def cursor(self):
        connection = self
        class Cursor:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def execute(self, sql, params):
                connection.queries.append((sql, params))
                if '.head ' in sql:
                    self.description = [SimpleNamespace(name='position')]; self.values = [('1',)]
                else:
                    connection.reads += 1
                    tx, raw = transaction('changed' if connection.drift and connection.reads == 2 else 'original')
                    self.description = [SimpleNamespace(name=name) for name in ('position', 'batch_id', 'payload', 'digest')]
                    self.values = [(tx.position, tx.batch.batch_id, raw.decode(), tx.payload_digest)]
            def fetchmany(self, count): return self.values
        return Cursor()
    def rollback(self):
        self.calls.append('rollback')
        if self.rollback_error: raise self.rollback_error
    def close(self):
        self.calls.append('close')
        if self.close_error: raise self.close_error


class Policy:
    def __init__(self): self.calls = []; self.error = None; self.fail_on = 1; self.result = None
    def admit_source(self, registration, request, session, context):
        self.calls.append((registration.metadata(), dict(request), session, context))
        if self.error is not None and len(self.calls) == self.fail_on: raise self.error
        return self.result


class Tests(unittest.TestCase):
    def setUp(self):
        self.connection = Connection(); self.policy = Policy(); self.context = object(); self.factory_calls = []
        def factory(context):
            self.factory_calls.append(context)
            return self.connection
        self.registration = OutboxSourceRegistration(SCOPE, 'existing_source', 'a' * 64, factory)
        self.owner = RegisteredOutboxSources((self.registration,), self.policy)
        self.tx, self.raw = transaction()
        self.request = request_for('stream', self.tx, 'original-predecessor', {'A': 'original-schema'})

    def test_exact_original_read_closing_admission_and_owned_cleanup(self):
        self.assertIsNone(self.owner.admit(self.request, context=self.context))
        self.assertEqual(self.factory_calls, [self.context]); self.assertEqual(self.connection.reads, 2)
        self.assertEqual(len(self.policy.calls), 3)
        self.assertEqual(self.connection.calls, ['rollback', 'close'])
        self.assertTrue(all(not session.active for facts, request, session, context in self.policy.calls))
        self.assertEqual({sql.split()[0] for sql, params in self.connection.queries}, {'SELECT'})
        self.assertEqual(self.owner.metadata()['registrations'][0]['scope']['feed'], 'A')
        self.assertNotIn('connection_factory', repr(self.registration))

    def test_scope_and_request_conflicts_refuse_before_factory(self):
        changed = dict(self.request); changed['predecessor'] = 'replacement'
        with self.assertRaises(ValueError): self.owner.admit(changed, context=self.context)
        other = request_for('stream', transaction()[0], 'original-predecessor', {'A': 'original-schema'})
        wrong_scope = AckScope(SCOPE.service_schema, SCOPE.scope_id, SCOPE.installation_id,
            SCOPE.consumer, SCOPE.feed, 'wrong-epoch')
        owner = RegisteredOutboxSources((OutboxSourceRegistration(wrong_scope, 'existing_source', 'a' * 64,
            self.registration.connection_factory),), self.policy)
        with self.assertRaises(SourceSessionError): owner.admit(other, context=self.context)
        self.assertEqual(self.factory_calls, [])

    def test_closing_original_group_drift_and_current_policy_failure_withhold(self):
        self.connection.drift = True
        with self.assertRaises(SourceSessionError): self.owner.admit(self.request, context=self.context)
        self.assertEqual(self.connection.calls, ['rollback', 'close'])
        self.connection = Connection(); primary = PermissionError('closed current authority')
        self.policy = Policy(); self.policy.error = primary; self.policy.fail_on = 3
        self.owner = RegisteredOutboxSources((self.registration,), self.policy)
        with self.assertRaises(PermissionError) as observed: self.owner.admit(self.request, context=self.context)
        self.assertIs(observed.exception, primary); self.assertEqual(self.connection.calls, ['rollback', 'close'])

    def test_false_policy_return_and_failed_session_construction_always_close(self):
        self.policy.result = True
        with self.assertRaises(SourceSessionError): self.owner.admit(self.request, context=self.context)
        self.assertEqual(self.connection.calls, ['rollback', 'close']); self.assertEqual(self.connection.reads, 0)
        self.connection = Connection()
        with patch('ashlar_host.source_sessions.Session', side_effect=ValueError('session construction')):
            with self.assertRaises(ValueError): self.owner.admit(self.request, context=self.context)
        self.assertEqual(self.connection.calls, ['rollback', 'close'])

    def test_cleanup_cancellation_priority_and_all_cleanup_attempted(self):
        self.policy.error = ValueError('body'); cancellation = KeyboardInterrupt('rollback')
        self.connection.rollback_error = cancellation; self.connection.close_error = OSError('close')
        with self.assertRaises(KeyboardInterrupt) as observed: self.owner.admit(self.request, context=self.context)
        self.assertIs(observed.exception, cancellation); self.assertEqual(self.connection.calls, ['rollback', 'close'])
        self.connection = Connection(); self.policy.error = KeyboardInterrupt('body')
        self.policy.calls.clear()
        self.connection.rollback_error = SystemExit('rollback')
        with self.assertRaises(KeyboardInterrupt) as observed: self.owner.admit(self.request, context=self.context)
        self.assertIs(observed.exception, self.policy.error); self.assertEqual(self.connection.calls, ['rollback', 'close'])

    def test_success_cleanup_failure_withholds_and_invalidates_session(self):
        primary = OSError('rollback'); self.connection.rollback_error = primary
        with self.assertRaises(OSError) as observed: self.owner.admit(self.request, context=self.context)
        self.assertIs(observed.exception, primary); self.assertEqual(self.connection.calls, ['rollback', 'close'])
        self.assertFalse(self.policy.calls[-1][2].active)

    def test_registration_validation_and_owned_copy(self):
        for schema, signature, factory in (('bad;namespace', 'a' * 64, lambda _: None),
                                           ('existing', 'A' * 64, lambda _: None), ('existing', 'a' * 64, None)):
            with self.assertRaises(SourceSessionError): OutboxSourceRegistration(SCOPE, schema, signature, factory)
        with self.assertRaises(SourceSessionError): RegisteredOutboxSources((self.registration, self.registration), self.policy)
        with self.assertRaises(SourceSessionError): RegisteredOutboxSources((self.registration,), object())
        object.__setattr__(self.registration.scope, 'epoch', 'borrowed-change')
        self.assertEqual(self.owner.metadata()['registrations'][0]['scope']['epoch'], 'original-epoch')

    def test_scope_subclasses_refuse_without_executable_copy_or_equality(self):
        class Borrowed(str):
            def __eq__(self, other): raise AssertionError('borrowed equality')
            def __deepcopy__(self, memo): raise AssertionError('borrowed deepcopy')
        for field in ('service_schema', 'scope_id', 'installation_id', 'consumer', 'feed', 'epoch'):
            scope = AckScope(SCOPE.service_schema, SCOPE.scope_id, SCOPE.installation_id,
                SCOPE.consumer, SCOPE.feed, SCOPE.epoch)
            object.__setattr__(scope, field, Borrowed(getattr(scope, field)))
            with self.assertRaises(SourceSessionError):
                OutboxSourceRegistration(scope, 'existing_source', 'a' * 64, lambda _: None)

    def test_factory_failure_and_malformed_acquired_connection_cleanup(self):
        primary = OSError('factory')
        def failed(context): raise primary
        owner = RegisteredOutboxSources((OutboxSourceRegistration(SCOPE, 'existing_source',
            'a' * 64, failed),), self.policy)
        with self.assertRaises(OSError) as observed: owner.admit(self.request, context=self.context)
        self.assertIs(observed.exception, primary)
        self.assertEqual(self.connection.calls, [])
        self.connection.rollback = None
        with self.assertRaises(SourceSessionError): self.owner.admit(self.request, context=self.context)
        self.assertEqual(self.connection.calls, ['close'])

    def test_borrowed_request_mutation_cannot_change_original_snapshot(self):
        original = dict(self.request)
        admit = self.policy.admit_source
        def mutate(registration, request, session, context):
            self.request['source_batch_json'] = 'replacement'
            with self.assertRaises(TypeError): request['predecessor'] = 'replacement'
            return admit(registration, request, session, context)
        self.policy.admit_source = mutate
        self.owner.admit(self.request, context=self.context)
        self.assertTrue(all(request == original for facts, request, session, context in self.policy.calls))

    def test_opening_refusal_and_closing_registration_change_withhold(self):
        primary = PermissionError('opening')
        self.policy.error = primary
        with self.assertRaises(PermissionError) as observed: self.owner.admit(self.request, context=self.context)
        self.assertIs(observed.exception, primary)
        self.assertEqual(self.connection.reads, 0)
        self.assertEqual(self.connection.calls, ['rollback', 'close'])
        self.connection = Connection(); self.policy.error = None; self.policy.calls.clear()
        admit = self.policy.admit_source
        def mutate(registration, request, session, context):
            result = admit(registration, request, session, context)
            if len(self.policy.calls) == 3:
                object.__setattr__(registration, 'source_signature_sha256', 'b' * 64)
            return result
        self.policy.admit_source = mutate
        with self.assertRaises(SourceSessionError): self.owner.admit(self.request, context=self.context)
        self.assertEqual(self.connection.calls, ['rollback', 'close'])

    def driver(self, profile='ashlar-commerce-evolution-source-set/0.1', sessions=True):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        db = sqlite3.connect(':memory:'); self.addCleanup(db.close)
        db.execute('CREATE TABLE local_plan (operation TEXT PRIMARY KEY, original TEXT, digest TEXT)')
        transport = SimpleNamespace(db=db, journal_path=Path(temp.name) / 'original.sqlite')
        facts = {'profile': profile, 'qualification': 'inert injected wiring only',
            'sources': [{'source_system': 'A', 'epoch': 'original-epoch'}]}
        admission = SimpleNamespace(metadata=lambda: json.loads(json.dumps(facts)), admit=lambda _: None)
        return NativeDriver(transport, SimpleNamespace(), self.context, {}, {}, (), {},
            source_admission=admission, source_sessions=self.owner if sessions else None)

    def test_driver_injection_requires_held_context_and_never_discovers_connection(self):
        driver = self.driver()
        with patch('ashlar_host.driver.connect', side_effect=AssertionError('private discovery invoked')):
            with self.assertRaises(PermissionError): driver.source_admit(self.request)
            self.assertEqual(self.factory_calls, [])
            with driver.writer('stream', self.context): driver.source_admit(self.request)
        self.assertEqual(self.connection.calls, ['rollback', 'close'])

    def test_source_set_profile_cannot_select_legacy_default(self):
        with self.assertRaises(PermissionError): self.driver(sessions=False)
        legacy = self.driver(profile='qualified-private-existing/0.1', sessions=False)
        self.assertIsNone(legacy.source_sessions)


if __name__ == '__main__': unittest.main()
