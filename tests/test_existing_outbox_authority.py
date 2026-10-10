"""Current PostgreSQL authority gates on inert native observations, no PG proof."""
from types import SimpleNamespace
import unittest
from ashlar.native import SQLResult
from ashlar_host.ack import AckScope
from ashlar_host.source_sessions import OutboxSourceRegistration
from ashlar_host.postgres import Session
from ashlar_host.existing_outbox_authority import (ExistingOutboxAuthority,
    OutboxAuthorityRegistration, OutboxAuthorityError)
import test_evolution_publication as original


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): original.Tests.setUpClass()

    def setUp(self):
        _, plan, _ = original.Tests().original(); self.request = plan.document()['request']
        self.context = object(); self.calls = []; self.cleanup = []; self.fail = None
        self.scope = AckScope('ashlar_ack_existing', '11111111-1111-1111-1111-111111111111',
            '22222222-2222-2222-2222-222222222222', 'ordinary', 'A', 'shared-epoch')
        self.source = OutboxSourceRegistration(self.scope, 'ashlar_ack_source_existing', 'a' * 64, lambda context: None)
        outer = self
        class Writer:
            def admit_source_writer(self, registration, context):
                if context is not outer.context or registration.metadata() != outer.source.metadata():
                    raise PermissionError('Original independent writer lease differs')
                outer.calls.append('writer')
        self.writer = Writer()
        class Connection:
            autocommit = False
            def rollback(self): outer.cleanup.append('rollback')
            def close(self): outer.cleanup.append('close')
        self.connection = Connection()
        self.selected = OutboxAuthorityRegistration(self.source, 'ordinary_reader', 'ordinary_ack',
            'truss_e2e', lambda context: self.connection)
        self.owner = ExistingOutboxAuthority((self.selected,), self.writer)
        self.session = Session(Connection())
        self.session.query = lambda statement, parameters: self.observe(statement, parameters, 'ordinary_reader')
        # Session type is kept exact; owner-created metadata session SQL is routed
        # by replacing its class method only during the test invocation.

    def observe(self, statement, parameters, role):
        self.calls.append((statement, dict(parameters)))
        if 'pg_catalog.pg_roles' in statement:
            row = {'session_user': role, 'current_user': role, 'database': 'truss_e2e',
                'isolation': 'read committed', 'rolsuper': False, 'rolcreaterole': False,
                'rolcreatedb': False, 'rolreplication': False, 'rolbypassrls': False}
        elif 'scope_binding(CAST' in statement:
            value = self.source.metadata()['scope']; value.pop('service_schema')
            value.update(source_schema=self.source.source_schema, source_signature_sha256='a' * 64)
            row = {'binding': value}
        elif parameters['namespace'] == self.scope.service_schema:
            row = {'usage': True, 'create': False, 'binding': True, 'observe': True,
                'ack': True, 'installation_write': False, 'batch_write': False, 'head_write': False,
                'append': False, 'source_owner': False, 'scope_write': False, 'receipt_write': False, 'owner': False}
        else:
            row = {'usage': True, 'create': False, 'batch_read': True, 'head_read': True,
                'batch_write': False, 'head_write': False, 'append': False, 'owner': False}
        if self.fail is not None: self.fail(row, statement)
        return SQLResult([row])

    def admit(self):
        with unittest.mock.patch.object(Session, 'query', lambda session, sql, parameters:
                self.observe(sql, parameters, 'ordinary_ack')):
            return self.owner.admit_source(self.source, self.request, self.session, self.context)

    def test_actual_complete_role_privilege_signature_and_writer_gates(self):
        self.assertIsNone(self.admit())
        self.assertEqual(self.cleanup, ['rollback', 'close'])
        self.assertEqual(self.calls.count('writer'), 4)
        queries = [call[0] for call in self.calls if type(call) is tuple]
        self.assertTrue(any('scope_binding(CAST' in sql for sql in queries))
        self.assertTrue(all(sql.startswith('SELECT ') for sql in queries))

    def test_weak_roles_isolation_and_privileges_refuse(self):
        for field, value in (('rolsuper', True), ('isolation', 'repeatable read'),
                              ('batch_write', True), ('batch_read', False), ('owner', True)):
            def fail(row, sql):
                if field in row: row[field] = value
            self.fail = fail
            with self.assertRaises(OutboxAuthorityError): self.admit()

    def test_public_ack_scope_renews_same_session_without_owning_cleanup(self):
        ack_session = Session(self.connection)
        with unittest.mock.patch.object(Session, 'query', lambda session, sql, parameters:
                self.observe(sql, parameters, 'ordinary_ack')):
            self.assertIsNone(self.owner.admit_scope(self.scope, ack_session, self.context))
            self.assertEqual(self.cleanup, [])
            changed = AckScope(self.scope.service_schema, self.scope.scope_id,
                self.scope.installation_id, self.scope.consumer, self.scope.feed, 'other-epoch')
            with self.assertRaises(OutboxAuthorityError):
                self.owner.admit_scope(changed, ack_session, self.context)
            reads = [0]
            def closing_drift(row, sql):
                if 'pg_catalog.pg_roles' in sql:
                    reads[0] += 1
                    if reads[0] == 2: row['rolsuper'] = True
            self.fail = closing_drift
            with self.assertRaises(OutboxAuthorityError):
                self.owner.admit_scope(self.scope, ack_session, self.context)
        self.assertEqual(self.cleanup, [])

    def test_scope_signature_epoch_and_current_ack_privileges_refuse(self):
        for field in ('source_signature_sha256', 'epoch', 'feed'):
            def fail(row, sql):
                if 'binding' in row and type(row['binding']) is dict: row['binding'][field] = 'changed'
            self.fail = fail
            with self.assertRaises(OutboxAuthorityError): self.admit()
        for field in ('ack', 'source_owner', 'installation_write'):
            self.fail = lambda row, sql: row.update({field: field != 'ack'}) if 'ack' in row else None
            with self.assertRaises(OutboxAuthorityError): self.admit()
        self.assertEqual(self.cleanup, ['rollback', 'close'] * 6)

    def test_missing_or_wrong_writer_never_queries_source(self):
        with self.assertRaises(OutboxAuthorityError): ExistingOutboxAuthority((self.selected,), object())
        self.context = object()
        # Writer below retains actual expected token; no descriptor flag grants it.
        class Refuse:
            def admit_source_writer(self, registration, context): raise PermissionError()
        self.owner = ExistingOutboxAuthority((self.selected,), Refuse())
        with self.assertRaises(PermissionError): self.admit()
        self.assertEqual(self.calls, [])

    def test_factory_failure_and_invalid_acquisition_cleanup(self):
        error = RuntimeError()
        def factory(context): raise error
        selected = OutboxAuthorityRegistration(self.source, 'ordinary_reader', 'ordinary_ack', 'truss_e2e', factory)
        self.owner = ExistingOutboxAuthority((selected,), self.writer)
        with self.assertRaises(RuntimeError) as caught: self.admit()
        self.assertIs(caught.exception, error); self.assertEqual(self.cleanup, [])
        self.connection.autocommit = True; self.owner = ExistingOutboxAuthority((self.selected,), self.writer)
        with self.assertRaises(OutboxAuthorityError): self.admit()
        self.assertEqual(self.cleanup, ['rollback', 'close'])

    def test_cancellation_identity_all_cleanup_and_closing_priority(self):
        for kind in (KeyboardInterrupt, SystemExit, GeneratorExit):
            cancellation = kind()
            def fail(row, sql):
                if 'binding' in row and type(row['binding']) is dict: raise cancellation
            self.fail = fail; self.cleanup.clear()
            def rollback(): self.cleanup.append('rollback'); raise RuntimeError()
            self.connection.rollback = rollback
            with self.assertRaises(kind) as caught: self.admit()
            self.assertIs(caught.exception, cancellation); self.assertEqual(self.cleanup, ['rollback', 'close'])
        cancellation = KeyboardInterrupt()
        def fail(row, sql):
            if 'binding' in row and type(row['binding']) is dict: raise ValueError()
        def rollback(): self.cleanup.append('rollback'); raise cancellation
        self.fail = fail; self.connection.rollback = rollback; self.cleanup.clear()
        with self.assertRaises(KeyboardInterrupt) as caught: self.admit()
        self.assertIs(caught.exception, cancellation); self.assertEqual(self.cleanup, ['rollback', 'close'])


if __name__ == '__main__': unittest.main()
