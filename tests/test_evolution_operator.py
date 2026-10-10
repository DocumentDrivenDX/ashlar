"""Existing owner construction/custody tests; no native sessions executed."""
import unittest
from contextlib import contextmanager
import json
import os
import sqlite3
from unittest.mock import patch
from ashlar_host.driver import NativeDriver
from ashlar_host.ack_sessions import RegisteredOutboxAcks
from ashlar_host.evolution_run import EvolutionRunLedger
from ashlar_host.evolution_plan import EvolutionPlanError
from ashlar_host.delta_custody import DeltaTarget, LOCAL_OPERATION_CAPACITY_8M, LocalDeltaTransport
from ashlar_host.evolution_operator import (ExistingEvolutionOperator, ExistingEvolutionLease,
    ExistingEvolutionNativePolicy)
from ashlar_host.existing_outbox_authority import OutboxAuthorityRegistration
from ashlar_host.evolution_cli import FreshEvolutionInvocation, ResumeEvolutionInvocation
from ashlar_host.config import FreshCommerceEvolutionConfig, ResumeCommerceEvolutionConfig
from ashlar_host.evolution_composition import NativeEvolutionRunPolicy
import test_evolution_run as original


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): original.Tests.setUpClass()

    def setUp(self):
        self.fixture = original.Tests(); self.fixture.setUp(); self.addCleanup(self.fixture.doCleanups)
        self.driver = self.fixture.driver; self.closes = []
        self.driver.transport.close = lambda: self.closes.append('transport')

    def construct(self, factory):
        source = self.driver
        return NativeDriver.with_registered_evolution(source.transport, source.policy, source.context,
            source.tables, source.allowed_changes, source.columns, source_admission=source.source_admission,
            source_sessions=source.source_sessions, ack_factory=factory)

    def installed(self, capacity=None):
        fixture = self.fixture; targets = []
        for index, old in enumerate(self.driver.transport.targets.values()):
            path = fixture.root / ('table-' + str(index)); path.mkdir()
            targets.append(DeltaTarget(old.table, path, old.uuid))
        selected = tuple(OutboxAuthorityRegistration(item, 'ordinary_reader', 'ordinary_ack',
            'existing_outbox', lambda context: self.fail('no ordinary connection expected'))
            for item in self.driver.source_sessions.registrations)
        self.native_cleanup = []
        @contextmanager
        def native():
            try: yield object()
            finally: self.native_cleanup.append('native')
        operator = ExistingEvolutionOperator(self.driver.transport.installation_id,
            self.driver.transport.journal_path, fixture.root / 'original-run.json',
            tuple(targets), selected, capacity, native)
        invocation = FreshEvolutionInvocation(fixture.path, fixture.root / 'result.json',
            fixture.producer, fixture.request)
        registry = {'installation': operator.installation_id,
            'targets': [{'table': item.table, 'path': str(item.path), 'uuid': item.uuid}
                for item in sorted(targets, key=lambda item: item.table)]}
        if capacity is not None: registry['operation_capacity'] = dict(capacity)
        from ashlar_host.delta_custody import encoded
        connection = sqlite3.connect(operator.journal_path)
        self.driver.transport.db.backup(connection)
        with connection:
            connection.execute('CREATE TABLE local_installation(id INTEGER PRIMARY KEY CHECK(id=1), original TEXT NOT NULL)')
            connection.execute('INSERT INTO local_installation VALUES(1,?)', (encoded(registry),))
        connection.execute('PRAGMA journal_mode=WAL'); connection.close()
        os.chmod(operator.journal_path, 0o600)
        reservation = (encoded({'profile': 'ashlar-private-native-installation-reservation/0.1',
            'registry': registry, 'original_journal_path': str(operator.journal_path)}) + '\n').encode()
        for item in targets:
            marker = item.path / '.ashlar-local-installation-reservation.json'
            marker.write_bytes(reservation); os.chmod(marker, 0o600)
        self.driver.transport.targets = {item.table: item for item in targets}
        return operator, invocation

    def test_owned_factory_copies_complete_ack_owner_and_blocks_early_writer(self):
        def factory(owner):
            with self.assertRaises(PermissionError):
                with owner.writer('premature', owner.context): self.fail('writer released')
            with self.assertRaises(PermissionError): owner.acknowledge({}, None, owner.context)
            return self.driver.ack_sessions
        owner = self.construct(factory)
        self.assertIs(type(owner), NativeDriver)
        self.assertIsNot(owner.ack_sessions, self.driver.ack_sessions)
        self.assertEqual(owner.ack_sessions.metadata(), self.driver.ack_sessions.metadata())
        self.assertEqual(self.closes, [])
        with owner.writer('held', owner.context): self.assertIsNone(owner.require(owner.context))

    def test_incomplete_or_missing_acks_close_and_keep_leaked_owner_unusable(self):
        for supplied in (object(), RegisteredOutboxAcks(self.driver.ack_sessions.registrations[:1])):
            exposed = []
            def factory(owner): exposed.append(owner); return supplied
            with self.assertRaises(PermissionError): self.construct(factory)
            with self.assertRaises(PermissionError):
                with exposed[0].writer('later', exposed[0].context): self.fail('writer released')
        self.assertEqual(self.closes, ['transport', 'transport'])

    def test_missing_existing_journal_refuses_without_recreating_or_factory(self):
        for table in ('local_source_plan', 'local_publication_artifact'):
            self.driver.transport.db.execute('DROP TABLE ' + table)
            def factory(owner): self.fail('factory called with missing original custody')
            with self.assertRaises(PermissionError): self.construct(factory)
            names = {row[0] for row in self.driver.transport.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn(table, names)
        self.assertEqual(self.closes, ['transport', 'transport'])

    def test_original_factory_cancel_and_cleanup_cancel_priority(self):
        for kind in (KeyboardInterrupt, SystemExit, GeneratorExit):
            cancellation = kind()
            def factory(owner): raise cancellation
            with self.assertRaises(kind) as caught: self.construct(factory)
            self.assertIs(caught.exception, cancellation)
        cancellation = KeyboardInterrupt()
        def close(): self.closes.append('transport'); raise cancellation
        self.driver.transport.close = close
        def factory(owner): raise RuntimeError()
        with self.assertRaises(KeyboardInterrupt) as caught: self.construct(factory)
        self.assertIs(caught.exception, cancellation)
        self.assertEqual(self.closes, ['transport'] * 4)

    def test_existing_ledger_inspection_is_readonly_and_current_custody_required(self):
        driver = self.driver; fixture = self.fixture; calls = []
        with driver.writer('original', driver.context):
            original_run = driver.capture_evolution_run(fixture.request, context=driver.context)
            ledger = EvolutionRunLedger.open(fixture.path, fixture.policy,
                context=driver.context, original=original_run)
            ledger.close()
            before = fixture.path.read_bytes()
            def custody(path, retained, context):
                self.assertEqual(path, fixture.path)
                self.assertEqual(retained.raw, original_run.raw)
                self.assertIs(context, driver.context)
                driver.require(context); calls.append('renew')
            view = EvolutionRunLedger.inspect(fixture.path, original_run,
                context=driver.context, custody=custody)
            self.assertEqual(len(view.slots), 8); self.assertEqual(calls, ['renew', 'renew'])
            self.assertEqual(fixture.path.read_bytes(), before)
            cancellation = KeyboardInterrupt()
            def cancel(*args): raise cancellation
            with self.assertRaises(KeyboardInterrupt) as caught:
                EvolutionRunLedger.inspect(fixture.path, original_run,
                    context=driver.context, custody=cancel)
            self.assertIs(caught.exception, cancellation)
            renewals = [0]
            def closing_cancel(*args):
                renewals[0] += 1
                if renewals[0] == 2: raise cancellation
                return custody(*args)
            with self.assertRaises(KeyboardInterrupt) as caught:
                EvolutionRunLedger.inspect(fixture.path, original_run,
                    context=driver.context, custody=closing_cancel)
            self.assertIs(caught.exception, cancellation)
            import sqlite3
            corrupt = sqlite3.connect(fixture.path)
            try:
                with corrupt: corrupt.execute('UPDATE original_run SET digest=?', ('b' * 64,))
                with self.assertRaises(EvolutionPlanError):
                    EvolutionRunLedger.inspect(fixture.path, original_run,
                        context=driver.context, custody=custody)
                with corrupt:
                    corrupt.execute('UPDATE original_run SET digest=?', (original_run.sha256,))
                    corrupt.execute('DELETE FROM original_slot WHERE ordinal=3')
            finally: corrupt.close()
            with self.assertRaises(EvolutionPlanError):
                EvolutionRunLedger.inspect(fixture.path, original_run,
                    context=driver.context, custody=custody)

    def test_concrete_provider_composes_current_owners_and_closes_once(self):
        operator, invocation = self.installed(LOCAL_OPERATION_CAPACITY_8M)
        admissions = self.driver.source_admission.admissions
        def producer(config, *, source_system, epoch):
            return next(item for item in admissions if item.prepared.source_system == source_system)
        with patch('ashlar_host.evolution_operator.admit_commerce_evolution', side_effect=producer), \
                patch('ashlar_host.evolution_operator.LocalDeltaTransport', return_value=self.driver.transport) as native:
            with operator.open_evolution(invocation) as config:
                self.assertIs(type(config), FreshCommerceEvolutionConfig)
                self.assertIs(type(config.policy), NativeEvolutionRunPolicy)
                self.assertIs(type(config.driver.policy), ExistingEvolutionNativePolicy)
                self.assertEqual(config.driver.source_sessions.metadata()['registrations'],
                    config.driver.ack_sessions.metadata()['registrations'])
                self.assertFalse(config.driver.policy.initializing)
                with config.driver.writer('original', config.context):
                    self.assertIsNone(config.driver.require(config.context))
                self.assertEqual(native.call_args.kwargs['capacity'], LOCAL_OPERATION_CAPACITY_8M)
            self.assertEqual(self.closes, ['transport'])
            self.assertEqual(self.native_cleanup, ['native'])
        self.assertFalse(operator.reservation_path.exists())

    def test_actual_lease_original_run_resume_and_loss_refuse(self):
        operator, invocation = self.installed(); context = self.driver.context
        with self.assertRaises(PermissionError):
            with ExistingEvolutionLease.open(operator, invocation, context) as lease:
                with self.driver.writer('original', context):
                    original_run = self.driver.capture_evolution_run(invocation.request, context=context)
                    lease.admit_run(original_run, context)
                    ledger = EvolutionRunLedger.open(invocation.ledger_path, lease,
                        context=context, original=original_run)
                    lease.admit_ledger(original_run, EvolutionRunLedger.inspect(invocation.ledger_path,
                        original_run, context=context, custody=lease._ledger_custody), context)
                    ledger.close()
                lock = lease.lock_path; lock.unlink(); lock.touch(mode=0o600)
                with self.assertRaises(PermissionError): lease.renew(context)
        resume = ResumeEvolutionInvocation(invocation.ledger_path, invocation.receipt_path,
            invocation.producer, original_run.sha256)
        with ExistingEvolutionLease.open(operator, resume, context) as lease:
            lease.admit_run(original_run, context)
        operator.reservation_path.unlink()
        with self.assertRaises(FileNotFoundError):
            with ExistingEvolutionLease.open(operator, resume, context): self.fail('missing original released')
        self.assertFalse(operator.reservation_path.exists())

    def test_lease_mismatched_registry_mode_and_competing_owner_refuse(self):
        operator, invocation = self.installed(); context = object()
        with ExistingEvolutionLease.open(operator, invocation, context):
            with self.assertRaises(BlockingIOError):
                with ExistingEvolutionLease.open(operator, invocation, object()): self.fail('competing lease')
        connection = sqlite3.connect(operator.journal_path)
        try:
            connection.execute('PRAGMA journal_mode=DELETE')
            with self.assertRaises(PermissionError):
                with ExistingEvolutionLease.open(operator, invocation, context): self.fail('wrong mode')
            self.assertEqual(connection.execute('PRAGMA journal_mode').fetchone(), ('delete',))
            connection.execute('PRAGMA journal_mode=WAL')
            with connection: connection.execute('UPDATE local_installation SET original=?', ('{}',))
            with self.assertRaises(PermissionError):
                with ExistingEvolutionLease.open(operator, invocation, context): self.fail('wrong registry')
        finally: connection.close()
        self.assertFalse(operator.reservation_path.exists())

    def test_provider_body_cancellation_and_construction_failure_close_once(self):
        operator, invocation = self.installed()
        admissions = self.driver.source_admission.admissions
        def producer(config, *, source_system, epoch):
            return next(item for item in admissions if item.prepared.source_system == source_system)
        for kind in (KeyboardInterrupt, SystemExit, GeneratorExit):
            cancellation = kind()
            with patch('ashlar_host.evolution_operator.admit_commerce_evolution', side_effect=producer), \
                    patch('ashlar_host.evolution_operator.LocalDeltaTransport', return_value=self.driver.transport):
                with self.assertRaises(kind) as caught:
                    with operator.open_evolution(invocation): raise cancellation
                self.assertIs(caught.exception, cancellation)
        self.assertEqual(self.closes, ['transport'] * 3)
        self.assertEqual(self.native_cleanup, ['native'] * 3)
        original_factory = NativeDriver.with_registered_evolution
        def fail(*args, **kwargs):
            def ack_factory(driver): raise RuntimeError('failed construction')
            kwargs['ack_factory'] = ack_factory
            return original_factory(*args, **kwargs)
        with patch('ashlar_host.evolution_operator.admit_commerce_evolution', side_effect=producer), \
                patch('ashlar_host.evolution_operator.LocalDeltaTransport', return_value=self.driver.transport), \
                patch.object(NativeDriver, 'with_registered_evolution', side_effect=fail):
            with self.assertRaises(RuntimeError):
                with operator.open_evolution(invocation): self.fail('failed construction released')
        self.assertEqual(self.closes, ['transport'] * 4)
        self.assertEqual(self.native_cleanup, ['native'] * 4)

    def test_owned_native_journal_pragma_failure_closes_and_preserves_cancel(self):
        operator, _ = self.installed(); closes = []
        for kind in (KeyboardInterrupt, SystemExit, GeneratorExit):
            cancellation = kind()
            class Connection:
                def execute(self, statement): raise cancellation
                def close(self): closes.append('close')
            with patch('ashlar_host.delta_custody.sqlite3.connect', return_value=Connection()), \
                    patch.object(LocalDeltaTransport, '_profile'), patch.object(LocalDeltaTransport, '_detail'):
                with self.assertRaises(kind) as caught:
                    LocalDeltaTransport(object(), operator.journal_path, operator.installation_id,
                        operator.targets, self.driver.policy)
                self.assertIs(caught.exception, cancellation)
        self.assertEqual(closes, ['close'] * 3)

    def test_provider_cleanup_cancel_priority_and_lease_released_after_failure(self):
        operator, invocation = self.installed(); admissions = self.driver.source_admission.admissions
        def producer(config, *, source_system, epoch):
            return next(item for item in admissions if item.prepared.source_system == source_system)
        closing = SystemExit()
        def close(): self.closes.append('transport'); raise closing
        self.driver.transport.close = close
        with patch('ashlar_host.evolution_operator.admit_commerce_evolution', side_effect=producer), \
                patch('ashlar_host.evolution_operator.LocalDeltaTransport', return_value=self.driver.transport):
            with self.assertRaises(SystemExit) as caught:
                with operator.open_evolution(invocation): raise RuntimeError('ordinary body')
            self.assertIs(caught.exception, closing)
            original_cancel = KeyboardInterrupt()
            with self.assertRaises(KeyboardInterrupt) as caught:
                with operator.open_evolution(invocation): raise original_cancel
            self.assertIs(caught.exception, original_cancel)
        self.assertEqual(self.closes, ['transport'] * 2)
        self.assertEqual(self.native_cleanup, ['native'] * 2)
        with ExistingEvolutionLease.open(operator, invocation, object()): pass

    def test_capacity_snapshot_and_exact_existing_capacity_refusal(self):
        configured = dict(LOCAL_OPERATION_CAPACITY_8M)
        operator, invocation = self.installed(configured)
        configured.clear()
        self.assertEqual(operator.capacity, LOCAL_OPERATION_CAPACITY_8M)
        from dataclasses import replace
        mismatched = replace(operator, capacity=None)
        with self.assertRaises(PermissionError):
            with ExistingEvolutionLease.open(mismatched, invocation, object()): self.fail('changed capacity')
        self.assertFalse(operator.reservation_path.exists())


if __name__ == '__main__': unittest.main()
