"""Eight-step public composition with original semantics/inert native ports only."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from ashlar_host.ack import AckScope
from ashlar_host.ack_sessions import OutboxAckRegistration, RegisteredOutboxAcks
from ashlar_host.source_sessions import OutboxSourceRegistration, RegisteredOutboxSources
from ashlar_host.config import (EvolutionAdmissionConfig, FreshCommerceEvolutionConfig,
    ResumeCommerceEvolutionConfig, HostError)
from ashlar_host.commerce_evolution import publish_commerce_evolution, resume_commerce_evolution
from ashlar_host.evolution_run import EvolutionRunRequest, EvolutionRunDefinition, EvolutionRunLedger, PROFILE, encoded
from ashlar_host.evolution_plan import EvolutionPlanError
from ashlar_host.driver import encoded as text
from ashlar_host.delta_custody import LocalDeltaEffects, LocalDeltaError
import test_evolution_publication as transaction_tests
from test_evolution_source_set import CLOCK_A, CLOCK_B


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): transaction_tests.Tests.setUpClass()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.driver, self.plan, self.batch = transaction_tests.Tests().original()
        self.driver.held = False; transport = self.driver.transport
        transport.policy = self.driver.policy
        transport.journal_path = self.root / 'native.sqlite'
        transport.db = sqlite3.connect(':memory:'); self.addCleanup(transport.db.close)
        for statement in (
            'CREATE TABLE local_operation(operation TEXT PRIMARY KEY,intent TEXT,intent_sha TEXT,before_version INTEGER,state TEXT,receipt TEXT)',
            'CREATE TABLE local_plan(operation TEXT PRIMARY KEY,original TEXT,digest TEXT)',
            'CREATE TABLE local_source_plan(request_digest TEXT,original TEXT)',
            'CREATE TABLE local_publication_artifact(request_digest TEXT,request TEXT,artifact TEXT)'):
            transport.db.execute(statement)
        self.snapshots = {}; self.versions = {}
        for target in transport.targets.values():
            self.versions[target.table] = 0
            self.snapshots[(target.table, 0)] = transport._snapshot(target, 0)
        transport._snapshot = lambda target, version: self.snapshots[(target.table, version)]
        transport.original_history = lambda target: [{'version': self.versions[target.table]}]
        transport._profile = lambda: None
        @contextmanager
        def lock(): yield
        transport._lock = lock
        registrations = []
        for ordinal, source in enumerate(('A', 'B'), 1):
            scope = AckScope('ashlar_ack_existing', '11111111-1111-1111-1111-' + str(ordinal).zfill(12),
                '22222222-2222-2222-2222-222222222222', 'ordinary', source, 'shared-epoch')
            registrations.append(OutboxSourceRegistration(scope, 'source_' + source, 'a' * 64, lambda context: None))
        native_policy = SimpleNamespace(admit_source=lambda *args: None,
            admit_scope=lambda *args: None, admit_publication=lambda *args: None)
        self.driver.source_sessions = RegisteredOutboxSources(tuple(registrations), native_policy)
        self.driver.ack_sessions = RegisteredOutboxAcks(tuple(OutboxAckRegistration(item, lambda context: None,
            native_policy) for item in registrations))
        self.request = EvolutionRunRequest(('A', 'B'), 'original-eight-step', 'original-origin',
            (CLOCK_A, CLOCK_B), tuple('publication-' + str(n) for n in range(8)), ('1791547200000000',) * 8)
        self.producer = EvolutionAdmissionConfig(Path('/explicit/source'), Path('/explicit/bun'),
            Path('/explicit/git'), 1, 1024, 4194304)
        outer = self
        class Policy:
            def admit_run(self, original, context):
                if context is not outer.driver.context or not outer.driver.held:
                    raise PermissionError('independent held authority required')
                outer.driver.validate_evolution_run(original, context=context)
            def admit_ledger(self, original, inventory, context):
                self.admit_run(original, context)
                if type(inventory.slots) is not tuple or len(inventory.slots) != 8:
                    raise PermissionError('complete original immutable custody required')
            def admit_attempt(self, original, ordinal, plan, context):
                self.admit_run(original, context)
                value = plan.document(); step = original.document()['schedule'][ordinal]
                if value['publication_id'] != step['publication_id']:
                    raise PermissionError('original ordinal binding required')
                if json.loads(value['request']['source_checkpoint_json'])['feed'] != step['source']:
                    raise PermissionError('original source binding required')
        self.policy = Policy(); self.path = self.root / 'run.sqlite'
        self.config = FreshCommerceEvolutionConfig(self.driver, self.path, self.policy,
            self.producer, self.driver.context, self.request)

    def producer_call(self, config, *, source_system, epoch):
        return next(item for item in self.driver.source_admission.admissions if item.prepared.source_system == source_system)

    def native_ports(self):
        from test_stored_publisher import PhaseExecutor
        self.phases = PhaseExecutor(); self.driver.transport.targets[self.driver.tables['attempts']].uuid = 'attempt-uuid'
        self.events = []; self.manifests = []
        def apply(request, context):
            active = self.driver.policy.active
            self.events.append(('apply', active['publication_id']))
            for role, rows in active['expected'].items():
                table = self.driver.tables[role]; self.versions[table] += 1
                self.snapshots[(table, self.versions[table])] = {'rows': sorted(rows, key=text),
                    'row_sha256': hashlib.sha256(text(sorted(rows, key=text)).encode()).hexdigest()}
            manifest = {'publication_id': active['publication_id'], 'profile_version': 'ashlar-delta/0.3',
                'table_versions_json': text({self.driver.tables[role]: self.versions[self.driver.tables[role]] for role in active['expected']}),
                'schema_revisions_json': request['schema_revisions_json'], 'source_progress_json': text(active['progress']),
                'validation_report_json': text({'complete': True, 'request_digest': request['request_digest']}),
                'recorded_at': active['recorded_at']}
            return text({'effects': {'qualification': 'inert original full-run ports'}, 'manifest': manifest})
        self.driver.apply = apply
        self.driver.recover_apply = lambda *args: self.fail('committed replay must not reapply')
        self.driver.validate = lambda *args: None
        self.driver.acknowledge = lambda request, descriptor, context: self.events.append(('ack', descriptor.publication_id))
        outer = self
        class Manifest:
            def commit(self, row, *, context): outer.manifests.append(dict(row)); return dict(row)
            def recover(self, row, *, context): raise AssertionError('no replacement manifest')
        return (patch('ashlar_host.driver.AttemptExecutor', return_value=self.phases),
                patch('ashlar_host.driver.ManifestPort', return_value=Manifest()),
                patch('ashlar_host.evolution_admission.admit_commerce_evolution', side_effect=self.producer_call))

    def test_full_eight_original_steps_and_resume_exact_replay(self):
        first, second, third = self.native_ports()
        with first, second, third:
            result = publish_commerce_evolution(self.config)
            self.driver.policy.active = None
            resumed = resume_commerce_evolution(ResumeCommerceEvolutionConfig(self.driver, self.path,
                self.policy, self.producer, self.driver.context, result['run_sha256']))
        self.assertEqual(result, resumed); self.assertEqual(len(result['publications']), 8)
        self.assertEqual(len(self.manifests), 8); self.assertEqual(len(self.phases.rows), 40)
        self.assertEqual(len([event for event in self.events if event[0] == 'apply']), 8)
        self.assertEqual(len([event for event in self.events if event[0] == 'ack']), 16)
        self.assertEqual([descriptor.publication_id for descriptor in result['publications']], list(self.request.publication_ids))
        self.assertFalse(self.driver.held)

    def test_retention_only_crash_resumes_original_without_plan_regeneration(self):
        first, second, third = self.native_ports()
        original_prepare = LocalDeltaEffects.prepare_unsubmitted
        primary = KeyboardInterrupt('after exact original retention')
        with first, second, third:
            with patch.object(LocalDeltaEffects, 'prepare_unsubmitted', side_effect=primary):
                with self.assertRaises(KeyboardInterrupt) as observed: publish_commerce_evolution(self.config)
            self.assertIs(observed.exception, primary); self.assertEqual(self.events, [])
            with self.driver.writer('original', self.driver.context):
                ledger = EvolutionRunLedger.open(self.path, self.policy, context=self.driver.context,
                    expected_sha256=self.definition_digest())
                self.assertEqual(ledger.slot(0)[0], 'retained')
                raw = ledger.slot(0)[1]; digest = ledger.original.sha256; ledger.close()
            original_plan = self.driver.plan_evolution_transaction
            def plan(original, ordinal, progress, *, context):
                if ordinal == 0: raise AssertionError('original retained plan regenerated')
                return original_plan(original, ordinal, progress, context=context)
            with patch.object(self.driver, 'plan_evolution_transaction', side_effect=plan):
                result = resume_commerce_evolution(ResumeCommerceEvolutionConfig(self.driver, self.path,
                    self.policy, self.producer, self.driver.context, digest))
            with sqlite3.connect(self.path) as connection:
                self.assertEqual(connection.execute('SELECT original FROM original_slot WHERE ordinal=0').fetchone()[0], raw)
        self.assertEqual(len(result['publications']), 8)

    def definition_digest(self):
        with sqlite3.connect(self.path) as connection:
            return connection.execute('SELECT digest FROM original_run').fetchone()[0]

    def test_started_with_lost_whole_effect_plan_refuses_replacement(self):
        first, second, third = self.native_ports()
        primary = KeyboardInterrupt('after original started reservation')
        with first, second, third:
            with patch.object(self.driver, 'apply', side_effect=primary):
                with self.assertRaises(KeyboardInterrupt): publish_commerce_evolution(self.config)
            self.driver.transport.db.execute('DELETE FROM local_plan')
            with self.assertRaises(LocalDeltaError):
                resume_commerce_evolution(ResumeCommerceEvolutionConfig(self.driver, self.path,
                    self.policy, self.producer, self.driver.context, self.definition_digest()))
        self.assertEqual(self.events, []); self.assertFalse(self.driver.held)

    def test_missing_or_corrupt_original_slot_and_replacement_file_refuse(self):
        with self.driver.writer('original', self.driver.context):
            original = self.driver.capture_evolution_run(self.request, context=self.driver.context)
            ledger = EvolutionRunLedger.open(self.path, self.policy, context=self.driver.context, original=original)
            ledger.connection.execute('DELETE FROM original_slot WHERE ordinal=7'); ledger.connection.commit()
            with self.assertRaises(EvolutionPlanError): ledger.slot(0)
            ledger.close()
            with self.assertRaises(EvolutionPlanError):
                EvolutionRunLedger.open(self.path, self.policy, context=self.driver.context, expected_sha256=original.sha256)
            with self.assertRaises(FileExistsError):
                EvolutionRunLedger.open(self.path, self.policy, context=self.driver.context, original=original)
        self.assertFalse(self.driver.held)

    def test_malformed_sequence_and_wal_resume_are_readonly_refusals(self):
        with self.driver.writer('original', self.driver.context):
            original = self.driver.capture_evolution_run(self.request, context=self.driver.context)
            ledger = EvolutionRunLedger.open(self.path, self.policy, context=self.driver.context, original=original)
            plan = self.driver.plan_evolution_transaction(original, 0, {}, context=self.driver.context)
            ledger.retain(0, plan)
            ledger.connection.execute("UPDATE original_slot SET state='retained',original=?,digest=? WHERE ordinal=1", (plan.raw, plan.sha256))
            ledger.connection.commit()
            with self.assertRaises(EvolutionPlanError): ledger.admit()
            ledger.close()
            unrelated = self.root / 'unrelated.sqlite'
            with sqlite3.connect(unrelated) as connection:
                connection.execute('CREATE TABLE unrelated(value TEXT)')
                self.assertEqual(connection.execute('PRAGMA journal_mode=WAL').fetchone()[0], 'wal')
            with self.assertRaises(EvolutionPlanError):
                EvolutionRunLedger.open(unrelated, self.policy, context=self.driver.context, expected_sha256=original.sha256)
            with sqlite3.connect(unrelated) as connection:
                self.assertEqual(connection.execute('PRAGMA journal_mode').fetchone()[0], 'wal')
                self.assertEqual(connection.execute('SELECT count(*) FROM unrelated').fetchone()[0], 0)

    def test_unknown_or_subclass_schedule_and_unknown_options_refuse(self):
        for order in (('A', 'A'), ('A',), ('A', 'B', 'C')):
            with self.assertRaises(EvolutionPlanError):
                EvolutionRunRequest(order, self.request.stream, self.request.predecessor,
                    self.request.clocks, self.request.publication_ids, self.request.recorded_at)
        with self.assertRaises(EvolutionPlanError):
            EvolutionRunRequest(self.request.source_order, self.request.stream, self.request.predecessor,
                (('2026-10-09T12:00:00.1234567+00:00', *CLOCK_A[1:]), CLOCK_B),
                self.request.publication_ids, self.request.recorded_at)
        with self.assertRaises(TypeError): FreshCommerceEvolutionConfig(self.driver, self.path, self.policy,
            self.producer, self.driver.context, self.request, fault=True)
        with self.driver.writer('original', self.driver.context):
            original = self.driver.capture_evolution_run(self.request, context=self.driver.context)
            value = original.document(); value['schedule'][0]['prefix'] = True
            with self.assertRaises(EvolutionPlanError): EvolutionRunDefinition(encoded(value))
        self.assertFalse(self.path.exists())

    def test_positive_reservation_still_refuses_known_ordinal_or_named_commit(self):
        with self.driver.writer('original', self.driver.context):
            original = self.driver.capture_evolution_run(self.request, context=self.driver.context)
            ledger = EvolutionRunLedger.open(self.path, self.policy, context=self.driver.context, original=original)
            plan = self.driver.plan_evolution_transaction(original, 0, {}, context=self.driver.context)
            ledger.retain(0, plan); journal = ledger.attempt(0)
            self.driver.policy.activate_evolution(plan, self.driver.context, journal.policy)
            value = plan.document(); digest = value['request']['request_digest']; effects = LocalDeltaEffects(self.driver.transport)
            def prepare():
                return effects.prepare_unsubmitted('effects:' + digest, digest, value['selected_steps'],
                    reservation=journal, plan=plan, context=self.driver.context)
            self.driver.transport.db.execute('INSERT INTO local_operation VALUES(?,?,?,?,?,?)',
                (value['operations'][0], text({'operation': value['operations'][0], 'request_digest': digest}),
                 'original', 0, 'submitted', None))
            with self.assertRaises(LocalDeltaError): prepare()
            self.driver.transport.db.execute('DELETE FROM local_operation')
            self.driver.transport.original_history = lambda target: [{'version': 0, 'userMetadata': text({
                'profile': 'ashlar-local-delta-commit/0.1', 'installation_id': self.driver.transport.installation_id,
                'operation': value['operations'][0], 'request_digest': digest})}]
            with self.assertRaises(LocalDeltaError): prepare()
            self.assertEqual(self.driver.transport.db.execute('SELECT count(*) FROM local_plan').fetchone()[0], 0)
            self.assertEqual(ledger.slot(0)[0], 'retained'); ledger.close()

    def test_started_intact_original_plan_recovers_without_reapplying(self):
        first, second, third = self.native_ports(); apply = self.driver.apply
        primary = OSError('lost original apply response')
        def lost(request, context):
            artifact = apply(request, context)
            self.driver.transport.db.execute('INSERT INTO local_publication_artifact VALUES(?,?,?)',
                (request['request_digest'], text(request), artifact))
            self.driver.transport.db.commit()
            raise primary
        def recover(request, context):
            self.events.append(('recover', self.driver.policy.active['publication_id']))
            row = self.driver.transport.db.execute('SELECT request,artifact FROM local_publication_artifact WHERE request_digest=?',
                (request['request_digest'],)).fetchone()
            if row is None or row[0] != text(request): raise AssertionError('lost original artifact')
            return row[1]
        with first, second, third:
            self.driver.apply = lost
            with self.assertRaises(OSError) as observed: publish_commerce_evolution(self.config)
            self.assertIs(observed.exception, primary)
            self.driver.apply = apply; self.driver.recover_apply = recover
            result = resume_commerce_evolution(ResumeCommerceEvolutionConfig(self.driver, self.path,
                self.policy, self.producer, self.driver.context, self.definition_digest()))
        self.assertEqual(len(result['publications']), 8)
        self.assertEqual(len([event for event in self.events if event[0] == 'apply']), 8)
        self.assertEqual([event for event in self.events if event[0] == 'recover'], [('recover', 'publication-0')])
        self.assertEqual(len(self.manifests), 8); self.assertFalse(self.driver.held)

    def test_producer_or_lineage_refusal_and_cleanup_cancellation_withhold(self):
        first, second, third = self.native_ports()
        primary = KeyboardInterrupt('ledger cleanup'); original_close = EvolutionRunLedger.close
        def close(ledger):
            original_close(ledger)
            raise primary
        with first, second:
            with patch('ashlar_host.evolution_admission.admit_commerce_evolution', side_effect=PermissionError('current producer')):
                with patch.object(EvolutionRunLedger, 'close', close):
                    with self.assertRaises(KeyboardInterrupt) as observed: publish_commerce_evolution(self.config)
        self.assertIs(observed.exception, primary); self.assertEqual(self.events, []); self.assertFalse(self.driver.held)
        with self.driver.writer('original', self.driver.context):
            previous = self.policy.admit_ledger
            self.policy.admit_ledger = lambda *args: True
            with self.assertRaises(EvolutionPlanError):
                EvolutionRunLedger.open(self.path, self.policy, context=self.driver.context, expected_sha256=self.definition_digest())
            self.policy.admit_ledger = previous

    def test_closed_config_and_missing_resume_never_initialize(self):
        with self.assertRaises(HostError): ResumeCommerceEvolutionConfig(self.driver, self.path,
            self.policy, self.producer, self.driver.context, 'replacement')
        with self.assertRaises(EvolutionPlanError): resume_commerce_evolution(self.config)
        with self.assertRaises(FileNotFoundError):
            resume_commerce_evolution(ResumeCommerceEvolutionConfig(self.driver, self.path,
                self.policy, self.producer, self.driver.context, 'a' * 64))
        self.assertFalse(self.path.exists()); self.assertFalse(self.driver.held)


if __name__ == '__main__': unittest.main()
