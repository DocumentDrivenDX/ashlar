"""Original native gate guard controls with inert query/session ports; no native claim."""
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
import unittest
import test_evolution_run as run
from ashlar_host.evolution_run import EvolutionRunLedgerView
from ashlar_host.evolution_composition import NativeEvolutionRunPolicy


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): run.Tests.setUpClass()

    def setUp(self):
        self.host = run.Tests(); self.host.setUp(); self.addCleanup(self.host.doCleanups)
        self.driver = self.host.driver; self.driver.held = True
        self.original = self.driver.capture_evolution_run(self.host.request, context=self.driver.context)
        self.empty = [(index, 'unprepared', None, None, None) for index in range(8)]
        self.reads = []
        outer = self
        class Store:
            def __init__(self, executor, policy, table, uuid, *, original_request):
                self.request = original_request
            @contextmanager
            def session(self, context):
                outer.reads.append(self.request)
                yield SimpleNamespace(read=lambda stream, batch: ())
        self.patch = patch('ashlar.attempt_store.DeltaAttemptStore', Store)
        self.patch.start(); self.addCleanup(self.patch.stop)
        self.admission = SimpleNamespace(admit=lambda *args: None)

    def admit(self, slots):
        return self.driver.admit_evolution_run_ledger(self.original, EvolutionRunLedgerView(tuple(slots)),
            context=self.driver.context, attempt_admission=self.admission)

    def test_empty_original_inventory_observes_all_eight_qualified_scopes(self):
        self.admit(self.empty)
        import json
        self.assertEqual([json.loads(item['source_checkpoint_json'])['feed'] for item in self.reads], ['A','B'] * 4)
        self.assertEqual([json.loads(item['source_checkpoint_json'])['position'] for item in self.reads], ['1','1','2','2','3','3','4','4'])
        self.assertIsNone(self.driver.policy.active)

    def test_malformed_complete_inventory_refuses_before_native_session(self):
        for slots in (self.empty[:-1], [(False, *self.empty[0][1:]), *self.empty[1:]],
                      [(0, 'unprepared', b'foreign', None, None), *self.empty[1:]]):
            with self.assertRaises(PermissionError): self.admit(slots)
            self.assertEqual(self.reads, [])

    def test_started_missing_original_effect_plan_refuses_and_restores_active(self):
        plan = self.driver.plan_evolution_transaction(self.original, 0, {}, context=self.driver.context)
        marker = {'original': 'active'}; self.driver.policy.active = marker
        slots = [(0, 'publication-started', plan.raw, plan.sha256, None), *self.empty[1:]]
        from ashlar_host.delta_custody import LocalDeltaError
        with self.assertRaises(LocalDeltaError): self.admit(slots)
        self.assertIs(self.driver.policy.active, marker)
        self.assertEqual(self.driver.transport.db.execute('SELECT count(*) FROM local_plan').fetchone()[0], 0)

    def test_not_started_refuses_original_ordinal_or_named_commit_without_phase_or_plan(self):
        import json
        plan = self.driver.plan_evolution_transaction(self.original, 0, {}, context=self.driver.context)
        value = plan.document(); key = value['operations'][0]
        inventories = [self.empty, [(0, 'retained', plan.raw, plan.sha256, None), *self.empty[1:]]]
        for slots in inventories:
            self.driver.transport.db.execute('INSERT INTO local_operation VALUES(?,?,?,?,?,?)',
                (key, json.dumps({'operation': key, 'request_digest': 'different'}), 'untrusted', 0, 'submitted', None))
            with self.assertRaises(PermissionError): self.admit(slots)
            self.driver.transport.db.execute('DELETE FROM local_operation')
            original_history = self.driver.transport.original_history
            self.driver.transport.original_history = lambda target: [{'version': 0, 'userMetadata': json.dumps({
                'profile': 'ashlar-local-delta-commit/0.1', 'installation_id': self.driver.transport.installation_id,
                'operation': key, 'request_digest': 'different'})}]
            with self.assertRaises(PermissionError): self.admit(slots)
            self.driver.transport.original_history = original_history
        self.assertEqual(self.driver.transport.db.execute('SELECT count(*) FROM local_plan').fetchone()[0], 0)

    def test_independent_current_reservation_refusal_precedes_native_observation(self):
        error = KeyboardInterrupt('original cancellation')
        def refuse(*args): raise error
        authority = SimpleNamespace(admit_run=refuse, admit_ledger=refuse, admit_attempt=refuse)
        policy = NativeEvolutionRunPolicy(self.driver, authority)
        with self.assertRaises(KeyboardInterrupt) as observed:
            policy.admit_ledger(self.original, EvolutionRunLedgerView(tuple(self.empty)), self.driver.context)
        self.assertIs(observed.exception, error); self.assertEqual(self.reads, [])

    def test_completed_original_requires_each_handle_and_protected_ack_readback(self):
        import hashlib, json
        from ashlar.attempt_store import PhaseRecord, PHASES
        from ashlar_host.driver import encoded
        from ashlar_host.delta_custody import LocalDeltaEffects
        from ashlar_host.ack_sessions import RegisteredOutboxAcks
        plan = self.driver.plan_evolution_transaction(self.original, 0, {}, context=self.driver.context)
        value = plan.document(); request = value['request']; digest = request['request_digest']
        source = {'request': request, 'generated_steps': value['generated_steps'],
            'selected_steps': value['selected_steps'], 'zero_match_elisions': value['zero_match_elisions'],
            'complete_prior_oracle': value['previous_expected'], 'observed_native_prior': value['observed_native_prior']}
        for role, rows in value['expected'].items():
            table = self.driver.tables[role]; self.host.versions[table] = 1
            self.host.snapshots[(table, 1)] = {'rows': sorted(rows, key=encoded), 'row_sha256': 'a' * 64}
        row = {'publication_id': value['publication_id'], 'profile_version': 'ashlar-delta/0.3',
            'table_versions_json': encoded({self.driver.tables[role]: 1 for role in value['expected']}),
            'schema_revisions_json': request['schema_revisions_json'], 'source_progress_json': encoded(value['progress']),
            'validation_report_json': encoded({'complete': True, 'request_digest': digest,
                'predecessor': request['predecessor'], 'source_admission': value['source_admission']}),
            'recorded_at': value['recorded_at']}
        artifact = encoded({'effects': {'original_source_plan_sha256': hashlib.sha256(encoded(source).encode()).hexdigest()}, 'manifest': row})
        records = []
        for index, phase in enumerate(PHASES):
            payload = encoded({'request': request, 'result_json': artifact if index >= 2 else None,
                               'descriptor_json': encoded(row) if index == 4 else None})
            records.append(PhaseRecord(phase, digest, payload, hashlib.sha256(payload.encode()).hexdigest()))
        handles = []
        for key, step in zip(value['operations'], value['selected_steps']):
            handles.append((key, {'request_digest': digest, 'table': self.driver.tables['object_current'], **step}))
        for record in records:
            phase_row = {'stream': request['stream'], 'batch_id': request['batch_id'], **vars(record)}
            handles.append(('phase-' + record.phase, {'request_digest': digest, 'table': self.driver.tables['attempts'],
                'statement': 'inert phase', 'parameters': {'payload': encoded(phase_row)}}))
        handles.append(('manifest-original', {'request_digest': digest, 'table': self.driver.tables['manifest'],
            'statement': 'inert manifest', 'parameters': {'row': encoded(row)}}))
        db = self.driver.transport.db
        for key, intent in handles: db.execute('INSERT INTO local_operation VALUES(?,?,?,?,?,?)',
            (key, encoded(intent), 'inert', 0, 'committed', 'inert'))
        db.execute('INSERT INTO local_source_plan VALUES(?,?)', (digest, encoded(source)))
        db.execute('INSERT INTO local_publication_artifact VALUES(?,?,?)', (digest, encoded(request), artifact))
        slots = [(0, 'completed', plan.raw, plan.sha256, encoded(row).encode()), *self.empty[1:]]
        self.patch.stop()
        outer = self
        class Store:
            def __init__(self, *args, original_request): self.request = original_request
            @contextmanager
            def session(self, context):
                yield SimpleNamespace(read=lambda *args: tuple(records) if self.request == request else ())
        inspected = []; self.driver.transport.inspect_committed = lambda key, *args, **kwargs: inspected.append(key)
        self.driver.descriptors = lambda publication: [dict(row)] if publication == row['publication_id'] else []
        with patch('ashlar.attempt_store.DeltaAttemptStore', Store), patch.object(LocalDeltaEffects, 'observe',
                return_value={'states': ['committed'] * len(value['operations'])}), patch.object(
                RegisteredOutboxAcks, 'reconcile', return_value=b'exact inert receipt') as ack:
            self.admit(slots)
            self.assertEqual(set(inspected), {key for key, _ in handles}); self.assertEqual(ack.call_count, 1)
            ack.return_value = None
            with self.assertRaises(PermissionError): self.admit(slots)
            ack.return_value = b'exact inert receipt'
            db.execute('DELETE FROM local_operation WHERE operation=?', (handles[0][0],))
            with self.assertRaises(PermissionError): self.admit(slots)
        self.assertIsNone(self.driver.policy.active)

if __name__ == '__main__': unittest.main()
