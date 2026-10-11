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

    def retained_ledger(self):
        from ashlar_host.evolution_run import EvolutionRunLedger
        policy = NativeEvolutionRunPolicy(self.driver, self.host.policy)
        ledger = EvolutionRunLedger.open(self.host.path, policy,
            context=self.driver.context, original=self.original)
        self.addCleanup(ledger.close)
        plan = self.driver.plan_evolution_transaction(self.original, 0, {}, context=self.driver.context)
        ledger.retain(0, plan)
        return ledger, plan

    def after_opening_callback(self, ledger, effect):
        # The public native policy stays real and frozen; inject only its
        # independently supplied authority's callback after full opening.
        if not hasattr(ledger, '_test_original_admission'):
            ledger._test_original_admission = ledger._admitted_inventory
            ledger._test_original_authority = self.host.policy.admit_attempt
        gate = {'opened': False}
        def opening():
            gate['opened'] = False
            view = ledger._test_original_admission()
            gate['opened'] = True
            return view
        def authority(*args):
            ledger._test_original_authority(*args)
            if gate['opened']: effect(*args)
        ledger._admitted_inventory = opening
        self.host.policy.admit_attempt = authority

    def test_load_one_full_admission_and_current_attempt_policy(self):
        ledger, plan = self.retained_ledger()
        history = self.driver.transport.original_history
        full = self.driver.admit_evolution_run_ledger
        attempt = self.host.policy.admit_attempt
        events = []
        def histories(target):
            events.append('history'); return history(target)
        def admission(*args, **kwargs):
            events.append('full'); return full(*args, **kwargs)
        def current_attempt(*args):
            events.append('attempt'); return attempt(*args)
        self.driver.transport.original_history = histories
        self.driver.admit_evolution_run_ledger = admission
        self.host.policy.admit_attempt = current_attempt
        loaded = ledger.attempt(0).load(expected_sha256=plan.sha256, context=self.driver.context)
        self.assertEqual(loaded.raw, plan.raw)
        self.assertEqual(events.count('full'), 1)
        self.assertEqual(events.count('history'), 16)
        self.assertGreaterEqual(events.count('attempt'), 1)
        events.clear(); self.assertIsNone(ledger.admit())
        self.assertEqual(events.count('full'), 1)
        events.clear(); ledger.slot(0)
        self.assertEqual(events.count('full'), 1)
        events.clear(); ledger.attempt(0).admit(plan, self.driver.context)
        self.assertEqual(events.count('full'), 1)
        self.driver.admit_evolution_run_ledger = lambda *args, **kwargs: self.fail('wrong context entered native admission')
        from ashlar_host.evolution_plan import EvolutionPlanError
        with self.assertRaises(EvolutionPlanError):
            ledger.attempt(0).load(expected_sha256=plan.sha256, context=object())

    def test_load_opening_callback_cannot_bless_changed_inventory(self):
        from ashlar_host.evolution_plan import EvolutionPlanError
        ledger, plan = self.retained_ledger()
        original = self.host.policy.admit_ledger
        def mutate(*args):
            original(*args)
            ledger.connection.execute("UPDATE original_slot SET descriptor=? WHERE ordinal=7", (b'foreign',))
        self.host.policy.admit_ledger = mutate
        with self.assertRaises(EvolutionPlanError):
            ledger.attempt(0).load(expected_sha256=plan.sha256, context=self.driver.context)

    def test_load_closing_catches_callback_unrelated_slot_or_schema_change(self):
        from ashlar_host.evolution_plan import EvolutionPlanError
        ledger, plan = self.retained_ledger()
        def change_slot(*args):
            ledger.connection.execute("UPDATE original_slot SET descriptor=? WHERE ordinal=7", (b'foreign',))
        self.after_opening_callback(ledger, change_slot)
        with self.assertRaises(EvolutionPlanError):
            ledger.attempt(0).load(expected_sha256=plan.sha256, context=self.driver.context)
        ledger.connection.execute('UPDATE original_slot SET descriptor=NULL WHERE ordinal=7')
        def change_schema(*args):
            ledger.connection.execute('CREATE TABLE foreign_table(id INTEGER)')
        self.after_opening_callback(ledger, change_schema)
        with self.assertRaises(EvolutionPlanError):
            ledger.attempt(0).load(expected_sha256=plan.sha256, context=self.driver.context)
        ledger.connection.execute('DROP TABLE foreign_table')
        def cancel_after_change(*args):
            ledger.connection.execute("UPDATE original_slot SET descriptor=? WHERE ordinal=7", (b'foreign',))
            raise primary
        primary = KeyboardInterrupt('current callback cancellation')
        self.after_opening_callback(ledger, cancel_after_change)
        with self.assertRaises(KeyboardInterrupt) as observed:
            ledger.attempt(0).load(expected_sha256=plan.sha256, context=self.driver.context)
        self.assertIs(observed.exception, primary)

    def test_load_callback_file_or_parent_replacement_refuses(self):
        import os
        from ashlar_host.evolution_plan import EvolutionPlanError
        ledger, plan = self.retained_ledger()
        path = ledger.path; saved = path.with_name('original-saved.sqlite')
        def replace_file(*args):
            path.rename(saved); path.write_bytes(saved.read_bytes())
        self.after_opening_callback(ledger, replace_file)
        with self.assertRaises(EvolutionPlanError):
            ledger.attempt(0).load(expected_sha256=plan.sha256, context=self.driver.context)
        path.unlink(); saved.rename(path)
        parent = path.parent; old_parent = parent.with_name(parent.name + '-saved')
        def replace_parent(*args):
            parent.rename(old_parent); parent.mkdir(mode=0o700)
            os.link(old_parent / path.name, path)
        self.after_opening_callback(ledger, replace_parent)
        try:
            with self.assertRaises(EvolutionPlanError):
                ledger.attempt(0).load(expected_sha256=plan.sha256, context=self.driver.context)
        finally:
            path.unlink(); parent.rmdir(); old_parent.rename(parent)

    def test_load_rejected_digest_renews_and_current_authority_revocation_refuses(self):
        from ashlar_host.evolution_plan import EvolutionPlanError
        ledger, plan = self.retained_ledger()
        inventory = ledger._inventory; reads = []
        def observe_inventory():
            reads.append('inventory'); return inventory()
        ledger._inventory = observe_inventory
        with self.assertRaises(EvolutionPlanError):
            ledger.attempt(0).load(expected_sha256='f' * 64, context=self.driver.context)
        self.assertEqual(len(reads), 2)
        with self.assertRaises(EvolutionPlanError):
            ledger.attempt(1).load(expected_sha256='f' * 64, context=self.driver.context)
        def revoked(*args):
            self.driver.held = False
            raise PermissionError('Actual held authority revoked')
        self.after_opening_callback(ledger, revoked)
        try:
            with self.assertRaises(PermissionError):
                ledger.attempt(0).load(expected_sha256=plan.sha256, context=self.driver.context)
        finally: self.driver.held = True

    def test_empty_original_inventory_observes_all_eight_qualified_scopes(self):
        self.admit(self.empty)
        import json
        self.assertEqual([json.loads(item['source_checkpoint_json'])['feed'] for item in self.reads], ['A','B'] * 4)
        self.assertEqual([json.loads(item['source_checkpoint_json'])['position'] for item in self.reads], ['1','1','2','2','3','3','4','4'])
        self.assertIsNone(self.driver.policy.active)

    def test_single_admission_reuses_complete_histories_with_timestamp_and_closing(self):
        from datetime import datetime
        calls = []
        def history(target):
            calls.append(target.table)
            return [{'version': 0, 'timestamp': datetime(2026, 10, 11),
                     'operationMetrics': {'unknown_metric': 'retained'}}]
        self.driver.transport.original_history = history
        self.admit(self.empty)
        tables = set(self.driver.transport.targets)
        self.assertEqual(len(calls), 2 * len(tables) + 4)
        self.assertEqual(set(calls), tables)
        graph_tables = {self.driver.tables[role] for role in self.driver.source_admission.columns}
        self.assertTrue(all(calls.count(table) == (3 if table in graph_tables else 2) for table in tables))
        self.assertEqual(len(self.reads), 8)

    def test_closing_history_numeric_extension_or_uuid_drift_refuses(self):
        from datetime import datetime, timezone, timedelta
        from ashlar_host.delta_custody import LocalDeltaError
        original_detail = self.driver.transport._detail
        for kind in ('number', 'extension', 'uuid', 'fold', 'timezone'):
            calls = {}
            def history(target):
                calls[target.table] = calls.get(target.table, 0) + 1
                row = {'version': 0, 'timestamp': datetime(2026, 10, 11), 'extension': 'original'}
                if kind == 'timezone':
                    row['timestamp'] = datetime(2026, 10, 11, tzinfo=timezone.utc)
                if calls[target.table] == 2:
                    if kind == 'fold': row['timestamp'] = row['timestamp'].replace(fold=1)
                    if kind == 'timezone': row['timestamp'] = row['timestamp'].replace(tzinfo=timezone(timedelta(0), 'foreign-name'))
                    if kind == 'number': row['version'] = False
                    if kind == 'extension': row['extension'] = 'changed'
                return [row]
            def detail(target):
                if kind == 'uuid' and calls.get(target.table, 0) == 1:
                    raise LocalDeltaError('Original native Delta UUID changed')
                return original_detail(target)
            self.driver.transport.original_history = history
            self.driver.transport._detail = detail
            with self.assertRaises((PermissionError, LocalDeltaError)): self.admit(self.empty)
            self.assertIsNone(self.driver.policy.active)
        self.driver.transport._detail = original_detail

    def test_named_commit_any_role_refuses_and_closing_runs_on_cancellation(self):
        import json
        from ashlar_host.evolution_composition import OriginalRunCorrespondence
        request = OriginalRunCorrespondence(self.original,
            self.driver.evolution_sources(context=self.driver.context)).at(7)['request']
        for selected in self.driver.transport.targets:
            calls = []
            def history(target):
                calls.append(target.table)
                marker = {'profile': 'ashlar-local-delta-commit/0.1',
                    'installation_id': self.driver.transport.installation_id,
                    'request_digest': request['request_digest']}
                return [{'version': 0, 'userMetadata': json.dumps(marker) if target.table == selected else None}]
            self.driver.transport.original_history = history
            with self.assertRaises(PermissionError): self.admit(self.empty)
            self.assertEqual(len(calls), 2 * len(self.driver.transport.targets))
        primary = KeyboardInterrupt('body cancellation')
        calls = []
        def history(target):
            calls.append(target.table)
            if len(calls) > len(self.driver.transport.targets):
                raise ValueError('closing history failure')
            return [{'version': 0}]
        self.driver.transport.original_history = history
        original_plan_graph = self.driver.plan_graph
        self.driver.plan_graph = lambda *args, **kwargs: (_ for _ in ()).throw(primary)
        try:
            with self.assertRaises(KeyboardInterrupt) as observed: self.admit(self.empty)
            self.assertIs(observed.exception, primary)
            self.assertGreater(len(calls), len(self.driver.transport.targets))
        finally:
            self.driver.plan_graph = original_plan_graph

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
        from ashlar_host.evolution_run import EvolutionRunLedger
        ledger = EvolutionRunLedger.open(self.host.path,
            NativeEvolutionRunPolicy(self.driver, self.host.policy),
            context=self.driver.context, original=self.original)
        self.addCleanup(ledger.close)
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
        identities = []; closing_error = []; session_closing = []
        class Store:
            def __init__(self, *args, original_request): self.request = original_request
            def renew_identity(self, context):
                identities.append('closing-uuid')
                if closing_error: raise closing_error[0]
            @contextmanager
            def session(self, context):
                try:
                    yield SimpleNamespace(read=lambda *args: tuple(records) if self.request == request else ())
                finally:
                    if session_closing: session_closing[0]()
        inspected = []; self.driver.transport.inspect_committed = lambda key, *args, **kwargs: inspected.append(key)
        self.driver.descriptors = lambda publication: [dict(row)] if publication == row['publication_id'] else []
        with patch('ashlar.attempt_store.DeltaAttemptStore', Store), patch.object(LocalDeltaEffects, 'observe',
                return_value={'states': ['committed'] * len(value['operations'])}), patch.object(
                RegisteredOutboxAcks, 'reconcile', return_value=b'exact inert receipt') as ack:
            self.admit(slots)
            self.assertEqual(set(inspected), {key for key, _ in handles}); self.assertEqual(ack.call_count, 1)
            with ledger.connection:
                ledger.connection.execute("UPDATE original_slot SET state='completed',original=?,digest=?,descriptor=? WHERE ordinal=0",
                    (plan.raw, plan.sha256, encoded(row).encode()))
            validator = self.driver._validate_evolution_run_ledger; validations = []
            def count(*args, **kwargs):
                validations.append(kwargs.get('selected_completed')); return validator(*args, **kwargs)
            unchanged = (db.total_changes, ledger.connection.total_changes)
            with patch.object(self.driver, '_validate_evolution_run_ledger', count):
                observed = self.driver.observe_completed_evolution_attempt(ledger.attempt(0),
                    context=self.driver.context, expected_sha256=plan.sha256)
            self.assertEqual(dict(observed.raw), row)
            self.assertEqual(validations, [0]); self.assertEqual(ack.call_count, 2)
            self.assertEqual((db.total_changes, ledger.connection.total_changes), unchanged)
            self.assertEqual(identities, ['closing-uuid'])
            self.assertEqual(ledger.connection.execute('SELECT state FROM original_slot WHERE ordinal=0').fetchone(), ('completed',))
            primary = KeyboardInterrupt('completed validation cancellation')
            def cancelled(*args, **kwargs):
                ledger.connection.execute('CREATE TABLE changed_schema(id INTEGER)')
                raise primary
            with patch.object(self.driver, '_validate_evolution_run_ledger', cancelled):
                with self.assertRaises(KeyboardInterrupt) as caught:
                    self.driver.observe_completed_evolution_attempt(ledger.attempt(0),
                        context=self.driver.context, expected_sha256=plan.sha256)
                self.assertIs(caught.exception, primary)
            ledger.connection.execute('DROP TABLE changed_schema')
            self.assertEqual(identities, ['closing-uuid', 'closing-uuid'])
            closing_error.append(PermissionError('closing attempt UUID drift'))
            with self.assertRaises(PermissionError):
                self.driver.observe_completed_evolution_attempt(ledger.attempt(0),
                    context=self.driver.context, expected_sha256=plan.sha256)
            closing_error.clear()
            session_closing.append(lambda: setattr(self.driver, 'held', False))
            try:
                with self.assertRaises(PermissionError):
                    self.driver.observe_completed_evolution_attempt(ledger.attempt(0),
                        context=self.driver.context, expected_sha256=plan.sha256)
            finally:
                session_closing.clear(); self.driver.held = True
            ack.return_value = None
            with self.assertRaises(PermissionError): self.admit(slots)
            ack.return_value = b'exact inert receipt'
            db.execute('DELETE FROM local_operation WHERE operation=?', (handles[0][0],))
            with self.assertRaises(PermissionError): self.admit(slots)
        self.assertIsNone(self.driver.policy.active)

if __name__ == '__main__': unittest.main()
