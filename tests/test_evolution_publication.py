"""Original semantic/transaction port controls; no native or installed-run claim."""
import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch
from ashlar.outbox import OutboxTransaction
from ashlar_host.commerce_evolution import (PROFILE, EvolutionAttemptPlan, EvolutionPlanJournal,
    publish_evolution_transaction, resume_evolution_transaction, EvolutionPlanError)
from ashlar_host.config import FreshEvolutionTransactionConfig, ResumeEvolutionTransactionConfig, HostError
from ashlar_host.driver import NativeDriver, PrivatePolicy, request_for, progress_union, local_effect_plan, encoded
from ashlar_host.delta_custody import LocalDeltaEffects
import test_evolution_source_set as original_sources
from test_evolution_source_set import CLOCK_A, CLOCK_B


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        original_sources.Tests.setUpClass()
        cls.sources = original_sources.Tests.sources

    def original(self):
        sources = self.sources; previous = {'A': 0, 'B': 0}; following = {'A': 1, 'B': 0}
        clocks = {'A': CLOCK_A, 'B': CLOCK_B}; batch = sources.admissions[0].prepared.batches[0]
        raw = batch.begin + b''.join(item.raw for item in batch.records) + batch.commit
        transaction = OutboxTransaction('ashlar-postgresql-outbox/0.1', batch.feed, batch.epoch,
            '0', '1', hashlib.sha256(raw).hexdigest(), batch)
        request = request_for('original-stream', transaction, 'original-origin',
            {'A': sources.admissions[0].prepared.schema_revisions[0]})
        driver = object.__new__(NativeDriver); driver.context = object(); driver.held = True
        driver.source_admission = sources; driver.original_admission = encoded(sources.metadata())
        driver.source_sessions = object(); driver.ack_sessions = object(); driver.columns = sources.columns
        driver.tables = {role: 'local.original.' + role for role in (*sources.columns, 'attempts', 'manifest')}
        targets = {table: SimpleNamespace(table=table, uuid='uuid-' + role, path=Path('/original') / role)
                   for role, table in driver.tables.items()}
        snapshots = {table: {'rows': [], 'row_sha256': hashlib.sha256(b'[]').hexdigest()}
                     for role, table in driver.tables.items()}
        driver.transport = SimpleNamespace(targets=targets, installation_id='original-installation',
            operation_capacity=100, _snapshot=lambda target, version: snapshots[target.table],
            _detail=lambda target: None, original_history=lambda target: [{'version': 0}],
            _target=lambda statement: None, _parameters=lambda parameters: None)
        driver.policy = PrivatePolicy(driver.context, tuple(targets.values())); driver.policy.initializing = False
        driver.allowed_changes = sources.changes; driver.source_reads = []
        driver.source_admit = lambda supplied: driver.source_reads.append(dict(supplied))
        graph_tables = {role: driver.tables[role] for role in sources.columns}
        _, generated = driver.plan_graph(sources.state_at(previous), batch, graph_tables, materialized_at=CLOCK_A[0])
        prior = sources.oracle(previous, materialized_at=clocks)
        selected, elisions = local_effect_plan(generated, prior, graph_tables)
        _, _, operations = LocalDeltaEffects(driver.transport).original_plan('effects:' + request['request_digest'],
            request['request_digest'], selected)
        value = dict(profile=PROFILE, request=request, generated_steps=generated, selected_steps=selected,
            zero_match_elisions=elisions, observed_native_prior={table: {'uuid': targets[table].uuid,
            'version': 0, 'snapshot': snapshots[table]} for table in graph_tables.values()},
            publication_id='original-A-1', materialized_at=CLOCK_A[0], recorded_at='1791547200000000',
            previous_expected=prior, expected=sources.oracle(following, materialized_at=clocks),
            previous_progress={}, progress=progress_union({}, json.loads(request['source_checkpoint_json'])),
            schema_state={'previous_prefixes': previous, 'prefixes': following, 'clocks': clocks},
            resource_registry={table: {'uuid': target.uuid, 'path': str(target.path)} for table, target in targets.items()},
            source_admission=sources.metadata(), operations=operations)
        return driver, EvolutionAttemptPlan(encoded(value).encode()), batch

    def test_complete_original_semantics_native_anchors_and_operation_identity(self):
        driver, plan, batch = self.original()
        self.assertEqual(driver.admit_evolution_plan(plan, context=driver.context, fresh=True), batch)
        self.assertEqual(driver.source_reads, [plan.document()['request']])
        for field, replacement in (('operations', ['replacement'] * len(plan.document()['operations'])),
                                   ('recorded_at', 'not-a-clock'), ('source_admission', {'profile': 'replacement'})):
            value = plan.document(); value[field] = replacement
            with self.assertRaises((PermissionError, EvolutionPlanError)):
                driver.admit_evolution_plan(EvolutionAttemptPlan(encoded(value).encode()), context=driver.context, fresh=True)

    def test_oracle_prefix_clock_registry_and_prior_refusal_before_source_read(self):
        for mutation in ('oracle', 'prefix', 'clock', 'registry', 'prior', 'progress'):
            driver, plan, batch = self.original(); value = plan.document()
            if mutation == 'oracle': value['expected']['object_current'][0]['id'] = 'replacement'
            elif mutation == 'prefix': value['schema_state']['prefixes']['B'] = 1
            elif mutation == 'clock': value['schema_state']['clocks']['B'][0] = 'not-UTC'
            elif mutation == 'registry': next(iter(value['resource_registry'].values()))['uuid'] = 'replacement'
            elif mutation == 'prior': next(iter(value['observed_native_prior'].values()))['snapshot']['rows'] = [{'replacement': 'row'}]
            else:
                value['previous_progress']['unconsumed'] = dict(value['progress']['A'])
                value['progress']['unconsumed'] = dict(value['progress']['A'])
            with self.assertRaises((ValueError, PermissionError)):
                driver.admit_evolution_plan(EvolutionAttemptPlan(encoded(value).encode()), context=driver.context, fresh=True)
            self.assertEqual(driver.source_reads, [])

    def test_wrong_numeric_carriers_and_noncanonical_record_clock_refuse(self):
        for replacement in (False, 0.0):
            driver, plan, batch = self.original(); value = plan.document()
            next(iter(value['observed_native_prior'].values()))['version'] = replacement
            with self.assertRaises(PermissionError):
                driver.admit_evolution_plan(EvolutionAttemptPlan(encoded(value).encode()), context=driver.context, fresh=True)
        for replacement in (True, 1.0):
            driver, plan, batch = self.original(); value = plan.document()
            value['schema_state']['prefixes']['A'] = replacement
            with self.assertRaises((PermissionError, ValueError)):
                driver.admit_evolution_plan(EvolutionAttemptPlan(encoded(value).encode()), context=driver.context, fresh=True)
        driver, plan, batch = self.original(); value = plan.document()
        value['source_admission']['sources'][0]['custody']['files'] = float(
            value['source_admission']['sources'][0]['custody']['files'])
        with self.assertRaises(PermissionError):
            driver.admit_evolution_plan(EvolutionAttemptPlan(encoded(value).encode()), context=driver.context, fresh=True)
        for clock in ('١', '01', str(2**63)):
            driver, plan, batch = self.original(); value = plan.document(); value['recorded_at'] = clock
            with self.assertRaises(PermissionError):
                driver.admit_evolution_plan(EvolutionAttemptPlan(encoded(value).encode()), context=driver.context, fresh=True)

    def test_fresh_requires_current_prior_resume_keeps_original_historical_anchor(self):
        driver, plan, batch = self.original()
        driver.transport.original_history = lambda target: [{'version': 9}]
        with self.assertRaises(PermissionError): driver.admit_evolution_plan(plan, context=driver.context, fresh=True)
        self.assertEqual(driver.admit_evolution_plan(plan, context=driver.context, fresh=False), batch)

    def test_public_policy_activation_requires_independent_admission_and_owns_copy(self):
        driver, plan, batch = self.original(); primary = PermissionError('independent policy')
        class Refuse:
            def admit(self, supplied, context): raise primary
        with self.assertRaises(PermissionError) as observed:
            driver.policy.activate_evolution(plan, driver.context, Refuse())
        self.assertIs(observed.exception, primary); self.assertIsNone(driver.policy.active)
        calls = []
        policy = SimpleNamespace(admit=lambda supplied, context: calls.append(supplied.raw))
        driver.policy.activate_evolution(plan, driver.context, policy)
        self.assertEqual(calls, [plan.raw]); value = plan.document(); value['request']['predecessor'] = 'borrowed'
        self.assertNotEqual(value['request'], driver.policy.active['request'])

    def test_actual_stored_publication_retains_before_effects_and_resume_replays_original(self):
        from test_stored_publisher import PhaseExecutor
        driver, plan, batch = self.original(); driver.held = False
        phases = PhaseExecutor(); driver.transport.targets[driver.tables['attempts']].uuid = 'attempt-uuid'
        # Rebind the honest inert registry after changing the attempt-store test UUID.
        value = plan.document(); value['resource_registry'][driver.tables['attempts']]['uuid'] = 'attempt-uuid'
        plan = EvolutionAttemptPlan(encoded(value).encode())
        events = []; manifests = []
        def artifact(request, supplied):
            events.append('apply')
            row = {'publication_id': value['publication_id'], 'profile_version': 'ashlar-delta/0.3',
                'table_versions_json': encoded({driver.tables[role]: 1 for role in driver.columns}),
                'schema_revisions_json': request['schema_revisions_json'],
                'source_progress_json': encoded(value['progress']),
                'validation_report_json': encoded({'complete': True, 'request_digest': request['request_digest']}),
                'recorded_at': value['recorded_at']}
            return encoded({'effects': {'qualification': 'inert stored composition only'}, 'manifest': row})
        driver.apply = artifact
        driver.recover_apply = lambda *args: self.fail('committed replay cannot apply')
        driver.validate = lambda *args: None
        driver.acknowledge = lambda *args: events.append('ack')
        class Manifest:
            def commit(self, row, *, context):
                manifests.append(dict(row)); return dict(row)
            def recover(self, row, *, context): raise AssertionError('committed replay cannot commit')
        with tempfile.TemporaryDirectory() as directory:
            driver.transport.journal_path = Path(directory) / 'native-original.sqlite'
            class Admission:
                def admit(self, supplied, context):
                    if not driver.held or context is not driver.context or supplied.raw != plan.raw:
                        raise PermissionError('exact original held authority')
            journal = EvolutionPlanJournal(Path(directory) / 'attempt.json', Admission())
            def prepare(*args, **kwargs):
                self.assertEqual(journal.path.read_bytes(), plan.raw)
                events.append('prepare-effects')
            def observe(*args, **kwargs):
                self.assertEqual(journal.path.read_bytes(), plan.raw)
                events.append('observe-effects')
            with patch('ashlar_host.driver.AttemptExecutor', return_value=phases), \
                 patch('ashlar_host.driver.ManifestPort', return_value=Manifest()), \
                 patch.object(LocalDeltaEffects, 'prepare', side_effect=prepare), \
                 patch.object(LocalDeltaEffects, 'observe', side_effect=observe):
                first = publish_evolution_transaction(FreshEvolutionTransactionConfig(
                    driver, journal, driver.context, plan.raw))
                driver.policy.active = None
                second = resume_evolution_transaction(ResumeEvolutionTransactionConfig(
                    driver, journal, driver.context, plan.sha256))
            self.assertEqual(first, second); self.assertEqual(len(manifests), 1)
            self.assertEqual(events, ['prepare-effects', 'apply', 'ack', 'observe-effects', 'ack'])
            self.assertFalse(driver.held); self.assertEqual(journal.path.read_bytes(), plan.raw)
            self.assertEqual([row['phase'] for row in phases.rows],
                ['prepared', 'applying', 'applied', 'committing', 'committed'])
            # Retained transaction bytes never authorize recreation of lost effect custody.
            with patch('ashlar_host.driver.AttemptExecutor', return_value=phases), \
                 patch.object(LocalDeltaEffects, 'observe', side_effect=PermissionError('missing original whole plan')):
                with self.assertRaises(PermissionError):
                    resume_evolution_transaction(ResumeEvolutionTransactionConfig(
                        driver, journal, driver.context, plan.sha256))
            self.assertFalse(driver.held); self.assertEqual(len(manifests), 1)
            self.assertEqual(journal.path.read_bytes(), plan.raw)
            # An already committed replay still cannot release success after closing custody changes.
            def changed_after_ack(*args): journal.path.write_bytes(b'changed original custody')
            driver.acknowledge = changed_after_ack
            with patch('ashlar_host.driver.AttemptExecutor', return_value=phases), \
                 patch.object(LocalDeltaEffects, 'observe', side_effect=observe):
                with self.assertRaises(EvolutionPlanError):
                    resume_evolution_transaction(ResumeEvolutionTransactionConfig(
                        driver, journal, driver.context, plan.sha256))
            self.assertFalse(driver.held); self.assertEqual(len(manifests), 1)


    def test_distinct_configs_public_routing_and_no_ambient_initialization(self):
        driver, plan, batch = self.original(); calls = []
        port = SimpleNamespace(publish_evolution_attempt=lambda journal, **arguments: calls.append(arguments) or 'descriptor')
        with tempfile.TemporaryDirectory() as directory:
            journal = EvolutionPlanJournal(Path(directory) / 'original.json', SimpleNamespace(admit=lambda plan, context: None))
            fresh = FreshEvolutionTransactionConfig(port, journal, driver.context, plan.raw)
            resume = ResumeEvolutionTransactionConfig(port, journal, driver.context, plan.sha256)
            self.assertEqual(publish_evolution_transaction(fresh), 'descriptor')
            self.assertEqual(resume_evolution_transaction(resume), 'descriptor')
            self.assertEqual(calls[0]['original_plan'].raw, plan.raw)
            self.assertNotIn('original_plan', calls[1]); self.assertEqual(calls[1]['expected_sha256'], plan.sha256)
            with self.assertRaises(EvolutionPlanError): publish_evolution_transaction(resume)
            with self.assertRaises(EvolutionPlanError): resume_evolution_transaction(fresh)
            with self.assertRaises(HostError): ResumeEvolutionTransactionConfig(port, journal, driver.context, 'replacement')
            self.assertFalse(journal.path.exists())


if __name__ == '__main__': unittest.main()
