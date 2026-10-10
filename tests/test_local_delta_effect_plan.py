"""Actual owned SQLite/transport ports with an inert engine; no native qualification."""
from contextlib import contextmanager
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from ashlar_host.delta_custody import (DeltaTarget, LocalDeltaTransport, LocalDeltaEffects,
    LocalDeltaError, LocalDeltaUncertain, original_operation_intent, sha)

TABLE = 'local.runtime.items'
SQL = 'INSERT INTO `local`.`runtime`.`items` VALUES (:id)'
STEPS = [{'statement': SQL, 'parameters': {'id': '1'}},
         {'statement': SQL, 'parameters': {'id': '2'}}]


class Policy:
    @contextmanager
    def writer(self, operation, context):
        if context != 'held': raise PermissionError('held original writer required')
        yield
    def admit(self, intent, context):
        if context != 'held': raise PermissionError('held original writer required')


class Row(dict):
    def asDict(self): return dict(self)


class Engine:
    def __init__(self):
        self.values = {'spark.sql.session.timeZone': 'UTC', 'spark.sql.ansi.enabled': 'true'}
        self.conf = SimpleNamespace(get=lambda k, default=None: self.values.get(k, default),
            set=lambda k, v: self.values.update({k: v}), unset=lambda k: self.values.pop(k, None))
        self.history = [{'version': 0, 'operation': 'WRITE', 'userMetadata': None}]
        self.properties = {}; self.writes = 0; self.fail = False
        self.read = SimpleNamespace(format=lambda _: SimpleNamespace(option=lambda *a:
            SimpleNamespace(load=lambda _: SimpleNamespace(count=lambda: 0))))
    def sql(self, statement, args=None):
        if statement.startswith('DESCRIBE DETAIL'):
            return SimpleNamespace(first=lambda: Row(id='original-uuid', properties=self.properties))
        if statement.startswith('DESCRIBE HISTORY'):
            return SimpleNamespace(limit=lambda _: SimpleNamespace(collect=lambda: [Row(r) for r in self.history]))
        if statement.startswith('ALTER TABLE'):
            import re
            self.properties = dict(re.findall("'([^']+)'='([^']+)'", statement))
        else:
            self.writes += 1
            if self.fail: raise OSError('uncertain original submission')
        self.history.insert(0, {'version': self.history[0]['version'] + 1, 'operation': 'WRITE',
            'userMetadata': self.values.get('spark.databricks.delta.commitInfo.userMetadata')})
        return SimpleNamespace(collect=lambda: [])


class Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); (self.root / 'table').mkdir()
        self.engine = Engine(); self.target = DeltaTarget(TABLE, self.root / 'table', 'original-uuid')
        self.transport = LocalDeltaTransport.initialize(self.engine, self.root / 'original.sqlite',
            'original-installation', (self.target,), Policy(), context='held')
        self.addCleanup(self.transport.close)
        self.transport._history = lambda _: list(self.engine.history)
        self.transport._snapshot = lambda target, version: {'schema': [['id', 'string']], 'rows': [], 'row_sha256': 'a' * 64}
        self.effects = LocalDeltaEffects(self.transport)

    def prepare(self, steps=STEPS):
        return self.effects.prepare('whole-plan', 'b' * 64, steps, context='held')

    def observe(self, steps=STEPS):
        return self.effects.observe('whole-plan', 'b' * 64, steps, context='held')

    def test_prepare_retains_exact_whole_original_without_engine_mutation(self):
        prepared = self.prepare(); self.assertEqual(self.engine.writes, 0)
        self.assertEqual(prepared, self.prepare())
        self.assertEqual(self.observe()['states'], ['no-operation-record', 'no-operation-record'])
        self.assertEqual(len(set(prepared['operation_ids'])), 2)
        original, digest = self.transport.db.execute('SELECT original,digest FROM local_plan').fetchone()
        self.assertEqual(json.loads(original)['steps'], STEPS); self.assertEqual(digest, prepared['plan_sha256'])
        with self.assertRaises(LocalDeltaError): self.prepare(list(reversed(STEPS)))
        self.assertEqual(self.engine.writes, 0)

    def test_invalid_identity_and_changed_intent_refuse_before_retention(self):
        for operation, digest in (('', 'b' * 64), ('a' * 1025, 'b' * 64), ('whole', 'B' * 64), ('whole', 'short')):
            with self.assertRaises(LocalDeltaError):
                self.effects.prepare(operation, digest, STEPS, context='held')
        self.assertEqual(self.transport.db.execute('SELECT count(*) FROM local_plan').fetchone()[0], 0)
        with self.assertRaises(PermissionError): self.effects.prepare('whole', 'b' * 64, STEPS, context='wrong')
        self.assertEqual(self.engine.writes, 0)

    def test_missing_plan_never_reconstructed_by_observation_or_recovery(self):
        for method in (self.effects.observe, self.effects.recover):
            with self.assertRaises(LocalDeltaError): method('whole-plan', 'b' * 64, STEPS, context='held')
        self.assertEqual(self.transport.db.execute('SELECT count(*) FROM local_plan').fetchone()[0], 0)
        self.assertEqual(self.engine.writes, 0)

    def test_recovery_routes_retained_original_and_only_first_submits_missing_ordinal(self):
        self.prepare()
        key = self.prepare()['operation_ids'][0]
        self.transport.mutation(key, SQL, STEPS[0]['parameters'], intent_digest='b' * 64, context='held')
        self.assertEqual(self.observe()['states'], ['committed', 'no-operation-record'])
        with patch.object(self.transport, 'recover', wraps=self.transport.recover) as recover, patch.object(
                self.transport, 'mutation', wraps=self.transport.mutation) as mutate:
            self.effects.recover('whole-plan', 'b' * 64, STEPS, context='held')
        self.assertEqual(recover.call_args.args[0], key)
        self.assertEqual(mutate.call_args.args[0], self.prepare()['operation_ids'][1])
        self.assertEqual(self.engine.writes, 2)
        self.assertEqual(self.observe()['states'], ['committed', 'committed'])

    def test_submitted_absent_commit_remains_uncertain_without_replacement(self):
        self.prepare(); self.engine.fail = True
        with self.assertRaises(OSError): self.effects.recover('whole-plan', 'b' * 64, STEPS, context='held')
        self.assertEqual(self.observe()['states'], ['submitted', 'no-operation-record'])
        self.engine.fail = False
        with patch.object(self.transport, 'mutation', wraps=self.transport.mutation) as mutate:
            with self.assertRaises(LocalDeltaUncertain): self.effects.recover('whole-plan', 'b' * 64, STEPS, context='held')
        mutate.assert_not_called(); self.assertEqual(self.engine.writes, 1)

    def test_lost_whole_plan_with_original_ordinal_never_recreated(self):
        self.prepare(); key = self.prepare()['operation_ids'][0]
        self.transport.mutation(key, SQL, STEPS[0]['parameters'], intent_digest='b' * 64, context='held')
        with self.transport.db: self.transport.db.execute('DELETE FROM local_plan WHERE operation=?', ('whole-plan',))
        for method in (self.effects.prepare, self.effects.run, self.effects.recover, self.effects.observe):
            with self.subTest(method=method.__name__):
                with self.assertRaises(LocalDeltaError): method('whole-plan', 'b' * 64, STEPS, context='held')
                self.assertEqual(self.transport.db.execute('SELECT count(*) FROM local_plan').fetchone()[0], 0)
        self.assertEqual(self.engine.writes, 1)

    def test_missing_ordinal_with_named_native_commit_refuses_replacement(self):
        self.prepare(); key = self.prepare()['operation_ids'][0]
        self.transport.mutation(key, SQL, STEPS[0]['parameters'], intent_digest='b' * 64, context='held')
        with self.transport.db: self.transport.db.execute('DELETE FROM local_operation WHERE operation=?', (key,))
        with self.assertRaises(LocalDeltaError): self.effects.recover('whole-plan', 'b' * 64, STEPS, context='held')
        self.assertEqual(self.engine.writes, 1)

    def test_prepared_but_unsubmitted_original_enters_recovery_port(self):
        self.prepare(); key = self.prepare()['operation_ids'][0]
        original = original_operation_intent('original-installation', key, 'b' * 64, SQL, STEPS[0]['parameters'], self.target)
        with self.transport.db:
            self.transport.db.execute('INSERT INTO local_operation VALUES(?,?,?,?,?,NULL)',
                (key, original, sha(original), self.engine.history[0]['version'], 'prepared'))
        self.assertEqual(self.observe()['states'], ['prepared', 'no-operation-record'])
        with patch.object(self.transport, 'recover', wraps=self.transport.recover) as recover:
            self.effects.recover('whole-plan', 'b' * 64, STEPS, context='held')
        self.assertEqual(recover.call_args.args[0], key); self.assertEqual(self.engine.writes, 2)

    def test_ambiguous_original_commit_refuses_without_replacement(self):
        self.prepare(); key = self.prepare()['operation_ids'][0]
        self.transport.mutation(key, SQL, STEPS[0]['parameters'], intent_digest='b' * 64, context='held')
        self.engine.history.insert(0, {**self.engine.history[0], 'version': 3})
        with patch.object(self.transport, 'mutation', wraps=self.transport.mutation) as mutate:
            with self.assertRaises(LocalDeltaUncertain): self.effects.recover('whole-plan', 'b' * 64, STEPS, context='held')
        mutate.assert_not_called(); self.assertEqual(self.engine.writes, 1)

    def test_operation_status_conflict_refuses(self):
        self.prepare(); key = self.prepare()['operation_ids'][0]
        self.transport.mutation(key, SQL, STEPS[0]['parameters'], intent_digest='b' * 64, context='held')
        with self.transport.db:
            self.transport.db.execute("UPDATE local_operation SET state='unknown' WHERE operation=?", (key,))
        with self.assertRaises(LocalDeltaError): self.observe()
        self.assertEqual(self.engine.writes, 1)

    def test_suppressing_writer_cannot_release_failed_preparation_or_observation(self):
        class SuppressingPolicy:
            def __init__(self, fail_on): self.calls = 0; self.fail_on = fail_on
            @contextmanager
            def writer(self, operation, context):
                try: yield
                except BaseException: pass
            def admit(self, intent, context):
                self.calls += 1
                if self.calls == self.fail_on: raise PermissionError('admission failure')
        for fail_on in (1, 2):
            with self.subTest(operation='prepare', fail_on=fail_on):
                self.transport.policy = SuppressingPolicy(fail_on)
                with self.assertRaises(LocalDeltaUncertain): self.prepare()
                if fail_on == 1:
                    self.assertEqual(self.transport.db.execute('SELECT count(*) FROM local_plan').fetchone()[0], 0)
                else:
                    self.assertEqual(self.transport.db.execute('SELECT count(*) FROM local_plan').fetchone()[0], 1)
        for fail_on in (1, 2):
            with self.subTest(operation='observe', fail_on=fail_on):
                self.transport.policy = SuppressingPolicy(fail_on)
                with self.assertRaises(LocalDeltaUncertain): self.observe()
        self.assertEqual(self.engine.writes, 0)

    def test_frozen_steps_not_borrowed_during_validation(self):
        supplied = json.loads(json.dumps(STEPS)); original_target = self.transport._target
        def mutate_input(sql):
            supplied.clear()
            return original_target(sql)
        with patch.object(self.transport, '_target', side_effect=mutate_input): self.prepare(supplied)
        original = self.transport.db.execute('SELECT original FROM local_plan').fetchone()[0]
        self.assertEqual(json.loads(original)['steps'], STEPS)
        self.assertEqual(self.observe()['states'], ['no-operation-record', 'no-operation-record'])
    def test_mutated_borrowed_list_cannot_skip_invalid_frozen_step(self):
        supplied = [json.loads(json.dumps(STEPS[0])), {'statement': 'DELETE untrusted', 'parameters': {}}]
        original_target = self.transport._target
        def mutate_input(sql):
            supplied.clear()
            return original_target(sql)
        with patch.object(self.transport, '_target', side_effect=mutate_input):
            with self.assertRaises(LocalDeltaError): self.prepare(supplied)
        self.assertEqual(self.transport.db.execute('SELECT count(*) FROM local_plan').fetchone()[0], 0)
        self.assertEqual(self.engine.writes, 0)


if __name__ == '__main__': unittest.main()
