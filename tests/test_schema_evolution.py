from dataclasses import replace
from pathlib import Path
import sys
import unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from run_schema_evolution import inputs, run
from ashlar.apply import ApplyError, empty_state, plan_apply
from ashlar.schema_policies import SchemaPolicies
from ashlar.semantic_policy import SemanticPolicyError
from ashlar.whole_entity import changes_from_batch
from whole_graph_sql import graph_sql_plan


class SchemaEvolutionTests(unittest.TestCase):
    def test_original_umf_evolution_retains_history_and_partial_evidence(self):
        result = run()
        self.assertEqual(result['history_revisions'], ['1', '3'])
        self.assertEqual(result['live_schema_revisions'], ['3'])
        self.assertEqual(result['complete_interpretations'], [False, False])
        self.assertTrue(result['replay_unchanged'])
        self.assertEqual((result['events'], result['tombstones']), (4, 1))

    def test_cross_revision_refuses_without_explicit_transition_and_prior_survives(self):
        _, policies, transition, batches = inputs()
        prior = plan_apply(empty_state(), changes_from_batch(batches[0]), schema_policy=policies)
        changes = changes_from_batch(batches[1])
        for policy in [None, lambda *args: False]:
            with self.assertRaises(ApplyError):
                plan_apply(prior, changes, schema_policy=policies, schema_transition_policy=policy)
        self.assertEqual({row.schema_revision for row in prior.current.values()}, {'1'})
        admitted = plan_apply(prior, changes, schema_policy=policies, schema_transition_policy=transition)
        def never_repeat(*args):
            raise AssertionError('Already retained transition reexecuted')
        self.assertEqual(plan_apply(admitted, changes, schema_policy=policies,
                                   schema_transition_policy=never_repeat), admitted)

    def test_revision_dispatch_never_uses_newer_schema_as_fallback(self):
        _, policies, _, batches = inputs()
        change = changes_from_batch(batches[0])[0]
        for revision in ['unknown', '2']:
            with self.assertRaises(ApplyError):
                policies(replace(change, state=replace(change.state, schema_revision=revision)))
        with self.assertRaises(SemanticPolicyError):
            policies(replace(change, state=replace(change.state, props_json='{"23":"label","24":"new"}')))
        with self.assertRaises(ApplyError):
            SchemaPolicies({('local-example', '1'): lambda change: False})(change)

    def test_native_sql_planner_requires_same_transition_admission(self):
        _, policies, transition, batches = inputs()
        prior = plan_apply(empty_state(), changes_from_batch(batches[0]), schema_policy=policies)
        tables = {name: 'catalog.schema.' + name for name in
                  ['object_current', 'edge_current', 'tombstone', 'whole_source_history']}
        with self.assertRaises(ApplyError):
            graph_sql_plan(prior, batches[1], tables, materialized_at='2026-10-08T17:00:00+00:00', schema_policy=policies)
        after, steps = graph_sql_plan(prior, batches[1], tables,
            materialized_at='2026-10-08T17:00:00+00:00', schema_policy=policies,
            schema_transition_policy=transition)
        self.assertTrue(steps)
        self.assertEqual(len(after.history), 3)
