import hashlib
from pathlib import Path
import sys
import unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from run_schema_evolution import inputs
from ashlar.apply import empty_state, plan_apply
from ashlar.recovery import RecoveryError, recover_whole_entity_state
from ashlar.staging import batch_row
from ashlar.whole_entity import changes_from_batch


class RecoveryTests(unittest.TestCase):
    def prepare(self):
        _, policy, transition, batches = inputs()
        rows = [batch_row(batch) for batch in batches]
        arguments = dict(prior=empty_state(), feed='local-evolution', epoch='example-1',
                         cursor_before='0', cursor_after=batches[-1].cursor_after,
                         schema_policy=policy, schema_transition_policy=transition)
        return rows, arguments, batches

    def test_complete_schema_evolved_interval_matches_direct_apply(self):
        rows, args, batches = self.prepare()
        recovered = recover_whole_entity_state(rows, **args)
        state = empty_state()
        for batch in batches:
            state = plan_apply(state, changes_from_batch(batch), schema_policy=args['schema_policy'],
                               schema_transition_policy=args['schema_transition_policy'])
        self.assertEqual(recovered.state, state)
        digest = hashlib.sha256()
        for row in rows:
            raw = row['batch_json'].encode();digest.update(len(raw).to_bytes(8, 'big'));digest.update(raw)
        self.assertEqual(recovered.ordered_batch_digest, digest.hexdigest())
        self.assertEqual((recovered.batches, recovered.events), (3, 4))
        self.assertEqual(recovered.cursor_after, batches[-1].cursor_after)

    def test_missing_reordered_mixed_and_overlapping_intervals_refuse(self):
        rows, args, _ = self.prepare()
        for bad in [rows[1:], rows[:-1], rows[::-1], [rows[0], rows[0]], []]:
            with self.assertRaises(RecoveryError):recover_whole_entity_state(bad, **args)
        for fields in [dict(feed='other'), dict(epoch='other'), dict(cursor_after='999'),
                       dict(cursor_before='01')]:
            with self.assertRaises(RecoveryError):recover_whole_entity_state(rows, **dict(args, **fields))

    def test_partial_restart_requires_exact_trusted_prior_and_boundaries(self):
        rows, args, batches = self.prepare()
        prior = plan_apply(empty_state(), changes_from_batch(batches[0]), schema_policy=args['schema_policy'])
        full = recover_whole_entity_state(rows, **args)
        partial = recover_whole_entity_state(rows[1:], **dict(args, prior=prior, cursor_before=batches[0].cursor_after))
        self.assertEqual(partial.state, full.state)
        self.assertEqual({value.schema_revision for value in prior.current.values()}, {'1'})

    def test_finite_limits_refuse_before_exposing_partial_state(self):
        rows, args, _ = self.prepare()
        for fields in [dict(max_batches=1), dict(max_events=1), dict(max_artifact_bytes=10),
                       dict(max_batches=True), dict(max_events=0)]:
            with self.assertRaises(RecoveryError):recover_whole_entity_state(rows, **dict(args, **fields))
        self.assertEqual(len(args['prior'].current), 0)
