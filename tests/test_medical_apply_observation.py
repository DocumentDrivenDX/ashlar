# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual apply/journal/source owners; effect execution and native rows inert."""
from contextlib import ExitStack
import json
import unittest
from unittest.mock import patch

from ashlar_host.delta_custody import encoded, sha, LOCAL_OPERATION_CAPACITY_8M
from ashlar_host.driver import LocalDeltaEffects
import test_medical_publication_observation as observations


class Tests(unittest.TestCase):
    def fixture(self, stack, *, limited=False):
        owner = observations.Tests()
        self.addCleanup(owner.doCleanups)
        f, driver, plan, bounds, descriptor, value = owner.fixture(stack)
        if limited:
            sizes = [len(encoded({'schema': json.loads(value['schemas'][role]),
                                  'rows': value['expected'][role]}).encode())
                     for role in value['expected']]
            # Select this bound at construction of a separate fresh actual owner.
            owner = observations.Tests()
            self.addCleanup(owner.doCleanups)
            f, driver, plan, bounds, descriptor, value = owner.fixture(stack, total_bytes=max(sizes)+1024)
        transport = driver.transport
        transport.operation_capacity = LOCAL_OPERATION_CAPACITY_8M
        applied = [False]
        calls = []

        def detail(target):
            self.assertIs(transport.observation_bounds, bounds)
            return {'id': target.uuid, 'location': target.path.as_uri()}

        def capture(target, version, *, remaining_bytes=None):
            self.assertIs(transport.observation_bounds, bounds)
            role = target.table.split('.')[-1]
            calls.append((version, role, remaining_bytes))
            rows = value['previous_expected' if version == 1 else 'expected'][role]
            rows = sorted(json.loads(encoded(rows)), key=encoded)
            return {'schema': [[name, family.lower()] for name, family in value['columns'][role]],
                    'rows': rows, 'row_sha256': sha(encoded(rows))}, json.loads(value['schemas'][role])

        transport._detail = detail
        transport.bounded_snapshot = capture
        transport._snapshot = lambda *args: self.fail('Unbudgeted apply snapshot used')
        transport.original_history = lambda target: ([{'version': 2}, {'version': 1}]
                                                     if applied[0] else [{'version': 1}])

        def effect(*args, **kwargs):
            applied[0] = True
            return {}

        # These controls isolate cumulative capture traversal, not effect-proof resolution.
        # test_medical_effect_proof exercises the actual journal/native inspector owners.
        stack.enter_context(patch.object(driver, '_validate_medical_effect_proof', return_value=None))
        stack.enter_context(patch.object(LocalDeltaEffects, 'run', side_effect=effect))
        return f, driver, bounds, value, calls, applied

    def test_initial_and_retained_apply_use_cumulative_original_bags(self):
        with ExitStack() as stack:
            f, driver, bounds, value, calls, applied = self.fixture(stack)
            with driver.writer('original', driver.context), driver.transport.bounded_observations(bounds):
                artifact = driver.apply(value['request'], driver.context)
                repeated = driver.apply(value['request'], driver.context)
            self.assertEqual(artifact, repeated)
            self.assertTrue(applied[0])
            self.assertEqual(len(calls), 20)  # 12 initial, 8 retained prior/manifest.
            for start in (0, 4, 8, 12, 16):
                remaining = [entry[2] for entry in calls[start:start+4]]
                self.assertEqual(remaining[0], bounds.total_bytes)
                self.assertTrue(all(a > b for a, b in zip(remaining, remaining[1:])))
            self.assertEqual({entry[0] for entry in calls[:4]}, {1})
            self.assertEqual({entry[0] for entry in calls[4:12]}, {2})
            self.assertEqual(driver.transport.db.execute('SELECT COUNT(*) FROM local_publication_artifact').fetchone(), (1,))
            self.assertTrue(all(c.closes == 1 for c in f.connections))

    def test_aggregate_limit_refuses_artifact_even_when_each_bag_fits(self):
        with ExitStack() as stack:
            _, driver, bounds, value, calls, applied = self.fixture(stack, limited=True)
            with driver.writer('original', driver.context), driver.transport.bounded_observations(bounds):
                with self.assertRaises(PermissionError):
                    driver.apply(value['request'], driver.context)
            self.assertTrue(applied[0])
            self.assertGreater(len(calls), 4)
            self.assertEqual(driver.transport.db.execute('SELECT COUNT(*) FROM local_publication_artifact').fetchone(), (0,))

    def test_bound_interval_required_before_any_effect_or_capture(self):
        with ExitStack() as stack:
            _, driver, _, value, calls, applied = self.fixture(stack)
            with driver.writer('original', driver.context), self.assertRaises(PermissionError):
                driver.apply(value['request'], driver.context)
            self.assertEqual(calls, [])
            self.assertFalse(applied[0])


if __name__ == '__main__':
    unittest.main()
