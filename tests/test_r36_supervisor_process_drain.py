import importlib.util
from pathlib import Path
import unittest


SUPERVISOR = (Path(__file__).resolve().parents[1] / "tests/fixtures/golden"
    / "current-upstream-release-20261101/r21-truss052-fresh-source-custody"
    / "r28-page-sequence/r36_truss052_supervisor.py")
SPEC = importlib.util.spec_from_file_location("r36_supervisor_drain_test", SUPERVISOR)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ProcessDrainTests(unittest.TestCase):
    def test_waits_for_a_short_natural_descendant_drain(self):
        clock = [0.0]
        snapshots = iter([([123], 100, None), ([], 0, None)])
        result = MODULE._wait_for_process_group_drain(
            ([123], 100, None), lambda: next(snapshots), grace_seconds=2,
            poll_interval=0.25, monotonic=lambda: clock[0],
            sleep=lambda duration: clock.__setitem__(0, clock[0] + duration))
        self.assertEqual(result, ([], 0, None))
        self.assertEqual(clock[0], 0.5)

    def test_keeps_process_alive_as_failure_after_bounded_grace(self):
        clock = [0.0]
        polls = []

        def snapshot():
            polls.append(clock[0])
            return ([123], 100, None)

        result = MODULE._wait_for_process_group_drain(
            ([123], 100, None), snapshot, grace_seconds=1,
            poll_interval=0.5, monotonic=lambda: clock[0],
            sleep=lambda duration: clock.__setitem__(0, clock[0] + duration))
        self.assertEqual(result[0], [123])
        self.assertEqual(clock[0], 1)
        self.assertEqual(len(polls), 2)


if __name__ == "__main__":
    unittest.main()
