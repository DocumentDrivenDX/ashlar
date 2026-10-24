"""Effect-free checks for the optional concurrent R05 table-write path."""
import threading
import unittest

from full_delta4_trial import TrialRefused, _parallel_roles, _write_schedule
import r05_full_delta4_supervisor as supervisor


class ParallelWriteChecks(unittest.TestCase):
    def test_sequential_is_the_default_and_unknown_schedule_refuses(self):
        self.assertEqual("sequential", _write_schedule({}))
        self.assertEqual("parallel-node-edge",
                         _write_schedule({"writeSchedule": "parallel-node-edge"}))
        with self.assertRaises(TrialRefused):
            _write_schedule({"writeSchedule": "parallel-node-edge-extra"})

    def test_success_runs_both_distinct_roles_and_returns_role_results(self):
        both_started = threading.Barrier(2)

        def execute(role):
            both_started.wait(timeout=2)
            return role + "-committed"

        result = _parallel_roles(("object_current", "edge_current"), execute,
                                 lambda: self.fail("unexpected cancellation"))
        self.assertEqual({"object_current": "object_current-committed",
                          "edge_current": "edge_current-committed"}, result)

    def test_failure_cancels_peer_and_joins_it_before_returning(self):
        peer_started = threading.Event()
        release_peer = threading.Event()
        peer_finished = threading.Event()
        canceled = threading.Event()

        def execute(role):
            if role == "object_current":
                self.assertTrue(peer_started.wait(timeout=2))
                raise RuntimeError("node write failed")
            peer_started.set()
            release_peer.wait(timeout=2)
            peer_finished.set()
            return "edge stopped"

        def cancel():
            canceled.set()
            release_peer.set()

        with self.assertRaisesRegex(RuntimeError, "node write failed"):
            _parallel_roles(("object_current", "edge_current"), execute, cancel)
        self.assertTrue(canceled.is_set())
        self.assertTrue(peer_finished.is_set())

    def test_supervisor_recognizes_and_bounds_concurrent_write_phase(self):
        started = 10_000_000_000
        state = {
            "profile": "ashlar-r05-full-delta4-local-trial/0.1",
            "activePhase": "concurrent_writes",
            "phases": {"concurrent_writes": {"startedMonotonicNs": started}},
        }
        self.assertIsNone(supervisor._phase_reason(
            state, started + 840_000_000_000, started))
        self.assertEqual("phase-cap-exceeded:concurrent_writes",
                         supervisor._phase_reason(
                             state, started + 840_000_000_001, started))
        self.assertEqual("concurrent_writes", supervisor._phase_from_job_group(
            "r05-full-concurrent_writes-123456789"))


if __name__ == "__main__":
    unittest.main()
