"""Inert negative checks for R05 supervisor gates; no Spark or filesystem effects."""
import unittest
import sys
import json
import tempfile
from pathlib import Path

import r05_full_delta4_supervisor as supervisor


class SupervisorGateTests(unittest.TestCase):
    def test_import_is_inert(self):
        self.assertNotIn("pyspark", sys.modules)
        self.assertFalse(any(name.startswith("pyspark.") for name in sys.modules))

    def test_missing_owner_approval_fails_closed(self):
        with self.assertRaises(supervisor.SupervisorRefused):
            supervisor._validate_approval(
                {}, packet_digest="a" * 64, source_digest="b" * 64,
                runtime_digest="c" * 64)

    def test_exact_byte_limits_are_admitted_but_one_byte_over_is_refused(self):
        self.assertIsNone(supervisor._capacity_reason(
            supervisor.ROOT_BYTES, supervisor.PARQUET_BYTES))
        self.assertEqual("attempt-root-byte-cap-exceeded",
                         supervisor._capacity_reason(supervisor.ROOT_BYTES + 1, 0))
        self.assertEqual("active-parquet-byte-cap-exceeded",
                         supervisor._capacity_reason(0, supervisor.PARQUET_BYTES + 1))

    def test_phase_deadline_is_hard_limit_evidence(self):
        started = 10_000_000_000
        state = {
            "profile": "ashlar-r05-full-delta4-local-trial/0.1",
            "activePhase": "row_value_scan",
            "phases": {"row_value_scan": {"startedMonotonicNs": started}},
        }
        self.assertIsNone(supervisor._phase_reason(
            state, started + 120_000_000_000, started))
        self.assertEqual("phase-cap-exceeded:row_value_scan",
                         supervisor._phase_reason(
                             state, started + 120_000_000_001, started))

    def test_disappearing_spark_scratch_entry_is_tolerated_only_in_scratch(self):
        root = Path("/private/tmp/r05-capacity-race")
        missing = lambda _path: (_ for _ in ()).throw(FileNotFoundError(2, "gone"))
        scratch_entry = root / "spark-local" / "shuffle_4.index"
        self.assertIsNone(supervisor._capacity_stat(root, scratch_entry, missing))
        with self.assertRaises(FileNotFoundError):
            supervisor._capacity_stat(root, root / "object_current" / "receipt.json", missing)

    def test_private_diagnostic_cap_is_enforced(self):
        with tempfile.TemporaryDirectory(prefix="r05-diag-") as temporary:
            root = Path(temporary)
            path = root / supervisor.WORKER_DIAGNOSTICS
            path.write_bytes(b"12345")
            prior = supervisor.DIAGNOSTICS_BYTES
            try:
                supervisor.DIAGNOSTICS_BYTES = 4
                self.assertEqual("worker-diagnostics-byte-cap-exceeded",
                                 supervisor._diagnostic_reason(root))
                evidence = supervisor._private_file_evidence(path, 4)
                self.assertEqual(5, evidence["bytes"])
                self.assertEqual("cap-exceeded", evidence["status"])
                self.assertEqual(64, len(evidence["sha256"]))
            finally:
                supervisor.DIAGNOSTICS_BYTES = prior

    def test_event_log_summary_aggregates_task_stage_metrics_offline(self):
        event = {
            "Event": "SparkListenerTaskEnd", "Stage ID": 7,
            "Task Metrics": {
                "Executor Run Time": 12, "Executor CPU Time": 900,
                "JVM GC Time": 3, "Input Metrics": {"Bytes Read": 40},
                "Shuffle Read Metrics": {"Remote Bytes Read": 50, "Local Bytes Read": 10},
                "Shuffle Write Metrics": {"Shuffle Bytes Written": 70},
                "Memory Bytes Spilled": 80, "Disk Bytes Spilled": 90,
            },
        }
        with tempfile.TemporaryDirectory(prefix="r05-event-") as temporary:
            root = Path(temporary)
            events = root / supervisor.SPARK_EVENT_DIR
            events.mkdir()
            (events / "events.json").write_text(json.dumps(event) + "\n", encoding="utf-8")
            summary = supervisor._spark_event_summary(root)
        self.assertEqual("parsed", summary["status"])
        self.assertEqual({
            "stageId": 7, "tasks": 1, "executorRunTimeMs": 12,
            "executorCpuTimeNs": 900, "jvmGcTimeMs": 3, "inputBytes": 40,
            "shuffleReadBytes": 60, "shuffleWriteBytes": 70,
            "memorySpillBytes": 80, "diskSpillBytes": 90,
        }, summary["stages"][0])


if __name__ == "__main__":
    unittest.main()
