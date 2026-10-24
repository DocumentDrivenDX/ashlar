import unittest
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from r05_edge_writer_profile import (
    ACTIVE_DELTA_BYTES, EDGE_ROWS, MIN_FREE_BYTES, NODE_ROWS, ROOT_BYTES,
    TOTAL_SECONDS, WORKER_SECONDS, ProfileRefused, _parse_events_child,
    _validate_authorization,
)
from r05_edge_writer_profile import full
from full_delta4_generator import build_generation_plan


class EdgeWriterProfileTests(unittest.TestCase):
    def test_fixed_profile_is_one_edge_write_with_bounded_caps(self):
        self.assertEqual((NODE_ROWS, EDGE_ROWS), (45_000_000, 225_000_000))
        self.assertEqual((WORKER_SECONDS, TOTAL_SECONDS), (780, 900))
        self.assertEqual(ACTIVE_DELTA_BYTES, 16 * 1024**3)
        self.assertEqual(ROOT_BYTES, 32 * 1024**3)
        self.assertEqual(MIN_FREE_BYTES, 64 * 1024**3)

    def test_requested_authorization_cannot_launch(self):
        pins = {"packetSha256": "a" * 64, "sourcePinsSha256": "b" * 64,
                "runtimePinsSha256": "c" * 64}
        request = {
            "profile": "ashlar-r05-edge-writer-authorization/0.1",
            "status": "requested", "packetSha256": pins["packetSha256"],
            "sourcePinsSha256": pins["sourcePinsSha256"],
            "runtimePinsSha256": pins["runtimePinsSha256"], "attempts": 1,
            "nodeRows": NODE_ROWS, "edgeRows": EDGE_ROWS,
            "totalSeconds": TOTAL_SECONDS, "workerSeconds": WORKER_SECONDS,
            "activeDeltaBytes": ACTIVE_DELTA_BYTES, "rootBytes": ROOT_BYTES,
            "minimumFreeBytes": MIN_FREE_BYTES,
            "predictiveOptimizationDisabled": False, "ownerDecision": "pending review",
        }
        with self.assertRaises(ProfileRefused):
            _validate_authorization(request, pins)

    def test_pinned_umf_plan_contains_one_exact_edge_projection(self):
        plan = build_generation_plan("/offline/r05/object_current",
                                     "/offline/r05/edge_current",
                                     node_rows=NODE_ROWS, edge_rows=EDGE_ROWS)
        edge = next(item for item in plan.roles if item.role == "edge_current")
        self.assertIn("FROM range(225000000)", edge.insert_sql)
        self.assertIn("xxhash64(", edge.insert_sql)
        self.assertIn("AS `source_id`", edge.insert_sql)
        self.assertIn("AS `target_id`", edge.insert_sql)
        self.assertEqual(len(plan.roles), 2)  # plan is built offline; worker selects only edge_current

    def test_nested_spark_42_eventlog_v2_layout_retains_task_metrics(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            event_dir = root / "spark-events" / "eventlog_v2_local-1792835487036"
            event_dir.mkdir(parents=True)
            events = [
                {"Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
                 "executionId": 7, "rootExecutionId": 7, "description": "edge INSERT"},
                {"Event": "SparkListenerJobStart", "Job ID": 3,
                 "Stage Infos": [{"Stage ID": 11}],
                 "Properties": {"spark.sql.execution.id": "7",
                                "spark.jobGroup.id": "r05-full-edge_write-42"}},
                {"Event": "SparkListenerTaskEnd", "Stage ID": 11,
                 "Task Metrics": {"Executor Run Time": 120, "Executor CPU Time": 90000000,
                                  "JVM GC Time": 4, "Output Metrics": {
                                      "Bytes Written": 4096, "Records Written": 100}}},
            ]
            log = event_dir / "events_1_local-1792835487036"
            log.write_text("".join(json.dumps(event) + "\n" for event in events))
            (event_dir / "appstatus_local-1792835487036").write_text("not an event record\n")
            self.assertEqual(len(full._event_log_files(root / "spark-events")), 1)
            self.assertEqual(_parse_events_child(root), 0)
            summary = json.loads((root / "r05-edge-writer-event-summary.json").read_text())
            self.assertEqual(summary["status"], "parsed")
            self.assertEqual(summary["stages"][0]["executorCpuTimeNs"], 90_000_000)
            self.assertEqual(summary["stages"][0]["outputBytesWritten"], 4096)
            self.assertEqual(summary["stages"][0]["phaseNames"], ["edge_write"])
            self.assertEqual(summary["profileStatus"], "usable")

    def test_empty_event_log_is_not_a_usable_profile(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "spark-events").mkdir()
            self.assertEqual(_parse_events_child(root), 2)
            summary = json.loads((root / "r05-edge-writer-event-summary.json").read_text())
            self.assertEqual(summary["status"], "parsed")
            self.assertEqual(summary["profileStatus"], "unusable")

    def test_event_log_root_symlink_is_refused(self):
        with TemporaryDirectory() as temporary:
            base = Path(temporary)
            real = base / "real"
            real.mkdir()
            (base / "spark-events").symlink_to(real, target_is_directory=True)
            with self.assertRaises(PermissionError):
                full._event_log_files(base / "spark-events")


if __name__ == "__main__":
    unittest.main()
