"""Inert negative checks for R05 supervisor gates; no Spark or filesystem effects."""
import unittest
import sys
import json
import tempfile
from pathlib import Path

import r05_full_delta4_supervisor as supervisor


def _summarize_events(events):
    with tempfile.TemporaryDirectory(prefix="r05-event-case-") as temporary:
        root = Path(temporary)
        event_dir = root / supervisor.SPARK_EVENT_DIR
        event_dir.mkdir()
        (event_dir / "events.json").write_text(
            "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
        return supervisor._spark_event_summary(root)


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

    def test_worker_executes_two_separately_timed_persisted_scans(self):
        import ast
        worker = Path(__file__).with_name("full_delta4_trial.py")
        tree = ast.parse(worker.read_text(encoding="utf-8"))
        run = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                   and node.name == "run_full_delta4_trial")
        calls = [node for node in ast.walk(run) if isinstance(node, ast.Call)]
        row_scans = [node for node in calls if isinstance(node.func, ast.Name)
                     and node.func.id == "_row_value_scan"]
        endpoint_scans = [node for node in calls if isinstance(node.func, ast.Name)
                          and node.func.id == "_endpoint_scan"]
        scan_phases = {node.args[2].value for node in calls
                       if isinstance(node.func, ast.Name) and node.func.id == "_phase"
                       and len(node.args) >= 3 and isinstance(node.args[2], ast.Constant)}
        self.assertEqual(1, len(row_scans))
        self.assertEqual(1, len(endpoint_scans))
        self.assertTrue({"row_value_scan", "endpoint_scan"}.issubset(scan_phases))
        self.assertNotIn("include_endpoint_formula", {kw.arg for kw in row_scans[0].keywords})
        self.assertEqual(1200, supervisor.PHASE_SECONDS["concurrent_writes"])
        self.assertEqual(120, supervisor.PHASE_SECONDS["endpoint_scan"])

    def test_full_worker_uses_spark_420_supported_job_group_clear(self):
        import ast
        worker = Path(__file__).with_name("full_delta4_trial.py")
        tree = ast.parse(worker.read_text(encoding="utf-8"))
        helper = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                      and node.name == "_clear_spark_job_group")
        properties = {node.value for node in ast.walk(helper)
                      if isinstance(node, ast.Constant) and isinstance(node.value, str)}
        properties.discard(ast.get_docstring(helper))
        calls = [node for node in ast.walk(helper) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Attribute)
                 and node.func.attr == "setLocalProperty"]
        self.assertEqual({"spark.job.description", "spark.jobGroup.id",
                          "spark.job.interruptOnCancel"}, properties)
        self.assertTrue(any(len(call.args) == 2
                            and isinstance(call.args[0], ast.Name) and call.args[0].id == "key"
                            and isinstance(call.args[1], ast.Constant)
                            and call.args[1].value is None for call in calls))
        self.assertFalse(any(isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                             and node.func.attr == "clearJobGroup"
                             for node in ast.walk(tree)))

    def test_endpoint_scan_is_independent_and_complete(self):
        endpoint = {"logicalScan": "endpointScan", "plan": "edge Delta aggregate",
                    "rows": 5_000_000_000, "invalid_source": 0,
                    "invalid_target": 0, "source_out_of_range": 0,
                    "target_out_of_range": 0}
        supervisor._verify_endpoint_scan_witness(endpoint, 5_000_000_000)
        with self.assertRaises(supervisor.SupervisorRefused):
            supervisor._verify_endpoint_scan_witness(
                {**endpoint, "logicalScan": "rowValueScan"}, 5_000_000_000)
        with self.assertRaises(supervisor.SupervisorRefused):
            supervisor._verify_endpoint_scan_witness(
                {**endpoint, "invalid_target": 1}, 5_000_000_000)

    def test_process_group_rss_parser_sums_only_owned_group(self):
        observed = b"  41 1024\n  42 4096\n  99 8192\n"
        self.assertEqual(1024 * 1024,
                         supervisor._parse_process_group_rss(observed, 41))
        with self.assertRaises(supervisor.SupervisorRefused):
            supervisor._parse_process_group_rss(b"  99 8192\n", 41)

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
        events = [{
            "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
            "executionId": 12, "rootExecutionId": 12, "description": "collect scan action",
        }, {
            "Event": "SparkListenerJobStart", "Job ID": 3,
            "Properties": {"spark.sql.execution.id": "12",
                           "spark.jobGroup.id": "r05-full-row_value_scan-123456789"},
            "Stage Infos": [{"Stage ID": 7}],
        }, {
            "Event": "SparkListenerTaskEnd", "Stage ID": 7,
            "Task Metrics": {
                "Executor Run Time": 12, "Executor CPU Time": 900,
                "JVM GC Time": 3, "Input Metrics": {"Bytes Read": 40},
                "Shuffle Read Metrics": {"Remote Bytes Read": 50, "Local Bytes Read": 10},
                "Shuffle Write Metrics": {"Shuffle Bytes Written": 70},
                "Memory Bytes Spilled": 80, "Disk Bytes Spilled": 90,
            },
        }]
        with tempfile.TemporaryDirectory(prefix="r05-event-") as temporary:
            root = Path(temporary)
            event_dir = root / supervisor.SPARK_EVENT_DIR
            event_dir.mkdir()
            (event_dir / "events.json").write_text(
                "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            summary = supervisor._spark_event_summary(root)
        self.assertEqual("parsed", summary["status"])
        self.assertEqual({
            "stageId": 7, "tasks": 1, "executorRunTimeMs": 12,
            "executorCpuTimeNs": 900, "jvmGcTimeMs": 3, "inputBytes": 40,
            "shuffleReadBytes": 60, "shuffleWriteBytes": 70,
            "memorySpillBytes": 80, "diskSpillBytes": 90,
            "jobIds": ["3"], "sqlExecutionIds": ["12"],
            "phaseNames": ["row_value_scan"],
        }, summary["stages"][0])
        self.assertEqual("complete", summary["stagePhaseAttribution"])
        self.assertEqual(["7"], summary["sqlExecutions"][0]["stageIds"])

    def test_event_log_without_worker_phase_group_is_explicitly_incomplete(self):
        events = [{"Event": "SparkListenerJobStart", "Job ID": 3,
                   "Properties": {"spark.sql.execution.id": "12"},
                   "Stage Infos": [{"Stage ID": 7}]},
                  {"Event": "SparkListenerTaskEnd", "Stage ID": 7, "Task Metrics": {}}]
        with tempfile.TemporaryDirectory(prefix="r05-event-untagged-") as temporary:
            root = Path(temporary)
            event_dir = root / supervisor.SPARK_EVENT_DIR
            event_dir.mkdir()
            (event_dir / "events.json").write_text(
                "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            summary = supervisor._spark_event_summary(root)
        self.assertEqual("incomplete", summary["stagePhaseAttribution"])
        self.assertEqual(1, summary["untaggedJobs"])

    def test_zero_task_events_cannot_claim_complete_phase_attribution(self):
        summary = _summarize_events([{
            "Event": "SparkListenerJobStart", "Job ID": 3,
            "Properties": {"spark.jobGroup.id": "r05-full-row_value_scan-123456789"},
            "Stage Infos": [{"Stage ID": 7}],
        }])
        self.assertEqual("parsed", summary["status"])
        self.assertEqual([], summary["stages"])
        self.assertEqual("incomplete", summary["stagePhaseAttribution"])

    def test_malformed_event_job_and_stage_records_fail_parse(self):
        malformed = [
            [42],
            [{"Event": "SparkListenerJobStart", "Properties": {}, "Stage Infos": []}],
            [{"Event": "SparkListenerJobStart", "Job ID": 1,
              "Properties": {}, "Stage Infos": [None]}],
        ]
        for events in malformed:
            with self.subTest(events=events):
                self.assertEqual("parse-failed", _summarize_events(events)["status"])

    def test_event_and_description_caps_report_incomplete(self):
        previous_records = supervisor.MAX_EVENT_RECORDS
        try:
            supervisor.MAX_EVENT_RECORDS = 1
            self.assertEqual("event-record-count-cap-exceeded",
                             _summarize_events([{"Event": "Other"}, {"Event": "Other"}])["reason"])
        finally:
            supervisor.MAX_EVENT_RECORDS = previous_records
        previous_description = supervisor.MAX_SQL_DESCRIPTION_BYTES
        try:
            supervisor.MAX_SQL_DESCRIPTION_BYTES = 3
            summary = _summarize_events([{
                "Event": "SparkListenerSQLExecutionStart", "executionId": 1,
                "rootExecutionId": 1, "description": "long",
            }])
            self.assertEqual("incomplete", summary["status"])
            self.assertEqual("sql-description-byte-cap-exceeded", summary["reason"])
        finally:
            supervisor.MAX_SQL_DESCRIPTION_BYTES = previous_description
        previous_jobs = supervisor.MAX_JOBS
        try:
            supervisor.MAX_JOBS = 0
            summary = _summarize_events([{
                "Event": "SparkListenerJobStart", "Job ID": 1,
                "Properties": {}, "Stage Infos": [],
            }])
            self.assertEqual("incomplete", summary["status"])
            self.assertEqual("job-count-cap-exceeded", summary["reason"])
        finally:
            supervisor.MAX_JOBS = previous_jobs

    def test_job_group_limit_counts_utf8_bytes(self):
        summary = _summarize_events([{
            "Event": "SparkListenerJobStart", "Job ID": 1,
            "Properties": {"spark.jobGroup.id": "é" * 65}, "Stage Infos": [],
        }])
        self.assertEqual("incomplete", summary["status"])
        self.assertEqual("job-group-id-byte-cap-exceeded", summary["reason"])


if __name__ == "__main__":
    unittest.main()
