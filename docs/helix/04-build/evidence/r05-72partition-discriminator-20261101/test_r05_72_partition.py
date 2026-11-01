"""Effect-free checks for the single R05 72-partition candidate packet."""
from pathlib import Path
from types import SimpleNamespace
import ast
import hashlib
import importlib
import json
import tempfile
import unittest

import r05_72_partition_worker as worker
import r05_72_partition_supervisor as supervisor
from full_delta4_generator import RolePlan, build_generation_plan
import r05_72_runtime as runtime


class WriterDiscriminatorChecks(unittest.TestCase):
    def test_write_stage_gate_ignores_unrelated_untagged_delta_jobs(self):
        event_summary = {
            "status": "parsed", "stagePhaseAttribution": "incomplete",
            "untaggedJobs": 16, "unmappedTaskStages": 14,
            "stages": [
                {"stageId": 12, "phaseNames": ["node_write"], "tasks": 72},
                {"stageId": 13, "phaseNames": ["edge_write"], "tasks": 72},
                {"stageId": 0, "phaseNames": [], "tasks": 1},
            ],
        }
        self.assertTrue(supervisor._write_stage_attribution_valid(event_summary))
        changed = json.loads(json.dumps(event_summary))
        changed["stages"][1]["tasks"] = 71
        self.assertFalse(supervisor._write_stage_attribution_valid(changed))
        changed = json.loads(json.dumps(event_summary))
        changed["stages"][0]["phaseNames"] = []
        self.assertFalse(supervisor._write_stage_attribution_valid(changed))

    def test_delta_detail_uses_current_path_quoting_api(self):
        class Query:
            def limit(self, count):
                self.limit_count = count
                return self

            def collect(self):
                return [SimpleNamespace(asDict=lambda recursive: {
                    "format": "delta", "id": "table-uuid"})]

        class Spark:
            def sql(self, statement):
                self.statement = statement
                return Query()

        spark = Spark()
        detail = __import__("full_delta4_trial")._table_detail(
            spark, Path("/private/tmp/r05 compatibility table"))
        self.assertEqual("table-uuid", detail["id"])
        self.assertEqual(
            "DESCRIBE DETAIL delta.`/private/tmp/r05 compatibility table`",
            spark.statement)

    def test_executed_writer_and_cleanup_lazy_imports_resolve(self):
        packet = Path(worker.__file__).parent
        targets = {
            packet / "full_delta4_trial.py": {"_table_detail", "_validate_table"},
            packet / "r05_72_partition_supervisor.py": {"_verify_and_cleanup"},
        }
        for source_path, function_names in targets.items():
            tree = ast.parse(source_path.read_text(encoding="utf-8"))
            functions = {node.name: node for node in tree.body
                         if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
            for function_name in function_names:
                with self.subTest(function=function_name):
                    self.assertIn(function_name, functions)
                    for node in ast.walk(functions[function_name]):
                        if (isinstance(node, ast.ImportFrom)
                                and node.module and node.module.startswith("ashlar_host.")):
                            module = importlib.import_module(node.module)
                            for alias in node.names:
                                self.assertTrue(hasattr(module, alias.name),
                                                f"{node.module}.{alias.name}")

    def test_exact_range_rewrite_preserves_projection_and_ddl(self):
        for role, count in (("object_current", worker.NODE_ROWS),
                            ("edge_current", worker.EDGE_ROWS)):
            original = RolePlan(
                role=role, path=f"/owned/{role}", ddl="CREATE TABLE exact_shape",
                insert_sql=f"INSERT INTO delta.`/owned/{role}`\nSELECT id, CAST(id AS BIGINT) AS value FROM range({count})",
                schema_sha256="a" * 64, layout={}, writer_settings=())
            candidate = worker._partitioned_plan(original)
            self.assertEqual(original.ddl, candidate.ddl)
            self.assertIn(f"range(0, {count}, 1, 72)", candidate.insert_sql)
            self.assertEqual(candidate.insert_sql.count("FROM range("), 1)
            self.assertEqual("SELECT id, CAST(id AS BIGINT) AS value FROM range(0, "
                             f"{count}, 1, 72)", worker._source_select(candidate))

    def test_range_rewrite_refuses_other_source_or_multiple_ranges(self):
        for sql in ("INSERT INTO delta.`/owned/x` SELECT id FROM table_x",
                    f"INSERT INTO delta.`/owned/x` SELECT a FROM range({worker.NODE_ROWS}) JOIN range({worker.NODE_ROWS})"):
            with self.subTest(sql=sql), self.assertRaises(worker.PacketRefused):
                worker._partitioned_plan(RolePlan(role="object_current", path="/owned/x", ddl="",
                                                   insert_sql=sql, schema_sha256="a" * 64,
                                                   layout={}, writer_settings=()))

    def test_umf_generated_ddl_and_values_change_only_at_range_slice(self):
        plan = build_generation_plan("/owned/object_current", "/owned/edge_current",
                                    node_rows=worker.NODE_ROWS, edge_rows=worker.EDGE_ROWS)
        self.assertEqual({"object_current", "edge_current"}, {role.role for role in plan.roles})
        for role in plan.roles:
            candidate = worker._partitioned_plan(role)
            count = worker.NODE_ROWS if role.role == "object_current" else worker.EDGE_ROWS
            self.assertEqual(role.ddl, candidate.ddl)
            self.assertEqual(role.writer_settings, candidate.writer_settings)
            self.assertEqual(role.insert_sql.replace(
                f"FROM range({count})", f"FROM range(0, {count}, 1, 72)"),
                candidate.insert_sql)

    def test_approval_is_closed_to_one_candidate_and_fixed_limits(self):
        approval = {
            "profile": "ashlar-r05-72-partition-authorization/0.1", "status": "approved",
            "packetSha256": "a" * 64, "attempts": 1, "nodeRows": worker.NODE_ROWS,
            "edgeRows": worker.EDGE_ROWS, "totalSeconds": 900, "workerSeconds": 780,
            "rootBytes": 32 * 1024**3, "activeDeltaBytes": 16 * 1024**3,
            "minimumFreeBytes": 64 * 1024**3, "predictiveOptimizationDisabled": False,
            "sourcePinsSha256": "b" * 64, "runtimePinsSha256": "c" * 64,
            "ownerDecision": "reviewed local discriminator",
        }
        self.assertIs(worker._closed_approval(approval, "a" * 64), approval)
        for key, value in (("attempts", 2), ("edgeRows", 5_000_000_000),
                           ("totalSeconds", 901), ("predictiveOptimizationDisabled", True)):
            changed = dict(approval, **{key: value})
            with self.subTest(key=key), self.assertRaises(worker.PacketRefused):
                worker._closed_approval(changed, "a" * 64)

    def test_packet_is_single_candidate_and_does_not_require_graphframes(self):
        self.assertEqual(("candidate-72",), worker.MODES)
        self.assertEqual(72, worker.PARTITIONS)
        source = Path(worker.__file__).read_text(encoding="utf-8")
        self.assertNotIn("_admit_canonical_graphframes_runtime", source)
        self.assertNotIn("GraphFrame(", source)
        self.assertIn("range(0, {count}, 1, {PARTITIONS})", source)

    def test_runtime_receipt_is_delta_only_and_hashes_reconcile(self):
        receipt = json.loads((Path(worker.__file__).parent / "writer-only-runtime-receipt.json")
                             .read_text(encoding="utf-8"))
        self.assertEqual("ashlar-r05-72-writer-runtime/0.1", receipt["profile"])
        self.assertEqual("observed-candidate-awaiting-review", receipt["status"])
        self.assertEqual({"delta-spark_4.2_2.13-4.4.1.jar", "delta-storage-4.4.1.jar"},
                         set(receipt["jars"]))
        self.assertNotIn("graphframes", receipt)
        for record in receipt["jars"].values():
            path = Path(record["path"])
            self.assertEqual(record["bytes"], path.stat().st_size)
            self.assertEqual(record["sha256"], hashlib.sha256(path.read_bytes()).hexdigest())

    def test_event_log_aggregates_write_task_duration_and_resource_metrics(self):
        events = [{
            "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
            "executionId": 12, "rootExecutionId": 12, "description": "Delta INSERT",
        }, {
            "Event": "SparkListenerJobStart", "Job ID": 3,
            "Properties": {"spark.sql.execution.id": "12",
                           "spark.jobGroup.id": "r05-full-node_write-72"},
            "Stage Infos": [{"Stage ID": 7}],
        }, {
            "Event": "SparkListenerTaskEnd", "Stage ID": 7,
            "Task Info": {"Launch Time": 100, "Finish Time": 133},
            "Task Metrics": {"Executor Run Time": 31, "Executor CPU Time": 900,
                             "JVM GC Time": 3, "Input Metrics": {"Bytes Read": 40},
                             "Shuffle Read Metrics": {"Remote Bytes Read": 50,
                                                       "Local Bytes Read": 10},
                             "Shuffle Write Metrics": {"Shuffle Bytes Written": 70},
                             "Memory Bytes Spilled": 80, "Disk Bytes Spilled": 90},
        }]
        with tempfile.TemporaryDirectory(prefix="r05-72-events-") as temporary:
            root = Path(temporary)
            event_dir = root / runtime.SPARK_EVENT_DIR
            event_dir.mkdir()
            (event_dir / "events.json").write_text(
                "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            summary = runtime._spark_event_summary(root)
        self.assertEqual("parsed", summary["status"])
        stage = summary["stages"][0]
        self.assertEqual("node_write", stage["phaseNames"][0])
        self.assertEqual(33, stage["minTaskDurationMs"])
        self.assertEqual(33, stage["maxTaskDurationMs"])
        self.assertEqual(33, stage["taskDurationMsSum"])
        self.assertEqual(900, stage["executorCpuTimeNs"])
        self.assertEqual(3, stage["jvmGcTimeMs"])
        self.assertEqual(70, stage["shuffleWriteBytes"])
        self.assertEqual(90, stage["diskSpillBytes"])


if __name__ == "__main__":
    unittest.main()
