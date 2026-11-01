"""Effect-free range-task planning checks; no Spark or native writes."""
import ast
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import full_delta4_generator as generator
import full_delta4_trial as trial_worker


class RangePartitionPlanChecks(unittest.TestCase):
    def test_trial_entrypoint_requires_and_forwards_both_bounded_counts(self):
        trial_path = Path(generator.__file__).with_name("full_delta4_trial.py")
        trial_tree = ast.parse(trial_path.read_text(encoding="utf-8"))
        trial = next(node for node in trial_tree.body
                     if isinstance(node, ast.FunctionDef)
                     and node.name == "run_full_delta4_trial")
        parameters = {item.arg for item in trial.args.kwonlyargs}
        self.assertTrue({"object_range_partitions", "edge_range_partitions"}
                        .issubset(parameters))
        calls = [node for node in ast.walk(trial) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Name)
                 and node.func.id == "build_generation_plan"]
        self.assertEqual(1, len(calls))
        forwarded = {item.arg: item.value for item in calls[0].keywords}
        self.assertIsInstance(forwarded["object_range_partitions"], ast.Name)
        self.assertEqual("object_range_partitions",
                         forwarded["object_range_partitions"].id)
        self.assertIsInstance(forwarded["edge_range_partitions"], ast.Name)
        self.assertEqual("edge_range_partitions", forwarded["edge_range_partitions"].id)

        supervisor_path = trial_path.with_name("r05_full_delta4_supervisor.py")
        supervisor_tree = ast.parse(supervisor_path.read_text(encoding="utf-8"))
        calls = [node for node in ast.walk(supervisor_tree) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Name)
                 and node.func.id == "run_full_delta4_trial"]
        self.assertEqual(1, len(calls))
        local_counts = {item.arg: item.value.value for item in calls[0].keywords
                        if item.arg in ("object_range_partitions", "edge_range_partitions")
                        and isinstance(item.value, ast.Constant)}
        self.assertEqual({"object_range_partitions": 18,
                          "edge_range_partitions": 18}, local_counts)
        with self.assertRaises(ValueError):
            trial_worker.run_full_delta4_trial(
                spark=None, attempt_root="/unused", authorization_path="/unused",
                packet_sha256="", supervised_child_started=0,
                object_range_partitions=0, edge_range_partitions=18)
        with self.assertRaisesRegex(trial_worker.TrialRefused, "offline-plan-only"):
            trial_worker.run_full_delta4_trial(
                spark=None, attempt_root="/unused", authorization_path="/unused",
                packet_sha256="", supervised_child_started=0,
                object_range_partitions=256, edge_range_partitions=1_024)

    def test_role_partition_counts_are_explicit_and_independent_of_local_cores(self):
        def fields(role):
            return tuple((name, "unused") for name in
                         generator._expressions(role, generator.EDGE_ROWS).keys())

        carrier_fields = {role: fields(role) for role in generator.ROLES}
        carriers = SimpleNamespace(
            generated_bytes=json.dumps({
                "profile": "ashlar-managed-current-carriers/0.4-xxhash64"
            }).encode(),
            ordered_fields=lambda role: carrier_fields[role],
        )

        class Bridge:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def realize(self, _source, path, role):
                return {
                    "fields": carrier_fields[role],
                    "proposal": {
                        "sql": f"CREATE TABLE delta.`{path}` (unused STRING)",
                        "schemaSha256": "a" * 64,
                        "writerSettings": [],
                    },
                    "layout": {},
                }

        with (
            patch.object(generator, "validate_offline_oracle"),
            patch.object(generator, "load_current_managed_xxhash64_carriers",
                         return_value=carriers),
            patch.object(generator, "pinned_resources", return_value=(
                {}, {}, {"object_current.umf.json": b"{}",
                         "edge_current.umf.json": b"{}"}, {})),
            patch.object(generator.LocalUmfDelta4Bridge, "open", return_value=Bridge()),
        ):
            plan = generator.build_generation_plan(
                "/offline/object_current", "/offline/edge_current",
                node_rows=generator.NODE_ROWS, edge_rows=generator.EDGE_ROWS,
                object_range_partitions=256, edge_range_partitions=1_024,
            )
            local_plan = generator.build_generation_plan(
                "/offline/object_current", "/offline/edge_current",
                node_rows=generator.NODE_ROWS, edge_rows=generator.EDGE_ROWS,
            )

        roles = {role.role: role for role in plan.roles}
        self.assertEqual(256, roles["object_current"].range_partitions)
        self.assertEqual(1_024, roles["edge_current"].range_partitions)
        local_roles = {role.role: role for role in local_plan.roles}
        self.assertEqual(18, local_roles["object_current"].range_partitions)
        self.assertEqual(18, local_roles["edge_current"].range_partitions)
        self.assertIn("FROM range(0, 1000000000, 1, 18)",
                      local_roles["object_current"].insert_sql)
        self.assertIn("FROM range(0, 1000000000, 1, 256)",
                      roles["object_current"].insert_sql)
        self.assertIn("FROM range(0, 5000000000, 1, 1024)",
                      roles["edge_current"].insert_sql)
        self.assertIn("xxhash64(CAST('r05-full-scale-source' AS STRING)",
                      roles["object_current"].insert_sql)
        self.assertIn("CAST(floor(id / 5) AS BIGINT) AS `source_id`",
                      roles["edge_current"].insert_sql)
        self.assertIn("pmod(floor(id / 5) + floor(pmod(id, 5) / 2), 1000000000)",
                      roles["edge_current"].insert_sql)
        self.assertEqual(((0, 0), (0, 0)), plan.endpoint_oracle["parallelEdgePair"])

    def test_range_partition_counts_are_exact_positive_bounded_role_inputs(self):
        for invalid in (True, 0, -1, 1.5, 1_000_001, 1_001):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                generator.validate_range_partition_count(invalid, 1_000)

        self.assertEqual(1_000, generator.validate_range_partition_count(1_000, 1_000))


if __name__ == "__main__":
    unittest.main()
