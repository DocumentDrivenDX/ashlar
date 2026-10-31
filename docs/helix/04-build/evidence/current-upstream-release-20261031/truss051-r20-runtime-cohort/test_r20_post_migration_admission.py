"""Import-only and fail-closed checks for the R20 public migration stage."""
from __future__ import annotations

import json
import hashlib
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import r20_post_migration_admission as candidate
import r20_pre_migration_custody_worker as prefix_worker

HERE = Path(__file__).resolve().parent
here = HERE
PACKET = Path(__file__).resolve().parents[2] / "current-upstream-release-20261024" / "truss051-r16-effectfree-candidate-20261029"


class R20PublicMigrationStageTests(unittest.TestCase):
    def test_isolated_child_imports_pinned_ashlar_from_unrelated_cwd(self):
        scope = json.loads((HERE / "full-lifecycle-scope-proposal-r20.json").read_bytes())
        runtime = scope["runtime"]
        with tempfile.TemporaryDirectory() as temporary:
            env = {"PATH": f"{Path(runtime['python_path']).parent}:/usr/bin:/bin:/opt/homebrew/bin",
                   "HOME": "/Users/erik", "PYTHONNOUSERSITE": "1",
                   "PYTHONDONTWRITEBYTECODE": "1", "TMPDIR": "/private/tmp"}
            result = subprocess.run(
                [runtime["python_path"], "-I", "-B", str(HERE / "r20_post_migration_admission.py"),
                 "--source-preflight", "--scope", str(HERE / "full-lifecycle-scope-proposal-r20.json")],
                cwd=temporary, env=env, stdin=subprocess.DEVNULL, capture_output=True,
                text=True, timeout=30, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["profile"], "ashlar-r20-isolated-source-preflight/0.1")
        self.assertEqual(report["native_effects"], "none")
        source_root = Path(scope["ashlar_source"]["root_path"])
        self.assertTrue(all(Path(path).is_relative_to(source_root)
                            for path in report["module_paths"].values()))
        self.assertEqual(report["runtime"]["core_version"], "0.10.4")
        self.assertEqual(report["runtime"]["core_wheel_sha256"],
                         "a3a4a68fcc4dfb637d7e9229ac7f6cbf029e464442304e6e49f8fc0c2467845e")

    def test_frozen_custody_and_profile_match_governing_packet_evidence(self):
        scope = json.loads((here / "full-lifecycle-scope-proposal-r20.json").read_bytes())
        expected = scope["source_custody"]
        for key, value in candidate.FROZEN_CUSTODY.items():
            self.assertEqual(expected[key], value, key)
        prior_receipt = json.loads((Path(__file__).resolve().parents[1].parent
            / "current-upstream-release-20261023"
            / "truss049-r12-current-selected-source-lifecycle-20261024"
            / "preparation.json").read_bytes())
        self.assertEqual(candidate.FROZEN_CUSTODY["expected_genesis_checkpoint_sha256"],
                         prior_receipt["immutable_inputs"]["genesis_checkpoint_sha256"])
        self.assertEqual(candidate.WEFT_VERSION, "0.4.56")
        self.assertEqual(candidate.CARRIER_PROFILE, "ashlar-delta/0.4-xxhash64")

    def test_installed_public_runtime_reports_exact_outer_embedded_and_route_hashes(self):
        try:
            import truss  # noqa: F401
        except ImportError:
            self.skipTest("Pinned Truss runtime is not installed in this interpreter")
        runtime = candidate.verify_installed_runtime()
        self.assertEqual(runtime["core_version"], "0.10.4")
        self.assertEqual(runtime["core_wheel_sha256"], "a3a4a68fcc4dfb637d7e9229ac7f6cbf029e464442304e6e49f8fc0c2467845e")
        self.assertEqual(runtime["truss_wheel_sha256"], candidate.TRUSS_WHEEL_SHA256)
        self.assertEqual(runtime["outer_index_sha256"], candidate.TRUSS_051_OUTER_INDEX_SHA256)
        self.assertEqual(runtime["embedded_050_index_sha256"], candidate.TRUSS_050_EMBEDDED_INDEX_SHA256)
        self.assertEqual(runtime["source_manifest_sha256"], candidate.TRUSS_048_MANIFEST_SHA256)
        self.assertEqual(runtime["target_manifest_sha256"], candidate.TRUSS_051_NATIVE_MANIFEST_SHA256)
        self.assertEqual(len(runtime["edges"]), 2)
        self.assertIs(runtime["native_migration_invoked"], False)

    def test_public_migration_result_detail_is_retained_in_serialized_receipt(self):
        try:
            from truss.migrations import MigrationResult
        except ImportError:
            self.skipTest("Pinned Truss MigrationResult is not installed in this interpreter")
        result = MigrationResult("committed_unverified", "ref-1", "attempt-1",
                                 "source-sha", "target-sha", "verification detail")
        self.assertEqual(candidate._result_dict(result), {
            "outcome": "committed_unverified", "reference": "ref-1",
            "attempt_id": "attempt-1", "source": "source-sha", "target": "target-sha",
            "detail": "verification detail", "receipt": None, "current_verification": None,
        })

    def test_null_scope_template_fails_before_any_native_runtime_import(self):
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            approval = root / "approval.json"
            worker = root / "worker.py"
            approval.write_text("{}\n")
            worker.write_text("# test only\n")
            with self.assertRaisesRegex(candidate.AdmissionError, "full-lifecycle scope"):
                candidate._validate_scope_approval(
                    PACKET / "future-scope-template.json", approval, worker, worker)

    def test_real_scope_validator_accepts_fully_shaped_inert_approval(self):
        """Exercise the actual R20 validator through retained-source admission."""
        scope_raw = (HERE / "full-lifecycle-scope-proposal-r20.json").read_bytes()
        scope = json.loads(scope_raw)
        candidate._preflight_ashlar_source(scope)
        from ashlar_host.local_current_delta4_umf_sdk010 import (
            BRIDGE_SHA256, BUILD_INPUTS_SHA256, MANIFEST_SHA256, ROLE_SEMANTICS_SHA256,
            SOURCE_POLICY_SHA256)

        here = HERE
        workers = scope["workers"]
        runtime = scope["runtime"]
        bridge = {
            "profile": "ashlar-local-current-delta4-umf-source-sdk010/0.1",
            "carrier_profile": candidate.CARRIER_PROFILE,
            "bridge_sha256": BRIDGE_SHA256,
            "resource_sha256": MANIFEST_SHA256,
            "source_policy_sha256": SOURCE_POLICY_SHA256,
            "sdk_version": "0.10.0",
            "sdk_archive_sha256": "2fb63dea5ca85b32c6bad823703de526875ac79490234dbfea4a8863c7d5fe6c",
            "build_inputs_sha256": BUILD_INPUTS_SHA256,
            "role_semantics_sha256": ROLE_SEMANTICS_SHA256,
        }
        jar_hashes = {
            "delta-spark_4.2_2.13-4.4.1.jar":
                "2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be",
            "delta-storage-4.4.1.jar":
                "f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f",
        }
        connection_factory_sha256 = hashlib.sha256(
            __import__("inspect").getsource(candidate._role_connection_factory).encode()
        ).hexdigest()
        approval = {
            "profile": "ashlar-truss-r20-full-native-lifecycle-approval/0.1",
            "scope_sha256": hashlib.sha256(scope_raw).hexdigest(),
            "workers": workers,
            "source_archive_sha256": candidate.SOURCE_ARCHIVE_SHA256,
            "source_manifest_sha256": candidate.SOURCE_MANIFEST_SHA256,
            "runtime": {
                "core_version": "0.10.4",
                "core_wheel_sha256": "a3a4a68fcc4dfb637d7e9229ac7f6cbf029e464442304e6e49f8fc0c2467845e",
                "truss_version": "0.0.51",
                "wheel_sha256": candidate.TRUSS_WHEEL_SHA256,
                "weft_version": candidate.WEFT_VERSION,
                "weft_wheel_sha256": candidate.WEFT_WHEEL_SHA256,
                "pyspark_version": candidate.SPARK_VERSION,
                "delta_spark_version": candidate.DELTA_VERSION,
                "carrier_profile": candidate.CARRIER_PROFILE,
                "outer_migration_index_sha256": candidate.TRUSS_051_OUTER_INDEX_SHA256,
                "embedded_050_index_sha256": candidate.TRUSS_050_EMBEDDED_INDEX_SHA256,
                "runtime_manifest_sha256": candidate.TRUSS_051_RUNTIME_MANIFEST_SHA256,
                "jar_sha256": jar_hashes,
                "bun_sha256": runtime["bun_sha256"],
                "connection_factory_sha256": connection_factory_sha256,
            },
            "seed": {
                "request_sha256": candidate.SEED_REQUEST_SHA256,
                "origin_map_sha256": candidate.ORIGIN_MAP_SHA256,
                "origin_gate_code_sha256": scope["seed_custody"]["origin_gate_code_sha256"],
            },
            "bridge": bridge,
            "ashlar_source": scope["ashlar_source"],
            "destination_id": scope["publication"]["destination_id"],
            "scope": "one owned PostgreSQL start; preserve/restore and .48 custody; one exact .48-to-.51 migration; public source/view/page recheck; one no-change genesis Delta publication with independent verification and protected ACK; one approved seven-operation seed; one independently verified actual continuation with protected ACK; stop",
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            scope_path = root / "scope.json"
            approval_path = root / "inert-approval.json"
            scope_path.write_bytes(scope_raw)
            approval_path.write_text(json.dumps(approval), encoding="utf-8")
            validated, returned_raw = candidate._validate_scope_approval(
                scope_path, approval_path, here / "r20_post_migration_admission.py",
                here / "r20_pre_migration_custody_worker.py", here / "run_supervisor_r20.py")
        self.assertEqual(returned_raw, scope_raw)
        self.assertEqual(validated["recovery_source_custody"], candidate.RECOVERY_SOURCE_CUSTODY)

    def test_sdk010_bridge_probe_matches_all_six_roles_without_native_effects(self):
        scope = json.loads((HERE / "full-lifecycle-scope-proposal-r20.json").read_bytes())
        result = candidate._verify_delta4_bridge(scope)
        self.assertEqual(result["profile"], "ashlar-r20-local-delta4-bridge-probe/0.1")
        self.assertEqual(result["realization_profile"], "ashlar-local-current-delta4-umf-source-sdk010/0.1")
        self.assertEqual(result["sdk_version"], "0.10.0")
        self.assertIs(result["native_effects"], False)
        self.assertEqual({item["carrier_role"] for item in result["proposals"]},
                         {"object_current", "edge_current", "tombstone", "whole_source_history",
                          "publication_manifest", "publication_attempt_phase"})

    def test_prefix_gate_requires_actual_receipt_bytes_and_page_archive_files(self):
        template = json.loads((here / "full-lifecycle-scope-proposal-r20.json").read_bytes())
        with self.assertRaises(candidate.AdmissionError):
            candidate._verify_prefix_receipt(template, b"{}\n")

    def test_migration_component_uses_live_connection_and_does_not_manage_server_lifecycle(self):
        import inspect
        signature = inspect.signature(candidate.run_migration_stage)
        self.assertEqual(tuple(signature.parameters),
                         ("scope_path", "approval_path", "worker_path", "prefix_worker_path",
                          "conn_uri", "prefix_receipt_raw"))
        source = inspect.getsource(candidate.run_migration_stage)
        self.assertNotIn("pg_ctl", source)
        self.assertNotIn("start\"]", source)
        self.assertIn("SHOW data_directory", source)
        self.assertIn("observed_data == restored", source)

    def test_frozen_genesis_cursor_is_the_page_next_checkpoint(self):
        import inspect
        source = inspect.getsource(candidate._verify_prefix_receipt)
        readback = inspect.getsource(candidate._run_migration_connected)
        self.assertIn('page.get("next_checkpoint_sha256") == FROZEN_CUSTODY', source)
        self.assertIn('page_after.get("next_checkpoint_sha256") == FROZEN_CUSTODY', readback)
        self.assertNotIn('page.get("prior_checkpoint_sha256") == FROZEN_CUSTODY', source)
        self.assertNotIn('page_after.get("prior_checkpoint_sha256") == FROZEN_CUSTODY', readback)

    def test_composed_worker_durably_binds_custody_before_migration_under_one_start(self):
        import inspect
        source = inspect.getsource(candidate.run_one_start_prefix_and_migration)
        self.assertIn('write_new(root / "pre-migration-custody-live.json", raw)', source)
        self.assertLess(source.index('write_new(root / "pre-migration-custody-live.json", raw)'),
                        source.index('migration = run_migration_stage('))
        self.assertIn('live_lifecycle=live_lifecycle', source)
        self.assertIn('signal.alarm(scope["limits"]["child_deadline_seconds"])', source)
        self.assertIn('root / "lifecycle-stop-receipt.json"', source)
        self.assertIn('"continuation_acknowledged"', source)
        self.assertIn('attempt_root_bytes_max', source)

    def test_full_graph_lifecycle_has_no_unavailable_private_origin_module(self):
        import inspect
        source = inspect.getsource(candidate._run_post_migration_graph_lifecycle)
        self.assertNotIn("ashlar_host.r20_native_origin_gate", source)
        self.assertIn("ORIGIN_MAP_SHA256", source)
        self.assertIn("run_continue_truss_graph_command", source)
        self.assertIn('umf_sdk_version=scope["bridge"]["sdk_version"]', source)

    def test_one_start_composition_runs_live_callback_and_hash_binds_durable_receipts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            output = root / "attempt"
            scope = {
                "profile": candidate.FULL_LIFECYCLE_SCOPE,
                "output_root": str(output),
                "source": {"worker_sha256": "0" * 64},
                "workers": {"custody_prefix_sha256": "1" * 64},
                "source_custody": dict(prefix_worker.FROZEN_SOURCE_CUSTODY),
                "limits": {"child_deadline_seconds": 900,
                           "attempt_root_bytes_max": 16 * 1024 * 1024},
            }
            scope_raw = b"frozen, test-only full scope bytes\n"
            prefix_observed = {
                "profile": "ashlar-truss051-r20-pre-migration-custody/0.1",
                "status": "custody_prefix_verified_migration_not_invoked",
                "source_custody": {"source_sha256": candidate.FROZEN_CUSTODY["expected_source_sha256"]},
                "genesis_page": {"next_checkpoint_sha256":
                    candidate.FROZEN_CUSTODY["expected_genesis_checkpoint_sha256"]},
                "migration_invoked": False, "spark_started": False,
            }
            lifecycle = {
                "migration_invoked": True, "publication_invoked": True,
                "genesis_acknowledged": True, "seed_invoked": True,
                "continuation_invoked": True, "continuation_acknowledged": True,
                "stage_receipts": {"complete": "a" * 64},
            }
            calls = []

            def fake_run_prefix(_scope, _source_root, _helper, *, stage, operations,
                                live_lifecycle):
                calls.append("prefix-started")
                output.mkdir(mode=0o700)
                stage("attempt-created")
                calls.append("custody-complete")
                returned = live_lifecycle(prefix_observed, "host=/approved/socket port=55439 dbname=postgres user=postgres")
                self.assertEqual(returned, lifecycle)
                final = {**prefix_observed, **lifecycle,
                         "status": "custody_prefix_and_live_lifecycle_completed",
                         "pg_stopped": True, "effect_state": {"native_effects": "stopped"}}
                prefix_worker.write_new(output / "lifecycle-stop-receipt.json", final)
                calls.append("prefix-stopped")
                return final

            def migration(*args):
                calls.append("migration")
                pre = output / "pre-migration-custody-live.json"
                self.assertTrue(pre.is_file(), "custody receipt must be durable before migration")
                self.assertEqual(json.loads(pre.read_bytes()), prefix_observed)
                self.assertIn("dbname=postgres", args[4])
                self.assertEqual(args[5], pre.read_bytes())
                return {"migration_invoked": True, "readback": "mocked exact stage"}

            def graph_lifecycle(_scope, _conn_uri, migration_result):
                calls.append("publication-seed-continuation")
                self.assertTrue(migration_result["migration_invoked"])
                return lifecycle

            with (patch.object(candidate, "_validate_scope_approval",
                               return_value=(scope, scope_raw)),
                  patch.object(candidate, "verify_installed_runtime",
                               return_value={"native_migration_invoked": False}),
                  patch.object(candidate, "_verify_delta4_bridge",
                               return_value={"profile": "effect-free mocked bridge", "native_effects": False}),
                  patch.object(candidate, "run_migration_stage", side_effect=migration),
                  patch.object(candidate, "_run_post_migration_graph_lifecycle",
                               side_effect=graph_lifecycle),
                  patch.object(prefix_worker, "verify_scope"),
                  patch.object(prefix_worker, "run_prefix", side_effect=fake_run_prefix)):
                receipt = candidate.run_one_start_prefix_and_migration(
                    prefix_worker, root / "scope.json", root / "approval.json",
                    root / "migration.py", root / "prefix.py", root, root / "helper.py")

            self.assertEqual(calls, ["prefix-started", "custody-complete", "migration",
                                     "publication-seed-continuation", "prefix-stopped"])
            self.assertEqual(receipt["status"], "one_start_full_lifecycle_complete_and_stopped")
            self.assertTrue(receipt["single_postgres_start"])
            self.assertTrue(receipt["postgres_stopped_by_prefix_finally"])
            self.assertEqual(receipt["pre_migration_receipt_sha256"],
                             candidate.sha((json.dumps(prefix_observed, sort_keys=True,
                                 separators=(",", ":"), allow_nan=False) + "\n").encode()))
            self.assertEqual(json.loads((output / "lifecycle-stop-receipt.json").read_bytes())[
                             "continuation_acknowledged"], True)


if __name__ == "__main__":
    unittest.main()
