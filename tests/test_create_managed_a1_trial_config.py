# SPDX-License-Identifier: MIT OR Apache-2.0
"""Inert closure check for the full managed A1 input constructor."""
import hashlib
import copy
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from types import SimpleNamespace

from tools.create_managed_a1_trial_config import build_config
from tools.run_managed_a1_serverless import (
    INITIALIZER_TIMEOUT_SECONDS, MAX_SOURCE_BOOTSTRAP_SECONDS,
    PUBLISHER_TIMEOUT_SECONDS, VERIFIER_TIMEOUT_SECONDS,
    WORST_SERVERLESS_DBUH, build_payload, _admit_job_readback, _admit_classic_cluster_readback,
    _job_settings, _validate_task_runtime, _require_independent_classic_task_processes,
)
from ashlar_host.configured_local_runtime import (PROFILE, MANAGED_SERVERLESS_PROFILE,
                                                  MANAGED_CLASSIC_PROFILE)


class CreateManagedA1TrialConfigTests(unittest.TestCase):
    def test_complete_config_passes_the_actual_runner_lock(self):
        runtime = {"runtimeName": "databricks-serverless", "sparkVersion": "4.2.0",
            "sparkBuildText": "Spark 4.2.0", "runtimeTag": "17.3.x-scala2.13",
            "principal": "owner@example.com", "catalog": "client_dev",
            "schema": "default", "protocol": PROFILE.identity,
            "serverlessApiProbe": {"profile": "ashlar-spark-connect-tag-api-probe/0.2",
                "providedJobSessionBorrowed": True, "tagAddedAndObserved": True,
                "taggedQueryRowCount": 1, "taggedQueryValue": 1,
                "interruptTagResponseType": "list", "interruptTagResponseCount": 0,
                "tagRemoved": True, "preexistingTagsRestored": True,
                "operationTerminationClaimed": False},
            "sideEffects": []}
        dependencies = [{"path": "/Workspace/Shared/ashlar/dist/ashlar.whl",
                         "sha256": "a" * 64}]
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
            receipt = Path(temporary) / "qualification.json"
            receipt.write_bytes(b"local qualification receipt")
            venv_bin = Path(temporary) / "venv" / "bin"
            venv_bin.mkdir(parents=True)
            venv_python = venv_bin / "python"
            venv_python.symlink_to(sys.executable)
            config = build_config(runtime_result=runtime, trial_id="a1-config-check",
                notebook_root="/Workspace/Shared/ashlar",
                environment_version=7, dependencies=dependencies,
                local_receipt=receipt, evidence_dir="docs/helix/04-build/evidence/test",
                bun_path=Path(shutil.which("bun")), python=venv_python)
            self.assertEqual(config["holder_bootstrap"]["python"], str(venv_python))
            self.assertTrue(Path(config["holder_bootstrap"]["python"]).is_symlink())
        payload, locked = build_payload(config)
        self.assertIs(locked, config)
        self.assertEqual(config["initializer"]["timeout_seconds"],
                         INITIALIZER_TIMEOUT_SECONDS)
        self.assertEqual(config["publisher"]["timeout_seconds"],
                         PUBLISHER_TIMEOUT_SECONDS)
        self.assertEqual(config["verifier"]["timeout_seconds"],
                         VERIFIER_TIMEOUT_SECONDS)
        paid_cap_seconds = (INITIALIZER_TIMEOUT_SECONDS + 2 * PUBLISHER_TIMEOUT_SECONDS
                            + 4 * VERIFIER_TIMEOUT_SECONDS)
        self.assertEqual(paid_cap_seconds, 1140)  # Stage ceilings are not additive wall time.
        self.assertEqual((INITIALIZER_TIMEOUT_SECONDS, PUBLISHER_TIMEOUT_SECONDS,
                          VERIFIER_TIMEOUT_SECONDS), (180, 180, 150))
        self.assertLessEqual(max(INITIALIZER_TIMEOUT_SECONDS, PUBLISHER_TIMEOUT_SECONDS,
                                 VERIFIER_TIMEOUT_SECONDS) + 15,
                             config["maximum_wall_seconds"] - config["cleanup_reserve_seconds"])
        self.assertLessEqual(MAX_SOURCE_BOOTSTRAP_SECONDS,
                             config["maximum_wall_seconds"] - config["cleanup_reserve_seconds"])
        self.assertEqual(MAX_SOURCE_BOOTSTRAP_SECONDS, 275)
        self.assertGreaterEqual(config["cost"]["authorized_total_dbu"],
            config["maximum_wall_seconds"] * WORST_SERVERLESS_DBUH / 3600)
        self.assertLessEqual(config["cost"]["authorized_total_dbu"] * 0.47
                             + config["cost"]["cleanup_reserve_usd"], 100)
        self.assertEqual(config["cost"]["authorized_total_dbu"], 166.0)
        self.assertLessEqual(166.0 * 0.47 + 20.0, 100.0)
        self.assertLess(config["cost"]["estimated_total_usd"], 10)
        self.assertEqual(config["expected_runtime"]["protocol"], MANAGED_SERVERLESS_PROFILE.identity)
        self.assertEqual(config["protocol_profile"]["identity"], MANAGED_SERVERLESS_PROFILE.identity)
        self.assertIn("v2Checkpoint", config["protocol_profile"]["features"])
        self.assertNotIn("futureFeature", config["protocol_profile"]["features"])
        self.assertEqual(len(config["initializer"]["dependencies"]), 1)
        altered = copy.deepcopy(config)
        altered["protocol_profile"]["features"].append("futureFeature")
        with self.assertRaisesRegex(ValueError, "versioned managed serverless reader protocol profile"):
            build_payload(altered)
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
            receipt = Path(temporary) / "qualification.json"
            receipt.write_bytes(b"local qualification receipt")
            with self.assertRaisesRegex(ValueError, "exact Databricks serverless identity"):
                build_config(runtime_result={**runtime, "sparkVersion": "4.3.0"},
                    trial_id="a1-config-check", notebook_root="/Workspace/Shared/ashlar",
                    environment_version=7, dependencies=dependencies, local_receipt=receipt,
                    evidence_dir="docs/helix/04-build/evidence/test",
                    bun_path=Path(shutil.which("bun")), python=Path(sys.executable))

    def test_classic_config_locks_live_dedicated_cluster_and_jobs_payload(self):
        from tools.run_managed_a1_serverless import HOST
        trial_id = "a1-classic-check"
        owner = "owner@example.test"
        cluster = {"cluster_id": "0712-123456-abc123", "cluster_name": "ashlar-release-classic-test",
            "state": "RUNNING", "single_user_name": owner,
            "node_type_id": "Standard_D4ds_v5", "driver_node_type_id": "Standard_D4ds_v5",
            "spark_version": "19.x-scala2.13", "spark_conf": {
                "spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*]",
                "spark.sql.session.timeZone": "UTC"},
            "num_workers": 0, "data_security_mode": "SINGLE_USER", "runtime_engine": "STANDARD",
            "autotermination_minutes": 10, "policy_id": "001654FA4AA08D1A",
            "custom_tags": {"ashlar_owner": "ashlar-release",
                "ashlar_trial": trial_id.replace("-", "_"), "ashlar_nonce": "a" * 32}}
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
            root = Path(temporary)
            receipt = root / "qualification.json"
            receipt.write_bytes(b"local qualification receipt")
            venv_bin = root / "venv" / "bin"
            venv_bin.mkdir(parents=True)
            python = venv_bin / "python"
            python.symlink_to(sys.executable)
            config = build_config(runtime_result={"principal": owner}, trial_id=trial_id,
                notebook_root="/Workspace/Users/owner@example.test/release",
                environment_version=None,
                dependencies=[{"path": "/Workspace/Shared/ashlar/ashlar.whl", "sha256": "b" * 64}],
                local_receipt=receipt, evidence_dir="docs/helix/04-build/evidence/test",
                bun_path=Path(shutil.which("bun")), python=python,
                execution_profile="classic_existing_cluster", classic_cluster_readback=cluster,
                remaining_wall_seconds=840)
        payload, _ = build_payload(config)
        self.assertEqual(config["execution_profile"], "classic_existing_cluster")
        self.assertEqual(config["maximum_wall_seconds"], 840)
        self.assertEqual(config["protocol_profile"]["identity"], MANAGED_CLASSIC_PROFILE.identity)
        self.assertEqual(config["cost"]["classic_cluster_usd_ceiling"], 5.0)
        for stage in ("initializer", "publisher", "verifier"):
            self.assertIsNone(payload[stage]["environment"])
            self.assertEqual(payload[stage]["task"]["existing_cluster_id"], cluster["cluster_id"])
            self.assertEqual(payload[stage]["task"]["libraries"], [
                {"whl": "/Workspace/Shared/ashlar/ashlar.whl"}])
            self.assertNotIn("environment_key", payload[stage]["task"])
        expected = _job_settings(config, "initializer", payload["initializer"])
        actual = {"settings": copy.deepcopy(expected), "run_as_user_name": owner}
        actual["settings"]["tasks"][0]["notebook_task"]["source"] = "WORKSPACE"
        _admit_job_readback(actual, expected, trial_id)
        changed = copy.deepcopy(actual)
        changed["settings"]["tasks"][0]["existing_cluster_id"] = "shared-default"
        with self.assertRaisesRegex(ValueError, "artifact lock"):
            _admit_job_readback(changed, expected, trial_id)
        _admit_classic_cluster_readback(cluster, config["classic_cluster"])
        changed_cluster = dict(cluster, num_workers=1)
        with self.assertRaisesRegex(ValueError, "exact live owner/configuration lock"):
            _admit_classic_cluster_readback(changed_cluster, config["classic_cluster"])

    def test_classic_task_identity_requires_task_process_and_separation(self):
        expected = {"runtimeName": "databricks-jobs-cluster", "sparkVersion": "4.2.0",
            "sparkBuildText": None, "runtimeTag": "19.x-scala2.13",
            "principal": "owner@example.test", "catalog": "client_dev", "schema": "trial",
            "protocol": MANAGED_CLASSIC_PROFILE.identity, "clusterId": "cluster-1",
            "sessionTimeZone": "UTC"}
        opening = dict(expected, sparkApplicationId="app-1")
        task = {"runtimeIdentity": {"opening": opening, "closing": opening},
            "taskProcess": {"pid": 123, "nonce": "a" * 32}}
        _validate_task_runtime(task, expected, "publisher")
        verifier = {"taskProcess": {"pid": 456, "nonce": "b" * 32}}
        _require_independent_classic_task_processes(task, verifier, "A0")
        with self.assertRaisesRegex(ValueError, "distinct task processes"):
            _require_independent_classic_task_processes(task,
                {"taskProcess": task["taskProcess"]}, "A0")
        task.pop("taskProcess")
        with self.assertRaisesRegex(ValueError, "task-local process identity"):
            _validate_task_runtime(task, expected, "publisher")

    def test_selected_managed_profile_flows_through_actual_environment_observer(self):
        from ashlar_host.managed_a1_serverless_tasks import _environment

        class Row(dict):
            def asDict(self, recursive=True):
                return dict(self)

        class Frame:
            def __init__(self, row):
                self.rows=[Row(row)]
                self.schema=SimpleNamespace(fields=[SimpleNamespace(name=name,
                    dataType=SimpleNamespace(simpleString=lambda: "string")) for name in row])
            def limit(self, count):
                return self
            def collect(self):
                return self.rows

        class Spark:
            version="4.2.0"
            conf=type("Conf", (), {"get": lambda self, key: None})()
            def sql(self, query, args=None):
                if "current_user()" in query:
                    return Frame({"principal":"owner@example.com","catalog":"client_dev",
                                  "schema":"trial_schema"})
                if "version() AS sparkBuildText" in query:
                    return Frame({"sparkBuildText":"Spark SQL build"})
                if "current_timezone()" in query:
                    return Frame({"session_timezone":"UTC"})
                raise AssertionError("Unexpected environment SQL")

        expected={"runtimeName":"databricks-serverless","sparkVersion":"4.2.0",
            "principal":"owner@example.com","catalog":"client_dev","schema":"trial_schema",
            "protocol":MANAGED_SERVERLESS_PROFILE.identity,"sparkBuildText":"Spark SQL build",
            "runtimeTag":None}
        observed=_environment(Spark(),MANAGED_SERVERLESS_PROFILE,expected)
        self.assertEqual(observed["protocol"],MANAGED_SERVERLESS_PROFILE.identity)


if __name__ == "__main__":
    unittest.main()
