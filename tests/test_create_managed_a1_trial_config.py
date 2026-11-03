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
    INITIALIZER_TIMEOUT_SECONDS, MAX_PAID_TASK_SECONDS,
    MAX_SOURCE_BOOTSTRAP_SECONDS, PUBLISHER_TIMEOUT_SECONDS,
    RUNTIME_PROBE_TASK_SECONDS, VERIFIER_TIMEOUT_SECONDS, build_payload,
)
from ashlar_host.configured_local_runtime import PROFILE, MANAGED_SERVERLESS_PROFILE


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
        self.assertEqual(paid_cap_seconds, 630)
        self.assertEqual(paid_cap_seconds + RUNTIME_PROBE_TASK_SECONDS,
                         MAX_PAID_TASK_SECONDS)
        self.assertEqual(630 + MAX_SOURCE_BOOTSTRAP_SECONDS + 45,
                         config["maximum_wall_seconds"] - config["cleanup_reserve_seconds"])
        self.assertEqual(config["cost"]["authorized_total_dbu"], 128.0)
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
