# SPDX-License-Identifier: MIT OR Apache-2.0
"""Inert closure check for the full managed A1 input constructor."""
import hashlib
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

from tools.create_managed_a1_trial_config import build_config
from tools.run_managed_a1_serverless import build_payload
from ashlar_host.configured_local_runtime import PROFILE


class CreateManagedA1TrialConfigTests(unittest.TestCase):
    def test_complete_config_passes_the_actual_runner_lock(self):
        runtime = {"runtimeName": "databricks-serverless", "sparkVersion": "4.2.0",
            "sparkBuildText": "Spark 4.2.0", "runtimeTag": "17.3.x-scala2.13",
            "principal": "owner@example.com", "catalog": "client_dev",
            "schema": "default", "protocol": PROFILE.identity,
            "serverlessApiProbe": {"profile": "ashlar-spark-connect-tag-api-probe/0.1",
                "newSessionDistinct": True, "tagAddedAndObserved": True,
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
        self.assertEqual(config["initializer"]["timeout_seconds"]
            + 2 * config["publisher"]["timeout_seconds"]
            + 4 * config["verifier"]["timeout_seconds"], 420)
        self.assertEqual(config["cost"]["authorized_total_dbu"], 128.0)
        self.assertLess(config["cost"]["estimated_total_usd"], 10)
        self.assertEqual(config["expected_runtime"]["protocol"], PROFILE.identity)
        self.assertEqual(len(config["initializer"]["dependencies"]), 1)


if __name__ == "__main__":
    unittest.main()
