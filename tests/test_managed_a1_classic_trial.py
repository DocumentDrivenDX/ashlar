import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest import mock
from unittest import skipUnless
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools import run_managed_a1_classic_trial as trial


class ClassicTrialWrapperTests(unittest.TestCase):
    def test_cli_flat_cluster_list_is_bounded(self):
        with mock.patch.object(trial, "_databricks", return_value=b"[]"):
            self.assertEqual(trial._list_clusters(trial.time.monotonic() + 5), [])

        oversized = json.dumps([{}] * 10000).encode()
        with mock.patch.object(trial, "_databricks", return_value=oversized):
            with self.assertRaises(trial.Refusal):
                trial._list_clusters(trial.time.monotonic() + 5)

    def test_cleanup_uses_nonce_owner_even_if_effective_config_drifted(self):
        spec = trial._verify_create_spec(trial.CREATE_SPEC)
        cluster = {
            "cluster_id": "owned-123", "cluster_name": spec["cluster_name"],
            "custom_tags": spec["custom_tags"], "creator_user_name": spec["single_user_name"],
            "single_user_name": spec["single_user_name"], "state": "RUNNING",
            # Deliberate effective-config mismatch must not prevent cleanup.
            "policy_id": "changed-policy",
        }
        calls = []

        def fake(*args, deadline, output_cap=trial.MAX_COMMAND_OUTPUT):
            calls.append(args)
            if args[:2] == ("clusters", "get"):
                value = dict(cluster)
                if any(call[:2] == ("clusters", "delete") for call in calls):
                    value["state"] = "TERMINATED"
                return json.dumps(value).encode()
            if args[:2] == ("clusters", "list"):
                return b"[]"
            return b"{}"

        with mock.patch.object(trial, "_databricks", side_effect=fake):
            result = trial._cleanup_owned_cluster("owned-123", spec,
                                                   trial.time.monotonic() + 10)
        self.assertEqual(result["status"], "deleted")
        self.assertIn(("clusters", "delete", "owned-123", "--no-wait"), calls)
        self.assertIn(("clusters", "permanent-delete", "owned-123"), calls)

    def test_cleanup_refuses_foreign_nonce_owner_without_delete(self):
        spec = trial._verify_create_spec(trial.CREATE_SPEC)
        foreign = {"cluster_id": "foreign-123", "cluster_name": spec["cluster_name"],
            "custom_tags": {**spec["custom_tags"], "ashlar_owner": "other-owner"},
            "creator_user_name": spec["single_user_name"],
            "single_user_name": spec["single_user_name"], "state": "RUNNING"}
        calls = []

        def fake(*args, deadline, output_cap=trial.MAX_COMMAND_OUTPUT):
            calls.append(args)
            return json.dumps(foreign).encode()

        with mock.patch.object(trial, "_databricks", side_effect=fake):
            result = trial._cleanup_owned_cluster("foreign-123", spec,
                                                   trial.time.monotonic() + 10)
        self.assertEqual(result["status"], "cleanup_incomplete")
        self.assertFalse(any(args[:2] == ("clusters", "delete") or
                             args[:2] == ("clusters", "permanent-delete")
                             for args in calls))

    @skipUnless(Path("/private/tmp").is_dir(), "Mac qualification path is unavailable")
    def test_preflight_is_local_only_and_keeps_selected_python_path(self):
        repo_receipt = (trial.REPO / "docs/helix/04-build/evidence/"
            "current-upstream-release-20261102/r61-fresh-truss055-attempt13-continuation/"
            "qualification-closeout.json")
        self.assertTrue(repo_receipt.is_file())
        with tempfile.TemporaryDirectory(prefix="ashlar-classic-preflight-",
                                         dir="/private/tmp") as temporary:
            temp = Path(temporary)
            fake_cli = temp / "databricks"
            log = temp / "cli.log"
            fake_cli.write_text("#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"" +
                str(log) + "\"\nprintf 'Databricks CLI v1.21.0\\n'\n")
            fake_cli.chmod(0o700)
            fake_bun = temp / "bun"
            fake_bun.write_text("#!/bin/sh\nexit 0\n")
            fake_bun.chmod(0o700)
            dependency = temp / "candidate.whl"
            dependency.write_bytes(b"local preflight placeholder")
            selected_python = temp / "selected-venv-python"
            selected_python.symlink_to(Path(sys.executable).resolve())
            output_root = Path(temporary + "-output")
            output_root.unlink(missing_ok=True)
            args = ["--preflight-only", "--notebook-root", "/Workspace/Shared/ashlar",
                "--local-receipt", str(repo_receipt), "--dependency",
                str(dependency) + "=/Workspace/Shared/candidate.whl", "--bun", str(fake_bun),
                "--python", str(selected_python), "--output-root", str(output_root)]
            with mock.patch.dict(os.environ, {"PATH": str(temp) + os.pathsep + os.environ["PATH"]}):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    code = trial.main(args)
            self.assertEqual(code, 0, stdout.getvalue())
            result = json.loads(stdout.getvalue())
            self.assertEqual(result["status"], "preflight_passed_no_resource_effects")
            self.assertEqual(result["selected_python"], str(selected_python))
            self.assertEqual(log.read_text().splitlines(), ["version"])
            self.assertFalse(output_root.exists())

    def test_runner_wall_subtracts_outer_reserve_without_resetting_clock(self):
        global_deadline = 1000.0
        after_startup = 125.0
        wall = trial._runner_wall_seconds(global_deadline, after_startup)
        self.assertEqual(wall, 815)
        self.assertEqual(after_startup + wall, global_deadline -
                         trial.CLUSTER_CLEANUP_RESERVE_SECONDS)
        with self.assertRaises(trial.Refusal):
            trial._runner_wall_seconds(1000, 880)

    def test_unknown_create_is_not_reported_as_clean(self):
        state = {"create_attempted": True, "create_response_received": False}
        cleanup = {"status": "no_matching_owned_cluster"}
        self.assertTrue(trial._create_outcome_unknown(state, None, cleanup))
        self.assertFalse(trial._create_outcome_unknown(
            {"create_attempted": False}, None, cleanup))
        self.assertFalse(trial._create_outcome_unknown(
            {"create_attempted": True, "create_response_received": True}, None, cleanup))


if __name__ == "__main__":
    unittest.main()
