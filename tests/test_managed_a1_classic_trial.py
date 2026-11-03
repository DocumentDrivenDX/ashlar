import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
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
            args.extend(["--dependency", str(trial.CLASSIC_REQUIREMENTS_LOCAL) +
                "=-r " + trial.CLASSIC_REQUIREMENTS_REMOTE])
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

    def test_requirements_mapping_is_exact_and_hashed(self):
        locked = (str(trial.CLASSIC_REQUIREMENTS_LOCAL) + "=-r " +
                  trial.CLASSIC_REQUIREMENTS_REMOTE)
        trial._validate_dependency_mappings([locked])

        with mock.patch.object(trial, "CLASSIC_REQUIREMENTS_SHA256", "0" * 64):
            with self.assertRaisesRegex(trial.Refusal, "exact locked artifact"):
                trial._validate_dependency_mappings([locked])

        with tempfile.TemporaryDirectory(prefix="classic-wrong-requirements-") as temp:
            wrong = Path(temp) / trial.CLASSIC_REQUIREMENTS_LOCAL.name
            wrong.write_bytes(trial.CLASSIC_REQUIREMENTS_LOCAL.read_bytes())
            wrong_path = str(wrong) + "=-r " + trial.CLASSIC_REQUIREMENTS_REMOTE
            with self.assertRaisesRegex(trial.Refusal, "exact locked artifact"):
                trial._validate_dependency_mappings([wrong_path])

    def test_real_cli_rejects_previous_positional_spark_version_before_api(self):
        cli = trial._cli_path()
        code, output = trial._run_process([
            str(cli), "--profile", trial.PROFILE, "clusters", "create",
            trial.SPARK_VERSION, "--json", "@" + str(trial.CREATE_SPEC),
            "--no-wait", "--output", "json"],
            deadline=time.monotonic() + 10, output_cap=4096)
        self.assertNotEqual(code, 0)
        self.assertIn(b"when --json flag is specified, no positional arguments are allowed",
                      output)
        self.assertIn(b"Provide 'spark_version' in your JSON input", output)

    def test_json_create_argv_uses_spec_without_positional_spark_version(self):
        spec = trial._verify_create_spec(trial.CREATE_SPEC)
        with tempfile.TemporaryDirectory(prefix="classic-fake-cli-") as temporary:
            temp = Path(temporary)
            argv_log = temp / "argv.txt"
            fake_cli = temp / "databricks"
            fake_cli.write_text("#!" + sys.executable + "\nimport json,sys\n"
                "open(" + repr(str(argv_log)) + ", 'w').write('\\n'.join(sys.argv[1:]))\n"
                "print('fake warning on stderr', file=sys.stderr)\n"
                "print(json.dumps({'cluster_id':'fake-id'}))\n")
            fake_cli.chmod(0o700)
            with mock.patch.dict(os.environ,
                    {"PATH": str(temp) + os.pathsep + os.environ.get("PATH", "")}):
                stderr = io.StringIO()
                with contextlib.redirect_stderr(stderr):
                    output = trial._databricks("clusters", "create", "--json",
                        "@" + str(trial.CREATE_SPEC), "--no-wait",
                        deadline=time.monotonic() + 10)
            self.assertEqual(json.loads(output), {"cluster_id": "fake-id"})
            self.assertEqual(stderr.getvalue(), "fake warning on stderr\n")
            argv = argv_log.read_text().splitlines()
            self.assertEqual(argv[2:7], ["clusters", "create", "--json",
                "@" + str(trial.CREATE_SPEC), "--no-wait"])
            self.assertNotIn(trial.SPARK_VERSION, argv)
            self.assertEqual(spec["spark_version"], trial.SPARK_VERSION)

    def test_cli_argument_refusal_is_durable_and_not_unknown_create(self):
        spec = trial._verify_create_spec(trial.CREATE_SPEC)
        parser_error = RuntimeError(
            "Databricks CLI returned nonzero: Error: when --json flag is specified, "
            "no positional arguments are allowed. Provide 'spark_version' in your JSON input")
        with tempfile.TemporaryDirectory(prefix="classic-preapi-refusal-") as temporary:
            temp = Path(temporary)
            state_path = temp / "state.json"
            state = {"profile": "ashlar-managed-classic-a1-trial-state/0.1",
                "create_attempted": False, "create_api_submitted": False}
            with (mock.patch.object(trial, "_find_unique_owned_cluster", return_value=None),
                  mock.patch.object(trial, "_cli_path", return_value=Path("/fake/databricks")),
                  mock.patch.object(trial, "_databricks", side_effect=parser_error)):
                with self.assertRaises(trial.CreateCliPreApiRefusal):
                    trial._create_or_find(spec, trial.CREATE_SPEC,
                        time.monotonic() + 10, state, state_path)
            retained = json.loads(state_path.read_text())
            self.assertFalse(retained["create_api_submitted"])
            self.assertEqual(retained["create_outcome"], "pre_api_cli_argument_refusal")
            self.assertIn("no positional arguments are allowed", retained["create_error"])
            self.assertFalse(retained["create_response_received"])

    def test_full_execute_fake_provider_preserves_durable_state_and_cleanup(self):
        spec = trial._verify_create_spec(trial.CREATE_SPEC)
        with tempfile.TemporaryDirectory(prefix="classic-full-fake-") as temporary:
            root = Path(temporary) / "trial-output"
            args = type("Args", (), {
                "profile": trial.PROFILE, "cluster_create_spec": trial.CREATE_SPEC,
                "trial_id": trial.TRIAL_ID, "notebook_root": "/Workspace/Shared/test",
                "local_receipt": Path("/receipt.json"), "evidence_dir": "evidence",
                "bun": Path("/bin/true"), "python": Path(sys.executable),
                "dependency": [], "output_root": root})()
            cluster = {"cluster_id": "fake-id", "cluster_name": spec["cluster_name"],
                "custom_tags": spec["custom_tags"],
                "creator_user_name": spec["single_user_name"],
                "single_user_name": spec["single_user_name"], "state": "RUNNING",
                "policy_id": spec["policy_id"], "node_type_id": spec["node_type_id"],
                "driver_node_type_id": spec["driver_node_type_id"],
                "spark_version": spec["spark_version"], "num_workers": spec["num_workers"],
                "data_security_mode": spec["data_security_mode"],
                "runtime_engine": spec["runtime_engine"],
                "autotermination_minutes": spec["autotermination_minutes"],
                "azure_attributes": spec["azure_attributes"],
                "spark_conf": spec["spark_conf"], "autoscale": None}
            lifecycle = {"terminated": False, "deleted": False, "commands": []}

            def fake_databricks(*command, deadline, output_cap=trial.MAX_COMMAND_OUTPUT):
                lifecycle["commands"].append(command)
                if command[:2] == ("auth", "describe"):
                    return json.dumps({"status": "success", "details":
                        {"host": trial.HOST}}).encode()
                if command[:2] == ("clusters", "list"):
                    return b"[]"
                if command[:2] == ("clusters", "create"):
                    self.assertEqual(command[2:], ("--json", "@" + str(trial.CREATE_SPEC),
                                                    "--no-wait"))
                    return b'{"cluster_id":"fake-id"}'
                if command[:2] == ("clusters", "get"):
                    current = dict(cluster)
                    if lifecycle["terminated"]:
                        current["state"] = "TERMINATED"
                    persisted = json.loads((root / "state.json").read_text())
                    self.assertEqual(persisted["state_path"], str(root / "state.json"))
                    self.assertEqual(persisted["watchdog_pid"], 42424)
                    return json.dumps(current).encode()
                if command[:2] == ("clusters", "delete"):
                    lifecycle["terminated"] = True
                    return b"{}"
                if command[:2] == ("clusters", "permanent-delete"):
                    lifecycle["deleted"] = True
                    return b"{}"
                raise AssertionError("Unexpected fake Databricks command: " + repr(command))

            class DummyWatchdog:
                pid = 42424
                def wait(self, timeout=None):
                    return 0

            def fake_generator(_args, _readback, config_path, remaining, _deadline, runtime):
                config = {"execution_profile": "classic_existing_cluster",
                    "maximum_wall_seconds": remaining,
                    "expected_runtime": {"clusterId": runtime["cluster_id"]},
                    "classic_cluster": {"cluster_id": runtime["cluster_id"],
                        "policy_id": trial.POLICY_ID,
                        "owner": runtime["single_user_name"]},
                    "cleanup_reserve_seconds": trial.RUNNER_CLEANUP_RESERVE_SECONDS}
                config_path.write_text(json.dumps(config))
                return config

            local = {"cli": "/fake/databricks", "cli_version": trial.CLI_VERSION,
                "python": str(args.python), "bun": str(args.bun),
                "local_receipt": str(args.local_receipt)}
            with (mock.patch.object(trial, "_local_preflight",
                        return_value=(spec, local, root, args.python)),
                  mock.patch.object(trial, "_spawn_watchdog", return_value=DummyWatchdog()),
                  mock.patch.object(trial, "_databricks", side_effect=fake_databricks),
                  mock.patch.object(trial, "_run_generator", side_effect=fake_generator),
                  mock.patch.object(trial, "_run_managed_runner", return_value={
                      "returncode": 0, "status": "acknowledged_and_cleaned",
                      "result": {"schema_absent_after_cleanup": True,
                                 "source_holder_closed": True}})):
                with contextlib.redirect_stdout(io.StringIO()):
                    code = trial._execute(args)
            self.assertEqual(code, 0)
            receipt = json.loads((root / "trial-receipt.json").read_text())
            state = json.loads((root / "state.json").read_text())
            self.assertEqual(receipt["status"], "passed")
            self.assertEqual(receipt["cluster_cleanup"]["status"], "deleted")
            self.assertEqual(state["state_path"], str(root / "state.json"))
            self.assertEqual(state["watchdog_pid"], 42424)
            self.assertEqual(state["create_api_submitted"], True)
            self.assertTrue(lifecycle["terminated"] and lifecycle["deleted"])
            self.assertTrue(any(command[:2] == ("clusters", "create") and
                trial.SPARK_VERSION not in command for command in lifecycle["commands"]))

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
