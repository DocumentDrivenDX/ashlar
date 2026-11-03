# SPDX-License-Identifier: MIT OR Apache-2.0
"""Focused local controller-protocol and uncertain-submit recovery checks."""
import json
import hashlib
import os
import copy
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from tools.run_managed_a1_serverless import (
    _LocalPinStages, _recover_run_after_lost_response, _run_stage, run,
    _validate_ack_settlement,
    _verify_terminal_proof,
    _admit_job_readback, _job_settings,
    _write_private_json, DatabricksCLI,
)
from ashlar_host.a1_ack_attempt import A1AckSettlement


class _PinHandle:
    def __init__(self, events): self.events = events
    def renew(self): self.events.append("renew")
    def close(self): self.events.append("close")


class _PinOwner:
    native_timeout_seconds = 60
    def __init__(self): self.events = []
    def open_native_pin(self):
        self.events.append("open")
        return _PinHandle(self.events)


class _RunsAPI:
    def __init__(self, pages): self.pages = iter(pages); self.cancelled = []
    def call(self, method, path, body=None):
        if path.startswith("/api/2.2/jobs/runs/list"):
            return next(self.pages)
        if path == "/api/2.2/jobs/runs/cancel":
            self.cancelled.append(body["run_id"])
            return {}
        raise AssertionError((method, path, body))


class _LostRunNowAPI:
    def __init__(self, *, visible=True):
        self.deadline = None
        self.visible = visible
        self.settings = None
        self.run_now_count = 0
        self.run_list_count = 0
        self.deleted = False

    def call(self, method, path, body=None):
        if method == "post" and path == "/api/2.2/jobs/create":
            self.settings = body
            return {"job_id": 17}
        if method == "get" and path == "/api/2.2/jobs/get?job_id=17":
            return {"settings": self.settings,
                    "run_as_user_name": self.settings["run_as"]["user_name"]}
        if method == "get" and path.startswith("/api/2.2/jobs/runs/list?"):
            self.run_list_count += 1
            rows = [] if self.run_list_count == 1 or not self.visible else [{"run_id": 29,
                "job_id": 17, "start_time": int(time.time() * 1000)}]
            return {"runs": rows, "next_page_token": None}
        if method == "post" and path == "/api/2.2/jobs/run-now":
            self.run_now_count += 1
            raise TimeoutError("response lost after server acceptance")
        if method == "get" and path == "/api/2.2/jobs/runs/get?run_id=29":
            return {"run_id": 29, "state": {"life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS"}, "tasks": []}
        if method == "get" and path == "/api/2.2/jobs/runs/get-output?run_id=29":
            return {"notebook_output": {"result": '{"ok":true}'}}
        if method == "delete" and path == "/api/2.2/jobs/delete":
            self.deleted = True
            return {}
        raise AssertionError((method, path, body))


class ManagedA1ServerlessRunnerTests(unittest.TestCase):
    def test_workspace_exports_use_source_for_notebooks_and_auto_for_files(self):
        calls = []
        def fake_run(argv, **kwargs):
            calls.append(argv)
            return type("Completed", (), {"returncode": 0, "stderr": ""})()
        with patch("tools.run_managed_a1_serverless.subprocess.run", fake_run):
            cli = DatabricksCLI("aidev-cus")
            cli.export_workspace("/Workspace/notebook", "/tmp/notebook.py", notebook=True)
            cli.export_workspace("/Workspace/wheel.whl", "/tmp/wheel.whl", notebook=False)
        self.assertEqual(calls[0][calls[0].index("--format") + 1], "SOURCE")
        self.assertEqual(calls[1][calls[1].index("--format") + 1], "AUTO")

    def test_actual_provider_probe_readback_admits_only_documented_defaults(self):
        from tools.prepare_a1_serverless_runtime_probe import build_payload
        from pathlib import Path

        submitted_path = Path("/private/tmp/ashlar-runtime-probe-job.json")
        readback_path = Path("/private/tmp/ashlar-runtime-probe-job-readback.json")
        if not submitted_path.is_file() or not readback_path.is_file():
            self.skipTest("Captured actual serverless Jobs probe pair is unavailable")
        submitted = json.loads(submitted_path.read_text(encoding="utf-8"))
        readback = json.loads(readback_path.read_text(encoding="utf-8"))
        protocol = submitted["tasks"][0]["notebook_task"]["base_parameters"]["protocol_identity"]
        current = build_payload(submitted["tasks"][0]["notebook_task"]["notebook_path"],
            Path(__file__).resolve().parents[1] / "notebooks/managed_a1/runtime_probe.py",
            int(submitted["environments"][0]["spec"]["environment_version"]),
            submitted["run_as"]["user_name"], protocol)
        self.assertNotIn("max_retries", current)
        self.assertNotIn("retry_on_timeout", current)
        self.assertEqual(current["tasks"][0]["max_retries"], 0)
        self.assertIs(current["tasks"][0]["retry_on_timeout"], False)
        a1_settings = _job_settings({"trial_id": "a1-readback-test",
            "expected_principal": submitted["run_as"]["user_name"],
            "initializer": {"timeout_seconds": 60}}, "initializer", {
                "task": {"task_key": "initializer", "depends_on": [],
                    "timeout_seconds": 60, "max_retries": 0, "retry_on_timeout": False,
                    "disable_auto_optimization": True,
                    "notebook_task": {"notebook_path": "/Workspace/initialize",
                        "source": "WORKSPACE", "base_parameters": {"task_parameters_json": "{}"}},
                    "environment_key": "initializer_env"},
                "environment": {"environment_key": "initializer_env",
                    "spec": {"environment_version": "6", "dependencies": [
                        {"library": {"whl": "/Workspace/ashlar.whl"}}]}}})
        self.assertNotIn("max_retries", a1_settings)
        self.assertNotIn("retry_on_timeout", a1_settings)
        self.assertEqual(a1_settings["tasks"][0]["max_retries"], 0)
        self.assertIs(a1_settings["tasks"][0]["retry_on_timeout"], False)
        self.assertIs(a1_settings["tasks"][0]["disable_auto_optimization"], True)
        expected = copy.deepcopy(submitted)
        expected.pop("max_retries", None)
        expected.pop("retry_on_timeout", None)
        expected["tasks"][0]["notebook_task"]["source"] = "WORKSPACE"
        _admit_job_readback(readback, expected, None)

        changed = copy.deepcopy(readback)
        changed["settings"]["max_retries"] = 1
        with self.assertRaisesRegex(ValueError, "job-level retries"):
            _admit_job_readback(changed, expected, None)
        changed = copy.deepcopy(readback)
        changed["settings"]["tasks"][0]["notebook_task"]["source"] = "GIT"
        with self.assertRaisesRegex(ValueError, "pinned serverless artifact lock"):
            _admit_job_readback(changed, expected, None)
        changed = copy.deepcopy(readback)
        changed["settings"]["tasks"][0]["max_retries"] = 1
        with self.assertRaisesRegex(ValueError, "pinned serverless artifact lock"):
            _admit_job_readback(changed, expected, None)
        changed = copy.deepcopy(readback)
        changed["settings"]["tasks"][0]["existing_cluster_id"] = "shared"
        with self.assertRaisesRegex(ValueError, "unselected task settings"):
            _admit_job_readback(changed, expected, None)
        changed = copy.deepcopy(readback)
        changed["settings"]["environments"][0]["spec"]["dependencies"] = [
            "/Volumes/client_dev/other.whl"]
        with self.assertRaisesRegex(ValueError, "artifact locks"):
            _admit_job_readback(changed, expected, None)

    def test_real_holder_settlement_matched_is_accepted_and_exact_bytes_are_checked(self):
        request, manifest, snapshot, next_checkpoint, receipt = (
            b"request", b"manifest", b"selection", b"next", b"receipt")
        settlement = A1AckSettlement("matched", next_checkpoint, receipt,
            hashlib.sha256(request).hexdigest(),
            hashlib.sha256(manifest).hexdigest(),
            hashlib.sha256(snapshot).hexdigest())
        _validate_ack_settlement(settlement, request_bytes=request,
            manifest_bytes=manifest, snapshot_bytes=snapshot,
            next_checkpoint_bytes=next_checkpoint, receipt_bytes=receipt)
        with self.assertRaisesRegex(ValueError, "did not reconcile"):
            _validate_ack_settlement(settlement, request_bytes=request,
                manifest_bytes=manifest, snapshot_bytes=snapshot,
                next_checkpoint_bytes=b"different", receipt_bytes=receipt)

    def test_ack_settlement_rejects_old_runner_only_outcome(self):
        settlement = A1AckSettlement("acknowledged_exactly_reconciled", b"next", b"receipt",
            "a", "b", "c")
        with self.assertRaisesRegex(ValueError, "did not reconcile"):
            _validate_ack_settlement(settlement, request_bytes=b"request",
                manifest_bytes=b"manifest", snapshot_bytes=b"selection",
                next_checkpoint_bytes=b"next", receipt_bytes=b"receipt")

    def test_parent_native_pin_survives_verifier_until_source_ack(self):
        owner = _PinOwner()
        stages = _LocalPinStages(owner, lifetime_seconds=60)
        stages("open")
        stages("pre_ack")
        stages("close")
        self.assertEqual(owner.events, ["open", "renew", "renew"])
        self.assertEqual(stages.stages, ["open", "pre_ack", "close"])
        self.assertIsNotNone(stages.handle)
        stages.renew_for_ack()
        self.assertEqual(owner.events[-1], "renew")
        stages.begin_post_ack_observation()
        stages("open")
        stages("pre_ack")
        stages("close")
        stages.renew_after_post_ack_observation()
        stages.close_after_ack()
        self.assertEqual(owner.events[-1], "close")
        self.assertIsNone(stages.handle)

    def test_post_ack_close_requires_its_own_terminal_native_proof(self):
        request, manifest, receipt = b"request", b"manifest", b"receipt"
        vector = {"authority": "managed-current-spark:owner",
            "scope_kind": "publication", "scope_id": "pub-1",
            "custody_digest": "f" * 64,
            "targets": {role: ["uuid-" + role, 1] for role in (
                "object_current", "edge_current", "tombstone", "whole_source_history")}}
        targets = {role: {"uuid": item[0], "version": item[1]}
                   for role, item in vector["targets"].items()}
        observation = {"profile": "ashlar-managed-operational-retention/0.1",
            "interval_id": "interval", "outcome": "observed_available_at_checks",
            "authority": "read_observation_only_no_source_ack",
            "maximum_elapsed_seconds": 60,
            "observations": [{"stage": stage, "predictive_optimization": "ENABLE",
                "vector_targets": targets} for stage in ("open", "pre_ack", "close")]}
        observation_raw = json.dumps(observation, sort_keys=True,
            separators=(",", ":")).encode()
        output = {"profile": "ashlar-managed-a1-verifier-result/0.1",
            "phase": "a0", "observationPhase": "post_ack_close",
            "runtimeIdentity": {"opening": {}, "closing": {}},
            "predictiveOptimization": "ENABLE", "managedObservation": observation,
            "managedObservationSha256": hashlib.sha256(observation_raw).hexdigest(),
            "requestSha256": hashlib.sha256(request).hexdigest(),
            "manifestSha256": hashlib.sha256(manifest).hexdigest(),
            "destinationReceiptSha256": hashlib.sha256(receipt).hexdigest(),
            "vector": vector}
        proof = _verify_terminal_proof(output, 10, 11, request, manifest, receipt,
            "a" * 64, vector, phase="a0", observation_phase="post_ack_close")
        self.assertEqual(proof["outcome"], "durably_verified")
        with self.assertRaisesRegex(ValueError, "wrong source-ACK observation phase"):
            _verify_terminal_proof(output, 10, 11, request, manifest, receipt,
                "a" * 64, vector, phase="a0", observation_phase="pre_ack")

    def test_lost_run_now_response_recovers_unique_run_from_fresh_job(self):
        api = _RunsAPI([{"runs": [{"run_id": 418, "job_id": 7,
            "start_time": 100100}], "next_page_token": None}])
        state = []
        found = _recover_run_after_lost_response(api, 7, "token", 100000,
            time.monotonic() + 5, set(), state.append)
        self.assertEqual(found, 418)
        self.assertEqual(state[-1]["submission_state"],
                         "run_id_recovered_after_lost_response")
        self.assertEqual(api.cancelled, [])

    def test_ambiguous_runs_are_cancelled_and_recorded_unknown_without_retry(self):
        api = _RunsAPI([{"runs": [
            {"run_id": 418, "job_id": 7, "start_time": 100100},
            {"run_id": 419, "job_id": 7, "start_time": 100200}],
            "next_page_token": None}])
        state = []
        found = _recover_run_after_lost_response(api, 7, "token", 100000,
            time.monotonic() + 5, set(), state.append)
        self.assertIsNone(found)
        self.assertEqual(api.cancelled, [418, 419])
        self.assertEqual(state[-1]["submission_state"],
                         "multiple_runs_unknown_cancel_requested")

    def test_lost_run_now_response_is_reconciled_without_resubmission(self):
        api = _LostRunNowAPI()
        config = {"trial_id": "a1-regression", "expected_principal": "owner@example.com",
            "initializer": {"timeout_seconds": 60}}
        task_payload = {"task": {"task_key": "initializer", "depends_on": [],
            "timeout_seconds": 60, "max_retries": 0, "retry_on_timeout": False,
            "disable_auto_optimization": True,
            "notebook_task": {"notebook_path": "/Workspace/initialize",
                              "base_parameters": {"task_parameters_json": "{}"}},
            "environment_key": "initializer_env"},
            "environment": {"environment_key": "initializer_env",
                            "spec": {"environment_version": "1", "dependencies": ["/Volumes/x.whl"]}}}
        recovery = {"jobs": {}, "known_run_ids": {}}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "private"
            root.mkdir(mode=0o700); os.chmod(root, 0o700)
            run_id, task_run_id, snapshot, output = _run_stage(api, config,
                "initializer", task_payload, {"task_parameters_json": "{}"},
                time.monotonic() + 10, cleanup_deadline=time.monotonic() + 15,
                recovery_state=recovery, recovery_path=root / "recovery.json")
        self.assertEqual((run_id, task_run_id), (29, 29))
        self.assertEqual(snapshot["state"]["result_state"], "SUCCESS")
        self.assertEqual(output, {"ok": True})
        self.assertEqual(api.run_now_count, 1)
        self.assertTrue(api.deleted)
        self.assertEqual(recovery["known_run_ids"]["initializer"], 29)
        self.assertEqual(recovery["jobs"]["initializer"]["run_now_state"],
                         "terminal_success")

    def test_phase_jobs_reuse_role_timeout_but_get_distinct_job_identity(self):
        api = _LostRunNowAPI()
        config = {"trial_id": "a1-phases", "expected_principal": "owner@example.com",
            "publisher": {"timeout_seconds": 120}}
        task_payload = {"task": {"task_key": "publisher", "depends_on": [],
            "timeout_seconds": 120, "max_retries": 0, "retry_on_timeout": False,
            "disable_auto_optimization": True,
            "notebook_task": {"notebook_path": "/Workspace/publisher",
                              "base_parameters": {"task_parameters_json": "{}"}},
            "environment_key": "publisher_env"},
            "environment": {"environment_key": "publisher_env",
                            "spec": {"environment_version": "1", "dependencies": ["/Volumes/x.whl"]}}}
        recovery = {"jobs": {}, "known_run_ids": {}}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "private"
            root.mkdir(mode=0o700); os.chmod(root, 0o700)
            _run_stage(api, config, "a0-publisher", task_payload,
                {"task_parameters_json": "{}"}, time.monotonic() + 10,
                cleanup_deadline=time.monotonic() + 15,
                recovery_state=recovery, recovery_path=root / "recovery.json",
                config_key="publisher")
        self.assertEqual(api.settings["name"], "ashlar-a1-a1-phases-a0-publisher")
        self.assertEqual(api.settings["timeout_seconds"], 120)
        self.assertEqual(api.run_now_count, 1)

    def test_unseen_lost_run_now_is_durably_marked_unknown_without_resubmit(self):
        api = _LostRunNowAPI(visible=False)
        config = {"trial_id": "a1-unknown", "expected_principal": "owner@example.com",
            "initializer": {"timeout_seconds": 60}}
        task_payload = {"task": {"task_key": "initializer", "depends_on": [],
            "timeout_seconds": 60, "max_retries": 0, "retry_on_timeout": False,
            "disable_auto_optimization": True,
            "notebook_task": {"notebook_path": "/Workspace/initialize",
                              "base_parameters": {"task_parameters_json": "{}"}},
            "environment_key": "initializer_env"},
            "environment": {"environment_key": "initializer_env",
                            "spec": {"environment_version": "1", "dependencies": ["/Volumes/x.whl"]}}}
        recovery = {"jobs": {}, "known_run_ids": {}}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "private"
            root.mkdir(mode=0o700); os.chmod(root, 0o700)
            path = root / "recovery.json"
            with self.assertRaises(Exception):
                _run_stage(api, config, "initializer", task_payload,
                    {"task_parameters_json": "{}"}, time.monotonic() + 0.5,
                    cleanup_deadline=time.monotonic() + 0.8,
                    recovery_state=recovery, recovery_path=path)
            persisted = json.loads(path.read_text())
        self.assertEqual(api.run_now_count, 1)
        self.assertFalse(api.deleted)
        stage = persisted["jobs"]["initializer"]
        self.assertEqual(stage["submission_state"], "unknown_submission_exposure")
        self.assertEqual(stage["run_now_state"], "unknown_submission_exposure")

    def test_private_recovery_receipt_is_fsynced_owner_only_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "private"
            root.mkdir(mode=0o700)
            os.chmod(root, 0o700)
            path = root / "recovery.json"
            _write_private_json(path, {"phase": "run_now_intent", "run_ids": {}})
            self.assertEqual(json.loads(path.read_text()),
                             {"phase": "run_now_intent", "run_ids": {}})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_outer_wall_deadline_writes_recovery_receipt_on_blocked_operation(self):
        def blocked(*args, **kwargs):
            time.sleep(3)
        with patch("tools.run_managed_a1_serverless._launch_trial", side_effect=blocked):
            with self.assertRaisesRegex(RuntimeError, "private recovery receipt retained") as caught:
                run({"trial_id": "a1-deadline", "host": "https://example.invalid",
                     "maximum_wall_seconds": 1}, {}, "aidev-cus", launch=True)
        path = Path(str(caught.exception).rsplit(" ", 1)[-1])
        receipt = json.loads(path.read_text())
        self.assertEqual(receipt["failure_type"], "TimeoutError")
        self.assertEqual(receipt["phase"], "launch_gate")
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        import shutil
        shutil.rmtree(path.parent)


if __name__ == "__main__":
    unittest.main()
