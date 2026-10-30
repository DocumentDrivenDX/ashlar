"""Inert fail-closed checks for the R16 bounded one-shot entrypoint."""
from __future__ import annotations

import contextlib
import io
import json
import os
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import run_supervisor_r16 as supervisor

HERE = Path(__file__).resolve().parent
PACKET = HERE.parent / "truss051-r16-effectfree-candidate-20261029"
SCOPE_PROPOSAL = HERE / "full-lifecycle-scope-proposal-r16.json"
FAILURE_EVIDENCE = HERE / "r16-failure-evidence.json"
APPROVAL = HERE / "approval-r16-exact-one-run.json"


class R16SupervisorTests(unittest.TestCase):
    def test_scope_proposal_binds_real_workers_and_frozen_inputs_without_approval(self):
        import hashlib
        scope = json.loads(SCOPE_PROPOSAL.read_bytes())
        digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
        self.assertEqual(scope["proposal_status"], "proposed_not_approved")
        self.assertIs(scope["approval_artifact_created"], False)
        self.assertIs(scope["native_effects_executed"], False)
        # The spent R16 scope is historical and remains bound to the bytes it
        # approved. The corrected worker is a new candidate, never a mutation
        # of that one-run scope.
        self.assertEqual(scope["workers"]["custody_prefix_sha256"],
                         "ffd5ded577b0674aca19d4ffce1adf1c38ceefae31dae2b1ae4e021333107e10")
        self.assertNotEqual(scope["workers"]["custody_prefix_sha256"],
                            digest(HERE / "r16_pre_migration_custody_worker.py"))
        self.assertEqual(scope["workers"]["migration_readback_sha256"],
                         digest(HERE / "r16_post_migration_admission.py"))
        self.assertEqual(scope["workers"]["supervisor_sha256"],
                         digest(HERE / "run_supervisor_r16.py"))
        failure = json.loads(FAILURE_EVIDENCE.read_bytes())
        self.assertEqual(failure["scope_sha256"], digest(SCOPE_PROPOSAL))
        self.assertEqual(failure["approval_sha256"], digest(APPROVAL))
        self.assertEqual(failure["effect_boundary"]["postgres_start_intent_present"], False)
        self.assertEqual(failure["attempt"]["status"], "failed_before_postgres_start")
        self.assertEqual(scope["source_custody"]["expected_genesis_checkpoint_sha256"],
                         "c74e4c2dd697421f26a378454ec489bcb83501a4997235b670bdb011e4e7be8e")
        prefix = __import__("r16_pre_migration_custody_worker")
        template = json.loads((PACKET / "future-scope-template.json").read_bytes())
        self.assertEqual(scope["source_custody"], prefix.FROZEN_SOURCE_CUSTODY)
        self.assertEqual(scope["runtime"]["weft_version"], "0.4.56")
        migration = __import__("r16_post_migration_admission")
        self.assertEqual(tuple(scope["migration"]["route"]), migration.MIGRATION_EDGES)
        self.assertEqual(scope["source"]["source_root_path"],
                         "/private/tmp/ashlar-r16-source-48fb9cb0-check/src")
        self.assertTrue(Path(scope["source"]["source_root_path"]).is_dir())
        self.assertEqual(scope["source"]["helper_sha256"], supervisor.SOURCE_HELPER_SHA256)
        self.assertEqual(scope["seed_custody"]["origin_gate_code_sha256"],
                         "abb5895a1ad15f092b66355832c8c99b185856afd6ca2bab446710d819615aa3")
        self.assertNotEqual(scope["seed_custody"]["origin_gate_code_sha256"],
                            template["seed_custody"]["origin_gate_code_sha256"])
        self.assertIn("stale future-template origin-gate digest", " ".join(scope["proposal_gates"]))
        self.assertIn("historical acceptance-origin file are absent", " ".join(scope["proposal_gates"]))
        self.assertIn("local_truss_acceptance_trial", scope["seed_custody"]["semantic_approval_proposal"])
        root = Path(scope["output_root"])
        self.assertEqual(scope["limits"]["process_group_rss_bytes_max"], 2147483648)
        self.assertEqual(scope["limits"]["attempt_entries_max"], 10000)
        self.assertTrue(Path(scope["state_custody"]["restored_pgdata_path"]).is_relative_to(root))
        self.assertTrue(Path(scope["state_custody"]["registry_path"]).is_relative_to(root))
        self.assertTrue(Path(scope["runtime"]["socket_directory"]).is_relative_to(root))
        limits = scope["limits"]
        self.assertEqual((limits["attempts"], limits["automatic_retries"], limits["child_deadline_seconds"]),
                         (1, 0, 600))
        self.assertFalse(limits["cloud_resources"])

    def test_tampered_source_root_is_rejected_before_host_modules_load(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            source_root = root / "tampered-source"; source_root.mkdir()
            scope_path = root / "scope.json"
            source = {
                "archive_path": "/private/tmp/ashlar-r16-source-48fb9cb0.tar",
                "archive_sha256": supervisor.SOURCE_ARCHIVE_SHA256,
                "manifest_path": str(PACKET / "source-inputs-manifest-r16.json"),
                "manifest_sha256": supervisor.SOURCE_MANIFEST_SHA256,
                "helper_path": str(PACKET / "r16_pre_migration_source_helper.py"),
                "source_root_path": str(source_root),
            }
            scope_path.write_text(json.dumps({
                "output_root": str(root / "no-attempt"),
                "supervisor_path": str(HERE / "run_supervisor_r16.py"),
                "runtime": {"python_path": sys.executable},
                "source": source,
            }) + "\n")
            loaded = []
            real_load = supervisor._load_module
            def track(name, path):
                loaded.append(name)
                return real_load(name, path)
            before_path = list(sys.path)
            out = io.StringIO(); err = io.StringIO()
            with patch.object(supervisor, "_load_module", side_effect=track), \
                 patch.dict(os.environ, {"ASHLAR_R16_CHILD_NONCE": "a" * 64}), \
                 contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                status = supervisor.main(["--child", "--scope", str(scope_path), "--approval", str(root / "missing.json")])
            self.assertEqual(status, 2)
            self.assertEqual(loaded, ["r16_supervisor_frozen_source_verifier"])
            self.assertNotIn(str(source_root), sys.path)
            self.assertFalse((root / "no-attempt").exists())
            self.assertIn("source file inventory mismatch", err.getvalue())
            sys.path[:] = before_path

    def test_parent_watchdog_terminates_and_reaps_timed_out_child_process_group(self):
        child = subprocess.Popen([sys.executable, "-I", "-c", "import time; time.sleep(30)"],
                                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, start_new_session=True, close_fds=True)
        def members(group_id):
            return [child.pid] if child.poll() is None else []
        with patch.object(supervisor, "_process_group_members", side_effect=members), \
             patch.object(supervisor.os, "killpg", wraps=os.killpg) as killpg:
            outcome = supervisor._monitor_child(child, deadline_seconds=0.15,
                                                sample=lambda: {}, poll_seconds=0.01)
        self.assertEqual(outcome["cause"], "child_execution_deadline_exceeded")
        self.assertIsNotNone(child.returncode, "watchdog must wait/reap its child")
        self.assertEqual(outcome["process_group_remaining"], [])
        killpg.assert_called_once_with(child.pid, signal.SIGTERM)

    def test_parent_watchdog_terminates_child_when_live_sample_raises(self):
        child = subprocess.Popen([sys.executable, "-I", "-c", "import time; time.sleep(30)"],
                                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, start_new_session=True, close_fds=True)
        def members(group_id):
            return [child.pid] if child.poll() is None else []
        with patch.object(supervisor, "_process_group_members", side_effect=members), \
             patch.object(supervisor.os, "killpg", wraps=os.killpg):
            outcome = supervisor._monitor_child(child, deadline_seconds=10,
                sample=lambda: (_ for _ in ()).throw(OSError("mock sample failure")), poll_seconds=0.01)
        self.assertIn("live_resource_monitor_failed", outcome["cause"])
        self.assertIsNotNone(child.returncode)
        self.assertEqual(outcome["process_group_remaining"], [])

    def test_child_success_requires_full_stages_and_durable_stop_then_reports_native_effects(self):
        stages = {name: True for name in ("migration_invoked", "publication_invoked",
            "genesis_acknowledged", "seed_invoked", "continuation_invoked",
            "continuation_acknowledged")}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            child = {"profile": "ashlar-truss-r16-full-native-lifecycle/0.1",
                "status": "one_start_full_lifecycle_complete_and_stopped",
                "scope_sha256": "a" * 64, "single_postgres_start": True,
                "postgres_stopped_by_prefix_finally": True, **stages}
            (root / "lifecycle-stop-receipt.json").write_text(json.dumps({"pg_stopped": True, **stages}))
            (root / "one-start-migration-readback.json").write_text(json.dumps(child))
            admitted = supervisor._validate_child_success(child, "a" * 64, root)
            self.assertEqual(len(admitted["lifecycle_stop_receipt_sha256"]), 64)
            parent_fields = supervisor._completed_supervisor_fields(admitted)
            self.assertIs(parent_fields["native_effects_occurred"], True)
            self.assertEqual(parent_fields["native_effects"], "completed")
            self.assertTrue(parent_fields["effects_may_remain"])
            self.assertTrue(parent_fields["postgres_stopped"])
            with self.assertRaisesRegex(ValueError, "missing full-lifecycle"):
                supervisor._validate_child_success({**child, "seed_invoked": False}, "a" * 64, root)
            (root / "one-start-migration-readback.json").write_text(json.dumps({**child, "extra": "altered"}))
            with self.assertRaisesRegex(ValueError, "differs from durable"):
                supervisor._validate_child_success(child, "a" * 64, root)

    def test_tree_byte_sample_tolerates_scratch_file_disappearing_before_stat(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            scratch = root / "ephemeral-spark-scratch"
            scratch.write_bytes(b"temporary")
            original_is_file = Path.is_file
            original_stat = Path.stat
            def is_file(path):
                return True if path == scratch else original_is_file(path)
            def stat(path, *args, **kwargs):
                if path == scratch:
                    raise FileNotFoundError(str(path))
                return original_stat(path, *args, **kwargs)
            with patch.object(Path, "is_file", is_file), patch.object(Path, "stat", stat):
                self.assertEqual(supervisor._tree_bytes(root, maximum=1024), 0)

    def test_start_intent_timeout_is_classified_as_native_effects_unknown(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            attempt = root / "attempt"; attempt.mkdir()
            restored = attempt / "restored-pgdata"; restored.mkdir()
            (attempt / "postgres-start-intent.json").write_text(json.dumps({"pgdata_path": str(restored)}))
            scope = {"output_root": str(attempt), "state_custody": {"restored_pgdata_path": str(restored)},
                     "runtime": {"pg_ctl_path": "/no/pg_ctl"}}
            receipt = supervisor._watchdog_failure_receipt(scope, b"frozen scope",
                "child_execution_deadline_exceeded", {"returncode": -9}, {})
            self.assertEqual(receipt["native_effects"], "unknown")
            self.assertTrue(receipt["effects_may_remain"])

    def test_failure_receipt_is_not_reported_persisted_when_fsync_fails(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            worker = type("WorkerState", (), {"_EFFECT_STATE": {
                "phase": "postgres-started", "native_effects": "may_have_occurred",
                "effects_may_remain": True}})()
            with patch.object(supervisor.os, "fsync", side_effect=OSError("inert fsync fault")):
                receipt = supervisor._failure_receipt({"output_root": str(root)}, b"scope",
                                                      RuntimeError("mocked"), worker)
            self.assertFalse(receipt["failure_receipt_persisted"])

    def test_effect_free_failure_does_not_create_attempt_or_receipt(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            scope_path = root / "scope.json"
            scope_path.write_text(json.dumps({
                "output_root": str(root / "native-attempt-not-created"),
                "runtime": {"python_path": sys.executable},
                "source": {
                    "source_root_path": "/private/tmp/ashlar-r16-native-source-input-48fb9cb0/src",
                    "helper_path": str(PACKET / "r16_pre_migration_source_helper.py"),
                },
                "supervisor_path": str(HERE / "run_supervisor_r16.py"),
            }) + "\n")
            out = io.StringIO(); err = io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                result = supervisor.main(["--scope", str(scope_path),
                    "--approval", str(root / "missing-owner-approval.json")])
            self.assertEqual(result, 2)
            receipt = json.loads(err.getvalue())
            self.assertEqual(receipt["status"], "supervisor_admission_failed")
            self.assertEqual(receipt["native_effects"], "none")
            self.assertFalse(receipt["effects_may_remain"])
            self.assertFalse((root / "native-attempt-not-created").exists())

    def test_post_attempt_failure_is_durably_recorded_with_stop_receipt_hash(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            (root / "lifecycle-stop-receipt.json").write_bytes(b'{"pg_stopped":false}\n')
            worker = type("WorkerState", (), {"_EFFECT_STATE": {
                "phase": "postgres-stop-failed", "native_effects": "server_may_be_running",
                "effects_may_remain": True}})()
            receipt = supervisor._failure_receipt({"output_root": str(root)}, b"approved scope",
                                                  RuntimeError("stop uncertain"), worker)
            self.assertTrue(receipt["failure_receipt_persisted"])
            self.assertEqual(receipt["native_effects"], "server_may_be_running")
            persisted = json.loads((root / "supervisor-failure.json").read_bytes())
            self.assertEqual(persisted["durable_stop_receipt_sha256"],
                             receipt["durable_stop_receipt_sha256"])
            self.assertTrue(persisted["effects_may_remain"])

    def test_no_effect_phase_never_writes_into_an_existing_output_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            receipt = supervisor._failure_receipt({"output_root": str(root)}, b"scope",
                RuntimeError("admission blocked"), None)
            self.assertFalse(receipt["failure_receipt_persisted"])
            self.assertFalse((root / "supervisor-failure.json").exists())


if __name__ == "__main__":
    unittest.main()
