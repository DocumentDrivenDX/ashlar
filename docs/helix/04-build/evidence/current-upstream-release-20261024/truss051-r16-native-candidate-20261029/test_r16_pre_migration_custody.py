"""Inert tests for the guarded pre-migration custody prefix."""
from __future__ import annotations

import hashlib
import io
import json
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import r16_pre_migration_custody_worker as worker


def canonical_sha(value):
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
    return hashlib.sha256(raw).hexdigest()


def scope_for(root):
    selected = root / "selected-incomplete"
    selected.mkdir()
    (selected / "log").write_text("observed incomplete PGDATA\n")
    backup = root / "cold-backup"
    (backup / "global").mkdir(parents=True)
    (backup / "PG_VERSION").write_text("18\n")
    (backup / "global" / "pg_control").write_bytes(b"bounded cold-cluster control")
    output = root / "attempt"
    state = {
        "selected_incomplete_pgdata_path": str(selected),
        "selected_incomplete_pgdata_tree_sha256": worker.tree(selected)["tree_sha256"],
        "discrepancy_preservation_path": str(output / "preserved"),
        "cold_backup_path": str(backup),
        "cold_backup_tree_sha256": worker.tree(backup)["tree_sha256"],
        "restored_pgdata_path": str(output / "restored"),
        "registry_path": str(output / "registry.sqlite3"),
        "registry_backup_receipt_path": str(output / "registry-absence.json"),
        "new_registry_pre_effect_backup_sha256_or_exact_creation_rule": "registry remains absent until the separately gated migration stage",
        "fresh_process_inventory_rule": "fresh /bin/ps -Ao pid=,command= immediately before start; require successful output and no selected/restored PGDATA references",
        "genesis_page_path": str(output / "genesis-page.json"),
        "expected_registry_receipt_sha256": None,
    }
    state["new_registry_pre_effect_backup_sha256"] = canonical_sha({
        "profile": "ashlar-r16-registry-absence-before-migration/0.1",
        "registry_path": state["registry_path"],
        "exact_creation_rule": state["new_registry_pre_effect_backup_sha256_or_exact_creation_rule"],
        "registry_created": False,
    })
    source_custody = {
        "expected_accepted_source_document_id": "ashlar-current-045-lifecycle-20261021",
        "expected_source_revision": "ashlar-current-045-lifecycle-r1",
        "expected_source_registration_id": "ccfcd423-5893-4df9-8f88-6504e086bd4d",
        "expected_source_sha256": "498a82f313fd3ceaeb6195e133b403aa829acfcda7cec676a33a5790a57d456b",
        "expected_catalog_revision": 1,
        "expected_source_report_sha256": "c797191c90c516b0997801a63605d297e5e429b50e922cd7da2d39640d570c9a",
        "expected_complete_view_sha256": "cc36fe489326f73c2eaf153ba2e025feeb76eaa88ffa0d6b675a1f4772264dc7",
        "expected_feed_id": "ashlar-current-045-lifecycle",
        "expected_consumer_id": "ashlar-current-048-lifecycle-20261022",
        "expected_consumer_registration_id": "9f03eb5d-ce72-437f-8b32-0112c130e724",
        "expected_genesis_checkpoint_sha256": "c74e4c2dd697421f26a378454ec489bcb83501a4997235b670bdb011e4e7be8e",
    }
    return {
        "profile": "ashlar-truss-r16-custody-prefix-scope/0.1",
        "authorization": "one_run_local_custody_prefix",
        "output_root": str(output),
        "limits": {"attempts": 1, "automatic_retries": 0, "cloud_resources": False,
                   "child_deadline_seconds": 600, "attempt_root_bytes_max": worker.MAX_TREE_BYTES,
                   "combined_log_bytes_max": 16 * 1024 * 1024,
                   "aggregate_retained_plus_attempt_bytes_max": 2 * 1024 * 1024 * 1024,
                   "parallelism": "sequential local only"},
        "state_custody": state,
        "runtime": {"socket_directory": str(output / "socket"), "port": 55439,
                    "database": "postgres", "bootstrap_user": "postgres",
                    "pg_ctl_path": "/fake/pg_ctl",
                    "truss048_python": worker.OLD48_PYTHON,
                    "truss048_runtime_receipt_path": "/fake/runtime-receipt.json",
                    "truss048_runtime_receipt_sha256": worker.OLD48_RUNTIME_SHA},
        "source": {"archive_path": "/fake/source.tar", "archive_sha256": worker.ARCHIVE_SHA,
                   "worker_sha256": "a" * 64},
        "source_custody": source_custody,
        "seed_custody": {"request_sha256": worker.SEED_REQUEST_SHA,
                          "origin_map_expected_sha256": worker.ORIGIN_SHA},
    }


class CustodyPrefixTests(unittest.TestCase):
    def test_template_nulls_fail_before_creating_or_copying_anything(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            target = root / "attempt"
            with self.assertRaisesRegex(worker.StopRun, "custody-prefix scope"):
                worker.verify_scope({"profile": "ashlar-truss-r16-future-native-scope-template/0.1"})
            self.assertFalse(target.exists())

    def test_output_tree_cannot_overlap_selected_or_cold_pgdata(self):
        for source_key in ("selected_incomplete_pgdata_path", "cold_backup_path"):
            with self.subTest(source=source_key), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve(); scope = scope_for(root)
                source = Path(scope["state_custody"][source_key])
                old_output = Path(scope["output_root"])
                new_output = source / "attempt"
                scope["output_root"] = str(new_output)
                for key in ("discrepancy_preservation_path", "restored_pgdata_path", "registry_path",
                            "registry_backup_receipt_path", "genesis_page_path"):
                    scope["state_custody"][key] = str(new_output / Path(scope["state_custody"][key]).relative_to(old_output))
                scope["runtime"]["socket_directory"] = str(new_output / "socket")
                with self.assertRaisesRegex(worker.StopRun, "may not overlap either PGDATA source"):
                    worker.verify_scope(scope)
                self.assertFalse(new_output.exists())

    def test_socket_and_source_custody_are_exactly_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); scope = scope_for(root)
            socket_path = scope["runtime"]["socket_directory"]
            scope["runtime"]["socket_directory"] = str(root / "outside-socket")
            with self.assertRaisesRegex(worker.StopRun, "socket_directory must be under"):
                worker.verify_scope(scope)
            scope["runtime"]["socket_directory"] = socket_path
            scope["source_custody"]["expected_source_sha256"] = "0" * 64
            with self.assertRaisesRegex(worker.StopRun, "differ from the frozen R16 packet"):
                worker.verify_scope(scope)

    def test_cold_backup_hash_mismatch_fails_before_attempt_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); scope = scope_for(root)
            scope["state_custody"]["cold_backup_tree_sha256"] = "0" * 64
            with patch.object(worker, "_verify_static_inputs", return_value=Path("/fake/pg_ctl")):
                with self.assertRaisesRegex(worker.StopRun, "cold-backup source differs"):
                    worker.run_prefix(scope, root, Path("/fake/helper"), operations={"tree": worker.tree})
            self.assertFalse(Path(scope["output_root"]).exists())

    def test_failure_after_start_attempt_is_reported_as_effectful(self):
        output = io.StringIO()
        def fail_after_start(*_args):
            worker._mark_effect("postgres-start-attempted", native="possible", may_remain=True)
            raise worker.StopRun("simulated stage failure")
        with patch("sys.argv", ["worker", "--scope", "/scope", "--approval", "/approval",
                                "--source-root", "/source", "--helper", "/helper"]), \
             patch.object(worker, "verify_approval", return_value={"limits": {"child_deadline_seconds": 600}}), \
             patch.object(worker, "run_prefix", side_effect=fail_after_start), \
             redirect_stdout(output):
            self.assertEqual(worker.main(), 2)
        report = json.loads(output.getvalue())
        self.assertTrue(report["native_effects"])
        self.assertTrue(report["effect_state"]["effects_may_remain"])
        self.assertEqual(report["effect_state"]["phase"], "postgres-start-attempted")

    def _ops(self, scope, calls, *, bad_source=False):
        restored = Path(scope["state_custody"]["restored_pgdata_path"])
        page_path = Path(scope["state_custody"]["genesis_page_path"])
        expected = scope["source_custody"]
        source = {
            "document_id": expected["expected_accepted_source_document_id"],
            "document_revision": expected["expected_source_revision"],
            "source_registration_id": expected["expected_source_registration_id"],
            "source_sha256": "bad" if bad_source else expected["expected_source_sha256"],
            "catalog_revision": expected["expected_catalog_revision"],
            "report_sha256": expected["expected_source_report_sha256"],
            "view_sha256": expected["expected_complete_view_sha256"],
        }

        def start():
            calls.append("start")
            (restored / "postmaster.pid").write_text("1234\n" + str(restored) + "\n")
            return subprocess.CompletedProcess([], 0, "started\n", "")

        def stop():
            calls.append("stop")
            (restored / "postmaster.pid").unlink()
            return subprocess.CompletedProcess([], 0, "stopped\n", "")

        def helper(_python, _helper, _source_root, _conninfo, _attempt, *args):
            calls.append("custody" if args == ("--custody",) else "page")
            if args == ("--custody",):
                return source
            page_path.write_bytes(b"frozen genesis page archive")
            return {"consumer_id": expected["expected_consumer_id"],
                    "registration_id": expected["expected_consumer_registration_id"],
                    "writer_xid": "9", "page_sha256": "1" * 64,
                    "archive_sha256": hashlib.sha256(page_path.read_bytes()).hexdigest(),
                    "prior_checkpoint_sha256": "2" * 64,
                    "next_checkpoint_sha256": expected["expected_genesis_checkpoint_sha256"]}

        return {"process_inventory": lambda: subprocess.CompletedProcess([], 0, "100 current-worker\n", ""),
                "start": start, "stop": stop, "connect": lambda _params: str(restored),
                "helper": helper}

    def test_stages_are_sequential_and_stop_after_exact_custody(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); scope = scope_for(root); calls = []
            with patch.object(worker, "_verify_static_inputs", return_value=Path("/fake/pg_ctl")):
                result = worker.run_prefix(scope, root, Path("/fake/helper"),
                                           stage=calls.append, operations=self._ops(scope, calls))
            self.assertEqual(result["status"], "custody_prefix_verified_migration_not_invoked")
            self.assertTrue(result["pg_stopped"])
            self.assertFalse(result["migration_invoked"])
            self.assertNotIn("migration", calls)
            self.assertLess(calls.index("source-custody-verified"), calls.index("page"))
            self.assertEqual(calls[-1], "stop")

    def test_source_mismatch_stops_before_page_read_and_stops_owned_server(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); scope = scope_for(root); calls = []
            with patch.object(worker, "_verify_static_inputs", return_value=Path("/fake/pg_ctl")):
                with self.assertRaisesRegex(worker.StopRun, "source/view custody mismatch"):
                    worker.run_prefix(scope, root, Path("/fake/helper"),
                                      stage=calls.append, operations=self._ops(scope, calls, bad_source=True))
            self.assertIn("start", calls)
            self.assertIn("stop", calls)
            self.assertNotIn("page", calls)
            self.assertNotIn("custody-prefix-complete", calls)

    def test_stop_failure_keeps_server_uncertainty_and_diagnostic(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); scope = scope_for(root); calls = []
            ops = self._ops(scope, calls)
            ops["stop"] = lambda: subprocess.CompletedProcess([], 1, "", "controlled stop failure")
            with patch.object(worker, "_verify_static_inputs", return_value=Path("/fake/pg_ctl")):
                with self.assertRaisesRegex(worker.StopRun, "returncode=1.*pid_file_remains=True.*controlled stop failure"):
                    worker.run_prefix(scope, root, Path("/fake/helper"), operations=ops)
            self.assertEqual(worker._EFFECT_STATE["native_effects"], "server_may_be_running")
            self.assertTrue(worker._EFFECT_STATE["effects_may_remain"])

    def test_nonzero_start_with_owned_pid_still_enters_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); scope = scope_for(root); calls = []
            restored = Path(scope["state_custody"]["restored_pgdata_path"])
            def failed_start():
                calls.append("start")
                (restored / "postmaster.pid").write_text("1234\n" + str(restored) + "\n")
                return subprocess.CompletedProcess([], 1, "", "startup timeout")
            ops = self._ops(scope, calls); ops["start"] = failed_start
            with patch.object(worker, "_verify_static_inputs", return_value=Path("/fake/pg_ctl")):
                with self.assertRaisesRegex(worker.StopRun, "startup timeout"):
                    worker.run_prefix(scope, root, Path("/fake/helper"), operations=ops)
            self.assertEqual(calls[-1], "stop")
            self.assertFalse((restored / "postmaster.pid").exists())


if __name__ == "__main__":
    unittest.main()
