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

import r18_pre_migration_custody_worker as worker


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
        "fresh_process_inventory_rule": "fresh /bin/ps -Ao pid=,command= immediately before start; require successful output and no postgres/pg_ctl owner of selected or restored PGDATA via -D/--pgdata; unrelated command-line path mentions are allowed",
        "genesis_page_path": str(output / "genesis-page.json"),
        "expected_registry_receipt_sha256": None,
    }
    state["new_registry_pre_effect_backup_sha256"] = canonical_sha({
        "profile": "ashlar-r18-registry-absence-before-migration/0.1",
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
        "profile": "ashlar-truss-r18-custody-prefix-scope/0.1",
        "authorization": "one_run_local_custody_prefix",
        "output_root": str(output),
        "limits": {"attempts": 1, "automatic_retries": 0, "cloud_resources": False,
                   "child_deadline_seconds": 900, "attempt_root_bytes_max": worker.MAX_TREE_BYTES,
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
    def setUp(self):
        # Existing prefix tests use synthetic tiny PGDATA trees. The dedicated
        # test below exercises the real retained R16 receipts independently.
        self.recovery_pin_patch = patch.object(worker, "verify_recovery_source_custody", lambda scope: None)
        self.recovery_pin_patch.start()
        self.addCleanup(self.recovery_pin_patch.stop)

    def test_inventory_ignores_unrelated_process_mentions_of_target_path(self):
        target = Path("/tmp/attempt/restored")
        rows, count, matched = worker._postgres_path_owners(
            f"42 python inspect {target}\n43 /bin/zsh -c 'find {target}\n44 helper unmatched'\n",
            (target,),
        )
        self.assertEqual(rows, [])
        self.assertEqual(count, 0)
        self.assertEqual(matched, set())

    def test_inventory_blocks_postgres_and_pg_ctl_owners(self):
        target = Path("/tmp/attempt/restored")
        output = (f"42 /usr/local/bin/postgres -D {target}\n"
                  f"43 /usr/local/bin/pg_ctl --pgdata={target} status\n"
                  f"44 postgres -D{target}\n45 postgres --pgdata={target}\n")
        rows, count, matched = worker._postgres_path_owners(output, (target,))
        self.assertEqual([row["executable"] for row in rows], ["postgres", "pg_ctl", "postgres", "postgres"])
        self.assertTrue(all(row["pgdata_path"] == str(target) for row in rows))
        self.assertEqual(count, 4)
        self.assertEqual(matched, {str(target)})

    def test_inventory_normalizes_lexical_pgdata_variants(self):
        target = Path("/tmp/attempt/restored")
        output = ("42 postgres -D /tmp/attempt/./restored/\n"
                  "43 pg_ctl --pgdata=/tmp/attempt/other/../restored\n")
        rows, count, _matched = worker._postgres_path_owners(output, (target,))
        self.assertEqual([row["pid"] for row in rows], ["42", "43"])
        self.assertEqual(count, 2)

    def test_inventory_malformed_or_unavailable_output_refuses(self):
        target = Path("/tmp/attempt/restored")
        with self.assertRaisesRegex(worker.StopRun, "malformed process inventory"):
            worker._postgres_path_owners("not-a-pid postgres -D /tmp/attempt/restored\n", (target,))
        with self.assertRaisesRegex(worker.StopRun, "malformed PostgreSQL process command"):
            worker._postgres_path_owners("42 postgres 'unterminated\n", (target,))

    def test_unavailable_inventory_stops_before_start(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); scope = scope_for(root); calls = []
            ops = self._ops(scope, calls)
            ops["process_inventory"] = lambda: subprocess.CompletedProcess([], 1, "", "ps unavailable")
            with patch.object(worker, "_verify_static_inputs", return_value=Path("/fake/pg_ctl")):
                with self.assertRaisesRegex(worker.StopRun, "process inventory unavailable"):
                    worker.run_prefix(scope, root, Path("/fake/helper"), operations=ops)
            self.assertNotIn("start", calls)

    def test_owned_process_receipt_is_sanitized_and_durable_before_refusal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); scope = scope_for(root); calls = []
            target = Path(scope["state_custody"]["selected_incomplete_pgdata_path"])
            inventory = f"42 postgres -D {target}\n43 helper secret-token\n"
            ops = self._ops(scope, calls)
            ops["process_inventory"] = lambda: subprocess.CompletedProcess([], 0, inventory, "")
            with patch.object(worker, "_verify_static_inputs", return_value=Path("/fake/pg_ctl")):
                with self.assertRaisesRegex(worker.StopRun, "owns selected/restored PGDATA"):
                    worker.run_prefix(scope, root, Path("/fake/helper"), operations=ops)
            attempt = Path(scope["output_root"])
            receipt = json.loads((attempt / "pre-start-process-inventory.json").read_bytes())
            self.assertEqual(receipt["output_sha256"], hashlib.sha256(inventory.encode()).hexdigest())
            self.assertEqual(receipt["output_bytes"], len(inventory.encode()))
            self.assertEqual(receipt["owners"], [{"pid": "42", "executable": "postgres",
                                                  "pgdata_path": str(target)}])
            self.assertEqual(receipt["owner_count"], 1)
            self.assertFalse(receipt["owners_truncated"])
            self.assertFalse((attempt / "pre-start-process-inventory.txt").exists())
            self.assertNotIn("secret-token", json.dumps(receipt))
            self.assertNotIn("start", calls)

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
            with self.assertRaisesRegex(worker.StopRun, "differ from the frozen R18 packet"):
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
             patch.object(worker, "verify_approval", return_value={"limits": {"child_deadline_seconds": 900}}), \
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
            intent = Path(scope["output_root"]) / "postgres-start-intent.json"
            self.assertTrue(intent.is_file(), "durable start intent must precede pg_ctl")
            self.assertEqual(json.loads(intent.read_bytes())["pgdata_path"], str(restored))
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

    def test_live_lifecycle_receipt_is_durable_before_callback_and_final_stop_receipt_is_truthful(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); scope = scope_for(root); calls = []
            def lifecycle(prefix, conninfo):
                calls.append("lifecycle")
                self.assertIn("dbname=postgres", conninfo)
                pre_effect = {**prefix, "profile": "ashlar-r18-pre-effect-custody/0.1",
                              "migration_invoked": False, "publication_invoked": False,
                              "acknowledgement_invoked": False}
                worker.write_new(Path(scope["output_root"]) / "pre-migration-custody-live.json", pre_effect)
                self.assertTrue((Path(scope["output_root"]) / "pre-migration-custody-live.json").is_file())
                return {"migration_invoked": True, "publication_invoked": True,
                        "genesis_acknowledged": True, "seed_invoked": True,
                        "continuation_invoked": True, "continuation_acknowledged": True,
                        "stage_receipts": {"complete": "a" * 64}}
            with patch.object(worker, "_verify_static_inputs", return_value=Path("/fake/pg_ctl")):
                result = worker.run_prefix(scope, root, Path("/fake/helper"),
                    operations=self._ops(scope, calls), live_lifecycle=lifecycle)
            self.assertEqual(calls[-2:], ["lifecycle", "stop"])
            self.assertEqual(result["status"], "custody_prefix_and_live_lifecycle_completed")
            self.assertTrue(result["migration_invoked"])
            self.assertTrue(result["continuation_acknowledged"])
            final_path = Path(scope["output_root"]) / "lifecycle-stop-receipt.json"
            final = json.loads(final_path.read_bytes())
            self.assertTrue(final["pg_stopped"])
            self.assertTrue(final["migration_invoked"])
            self.assertFalse((Path(scope["output_root"]) / "pre-migration-custody.json").exists())

    def test_failed_live_callback_stops_once_and_records_unknown_outcomes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); scope = scope_for(root); calls = []
            def fail_after_possible_effect(_prefix, _conninfo):
                calls.append("lifecycle")
                raise RuntimeError("simulated uncertain lifecycle effect")
            with patch.object(worker, "_verify_static_inputs", return_value=Path("/fake/pg_ctl")):
                with self.assertRaisesRegex(worker.StopRun, "failed or is ambiguous after owned stop"):
                    worker.run_prefix(scope, root, Path("/fake/helper"),
                        operations=self._ops(scope, calls), live_lifecycle=fail_after_possible_effect)
            self.assertEqual(calls[-1], "stop")
            final = json.loads((Path(scope["output_root"]) / "lifecycle-stop-receipt.json").read_bytes())
            self.assertEqual(final["status"], "custody_prefix_lifecycle_callback_failed_outcome_unknown")
            self.assertEqual(final["migration_invoked"], "unknown")
            self.assertTrue(final["pg_stopped"])

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

    def test_callback_and_stop_failure_receipt_preserves_unknown_stage_outcomes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); scope = scope_for(root); calls = []
            ops = self._ops(scope, calls)
            ops["stop"] = lambda: subprocess.CompletedProcess([], 1, "", "stop not verified")
            def uncertain(_prefix, _conninfo):
                raise RuntimeError("migration outcome uncertain")
            with patch.object(worker, "_verify_static_inputs", return_value=Path("/fake/pg_ctl")):
                with self.assertRaisesRegex(worker.StopRun, "prior failure: migration outcome uncertain"):
                    worker.run_prefix(scope, root, Path("/fake/helper"), operations=ops,
                                      live_lifecycle=uncertain)
            final = json.loads((Path(scope["output_root"]) / "lifecycle-stop-receipt.json").read_bytes())
            self.assertFalse(final["pg_stopped"])
            self.assertEqual(final["migration_invoked"], "unknown")
            self.assertIn("server_may_be_running", json.dumps(final["effect_state"]))

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

class RecoverySourceCustodyTests(unittest.TestCase):
    def test_retained_r16_receipts_match_full_lifecycle_and_prefix_pins(self):
        proposal = Path(__file__).with_name("full-lifecycle-scope-proposal-r18.json")
        scope = json.loads(proposal.read_bytes())
        worker.verify_recovery_source_custody(scope)
        import r18_post_migration_admission as lifecycle
        lifecycle._verify_recovery_source_custody(scope)
        self.assertEqual(scope["state_custody"]["selected_incomplete_pgdata_path"],
                         worker.RECOVERY_SOURCE_CUSTODY["selected_discrepancy"]["path"])
        self.assertEqual(scope["state_custody"]["cold_backup_path"],
                         worker.RECOVERY_SOURCE_CUSTODY["cold_backup_copy"]["path"])

    def test_changed_receipt_hash_refuses_in_both_gates(self):
        proposal = Path(__file__).with_name("full-lifecycle-scope-proposal-r18.json")
        scope = json.loads(proposal.read_bytes())
        scope["recovery_source_custody"]["selected_discrepancy"]["receipt_sha256"] = "0" * 64
        with self.assertRaises(worker.StopRun):
            worker.verify_recovery_source_custody(scope)
        import r18_post_migration_admission as lifecycle
        with self.assertRaises(lifecycle.AdmissionError):
            lifecycle._verify_recovery_source_custody(scope)
