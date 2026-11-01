from __future__ import annotations
import ast, json, os, sqlite3, subprocess
from pathlib import Path
import unittest

HERE = Path(__file__).resolve().parent
WORKER = HERE / "r12_truss051_local_worker.py"

class R12ResumeContractTests(unittest.TestCase):
    def test_worker_compiles_and_never_replays_migration(self):
        tree = ast.parse(WORKER.read_text())
        attrs = [n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)]
        self.assertFalse({"prepare", "apply", "reconcile"}.intersection(attrs))

    def test_host_adoption_is_direct_not_wrapped_as_ok(self):
        text = WORKER.read_text()
        self.assertNotIn("require_ok(adopt_psycopg_host", text)
        self.assertEqual(text.count("adopt_psycopg_host("), 5)

    def test_continuation_gates_all_mutations_on_readback(self):
        text = WORKER.read_text()
        readback = text.index("completed.append(\"public_51_document_registration_readback\")")
        module_write = text.index("CREATE ROLE")
        consumer_write = text.index("consumer = require_ok(assembly.register_feed_consumer_in_transaction")
        seed_write = text.index("applied=require_ok(assembly.apply_in_transaction")
        self.assertLess(readback, module_write)
        self.assertLess(readback, consumer_write)
        self.assertLess(readback, seed_write)

    def test_r10_receipt_gate_precedes_r12_start(self):
        text = WORKER.read_text()
        gate = text.index("R10 terminal migration/custody receipts do not support safe resume")
        start = text.index("with runtime as pg:")
        self.assertLess(gate, start)
        self.assertIn('"prepare_called":False,"apply_called":False,"reconcile_called":False',text)


    def test_r10_obligation_id_matches_public_registry_schema(self):
        r10 = Path("/private/tmp/ashlar-r10-truss051-current-cohort-20261101")
        prepared = json.loads((r10 / "receipts/migration-prepared.json").read_text())
        obligation = prepared["original_obligation"]
        scope = json.loads((HERE / "r12-truss051-resume-scope.json").read_text())
        self.assertEqual(obligation["obligationId"], scope["migration"]["attempt_id"])
        self.assertNotIn("id", obligation)
        receipt = json.loads((r10 / "receipts/migration-receipt.json").read_text())
        db = sqlite3.connect(f"file:{r10 / 'truss-recovery-registry.sqlite3'}?mode=ro", uri=True)
        try:
            row = db.execute("SELECT id,original FROM obligation WHERE reference=?", (receipt["reference"],)).fetchone()
            observations = db.execute("SELECT original FROM observation WHERE reference=? ORDER BY ordinal", (receipt["reference"],)).fetchall()
        finally:
            db.close()
        self.assertEqual(row[0], obligation["obligationId"])
        registry_obligation = json.loads(row[1])
        self.assertEqual(registry_obligation["obligationId"], scope["migration"]["attempt_id"])
        self.assertEqual(len(observations), 2)
        worker_text = WORKER.read_text()
        self.assertIn('get("obligationId")', worker_text)
        self.assertNotIn('get("id") != scope["migration"]["attempt_id"]', worker_text)


    def test_actual_worker_preflight_passes_before_effect_roots(self):
        scope = json.loads((HERE / "r12-truss051-resume-scope.json").read_text())
        runtime = scope["runtime"]
        env = os.environ.copy()
        env["PYTHONPATH"] = runtime["pythonpath"]
        result = subprocess.run([runtime["python_path"], str(WORKER), "--preflight"],
            env=env, text=True, capture_output=True, timeout=120, check=True)
        receipt = json.loads(result.stdout)
        self.assertEqual(receipt["status"], "passed_effect_free_preflight")
        self.assertEqual(receipt["scope_sha256"], __import__("hashlib").sha256(
            (HERE / "r12-truss051-resume-scope.json").read_bytes()).hexdigest())
        self.assertFalse(receipt["output_root_created"])
        self.assertFalse(receipt["postgres_started"])
        self.assertFalse(receipt["native_database_calls"])
        self.assertFalse(receipt["migration_replayed"])
        for key in ("output_root", "supervisor_root", "cold_backup_pgdata",
                    "migration_clone_pgdata", "recovery_registry", "receipt_root"):
            path = Path(scope["paths"][key])
            self.assertFalse(path.exists(), f"preflight created {path}")
            self.assertFalse(path.is_symlink(), f"preflight created symlink {path}")

    def test_r12_is_no_ack_local_only(self):
        scope = json.loads((HERE / "r12-truss051-resume-scope.json").read_text())
        self.assertFalse(scope["acknowledgement"])
        self.assertFalse(scope["limits"]["cloud"])
        self.assertFalse(scope["production_authority"])
        self.assertIn("no prepare/apply/reconcile",scope["migration"]["method_order"])

if __name__ == "__main__": unittest.main()
