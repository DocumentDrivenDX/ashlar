import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

import r7_txn1_gate as gate
import r7_txn1_supervisor as supervisor

PACKET = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("r7_txn1_worker_test", PACKET / "r7_txn1_worker.py")
worker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(worker)


class CandidateTests(unittest.TestCase):
    def test_dispatch_is_bound_to_exact_sol_review_and_scope(self):
        selected = gate.manifest()
        self.assertEqual(selected["dispatchEnabled"],
                         selected["nativeTransactionEffectsEnabled"])
        self.assertFalse(selected["publication"])
        self.assertFalse(selected["ack"])
        inspected = gate.inspect(complete_source_admission=False)
        self.assertEqual(inspected["solReviewComplete"], selected["dispatchEnabled"])
        self.assertEqual(inspected["nativeTransactionEffectsEnabled"],
                         selected["dispatchEnabled"])
        self.assertFalse(inspected["transaction1DispatchAllowed"])
        self.assertFalse(inspected["packetDispatchGatesPassed"])
        if selected["dispatchEnabled"]:
            self.assertTrue(selected["solReviewReceiptSha256"])
            review = json.loads((gate.BASE / selected["solReviewReceiptPath"]).read_bytes())
            self.assertEqual(review["reviewStatus"], "accepted")
            self.assertEqual(review["reviewedPacketTreeSha256"], inspected["packetTreeSha256"])
            self.assertEqual(review["r7ReconciliationSha256"],
                             selected["r7"]["reconciliationSha256"])
            self.assertEqual(review["limits"], selected["limits"])
        else:
            self.assertIsNone(selected["solReviewReceiptPath"])
            self.assertIsNone(selected["solReviewReceiptSha256"])

    def test_native_effect_reporting_tracks_dispatch_policy(self):
        selected = gate.manifest()
        self.assertEqual(selected["nativeTransactionEffectsEnabled"], selected["dispatchEnabled"])
        self.assertFalse(selected["publication"])
        self.assertFalse(selected["ack"])
        inspected = gate.inspect(complete_source_admission=False)
        self.assertFalse(inspected["transaction1DispatchAllowed"])
        self.assertFalse(inspected["packetDispatchGatesPassed"])

    def test_outcome_classification_closes_after_dispatch_failure(self):
        self.assertEqual(supervisor.classify_outcome(False, False), "refused-before-dispatch")
        self.assertEqual(supervisor.classify_outcome(True, False), "uncertain-after-dispatch")
        self.assertEqual(supervisor.classify_outcome(True, True), "completed-verified")

    def test_terminal_vector_is_derived_from_reconciled_v1_heads(self):
        rows = [
            {"role": "object_current", "uuid": "object", "headVersion": 1},
            {"role": "edge_current", "uuid": "edge", "headVersion": 1},
            {"role": "tombstone", "uuid": "tomb", "headVersion": 1},
            {"role": "whole_source_history", "uuid": "history", "headVersion": 1},
            {"role": "attempts", "uuid": "attempts", "headVersion": 1},
            {"role": "manifest", "uuid": "manifest", "headVersion": 1},
        ]
        actual = worker._expected_head_vector({"roleHeads": rows})
        self.assertEqual(actual, {
            "object_current": {"uuid": "object", "version": 2},
            "edge_current": {"uuid": "edge", "version": 2},
            "tombstone": {"uuid": "tomb", "version": 1},
            "whole_source_history": {"uuid": "history", "version": 2},
            "publication_attempt_phase": {"uuid": "attempts", "version": 3},
            "publication_manifest": {"uuid": "manifest", "version": 1},
        })

    def test_real_runtime_config_binds_installation_stream_and_replaced_admission(self):
        selected = gate.manifest()
        reconciliation = gate.verify_reconciliation(selected)
        invocation_path = Path(selected["r7"]["attemptRoot"]) / "initialize-invocation.json"
        from ashlar_host.configured_local_cli import parse_configured_local
        original = parse_configured_local(invocation_path.read_bytes()).runtime
        with tempfile.TemporaryDirectory(prefix="r7-txn1-runtime-config-",
                                         dir="/private/tmp") as tmp:
            attempt = Path(tmp)
            runtime = worker._runtime_config(attempt, reconciliation)
            expected_output = attempt / "transaction1-output"
            self.assertFalse(expected_output.exists())
            self.assertEqual(runtime.installation_id, reconciliation["installationId"])
            self.assertEqual(runtime.stream, reconciliation["installationId"])
            self.assertEqual(runtime.installation_sha256,
                             reconciliation["installationSha256"])
            self.assertEqual(runtime.output, expected_output)
            self.assertIsNot(runtime.admission, original.admission)
            self.assertEqual(runtime.admission.output, expected_output / "admission")
            self.assertEqual(runtime.sdk010_admission.sdk_archive,
                             Path(selected["sdk010"]["sdkArchive"]))


if __name__ == "__main__":
    unittest.main()
