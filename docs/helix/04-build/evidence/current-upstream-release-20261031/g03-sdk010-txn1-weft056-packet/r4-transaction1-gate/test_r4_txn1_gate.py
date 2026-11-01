"""Focused inert checks for the SDK010 transaction-one release gate."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("r4_txn1_gate", HERE / "r4_txn1_gate.py")
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class Transaction1GateTests(unittest.TestCase):
    def test_uninitialized_install_blocks_without_fabricating_vector_or_dispatch(self):
        result = gate.readiness(False, False, False)
        self.assertFalse(result["transaction1PrerequisitesMet"])
        self.assertEqual(result["transaction1BlockedReasons"],
                         ["fresh SDK010 six-role initialization result"])
        self.assertEqual(len(result["downstreamConsumerGaps"]), 2)
        self.assertFalse(result["transaction1DispatchAllowed"])

    def test_dispatch_failure_is_uncertain_but_pre_dispatch_refusal_is_safe(self):
        self.assertEqual(gate.classify_outcome(False, False), "refused-before-dispatch")
        self.assertEqual(gate.classify_outcome(True, False), "uncertain-after-dispatch")
        self.assertEqual(gate.classify_outcome(True, True), "completed-verified")

    def test_worker_result_without_supervisor_closure_is_refused(self):
        result_raw = b'{"profile":"ashlar-g03-sdk010-six-role-init-result/0.1"}\n'
        with tempfile.TemporaryDirectory() as directory:
            status = Path(directory) / "execution-status.json"
            status.write_text(json.dumps({
                "profile": "ashlar-g03-sdk010-init-execution-status/0.1",
                "classification": "uncertain-after-dispatch",
                "effectsPossible": True,
                "processGroupEmpty": False,
                "resultSha256": gate.digest_bytes(result_raw),
            }))
            with self.assertRaisesRegex(ValueError, "supervisor did not verify"):
                gate.verify_supervisor_closure(
                    result_raw, status, "ashlar-g03-sdk010-init-execution-status/0.1")

    def test_missing_consumer_vector_and_compiler_outputs_are_downstream_only(self):
        result = gate.readiness(True, False, False)
        self.assertTrue(result["transaction1PrerequisitesMet"])
        self.assertEqual(result["transaction1BlockedReasons"], [])
        self.assertEqual(len(result["downstreamConsumerGaps"]), 2)
        self.assertFalse(result["transaction1DispatchAllowed"])

    def test_weft_artifact_is_bound_by_wheel_and_installed_direct_url(self):
        manifest = gate.selected()
        result = gate.check_weft(manifest)
        self.assertEqual(result["version"], "0.4.56")
        self.assertEqual(result["wheelSha256"], manifest["weft056"]["wheelSha256"])
        self.assertEqual(result["directUrlRecordSha256"],
                         manifest["weft056"]["directUrlRecordSha256"])

    def test_packet_inputs_and_null_vector_are_frozen(self):
        manifest = gate.selected()
        for name, expected in manifest["inputs"].items():
            self.assertEqual(gate.digest_file(HERE / "inputs" / name), expected)
        vector = gate.json.loads((HERE / "inputs/publication-vector.json").read_bytes())
        self.assertEqual(vector["state"], "immutable-null-template")
        self.assertIsNone(vector["publicationId"])
        self.assertIsNone(vector["manifestUuid"])
        self.assertIsNone(manifest["publicationVector"]["realizedVector"])


if __name__ == "__main__":
    unittest.main()
