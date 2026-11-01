"""Effect-free R46 recovery-operation input checks against retained receipts."""
import copy
import importlib.util
import json
from pathlib import Path
import unittest

HERE = Path(__file__).resolve().parent
WORKER = HERE / "r46-recovery-worker.py"
SCOPE = HERE / "r46-host-scope.json"


def load_worker():
    spec = importlib.util.spec_from_file_location("r46_recovery_input_worker", WORKER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class R46RecoveryInputTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.worker = load_worker()
        cls.scope = json.loads(SCOPE.read_bytes())
        cls.base = cls.worker.load_r42().load_base()
        cls.evidence = cls.base.validate_resume_evidence(cls.scope)["evidence"]
        pending_path = Path(cls.scope["recovery"]["pending_receipt_path"])
        cls.pending = cls.worker.strict_json(pending_path.read_bytes())
        cls.pending_raw = pending_path.read_bytes()

    def test_actual_retained_acceptance_drives_revision_and_operation_sentinel(self):
        self.assertNotIn("revision", self.scope["resume"]["source_postconditions"])
        inputs = self.worker._prepare_recovery_inputs(self.scope, self.pending, self.evidence)
        actual_acceptance = self.evidence["acceptance.json"]
        self.assertEqual(inputs["acceptance"]["revision"], actual_acceptance["revision"])
        self.assertEqual(inputs["acceptance"]["registration_id"], actual_acceptance["registration_id"])
        operation = self.worker._public_recovery_operation(
            self.scope, self.pending_raw, HERE / "unused-receipts", None, self.base, self.evidence)
        with self.assertRaises(self.worker.RecoveryInputShapeValidated) as caught:
            operation(None, {}, validate_only=True)
        self.assertEqual(caught.exception.inputs["acceptance"]["revision"], 1)
        self.assertEqual(caught.exception.inputs["pending"]["request_digest"], self.pending["request_digest"])

    def test_missing_acceptance_revision_refuses_before_public_calls(self):
        bad_evidence = copy.deepcopy(self.evidence)
        del bad_evidence["acceptance.json"]["revision"]
        with self.assertRaisesRegex(self.worker.Refusal, "validated acceptance receipt"):
            self.worker._prepare_recovery_inputs(self.scope, self.pending, bad_evidence)

    def test_acceptance_report_identity_must_match_source_postconditions(self):
        bad_evidence = copy.deepcopy(self.evidence)
        bad_evidence["acceptance.json"]["registration_id"] = "foreign-registration"
        with self.assertRaisesRegex(self.worker.Refusal, "bindings differ|acceptance"):
            self.worker._prepare_recovery_inputs(self.scope, self.pending, bad_evidence)


if __name__ == "__main__":
    unittest.main()
