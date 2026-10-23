"""Focused inert checks for the blocked R05 R4 packet; no Spark import/start."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import unittest

R4 = Path(__file__).resolve().parent
R3 = R4.parent / "r05-native-action-r3"
sys.path.insert(0, str(R3))
spec = importlib.util.spec_from_file_location("r05_supervisor", R3 / "bounded_r05_supervisor.py")
supervisor = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = supervisor
assert spec.loader is not None
spec.loader.exec_module(supervisor)


class R4PacketGateTests(unittest.TestCase):
    def test_candidate_is_blocked_and_proposal_is_not_authorization(self) -> None:
        packet_path = R4 / "invocation-packet.json"
        proposal_path = R4 / "authorization-proposal.json"
        packet = json.loads(packet_path.read_text())
        proposal = json.loads(proposal_path.read_text())
        self.assertEqual(packet["sourceRevision"], "c81513113dd31cfedc29507696873e212ca8c17f")
        self.assertEqual(packet["reviewStatus"], "independently-reviewed")
        self.assertEqual(packet["reviewId"], "sol-independent-review-2026-10-23-r05-r4")
        self.assertEqual(packet["executionStatus"], "blocked-proposed-authorization")
        self.assertEqual(packet["executionAuthorization"], "not-issued")
        self.assertEqual(proposal["status"], "proposed")
        self.assertEqual(proposal["invocationPacketSha256"], hashlib.sha256(packet_path.read_bytes()).hexdigest())
        with self.assertRaisesRegex(supervisor.NativeAttemptRefused, "Reviewed wrapper packet"):
            supervisor._verify_packet_and_authorization(
                packet_path, hashlib.sha256(packet_path.read_bytes()).hexdigest(),
                proposal_path, hashlib.sha256(proposal_path.read_bytes()).hexdigest())

    def test_runtime_inventory_and_execution_modules_are_reused_by_digest(self) -> None:
        packet = json.loads((R4 / "invocation-packet.json").read_text())
        inventory = Path(packet["runtime"]["runtimeTreeInventoryPath"])
        self.assertEqual(inventory, R3 / "runtime-tree-inventory.json")
        self.assertEqual(hashlib.sha256(inventory.read_bytes()).hexdigest(), packet["runtime"]["runtimeTreeInventorySha256"])
        for field in ("wrapper", "worker", "accountingModule"):
            selected = packet[field]
            path = Path(selected["path"])
            if not path.is_absolute():
                path = R3 / path
            self.assertTrue(path.is_file())
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), selected["sha256"])

    def test_all_supervisor_mandatory_retained_pins_are_closed(self) -> None:
        packet = json.loads((R4 / "invocation-packet.json").read_text())
        action = packet["action"]
        runtime = packet["runtime"]
        mandatory = {
            str((R3 / packet["wrapper"]["path"]).resolve()): packet["wrapper"]["sha256"],
            str((R3 / packet["worker"]["path"]).resolve()): packet["worker"]["sha256"],
            str(Path(packet["accountingModule"]["path"]).resolve()): packet["accountingModule"]["sha256"],
        }
        for path_key, digest_key in (
            ("actionModulePath", "actionModuleSha256"),
            ("generatorPath", "generatorSha256"),
            ("manifestPath", "manifestFileSha256"),
            ("anchorPath", "anchorSha256"),
            ("sourcePath", "sourceFileSha256"),
            ("oraclePath", "oracleSha256"),
            ("carrierPath", "carrierSha256"),
            ("materializerPath", "materializerSha256"),
        ):
            mandatory[str(Path(action[path_key]).resolve())] = action[digest_key]
        anchor_digest = Path(action["anchorDigestPath"]).resolve()
        mandatory[str(anchor_digest)] = hashlib.sha256(anchor_digest.read_bytes()).hexdigest()
        for path_key, digest_key in (
            ("pythonExecutable", "pythonExecutableSha256"),
            ("javaExecutable", "javaExecutableSha256"),
            ("sparkSubmitExecutable", "sparkSubmitSha256"),
        ):
            mandatory[runtime[path_key]] = runtime[digest_key]
        self.assertEqual(set(runtime["runtimeJars"]), set(runtime["runtimeJarSha256"]))
        for name, path in runtime["runtimeJars"].items():
            mandatory[path] = runtime["runtimeJarSha256"][name]
        retained = packet["retainedInputSha256"]
        self.assertEqual(set(packet["retainedInputs"]), set(retained))
        self.assertTrue(set(mandatory).issubset(retained))
        for path, digest in mandatory.items():
            self.assertEqual(retained[path], digest, path)
            self.assertEqual(hashlib.sha256(Path(path).read_bytes()).hexdigest(), digest, path)

    def test_proposed_attempt_parent_is_absent(self) -> None:
        proposal = json.loads((R4 / "authorization-proposal.json").read_text())
        self.assertFalse(Path(proposal["attemptParent"]).exists())


if __name__ == "__main__":
    unittest.main()
