import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from ashlar_host.pinned_delta_graphframes import ReviewedCanonicalActionDescriptor

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs/helix/04-build/evidence/canonical-graphframes-holder-candidate-20261023"
PACKET_DIR = EVIDENCE / "r05-native-action-r1"
PACKET_PATH = PACKET_DIR / "invocation-packet.json"
SPEC = importlib.util.spec_from_file_location(
    "bounded_r05_supervisor", PACKET_DIR / "bounded_r05_supervisor.py")
supervisor = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(supervisor)


def canonical_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode()


class R05NativeActionPacketTests(unittest.TestCase):
    def test_packet_binds_reviewed_r05_anchor_and_distinguishes_source_digests(self):
        packet = json.loads(PACKET_PATH.read_text())
        action = packet["action"]
        r05 = EVIDENCE / "action-discriminator-r1"
        manifest_obj = json.loads((r05 / "manifest.json").read_bytes())
        manifest_bytes = canonical_json(manifest_obj)
        source_file = (r05 / "source-rows.json").read_bytes()
        self.assertTrue(source_file.endswith(b"\n"))
        self.assertEqual(hashlib.sha256(source_file).hexdigest(), action["sourceFileSha256"])
        self.assertEqual(hashlib.sha256(source_file[:-1]).hexdigest(), action["sourceContentSha256"])
        self.assertEqual(hashlib.sha256(manifest_bytes).hexdigest(), action["manifestSha256"])
        descriptor = ReviewedCanonicalActionDescriptor.from_reviewed_bytes(
            manifest_bytes, (r05 / "generator.py").read_bytes(), source_file[:-1],
            (r05 / "independent-oracle.json").read_bytes(),
            review_anchor_path=r05 / "manifest.anchor.json",
            selected_review_anchor_sha256=(r05 / "manifest.anchor.sha256").read_text().strip())
        self.assertEqual(descriptor.sha256, action["descriptorSha256"])
        self.assertEqual(descriptor.fixture.row_counts, (5, 4))
        self.assertEqual(descriptor.cap, 3)
        self.assertEqual(descriptor.fixture.selected_seed,
                         ("ashlar-r05-source-a", 1, 101))

    def test_all_selected_retained_inputs_match_packet_hashes(self):
        packet = json.loads(PACKET_PATH.read_text())
        self.assertEqual(set(packet["retainedInputs"]), set(packet["retainedInputSha256"]))
        for path in packet["retainedInputs"]:
            self.assertEqual(supervisor.sha256_file(Path(path)),
                             packet["retainedInputSha256"][path], path)
        self.assertEqual(supervisor.sha256_file(PACKET_DIR / packet["worker"]["path"]),
                         packet["worker"]["sha256"])
        self.assertEqual(supervisor.sha256_file(PACKET_DIR / packet["wrapper"]["path"]),
                         packet["wrapper"]["sha256"])
        self.assertEqual(packet["action"]["outputRootPolicy"],
                         "attempt-root/fixture; private mode 0700")
        self.assertEqual(packet["hardBounds"]["totalSeconds"], 300)
        self.assertEqual(packet["hardBounds"]["retainedPlusAttemptBytes"], 2 << 30)

    def test_unqualified_cleanup_refuses_even_with_review_status_and_authorization_shape(self):
        packet = json.loads(PACKET_PATH.read_text())
        packet["reviewStatus"] = "independently-reviewed"
        packet["executionStatus"] = "authorized-for-one-attempt"
        with tempfile.TemporaryDirectory(prefix="ashlar-r05-cleanup-refusal-") as directory:
            root = Path(directory).resolve(strict=True)
            parent = root / "attempt-parent"
            parent.mkdir(mode=0o700)
            packet_path = root / "packet.json"
            packet_path.write_bytes(canonical_json(packet))
            packet_digest = supervisor.sha256_file(packet_path)
            authorization = {
                "profile": "ashlar-canonical-graphframes-execution-authorization/0.1",
                "status": "authorized-for-one-attempt",
                "invocationPacketSha256": packet_digest,
                "attempts": 1,
                "maximumSeconds": 300,
                "maximumAggregateBytes": 2 << 30,
                "maximumAttemptRootBytes": 512 << 20,
                "maximumDiagnosticsBytes": 128 << 20,
                "master": "local[2]",
                "attemptParent": str(parent.resolve()),
            }
            auth_path = root / "authorization.json"
            auth_path.write_bytes(canonical_json(authorization))
            with self.assertRaisesRegex(supervisor.NativeAttemptRefused,
                                        "cleanup qualification"):
                supervisor._verify_packet_and_authorization(
                    packet_path, packet_digest, auth_path,
                    supervisor.sha256_file(auth_path))
            self.assertEqual(list(parent.iterdir()), [])

    def test_checked_in_packet_cannot_start_native_attempt(self):
        packet = json.loads(PACKET_PATH.read_text())
        self.assertEqual(packet["status"], "preparation-only-no-native-attestation")
        self.assertEqual(packet["cleanupStatus"], "unqualified-no-owned-table-cleanup")
        self.assertEqual(packet["executionAuthorization"], "not-issued")
        self.assertTrue(all(value is None for key, value in packet["runtime"]["runtimeJars"].items()))
        self.assertIsNone(packet["runtime"]["pythonExecutable"])
        self.assertIsNone(packet["runtime"]["javaExecutable"])
        self.assertIsNone(packet["runtime"]["sparkSubmitExecutable"])
        with tempfile.TemporaryDirectory(prefix="ashlar-r05-no-run-") as directory:
            root = Path(directory).resolve(strict=True)
            auth = root / "authorization.json"
            auth.write_text("{}")
            parent = root / "attempt-parent"
            parent.mkdir(mode=0o700)
            with self.assertRaisesRegex(supervisor.NativeAttemptRefused,
                                        "[Rr]eviewed wrapper packet"):
                supervisor.run_reviewed_native_attempt(
                    packet_path=PACKET_PATH,
                    selected_packet_sha256=supervisor.sha256_file(PACKET_PATH),
                    authorization_path=auth,
                    selected_authorization_sha256=supervisor.sha256_file(auth),
                    attempt_parent=parent)
            self.assertEqual(list(parent.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
