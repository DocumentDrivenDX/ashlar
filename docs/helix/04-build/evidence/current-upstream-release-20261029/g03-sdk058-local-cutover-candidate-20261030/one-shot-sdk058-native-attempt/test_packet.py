"""Effect-free checks for the versioned G03 SDK058 packet boundary."""
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

PACKET = Path(__file__).resolve().parent
REPO = PACKET.parents[6]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(PACKET))
import packet_common
import packet_supervisor


class PacketTests(unittest.TestCase):
    def test_sdk058_packet_and_fresh_attempt_identity(self):
        selected = packet_common.manifest()
        self.assertEqual(selected["profile"],
                         "ashlar-g03-local-delta4-transaction1-sdk058-packet/0.1")
        self.assertEqual(selected["sdk058Admission"]["version"], "0.5.8")
        self.assertEqual(selected["umfDelta4Bridge"]["profile"],
                         "ashlar-local-current-delta4-umf-source-sdk058/0.1")
        self.assertEqual(selected["attemptHistory"]["reusePriorAttempts"], False)
        self.assertFalse(Path(selected["attemptHistory"]["newAttemptParent"]).exists())
        self.assertTrue(selected["attemptHistory"]["r1"]["attemptRoot"].endswith(
            "19c69741b58c4e46a15d4f3645ba7e23"))
        self.assertTrue(selected["attemptHistory"]["r2"]["attemptRoot"].endswith(
            "b7ad716baf2943f1baab0caeddf5ee17"))

    def test_sdk058_source_and_six_role_bridge_preflight_is_effect_free(self):
        result = packet_common.verify_packet(runtime=False)
        self.assertEqual(result["sourceAdmission"]["producerRevision"],
                         "umf-sdk-0.5.8/071544fcc83c5abcf1e280890e1516613f679a58cf245b31a7bf16f0b6dd24fd")
        self.assertIs(result["sourceAdmission"]["sparkStarted"], False)
        self.assertIs(result["bridge"]["sparkStarted"], False)
        self.assertEqual(set(result["bridge"]["roles"]), {
            "object_current", "edge_current", "tombstone", "whole_source_history",
            "publication_manifest", "publication_attempt_phase"})

    def test_runtime_cohort_preflight_without_spark(self):
        result = packet_common.verify_packet(runtime=True)
        self.assertEqual(result["runtime"]["pythonVersion"], "3.12.14")
        self.assertEqual(result["runtime"]["distributions"]["pyspark"], "4.2.0")
        self.assertEqual(result["runtime"]["distributions"]["delta-spark"], "4.4.1")
        self.assertFalse(result["runtime"]["umfProducer"]["dirty"])

    def test_both_stage_invocations_carry_exact_sdk058_admission(self):
        selected = packet_common.manifest()
        attempt = Path(selected["attemptHistory"]["newAttemptParent"]) / (
            "ashlar-g03-txn1-0123456789abcdef")
        value = packet_supervisor.invocation(attempt, selected)
        self.assertEqual(value["carrier_profile"], "ashlar-delta/0.4-xxhash64")
        self.assertEqual(value["bounds"]["whole_seconds"], 450)
        from ashlar_host.configured_local_cli import parse_configured_local
        from dataclasses import replace
        parsed = parse_configured_local(packet_common.canonical(value))
        runtime = replace(parsed.runtime, sdk058_admission=packet_common._sdk_config(selected))
        self.assertEqual(runtime.sdk058_admission.sdk_package_root,
                         Path(selected["sdk058Admission"]["sdkPackageRoot"]))
        self.assertEqual(runtime.sdk058_admission.source_admission_manifest,
                         Path(selected["sdk058Admission"]["sourceAdmissionManifest"]))
        transaction_runtime = replace(runtime, output=attempt / "transaction1-output",
            admission=replace(runtime.admission, output=attempt / "transaction1-output" / "admission"),
            installation_sha256="a" * 64)
        self.assertIs(transaction_runtime.sdk058_admission, runtime.sdk058_admission)

    def test_spawned_process_group_is_stopped_if_log_open_fails(self):
        class Child:
            pid = 54321
            stdout = None
            stderr = None
            def wait(self, timeout=None): return 1

        alive = {"value": True}
        signals = []
        def fake_killpg(pgid, sig):
            self.assertEqual(pgid, Child.pid)
            if sig == 0:
                if alive["value"]: return
                raise ProcessLookupError
            signals.append(sig)
            alive["value"] = False

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "initialize.stdout.log").write_bytes(b"existing")
            selected = {"runtime": {"python": "/fake/python"},
                        "limits": {"combinedStdoutStderrBytes": 1024}}
            with mock.patch.object(packet_supervisor.subprocess, "Popen", return_value=Child()), \
                    mock.patch.object(packet_supervisor.os, "killpg", side_effect=fake_killpg):
                with self.assertRaises(FileExistsError):
                    packet_supervisor.run_child(root, "initialize", "a" * 64,
                                                "b" * 64, 9999999999, selected, {})
        self.assertEqual(signals, [signal.SIGTERM])
        self.assertFalse(alive["value"])

    def test_transaction_scope_remains_local_and_unpublished(self):
        scope = packet_common.manifest()["scope"]
        self.assertEqual(scope["sourceTransaction"], "typed-graph-1")
        self.assertEqual(scope["expectedTerminalHeads"], {
            "object_current": 2, "edge_current": 2, "tombstone": 1,
            "whole_source_history": 2, "publication_attempt_phase": 3,
            "publication_manifest": 1})
        for claim in ("publication", "manifest", "ack", "consumer", "cloud", "scale", "trussClaim"):
            self.assertIs(scope[claim], False)

    def test_packet_digest_is_stable(self):
        self.assertEqual(packet_common.packet_digest(), packet_common.packet_digest())
        self.assertEqual(len(packet_common.packet_digest()), 64)

    def test_worker_bootstrap_under_isolated_python(self):
        worker = PACKET / "packet_worker.py"
        result = subprocess.run([sys.executable, "-I", "-B", str(worker), "--help"],
                                cwd=PACKET, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--stage", result.stdout)


if __name__ == "__main__":
    unittest.main()
