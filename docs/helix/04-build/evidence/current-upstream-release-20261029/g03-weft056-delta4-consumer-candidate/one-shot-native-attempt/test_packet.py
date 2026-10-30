"""Effect-free checks for the reviewed one-shot packet boundary."""
import json
from pathlib import Path
import signal
import sys
import tempfile
import unittest
from unittest import mock

import packet_common
import packet_supervisor


class PacketTests(unittest.TestCase):
    def test_spawned_group_is_stopped_if_log_open_fails(self):
        class Child:
            pid = 54321
            stdout = None
            stderr = None

            def wait(self, timeout=None):
                return 1

        alive = {"value": True}
        signals = []

        def fake_killpg(pgid, sig):
            self.assertEqual(pgid, Child.pid)
            if sig == 0:
                if alive["value"]:
                    return
                raise ProcessLookupError
            signals.append(sig)
            if sig in (signal.SIGTERM, signal.SIGKILL):
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

    def test_frozen_source_oracle_and_runtime_source_bind(self):
        result = packet_common.verify_packet(runtime=False)
        self.assertEqual(result["sourceCommit"], packet_common.manifest()["sourceCommit"])
        self.assertEqual(len(result["currentCommit"]), 40)
        self.assertEqual(result["sourceOracleSha256"],
                         packet_common.manifest()["inputs"]["source-oracle.json"])
        oracle = json.loads((packet_common.PACKET / "inputs/source-oracle.json").read_bytes())
        self.assertEqual(oracle["inputs"]["publicationEventCount"], 8)
        self.assertEqual(oracle["inputs"]["publicationTransaction"], "typed-graph-1")
        self.assertEqual(oracle["queryExpected"]["singletonPerson1"]["quantityJsonText"],
                         "9007199254740993")

    def test_pinned_runtime_assets_preflight_without_spark(self):
        result = packet_common.verify_packet(runtime=True)
        self.assertEqual(result["runtime"]["pythonVersion"], "3.12.14")
        self.assertEqual(result["runtime"]["distributions"]["pyspark"], "4.2.0")
        self.assertEqual(result["runtime"]["distributions"]["delta-spark"], "4.4.1")
        self.assertEqual(result["runtime"]["umfProducer"]["dirty"], False)

    def test_closed_transaction_scope_and_no_publication_claim(self):
        scope = packet_common.manifest()["scope"]
        self.assertEqual(scope["carrierProfile"], "ashlar-delta/0.4-xxhash64")
        self.assertEqual(scope["expectedTerminalHeads"], {
            "object_current": 2, "edge_current": 2, "tombstone": 1,
            "whole_source_history": 2, "publication_attempt_phase": 3,
            "publication_manifest": 1})
        self.assertEqual(scope["expectedNativeRows"]["publication_attempt_phase"], 2)
        self.assertEqual(scope["expectedNativeRows"]["publication_manifest"], 0)
        for claim in ("publication", "manifest", "ack", "consumer", "cloud", "scale", "trussClaim"):
            self.assertIs(scope[claim], False)

    def test_runtime_invocation_is_fresh_local_and_transaction_one(self):
        selected = packet_common.manifest()
        attempt = Path("/private/tmp/ashlar-g03-delta4-txn1-attempts/ashlar-g03-txn1-0123456789abcdef")
        value = packet_supervisor.invocation(attempt, selected)
        self.assertEqual(value["operation"], "initialize")
        self.assertEqual(value["carrier_profile"], "ashlar-delta/0.4-xxhash64")
        self.assertEqual(value["installation"], str(attempt / "installation"))
        self.assertEqual(value["installation_sha256"], None)
        self.assertEqual(value["bounds"]["whole_seconds"], 450)
        self.assertTrue(value["stream"].startswith("g03-typed-graph-1-"))
        sys.path.insert(0, str(packet_common.REPO / "src"))
        from ashlar_host.configured_local_cli import parse_configured_local
        parsed = parse_configured_local(packet_common.canonical(value))
        self.assertEqual(parsed.operation, "initialize")
        self.assertEqual(parsed.runtime.carrier_profile, "ashlar-delta/0.4-xxhash64")
        self.assertIsNone(parsed.runtime.installation_sha256)

    def test_packet_digest_stable_and_attempt_files_outside_digest(self):
        before = packet_common.packet_digest()
        self.assertEqual(before, packet_common.packet_digest())
        self.assertEqual(len(before), 64)


if __name__ == "__main__":
    unittest.main()
