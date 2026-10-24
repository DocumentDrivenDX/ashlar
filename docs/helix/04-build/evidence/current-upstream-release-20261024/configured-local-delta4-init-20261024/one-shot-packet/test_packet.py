"""Focused packet checks; these tests never launch Spark or packet execution."""
from __future__ import annotations

import sys
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

PACKET = Path(__file__).resolve().parent
sys.path.insert(0, str(PACKET))
sys.path.insert(0, "/Users/erik/Projects/ashlar/src")

from packet_common import (  # noqa: E402
    canonical, load_manifest, packet_tree_digest, verify_inputs,
    sol_review_path, verify_packet_assets, verify_umf_bridge,
)
from packet_supervisor import build_invocation  # noqa: E402


class Delta4InitPacketTests(unittest.TestCase):
    def test_pinned_effect_free_runtime_attestation(self):
        receipt = verify_packet_assets()
        self.assertEqual(receipt["distributions"], {
            "pyspark": "4.2.0", "delta-spark": "4.4.1", "mini-racer": "0.14.1",
            "py4j": "0.10.9.9"})
        self.assertEqual(receipt["umfProducer"]["head"],
                         "2ede4c879bea5db13794a6fa30e67c821306c17f")

    def test_all_six_umf_role_proposals_pass_before_any_spark_use(self):
        manifest = load_manifest()
        with tempfile.TemporaryDirectory(prefix="ashlar-delta4-packet-check-") as folder:
            receipt = verify_umf_bridge(Path(folder) / "installation", manifest)
        self.assertFalse(receipt["sparkStarted"])
        self.assertEqual(set(receipt["roles"]), set(manifest["scope"]["roles"]))
        self.assertEqual(receipt["candidateCommit"],
                         "259bf1ed91466ff7e9782d7517c14f2112282026")

    def test_frozen_source_inputs_and_explicit_invocation_profile(self):
        manifest = load_manifest()
        verify_inputs(PACKET / "inputs", manifest)
        invocation = build_invocation(Path("/private/tmp/owned-init-attempt"), manifest)
        self.assertEqual(invocation["profile"], "ashlar-configured-local/0.3")
        self.assertEqual(invocation["carrier_profile"], "ashlar-delta/0.4-xxhash64")
        self.assertEqual(invocation["operation"], "initialize")
        self.assertEqual(invocation["ordinal"], 0)
        self.assertIsNone(invocation["installation_sha256"])

    def test_packet_digest_and_canonical_bytes_are_deterministic(self):
        manifest = load_manifest()
        self.assertEqual(manifest["limits"]["wallSeconds"], 300)
        self.assertEqual(manifest["limits"]["ownedBytes"], 536870912)
        self.assertTrue(manifest["limits"]["noRetry"])
        self.assertEqual(packet_tree_digest(), packet_tree_digest())
        self.assertEqual(canonical({"b": 2, "a": 1}), b'{"a":1,"b":2}\n')

    def test_sol_review_receipt_path_is_bound_to_exact_lowercase_packet_digest(self):
        digest = "a" * 64
        self.assertEqual(sol_review_path(digest).name, f"sol-review-{digest}.json")
        for invalid in ("A" * 64, "a" * 63, "../" + "a" * 61):
            with self.assertRaises(ValueError):
                sol_review_path(invalid)

    def test_isolated_child_preserves_interpreter_flags_in_orig_argv(self):
        manifest = load_manifest()
        python = manifest["runtime"]["python"]
        expected = [python, "-I", "-B", "-u", "-c", "import sys,json;print(json.dumps({'orig':sys.orig_argv,'argv':sys.argv}))"]
        completed = subprocess.run(expected, check=True, capture_output=True, text=True,
                                   timeout=10, env={"PATH": "/usr/bin:/bin"})
        state = json.loads(completed.stdout)
        self.assertEqual(state["orig"], expected)
        self.assertEqual(state["argv"][:1], ["-c"])
        self.assertNotIn("-I", state["argv"])


if __name__ == "__main__":
    unittest.main()
