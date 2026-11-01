from __future__ import annotations

from pathlib import Path
import sys
import unittest


PACKET = (Path(__file__).resolve().parents[1] / "docs/helix/04-build/evidence/"
          "current-upstream-release-20261101/g03-sdk0101-r3-manifest-continuation")
sys.path.insert(0, str(PACKET))

from continuation_worker import (  # noqa: E402
    CARRIER_ROLE_BY_JOURNAL_ROLE, INVOCATION, read_invocation)
from continuation_supervisor import parse_process_table  # noqa: E402


class ContinuationPacketTests(unittest.TestCase):
    def test_invocation_binds_one_manifest_only_continuation(self):
        invocation, digest = read_invocation(INVOCATION)
        self.assertEqual(len(digest), 64)
        self.assertTrue(invocation["dispatchEnabled"])
        self.assertEqual(invocation["operation"]["expectedGraphPrepareApplyRecoverCalls"], 0)
        self.assertFalse(invocation["operation"]["effects"]["sourceAck"])
        self.assertFalse(invocation["operation"]["effects"]["publicationArtifact"])
        self.assertEqual(invocation["bounds"]["wallSeconds"], 900)
        self.assertEqual(invocation["bounds"]["rssBytes"], 2 * 1024**3)
        self.assertIn("continuation-r5-", invocation["bounds"]["attemptParent"])
        self.assertEqual(invocation["priorContinuationRefusal"]["elapsedMs"], 4455)
        self.assertFalse(invocation["priorContinuationRefusal"]["nativeApiCalled"])
        self.assertEqual(invocation["priorR4ContinuationRefusal"]["elapsedMs"], 139348)
        self.assertFalse(invocation["priorR4ContinuationRefusal"]["manifestHeadAdvanced"])

    def test_process_table_parser_sums_rss_as_bytes(self):
        rows = parse_process_table(
            "  101  101  128 python worker.py\n"
            "  102  101  256 java org.apache.spark.deploy\n"
        )
        self.assertEqual(rows, [
            (101, 101, 128 * 1024, "python worker.py"),
            (102, 101, 256 * 1024, "java org.apache.spark.deploy"),
        ])

    def test_invalid_numeric_process_inventory_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "Could not parse process RSS"):
            parse_process_table("101 101 invalid worker\n")

    def test_carrier_aliases_resolve_metadata_journal_roles(self):
        self.assertEqual(CARRIER_ROLE_BY_JOURNAL_ROLE, {
            "publication_attempt_phase": "attempts",
            "publication_manifest": "manifest",
        })


if __name__ == "__main__":
    unittest.main()
