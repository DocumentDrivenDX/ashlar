"""Effect-free checks for the held R05 paired writer packet."""
import unittest
from types import SimpleNamespace

from r05_paired_writer_packet import (
    EDGE_ROWS, NODE_ROWS, PacketRefused, _closed_approval, _source_select,
)
import r05_paired_writer_supervisor as supervisor


class PairedWriterPacketChecks(unittest.TestCase):
    def approval(self, **changes):
        value = {
            "profile": "ashlar-r05-paired-writer-authorization/0.1",
            "status": "approved", "packetSha256": "a" * 64, "attempts": 1,
            "nodeRows": NODE_ROWS, "edgeRows": EDGE_ROWS,
            "totalSeconds": 900, "workerSeconds": 780,
            "rootBytes": 32 * 1024**3, "activeDeltaBytes": 16 * 1024**3,
            "minimumFreeBytes": 64 * 1024**3,
            "predictiveOptimizationDisabled": False,
            "sourcePinsSha256": "b" * 64,
            "runtimePinsSha256": "c" * 64,
            "ownerDecision": "reviewed bounded local paired discriminator",
        }
        value.update(changes)
        return value

    def test_exact_profile_requires_explicit_owner_and_fixed_scales(self):
        self.assertEqual("reviewed bounded local paired discriminator",
                         _closed_approval(self.approval(), "a" * 64)["ownerDecision"])
        for changes in ({"nodeRows": 1_000_000_000}, {"edgeRows": 5_000_000_000},
                        {"attempts": 2}, {"predictiveOptimizationDisabled": True},
                        {"ownerDecision": " "}, {"extra": "unreviewed"}):
            with self.subTest(changes=changes), self.assertRaises(PacketRefused):
                _closed_approval(self.approval(**changes), "a" * 64)

    def test_generator_is_bound_to_the_one_paired_sample_and_source_select(self):
        self.assertEqual((45_000_000, 225_000_000), (NODE_ROWS, EDGE_ROWS))
        for rows in (NODE_ROWS, EDGE_ROWS):
            role = SimpleNamespace(insert_sql=(
                f"INSERT INTO delta.`/tmp/r05-pair/output`\nSELECT id FROM range({rows})"))
            source = _source_select(role)
            self.assertTrue(source.startswith("SELECT"))
            self.assertIn(f"range({rows})", source)
        self.assertEqual(225_000_000, NODE_ROWS * 5)

    def test_non_generator_sql_is_refused(self):
        class Invalid:
            insert_sql = "INSERT INTO delta.`/tmp/x` SELECT * FROM arbitrary_table"
        with self.assertRaises(PacketRefused):
            _source_select(Invalid())

    def test_cleanup_refuses_incomplete_receipts_and_preserves_attempt(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / supervisor.WORKER_FILE).write_text('{"status":"failed"}')
            (root / supervisor.OWNERSHIP_FILE).write_text('{}')
            owned = root / "must-remain"
            owned.mkdir()
            with self.assertRaises(supervisor.PairedSupervisorRefused):
                supervisor._verify_and_cleanup(root, {})
            self.assertTrue(owned.is_dir())

    def test_authorization_requires_runtime_and_source_digests(self):
        approval = self.approval()
        del approval["runtimePinsSha256"]
        with self.assertRaises(PacketRefused):
            _closed_approval(approval, "a" * 64)


if __name__ == "__main__":
    unittest.main()
