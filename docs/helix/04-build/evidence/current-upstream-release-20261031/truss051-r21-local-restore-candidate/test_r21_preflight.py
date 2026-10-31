from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import r21_pg18_layout_preflight as preflight


def make_pg18(root: Path):
    root.mkdir()
    (root / "PG_VERSION").write_text("18\n")
    for relative in preflight.PG18_INITDB_DIRS:
        (root / relative).mkdir(parents=True, exist_ok=True)


class Pg18LayoutPreflightTests(unittest.TestCase):
    def test_admits_complete_stopped_pg18_fixture(self):
        with tempfile.TemporaryDirectory() as temporary:
            pgdata = Path(temporary).resolve() / "pgdata"
            make_pg18(pgdata)
            result = preflight.inspect_pg18_layout(pgdata)
            self.assertTrue(result["admitted"], result["problems"])
            self.assertEqual(result["required_directory_count"], 23)
            self.assertEqual(result["missing_directories"], [])

    def test_lists_all_r20_layout_omissions(self):
        with tempfile.TemporaryDirectory() as temporary:
            pgdata = Path(temporary).resolve() / "pgdata"
            make_pg18(pgdata)
            expected = ("pg_wal/archive_status", "pg_wal/summaries", "pg_commit_ts",
                        "pg_dynshmem", "pg_notify", "pg_serial", "pg_snapshots",
                        "pg_twophase", "pg_replslot", "pg_tblspc", "pg_stat_tmp",
                        "pg_logical/snapshots", "pg_logical/mappings")
            for relative in expected:
                (pgdata / relative).rmdir()
            result = preflight.inspect_pg18_layout(pgdata)
            self.assertFalse(result["admitted"])
            self.assertEqual(tuple(result["missing_directories"]), expected)

    def test_rejects_wrong_major_version(self):
        with tempfile.TemporaryDirectory() as temporary:
            pgdata = Path(temporary).resolve() / "pgdata"
            make_pg18(pgdata)
            (pgdata / "PG_VERSION").write_text("17\n")
            result = preflight.inspect_pg18_layout(pgdata)
            self.assertFalse(result["admitted"])
            self.assertIn("PG_VERSION must identify PostgreSQL 18", result["problems"])

    def test_rejects_symlinked_required_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            root = root.resolve()
            pgdata = root / "pgdata"
            make_pg18(pgdata)
            (pgdata / "pg_notify").rmdir()
            (pgdata / "pg_notify").symlink_to(root, target_is_directory=True)
            result = preflight.inspect_pg18_layout(pgdata)
            self.assertFalse(result["admitted"])
            self.assertEqual(result["invalid_directories"], ["pg_notify"])

    def test_rejects_pid_marker(self):
        with tempfile.TemporaryDirectory() as temporary:
            pgdata = Path(temporary).resolve() / "pgdata"
            make_pg18(pgdata)
            (pgdata / "postmaster.pid").write_text("fixture\n")
            result = preflight.inspect_pg18_layout(pgdata)
            self.assertFalse(result["admitted"])
            self.assertTrue(result["postmaster_pid_present"])


if __name__ == "__main__":
    unittest.main()
