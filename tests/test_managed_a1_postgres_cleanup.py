# SPDX-License-Identifier: MIT OR Apache-2.0
"""Safety checks for stopping only the exact private Truss PGDATA."""
import os
from pathlib import Path
import signal
import tempfile
import time
import unittest
from unittest.mock import patch

from ashlar_host.managed_a1_postgres_cleanup import (
    ManagedA1PostgresCleanupError, cleanup_owned_postgres,
)


class ManagedA1PostgresCleanupTests(unittest.TestCase):
    def make_pgdata(self, root, pid):
        pgdata = root / "pgdata-A"
        pgdata.mkdir(mode=0o700)
        (pgdata / "PG_VERSION").write_text("18\n")
        (pgdata / "global").mkdir()
        (pgdata / "global" / "pg_control").write_bytes(b"preserved-control")
        pidfile = pgdata / "postmaster.pid"
        pidfile.write_text(f"{pid}\n{pgdata}\n")
        os.chmod(pidfile, 0o600)
        return pgdata

    def test_negative_initdb_pid_only_signals_verified_positive_process_id(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            root = root.resolve()
            os.chmod(root, 0o700)
            pgdata = self.make_pgdata(root, -42)
            pg_ctl = root / "pg_ctl"
            pg_ctl.write_text("pinned tool")
            control_tool = root / "pg_controldata"
            pg_ctl_lock = {"path": str(pg_ctl), "sha256": "fixed", "version": "18.6",
                "tools": {"pg_controldata": {"path": str(control_tool)}}}
            signal_calls = []

            def signal_verified(pid, sig):
                signal_calls.append((pid, sig))
                (pgdata / "postmaster.pid").unlink()

            with patch("ashlar_host.managed_a1_postgres_cleanup.inspect_pg_control",
                       return_value={"pg_version": "18"}), \
                 patch("ashlar_host.managed_a1_postgres_cleanup.inspect_pg_ctl",
                       return_value=pg_ctl_lock), \
                 patch("ashlar_host.managed_a1_postgres_cleanup._process_command",
                       return_value="postgres --single -D " + str(pgdata)), \
                 patch("ashlar_host.managed_a1_postgres_cleanup.os.kill",
                       side_effect=signal_verified):
                outcome = cleanup_owned_postgres(pgdata, pg_ctl_lock,
                    deadline=time.monotonic() + 3,
                    pg_controldata=root / "pg_controldata")
            self.assertEqual(signal_calls, [(42, signal.SIGTERM)])
            self.assertTrue(outcome["postgres_stopped"])
            self.assertTrue(outcome["pgdata_preserved"])
            self.assertTrue((pgdata / "global" / "pg_control").exists())

    def test_wrong_process_identity_refuses_stop_and_preserves_pidfile(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            root = root.resolve()
            os.chmod(root, 0o700)
            pgdata = self.make_pgdata(root, 42)
            pg_ctl = root / "pg_ctl"
            pg_ctl.write_text("pinned tool")
            control_tool = root / "pg_controldata"
            pg_ctl_lock = {"path": str(pg_ctl), "sha256": "fixed", "version": "18.6",
                "tools": {"pg_controldata": {"path": str(control_tool)}}}
            with patch("ashlar_host.managed_a1_postgres_cleanup.inspect_pg_control",
                       return_value={"pg_version": "18"}), \
                 patch("ashlar_host.managed_a1_postgres_cleanup.inspect_pg_ctl",
                       return_value=pg_ctl_lock), \
                 patch("ashlar_host.managed_a1_postgres_cleanup._process_command",
                       side_effect=ManagedA1PostgresCleanupError("foreign process")), \
                 patch("ashlar_host.managed_a1_postgres_cleanup.os.kill") as kill:
                with self.assertRaisesRegex(ManagedA1PostgresCleanupError, "foreign process"):
                    cleanup_owned_postgres(pgdata, pg_ctl_lock,
                        deadline=time.monotonic() + 3,
                        pg_controldata=root / "pg_controldata")
            kill.assert_not_called()
            self.assertTrue((pgdata / "postmaster.pid").exists())
            self.assertTrue((pgdata / "global" / "pg_control").exists())


if __name__ == "__main__":
    unittest.main()
