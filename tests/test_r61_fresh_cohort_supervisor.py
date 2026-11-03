"""Narrow effect-free checks for R61 supervisor custody bounds."""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools import r61_fresh_cohort_supervisor as supervisor


class _NoSuchProcess(Exception):
    pass


class _AccessDenied(Exception):
    pass


class _FakeProcess:
    def __init__(self, pid, rss, children=()):
        self.pid = pid
        self._rss = rss
        self._children = list(children)
        self.info = {"pid": pid}

    def memory_info(self):
        return type("Memory", (), {"rss": self._rss})()

    def children(self, recursive=False):
        return self._children


class _FakePsutil:
    NoSuchProcess = _NoSuchProcess
    AccessDenied = _AccessDenied

    def __init__(self, processes):
        self.processes = processes

    def process_iter(self, attrs=(), ad_value=None):
        return iter(self.processes.values())

    def Process(self, pid):
        return self.processes[pid]


class R61SupervisorCustodyTests(unittest.TestCase):
    def test_persistent_fasteners_lockfile_is_checked_without_unlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            root.chmod(0o700)
            path = root / "pgdata.truss-runtime.lock"
            path.write_bytes(b"")
            path.chmod(0o600)
            free = supervisor._probe_runtime_lease(path)
            self.assertEqual(free, {"present": True, "unlocked": True})
            self.assertTrue(path.exists())

            holder = subprocess.Popen([
                sys.executable, "-c",
                "import fcntl,sys,time; f=open(sys.argv[1],'a'); fcntl.lockf(f,fcntl.LOCK_EX); print('ready',flush=True); time.sleep(10)",
                str(path)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                self.assertEqual(holder.stdout.readline().strip(), "ready")
                self.assertEqual(supervisor._probe_runtime_lease(path),
                                 {"present": True, "unlocked": False})
            finally:
                holder.terminate()
                holder.wait(timeout=3)
                holder.stdout.close()
                holder.stderr.close()
            self.assertTrue(path.exists())
            self.assertEqual(supervisor._probe_runtime_lease(path),
                             {"present": True, "unlocked": True})

    def test_rss_deduplicates_group_and_escaped_postmaster_tree(self):
        with tempfile.TemporaryDirectory() as temporary:
            pgdata = Path(temporary) / "pgdata"
            pgdata.mkdir(mode=0o700)
            pidfile = pgdata / "postmaster.pid"
            pidfile.write_text(f"41\n{pgdata}\n")
            pidfile.chmod(0o600)
            child = _FakeProcess(42, 7)
            postmaster = _FakeProcess(41, 11, [child])
            fake = _FakePsutil({41: postmaster, 42: child})
            with patch.object(supervisor.os, "getpgid", side_effect=lambda pid: 99 if pid == 41 else 100):
                rss, group, postgres_tree = supervisor._group_snapshot(fake, 99, pgdata)
            self.assertEqual((rss, group, postgres_tree), (18, [41], [41, 42]))

    def test_rss_census_denial_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            pgdata = Path(temporary) / "pgdata"
            pgdata.mkdir(mode=0o700)
            proc = _FakeProcess(41, 11)
            fake = _FakePsutil({41: proc})
            with patch.object(supervisor.os, "getpgid", side_effect=PermissionError("census denied")):
                with self.assertRaisesRegex(RuntimeError, "census denied"):
                    supervisor._group_snapshot(fake, 99, pgdata)

    def test_reserved_receipt_is_written_without_deadline_dependency(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "supervisor-result.json"
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                supervisor._write_reserved_receipt(
                    fd, path, {"status": "failed_after_deadline", "effect_free": False})
            finally:
                os.close(fd)
            self.assertEqual(json.loads(path.read_text()),
                             {"status": "failed_after_deadline", "effect_free": False})


if __name__ == "__main__":
    unittest.main()
