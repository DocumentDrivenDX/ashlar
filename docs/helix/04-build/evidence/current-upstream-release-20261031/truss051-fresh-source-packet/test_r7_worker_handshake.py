from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("r7_worker", HERE / "r7_truss051_local_worker.py")
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)


class SupervisorHandshakeTest(unittest.TestCase):
    def test_supervisor_created_root_and_logs_pass_child_preflight(self):
        with tempfile.TemporaryDirectory() as temp:
            temp = Path(temp).resolve()
            output = temp / "worker-output"
            supervisor = temp / "supervisor"
            supervisor.mkdir(mode=0o700)
            for name in ("child.stdout.log", "child.stderr.log"):
                (supervisor / name).touch(mode=0o600)
            worker.validate_supervisor_handshake(output, supervisor)
            self.assertFalse(output.exists())

    def test_worker_output_root_must_remain_absent(self):
        with tempfile.TemporaryDirectory() as temp:
            temp = Path(temp).resolve()
            output = temp / "worker-output"
            supervisor = temp / "supervisor"
            supervisor.mkdir(mode=0o700)
            for name in ("child.stdout.log", "child.stderr.log"):
                (supervisor / name).touch(mode=0o600)
            output.mkdir()
            with self.assertRaises(worker.StopRun):
                worker.validate_supervisor_handshake(output, supervisor)

    def test_missing_supervisor_log_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            temp = Path(temp).resolve()
            supervisor = temp / "supervisor"
            supervisor.mkdir(mode=0o700)
            (supervisor / "child.stdout.log").touch(mode=0o600)
            with self.assertRaises(worker.StopRun):
                worker.validate_supervisor_handshake(temp / "worker-output", supervisor)


if __name__ == "__main__":
    unittest.main()
