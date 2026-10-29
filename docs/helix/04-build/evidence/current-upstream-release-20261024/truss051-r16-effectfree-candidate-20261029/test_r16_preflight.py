from __future__ import annotations

import argparse
import contextlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import r16_preflight_worker as worker
import run_supervisor_r16 as supervisor


class R16PreflightTests(unittest.TestCase):
    def test_archive_hash_mismatch_stops_before_external_commands(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            files = [root / name for name in ("archive", "wheel", "pg_ctl", "python", "seed")]
            for file in files:
                file.write_bytes(b"not the admitted input")
            args = argparse.Namespace(
                archive=str(files[0]), wheel=str(files[1]), pg_ctl=str(files[2]),
                runtime_python=str(files[3]), seed_request=str(files[4]),
            )
            with patch.object(worker, "run_readonly") as run:
                with self.assertRaisesRegex(ValueError, "source archive hash mismatch"):
                    worker.inspect(args)
                run.assert_not_called()

    def test_worker_requires_preflight_only_flag(self) -> None:
        with patch("sys.argv", ["r16_preflight_worker.py"]), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                worker.main()
        self.assertEqual(raised.exception.code, 2)

    def test_admitted_input_constants_are_fixed_and_execution_disabled(self) -> None:
        self.assertEqual(worker.COMMIT, "48fb9cb047f95854d59d0882af31156aab5f1f99")
        self.assertEqual(worker.ARCHIVE_SHA256, "cb57fdb7d338f345cfad157463fdbe94a2be7dcf69e728c474954c2939c14a0b")
        self.assertEqual(worker.TRUSS_WHEEL_SHA256, "eb59141c48e94bb93ab7053e9e8725012eee1749e4432e9c71804d23abbdff57")
        self.assertEqual(worker.PG_CTL_SHA256, "3021087af95fd33bfd83106c85de47c4756e44aa7edde78446e7593604d1b451")
        self.assertEqual(worker.SEED_REQUEST_SHA256, "56aed4d7a4789f45cdaa7af55a834ff38ca2b4bcb75f44658b3031ba5f8aa771")
        self.assertEqual(worker.PIN_LINE.split("#sha256=")[1].split("\"]")[0], worker.TRUSS_WHEEL_SHA256)
        self.assertEqual(supervisor.sha256(Path(worker.__file__)), supervisor.WORKER_SHA256)
        self.assertEqual(supervisor.sha256(Path(supervisor.__file__)), "a82c42f454889849147df5ab1126325781bed91629df0ff0bc6489e54816aba9")


if __name__ == "__main__":
    unittest.main()
