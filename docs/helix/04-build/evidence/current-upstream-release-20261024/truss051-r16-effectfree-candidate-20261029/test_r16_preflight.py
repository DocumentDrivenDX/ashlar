from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import r16_preflight_worker as worker
import run_supervisor_r16 as supervisor

PACKET = Path(__file__).resolve().parent
HELPER_PATH = PACKET / "r16_pre_migration_source_helper.py"
HELPER_SPEC = importlib.util.spec_from_file_location("r16_source_helper_tests", HELPER_PATH)
assert HELPER_SPEC is not None and HELPER_SPEC.loader is not None
source_helper = importlib.util.module_from_spec(HELPER_SPEC)
HELPER_SPEC.loader.exec_module(source_helper)
ORIGIN_GATE_PATH = PACKET / "r16_native_origin_gate.py"
ORIGIN_GATE_SPEC = importlib.util.spec_from_file_location("r16_origin_gate_tests", ORIGIN_GATE_PATH)
assert ORIGIN_GATE_SPEC is not None and ORIGIN_GATE_SPEC.loader is not None
origin_gate = importlib.util.module_from_spec(ORIGIN_GATE_SPEC)
ORIGIN_GATE_SPEC.loader.exec_module(origin_gate)


class R16PreflightTests(unittest.TestCase):
    def test_source_matcher_requires_exact_bytes_and_identity(self) -> None:
        content = b"fixture source"
        expected = {
            "document_id": "doc",
            "document_revision": "rev",
            "registration_id": "registration",
        }
        accepted = argparse.Namespace(
            content=content,
            sha256=hashlib.sha256(content).hexdigest(),
            document_id="doc",
            document_revision="rev",
            registration_id="registration",
        )
        sha = hashlib.sha256(content).hexdigest()
        self.assertTrue(source_helper.accepted_source_matches(accepted, expected, sha))
        accepted.content = bytearray(content)
        self.assertFalse(source_helper.accepted_source_matches(accepted, expected, sha))
        accepted.content = content
        accepted.registration_id = "other"
        self.assertFalse(source_helper.accepted_source_matches(accepted, expected, sha))

    def test_helper_requires_explicit_frozen_source_root(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "ASHLAR_R16_SOURCE_SRC"):
                source_helper.require_frozen_source_root()

    def test_effectful_helper_modes_refuse_missing_source_before_connection(self) -> None:
        with patch.dict(os.environ, {}, clear=True), patch.object(source_helper, "connection") as connect:
            with patch("sys.argv", [str(HELPER_PATH), "--custody"]):
                with self.assertRaisesRegex(RuntimeError, "ASHLAR_R16_SOURCE_SRC"):
                    source_helper.main()
            connect.assert_not_called()

    def test_genesis_archive_validates_before_write_and_omits_unsupported_facts(self) -> None:
        prior = json.dumps({"registrationId": source_helper.REGISTRATION,
                            "consumerId": source_helper.CONSUMER, "nextXid": "0"}).encode()
        nxt = json.dumps({"registrationId": source_helper.REGISTRATION,
                          "consumerId": source_helper.CONSUMER, "nextXid": "123"}).encode()
        page = argparse.Namespace(
            consumer_id=source_helper.CONSUMER,
            registration_id=source_helper.REGISTRATION,
            writer_xid="100",
            sha256="a" * 64,
            prior=argparse.Namespace(content=prior),
            next=argparse.Namespace(content=nxt),
        )
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "page.json"
            invalid = argparse.Namespace(**vars(page))
            invalid.registration_id = "wrong"
            encoder = Mock(return_value=b"archive")
            with self.assertRaisesRegex(RuntimeError, "genesis checkpoint"):
                source_helper.write_genesis_page_archive(invalid, output, encoder)
            self.assertFalse(output.exists())
            encoder.assert_not_called()

            result = source_helper.write_genesis_page_archive(page, output, lambda _page, _feed: b"archive")
            self.assertEqual(output.read_bytes(), b"archive")
            self.assertNotIn("facts", result)

    def test_missing_or_mismatched_origin_blocks_before_native_stages(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            with self.assertRaisesRegex(ValueError, "absent"):
                origin_gate.verify_origin_map(root / "missing.json")
            wrong = root / "origin.json"
            wrong.write_text("{}")
            with self.assertRaisesRegex(ValueError, "SHA-256 differs"):
                origin_gate.verify_origin_map(wrong)
            self.assertEqual(list(root.iterdir()), [wrong])
        proposed = PACKET / "origin-map-r16-proposed.json"
        checked = origin_gate.verify_origin_map(proposed.resolve())
        self.assertEqual(checked["sha256"], origin_gate.PROPOSED_ORIGIN_SHA256)
        self.assertEqual(origin_gate.PROPOSED_ORIGIN_MAP["authority_scope"], "local_truss_acceptance_trial")
        self.assertTrue(checked["review_required"])
        self.assertFalse(checked["execution_authorized"])

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
