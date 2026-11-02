# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure one-use A1 ACK attempt and read-only recovery record checks."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from ashlar_host.a1_ack_attempt import (
    A1AckAttemptError, A1AckSettlement, DurableA1AckAttemptStore,
    recover_lost_a1_ack,
)


class _ReadOnlyReconciler:
    def __init__(self, settlement):
        self.settlement = settlement
        self.calls = []

    def reconcile(self, attempt):
        self.calls.append(attempt)
        return self.settlement


class A1AckAttemptTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.store = DurableA1AckAttemptStore(self.root)
        self.values = dict(
            selection_bytes=b"exact selection wire bytes",
            request_bytes=b"exact request bytes",
            manifest_bytes=b"exact manifest bytes",
            receipt_bytes=b"exact receipt bytes",
            prior_checkpoint_bytes=b"prior-cursor",
            next_checkpoint_bytes=b"next-cursor",
            page_sha256="a" * 64,
            destination_id="destination-a",
            publication_id="publication-a")
        self.attempt = self.store.begin(**self.values)

    def settlement(self, *, outcome="matched", checkpoint=None, receipt=None):
        return A1AckSettlement(outcome,
            self.values["next_checkpoint_bytes"] if checkpoint is None else checkpoint,
            self.values["receipt_bytes"] if receipt is None else receipt,
            self.attempt.request_sha256, self.attempt.manifest_sha256,
            self.attempt.selection_sha256)

    def _lost_worker_record(self):
        child = subprocess.Popen([sys.executable, "-c", "pass"],
                                 start_new_session=True, close_fds=True)
        pgid = child.pid
        child.wait(timeout=3)
        # Ensure this test's PID is not reused as a live process group.
        with self.assertRaises(ProcessLookupError):
            os.killpg(pgid, 0)
        self.store.record_worker_started(process_group_id=pgid)
        self.store.mark_worker_lost(process_group_id=pgid,
                                    reason="ack_or_reconcile_unresolved")
        return pgid

    def test_persists_exact_bytes_before_any_worker_and_refuses_reuse(self):
        raw = self.store.path.read_bytes()
        loaded = self.store.load()
        self.assertEqual(loaded, self.attempt)
        self.assertEqual(self.store.path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.store.path.parent.stat().st_mode & 0o077, 0)
        self.assertEqual(loaded.selection_bytes, self.values["selection_bytes"])
        self.assertEqual(loaded.request_bytes, self.values["request_bytes"])
        self.assertEqual(loaded.manifest_bytes, self.values["manifest_bytes"])
        self.assertEqual(loaded.receipt_bytes, self.values["receipt_bytes"])
        with self.assertRaisesRegex(A1AckAttemptError, "Fresh bounded durable attempt path"):
            self.store.begin(**self.values)

    def test_matched_settlement_requires_exact_receipt_and_next_checkpoint(self):
        self.assertEqual(self.store.record_settlement(self.settlement()), "matched")
        self.assertTrue((self.root / "worker-settlement.json").is_file())
        other_temp = tempfile.TemporaryDirectory()
        self.addCleanup(other_temp.cleanup)
        other = Path(other_temp.name).resolve()
        store = DurableA1AckAttemptStore(other)
        attempt = store.begin(**self.values)
        mismatch = A1AckSettlement("matched", b"wrong cursor", b"wrong receipt",
            attempt.request_sha256, attempt.manifest_sha256, attempt.selection_sha256)
        with self.assertRaisesRegex(A1AckAttemptError, "exact original receipt/checkpoint"):
            store.record_settlement(mismatch)

    def test_lost_worker_uses_only_exact_read_only_reconciliation(self):
        self._lost_worker_record()
        observation = self.settlement()
        owner = _ReadOnlyReconciler(observation)
        self.assertIs(recover_lost_a1_ack(self.store, owner), observation)
        self.assertEqual(owner.calls, [self.attempt])
        saved = (self.root / "read-only-reconciliation.json").read_bytes()
        self.assertIn(b'"ack_retry_permitted":false', saved)
        self.assertIn(hashlib.sha256(self.values["receipt_bytes"]).hexdigest().encode(),
                      self.store.path.read_bytes())

    def test_recovery_refuses_live_process_group_before_calling_reconciler(self):
        self.store.record_worker_started(process_group_id=os.getpgrp())
        self.store.mark_worker_lost(process_group_id=os.getpgrp(), reason="timeout")
        owner = _ReadOnlyReconciler(self.settlement())
        with self.assertRaisesRegex(A1AckAttemptError, "process group remains live"):
            recover_lost_a1_ack(self.store, owner)
        self.assertEqual(owner.calls, [])

    def test_worker_loss_must_match_durable_original_worker_group(self):
        with self.assertRaisesRegex(A1AckAttemptError, "durable record is absent"):
            self.store.mark_worker_lost(process_group_id=99999999, reason="timeout")
        with self.assertRaisesRegex(A1AckAttemptError, "durable record is absent"):
            recover_lost_a1_ack(self.store, _ReadOnlyReconciler(self.settlement()))

    def test_missing_or_conflicting_reconciliation_never_records_success(self):
        for outcome in ("missing", "conflicting"):
            directory_temp = tempfile.TemporaryDirectory()
            self.addCleanup(directory_temp.cleanup)
            directory = Path(directory_temp.name).resolve()
            store = DurableA1AckAttemptStore(directory)
            attempt = store.begin(**self.values)
            child = subprocess.Popen([sys.executable, "-c", "pass"],
                                     start_new_session=True, close_fds=True)
            child.wait(timeout=3)
            with self.assertRaises(ProcessLookupError):
                os.killpg(child.pid, 0)
            store.record_worker_started(process_group_id=child.pid)
            store.mark_worker_lost(process_group_id=child.pid, reason="timeout")
            settlement = A1AckSettlement(outcome, b"prior-or-current", None,
                attempt.request_sha256, attempt.manifest_sha256,
                attempt.selection_sha256)
            owner = _ReadOnlyReconciler(settlement)
            self.assertIs(recover_lost_a1_ack(store, owner), settlement)
            record = (directory / "read-only-reconciliation.json").read_bytes()
            self.assertIn(b'"status":"incomplete"', record)
            self.assertNotIn(b'"status":"matched"', record)


if __name__ == "__main__":
    unittest.main()
