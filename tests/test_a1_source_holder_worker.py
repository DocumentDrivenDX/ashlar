# SPDX-License-Identifier: MIT OR Apache-2.0
"""Closed local IPC framing tests; no source or ACK authority is mocked."""
import socket
import threading
import unittest
from unittest.mock import Mock

from ashlar_host.a1_source_holder_worker import (
    A1SourceHolderWorkerError, A1Truss053SourceHolderProcess, receive_frame, send_frame,
)
from ashlar_host.a1_ack_attempt import A1AckSettlement


class A1SourceHolderWorkerTests(unittest.TestCase):
    def test_matched_ack_hold_closes_only_after_explicit_close_rpc(self):
        holder = A1Truss053SourceHolderProcess.__new__(A1Truss053SourceHolderProcess)
        holder.phase = "a0_matched"
        holder.snapshot_bytes = b"held-source-snapshot"
        holder.custody_digests = {"current_page": "page-sha"}
        holder._generation = 3
        holder.operation_timeout_seconds = 2
        holder._closed = False
        holder._heartbeat_error = None
        holder._heartbeat_stop = threading.Event()
        holder._heartbeat = None
        holder.process = Mock()
        holder.process.poll.return_value = None
        holder.channel = Mock()
        holder.channel.gettimeout.return_value = 2
        calls = []
        holder._rpc = lambda command, **kwargs: calls.append((command, kwargs))
        holder._reap_success = Mock()
        settlement = A1AckSettlement("matched", b'{"nextXid":759}', b"receipt",
            "a" * 64, "b" * 64, "c" * 64)

        holder.finish_ack(settlement, attempt_directory="/private/attempt", deadline=None)

        self.assertEqual(calls[0][0]["type"], "finish_ack")
        self.assertEqual(calls[0][0]["generation"], 3)
        self.assertEqual(calls[0][0]["settlement"]["checkpoint_bytes_base64"],
                         "eyJuZXh0WGlkIjo3NTl9")
        self.assertEqual(holder.phase, "a0_closed")
        self.assertIsNone(holder.snapshot_bytes)
        self.assertIsNone(holder.custody_digests)
        holder._reap_success.assert_not_called()

    def test_socketpair_round_trips_canonical_closed_frame(self):
        left, right = socket.socketpair()
        try:
            payload = {"type": "bind", "vector": {"targets": {"a.b.c": ["uuid", 7]}}}
            sender = threading.Thread(target=send_frame, args=(left, payload))
            sender.start()
            self.assertEqual(receive_frame(right), payload)
            sender.join()
        finally:
            left.close()
            right.close()

    def test_socketpair_round_trips_realistic_page_snapshot_frame(self):
        left, right = socket.socketpair()
        try:
            # R61's observed catalog transaction is 31,938 bytes before the
            # detached bundle adds accepted model/catalog evidence.
            payload = {"type": "a1_genesis", "phase": "a1",
                       "genesis_bytes_base64": "A" * 48_000}
            sender = threading.Thread(target=send_frame, args=(left, payload))
            sender.start()
            self.assertEqual(receive_frame(right), payload)
            sender.join()
        finally:
            left.close()
            right.close()

    def test_noncanonical_or_duplicate_json_is_refused(self):
        for raw in (b'{"type":"renew", "type":"close"}', b'{"type": "renew"}'):
            left, right = socket.socketpair()
            try:
                left.sendall(len(raw).to_bytes(4, "big") + raw)
                with self.assertRaises(A1SourceHolderWorkerError):
                    receive_frame(right)
            finally:
                left.close()
                right.close()

    def test_frame_limit_is_checked_before_reading_body(self):
        left, right = socket.socketpair()
        try:
            left.sendall((100).to_bytes(4, "big"))
            with self.assertRaisesRegex(A1SourceHolderWorkerError, "length"):
                receive_frame(right, maximum=99)
        finally:
            left.close()
            right.close()


if __name__ == "__main__":
    unittest.main()
