# SPDX-License-Identifier: MIT OR Apache-2.0
"""Closed local IPC framing tests; no source or ACK authority is mocked."""
import socket
import threading
import unittest

from ashlar_host.a1_source_holder_worker import (
    A1SourceHolderWorkerError, receive_frame, send_frame,
)


class A1SourceHolderWorkerTests(unittest.TestCase):
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
