# SPDX-License-Identifier: MIT OR Apache-2.0
"""Closed local IPC framing tests; no source or ACK authority is mocked."""
import socket
import threading
import unittest
from unittest.mock import Mock

from ashlar_host.a1_source_holder_worker import (
    A1SourceHolderWorkerError, A1Truss053SourceHolderProcess, receive_frame, send_frame,
    run_worker,
)
from ashlar_host.a1_ack_attempt import A1AckSettlement, MAX_SETTLEMENT_RECORD_BYTES
from ashlar_host.truss053_source_custody import Truss053A1SourceHolder


class A1SourceHolderWorkerTests(unittest.TestCase):
    def test_worker_and_parent_accept_only_explicit_resumed_a1_genesis(self):
        from test_truss_detached_a1_genesis_codec import _raw

        server, client_channel = socket.socketpair()
        source = object.__new__(Truss053A1SourceHolder)
        source._closed = False
        source.phase = "a1"
        genesis = _raw()
        source.select_genesis = lambda: genesis
        source.close_before_ack = lambda: setattr(source, "_closed", True)
        server_error = []

        def serve():
            try:
                run_worker(server, source)
            except BaseException as error:
                server_error.append(error)

        thread = threading.Thread(target=serve)
        thread.start()
        try:
            process = Mock()
            process.pid = 12345
            process.poll.return_value = None
            client = A1Truss053SourceHolderProcess(process, client_channel,
                operation_timeout_seconds=2)
            self.assertEqual(client.wait_genesis(expected_phase="a1",
                expected_a_prior_cursor="0"), genesis)
            self.assertEqual(client.phase, "a1")
            self.assertEqual(client._rpc({"type": "close"}, response_type="closed",
                                         maximum=1024), {"type": "closed"})
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(server_error, [])
        finally:
            client_channel.close()
            server.close()

    def test_real_bind_command_and_bound_response_round_trip(self):
        server, client_channel = socket.socketpair()
        source = object.__new__(Truss053A1SourceHolder)
        source._closed = False
        source.phase = "a0"
        bound = []
        source.select_genesis = lambda: b"bounded local protocol fixture"
        source.bind_publication = lambda **value: bound.append(value)
        source.close_before_ack = lambda: setattr(source, "_closed", True)
        server_error = []
        manifest = b'{"manifest":"' + b"m" * 2048 + b'"}'
        receipt = b'{"receipt":"' + b"r" * 2048 + b'"}'

        def serve():
            try:
                run_worker(server, source)
            except BaseException as error:
                server_error.append(error)

        thread = threading.Thread(target=serve)
        thread.start()
        try:
            ready = receive_frame(client_channel)
            self.assertEqual(ready, {"type": "genesis", "phase": "a0",
                                     "genesis_bytes_base64": "Ym91bmRlZCBsb2NhbCBwcm90b2NvbCBmaXh0dXJl"})
            process = Mock()
            process.pid = 12345
            process.poll.return_value = None
            client = A1Truss053SourceHolderProcess(process, client_channel,
                operation_timeout_seconds=2)
            client.genesis_bytes = b"genesis"
            client.snapshot_bytes = b"held"
            client.custody_digests = {"current_page": "page"}
            client._heartbeat_error = None
            vector = {"profile": "ashlar-pin-vector/0.1", "targets": {}}
            client.bind_publication(manifest_bytes=manifest,
                vector_payload=vector, receipt_bytes=receipt)
            self.assertEqual(bound, [{"manifest_bytes": manifest,
                "vector_payload": vector, "receipt_bytes": receipt}])
            self.assertEqual(client._rpc({"type": "close"}, response_type="closed",
                                         maximum=1024), {"type": "closed"})
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(server_error, [])
        finally:
            client_channel.close()
            server.close()

    def test_worker_settlement_frame_accepts_declared_maximum_payload(self):
        server, client = socket.socketpair()
        source = object.__new__(Truss053A1SourceHolder)
        source._closed = False
        source.phase = "a0"
        source.select_genesis = lambda: b"genesis"
        source.acknowledge_once = lambda **kwargs: A1AckSettlement(
            "matched", b"c" * (1024 * 1024), b"r" * (1024 * 1024),
            "a" * 64, "b" * 64, "c" * 64)
        source.close_before_ack = lambda: setattr(source, "_closed", True)
        errors = []

        def serve():
            try:
                run_worker(server, source)
            except BaseException as error:
                errors.append(error)

        thread = threading.Thread(target=serve)
        thread.start()
        try:
            self.assertEqual(receive_frame(client)["type"], "genesis")
            send_frame(client, {"type": "ack", "attempt_directory": "/private/tmp/attempt",
                                "generation": 1})
            result = receive_frame(client, maximum=MAX_SETTLEMENT_RECORD_BYTES)
            self.assertEqual(result["type"], "settlement")
            self.assertEqual(len(result["checkpoint_bytes_base64"]),
                             4 * ((1024 * 1024 + 2) // 3))
            self.assertEqual(len(result["receipt_bytes_base64"]),
                             4 * ((1024 * 1024 + 2) // 3))
            send_frame(client, {"type": "close"})
            self.assertEqual(receive_frame(client)["type"], "closed")
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
        finally:
            client.close()
            server.close()

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
        self.assertIsNone(holder._heartbeat)
        self.assertFalse(holder._heartbeat_stop.is_set())
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
