# SPDX-License-Identifier: MIT OR Apache-2.0
"""Long-lived same-host IPC worker for the fresh Truss .53 A1 source hold."""
from __future__ import annotations

import argparse
import base64
import importlib
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import sys
import threading
import time

from .truss053_source_custody import Truss053A1SourceHolder


MAX_COMMAND_BYTES = 24 * 1024 * 1024
MAX_READY_BYTES = 96 * 1024 * 1024
MAX_FRAME_BYTES = MAX_READY_BYTES


class A1SourceHolderWorkerError(RuntimeError):
    """Malformed IPC, unavailable holder, or incomplete source operation."""


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _pairs(items):
    out = {}
    for key, value in items:
        if key in out:
            raise ValueError("duplicate JSON member")
        out[key] = value
    return out


def _recv_exact(channel: socket.socket, count: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < count:
        part = channel.recv(count - len(chunks))
        if not part:
            raise EOFError("source-holder IPC closed")
        chunks.extend(part)
    return bytes(chunks)


def send_frame(channel: socket.socket, value: dict, *, maximum=MAX_FRAME_BYTES):
    raw = _canonical(value)
    if len(raw) > maximum:
        raise A1SourceHolderWorkerError("Bounded canonical source-holder frame required")
    channel.sendall(struct.pack(">I", len(raw)) + raw)


def receive_frame(channel: socket.socket, *, maximum=MAX_COMMAND_BYTES) -> dict:
    size = struct.unpack(">I", _recv_exact(channel, 4))[0]
    if not 0 < size <= maximum:
        raise A1SourceHolderWorkerError("Bounded source-holder frame length required")
    raw = _recv_exact(channel, size)
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (UnicodeError, ValueError, json.JSONDecodeError) as error:
        raise A1SourceHolderWorkerError("Closed exact source-holder JSON required") from error
    if type(value) is not dict or _canonical(value) != raw:
        raise A1SourceHolderWorkerError("Canonical source-holder object required")
    return value


def _bytes(value, label, maximum):
    if type(value) is not str or len(value) > 4 * ((maximum + 2) // 3):
        raise A1SourceHolderWorkerError(f"Bounded exact {label} bytes required")
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, TypeError) as error:
        raise A1SourceHolderWorkerError(f"Canonical {label} base64 required") from error
    if len(raw) > maximum or base64.b64encode(raw).decode("ascii") != value:
        raise A1SourceHolderWorkerError(f"Bounded canonical {label} bytes required")
    return raw


def _load_bootstrap(reference):
    if type(reference) is not str or reference.count(":") != 1:
        raise A1SourceHolderWorkerError("Trusted module:function source bootstrap required")
    module_name, function_name = reference.split(":", 1)
    if (not module_name or not function_name or any(not part.isidentifier()
            for part in module_name.split(".")) or not function_name.isidentifier()):
        raise A1SourceHolderWorkerError("Canonical trusted source bootstrap name required")
    factory = getattr(importlib.import_module(module_name), function_name, None)
    if not callable(factory):
        raise A1SourceHolderWorkerError("Trusted source bootstrap callable required")
    return factory


def run_worker(channel: socket.socket, holder: Truss053A1SourceHolder):
    """Serve a single source custody interval; authority never leaves this child."""
    if type(holder) is not Truss053A1SourceHolder:
        raise A1SourceHolderWorkerError("Exact .53 source holder required")
    genesis = holder.select_genesis()
    send_frame(channel, {"type": "genesis", "phase": "a0",
        "genesis_bytes_base64": base64.b64encode(genesis).decode("ascii")},
        maximum=MAX_READY_BYTES)
    while True:
        command = receive_frame(channel)
        kind = command.get("type")
        if kind == "advance_a1" and set(command) == {
                "type", "outcome", "checkpoint_bytes_base64", "receipt_bytes_base64",
                "request_sha256", "manifest_sha256", "selection_sha256"}:
            from .a1_ack_attempt import A1AckSettlement
            checkpoint = _bytes(command["checkpoint_bytes_base64"],
                                 "A0 settlement checkpoint", 1024 * 1024)
            receipt = (None if command["receipt_bytes_base64"] is None else
                _bytes(command["receipt_bytes_base64"], "A0 receipt", 1024 * 1024))
            settlement = A1AckSettlement(command["outcome"], checkpoint, receipt,
                command["request_sha256"], command["manifest_sha256"],
                command["selection_sha256"])
            raw = holder.advance_after_a0_ack(settlement=settlement)
            send_frame(channel, {"type": "a1_genesis", "phase": "a1",
                "genesis_bytes_base64": base64.b64encode(raw).decode("ascii")},
                maximum=MAX_READY_BYTES)
            continue
        if kind == "open" and set(command) == {
                "type", "request_bytes_base64", "destination_id", "publication_id"}:
            request = _bytes(command["request_bytes_base64"], "request", 4 * 1024 * 1024)
            snapshot = holder.open_prepublication(request_bytes=request,
                destination_id=command["destination_id"],
                publication_id=command["publication_id"])
            send_frame(channel, {"type": "ready",
                "snapshot_bytes_base64": base64.b64encode(snapshot).decode("ascii"),
                "custody_digests": holder.custody_digests}, maximum=MAX_FRAME_BYTES)
            continue
        if kind == "renew" and set(command) == {"type"}:
            holder.renew()
            send_frame(channel, {"type": "renewed",
                "custody_digests": holder.custody_digests}, maximum=4096)
        elif kind == "bind" and set(command) == {
                "type", "manifest_bytes_base64", "vector", "receipt_bytes_base64"}:
            manifest = _bytes(command["manifest_bytes_base64"], "manifest", 4 * 1024 * 1024)
            receipt = _bytes(command["receipt_bytes_base64"], "receipt", 1024 * 1024)
            holder.bind_publication(manifest_bytes=manifest,
                vector_payload=command["vector"], receipt_bytes=receipt)
            send_frame(channel, {"type": "bound"}, maximum=1024)
        elif kind == "ack" and set(command) == {
                "type", "attempt_directory", "generation"}:
            attempt_directory = command["attempt_directory"]
            if (type(attempt_directory) is not str or not Path(attempt_directory).is_absolute()
                    or type(command["generation"]) is not int):
                raise A1SourceHolderWorkerError("Exact durable attempt directory and generation required")
            result = holder.acknowledge_once(
                attempt_directory=Path(attempt_directory), generation=command["generation"])
            send_frame(channel, {"type": "settlement", "outcome": result.outcome,
                "checkpoint_bytes_base64": base64.b64encode(
                    result.checkpoint_bytes).decode("ascii"),
                "receipt_bytes_base64": None if result.receipt_bytes is None else
                    base64.b64encode(result.receipt_bytes).decode("ascii"),
                "request_sha256": result.request_sha256,
                "manifest_sha256": result.manifest_sha256,
                "selection_sha256": result.selection_sha256}, maximum=2 * 1024 * 1024)
            if result.outcome == "matched":
                continue
            return
        elif kind == "finish_ack" and set(command) == {
                "type", "attempt_directory", "generation", "settlement"}:
            value = command["settlement"]
            if (type(value) is not dict or set(value) != {
                    "outcome", "checkpoint_bytes_base64", "receipt_bytes_base64",
                    "request_sha256", "manifest_sha256", "selection_sha256"}):
                raise A1SourceHolderWorkerError("Exact matched settlement required to close source hold")
            from .a1_ack_attempt import A1AckSettlement
            checkpoint = _bytes(value["checkpoint_bytes_base64"], "settlement checkpoint", 1024 * 1024)
            receipt = (None if value["receipt_bytes_base64"] is None else
                _bytes(value["receipt_bytes_base64"], "settlement receipt", 1024 * 1024))
            settlement = A1AckSettlement(value["outcome"], checkpoint, receipt,
                value["request_sha256"], value["manifest_sha256"], value["selection_sha256"])
            attempt_directory = command["attempt_directory"]
            generation = command["generation"]
            if (type(attempt_directory) is not str or not Path(attempt_directory).is_absolute()
                    or type(generation) is not int):
                raise A1SourceHolderWorkerError("Exact durable ACK attempt required")
            holder.finish_ack(settlement=settlement,
                attempt_directory=Path(attempt_directory), generation=generation)
            send_frame(channel, {"type": "closed_ack"}, maximum=1024)
            if holder.phase == "a0_closed":
                continue
            return
        elif kind == "close" and set(command) == {"type"}:
            holder.close_before_ack()
            send_frame(channel, {"type": "closed"}, maximum=1024)
            return
        else:
            raise A1SourceHolderWorkerError("Unknown or noncanonical source-holder command")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--fd", required=True, type=int)
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)
    if args.fd < 3:
        raise A1SourceHolderWorkerError("Inherited private IPC file descriptor required")
    path = Path(args.config)
    if (not path.is_absolute() or path.is_symlink() or not path.is_file()
            or path.resolve(strict=True) != path):
        raise A1SourceHolderWorkerError("Canonical private bootstrap configuration required")
    info = path.stat()
    if info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_size > 4 * 1024 * 1024:
        raise A1SourceHolderWorkerError("Owner-private bounded bootstrap configuration required")
    factory = _load_bootstrap(args.bootstrap)
    holder = factory(path.read_bytes())
    channel = socket.socket(fileno=args.fd)
    try:
        run_worker(channel, holder)
    finally:
        if not holder._closed:
            holder.close_before_ack()
        channel.close()


class A1Truss053SourceHolderProcess:
    """Own one child process group and heartbeat its original A1 source hold."""

    def __init__(self, process, channel, *, operation_timeout_seconds=30,
                 renewal_interval_seconds=15):
        if (process.pid <= 1 or type(operation_timeout_seconds) is not int
                or not 1 <= operation_timeout_seconds <= 900
                or type(renewal_interval_seconds) not in (int, float)
                or not 1 <= renewal_interval_seconds <= 60):
            raise A1SourceHolderWorkerError("Bounded original worker process and deadlines required")
        self.process, self.channel = process, channel
        self.process_group_id = process.pid
        self.operation_timeout_seconds = operation_timeout_seconds
        self.renewal_interval_seconds = renewal_interval_seconds
        self._lock = threading.RLock()
        self._heartbeat_stop = threading.Event()
        self._heartbeat = None
        self._heartbeat_error = None
        self._closed = False
        self._generation = None
        self.snapshot_bytes = None
        self.phase = None
        channel.settimeout(operation_timeout_seconds)

    @classmethod
    def spawn(cls, *, bootstrap: str, config_path, python=None,
              operation_timeout_seconds=30, renewal_interval_seconds=15):
        """Spawn before publication; child gets only the private config path and IPC fd."""
        path = Path(config_path)
        if (not path.is_absolute() or path.is_symlink() or not path.is_file()
                or path.resolve(strict=True) != path):
            raise A1SourceHolderWorkerError("Canonical private child bootstrap config required")
        info = path.stat()
        if info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_size > 4 * 1024 * 1024:
            raise A1SourceHolderWorkerError("Owner-private bounded child bootstrap config required")
        parent, child = socket.socketpair()
        child.set_inheritable(True)
        executable = python or sys.executable
        command = [executable, "-m", "ashlar_host.a1_source_holder_worker",
                   "--fd", str(child.fileno()), "--bootstrap", bootstrap,
                   "--config", str(path)]
        env = dict(os.environ)
        source_root = str(Path(__file__).resolve().parents[1])
        existing = env.get("PYTHONPATH")
        env["PYTHONPATH"] = source_root if not existing else source_root + os.pathsep + existing
        try:
            process = subprocess.Popen(command, pass_fds=(child.fileno(),),
                start_new_session=True, close_fds=True, env=env)
        except BaseException:
            parent.close()
            child.close()
            raise
        child.close()
        return cls(process, parent, operation_timeout_seconds=operation_timeout_seconds,
                   renewal_interval_seconds=renewal_interval_seconds)

    def _rpc(self, command, *, response_type, maximum=MAX_COMMAND_BYTES):
        with self._lock:
            if self._closed:
                raise A1SourceHolderWorkerError("Original source-holder process is closed")
            send_frame(self.channel, command, maximum=maximum)
            response = receive_frame(self.channel, maximum=MAX_FRAME_BYTES)
            if response.get("type") != response_type:
                raise A1SourceHolderWorkerError(
                    "Original source-holder returned an unexpected IPC response")
            return response

    def wait_ready(self):
        response = receive_frame(self.channel, maximum=MAX_READY_BYTES)
        if set(response) != {"type", "snapshot_bytes_base64"} or response["type"] != "ready":
            raise A1SourceHolderWorkerError("Exact prepublication source snapshot event required")
        snapshot = _bytes(response["snapshot_bytes_base64"], "source snapshot", 48 * 1024 * 1024)
        self.snapshot_bytes = snapshot
        return snapshot

    def wait_genesis(self):
        response = receive_frame(self.channel, maximum=MAX_READY_BYTES)
        if (set(response) != {"type", "phase", "genesis_bytes_base64"}
                or response["type"] != "genesis" or response["phase"] != "a0"):
            raise A1SourceHolderWorkerError("Exact detached A/B genesis event required")
        raw = _bytes(response["genesis_bytes_base64"], "A/B genesis", 64 * 1024 * 1024)
        from .truss_detached_a1_genesis_codec import decode_detached_a1_genesis_snapshot
        decode_detached_a1_genesis_snapshot(raw)
        self.genesis_bytes = raw
        self.phase = "a0"
        return raw

    def open_prepublication(self, *, request_bytes: bytes, destination_id: str,
                            publication_id: str):
        if self.genesis_bytes is None or self.snapshot_bytes is not None:
            raise A1SourceHolderWorkerError("A/B genesis and one prepublication open required")
        response = self._rpc({"type": "open",
            "request_bytes_base64": base64.b64encode(request_bytes).decode("ascii"),
            "destination_id": destination_id, "publication_id": publication_id},
            response_type="ready", maximum=MAX_COMMAND_BYTES)
        if set(response) != {"type", "snapshot_bytes_base64", "custody_digests"}:
            raise A1SourceHolderWorkerError("Closed prepublication source snapshot event required")
        snapshot = _bytes(response["snapshot_bytes_base64"], "source snapshot", 4 * 1024 * 1024)
        from .truss_detached_source_snapshot_codec import decode_detached_source_snapshot
        from .truss053_source_custody import detached_source_custody_digests
        expected = detached_source_custody_digests(decode_detached_source_snapshot(snapshot))
        if response["custody_digests"] != expected:
            raise A1SourceHolderWorkerError("Child custody digest map differs from exact detached A page")
        self.snapshot_bytes = snapshot
        self.custody_digests = expected
        return snapshot

    def renew(self):
        self._ensure_healthy()
        response = self._rpc({"type": "renew"}, response_type="renewed", maximum=4096)
        if response.get("custody_digests") != self.custody_digests:
            raise A1SourceHolderWorkerError("Live Truss renewal differs from original A1 source binding")

    def start_heartbeat(self):
        if self.snapshot_bytes is None or self._heartbeat is not None:
            raise A1SourceHolderWorkerError("Ready original source holder and one heartbeat required")
        def pulse():
            while not self._heartbeat_stop.wait(self.renewal_interval_seconds):
                try:
                    self.renew()
                except BaseException as error:
                    self._heartbeat_error = error
                    self._heartbeat_stop.set()
        self._heartbeat = threading.Thread(target=pulse,
            name="ashlar-a1-source-renewal", daemon=True)
        self._heartbeat.start()

    def _ensure_healthy(self):
        if self._heartbeat_error is not None:
            raise A1SourceHolderWorkerError("Source-holder heartbeat failed") from self._heartbeat_error
        if self.process.poll() is not None:
            raise A1SourceHolderWorkerError("Original source-holder worker exited")

    def bind_publication(self, *, manifest_bytes: bytes, vector_payload: dict,
                         receipt_bytes: bytes):
        self._ensure_healthy()
        self._rpc({"type": "bind",
            "manifest_bytes_base64": base64.b64encode(manifest_bytes).decode("ascii"),
            "vector": vector_payload,
            "receipt_bytes_base64": base64.b64encode(receipt_bytes).decode("ascii")})

    def acknowledge_once(self, store, *, attempt_directory):
        """Fsync both worker and dispatch markers immediately before one ACK RPC."""
        from .a1_ack_attempt import DurableA1AckAttemptStore
        if type(store) is not DurableA1AckAttemptStore or self.snapshot_bytes is None:
            raise A1SourceHolderWorkerError("Prepared exact attempt and prepublication child required")
        self._ensure_healthy()
        attempt = store.load()
        if attempt.selection_bytes != self.snapshot_bytes:
            raise A1SourceHolderWorkerError("Prepared attempt differs from held source snapshot")
        self._heartbeat_stop.set()
        if self._heartbeat is not None:
            self._heartbeat.join(self.operation_timeout_seconds)
            if self._heartbeat.is_alive():
                raise A1SourceHolderWorkerError("Source heartbeat did not quiesce for one ACK command")
        self._ensure_healthy()
        self._generation = store.record_worker_started(
            process_group_id=self.process_group_id, selection_bytes=self.snapshot_bytes)
        store.mark_ack_dispatched(process_group_id=self.process_group_id,
                                  generation=self._generation)
        try:
            result = self._rpc({"type": "ack",
                "attempt_directory": str(Path(attempt_directory)),
                "generation": self._generation}, response_type="settlement",
                maximum=MAX_COMMAND_BYTES)
        except BaseException as error:
            self.terminate()
            store.mark_worker_lost(process_group_id=self.process_group_id,
                generation=self._generation, reason="ack_ipc_unknown")
            raise A1SourceHolderWorkerError(
                "ACK command may have been sent; use fresh read-only reconciliation") from error
        from .a1_ack_attempt import A1AckSettlement
        try:
            checkpoint = _bytes(result["checkpoint_bytes_base64"], "checkpoint", 1024 * 1024)
            receipt_text = result["receipt_bytes_base64"]
            receipt = None if receipt_text is None else _bytes(
                receipt_text, "receipt", 1024 * 1024)
            settlement = A1AckSettlement(result["outcome"], checkpoint, receipt,
                result["request_sha256"], result["manifest_sha256"],
                result["selection_sha256"])
            store.record_settlement(settlement)
        except (KeyError, TypeError, ValueError) as error:
            self.terminate()
            store.mark_worker_lost(process_group_id=self.process_group_id,
                generation=self._generation, reason="bad_settlement")
            raise A1SourceHolderWorkerError(
                "Unknown ACK result; use fresh read-only reconciliation") from error
        if settlement.outcome == "matched":
            self.phase = "a0_matched" if self.phase == "a0" else "a1_matched"
            self._heartbeat = None
            self._heartbeat_stop = threading.Event()
            # Keep the exact original source interval renewed while the
            # controller performs its fresh post-ACK Unity Catalog close read.
            self.start_heartbeat()
        else:
            self._reap_success()
        return settlement

    def finish_ack(self, settlement, *, attempt_directory, deadline=None):
        """Close the held source only after caller's post-ACK SQL close observation."""
        from .a1_ack_attempt import A1AckSettlement
        expected_phase = "a0_matched" if self.phase == "a0_matched" else "a1_matched"
        if (type(settlement) is not A1AckSettlement or settlement.outcome != "matched"
                or self.phase != expected_phase or self.snapshot_bytes is None
                or self._generation is None):
            raise A1SourceHolderWorkerError("Matched live ACK phase required before source close")
        self._ensure_healthy()
        self._heartbeat_stop.set()
        if self._heartbeat is not None:
            self._heartbeat.join(self.operation_timeout_seconds)
            if self._heartbeat.is_alive():
                raise A1SourceHolderWorkerError("Source heartbeat did not quiesce for source close")
        previous_timeout = self.channel.gettimeout()
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise A1SourceHolderWorkerError("Trial deadline expired before source close")
            self.channel.settimeout(min(self.operation_timeout_seconds, remaining))
        payload = {"outcome": settlement.outcome,
            "checkpoint_bytes_base64": base64.b64encode(settlement.checkpoint_bytes).decode("ascii"),
            "receipt_bytes_base64": None if settlement.receipt_bytes is None else
                base64.b64encode(settlement.receipt_bytes).decode("ascii"),
            "request_sha256": settlement.request_sha256,
            "manifest_sha256": settlement.manifest_sha256,
            "selection_sha256": settlement.selection_sha256}
        try:
            self._rpc({"type": "finish_ack", "attempt_directory": str(Path(attempt_directory)),
                "generation": self._generation, "settlement": payload},
                response_type="closed_ack", maximum=MAX_COMMAND_BYTES)
        finally:
            if not self._closed:
                self.channel.settimeout(previous_timeout)
        self.snapshot_bytes = None
        self.custody_digests = None
        if expected_phase == "a0_matched":
            self.phase = "a0_closed"
            # The matched settlement heartbeat is joined above. A1 is a new
            # source interval on this same child, so give it a fresh event and
            # permit exactly one new heartbeat.
            self._heartbeat = None
            self._heartbeat_stop = threading.Event()
        else:
            self._reap_success()
        return True

    def advance_after_a0_ack(self, settlement, deadline=None):
        """Keep the same source owner and apply A1 only after matched A0 ACK."""
        from .a1_ack_attempt import A1AckSettlement
        if (type(settlement) is not A1AckSettlement or settlement.outcome != "matched"
                or self.phase != "a0_closed" or self.snapshot_bytes is not None):
            raise A1SourceHolderWorkerError("Exact closed matched A0 settlement required")
        previous_timeout = self.channel.gettimeout()
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise A1SourceHolderWorkerError("Trial deadline expired before A1 source transition")
            self.channel.settimeout(min(self.operation_timeout_seconds, remaining))
        try:
            response = self._rpc({"type": "advance_a1",
                "outcome": settlement.outcome,
                "checkpoint_bytes_base64": base64.b64encode(
                    settlement.checkpoint_bytes).decode("ascii"),
                "receipt_bytes_base64": None if settlement.receipt_bytes is None else
                    base64.b64encode(settlement.receipt_bytes).decode("ascii"),
                "request_sha256": settlement.request_sha256,
                "manifest_sha256": settlement.manifest_sha256,
                "selection_sha256": settlement.selection_sha256},
                response_type="a1_genesis", maximum=MAX_COMMAND_BYTES)
        finally:
            if not self._closed:
                self.channel.settimeout(previous_timeout)
        if (set(response) != {"type", "phase", "genesis_bytes_base64"}
                or response["phase"] != "a1"):
            raise A1SourceHolderWorkerError("Exact A1 genesis event required")
        raw = _bytes(response["genesis_bytes_base64"], "A1 genesis", 64 * 1024 * 1024)
        from .truss_detached_a1_genesis_codec import decode_detached_a1_genesis_snapshot
        cursor = json.loads(settlement.checkpoint_bytes.decode("utf-8"))
        decode_detached_a1_genesis_snapshot(raw,
            expected_a_prior_cursor=cursor.get("nextXid"))
        self.phase = "a1"
        self.genesis_bytes = raw
        return raw

    def close_before_ack(self):
        if self._closed:
            return
        self._heartbeat_stop.set()
        if self._heartbeat is not None:
            self._heartbeat.join(self.operation_timeout_seconds)
        if self.process.poll() is None:
            self._rpc({"type": "close"}, response_type="closed", maximum=1024)
        self._reap_success()

    def terminate(self):
        """Kill and reap the entire holder process group within the hard bound."""
        if self._closed:
            return
        self._heartbeat_stop.set()
        try:
            os.killpg(self.process_group_id, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            self.process.wait(timeout=min(2, self.operation_timeout_seconds))
        except subprocess.TimeoutExpired:
            try:
                os.killpg(self.process_group_id, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.process.wait(timeout=self.operation_timeout_seconds)
        self.channel.close()
        self._closed = True

    def _reap_success(self):
        try:
            code = self.process.wait(timeout=self.operation_timeout_seconds)
            if code != 0:
                raise A1SourceHolderWorkerError("Source-holder child exited unsuccessfully")
        except subprocess.TimeoutExpired as error:
            self.terminate()
            raise A1SourceHolderWorkerError("Source-holder child exceeded its hard operation deadline") from error
        finally:
            if not self._closed:
                self.channel.close()
                self._closed = True


if __name__ == "__main__":
    main()
