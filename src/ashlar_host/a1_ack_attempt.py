# SPDX-License-Identifier: MIT OR Apache-2.0
"""Durable one-use A1 ACK attempt and read-only recovery record.

This module owns no Truss, database, UC, or verifier authority. It persists
exact inputs before dispatch and only accepts exact settlement observations.
The process supervisor is a separate layer.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import secrets
import stat


PROFILE = "ashlar-a1-protected-ack-attempt/0.1"
MAX_REQUEST_BYTES = 4 * 1024 * 1024
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_RECEIPT_BYTES = 1024 * 1024
MAX_CHECKPOINT_BYTES = 1024 * 1024
MAX_SETTLEMENT_RECORD_BYTES = (
    4 * ((MAX_RECEIPT_BYTES + 2) // 3)
    + 4 * ((MAX_CHECKPOINT_BYTES + 2) // 3) + 8192
)
MAX_SELECTION_BYTES = 4 * 1024 * 1024
MAX_ATTEMPT_RECORD_BYTES = 24 * 1024 * 1024
MAX_WORKER_LINE = 65536
MAX_WORKER_STDERR = 65536
MAX_PRE_DISPATCH_WORKERS = 8


class A1AckAttemptError(RuntimeError):
    """The durable attempt, worker, or read-only recovery was incomplete."""


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _json(raw: bytes) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON member")
            result[key] = value
        return result
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (UnicodeError, ValueError, json.JSONDecodeError) as error:
        raise A1AckAttemptError("Closed exact worker JSON required") from error
    if type(value) is not dict:
        raise A1AckAttemptError("Worker JSON object required")
    return value


def _encoded(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _frame(values) -> bytes:
    result = bytearray()
    for value in values:
        if type(value) is not bytes:
            raise A1AckAttemptError("Exact byte fields required for attempt identity")
        result.extend(len(value).to_bytes(8, "big"))
        result.extend(value)
    return bytes(result)


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def _unb64(value, label, maximum):
    if type(value) is not str or len(value) > 4 * ((maximum + 2) // 3):
        raise A1AckAttemptError(f"Bounded exact {label} bytes required")
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, TypeError) as error:
        raise A1AckAttemptError(f"Exact {label} base64 required") from error
    if len(raw) > maximum or _b64(raw) != value:
        raise A1AckAttemptError(f"Canonical bounded {label} bytes required")
    return raw


def _write_exclusive(path: Path, raw: bytes):
    path = Path(path)
    if (not path.is_absolute() or path.is_symlink() or path.exists()
            or not path.parent.is_dir() or len(raw) > MAX_ATTEMPT_RECORD_BYTES):
        raise A1AckAttemptError("Fresh bounded durable attempt path required")
    parent = path.parent.stat()
    if parent.st_uid != os.getuid() or parent.st_mode & 0o077:
        raise A1AckAttemptError("Owner-private attempt directory required")
    temporary = path.parent / f".{path.name}.{secrets.token_hex(16)}.tmp"
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        info = os.fstat(fd)
        if info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise A1AckAttemptError("Private attempt file ownership required")
        view = memoryview(raw)
        while view:
            count = os.write(fd, view)
            if count <= 0:
                raise A1AckAttemptError("Attempt write made no progress")
            view = view[count:]
        os.fsync(fd)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
    finally:
        os.close(fd)
    try:
        # Hard-link publication is atomic and refuses replacement. The final
        # name never points at a partially written transition record.
        os.link(temporary, path, follow_symlinks=False)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise
    temporary.unlink()
    dfd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)
    return _sha(raw)


def _read_private(path: Path, maximum: int) -> bytes:
    path = Path(path)
    if not path.exists():
        raise A1AckAttemptError("Required durable record is absent")
    if (not path.is_absolute() or path.is_symlink()
            or path.resolve(strict=True) != path
            or any(parent.is_symlink() for parent in path.parents)):
        raise A1AckAttemptError("Canonical durable attempt path required")
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o077 or not 0 <= info.st_size <= maximum):
            raise A1AckAttemptError("Owner-private bounded durable record required")
        result = bytearray()
        while len(result) <= info.st_size:
            chunk = os.read(fd, min(65536, info.st_size + 1 - len(result)))
            if not chunk:
                break
            result.extend(chunk)
        if len(result) != info.st_size:
            raise A1AckAttemptError("Durable record changed during read")
        return bytes(result)
    finally:
        os.close(fd)


@dataclass(frozen=True)
class A1AckAttempt:
    """Exact durable inputs for a single prepared ACK attempt."""
    path: Path
    attempt_id: str
    selection_bytes: bytes
    request_bytes: bytes
    manifest_bytes: bytes
    receipt_bytes: bytes
    prior_checkpoint_bytes: bytes
    next_checkpoint_bytes: bytes
    page_sha256: str
    destination_id: str
    publication_id: str

    @property
    def selection_sha256(self): return _sha(self.selection_bytes)
    @property
    def request_sha256(self): return _sha(self.request_bytes)
    @property
    def manifest_sha256(self): return _sha(self.manifest_bytes)
    @property
    def receipt_sha256(self): return _sha(self.receipt_bytes)


@dataclass(frozen=True)
class A1AckSettlement:
    outcome: str
    checkpoint_bytes: bytes
    receipt_bytes: bytes | None
    request_sha256: str
    manifest_sha256: str
    selection_sha256: str


class DurableA1AckAttemptStore:
    """Single-use private attempt directory; no overwrite or ACK retry path."""

    def __init__(self, directory: Path):
        self.directory = Path(directory)
        if (not self.directory.is_absolute() or self.directory.is_symlink()
                or not self.directory.is_dir()
                or self.directory.resolve(strict=True) != self.directory):
            raise A1AckAttemptError("Existing private absolute attempt directory required")
        info = self.directory.stat()
        if info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise A1AckAttemptError("Owner-private A1 attempt directory required")
        self.path = self.directory / "ack-attempt.json"

    def begin(self, *, selection_bytes: bytes, request_bytes: bytes,
              manifest_bytes: bytes, receipt_bytes: bytes,
              prior_checkpoint_bytes: bytes, next_checkpoint_bytes: bytes,
              page_sha256: str, destination_id: str, publication_id: str) -> A1AckAttempt:
        values = (selection_bytes, request_bytes, manifest_bytes, receipt_bytes,
                  prior_checkpoint_bytes, next_checkpoint_bytes)
        limits = (MAX_SELECTION_BYTES, MAX_REQUEST_BYTES, MAX_MANIFEST_BYTES,
                  MAX_RECEIPT_BYTES, MAX_CHECKPOINT_BYTES, MAX_CHECKPOINT_BYTES)
        if (any(type(raw) is not bytes or not raw or len(raw) > maximum
                for raw, maximum in zip(values, limits))
                or type(page_sha256) is not str or len(page_sha256) != 64
                or any(c not in "0123456789abcdef" for c in page_sha256)
                or type(destination_id) is not str or not destination_id
                or "\0" in destination_id or len(destination_id.encode()) > 1024
                or type(publication_id) is not str or not publication_id
                or "\0" in publication_id or len(publication_id.encode()) > 4096):
            raise A1AckAttemptError("Exact bounded A1 ACK inputs required")
        binding = _frame((selection_bytes, request_bytes, manifest_bytes,
                          receipt_bytes, prior_checkpoint_bytes,
                          next_checkpoint_bytes, page_sha256.encode(),
                          destination_id.encode(), publication_id.encode()))
        attempt_id = _sha(b"ashlar-a1-one-use-ack/0.1\0" + binding)
        attempt = A1AckAttempt(self.path, attempt_id, *values, page_sha256,
                               destination_id, publication_id)
        value = {
            "profile": PROFILE,
            "status": "prepared",
            "attempt_id": attempt_id,
            "selection_bytes_base64": _b64(selection_bytes),
            "selection_sha256": attempt.selection_sha256,
            "request_bytes_base64": _b64(request_bytes),
            "request_sha256": attempt.request_sha256,
            "manifest_bytes_base64": _b64(manifest_bytes),
            "manifest_sha256": attempt.manifest_sha256,
            "receipt_bytes_base64": _b64(receipt_bytes),
            "receipt_sha256": attempt.receipt_sha256,
            "prior_checkpoint_bytes_base64": _b64(prior_checkpoint_bytes),
            "next_checkpoint_bytes_base64": _b64(next_checkpoint_bytes),
            "page_sha256": page_sha256,
            "destination_id": destination_id,
            "publication_id": publication_id,
            "ack_retry_permitted": False,
        }
        _write_exclusive(self.path, _encoded(value) + b"\n")
        return attempt

    def load(self) -> A1AckAttempt:
        raw = _read_private(self.path, MAX_ATTEMPT_RECORD_BYTES)
        if not raw.endswith(b"\n") or raw.endswith(b"\n\n"):
            raise A1AckAttemptError("Single complete durable attempt record required")
        value = _json(raw[:-1])
        expected = {"profile", "status", "attempt_id", "selection_bytes_base64",
                    "selection_sha256", "request_bytes_base64", "request_sha256",
                    "manifest_bytes_base64", "manifest_sha256", "receipt_bytes_base64",
                    "receipt_sha256", "prior_checkpoint_bytes_base64",
                    "next_checkpoint_bytes_base64", "page_sha256", "destination_id",
                    "publication_id", "ack_retry_permitted"}
        if (set(value) != expected or value["profile"] != PROFILE
                or value["status"] not in ("prepared", "ack_command_may_have_been_sent")
                or value["ack_retry_permitted"] is not False):
            raise A1AckAttemptError("Exact prepared or dispatched attempt required")
        if (type(value["attempt_id"]) is not str
                or type(value["destination_id"]) is not str
                or type(value["publication_id"]) is not str
                or type(value["page_sha256"]) is not str):
            raise A1AckAttemptError("Exact durable attempt identity strings required")
        attempt = A1AckAttempt(
            self.path, value["attempt_id"],
            _unb64(value["selection_bytes_base64"], "selection", MAX_SELECTION_BYTES),
            _unb64(value["request_bytes_base64"], "request", MAX_REQUEST_BYTES),
            _unb64(value["manifest_bytes_base64"], "manifest", MAX_MANIFEST_BYTES),
            _unb64(value["receipt_bytes_base64"], "receipt", MAX_RECEIPT_BYTES),
            _unb64(value["prior_checkpoint_bytes_base64"], "prior checkpoint", MAX_CHECKPOINT_BYTES),
            _unb64(value["next_checkpoint_bytes_base64"], "next checkpoint", MAX_CHECKPOINT_BYTES),
            value["page_sha256"], value["destination_id"], value["publication_id"])
        expected_id = self._attempt_id(attempt)
        if (attempt.attempt_id != expected_id
                or any(value[key] != expected for key, expected in (
                    ("selection_sha256", attempt.selection_sha256),
                    ("request_sha256", attempt.request_sha256),
                    ("manifest_sha256", attempt.manifest_sha256),
                    ("receipt_sha256", attempt.receipt_sha256)))):
            raise A1AckAttemptError("Durable attempt byte binding differs")
        return attempt

    @staticmethod
    def _attempt_id(attempt):
        binding = _frame((attempt.selection_bytes, attempt.request_bytes,
            attempt.manifest_bytes, attempt.receipt_bytes,
            attempt.prior_checkpoint_bytes, attempt.next_checkpoint_bytes,
            attempt.page_sha256.encode(), attempt.destination_id.encode(),
            attempt.publication_id.encode()))
        return _sha(b"ashlar-a1-one-use-ack/0.1\0" + binding)

    def _read_record(self, name, maximum=4096):
        raw = _read_private(self.directory / name, maximum)
        if not raw.endswith(b"\n") or raw.endswith(b"\n\n"):
            raise A1AckAttemptError("Single complete durable worker record required")
        value = _json(raw[:-1])
        return value

    def record_settlement(self, settlement: A1AckSettlement, *, recovery=False):
        attempt = self.load()
        try:
            dispatch = self._read_record("ack-command-dispatched.json")
        except A1AckAttemptError as error:
            raise A1AckAttemptError("ACK dispatch marker required before settlement") from error
        if (dispatch.get("profile") != PROFILE
                or dispatch.get("attempt_id") != attempt.attempt_id
                or dispatch.get("status") != "ack_command_may_have_been_sent"
                or dispatch.get("ack_retry_permitted") is not False):
            raise A1AckAttemptError("ACK dispatch marker required before settlement")
        self._validate_settlement(attempt, settlement)
        name = "read-only-reconciliation.json" if recovery else "worker-settlement.json"
        value = {"profile": PROFILE,
            "status": "matched" if settlement.outcome == "matched" else "incomplete",
            "attempt_id": attempt.attempt_id, "outcome": settlement.outcome,
            "checkpoint_bytes_base64": _b64(settlement.checkpoint_bytes),
            "receipt_bytes_base64": None if settlement.receipt_bytes is None
                else _b64(settlement.receipt_bytes),
            "request_sha256": settlement.request_sha256,
            "manifest_sha256": settlement.manifest_sha256,
            "selection_sha256": settlement.selection_sha256,
            "ack_retry_permitted": False}
        _write_exclusive(self.directory / name, _encoded(value) + b"\n")
        return value["status"]

    def record_worker_started(self, *, process_group_id: int,
                              selection_bytes: bytes) -> int:
        """Bind exact worker selection before any source-open or ACK command."""
        if type(process_group_id) is not int or process_group_id <= 1:
            raise A1AckAttemptError("Original spawned worker process group required")
        attempt = self.load()
        if type(selection_bytes) is not bytes or selection_bytes != attempt.selection_bytes:
            raise A1AckAttemptError("Worker exact selection differs from prepared attempt")
        if os.name != "posix":
            raise A1AckAttemptError("POSIX source-worker process group required")
        try:
            os.killpg(process_group_id, 0)
        except (ProcessLookupError, PermissionError, TypeError, ValueError) as error:
            raise A1AckAttemptError("Live selected source-worker group required") from error
        if (self.directory / "ack-command-dispatched.json").exists():
            raise A1AckAttemptError("Dispatched ACK attempt cannot start another worker")
        generation = 1
        while (self.directory / f"worker-started-{generation:02d}.json").exists():
            lost = self._read_record(f"worker-lost-{generation:02d}.json")
            if (lost.get("attempt_id") != attempt.attempt_id
                    or lost.get("generation") != generation
                    or lost.get("status") != "worker_lost_before_ack_command"
                    or lost.get("ack_retry_permitted") is not True):
                raise A1AckAttemptError("Prior worker must be proven lost before ACK command")
            pgid = lost.get("process_group_id")
            try:
                os.killpg(pgid, 0)
            except ProcessLookupError:
                pass
            except (PermissionError, TypeError, ValueError) as error:
                raise A1AckAttemptError("Cannot prove prior worker group is absent") from error
            else:
                raise A1AckAttemptError("Prior worker process group remains live")
            generation += 1
        if generation > MAX_PRE_DISPATCH_WORKERS:
            raise A1AckAttemptError("Bounded pre-dispatch worker attempts exhausted")
        value = {"profile": PROFILE, "attempt_id": attempt.attempt_id,
                 "generation": generation,
                 "status": "worker_spawned_no_ack_command_yet",
                 "process_group_id": process_group_id,
                 "ack_retry_permitted": False}
        _write_exclusive(self.directory / f"worker-started-{generation:02d}.json",
                         _encoded(value) + b"\n")
        return generation

    def mark_ack_dispatched(self, *, process_group_id: int, generation: int):
        """Fsync the one-use marker immediately before writing the ACK command."""
        attempt = self.load()
        if self._read_record("ack-attempt.json", MAX_ATTEMPT_RECORD_BYTES).get(
                "status") != "prepared":
            raise A1AckAttemptError("ACK attempt is already dispatched")
        started = self._read_record(f"worker-started-{generation:02d}.json")
        if (started.get("attempt_id") != attempt.attempt_id
                or started.get("generation") != generation
                or started.get("process_group_id") != process_group_id
                or started.get("status") != "worker_spawned_no_ack_command_yet"):
            raise A1AckAttemptError("Exact active prepared worker required for ACK dispatch")
        latest = 0
        while (self.directory / f"worker-started-{latest + 1:02d}.json").exists():
            latest += 1
        if generation != latest:
            raise A1AckAttemptError("Only the latest live prepared worker may receive ACK")
        if (self.directory / f"worker-lost-{generation:02d}.json").exists():
            raise A1AckAttemptError("Lost pre-dispatch worker cannot receive ACK")
        if os.name != "posix":
            raise A1AckAttemptError("POSIX source-worker group liveness required")
        try:
            os.killpg(process_group_id, 0)
        except (ProcessLookupError, PermissionError, TypeError, ValueError) as error:
            raise A1AckAttemptError("Live original worker group required for ACK dispatch") from error
        value = {"profile": PROFILE, "attempt_id": attempt.attempt_id,
                 "generation": generation, "process_group_id": process_group_id,
                 "status": "ack_command_may_have_been_sent",
                 "ack_retry_permitted": False}
        _write_exclusive(self.directory / "ack-command-dispatched.json",
                         _encoded(value) + b"\n")

    @staticmethod
    def _validate_settlement(attempt, result):
        if (type(result) is not A1AckSettlement
                or result.outcome not in ("matched", "missing", "conflicting")
                or type(result.checkpoint_bytes) is not bytes
                or len(result.checkpoint_bytes) > MAX_CHECKPOINT_BYTES
                or result.request_sha256 != attempt.request_sha256
                or result.manifest_sha256 != attempt.manifest_sha256
                or result.selection_sha256 != attempt.selection_sha256):
            raise A1AckAttemptError("Exact original read-only reconciliation required")
        if result.outcome == "matched":
            if (result.checkpoint_bytes != attempt.next_checkpoint_bytes
                    or result.receipt_bytes != attempt.receipt_bytes):
                raise A1AckAttemptError("Matched ACK differs from exact original receipt/checkpoint")
        elif result.receipt_bytes is not None:
            raise A1AckAttemptError("Unmatched ACK must not claim retained receipt bytes")

    def mark_worker_lost(self, *, process_group_id: int, generation: int, reason: str):
        if (type(process_group_id) is not int or process_group_id <= 1
                or type(generation) is not int or not 1 <= generation <= MAX_PRE_DISPATCH_WORKERS
                or type(reason) is not str or not reason or len(reason) > 128):
            raise A1AckAttemptError("Bounded original worker-loss observation required")
        attempt = self.load()
        started = self._read_record(f"worker-started-{generation:02d}.json")
        if (started.get("profile") != PROFILE
                or started.get("attempt_id") != attempt.attempt_id
                or started.get("generation") != generation
                or started.get("status") != "worker_spawned_no_ack_command_yet"
                or started.get("process_group_id") != process_group_id
                or started.get("ack_retry_permitted") is not False):
            raise A1AckAttemptError("Worker loss must bind the recorded original group")
        dispatched = (self.directory / "ack-command-dispatched.json").exists()
        if dispatched:
            dispatch = self._read_record("ack-command-dispatched.json")
            if dispatch.get("generation") != generation:
                raise A1AckAttemptError("Worker loss must bind dispatched generation")
        if os.name != "posix":
            raise A1AckAttemptError("POSIX source-worker group loss observation required")
        try:
            os.killpg(process_group_id, 0)
        except ProcessLookupError:
            pass
        except (PermissionError, TypeError, ValueError) as error:
            raise A1AckAttemptError("Cannot prove source-worker group is absent") from error
        else:
            raise A1AckAttemptError("Source-worker process group remains live")
        value = {"profile": PROFILE, "attempt_id": attempt.attempt_id,
                 "generation": generation,
                 "status": ("worker_lost_ack_may_have_committed" if dispatched
                            else "worker_lost_before_ack_command"),
                 "process_group_id": process_group_id, "reason": reason,
                 "ack_retry_permitted": not dispatched}
        _write_exclusive(self.directory / f"worker-lost-{generation:02d}.json",
                         _encoded(value) + b"\n")


def recover_lost_a1_ack(store: DurableA1AckAttemptStore, read_only_reconciler,
                        ) -> A1AckSettlement:
    """Settle only an original lost worker through a fresh read-only owner."""
    attempt = store.load()
    dispatch = store._read_record("ack-command-dispatched.json")
    if (dispatch.get("profile") != PROFILE or dispatch.get("attempt_id") != attempt.attempt_id
            or dispatch.get("status") != "ack_command_may_have_been_sent"
            or dispatch.get("ack_retry_permitted") is not False):
        raise A1AckAttemptError("Durable ACK dispatch marker required for recovery")
    generation = dispatch.get("generation")
    lost = store._read_record(f"worker-lost-{generation:02d}.json")
    started = store._read_record(f"worker-started-{generation:02d}.json")
    if (started.get("profile") != PROFILE or started.get("attempt_id") != attempt.attempt_id
            or started.get("generation") != generation
            or started.get("status") != "worker_spawned_no_ack_command_yet"
            or started.get("ack_retry_permitted") is not False):
        raise A1AckAttemptError("Original worker start must be durable")
    if (lost.get("profile") != PROFILE or lost.get("attempt_id") != attempt.attempt_id
            or lost.get("generation") != generation
            or lost.get("status") != "worker_lost_ack_may_have_committed"
            or lost.get("ack_retry_permitted") is not False
            or type(lost.get("process_group_id")) is not int
            or lost["process_group_id"] <= 1
            or lost["process_group_id"] != started.get("process_group_id")):
        raise A1AckAttemptError("Reaped original worker and exact lost-attempt record required")
    if os.name != "posix":
        raise A1AckAttemptError("POSIX source-worker process-group recovery required")
    try:
        os.killpg(started["process_group_id"], 0)
    except ProcessLookupError:
        pass
    except (PermissionError, TypeError, ValueError) as error:
        raise A1AckAttemptError("Cannot prove original source-worker group is absent") from error
    else:
        raise A1AckAttemptError("Original source-worker process group remains live")
    # A reconciler must be the public fresh read-only Truss owner. The callback
    # receives opaque retained bytes and cannot obtain an ACK command here.
    reconcile = getattr(read_only_reconciler, "reconcile", None)
    if not callable(reconcile):
        raise A1AckAttemptError("Fresh public read-only ACK reconciliation owner required")
    result = reconcile(attempt)
    store.record_settlement(result, recovery=True)
    return result
