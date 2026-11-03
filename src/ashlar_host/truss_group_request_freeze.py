# SPDX-License-Identifier: MIT OR Apache-2.0
"""Durable pre-effect binding for an original public Truss group request."""
from __future__ import annotations

import base64
from dataclasses import fields, is_dataclass
from enum import Enum
import hashlib
import json
import os
from pathlib import Path
import secrets


PROFILE = "ashlar-truss-group-request-freeze/0.1"
_SHA = set("0123456789abcdef")


class TrussGroupRequestFreezeError(ValueError):
    """A pre-effect Truss group request was not bound or retained exactly."""


def _wire(value):
    if is_dataclass(value) and not isinstance(value, type):
        return {item.name: _wire(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, Enum):
        return value.value
    if type(value) is bytes:
        return {"bytes_base64": base64.b64encode(value).decode("ascii"),
                "sha256": hashlib.sha256(value).hexdigest()}
    if type(value) in (tuple, list):
        return [_wire(item) for item in value]
    if type(value) is dict:
        if any(type(key) is not str for key in value):
            raise TrussGroupRequestFreezeError("String-keyed Truss request carrier required")
        return {key: _wire(item) for key, item in value.items()}
    if value is None or type(value) in (str, bool, int, float):
        if type(value) is float and not __import__("math").isfinite(value):
            raise TrussGroupRequestFreezeError("Finite Truss request number required")
        return value
    raise TrussGroupRequestFreezeError(
        f"Unsupported public Truss request carrier: {type(value).__name__}")


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def freeze_public_truss_group_request(path, *, original_invocation_bytes: bytes,
        schedule_sha256: str, source: str, slot: str, semantic, request,
        documents) -> dict:
    """Fsync the exact invocation and public group carriers before effect.

    The caller must invoke this after public ``GroupContext.input/request``
    construction and immediately before the same ``apply_in_transaction``.
    There is deliberately no read/replace or replay path for an existing file.
    """
    path = Path(path)
    if (not path.is_absolute() or path.is_symlink() or path.exists()
            or path.resolve() != path or not path.parent.is_dir()
            or type(original_invocation_bytes) is not bytes
            or not 0 < len(original_invocation_bytes) <= 4 * 1024 * 1024
            or type(schedule_sha256) is not str or len(schedule_sha256) != 64
            or any(char not in _SHA for char in schedule_sha256)
            or source not in ("A", "B") or slot not in
               ("A1", "B1", "A2", "B2", "A3", "B3", "A4", "B4")
            or slot[0] != source):
        raise TrussGroupRequestFreezeError(
            "Fresh exact R04 slot path, source, schedule and invocation required")
    parent = path.parent.stat()
    if parent.st_uid != os.getuid() or parent.st_mode & 0o077:
        raise TrussGroupRequestFreezeError("Owner-private freeze directory required")
    semantic_bytes = _canonical(_wire(semantic))
    request_bytes = _canonical(_wire(request))
    document_bytes = _canonical(_wire(documents))
    body = {
        "profile": PROFILE,
        "schedule_sha256": schedule_sha256,
        "source": source,
        "slot": slot,
        "invocation_bytes_base64": base64.b64encode(
            original_invocation_bytes).decode("ascii"),
        "invocation_sha256": hashlib.sha256(original_invocation_bytes).hexdigest(),
        "semantic_bytes_base64": base64.b64encode(semantic_bytes).decode("ascii"),
        "semantic_sha256": hashlib.sha256(semantic_bytes).hexdigest(),
        "request_bytes_base64": base64.b64encode(request_bytes).decode("ascii"),
        "request_sha256": hashlib.sha256(request_bytes).hexdigest(),
        "documents_bytes_base64": base64.b64encode(document_bytes).decode("ascii"),
        "documents_sha256": hashlib.sha256(document_bytes).hexdigest(),
    }
    raw = _canonical({**body, "binding_sha256": hashlib.sha256(_canonical(body)).hexdigest()})
    if len(raw) > 24 * 1024 * 1024:
        raise TrussGroupRequestFreezeError("Bounded durable Truss freeze record required")
    temporary = path.parent / f".{path.name}.{secrets.token_hex(16)}.tmp"
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        view = memoryview(raw)
        while view:
            count = os.write(fd, view)
            if count <= 0:
                raise TrussGroupRequestFreezeError("Freeze write made no progress")
            view = view[count:]
        os.fsync(fd)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise
    finally:
        os.close(fd)
    try:
        os.link(temporary, path, follow_symlinks=False)
        temporary.unlink()
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise
    return {"path": str(path), "binding_sha256": hashlib.sha256(raw).hexdigest(),
            "invocation_sha256": body["invocation_sha256"],
            "semantic_sha256": body["semantic_sha256"],
            "request_sha256": body["request_sha256"],
            "documents_sha256": body["documents_sha256"]}
