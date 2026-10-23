"""Bounded correspondence checks for original public Truss cursor bytes.

This module preserves the selected bytes and unknown cursor extensions. It does
not authenticate a registration, construct Ashlar progress, or provide source or
ACK authority.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping


MAX_CURSOR_BYTES = 16_384
_PROFILE = "truss-caller-asserted-feed/0.1"
_IDENTITY_FIELDS = (
    ("installationId", "installation_id"),
    ("sourceEpoch", "epoch"),
    ("consumerId", "consumer_id"),
    ("registrationId", "registration_id"),
)


class TrussCursorCorrespondenceError(ValueError):
    """The exact cursor bytes do not correspond to the external selection."""


@dataclass(frozen=True)
class TrussCursorCorrespondence:
    original: bytes
    sha256: str
    profile: str
    identity: Mapping[str, str]
    next_xid: str
    extensions: Mapping[str, object]


def _object_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise TrussCursorCorrespondenceError("Duplicate cursor member")
        result[key] = value
    return result


def _reject_constant(value):
    raise TrussCursorCorrespondenceError("Non-JSON numeric constant refused")


def validate_selected_truss_cursor(
    cursor_bytes: bytes,
    *,
    cursor_sha256: str,
    selected_identity: Mapping[str, object],
) -> TrussCursorCorrespondence:
    """Validate exact public cursor syntax and selected identity correspondence.

    `selected_identity` must provide `installation_id`, `epoch`, `consumer_id`,
    and `registration_id`. Other selected fields, including feed and ACK custody,
    are intentionally not inferred from cursor bytes.
    """
    if type(cursor_bytes) is not bytes or not 1 <= len(cursor_bytes) <= MAX_CURSOR_BYTES:
        raise TrussCursorCorrespondenceError("Bounded original cursor bytes required")
    if type(cursor_sha256) is not str or not re.fullmatch(r"[0-9a-f]{64}", cursor_sha256, re.ASCII):
        raise TrussCursorCorrespondenceError("Selected lowercase cursor SHA-256 required")
    actual_digest = hashlib.sha256(cursor_bytes).hexdigest()
    if actual_digest != cursor_sha256:
        raise TrussCursorCorrespondenceError("Original cursor digest differs from selection")
    if not isinstance(selected_identity, Mapping):
        raise TrussCursorCorrespondenceError("Separately selected typed identity required")

    try:
        text = cursor_bytes.decode("utf-8", errors="strict")
        cursor = json.loads(
            text,
            object_pairs_hook=_object_pairs,
            parse_constant=_reject_constant,
        )
    except TrussCursorCorrespondenceError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise TrussCursorCorrespondenceError("Valid bounded UTF-8 cursor JSON required") from exc
    if type(cursor) is not dict or cursor.get("profile") != _PROFILE:
        raise TrussCursorCorrespondenceError("Public Truss cursor profile required")
    if type(cursor.get("nextXid")) is not str:
        raise TrussCursorCorrespondenceError("Canonical unsigned cursor position required")
    next_xid = cursor["nextXid"]
    if (len(next_xid) > 20 or not next_xid.isascii()
            or not re.fullmatch(r"(?:0|[1-9][0-9]*)", next_xid, re.ASCII)
            or int(next_xid) >= 2**64):
        raise TrussCursorCorrespondenceError("Canonical unsigned 64-bit cursor position required")

    matched = {}
    for cursor_key, selected_key in _IDENTITY_FIELDS:
        value = cursor.get(cursor_key)
        expected = selected_identity.get(selected_key)
        if (type(value) is not str or not value or "\0" in value
                or type(expected) is not str or not expected or "\0" in expected):
            raise TrussCursorCorrespondenceError("Required typed selected cursor identity required")
        if value != expected:
            raise TrussCursorCorrespondenceError("Cursor identity differs from external selection")
        matched[selected_key] = value

    extensions = {key: value for key, value in cursor.items()
                  if key not in {"profile", "nextXid", *(k for k, _ in _IDENTITY_FIELDS)}}
    return TrussCursorCorrespondence(
        original=cursor_bytes,
        sha256=actual_digest,
        profile=_PROFILE,
        identity=MappingProxyType(matched),
        next_xid=next_xid,
        extensions=MappingProxyType(extensions),
    )
