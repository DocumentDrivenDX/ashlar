# SPDX-License-Identifier: MIT OR Apache-2.0
"""Effect-free byte preflight for the proposed Truss 0.0.49 B source.

This module verifies source and origin custody only. It has no installation,
acceptance, PostgreSQL, feed, or execution functionality. The reviewed Truss
0.0.49 LocalPostgres API hardcodes port 5432; any future B installation needs
exclusive admission of that shared port after Arras/A is stopped.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

PROFILE = "ashlar-r04-b-source-byte-preflight/0.1"
SOURCE_SHA256 = "498a82f313fd3ceaeb6195e133b403aa829acfcda7cec676a33a5790a57d456b"
ORIGIN_SHA256 = "2e722d0a3c3124d2df2ff4434968de412ef6ad767d4edfd889e160f85a369247"
SOURCE_DOCUMENT_ID = "ashlar-current-045-lifecycle-20261021"
MAX_SOURCE_BYTES = 1_048_576
MAX_ORIGIN_BYTES = 65_536


def _read_exact(path: Path, maximum: int) -> bytes:
    if not path.is_absolute() or path.resolve() != path or path.is_symlink():
        raise ValueError("Canonical absolute non-symlink input path required")
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError("Selected original input exceeds its byte bound")
    return raw


def preflight(source_path: str | Path, origin_path: str | Path) -> dict:
    """Verify byte custody and return an inert report; never prepare or connect."""
    source_path, origin_path = Path(source_path), Path(origin_path)
    if source_path == origin_path:
        raise ValueError("Source and origin custody must be distinct")
    source = _read_exact(source_path, MAX_SOURCE_BYTES)
    origin = _read_exact(origin_path, MAX_ORIGIN_BYTES)
    source_hash = hashlib.sha256(source).hexdigest()
    origin_hash = hashlib.sha256(origin).hexdigest()
    if source_hash != SOURCE_SHA256:
        raise ValueError("Authentic retained source digest differs; stop without substitution")
    if origin_hash != ORIGIN_SHA256:
        raise ValueError("Exact B origin candidate digest differs; stop without regeneration")
    try:
        document = json.loads(source)
        origin_value = json.loads(origin)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Original source and origin must be valid retained JSON bytes") from error
    if (type(document) is not dict or document.get("id") != SOURCE_DOCUMENT_ID
            or document.get("umf") != "0.10.0"):
        raise ValueError("Original UMF source identity/version differs from the B packet")
    if type(origin_value) is not dict or origin_value != {
        "actor": "ashlar-r04-b-source-acceptance",
        "reason": "independent R04 B source acceptance using the retained original UMF 0.10.0 model bytes",
    }:
        raise ValueError("B-specific origin candidate differs from the reviewed packet")
    return {
        "profile": PROFILE,
        "status": "source_and_origin_bytes_verified_preflight_only",
        "source": {"path": str(source_path), "bytes": len(source), "sha256": source_hash,
                   "document_id": SOURCE_DOCUMENT_ID, "umf": "0.10.0"},
        "origin": {"path": str(origin_path), "bytes": len(origin), "sha256": origin_hash,
                   "approval": "unapproved_candidate"},
        "native_effects": 0,
        "execution_approval": None,
        "pgdata_path": None,
        "port": None,
        "port_admission": "exclusive shared-port serialization required; Truss fixes TCP port 5432",
        "stop_reason": "No execution approval or B-owned path/role/resource pins are present",
        "next_gates": ["exclusive 5432 admission after Arras/A stop", "human execution approval",
                       "B-owned PGDATA/root/roles/resource limits",
                       "fresh exact package/runtime pins"],
    }
