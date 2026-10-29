#!/usr/bin/env python3
"""Effect-free pre-run guard for the proposed R16 seed origin map."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

PROPOSED_ORIGIN_SHA256 = "c19540a4c1cc3561365cc4ec38a71da44fdc4648b8f27a6540c81eb5233badc5"
PROPOSED_ORIGIN_MAP = {
    "authority_scope": "local_truss_acceptance_trial",
    "profile": "ashlar-r16-seed-origin/0.1",
    "purpose": "Truss 0.0.51 local lifecycle qualification; not production source authority",
}
MAX_ORIGIN_BYTES = 65536


def verify_origin_map(path: str | Path) -> dict[str, object]:
    candidate = Path(path)
    if not candidate.is_absolute() or candidate.resolve() != candidate:
        raise ValueError("Canonical absolute seed origin-map path required")
    if candidate.is_symlink() or not candidate.is_file():
        raise ValueError("Required seed origin-map file is absent or not a regular file")
    if any(parent.is_symlink() for parent in candidate.parents):
        raise ValueError("Symlink in seed origin-map path")
    size = candidate.stat().st_size
    if size > MAX_ORIGIN_BYTES:
        raise ValueError("Seed origin map exceeds the 64 KiB bound")
    raw = candidate.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if digest != PROPOSED_ORIGIN_SHA256:
        raise ValueError("Seed origin-map SHA-256 differs from the proposed R16 semantic freeze")
    if json.loads(raw) != PROPOSED_ORIGIN_MAP:
        raise ValueError("Seed origin-map semantics differ from the proposed R16 local profile")
    return {"path": str(candidate), "bytes": size, "sha256": digest,
            "profile": PROPOSED_ORIGIN_MAP["profile"], "verified": True,
            "review_required": True, "execution_authorized": False}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin-map", required=True, type=Path)
    args = parser.parse_args()
    try:
        result = verify_origin_map(args.origin_map)
    except (OSError, ValueError) as exc:
        print(json.dumps({"profile": "ashlar-r16-origin-pre-run-gate/0.1",
                          "status": "blocked", "reason": str(exc),
                          "native_effects": False}, sort_keys=True))
        return 2
    print(json.dumps({"profile": "ashlar-r16-origin-pre-run-gate/0.1",
                      "status": "candidate_verified_review_required", "origin_map": result,
                      "native_effects": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
