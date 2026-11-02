#!/usr/bin/env python3
"""Effect-free structural preflight for the pending R61 local cohort packet."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys


REPO = Path(__file__).resolve().parents[1]
SCOPE = REPO / "docs/helix/04-build/evidence/current-upstream-release-20261102/r61-fresh-truss053-umf102/scope.json"
PENDING = (
    "truss_wheel",
    "truss_wheel_sha256",
    "truss_release_manifest_sha256",
    "umf_sdk_artifact",
    "umf_sdk_sha256",
    "umf_core_platform_artifact",
    "umf_core_sha256",
    "ashlar_candidate_commit",
    "ashlar_candidate_wheel",
    "ashlar_candidate_wheel_sha256",
    "graphframes_runtime_manifest",
    "graphframes_runtime_manifest_sha256",
)


def preflight() -> dict:
    scope = json.loads(SCOPE.read_text())
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=REPO, check=True,
        capture_output=True, text=True,
    ).stdout.strip()
    if head != scope["source"]["base_commit"]:
        raise RuntimeError(f"source HEAD differs from bound base: {head}")

    paths = scope["fresh_local_state"]
    root = Path(paths["root"])
    if any(Path(paths[key]).resolve().is_relative_to(Path(forbidden).resolve())
           or Path(forbidden).resolve().is_relative_to(Path(paths[key]).resolve())
           for key in ("root", "postgres_data", "delta_root", "evidence_root")
           for forbidden in paths["forbidden_state"]):
        raise RuntimeError("fresh output path overlaps a forbidden retained state")
    existing = [paths[key] for key in ("root", "postgres_data", "delta_root", "evidence_root")
                if Path(paths[key]).exists()]
    if existing:
        raise RuntimeError(f"fresh output paths already exist: {existing}")

    artifacts = scope["artifacts"]
    missing = [key for key in PENDING if not artifacts.get(key)]
    return {
        "status": "blocked_pending_candidate" if missing else "bindings_present_review_required",
        "effect_free": True,
        "native_dispatch_available": False,
        "source_head": head,
        "scope_path": str(SCOPE),
        "fresh_paths_absent": True,
        "r59_state_disjoint": True,
        "candidate_bindings_missing": missing,
        "limits": scope["limits"],
        "planned_workflow_steps": len(scope["workflow"]),
        "note": "This skeleton has no native execution path; candidate binding and independent review remain mandatory.",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preflight-only", action="store_true", required=True)
    args = parser.parse_args()
    receipt = preflight()
    print(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"status": "refused", "effect_free": True, "reason": str(exc)},
                         sort_keys=True, separators=(",", ":")), file=sys.stderr)
        raise SystemExit(2)
