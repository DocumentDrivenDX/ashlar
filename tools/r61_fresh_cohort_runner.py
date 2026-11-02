#!/usr/bin/env python3
"""Effect-free structural preflight for the pending R61 local cohort packet."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys


REPO = Path(__file__).resolve().parents[1]
SCOPE = REPO / "docs/helix/04-build/evidence/current-upstream-release-20261102/r61-fresh-truss053-umf102/scope.json"
PENDING = (
    "umf_sdk_artifact",
    "umf_sdk_sha256",
    "umf_core_platform_artifact",
    "umf_core_sha256",
    "truss_adapter_profile",
    "bun_executable",
    "postgresql_18_runtime",
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
    base = scope["source"]["base_commit"]
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", base, "HEAD"], cwd=REPO,
        capture_output=True,
    )
    if ancestor.returncode != 0:
        raise RuntimeError(f"pinned source base is not an ancestor of HEAD: {head}")
    dirty = subprocess.run(
        ["git", "status", "--porcelain"], cwd=REPO, check=True,
        capture_output=True, text=True,
    ).stdout
    if dirty:
        raise RuntimeError("source worktree is not clean")
    runner_path = REPO / scope["source"]["runner_path"]
    runner_sha = hashlib.sha256(runner_path.read_bytes()).hexdigest()
    if runner_sha != scope["source"]["runner_sha256"]:
        raise RuntimeError("runner source hash differs from scope")
    stage_path = REPO / scope["source"]["setup_stage_path"]
    stage_sha = hashlib.sha256(stage_path.read_bytes()).hexdigest()
    if stage_sha != scope["source"]["setup_stage_sha256"]:
        raise RuntimeError("fresh Truss setup stage source hash differs from scope")

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
    for path_key, hash_key in (
        ("truss_wheel", "truss_wheel_sha256"),
        ("umf_sdk_artifact", "umf_sdk_sha256"),
        ("umf_core_platform_artifact", "umf_core_sha256"),
        ("ashlar_candidate_wheel", "ashlar_candidate_wheel_sha256"),
    ):
        selected = artifacts.get(path_key)
        expected = artifacts.get(hash_key)
        if selected:
            artifact_path = Path(selected)
            if not artifact_path.is_absolute() or not artifact_path.is_file():
                raise RuntimeError(f"selected artifact is not a local regular file: {path_key}")
            observed = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
            if observed != expected:
                raise RuntimeError(f"selected artifact hash mismatch: {path_key}")
    source_model = REPO / scope["qualification_stage"]["source_model"]
    model_sha = hashlib.sha256(source_model.read_bytes()).hexdigest()
    if model_sha != scope["qualification_stage"]["source_model_sha256"]:
        raise RuntimeError("accepted UMF source model hash differs from scope")
    review_status = artifacts.get("upstream_candidate_review", "unbound")
    ready_to_review = not missing and review_status == "accepted"
    return {
        "status": ("blocked_pending_candidate" if missing else
                   "blocked_pending_independent_review" if not ready_to_review else
                   "bindings_present_review_required"),
        "effect_free": True,
        "native_dispatch_available": False,
        "source_head": head,
        "scope_path": str(SCOPE),
        "fresh_paths_absent": True,
        "r59_state_disjoint": True,
        "planned_bounds_enforced": False,
        "setup_stage_source_sha256": stage_sha,
        "upstream_candidate_commits": artifacts.get("upstream_candidate_commits"),
        "upstream_candidate_review": review_status,
        "source_model_sha256": model_sha,
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
