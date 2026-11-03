#!/usr/bin/env python3
"""Effect-free structural preflight for the pending R61 local cohort packet."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import stat
import sys
import zipfile


REPO = Path(__file__).resolve().parents[1]
SCOPE = REPO / "docs/helix/04-build/evidence/current-upstream-release-20261102/r61-fresh-truss055-attempt13-continuation/scope.json"
PENDING = (
    "umf_sdk_artifact",
    "umf_sdk_sha256",
    "umf_core_platform_artifact",
    "umf_core_sha256",
    "truss_adapter_profile",
    "bun_executable",
    "bun_sha256",
    "postgresql_18_runtime",
    "postgresql_18_runtime_manifest",
    "postgresql_18_runtime_manifest_sha256",
    "ashlar_candidate_wheel",
    "ashlar_candidate_wheel_sha256",
    "graphframes_runtime_manifest",
    "graphframes_runtime_manifest_sha256",
    "python_executable",
    "python_sha256",
    "runtime_manifest",
    "runtime_manifest_sha256",
    "worker_path",
    "worker_sha256",
    "upstream_candidate_review",
)


def _tree_sha256(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
        if path.is_symlink():
            raise RuntimeError("symlink in pinned continuation state")
        if path.is_file():
            relative = path.relative_to(root).as_posix().encode()
            digest.update(len(relative).to_bytes(4, "big"))
            digest.update(relative)
            digest.update(hashlib.sha256(path.read_bytes()).hexdigest().encode())
    return digest.hexdigest()


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
    supervisor_path = REPO / scope["source"]["supervisor_path"]
    supervisor_sha = hashlib.sha256(supervisor_path.read_bytes()).hexdigest()
    if supervisor_sha != scope["source"]["supervisor_sha256"]:
        raise RuntimeError("outer one-shot supervisor source hash differs from scope")

    paths = scope["fresh_local_state"]
    resume = scope.get("resume_after_genesis")
    resume_mode = type(resume) is dict
    root = Path(paths["root"])
    if any(Path(paths[key]).resolve().is_relative_to(Path(forbidden).resolve())
           or Path(forbidden).resolve().is_relative_to(Path(paths[key]).resolve())
           for key in ("root", "postgres_data", "delta_root", "evidence_root")
           for forbidden in paths["forbidden_state"]):
        raise RuntimeError("fresh output path overlaps a forbidden retained state")
    required_absent = ("root", "evidence_root") if resume_mode else (
        "root", "postgres_data", "delta_root", "evidence_root")
    existing = [paths[key] for key in required_absent if Path(paths[key]).exists()]
    if existing:
        raise RuntimeError(f"fresh output paths already exist: {existing}")
    if resume_mode:
        for key in ("postgres_data", "delta_root"):
            candidate = Path(paths[key])
            if (not candidate.is_dir() or candidate.is_symlink()
                    or candidate.resolve() != candidate):
                raise RuntimeError(f"retained continuation state directory unavailable: {key}")
        if (Path(paths["postgres_data"]) / "postmaster.pid").exists():
            raise RuntimeError("retained continuation PostgreSQL must be stopped")
        if (_tree_sha256(Path(paths["postgres_data"])) != resume.get("pgdata_tree_sha256")
                or _tree_sha256(Path(paths["delta_root"])) != resume.get("delta_tree_sha256")):
            raise RuntimeError("retained continuation PG/Delta tree hash differs")
        attempt = Path(resume["attempt_root"])
        inputs = {
            "setup_receipt": attempt / "setup-stage.json",
            "worker_failure": attempt / "worker-failure.json",
            "supervisor_result": attempt / "supervisor-result.json",
            "deferred_ack": Path(resume["deferred_ack_path"]),
            "source_page": Path(resume["source_page_path"]),
            "ack_reconciliation": Path(resume["ack_reconciliation_receipt"]),
            "manifest_readback": Path(resume["manifest_readback_receipt"]),
        }
        for name, path in inputs.items():
            if (not path.is_absolute() or path.is_symlink() or not path.is_file()
                    or hashlib.sha256(path.read_bytes()).hexdigest() != resume["input_sha256"].get(name)):
                raise RuntimeError(f"retained attempt13 evidence hash differs: {name}")
        failure = json.loads(inputs["worker_failure"].read_text())
        deferred_ack = json.loads(inputs["deferred_ack"].read_text())
        source_page = json.loads(inputs["source_page"].read_text())
        ack_proof = json.loads(inputs["ack_reconciliation"].read_text())
        manifest_proof = json.loads(inputs["manifest_readback"].read_text())
        if (failure.get("source_ack_attempted") is not True
                or failure.get("delta_write_state") != "confirmed_catalog_genesis_publication"
                or ack_proof.get("source_api_outcome") != "matched"
                or ack_proof.get("source_reconcile_called") is not True
                or ack_proof.get("source_ack_retry_or_replay_attempted") is not False
                or ack_proof.get("delta_write_attempted") is not False
                or ack_proof.get("cleanup_errors") != []
                or manifest_proof.get("status") != "read_only_delta_manifest_match"
                or manifest_proof.get("source_reconciliation_outcome") != "matched"
                or manifest_proof.get("source_reconciliation_receipt_sha256")
                    != resume["input_sha256"].get("ack_reconciliation")
                or manifest_proof.get("deferred_ack_sha256")
                    != resume["input_sha256"].get("deferred_ack")
                or manifest_proof.get("publication_id") != resume.get("publication_id")
                or manifest_proof.get("manifest_sha256") != resume.get("manifest_sha256")
                or deferred_ack.get("acknowledgement") != "deferred_not_attempted"
                or deferred_ack.get("publication_id") != resume.get("publication_id")
                or deferred_ack.get("manifest_sha256") != resume.get("manifest_sha256")
                or source_page.get("sha256") != resume.get("source_page_sha256")
                or deferred_ack.get("source_page", {}).get("page_sha256")
                    != resume.get("source_page_sha256")
                or deferred_ack.get("source_page", {}).get("next_sha256")
                    != resume.get("next_cursor_sha256")
                or hashlib.sha256(base64.b64decode(source_page["next"], validate=True)).hexdigest()
                    != resume.get("next_cursor_sha256")
                or Path(deferred_ack.get("source_page", {}).get("path", "")).resolve()
                    != inputs["source_page"].resolve()
                or manifest_proof.get("source_ack_retried") is not False
                or manifest_proof.get("source_or_delta_written") is not False
                or manifest_proof.get("postgres_reopened") is not False
                or manifest_proof.get("mismatched_fields") != []
                or manifest_proof.get("manifest_row_matches_retained_bytes") is not True):
            raise RuntimeError("retained genesis publication/ACK proofs are incomplete")
        for name in ("seed-group.json", "seed-group-result.json", "seed-continuation-config.json",
                     "seed-continuation-result.json", "continuation-group.json",
                     "continuation-group-result.json", "continuation-config.json",
                     "continuation-result.json"):
            if (attempt / name).exists() or (attempt / name).is_symlink():
                raise RuntimeError("prior continuation action exists; replay is refused")

    runtime_base = Path("/private/tmp") if sys.platform == "darwin" else Path("/tmp")
    socket_root = runtime_base / f"truss-pg18-{os.getuid()}"
    socket_identity = hashlib.sha256(os.fsencode(Path(paths["postgres_data"]).resolve())).hexdigest()[:24]
    socket_directory = socket_root / socket_identity
    runtime_lease = Path(str(Path(paths["postgres_data"]).resolve()) + ".truss-runtime.lock")
    if socket_root.exists() or socket_root.is_symlink():
        details = socket_root.lstat()
        if (stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode)
                or details.st_uid != os.getuid() or stat.S_IMODE(details.st_mode) != 0o700):
            raise RuntimeError("Truss LocalPostgres runtime root ownership/mode differs")
    if runtime_lease.is_symlink():
        raise RuntimeError("Truss .55 PGDATA lease must not be a symlink")
    if resume_mode:
        if socket_directory.exists() or socket_directory.is_symlink():
            info = socket_directory.lstat()
            if (not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode)
                    or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700
                    or any(socket_directory.iterdir())):
                raise RuntimeError("retained Truss socket directory must be owned, empty and mode 0700")
        if not runtime_lease.is_file() or runtime_lease.stat().st_uid != os.getuid():
            raise RuntimeError("owned retained Truss .55 runtime lease required")
        import fcntl
        lease_fd = os.open(runtime_lease, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0))
        try:
            try:
                fcntl.lockf(lease_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise RuntimeError("retained Truss runtime lease is still held") from error
            fcntl.lockf(lease_fd, fcntl.LOCK_UN)
        finally:
            os.close(lease_fd)
    elif socket_directory.exists() or runtime_lease.exists():
        raise RuntimeError("fresh Truss .55 socket directory and PGDATA lease must be absent")

    artifacts = scope["artifacts"]
    missing = [key for key in PENDING if not artifacts.get(key)]
    for candidate_commit in artifacts.get("upstream_candidate_commits", []):
        if subprocess.run(["git", "merge-base", "--is-ancestor", candidate_commit, "HEAD"],
                          cwd=REPO, stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL).returncode:
            missing.append("upstream_candidate_not_integrated:" + candidate_commit[:12])
    for path_key, hash_key in (
        ("truss_wheel", "truss_wheel_sha256"),
        ("runtime_manifest", "runtime_manifest_sha256"),
        ("graphframes_runtime_manifest", "graphframes_runtime_manifest_sha256"),
        ("bun_executable", "bun_sha256"),
        ("postgresql_18_runtime_manifest", "postgresql_18_runtime_manifest_sha256"),
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
            if path_key == "truss_wheel":
                with zipfile.ZipFile(artifact_path) as wheel:
                    member_sha = hashlib.sha256(wheel.read("truss/local_runtime.py")).hexdigest()
                if member_sha != scope["qualification_stage"]["truss_local_runtime_member_sha256"]:
                    raise RuntimeError("Truss .55 deterministic socket implementation hash differs")
    python_executable = artifacts.get("python_executable")
    if python_executable:
        selected_python = Path(python_executable)
        if (not selected_python.is_absolute() or not selected_python.exists()
                or not selected_python.resolve().is_file() or not os.access(selected_python, os.X_OK)
                or hashlib.sha256(selected_python.resolve().read_bytes()).hexdigest() != artifacts.get("python_sha256")):
            raise RuntimeError("selected worker Python path/hash mismatch")
    postgres_runtime = artifacts.get("postgresql_18_runtime")
    if postgres_runtime:
        runtime = Path(postgres_runtime)
        manifest = json.loads(Path(artifacts["postgresql_18_runtime_manifest"]).read_text())
        if not runtime.is_absolute() or runtime.resolve() != runtime or runtime.is_symlink() or not runtime.is_dir():
            raise RuntimeError("canonical selected PostgreSQL 18 runtime directory required")
        for binary, key in (("pg_ctl", "pg_ctl_sha256"), ("postgres", "postgres_sha256")):
            binary_path = runtime / "bin" / binary
            if (not binary_path.is_file() or not os.access(binary_path, os.X_OK)
                    or hashlib.sha256(binary_path.read_bytes()).hexdigest() != manifest.get(key)):
                raise RuntimeError(f"selected PostgreSQL runtime binary hash mismatch: {binary}")
        if not str(manifest.get("postgresql_version", "")).startswith("18."):
            raise RuntimeError("selected PostgreSQL runtime manifest is not major 18")
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
        "fresh_output_paths_absent": True,
        "retained_attempt13_source_and_delta_present": resume_mode,
        "retained_pgdata_stopped_and_hash_matched": resume_mode,
        "truss_runtime_lease_admissible": True,
        "truss_socket_directory_absent": not resume_mode,
        "retained_truss_socket_directory_empty": resume_mode,
        "truss_socket_root": str(socket_root),
        "truss_expected_socket_directory": str(socket_directory),
        "r59_state_disjoint": True,
        "planned_bounds_enforced": False,
        "setup_stage_source_sha256": stage_sha,
        "supervisor_source_sha256": supervisor_sha,
        "upstream_candidate_commits": artifacts.get("upstream_candidate_commits"),
        "upstream_candidate_review": review_status,
        "source_model_sha256": model_sha,
        "candidate_bindings_missing": missing,
        "limits": scope["limits"],
        "planned_workflow_steps": len(scope["workflow"]),
        "note": "The separate one-shot supervisor is bounded; missing candidate bindings and independent review still disable dispatch.",
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
