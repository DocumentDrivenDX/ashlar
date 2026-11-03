#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Build a closed, validated managed A1 runner config from local evidence locks."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT))
sys.path.insert(0, str(REPOSITORY_ROOT / "src"))
from tools.run_managed_a1_serverless import (
    HOST, INITIALIZER_TIMEOUT_SECONDS, PUBLISHER_TIMEOUT_SECONDS,
    VERIFIER_TIMEOUT_SECONDS, build_payload,
)


def _artifact(spec: str) -> dict:
    if "=" not in spec:
        raise ValueError("Artifact mapping must be LOCAL_FILE=/Workspace/or/Volumes/path")
    local_text, remote = spec.split("=", 1)
    local_source = Path(local_text).expanduser()
    if local_source.is_symlink():
        raise ValueError("Artifact source may not be a symbolic link")
    local = local_source.resolve(strict=True)
    if (not local.is_file()
            or not (remote.startswith("/Workspace/") or remote.startswith("/Volumes/"))):
        raise ValueError("Artifact requires a regular local file and immutable Workspace/Volume destination")
    if not local.name.endswith((".whl", ".txt")):
        raise ValueError("Task dependencies must be wheel or requirements files")
    if local.suffix == ".txt":
        remote = "-r " + remote
    return {"path": remote, "sha256": hashlib.sha256(local.read_bytes()).hexdigest()}


def build_config(*, runtime_result: dict, trial_id: str, notebook_root: str,
                 environment_version: int, dependencies: list[dict],
                 local_receipt: Path, evidence_dir: str, bun_path: Path,
                 python: Path) -> dict:
    from ashlar_host.configured_local_runtime import PROFILE, MANAGED_SERVERLESS_PROFILE

    if not re.fullmatch(r"a1-[a-z0-9][a-z0-9-]{0,49}", trial_id):
        raise ValueError("Unique bounded trial ID beginning a1- required")
    if not notebook_root.startswith("/Workspace/") or notebook_root.endswith("/"):
        raise ValueError("Uploaded notebook root must be an absolute Workspace path")
    if type(environment_version) is not int or environment_version < 1:
        raise ValueError("Read-back-confirmed serverless environment version required")
    if type(runtime_result) is not dict or set(runtime_result) != {
        "runtimeName", "sparkVersion", "sparkBuildText", "runtimeTag", "principal",
        "catalog", "schema", "protocol", "serverlessApiProbe", "sideEffects",
    }:
        raise ValueError("Closed read-only runtime probe result required")
    api_probe = runtime_result["serverlessApiProbe"]
    if (type(api_probe) is not dict or set(api_probe) != {
            "profile", "providedJobSessionBorrowed", "tagAddedAndObserved", "taggedQueryRowCount",
            "taggedQueryValue", "interruptTagResponseType", "interruptTagResponseCount",
            "tagRemoved", "preexistingTagsRestored", "operationTerminationClaimed"}
            or api_probe != {
                "profile": "ashlar-spark-connect-tag-api-probe/0.2",
                "providedJobSessionBorrowed": True, "tagAddedAndObserved": True,
                "taggedQueryRowCount": 1, "taggedQueryValue": 1,
                "interruptTagResponseType": "list",
                "interruptTagResponseCount": api_probe.get("interruptTagResponseCount"),
                "tagRemoved": True, "preexistingTagsRestored": True,
                "operationTerminationClaimed": False}
            or type(api_probe.get("interruptTagResponseCount")) is not int
            or api_probe["interruptTagResponseCount"] < 0):
        raise ValueError("Runtime probe must qualify Spark Connect session tags and interrupt response")
    if (runtime_result["runtimeName"] != "databricks-serverless"
            or runtime_result["sparkVersion"] != "4.2.0"
            or runtime_result["protocol"] != PROFILE.identity
            or runtime_result["sideEffects"] != []
            or not all(type(runtime_result.get(key)) is str and runtime_result[key]
                       for key in ("sparkVersion", "principal", "catalog", "schema"))
            or any(runtime_result.get(key) is not None
                   and type(runtime_result[key]) is not str
                   for key in ("sparkBuildText", "runtimeTag"))
            or not (runtime_result.get("sparkBuildText") or runtime_result.get("runtimeTag"))):
        raise ValueError("Probe runtime does not satisfy exact Databricks serverless identity contract")
    if not dependencies:
        raise ValueError("At least one immutable wheel or requirements artifact is required")
    if not local_receipt.is_file() or local_receipt.is_symlink():
        raise ValueError("Exact local A1 qualification receipt file required")
    bun_path = bun_path.resolve(strict=True)
    python = Path(os.path.normpath(str(python.expanduser())))
    if not bun_path.is_file() or not os.access(bun_path, os.X_OK):
        raise ValueError("Exact local Bun runtime required")
    if (not python.is_absolute() or not python.is_file()
            or not os.access(python, os.X_OK)
            or python.lstat().st_uid != os.getuid()
            or python.resolve(strict=True) != Path(sys.executable).resolve(strict=True)):
        raise ValueError("Owner-selected venv Python executable required")

    suffix = trial_id[3:].replace("-", "_")
    schema = "ashlar_a1_" + suffix
    volume_name = "ashlar_a1_volume_" + suffix
    runtime = {"runtimeName": "databricks-serverless",
        "sparkVersion": runtime_result["sparkVersion"],
        "sparkBuildText": runtime_result["sparkBuildText"],
        "runtimeTag": runtime_result["runtimeTag"],
        "principal": runtime_result["principal"],
        # Keep the raw runtime probe echo unchanged, but tasks use the derived
        # managed-serverless reader profile selected for this Spark version.
        "catalog": "client_dev", "schema": schema,
        "protocol": MANAGED_SERVERLESS_PROFILE.identity}
    cost = {"estimated_total_dbu": 16.0, "cleanup_reserve_dbu": 4.0,
        "authorized_total_dbu": 122.0, "estimated_total_usd": 9.75,
        "cleanup_reserve_usd": 20.0, "preferred_budget_exception": None,
        "includes_startup_driver_verifier_handoff_and_cleanup": True}
    tasks = {}
    for role in ("initializer", "publisher", "verifier"):
        notebook_name = {"initializer": "initialize.py", "publisher": "publish.py",
                         "verifier": "verify.py"}[role]
        notebook_file = Path(__file__).resolve().parents[1] / "notebooks" / "managed_a1" / notebook_name
        notebook_path = notebook_root + "/managed_a1/" + notebook_name
        tasks[role] = {"notebook_path": notebook_path,
            "notebook_sha256": hashlib.sha256(notebook_file.read_bytes()).hexdigest(),
            "environment_version": environment_version,
            "dependencies": dependencies,
            "base_parameters": {"task_parameters_json": "{}"},
            "timeout_seconds": {
                "initializer": INITIALIZER_TIMEOUT_SECONDS,
                "publisher": PUBLISHER_TIMEOUT_SECONDS,
                "verifier": VERIFIER_TIMEOUT_SECONDS,
            }[role]}
    now_us = str(int(datetime.now(timezone.utc).timestamp() * 1_000_000))
    qualification = local_receipt.resolve(strict=True)
    try:
        relative_receipt = qualification.relative_to(Path.cwd().resolve())
    except ValueError as error:
        raise ValueError("Local qualification receipt must be inside the repository") from error
    config = {"host": HOST, "trial_id": trial_id, "catalog": "client_dev",
        "schema": schema, "expected_principal": runtime["principal"],
        "expected_runtime": runtime, "maximum_wall_seconds": 900,
        "cleanup_reserve_seconds": 60, "cost": cost,
        "publisher": tasks["publisher"], "initializer": tasks["initializer"],
        "verifier": tasks["verifier"],
        "holder_bootstrap": {"bootstrap":
            "ashlar_host.managed_a1_source_bootstrap:bootstrap_from_private_config",
            "bun_path": str(bun_path), "python": str(python)},
        "trial": {"catalog": "client_dev", "schema": schema,
            "expected_runtime": runtime, "stream": "ashlar-managed-a1-" + suffix,
            "stage_id": "managed-a1-trial-" + trial_id[3:], "materialized_at": now_us,
            "destination_id": "client_dev." + schema, "cost": cost,
            "publisher_timeout_seconds": PUBLISHER_TIMEOUT_SECONDS,
            "verifier_timeout_seconds": VERIFIER_TIMEOUT_SECONDS},
        "protocol_profile": {"identity": MANAGED_SERVERLESS_PROFILE.identity,
            "reader_versions": sorted(MANAGED_SERVERLESS_PROFILE.reader_versions),
            "writer_versions": sorted(MANAGED_SERVERLESS_PROFILE.writer_versions),
            "features": sorted(MANAGED_SERVERLESS_PROFILE.features)},
        "volume_name": volume_name,
        "local_qualification": {"path": relative_receipt.as_posix(),
            "sha256": hashlib.sha256(qualification.read_bytes()).hexdigest()},
        "evidence_dir": evidence_dir}
    build_payload(config)
    return config


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-probe-result", type=Path, required=True)
    parser.add_argument("--trial-id", required=True)
    parser.add_argument("--notebook-root", required=True)
    parser.add_argument("--environment-version", type=int, required=True)
    parser.add_argument("--local-receipt", type=Path, required=True)
    parser.add_argument("--evidence-dir", default="docs/helix/04-build/evidence/current-upstream-release-20261102")
    parser.add_argument("--dependency", action="append", default=[], metavar="LOCAL=REMOTE",
                        help="Exact wheel/requirements file and its uploaded Workspace/Volume path; repeat per artifact")
    parser.add_argument("--bun", type=Path, default=Path(shutil.which("bun") or ""))
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = json.loads(args.runtime_probe_result.read_text(encoding="utf-8"))
    dependencies = [_artifact(item) for item in args.dependency]
    config = build_config(runtime_result=result, trial_id=args.trial_id,
        notebook_root=args.notebook_root, environment_version=args.environment_version,
        dependencies=dependencies, local_receipt=args.local_receipt,
        evidence_dir=args.evidence_dir, bun_path=args.bun, python=args.python)
    requested_output = args.output.expanduser()
    if requested_output.is_symlink():
        raise ValueError("Private config output may not be a symbolic link")
    output_parent = requested_output.parent.resolve(strict=True)
    output = output_parent / requested_output.name
    if (output.exists() or not output.parent.is_dir()
            or output.parent.stat().st_uid != os.getuid()
            or output.parent.stat().st_mode & 0o077):
        raise ValueError("New private config output path required")
    raw = json.dumps(config, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False).encode("utf-8")
    fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
        getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    directory_fd = os.open(output.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    print(str(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
