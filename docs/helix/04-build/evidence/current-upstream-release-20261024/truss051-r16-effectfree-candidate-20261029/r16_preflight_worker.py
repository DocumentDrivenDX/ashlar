#!/usr/bin/env python3
"""Read-only R16 admission worker. It has no native execution mode."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile


COMMIT = "48fb9cb047f95854d59d0882af31156aab5f1f99"
ARCHIVE_SHA256 = "cb57fdb7d338f345cfad157463fdbe94a2be7dcf69e728c474954c2939c14a0b"
TRUSS_WHEEL_SHA256 = "eb59141c48e94bb93ab7053e9e8725012eee1749e4432e9c71804d23abbdff57"
PG_CTL_SHA256 = "3021087af95fd33bfd83106c85de47c4756e44aa7edde78446e7593604d1b451"
SEED_REQUEST_SHA256 = "56aed4d7a4789f45cdaa7af55a834ff38ca2b4bcb75f44658b3031ba5f8aa771"
PIN_LINE = 'truss-local = ["umf-truss[native] @ https://github.com/DocumentDrivenDX/truss/releases/download/v0.0.51/umf_truss-0.0.51-py3-none-any.whl#sha256=eb59141c48e94bb93ab7053e9e8725012eee1749e4432e9c71804d23abbdff57"]'


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_readonly(args: list[str], *, timeout: int = 10) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, check=False, capture_output=True, text=True, timeout=timeout)


def inspect(args: argparse.Namespace) -> dict:
    archive = Path(args.archive)
    wheel = Path(args.wheel)
    pg_ctl = Path(args.pg_ctl)
    runtime_python = Path(args.runtime_python)
    seed_request = Path(args.seed_request)
    for label, path in (("archive", archive), ("wheel", wheel), ("pg_ctl", pg_ctl), ("runtime_python", runtime_python), ("seed_request", seed_request)):
        if not path.is_file():
            raise ValueError(f"{label} must be an existing regular file: {path}")

    archive_hash = sha256(archive)
    wheel_hash = sha256(wheel)
    pg_ctl_hash = sha256(pg_ctl)
    if archive_hash != ARCHIVE_SHA256:
        raise ValueError(f"source archive hash mismatch: {archive_hash}")
    with tarfile.open(archive, "r:") as snapshot:
        pyproject = snapshot.extractfile("pyproject.toml")
        if pyproject is None:
            raise ValueError("frozen source archive lacks pyproject.toml")
        pyproject_text = pyproject.read().decode("utf-8")
    if PIN_LINE not in pyproject_text.splitlines():
        raise ValueError("frozen source archive does not contain the exact Truss 0.0.51 native pin")
    if wheel_hash != TRUSS_WHEEL_SHA256:
        raise ValueError(f"Truss wheel hash mismatch: {wheel_hash}")
    seed_hash = sha256(seed_request)
    if seed_hash != SEED_REQUEST_SHA256:
        raise ValueError(f"seed request hash mismatch: {seed_hash}")
    seed = json.loads(seed_request.read_bytes())
    if seed.get("request_key") != "ashlar-current-048-lifecycle-seed-r1" or len(seed.get("records", [])) + len(seed.get("edges", [])) != 7:
        raise ValueError("exact retained seven-operation seed request required")
    if pg_ctl_hash != PG_CTL_SHA256:
        raise ValueError(f"external pg_ctl hash mismatch: {pg_ctl_hash}")

    version = run_readonly([str(pg_ctl), "--version"])
    if version.returncode != 0 or version.stdout.strip() != "pg_ctl (PostgreSQL) 18.6 (Homebrew)":
        raise ValueError("external pg_ctl version check failed")

    package_code = (
        "import importlib.metadata as m,json,pathlib; "
        "d=m.distribution('umf-truss'); "
        "p=pathlib.Path(d._path)/'direct_url.json'; "
        "print(json.dumps({'version':d.version,'direct_url':json.loads(p.read_text()) if p.exists() else None}))"
    )
    package = run_readonly([str(runtime_python), "-c", package_code])
    if package.returncode != 0:
        raise ValueError("isolated runtime Truss metadata inspection failed")
    metadata = json.loads(package.stdout)
    if metadata.get("version") != "0.0.51":
        raise ValueError("isolated runtime does not contain Truss 0.0.51")
    archive_info = (metadata.get("direct_url") or {}).get("archive_info") or {}
    if (archive_info.get("hashes") or {}).get("sha256") != TRUSS_WHEEL_SHA256:
        raise ValueError("installed Truss direct_url does not bind the official wheel")
    pip_check = run_readonly([str(runtime_python), "-m", "pip", "check"])
    if pip_check.returncode != 0 or pip_check.stdout.strip() != "No broken requirements found.":
        raise ValueError("isolated runtime pip check failed")

    return {
        "profile": "ashlar-truss-r16-effectfree-preflight/0.1",
        "native_effects": False,
        "worker_sha256": sha256(Path(__file__).resolve()),
        "source_commit": COMMIT,
        "source_archive": {"path": str(archive), "sha256": archive_hash},
        "source_pyproject_pin": PIN_LINE,
        "truss_wheel": {"path": str(wheel), "version": "0.0.51", "sha256": wheel_hash},
        "seed_request": {"path": str(seed_request), "bytes": seed_request.stat().st_size, "sha256": seed_hash,
                         "request_key": seed["request_key"], "operation_count": 7},
        "runtime_python": str(runtime_python),
        "installed_truss_direct_url": metadata["direct_url"],
        "pip_check": pip_check.stdout.strip(),
        "external_pg_ctl": {"path": str(pg_ctl), "sha256": pg_ctl_hash, "version": version.stdout.strip()},
        "execution_admitted": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preflight-only", action="store_true", required=True)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--wheel", required=True)
    parser.add_argument("--runtime-python", required=True)
    parser.add_argument("--pg-ctl", required=True)
    parser.add_argument("--seed-request", required=True)
    args = parser.parse_args()
    try:
        receipt = inspect(args)
    except (OSError, ValueError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        print(json.dumps({"native_effects": False, "execution_admitted": False, "error": str(exc)}, sort_keys=True))
        return 2
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
