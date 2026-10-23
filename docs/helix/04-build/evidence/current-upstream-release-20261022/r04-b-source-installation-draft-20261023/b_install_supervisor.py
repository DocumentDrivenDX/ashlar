#!/usr/bin/env python3
"""Bounded effect-free B packet supervisor; execution remains unapproved."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
WORKER = HERE / "b_install_worker.py"
CONFIG = HERE / "b-install-config.json"
MAX_RESULT = 65_536
PREP_DEADLINE = 120


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _load():
    raw = CONFIG.read_bytes()
    if len(raw) > MAX_RESULT or CONFIG.is_symlink():
        raise ValueError("bounded regular configuration required")
    value = json.loads(raw)
    return value, _sha(raw)


def preflight(run=subprocess.run):
    config, config_hash = _load()
    python = Path(config["runtime"]["venv"]) / "bin" / "python"
    if not python.is_file() or python.parent.name != "bin":
        raise RuntimeError("pinned Truss runtime interpreter unavailable")
    completed = run([str(python), "-I", "-B", str(WORKER), "--preflight"],
                    cwd=str(HERE), stdin=subprocess.DEVNULL, capture_output=True,
                    text=True, timeout=PREP_DEADLINE,
                    env={"PATH": "/usr/bin:/bin", "PYTHONNOUSERSITE": "1",
                         "PYTHONDONTWRITEBYTECODE": "1"})
    if completed.returncode != 0 or len(completed.stdout.encode()) > MAX_RESULT:
        raise RuntimeError("bounded worker byte preflight failed")
    lines = completed.stdout.splitlines()
    if len(lines) != 1:
        raise RuntimeError("exactly one worker JSON result required")
    worker = json.loads(lines[0])
    if any(worker.get(flag) is not False for flag in
           ("pg_started", "spark_started", "acceptance_invoked", "consumer_registered")):
        raise RuntimeError("worker preflight reported an unexpected effect")
    return {"profile": "ashlar-r04-b-supervisor-preflight/0.1",
            "status": "effect_free_preflight_passed",
            "config_sha256": config_hash,
            "worker_sha256": _sha(WORKER.read_bytes()),
            "supervisor_sha256": _sha(Path(__file__).read_bytes()),
            "worker": worker, "execution_approval": config["execution"]["approval"],
            "native_effects": 0}


def execute():
    config, _ = _load()
    # Deliberately fail closed until the packet, exclusive shared-port gate,
    # paths, identities, role names, runtime pins and human approval are frozen.
    if config["execution"]["approval"] is None:
        raise PermissionError("execution approval is null; no process or PGDATA created")
    if config["installation"].get("exclusive_port_admission") is None:
        raise PermissionError("exclusive 5432 admission is unresolved")
    raise PermissionError("reviewed R4 executor approval contract is not yet provisioned")


def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if args.preflight:
        print(json.dumps(preflight(), sort_keys=True))
        return 0
    execute()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
