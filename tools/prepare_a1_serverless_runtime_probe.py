#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Print a single-run Jobs create payload for the read-only runtime probe."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path


def build_payload(notebook_path: str, notebook_file: Path, environment_version: int,
                  principal: str) -> dict:
    if (notebook_file.is_symlink() or not notebook_file.is_file()
            or not notebook_path.startswith("/Workspace/")
            or type(environment_version) is not int or environment_version < 1
            or not re.fullmatch(r"[^\s@]+@[^\s@]+", principal)):
        raise ValueError("Exact Workspace notebook, read-back environment version, and principal required")
    digest = hashlib.sha256(notebook_file.read_bytes()).hexdigest()
    return {
        "name": "ashlar-a1-serverless-runtime-probe",
        "run_as": {"user_name": principal},
        "max_concurrent_runs": 1,
        "max_retries": 0,
        "retry_on_timeout": False,
        "timeout_seconds": 60,
        "tasks": [{
            "task_key": "runtime_probe",
            "depends_on": [],
            "timeout_seconds": 60,
            "max_retries": 0,
            "retry_on_timeout": False,
            "notebook_task": {"notebook_path": notebook_path,
                               "base_parameters": {}},
            "environment_key": "probe_env",
        }],
        "environments": [{
            "environment_key": "probe_env",
            "spec": {"environment_version": str(environment_version),
                     "dependencies": []},
        }],
        "tags": {"project": "ashlar", "purpose": "read-only-serverless-runtime-probe",
                 "notebook_sha256": digest},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--notebook-path", required=True,
                        help="Uploaded /Workspace path for notebooks/managed_a1/runtime_probe.py")
    parser.add_argument("--notebook-file", type=Path, default=Path(__file__).resolve().parents[1]
                        / "notebooks/managed_a1/runtime_probe.py")
    parser.add_argument("--environment-version", type=int, required=True,
                        help="Read-back-confirmed serverless environment version")
    parser.add_argument("--principal", required=True,
                        help="Exact workspace Jobs run_as principal")
    args = parser.parse_args()
    print(json.dumps(build_payload(args.notebook_path, args.notebook_file,
        args.environment_version, args.principal), sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
