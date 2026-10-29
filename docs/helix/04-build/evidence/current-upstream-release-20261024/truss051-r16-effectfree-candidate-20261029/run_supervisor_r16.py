#!/usr/bin/env python3
"""R16 read-only supervisor. It cannot launch a native campaign."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys


WORKER_SHA256 = "f03a190bbac08cf4caa59ed8927c0f48b9d346eeecd1260e03546b7b8a243fb2"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preflight-only", action="store_true", required=True)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--wheel", required=True)
    parser.add_argument("--runtime-python", required=True)
    parser.add_argument("--pg-ctl", required=True)
    parser.add_argument("--seed-request", required=True)
    parser.add_argument("--worker", default=str(Path(__file__).with_name("r16_preflight_worker.py")))
    args = parser.parse_args()
    worker = Path(args.worker)
    if not worker.is_file():
        print(json.dumps({"native_effects": False, "execution_admitted": False, "error": "worker missing"}))
        return 2
    worker_hash = sha256(worker)
    if worker_hash != WORKER_SHA256:
        print(json.dumps({"native_effects": False, "execution_admitted": False,
                          "error": "worker source hash mismatch", "worker_sha256": worker_hash}))
        return 2
    command = [
        sys.executable, str(worker), "--preflight-only",
        "--archive", args.archive, "--wheel", args.wheel,
        "--runtime-python", args.runtime_python, "--pg-ctl", args.pg_ctl,
        "--seed-request", args.seed_request,
    ]
    child = subprocess.run(command, check=False, capture_output=True, text=True, timeout=90)
    if child.returncode:
        print(json.dumps({"native_effects": False, "execution_admitted": False,
                          "worker_exit": child.returncode, "worker_output": child.stdout.strip(),
                          "worker_error": child.stderr.strip()}, sort_keys=True))
        return 2
    receipt = json.loads(child.stdout)
    if receipt.get("native_effects") is not False or receipt.get("execution_admitted") is not False:
        print(json.dumps({"native_effects": False, "execution_admitted": False,
                          "error": "worker receipt violates effect-free contract"}, sort_keys=True))
        return 2
    if receipt.get("worker_sha256") != WORKER_SHA256:
        print(json.dumps({"native_effects": False, "execution_admitted": False,
                          "error": "worker receipt hash mismatch"}, sort_keys=True))
        return 2
    receipt["supervisor"] = "r16-read-only-preflight"
    receipt["supervisor_sha256"] = sha256(Path(__file__).resolve())
    receipt["supervisor_worker_path"] = str(worker)
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
