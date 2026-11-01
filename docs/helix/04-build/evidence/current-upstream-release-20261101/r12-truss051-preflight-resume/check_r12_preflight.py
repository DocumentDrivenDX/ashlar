#!/usr/bin/env python3
"""Run the actual R12 worker's deterministic preflight without effects."""
from __future__ import annotations
import hashlib, json, os, subprocess
from pathlib import Path
HERE = Path(__file__).resolve().parent
SCOPE_PATH = HERE / "r12-truss051-resume-scope.json"

def main() -> None:
    raw = SCOPE_PATH.read_bytes()
    scope = json.loads(raw)
    runtime = scope["runtime"]
    env = os.environ.copy()
    env["PYTHONPATH"] = runtime["pythonpath"]
    result = subprocess.run([runtime["python_path"], str(HERE / "r12_truss051_local_worker.py"), "--preflight"],
        env=env, text=True, capture_output=True, timeout=120, check=True)
    proof = json.loads(result.stdout)
    assert proof["status"] == "passed_effect_free_preflight"
    assert proof["scope_sha256"] == hashlib.sha256(raw).hexdigest()
    assert proof["output_root_created"] is False and proof["postgres_started"] is False
    assert proof["native_database_calls"] is False and proof["migration_replayed"] is False
    for path in scope["paths"].values():
        candidate = Path(path)
        assert not candidate.exists() and not candidate.is_symlink(), candidate
    print(json.dumps({"status":"passed_actual_worker_preflight","worker_result":proof,
      "stderr":result.stderr,"worker_sha256":hashlib.sha256((HERE / "r12_truss051_local_worker.py").read_bytes()).hexdigest()},sort_keys=True))
if __name__ == "__main__": main()
