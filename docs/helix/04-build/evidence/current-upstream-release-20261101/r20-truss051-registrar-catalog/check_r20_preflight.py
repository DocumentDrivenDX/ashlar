#!/usr/bin/env python3
"""Run real R20 worker/supervisor effect-free preflights and bind the receipt."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess

HERE = Path(__file__).resolve().parent
scope_path = HERE / "r20-publication-scope.json"
scope_raw = scope_path.read_bytes()
scope = json.loads(scope_raw)
receipt = json.loads((HERE / scope["authorization_receipt"]).read_text())
worker_path = HERE / "r20_delta_publication_worker.py"
supervisor_path = HERE / "r20_supervisor.py"
digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()

assert receipt["scope_sha256"] == hashlib.sha256(scope_raw).hexdigest()
assert receipt["worker_sha256"] == digest(worker_path)
assert receipt["supervisor_sha256"] == digest(supervisor_path)
assert receipt["sol_review_status"] == "pending_sol_review"
assert receipt["dispatch_enabled"] is False
assert scope["publication"]["catalog_evidence_pre_spark"] is True
assert scope["publication"]["catalog_authority_role"] == "ashlar_local_dev_registrar_r6_20261031"

env = os.environ.copy()
env["PYTHONPATH"] = scope["runtime"]["pythonpath"]
python = scope["runtime"]["python_path"]


def run(path: Path) -> dict:
    result = subprocess.run([python, str(path), "--preflight"], env=env,
                            text=True, capture_output=True, timeout=180, check=True)
    return json.loads(result.stdout)


worker = run(worker_path)
assert worker["status"] == "passed_effect_free_worker_preflight"
assert worker["separate_catalog_factory_api"] == "passed"
assert not any(worker[name] for name in ("output_root_created", "postgres_started",
                                          "database_contacted", "spark_started",
                                          "delta_written", "acknowledgement"))
supervisor = run(supervisor_path)
assert supervisor["status"] == "passed_effect_free_supervisor_preflight"
assert not any(supervisor[name] for name in ("output_root_created", "supervisor_root_created",
                                              "worker_started", "postgres_started",
                                              "database_contacted"))
for name, path in scope["paths"].items():
    selected = Path(path)
    assert not selected.exists() and not selected.is_symlink(), (name, path)

result = {
    "status": "passed_actual_r20_worker_and_supervisor_effect_free_preflight",
    "scope_sha256": hashlib.sha256(scope_raw).hexdigest(),
    "worker_sha256": digest(worker_path),
    "supervisor_sha256": digest(supervisor_path),
    "worker": worker,
    "supervisor": supervisor,
    "registrar_catalog_api_shape": worker["separate_catalog_factory_api"],
    "all_r20_paths_absent": True,
    "native_dispatch_enabled": False,
}
receipt_path = HERE / "r20-effect-free-preflight-receipt.json"
receipt_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
print(json.dumps({"receipt_path": str(receipt_path), **result}, sort_keys=True))
