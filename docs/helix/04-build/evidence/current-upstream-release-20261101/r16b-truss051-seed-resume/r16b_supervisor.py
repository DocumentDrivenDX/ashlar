#!/usr/bin/env python3
"""R16 parameterized entrypoint reusing the reviewed R15 bounded supervisor."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
R15_PACKET = HERE.parent / "r15-truss051-view-transaction"
sys.path.insert(0, str(R15_PACKET))
R15_SUPERVISOR_PATH = R15_PACKET / "r15_truss051_supervisor.py"
R15_WORKER_PATH = R15_PACKET / "r15_truss051_local_worker.py"
SCOPE_PATH = HERE / "r16b-resume-scope.json"
WORKER_PATH = HERE / "r16b_seed_resume_worker.py"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load reviewed supervisor module: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


base = load_module("r15_reviewed_bounded_supervisor", R15_SUPERVISOR_PATH)
r15_worker = load_module("r15_reviewed_worker_contract", R15_WORKER_PATH)
base.SCOPE_PATH = SCOPE_PATH
base.WORKER = WORKER_PATH
base.EXPECTED_PATHS = json.loads(SCOPE_PATH.read_text())["paths"]


def validate_standing_authorization(scope, scope_raw, *, receipt=None, worker_raw=None,
                                    supervisor_raw=None, require_dispatch=True):
    receipt = receipt if receipt is not None else json.loads(
        (HERE / scope["authorization_receipt"]).read_text())
    worker_raw = worker_raw if worker_raw is not None else WORKER_PATH.read_bytes()
    supervisor_raw = supervisor_raw if supervisor_raw is not None else Path(__file__).read_bytes()
    expected = {
        "schema": "ashlar.standing-local-dispatch-binding/1",
        "authorization_basis": "standing_user_local_release_authorization",
        "user_authorization_statement": "User authorized fresh Truss source creation, a local seed, and a tested release; bounded local R16b continuation execution is within that standing authorization and requires no per-attempt user approval.",
        "scope_sha256": hashlib.sha256(scope_raw).hexdigest(),
        "worker_sha256": hashlib.sha256(worker_raw).hexdigest(),
        "supervisor_sha256": hashlib.sha256(supervisor_raw).hexdigest(),
        "attempts": 1,
        "cloud": False,
    }
    for key, value in expected.items():
        if receipt.get(key) != value:
            raise base.StopSupervisor(f"R16b standing authorization binding mismatch: {key}")
    if set(receipt) != set(expected) | {"sol_review_status", "dispatch_enabled"}:
        raise base.StopSupervisor("R16b authorization receipt fields changed")
    if require_dispatch:
        if receipt["sol_review_status"] != "sol_reviewed" or receipt["dispatch_enabled"] is not True:
            raise base.StopSupervisor("R16b dispatch remains pending Sol review")
    elif receipt["sol_review_status"] not in ("pending_sol_review", "sol_reviewed") or type(receipt["dispatch_enabled"]) is not bool:
        raise base.StopSupervisor("R16b effect-free preflight authorization malformed")


def validate_release_scope(scope):
    r15_worker.validate_release_cohort(scope["release_cohort"], scope["runtime"])
    expected_doc = {"document_id":"ashlar-local-dev-typed-graph-r6-20261031",
        "document_revision":"1","document_sha256":"2c9b295a8232cae4770255650bf634941ab223e582b79ae2bb78f6acf0b8a880",
        "catalog_revision":1,"registration_id":"bc6e5004-b07f-4a25-9c35-19a08c4b5f39"}
    if scope["source"]["catalog_accepted"] != expected_doc:
        raise base.StopSupervisor("R16b retained accepted R6 source identity changed")
    if scope.get("resume_source", {}).get("path") != "/private/tmp/ashlar-r15-truss051-current-cohort-20261101/r13-state-continuation-clone-pgdata":
        raise base.StopSupervisor("R16b exact R15 stopped source path changed")


base.validate_standing_authorization = validate_standing_authorization
base.validate_release_scope = validate_release_scope


if __name__ == "__main__":
    raise SystemExit(base.main())
