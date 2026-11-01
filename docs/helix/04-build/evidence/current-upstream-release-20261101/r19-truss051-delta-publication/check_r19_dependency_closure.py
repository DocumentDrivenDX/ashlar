#!/usr/bin/env python3
"""Effect-free proof that committed R19/R16b/R15 packet dependencies resolve."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
R15 = HERE.parent / "r15-truss051-view-transaction"
R16B = HERE.parent / "r16b-truss051-seed-resume"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load committed dependency: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    r19_scope = json.loads((HERE / "r19-publication-scope.json").read_text())
    r16_scope_path = R16B / "r16b-resume-scope.json"
    r16_scope = json.loads(r16_scope_path.read_text())
    prior = r16_scope["prior_r15"]
    r15_bound = {
        "r15-truss051-resume-scope.json": prior["scope_sha256"],
        "r15_truss051_local_worker.py": prior["worker_sha256"],
        "r15_truss051_supervisor.py": prior["supervisor_sha256"],
    }
    for name, expected in r15_bound.items():
        actual = digest(R15 / name)
        if actual != expected:
            raise AssertionError(f"R15 dependency hash mismatch: {name}")

    seed_path = R15 / "fresh-local-seed-request.json"
    seed = json.loads(seed_path.read_text())
    if digest(seed_path) != r16_scope["consumer_and_model"]["seed_request_sha256"]:
        raise AssertionError("R15 seed request hash differs from the R16b scope")

    packet = r19_scope["r16b_packet"]
    for key, name in (("scope_sha256", "r16b-resume-scope.json"),
                      ("worker_sha256", "r16b_seed_resume_worker.py"),
                      ("supervisor_sha256", "r16b_supervisor.py")):
        if digest(R16B / name) != packet[key]:
            raise AssertionError(f"R16b dependency hash mismatch: {name}")

    # Import the real R16b worker and supervisor; their relative R15 imports
    # must resolve from these committed sibling directories.
    r16_worker = load("r16b_closure_worker", R16B / "r16b_seed_resume_worker.py")
    r16_supervisor = load("r16b_closure_supervisor", R16B / "r16b_supervisor.py")
    for name, module in (("worker", r16_worker), ("supervisor", r16_supervisor)):
        if not getattr(module, "R15_PACKET", R15).resolve() == R15.resolve():
            raise AssertionError(f"R16b {name} resolves a different R15 sibling")

    # Reuse R19's own exact verifier against the retained R16b receipts. This
    # reads packet/receipt bytes only; it does not contact PostgreSQL or Spark.
    r19 = load("r19_closure_worker", HERE / "r19_delta_publication_worker.py")
    expected_page = r19.verify_r16b(r19_scope)
    if expected_page["prior_sha256"] != r19_scope["r16b_source"]["checkpoint_sha256"]:
        raise AssertionError("R19 prior page digest is not the exact R16b checkpoint")

    print(json.dumps({
        "status": "passed_committed_r19_r16b_r15_dependency_closure",
        "r15_dependency_sha256": r15_bound,
        "seed_request_sha256": digest(seed_path),
        "r16b_scope_sha256": digest(r16_scope_path),
        "r16b_worker_sha256": digest(R16B / "r16b_seed_resume_worker.py"),
        "r16b_supervisor_sha256": digest(R16B / "r16b_supervisor.py"),
        "r16b_receipts_verified_by": "R19 verify_r16b",
        "page_sha256": expected_page["page_sha256"],
        "database_contacted": False,
        "spark_started": False,
        "native_preflight": "not rerun; historical R19 output paths are consumed and retained",
    }, sort_keys=True))


if __name__ == "__main__":
    main()
