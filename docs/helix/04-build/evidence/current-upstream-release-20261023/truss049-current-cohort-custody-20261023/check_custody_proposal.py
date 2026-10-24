from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[5]
BASE = HERE.parent


def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def load(path: Path):
    return json.loads(path.read_text(), object_pairs_hook=reject_duplicate_keys)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args: str) -> str:
    return subprocess.check_output(["git", "-C", str(REPO), *args], text=True).strip()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


proposal = load(HERE / "custody-proposal.json")
refresh_path = BASE / "truss049-r5-current-cohort-refresh-20261023" / "refresh-proposal.json"
runtime_path = BASE / "truss049-current-cohort-runtime-20261023" / "runtime-receipt.json"
old_path = BASE / "truss049-r5-readiness-20261023" / "preparation.json"
refresh = load(refresh_path)
runtime = load(runtime_path)
old = load(old_path)

require(proposal["schema"] == "ashlar-truss049-current-cohort-custody-proposal/v1", "schema")
for field in ("execution_authorized", "approval_file_created", "started_marker_created", "native_or_source_effects_started"):
    require(proposal[field] is False, f"must remain false: {field}")
require(proposal["preflight"]["status"] == "not_run", "proposal must not claim preflight")
require(not proposal["preflight"]["supervisor_rebound_to_selected_source_and_runtime"], "supervisor unexpectedly rebound")
require(not proposal["preflight"]["worker_rebound_to_selected_source_and_runtime"], "worker unexpectedly rebound")
require(sha256(refresh_path) == proposal["selected_source"]["selection_sha256"], "refresh proposal hash")
require(sha256(runtime_path) == proposal["runtime"]["receipt_sha256"], "runtime receipt hash")
require(sha256(old_path) == proposal["prior_packet"]["preparation_sha256"], "historical R5 preparation hash")

source = proposal["selected_source"]
commit = source["commit"]
require(git("rev-parse", f"{commit}^{{tree}}") == source["tree"], "selected source tree")
require(refresh["refresh"]["selected_source"]["commit"] == commit, "selected source commit differs from refresh proposal")
for path, expected in source["runtime_input_objects"].items():
    require(git("rev-parse", f"{commit}:{path}") == expected, f"source object: {path}")
require(refresh["refresh"]["selected_source"]["runtime_input_objects"] == source["runtime_input_objects"], "source object map differs from refresh proposal")

expected_versions = proposal["runtime"]["versions"]
require(runtime["runtime"]["python"] == "3.12.14", "runtime Python")
require(runtime["runtime"]["platform"] == "macOS arm64", "runtime platform")
require(runtime["runtime"]["venv_isolated"] is True, "runtime isolation")
require(runtime["install"]["pip_check"].startswith("passed:"), "pip check")
require(runtime["install"]["wheel_count"] == 27, "selected wheel count")
for distribution, version in expected_versions.items():
    require(runtime["install"]["versions"].get(distribution) == version, f"runtime version: {distribution}")
require(runtime["scope"].startswith("Fresh isolated package runtime provenance only"), "runtime scope")

lineage = proposal["preserved_lineage"]
old_inputs = old["immutable_inputs"]
expected_lineage = {
    "source_document_id": old_inputs["source_document_id"],
    "source_revision": old_inputs["source_revision"],
    "source_sha256": old_inputs["source_sha256"],
    "source_origin_sha256": old_inputs["source_origin_sha256"],
    "source_registration_id": old_inputs["source_registration_id"],
    "feed_registration_receipt_sha256": old_inputs["existing_feed_registration_receipt"]["sha256"],
    "consumer_id": old_inputs["existing_consumer_id"],
    "consumer_registration_id": old_inputs["consumer_registration_id"],
    "genesis_checkpoint_sha256": old_inputs["genesis_checkpoint_sha256"],
    "prior_public_source_reobservation_receipt_sha256": old_inputs["prior_public_source_reobservation_receipt"]["sha256"],
    "seed_request_sha256": old_inputs["seed_request"]["sha256"],
}
for field, value in expected_lineage.items():
    require(lineage[field] == value, f"preserved lineage mismatch: {field}")
pg_ref = lineage["retained_pgdata_reference"]
require(pg_ref["source"] == "../truss049-r5-readiness-20261023/preparation.json", "PGDATA source")
require(pg_ref["json_pointer"] == "/immutable_inputs/retained_pgdata", "PGDATA pointer")
require(pg_ref["source_preparation_sha256"] == sha256(old_path), "PGDATA source digest")
require("retained_pgdata" not in pg_ref and "value" not in pg_ref, "raw PGDATA path copied")

require(runtime["install"]["versions"]["weft-sql"] == "0.4.53", "stale Weft runtime")
require("0.4.52" in proposal["prior_packet"]["state"], "R5 stale cohort disclosure")
for name in ("execution-approval.json", "started.json", "offline-preflight.json"):
    require(not (HERE / name).exists(), f"unexpected execution artifact: {name}")

print("Current-cohort custody proposal: static hash, source, runtime and lineage checks passed; no preflight/effects claimed")
