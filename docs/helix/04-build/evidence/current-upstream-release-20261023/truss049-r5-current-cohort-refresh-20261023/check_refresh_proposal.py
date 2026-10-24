from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path


HERE = Path(__file__).resolve().parent
REPO = HERE.parents[5]
def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


PROPOSAL = json.loads((HERE / "refresh-proposal.json").read_text(), object_pairs_hook=reject_duplicate_keys)
OLD_PREPARATION = HERE.parent / "truss049-r5-readiness-20261023" / "preparation.json"
OLD = json.loads(OLD_PREPARATION.read_text())


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def git(*args: str) -> str:
    return subprocess.check_output(["git", "-C", str(REPO), *args], text=True).strip()


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


require(PROPOSAL["status"] == "draft_effect_free_proposal", "proposal status")
require(PROPOSAL["execution_authorized"] is False, "execution must be unauthorized")
require(PROPOSAL["execution_approval_file_created"] is False, "no approval file")
require(PROPOSAL["native_or_source_effects_started"] is False, "no effects")

source = PROPOSAL["refresh"]["selected_source"]
selected = source["commit"]
require(git("rev-parse", f"{selected}^{{tree}}") == source["tree"], "selected commit tree mismatch")
require(OLD["source_checkout"]["commit"] == PROPOSAL["refresh"]["prior_packet_commit"], "prior source commit binding")
for path, expected in source["runtime_input_objects"].items():
    require(git("rev-parse", f"{selected}:{path}") == expected, f"selected source object mismatch: {path}")
    require(git("rev-parse", f"HEAD:{path}") == expected, f"live checkout runtime object mismatch: {path}")
require(source["runtime_input_objects"]["src"] == source["r5_source_objects"]["src"], "source tree unexpectedly differs from R5")
require(source["runtime_input_objects"]["sql"] == source["r5_source_objects"]["sql"], "SQL tree unexpectedly differs from R5")
require(source["r5_source_objects"] == OLD["source_checkout"]["runtime_input_objects"], "R5 source object baseline mismatch")
require(sha256(OLD_PREPARATION) == source["r5_preparation_sha256"], "R5 preparation changed")

changed_paths = git(
    "diff", "--name-only", OLD["source_checkout"]["commit"], selected, "--", "pyproject.toml", "src", "sql"
).splitlines()
require(changed_paths == ["pyproject.toml"], f"unexpected runtime input diff: {changed_paths}")
diff = git(
    "diff", "--no-ext-diff", "--unified=0", OLD["source_checkout"]["commit"], selected, "--", "pyproject.toml"
)
changed_lines = [line for line in diff.splitlines() if (line.startswith("+") and not line.startswith("+++")) or (line.startswith("-") and not line.startswith("---"))]
require(
    changed_lines == ['-    "weft-sql==0.4.48",', '+    "weft-sql==0.4.53",'],
    f"unexpected pyproject runtime pin diff: {changed_lines}",
)
require(
    source["only_runtime_input_change"] == "pyproject.toml: weft-sql 0.4.48 -> 0.4.53",
    "source pin change description mismatch",
)
require(git("diff", "--quiet", "HEAD", "--", "pyproject.toml", "src", "sql") == "", "unstaged runtime input change")
require(git("diff", "--cached", "--quiet", "HEAD", "--", "pyproject.toml", "src", "sql") == "", "staged runtime input change")
require(not git("ls-files", "--others", "--exclude-standard", "--", "src", "sql"), "untracked runtime input")

old_source = OLD["immutable_inputs"]
preserved = PROPOSAL["preserved_lineage"]
for proposal_key, old_key in (
    ("source_document_id", "source_document_id"),
    ("source_revision", "source_revision"),
    ("source_sha256", "source_sha256"),
    ("source_origin_sha256", "source_origin_sha256"),
    ("source_registration_id", "source_registration_id"),
    ("feed_registration_receipt_sha256", "existing_feed_registration_receipt"),
    ("prior_public_source_reobservation_receipt_sha256", "prior_public_source_reobservation_receipt"),
    ("consumer_id", "existing_consumer_id"),
    ("consumer_registration_id", "consumer_registration_id"),
    ("genesis_checkpoint_sha256", "genesis_checkpoint_sha256"),
    ("seed_request_sha256", "seed_request"),
):
    actual_old = old_source[old_key]
    if proposal_key in ("feed_registration_receipt_sha256", "prior_public_source_reobservation_receipt_sha256", "seed_request_sha256"):
        actual_old = actual_old["sha256"]
    require(preserved[proposal_key] == actual_old, f"preserved lineage mismatch: {proposal_key}")

pgdata_ref = preserved["retained_pgdata_reference"]
require(pgdata_ref["source"] == "../truss049-r5-readiness-20261023/preparation.json", "PGDATA source reference")
require(pgdata_ref["json_pointer"] == "/immutable_inputs/retained_pgdata", "PGDATA pointer")
require(pgdata_ref["preparation_sha256"] == source["r5_preparation_sha256"], "PGDATA pointer source digest")
require("value" not in pgdata_ref and "retained_pgdata_path" not in preserved, "raw PGDATA path must remain referenced, not copied")
require(type(old_source["retained_pgdata"]) is str and Path(old_source["retained_pgdata"]).is_absolute(), "retained PGDATA reference missing")

pins = PROPOSAL["selected_cohort"]
pyproject = (REPO / "pyproject.toml").read_text()
require(pins["truss_toolkit"]["wheel_sha256"] in pyproject, "Truss pin mismatch")
require('"weft-sql==0.4.53"' in pyproject, "current Weft pin mismatch")
require(pins["umf_core"]["wheel_sha256"] in pyproject, "UMF core pin mismatch")
require(pins["umf_access"]["wheel_sha256"] in pyproject, "UMF access pin mismatch")
require(OLD["selected_weft_candidate"]["wheel_sha256"] != pins["weft_sql"]["wheel_sha256"], "old Weft pin unexpectedly reused")

compat = PROPOSAL["compatibility_evidence"]
receipt = HERE.parent / "weft053-ashlar-adoption-20261023" / "README.md"
require(sha256(receipt) == compat["sha256"], "Weft 0.4.53 receipt changed")
receipt_text = receipt.read_text()
for needle in (compat["fixture_sha256"], compat["response_sha256"], pins["weft_sql"]["source_commit"], pins["weft_sql"]["wheel_sha256"]):
    require(needle in receipt_text, f"Weft adoption receipt missing {needle}")

print("R5 current-cohort refresh proposal: inert consistency checks passed")
