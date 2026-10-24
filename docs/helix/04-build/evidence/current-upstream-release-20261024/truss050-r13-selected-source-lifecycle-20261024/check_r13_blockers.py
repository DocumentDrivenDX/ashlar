#!/usr/bin/env python3
"""Effect-free R13 blocker confirmation; local reads and hashes only."""
from pathlib import Path
import hashlib, json, subprocess
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[5]
manifest = json.loads((HERE / "source-inputs-manifest.json").read_bytes())
def need(ok, message):
    if not ok:
        raise SystemExit("FAIL: " + message)
head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
need(head == manifest["commit"], "selected source HEAD changed")
need(subprocess.check_output(["git", "rev-parse", head + "^{tree}"], cwd=ROOT, text=True).strip() == manifest["tree"], "selected source tree changed")
need(len(manifest["files"]) == manifest["file_count"], "manifest inventory count")
for row in manifest["files"]:
    blob = subprocess.check_output(["git", "rev-parse", head + ":" + row["path"]], cwd=ROOT, text=True).strip()
    data = subprocess.check_output(["git", "show", head + ":" + row["path"]], cwd=ROOT)
    need(blob == row["git_blob"], "git blob mismatch: " + row["path"])
    need(len(data) == row["bytes"] and hashlib.sha256(data).hexdigest() == row["sha256"], "content mismatch: " + row["path"])
wheel = Path("/private/tmp/truss050-qualify/truss_toolkit-0.0.50-py3-none-any.whl")
need(wheel.is_file() and hashlib.sha256(wheel.read_bytes()).hexdigest() == "7b0483bb0c624c98515cf38ec30bbbf1ca43c1250e8880e5d00d520b4b7eec00", "official .50 wheel missing or hash mismatch")
old = ROOT / "docs/helix/04-build/evidence/current-upstream-release-20261023/truss049-r12-current-selected-source-lifecycle-20261024"
w = (old / "campaign_worker_r12.py").read_text(); s = (old / "run_supervisor_r12.py").read_text()
need("0.0.49" in w and "truss049" in s, "R12 .49 version binding not found")
need("e1001bd9e95c0fa555536b2c94c892b2b5b94cdb" in w and "e1001bd" in s, "R12 historical source binding not found")
metadata = Path("/private/tmp/ashlar-truss049-current-cohort-1f8c3fc2-r7/runtime/venv/lib/python3.12/site-packages/truss_toolkit-0.0.49.dist-info/METADATA")
need(metadata.is_file() and "Version: 0.0.49" in metadata.read_text(), "R12 runtime .49 metadata not found")
print(json.dumps({"profile":"ashlar-truss050-r13-effect-free-blocker-check/0.1", "outcome":"passed_blocker_confirmed", "source_commit":head, "source_tree":manifest["tree"], "source_file_count":manifest["file_count"], "wheel_sha256":"7b0483bb0c624c98515cf38ec30bbbf1ca43c1250e8880e5d00d520b4b7eec00", "r12_worker_supervisor_bound_to":"0.0.49 and historical e1001bd source", "r12_runtime_installed_truss":"0.0.49", "native_actions":False}, indent=2))
