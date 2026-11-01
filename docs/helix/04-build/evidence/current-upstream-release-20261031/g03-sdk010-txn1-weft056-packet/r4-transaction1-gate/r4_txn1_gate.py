#!/usr/bin/env python3
"""Effect-free readiness gate for SDK010 transaction 1; never dispatches Spark."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

PACKET = Path(__file__).resolve().parent
PARENT = PACKET.parent
REPO = PACKET.parents[6]
MANIFEST = PACKET / "packet-manifest.json"


def require(ok, message):
    if not ok:
        raise ValueError(message)


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=False, allow_nan=False) + "\n").encode()


def digest_bytes(raw):
    return hashlib.sha256(raw).hexdigest()


def digest_file(path):
    return digest_bytes(Path(path).read_bytes())


def selected():
    data = json.loads(MANIFEST.read_bytes())
    require(data.get("profile") == "ashlar-g03-sdk010-transaction1-release-gate/0.1",
            "transaction-1 gate manifest profile required")
    return data


def packet_digest():
    rows = []
    for path in sorted(PACKET.rglob("*")):
        if not path.is_file() or path.name == MANIFEST.name or "__pycache__" in path.parts:
            continue
        rows.append({"path": path.relative_to(PACKET).as_posix(),
                     "bytes": path.stat().st_size, "sha256": digest_file(path)})
    timing = PARENT / "r4_timing.py"
    rows.append({"path": "../r4_timing.py", "bytes": timing.stat().st_size,
                 "sha256": digest_file(timing)})
    return digest_bytes(canonical(rows))


def source_tree_preflight(manifest):
    git = manifest["gitExecutable"]
    head = subprocess.run([git, "-C", str(REPO), "rev-parse", "HEAD"], check=True,
                          capture_output=True, text=True, timeout=10).stdout.strip()
    subprocess.run([git, "-C", str(REPO), "merge-base", "--is-ancestor",
                    manifest["sourceCommit"], head], check=True, timeout=10)
    tree = subprocess.run([git, "-C", str(REPO), "rev-parse",
                           manifest["sourceCommit"] + ":src"], check=True,
                          capture_output=True, text=True, timeout=10).stdout.strip()
    require(tree == manifest["sourceTreeObject"], "admitted source tree object differs")
    changed = subprocess.run([git, "-C", str(REPO), "diff", "--quiet",
                              manifest["sourceCommit"], "HEAD", "--", "src"],
                             timeout=10).returncode
    status = subprocess.run([git, "-C", str(REPO), "status", "--porcelain",
                             "--untracked-files=all", "--", "src"], check=True,
                            capture_output=True, text=True, timeout=10).stdout
    require(changed == 0 and not status.strip(), "working src differs or is dirty")
    return {"head": head, "sourceTreeObject": tree}


def readiness(init_result_present, realized_vector_present, compiler_artifacts_present):
    missing = []
    if not init_result_present:
        missing.append("fresh SDK010 six-role initialization result")
    consumer_gaps = []
    if not realized_vector_present:
        consumer_gaps.append("publication-bound realized vector; none may be synthesized")
    if not compiler_artifacts_present:
        consumer_gaps.append("exact original .4-bound Weft 0.4.56 requests and responses")
    return {"transaction1PrerequisitesMet": not missing,
            "transaction1BlockedReasons": missing,
            "downstreamConsumerGaps": consumer_gaps,
            "transaction1DispatchAllowed": False}


def classify_outcome(dispatch_written, completion_verified):
    if not dispatch_written:
        return "refused-before-dispatch"
    if completion_verified:
        return "completed-verified"
    return "uncertain-after-dispatch"


def verify_supervisor_closure(result_raw, status_path, required_profile):
    require(status_path.is_file(), "R4 supervisor execution status missing")
    status_raw = status_path.read_bytes()
    status = json.loads(status_raw)
    require(status.get("profile") == required_profile
            and status.get("classification") == "completed"
            and status.get("processGroupEmpty") is True
            and status.get("resultSha256") == digest_bytes(result_raw)
            and status.get("effectsPossible") is True,
            "R4 supervisor did not verify successful process-group closure for this result")
    return status


def check_weft(manifest):
    pin = manifest["weft056"]
    wheel = Path(pin["wheelPath"])
    require(wheel.is_file() and digest_file(wheel) == pin["wheelSha256"],
            "pinned Weft 0.4.56 wheel unavailable or changed")
    probe = "import importlib.metadata as m,json; d=m.distribution('umf-weft'); print(json.dumps({'version':d.version,'direct_url':d.read_text('direct_url.json')}))"
    got = json.loads(subprocess.run([manifest["runtime"]["python"], "-c", probe], check=True,
                                    capture_output=True, text=True, timeout=15).stdout)
    direct = json.loads(got["direct_url"])
    archive_hash = direct.get("archive_info", {}).get("hashes", {}).get("sha256")
    if archive_hash is None:
        archive_hash = direct.get("archive_info", {}).get("hash", "").removeprefix("sha256=")
    require(got["version"] == pin["version"] and archive_hash == pin["wheelSha256"],
            "installed Weft identity does not bind the pinned wheel")
    record_hash = digest_bytes(got["direct_url"].encode())
    require(record_hash == pin["directUrlRecordSha256"],
            "installed Weft direct-url receipt differs")
    return {"version": got["version"], "wheelSha256": digest_file(wheel),
            "directUrlRecordSha256": record_hash}


def verify_init_receipt(manifest):
    ref = manifest["r4Initialization"]
    prepared_copy = PARENT / ref["preparedReceiptPath"]
    slot_copy = PARENT / ref["processSlotReceiptPath"]
    require(digest_file(prepared_copy) == ref["preparedReceiptSha256"],
            "R4 prepared receipt copy changed")
    require(digest_file(slot_copy) == ref["processSlotReceiptSha256"],
            "R4 process-slot receipt copy changed")
    attempt = Path(ref["attemptRoot"])
    prepared = attempt / "prepared.json"
    slot = attempt / "process-slot.json"
    require(digest_file(prepared) == ref["preparedReceiptSha256"]
            and digest_file(slot) == ref["processSlotReceiptSha256"],
            "R4 attempt receipts differ from reviewed copies")
    result_path = attempt / ref["requiredResult"]
    if not result_path.is_file():
        return {"present": False, "resultPath": str(result_path)}
    result_raw = result_path.read_bytes()
    result = json.loads(result_raw)
    status = verify_supervisor_closure(result_raw, attempt / "execution-status.json",
                                       ref["requiredExecutionStatusProfile"])
    require(result.get("profile") == ref["requiredResultProfile"]
            and result.get("packetTreeSha256") == ref["packetTreeSha256"]
            and result.get("preparedSha256") == ref["preparedReceiptSha256"]
            and result.get("sourcePolicyRevision") == ref["requiredSourcePolicyRevision"],
            "R4 initialization result identity differs")
    require(len(result.get("roleTargets", [])) == 6
            and len({row.get("uuid") for row in result["roleTargets"]}) == 6
            and {Path(row.get("path", "")).name for row in result["roleTargets"]}
                == set(ref["requiredRoleNames"]),
            "R4 initialization result lacks six unique role targets")
    installation = attempt / "installation"
    registry_path = installation / "installation.json"
    carrier_path = installation / "carrier-profile.json"
    runtime_report = attempt / "initialize-output/runtime-report.json"
    require(digest_file(registry_path) == result.get("installationSha256")
            and digest_file(carrier_path) == result.get("carrierProfileSha256")
            and digest_file(runtime_report) == result.get("runtimeReportSha256"),
            "R4 original installation registry/carrier/runtime report digest differs")
    registry = json.loads(registry_path.read_bytes())
    carrier = json.loads(carrier_path.read_bytes())
    require(registry.get("installation_id") == result.get("installationId")
            and registry.get("physical_profile") == ref["requiredCarrierProfile"]
            and registry.get("carrier_profile_sha256") == result.get("carrierProfileSha256")
            and registry.get("targets") == result["roleTargets"]
            and carrier.get("profile") == ref["requiredInstallationProfile"]
            and carrier.get("source_policy_revision") == ref["requiredSourcePolicyRevision"],
            "R4 installation registry or carrier profile differs")
    return {"present": True, "resultPath": str(result_path),
            "resultSha256": digest_bytes(result_raw),
            "executionStatusSha256": digest_file(attempt / "execution-status.json"),
            "processGroupEmpty": status["processGroupEmpty"], "result": result}


def inspect():
    manifest = selected()
    src = source_tree_preflight(manifest)
    for name, expected in manifest["inputs"].items():
        require(digest_file(PACKET / "inputs" / name) == expected,
                "frozen input digest differs: " + name)
    r4 = verify_init_receipt(manifest)
    weft = check_weft(manifest)
    vector = json.loads((PACKET / manifest["publicationVector"]["template"]).read_bytes())
    require(digest_file(PACKET / manifest["publicationVector"]["template"])
            == manifest["publicationVector"]["templateSha256"],
            "publication vector template digest differs")
    require(vector.get("state") == "immutable-null-template"
            and vector.get("publicationId") is None
            and vector.get("manifestUuid") is None
            and all(row.get("uuid") is None and row.get("version") is None
                    for row in vector["dataRoles"].values()),
            "publication vector template is not the frozen null template")
    query_scope = json.loads((PACKET / "inputs/query-scope.json").read_bytes())
    exact_artifacts_present = all(q.get("requestSha256") and q.get("responseSha256")
                                  for q in query_scope["queries"])
    ready = readiness(r4["present"], False, exact_artifacts_present)
    # Import the R4 verifier only after this gate's source tree check has passed.
    sys.path.insert(0, str(PARENT / "r4-sdk010-initialize-only"))
    import r4_packet
    r4_admission = r4_packet.verify_effect_free()
    require(r4_admission["packetTreeSha256"] == manifest["r4Initialization"]["packetTreeSha256"],
            "R4 SDK010 initialization packet identity differs")
    return {"profile": "ashlar-g03-sdk010-txn1-effect-free-gate-result/0.1",
            "packetTreeSha256": packet_digest(), "source": src,
            "weft056": weft, "initialization": {k: v for k, v in r4.items() if k != "result"},
            "sdk010SourceAdmissionSha256": r4_admission["sourceAdmissionReceiptSha256"],
            "publicationVectorTemplateSha256": digest_file(PACKET / "inputs/publication-vector.json"),
            "realizedPublicationVector": None,
            "compilerArtifactsPresent": exact_artifacts_present,
            **ready, "nativeEffects": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect",))
    args = parser.parse_args()
    result = inspect()
    print(canonical(result).decode(), end="")


if __name__ == "__main__":
    main()
