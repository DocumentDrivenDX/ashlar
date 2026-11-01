#!/usr/bin/env python3
"""Effect-free admission checks for the SDK010 R7 transaction-one candidate."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess

PACKET = Path(__file__).resolve().parent
BASE = PACKET.parent
REPO = PACKET.parents[6]
MANIFEST_PATH = PACKET / "packet-manifest.json"
ROLES = {"object_current", "edge_current", "tombstone", "whole_source_history",
         "attempts", "manifest"}


def require(ok: bool, message: str) -> None:
    if not ok:
        raise ValueError(message)


def sha_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def sha_file(path: Path) -> str:
    return sha_bytes(path.read_bytes())


def canonical(value) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=False, allow_nan=False) + "\n").encode()


def packet_digest() -> str:
    rows = []
    for path in sorted(PACKET.rglob("*")):
        if not path.is_file() or path.name == MANIFEST_PATH.name or "__pycache__" in path.parts:
            continue
        rows.append({"path": path.relative_to(PACKET).as_posix(),
                     "bytes": path.stat().st_size, "sha256": sha_file(path)})
    timing = BASE / "r4_timing.py"
    rows.append({"path": "../r4_timing.py", "bytes": timing.stat().st_size,
                 "sha256": sha_file(timing)})
    return sha_bytes(canonical(rows))


def manifest() -> dict:
    value = json.loads(MANIFEST_PATH.read_bytes())
    require(value.get("profile") == "ashlar-g03-sdk010-r7-transaction1-candidate/0.1",
            "SDK010 R7 transaction-one manifest required")
    return value


def verify_source_tree(selected: dict) -> dict:
    git = selected["git"]
    head = subprocess.run([git, "-C", str(REPO), "rev-parse", "HEAD"], check=True,
                          capture_output=True, text=True, timeout=10).stdout.strip()
    subprocess.run([git, "-C", str(REPO), "merge-base", "--is-ancestor",
                    selected["sourceCommit"], head], check=True, timeout=10)
    tree = subprocess.run([git, "-C", str(REPO), "rev-parse",
                           selected["sourceCommit"] + ":src"], check=True,
                          capture_output=True, text=True, timeout=10).stdout.strip()
    require(tree == selected["sourceTreeObject"], "admitted Ashlar src tree differs")
    changed = subprocess.run([git, "-C", str(REPO), "diff", "--quiet",
                              selected["sourceCommit"], "HEAD", "--", "src"],
                             timeout=10).returncode
    status = subprocess.run([git, "-c", "core.fsmonitor=false", "-c",
                             "core.hooksPath=/dev/null", "-C", str(REPO), "status",
                             "--porcelain", "--untracked-files=all", "--", "src"], check=True,
                            capture_output=True, text=True, timeout=120,
                            env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"}).stdout
    require(changed == 0 and not status.strip(), "Ashlar src differs or is dirty")
    return {"head": head, "sourceTreeObject": tree}


def verify_sparse_producer(selected: dict) -> dict:
    pin = selected["producer"]
    env = {"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "HOME": "/nonexistent",
           "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
           "GIT_TERMINAL_PROMPT": "0", "GIT_OPTIONAL_LOCKS": "0"}
    def git_read(args):
        return subprocess.run([pin["git"], "-C", pin["source"], *args], check=True,
                              capture_output=True, text=True, timeout=10,
                              env=env).stdout.strip()
    commit = git_read(["rev-parse", "HEAD"])
    tree = git_read(["rev-parse", "HEAD^{tree}"])
    sparse = git_read(["sparse-checkout", "list"]).splitlines()
    link = Path(pin["source"]) / "node_modules"
    status = subprocess.run([pin["git"], "-c", "core.fsmonitor=false", "-c",
                             "core.hooksPath=/dev/null", "-C", pin["source"], "status",
                             "--porcelain", "--untracked-files=normal"], check=True,
                            capture_output=True, text=True, timeout=10, env=env).stdout
    require(commit == pin["sourceCommit"] and tree == pin["sourceTree"]
            and sparse == pin["sparseCheckout"]
            and git_read(["config", "--get", "index.sparse"]) == "true"
            and link.is_symlink()
            and str(link.resolve(strict=True)) == pin["nodeModulesTarget"]
            and not status.strip(), "exact sparse UMF SDK010 producer checkout required")
    return {"commit": commit, "tree": tree, "sparseCheckout": sparse,
            "sparseIndex": True, "clean": True}


def verify_source_and_runtime(selected: dict) -> dict:
    producer = verify_sparse_producer(selected)
    sdk = selected["sdk010"]
    require(sha_file(Path(sdk["sdkArchive"])) == sdk["archiveSha256"]
            and sha_file(Path(sdk["sdkPackageRoot"]) / "dist/umf.js")
                == sdk["publicEntrySha256"], "SDK 0.10.0 archive or public entry changed")
    runtime = selected["runtime"]
    py = Path(runtime["python"])
    require(py.is_file() and sha_file(py.resolve()) == runtime["pythonSha256"],
            "pinned Python runtime changed")
    for item in runtime["jars"].values():
        require(sha_file(Path(item["path"])) == item["sha256"], "pinned Delta JAR changed")
    java = Path(runtime["javaHome"]) / "bin/java"
    require(sha_file(java.resolve()) == runtime["javaExecutableSha256"],
            "pinned Java runtime changed")
    versions = json.loads(subprocess.run([str(py), "-c",
        "import json,pyspark,sys; from importlib.metadata import version; "
        "print(json.dumps({'python':sys.version.split()[0],'pyspark':pyspark.__version__,"
        "'deltaSpark':version('delta-spark'),'miniRacer':version('mini-racer')}))"], check=True, capture_output=True,
        text=True, timeout=15).stdout)
    require(versions == {"python": runtime["pythonVersion"], "pyspark": runtime["pyspark"],
                         "deltaSpark": runtime["deltaSpark"], "miniRacer": runtime["miniRacer"]},
            "pinned local runtime versions differ")
    return {"producer": producer, "runtimeVersions": versions,
            "sdkArchiveSha256": sdk["archiveSha256"],
            "sdkPublicEntrySha256": sdk["publicEntrySha256"]}


def verify_frozen_inputs(selected: dict) -> None:
    for name, digest in selected["inputs"].items():
        require(sha_file(PACKET / "inputs" / name) == digest,
                "frozen G03 input changed: " + name)
    oracle = json.loads((PACKET / "inputs/source-oracle.json").read_bytes())
    require(oracle.get("inputs", {}).get("publicationTransaction") == "typed-graph-1"
            and oracle.get("inputs", {}).get("publicationEventCount") == 8,
            "frozen transaction-one source oracle required")


def verify_reconciliation(selected: dict) -> dict:
    r7 = selected["r7"]
    attempt = Path(r7["attemptRoot"])
    receipt_path = BASE / r7["reconciliationReceipt"]
    require(receipt_path.is_file(), "read-only R7 reconciliation receipt missing")
    raw = receipt_path.read_bytes()
    if r7.get("reconciliationSha256") is not None:
        require(sha_bytes(raw) == r7["reconciliationSha256"],
                "read-only R7 reconciliation receipt digest differs")
    receipt = json.loads(raw)
    require(receipt.get("profile") == r7["reconciliationProfile"]
            and receipt.get("attemptRoot") == str(attempt)
            and receipt.get("classification") ==
                "runtime-initialization-completed; worker-receipt-verification-failed-after-effects"
            and receipt.get("originalExecutionStatus") == "uncertain-after-dispatch"
            and receipt.get("processGroupEmpty") is True
            and receipt.get("nativeInitializationCompleted") is True
            and receipt.get("installationUsableForTransaction1") is True
            and receipt.get("publication") is False and receipt.get("ack") is False
            and receipt.get("sourcePolicyRevision") == r7["runtimePolicyRevision"]
            and receipt.get("workerFailure", {}).get("missingInitResult") is True
            and not (attempt / "init-result.json").exists(),
            "R7 read-only reconciliation does not establish the recorded boundary")
    installation = attempt / "installation"
    registry_path = installation / "installation.json"
    carrier_path = installation / "carrier-profile.json"
    report_path = attempt / "initialize-output/runtime-report.json"
    require(sha_file(registry_path) == receipt.get("installationSha256")
            and sha_file(carrier_path) == receipt.get("carrierProfileSha256")
            and sha_file(report_path) == receipt.get("runtimeReportSha256"),
            "R7 installation/carrier/runtime-report digests differ from reconciliation")
    require(sha_file(attempt / "initialize-invocation.json")
            == receipt.get("receiptSha256", {}).get("invocation"),
            "R7 source/runtime invocation digest differs from reconciliation")
    retained_receipts = {
        "dispatch": "dispatch.json", "dispatchSlot": "dispatch-process-slot.json",
        "executionStatus": "execution-status.json", "permit": "release-authorized-permit.json",
        "prepared": "prepared.json", "preparedSlot": "process-slot.json",
        "processGroupCleanup": "process-group-cleanup.json",
        "sourceAdmissionPreflight": "source-admission-preflight.json",
        "timing": "r7-timing-receipt.json",
    }
    for receipt_key, filename in retained_receipts.items():
        require(sha_file(attempt / filename) == receipt.get("receiptSha256", {}).get(receipt_key),
                "R7 original attempt receipt digest differs: " + filename)
    status = json.loads((attempt / "execution-status.json").read_bytes())
    cleanup = json.loads((attempt / "process-group-cleanup.json").read_bytes())
    require(status.get("profile") == "ashlar-g03-sdk010-init-execution-status/0.1"
            and status.get("classification") == "uncertain-after-dispatch"
            and status.get("effectsPossible") is True
            and cleanup.get("empty") is True and cleanup.get("inventoryVerified") is True
            and cleanup.get("remaining") == [] and cleanup.get("terminatedSurvivors") == [],
            "R7 original uncertainty and worker-group cleanup evidence differ")
    registry, carrier, runtime_report = (
        json.loads(registry_path.read_bytes()), json.loads(carrier_path.read_bytes()),
        json.loads(report_path.read_bytes()))
    require(registry.get("profile") == "ashlar-configured-local-installation/0.3"
            and registry.get("scope", {}).get("installation_id") == receipt.get("installationId")
            and registry.get("physical_profile") == r7["carrierProfile"]
            and carrier.get("profile") == r7["installationProfile"]
            and carrier.get("physical_profile") == r7["carrierProfile"]
            and carrier.get("umf_realization_profile") ==
                "ashlar-local-current-delta4-umf-source-sdk010/0.1"
            and carrier.get("source_policy_revision") ==
                receipt.get("carrierAdapterSourcePolicyRevision")
            and receipt.get("runtimePolicyRevision") == r7["runtimePolicyRevision"]
            and runtime_report.get("profile") == "ashlar-configured-local-initialized/0.3"
            and runtime_report.get("operation") == "initialize"
            and runtime_report.get("published") is False
            and runtime_report.get("protected_ack") is False
            and runtime_report.get("installation_id") == receipt.get("installationId")
            and runtime_report.get("installation", {}).get("scope", {}).get(
                "typed_graph_policy_revision") == r7["runtimePolicyRevision"],
            "R7 SDK010 runtime and carrier policy identities differ")
    rows = receipt.get("sixRoleHeadVector", receipt.get("roleHeads"))
    require(type(rows) is list and len(rows) == 6
            and {row.get("role") for row in rows} == ROLES
            and {row.get("headVersion") for row in rows} == {1}
            and len({row.get("uuid") for row in rows}) == 6,
            "R7 reconciliation must bind six unique role UUIDs at actual v1 heads")
    targets = {Path(row["path"]).name: row for row in registry.get("targets", [])}
    require(len(targets) == 6, "R7 installation registry must contain six targets")
    for name, digest in selected["inputs"].items():
        require(sha_file(attempt / "inputs" / name) == digest,
                "retained R7 input differs from frozen transaction-one input: " + name)
    for row in rows:
        target = targets.get(row["role"])
        require(target is not None and target.get("uuid") == row.get("uuid")
                and target.get("table") == row.get("table")
                and type(row.get("version0Sha256")) is str
                and len(row["version0Sha256"]) == 64
                and type(row.get("version1Sha256")) is str
                and len(row["version1Sha256"]) == 64,
                "R7 reconciliation role UUID/table differs from original registry")
        delta_log = Path(target["path"]) / "_delta_log"
        for version in (0, 1):
            log_path = delta_log / (f"{version:020d}.json")
            expected_hash = row[f"version{version}Sha256"]
            require(log_path.is_file() and sha_file(log_path) == expected_hash,
                    "R7 retained Delta log hash differs: " + row["role"])
        versions = sorted(int(path.name[:20]) for path in delta_log.glob("[0-9]" * 20 + ".json"))
        require(versions == [0, 1],
                "fresh current R7 Delta head must still be v1 before transaction dispatch")
    return {"receiptSha256": sha_bytes(raw), "attemptRoot": str(attempt),
            "installationId": receipt["installationId"],
            "installationSha256": receipt["installationSha256"],
            "carrierProfileSha256": receipt["carrierProfileSha256"],
            "runtimeReportSha256": receipt["runtimeReportSha256"],
            "runtimePolicyRevision": receipt["runtimePolicyRevision"],
            "carrierAdapterSourcePolicyRevision": receipt["carrierAdapterSourcePolicyRevision"],
            "roleHeads": rows, "processGroupEmpty": True}


def classify_outcome(dispatch_written: bool, completion_verified: bool) -> str:
    if not dispatch_written:
        return "refused-before-dispatch"
    if completion_verified:
        return "completed-verified"
    return "uncertain-after-dispatch"


def inspect(*, complete_source_admission: bool = True) -> dict:
    selected = manifest()
    require(selected.get("publication") is False and selected.get("ack") is False,
            "transaction-one packet cannot authorize publication or ACK effects")
    require(selected.get("nativeTransactionEffectsEnabled") is
            selected.get("dispatchEnabled"),
            "native transaction effect flag must track the dispatch enablement")
    review_path = selected.get("solReviewReceiptPath")
    review_sha = selected.get("solReviewReceiptSha256")
    review_complete = False
    if selected.get("dispatchEnabled") is True:
        require(type(review_path) is str and type(review_sha) is str,
                "dispatch enablement requires a bound Sol review receipt")
        review_file = BASE / review_path
        require(review_path == "r7-transaction1-sol-review.json"
                and review_file.parent == BASE and review_file.is_file()
                and not review_file.is_symlink() and sha_file(review_file) == review_sha,
                "bound Sol review receipt is missing or changed")
        review = json.loads(review_file.read_bytes())
        require(set(review) == {"profile", "reviewStatus", "reviewedPacketTreeSha256",
                                "sourceCommit", "sourceTreeObject",
                                "r7ReconciliationSha256", "limits", "authorizedEffects",
                                "excludedEffects"}
                and review["profile"] == "ashlar-g03-sdk010-r7-txn1-sol-review/0.1"
                and review["reviewStatus"] == "accepted"
                and review["reviewedPacketTreeSha256"] == packet_digest()
                and review["sourceCommit"] == selected["sourceCommit"]
                and review["sourceTreeObject"] == selected["sourceTreeObject"]
                and review["r7ReconciliationSha256"] ==
                    selected["r7"]["reconciliationSha256"]
                and review["limits"] == selected["limits"]
                and review["authorizedEffects"] == ["native-transaction-one"]
                and review["excludedEffects"] == ["publication", "manifest",
                                                    "protected-ack", "cloud"],
                "Sol review receipt does not bind this exact packet and transaction scope")
        review_complete = True
    else:
        require(review_path is None and review_sha is None,
                "disabled review candidate cannot carry an incomplete Sol receipt")
    source = verify_source_tree(selected)
    verify_frozen_inputs(selected)
    runtime = verify_source_and_runtime(selected)
    timing = BASE / "r4_timing.py"
    require(sha_file(timing) == selected["timingHelperSha256"],
            "shared bounded timing helper changed")
    r7 = verify_reconciliation(selected)
    admission_sha = None
    if complete_source_admission:
        # Complete SDK010 source-to-Core admission is effect-free and runs
        # after the admitted Ashlar tree and exact producer/runtime pins.
        import sys
        sys.path.insert(0, str(REPO / "src"))
        from ashlar_host.typed_graph_sdk010_admission import (
            TypedGraphSdk010AdmissionConfig, admit_typed_graph_sdk010)
        from ashlar.typed_graph_source import build_typed_graph_request
        from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration
        config_path = PACKET / "inputs/configured-typed-graph-source.json"
        source_config = load_typed_graph_jsonl_configuration(config_path)
        sdk = selected["sdk010"]
        sdk_config = TypedGraphSdk010AdmissionConfig(Path(sdk["node"]),
            Path(sdk["sdkPackageRoot"]), Path(sdk["sdkArchive"]))
        admission = admit_typed_graph_sdk010(sdk_config, source_config,
            source_config.model_source, build_typed_graph_request(source_config))
        admission_sha = sha_bytes(admission)
    return {"profile": "ashlar-g03-sdk010-r7-transaction1-inspection/0.1",
            "packetTreeSha256": packet_digest(),
            "source": source, "assets": runtime, "r7Initialization": r7,
            "sdk010AdmissionSha256": admission_sha,
            "completeSourceAdmission": complete_source_admission,
            "transaction": "typed-graph-1", "carrierProfile": selected["r7"]["carrierProfile"],
            "dispatchEnabled": selected["dispatchEnabled"],
            "solReviewComplete": review_complete,
            "publication": False,
            "ack": False,
            "nativeTransactionEffectsEnabled": selected["nativeTransactionEffectsEnabled"],
            "packetDispatchGatesPassed": bool(
                selected["dispatchEnabled"] and review_complete
                and complete_source_admission and r7["processGroupEmpty"]),
            "transaction1DispatchAllowed": False,
            "dispatchAuthorizationRequires": ["prepared_attempt", "one_shot_permit",
                                               "empty_process_slot"],
            "downstreamPublicationVector": None}


if __name__ == "__main__":
    print(canonical(inspect()).decode(), end="")
