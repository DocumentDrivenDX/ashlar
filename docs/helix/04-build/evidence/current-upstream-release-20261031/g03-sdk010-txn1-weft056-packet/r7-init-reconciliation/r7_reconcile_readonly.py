#!/usr/bin/env python3
"""Read-only reconciliation of a retained R7 initialization attempt."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path

PACKET = Path(__file__).resolve().parents[1] / "r7-sdk010-initialize-only"
MANIFEST_PATH = PACKET / "packet-manifest.json"
ROLES = ("object_current", "edge_current", "tombstone", "whole_source_history", "attempts", "manifest")

class VerificationError(ValueError):
    pass

def require(condition, message):
    if not condition:
        raise VerificationError(message)

def raw(path: Path) -> bytes:
    require(path.is_file() and not path.is_symlink(), f"required regular file missing: {path}")
    return path.read_bytes()

def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def file_sha(path: Path) -> str:
    return sha(raw(path))

def load(path: Path):
    return json.loads(raw(path))

def verify_policy_bindings(manifest, registry, carrier, runtime_report, carrier_digest):
    """Check each policy field against its own contract owner."""
    expected_umf_revision = manifest["sourcePolicyRevision"]
    install = runtime_report["installation"]
    scope = install["scope"]
    require(carrier.get("profile") == manifest["installationProfile"], "carrier profile differs")
    require(carrier.get("physical_profile") == manifest["carrierProfile"], "carrier physical profile differs")
    require(carrier.get("source_policy_revision") == "ashlar-local-current-delta4-sdk010-source-policy/0.1",
            "carrier adapter source-policy revision differs")
    require(carrier.get("umf_revision") == "0.10.0", "carrier UMF version differs")
    require(carrier.get("umf_archive_sha256") == manifest["sdk010"]["archiveSha256"],
            "carrier SDK010 archive digest differs")
    require(carrier.get("umf_realization_profile") == "ashlar-local-current-delta4-umf-source-sdk010/0.1",
            "carrier UMF realization profile differs")
    require(registry.get("physical_profile") == manifest["carrierProfile"]
            and registry.get("carrier_profile_sha256") == carrier_digest,
            "installation registry carrier binding differs")
    require(scope.get("typed_graph_policy_revision") == expected_umf_revision,
            "runtime source admission UMF policy revision differs")
    require(install.get("scope") == registry.get("scope"), "runtime report scope differs from registry")
    require(runtime_report.get("profile") == "ashlar-configured-local-initialized/0.3"
            and runtime_report.get("operation") == "initialize"
            and runtime_report.get("published") is False
            and runtime_report.get("protected_ack") is False,
            "runtime report is not a non-publishing initialize result")

def log_actions(path):
    rows = [json.loads(line) for line in raw(path).splitlines() if line.strip()]
    return rows

def verify_delta_role(attempt, registry, carrier, role, installation_id, registry_digest, journal_digest):
    targets = [row for row in registry["targets"] if Path(row["path"]).name == role]
    require(len(targets) == 1, f"registry role target is not unique: {role}")
    target = targets[0]
    role_def = carrier["roles"].get(role)
    require(role_def is not None, f"carrier role missing: {role}")
    role_path = attempt / "installation" / role
    require(Path(target["path"]) == role_path and Path(role_def["path"]) == role_path,
            f"role path differs from attempt custody: {role}")
    require(target.get("uuid") == role_def.get("uuid")
            and target.get("table") == role_def.get("table"), f"role registry/carrier identity differs: {role}")
    delta = role_path / "_delta_log"
    v0_path, v1_path = delta / "00000000000000000000.json", delta / "00000000000000000001.json"
    require(sorted(p.name for p in delta.glob("[0-9]*.json")) == [v0_path.name, v1_path.name],
            f"role Delta head is not exactly v1: {role}")
    v0, v1 = log_actions(v0_path), log_actions(v1_path)
    require([next(iter(x)) for x in v0] == ["commitInfo", "metaData", "protocol"]
            or ([next(iter(x)) for x in v0] == ["commitInfo", "metaData", "protocol", "domainMetadata"]),
            f"unexpected role v0 actions: {role}")
    require([next(iter(x)) for x in v1] == ["commitInfo", "metaData"], f"unexpected role v1 actions: {role}")
    c0, m0, protocol = v0[0]["commitInfo"], v0[1]["metaData"], v0[2]["protocol"]
    c1, m1 = v1[0]["commitInfo"], v1[1]["metaData"]
    require(c0.get("operation") == "CREATE TABLE" and c1.get("operation") == "SET TBLPROPERTIES",
            f"role commit sequence differs: {role}")
    require(m0.get("id") == target["uuid"] == m1.get("id")
            and m0.get("schemaString") == m1.get("schemaString")
            and m0.get("partitionColumns") == m1.get("partitionColumns"),
            f"role metadata changed across initialization commits: {role}")
    require(json.loads(m0["schemaString"]) == json.loads(role_def["umf_proposal"]["schemaJson"]),
            f"role Delta schema differs from carrier proposal: {role}")
    require(protocol.get("minReaderVersion") == role_def["protocol"]["reader_version"]
            and protocol.get("minWriterVersion") == role_def["protocol"]["writer_version"],
            f"role protocol differs from carrier profile: {role}")
    expected_layout = {}
    for disposition in role_def.get("umf_dispositions", []):
        pointer = disposition.get("sourcePointer", "")
        if pointer.startswith("/extensions/umf.delta.definition/properties/") and disposition.get("outcome") == "sql":
            key = pointer.rsplit("/", 1)[1]
            expected_layout[key] = disposition["authoredValue"]
    require(m0.get("configuration", {}) == expected_layout,
            f"role initial Delta properties differ from admitted UMF SQL dispositions: {role}")
    expected_props = {"ashlar.local.installation.id": installation_id,
                      "ashlar.local.registry.sha256": registry_digest,
                      "ashlar.local.journal.sha256": journal_digest}
    require(all(m1.get("configuration", {}).get(k) == val for k, val in expected_props.items()),
            f"role v1 binding properties differ: {role}")
    user_meta = json.loads(c1.get("userMetadata", "{}"))
    require(user_meta == {"installation_id": installation_id, "journal_sha256": journal_digest,
        "profile": "ashlar-local-delta-installation-commit/0.1",
        "registry_sha256": registry_digest, "uuid": target["uuid"]},
        f"role v1 commit custody differs: {role}")
    return {"role": role, "table": target["table"], "uuid": target["uuid"],
            "headVersion": 1, "version0Sha256": file_sha(v0_path), "version1Sha256": file_sha(v1_path)}

def reconcile(attempt: Path, manifest=None):
    attempt = attempt.resolve(strict=True)
    manifest = manifest or load(MANIFEST_PATH)
    require(attempt.name.startswith("ashlar-g03-sdk010-init-"), "attempt name is outside R7 namespace")
    prepared_path, slot_path = attempt / "prepared.json", attempt / "process-slot.json"
    permit_path, dispatch_path = attempt / "release-authorized-permit.json", attempt / "dispatch.json"
    status_path, cleanup_path = attempt / "execution-status.json", attempt / "process-group-cleanup.json"
    prepared, slot, permit, dispatch = map(load, (prepared_path, slot_path, permit_path, dispatch_path))
    status, cleanup = load(status_path), load(cleanup_path)
    require(prepared.get("profile") == "ashlar-g03-sdk010-r7-init-prepared/0.1"
            and prepared.get("attemptRoot") == str(attempt)
            and prepared.get("packetTreeSha256") == "fdcaf2cd958bcf1f724831870674bbc11475795bcfd287186017faa50e00b605"
            and prepared.get("manifestSha256") == file_sha(PACKET / "packet-manifest.json"),
            "prepared receipt is not bound to reviewed R7 packet")
    require(slot.get("profile") == "ashlar-g03-r7-process-slot/0.1" and slot.get("empty") is True,
            "prepared process slot was not empty")
    expected_permit = {"profile": "ashlar-g03-sdk010-r7-init-permit/0.1", "execute": True,
        "packetTreeSha256": prepared["packetTreeSha256"], "manifestSha256": prepared["manifestSha256"],
        "preparedSha256": file_sha(prepared_path), "processSlotSha256": file_sha(slot_path),
        "sourceAdmissionPreflightSha256": prepared["sourceAdmissionPreflightSha256"],
        "currentCommit": prepared["currentCommit"]}
    require(permit == expected_permit and dispatch.get("permitSha256") == file_sha(permit_path)
            and dispatch.get("packetTreeSha256") == prepared["packetTreeSha256"]
            and dispatch.get("noRetry") is True, "permit/dispatch custody differs")
    preflight_path = attempt / "source-admission-preflight.json"
    preflight = load(preflight_path)
    require(file_sha(preflight_path) == prepared["sourceAdmissionPreflightSha256"]
            and preflight.get("nativeEffects") is False and preflight.get("sparkStarted") is False
            and preflight.get("processGroupCleanup", {}).get("empty") is True,
            "prepared source admission preflight receipt differs")
    require(status.get("profile") == "ashlar-g03-sdk010-init-execution-status/0.1"
            and status.get("classification") == "uncertain-after-dispatch"
            and status.get("effectsPossible") is True
            and status.get("errorType") == "ValueError", "original execution status changed")
    stderr_text = raw(attempt / "initialize.stderr.log").decode("utf-8", errors="replace")
    require("ValueError: exact SDK010 six-role installation profile required" in stderr_text,
            "retained worker trace does not show the post-initialize profile predicate failure")
    timings = load(attempt / "r7-timing-receipt.json")
    timing_events = {row.get("stage"): row for row in timings.get("events", [])}
    require(timing_events.get("runtime.initialize", {}).get("outcome") == "ok"
            and timing_events.get("source.open_sdk010", {}).get("outcome") == "ok",
            "runtime initialize or SDK010 source admission did not complete")
    require(cleanup.get("empty") is True and cleanup.get("inventoryVerified") is True
            and cleanup.get("remaining") == [], "Spark process group closure is not proven")
    require(not (attempt / "init-result.json").exists(), "R7 worker unexpectedly wrote its result receipt")
    invocation = load(attempt / "initialize-invocation.json")
    require(file_sha(attempt / "initialize-invocation.json") == prepared["invocationSha256"]
            and invocation.get("operation") == "initialize", "invocation custody differs")
    installation = attempt / "installation"
    registry_path, carrier_path = installation / "installation.json", installation / "carrier-profile.json"
    report_path = attempt / "initialize-output/runtime-report.json"
    registry, carrier, report = load(registry_path), load(carrier_path), load(report_path)
    require(report.get("installation_sha256") == file_sha(registry_path)
            and registry.get("carrier_profile_sha256") == file_sha(carrier_path),
            "runtime report, registry, and carrier hashes do not cross-bind")
    require(report.get("installation_id") == registry.get("scope", {}).get("installation_id")
            and report.get("installation_id") == report.get("installation", {}).get("scope", {}).get("installation_id"),
            "installation ID differs across receipts")
    require(report["installation"]["scope"].get("source_sha256") == manifest["inputs"]["typed-graph-source.jsonl"]
            and report["installation"]["scope"].get("configuration_sha256") == manifest["inputs"]["configured-typed-graph-source.json"]
            and report["installation"]["scope"].get("model_sha256") == manifest["inputs"]["typed-graph-model.umf.json"],
            "runtime report source/config/model custody differs")
    verify_policy_bindings(manifest, registry, carrier, report, file_sha(carrier_path))
    expected_runtime = {"profile": "ashlar-configured-local-runtime/0.2", "pyspark": manifest["runtime"]["pyspark"],
        "delta_spark": manifest["runtime"]["deltaSpark"], "python": "3.12", "jdk": "21",
        "jars": {name: item["sha256"] for name, item in manifest["runtime"]["jars"].items()},
        "sessionSettings": {"spark.sql.legacy.parameterSubstitution.constantsOnly": "true"}}
    require(registry.get("runtime_identity") == expected_runtime
            and report["installation"].get("runtime_identity") == expected_runtime,
            "runtime identity differs from pinned R7 runtime")
    require(registry.get("profile") == "ashlar-configured-local-installation/0.3"
            and len(registry.get("targets", [])) == 6
            and len({r.get("uuid") for r in registry["targets"]}) == 6
            and {Path(r["path"]).name for r in registry["targets"]} == set(ROLES)
            and set(carrier.get("roles", {})) == set(ROLES), "registry/carrier does not have exact six roles")
    reservation_paths = [installation / role / ".ashlar-local-installation-reservation.json" for role in ROLES]
    reservations = [load(path) for path in reservation_paths]
    require(all(item.get("profile") == "ashlar-private-native-installation-reservation/0.1"
                and item.get("registry") == reservations[0].get("registry")
                and item.get("original_journal_path") == str((installation / "operations.sqlite").resolve())
                for item in reservations), "role installation reservations differ")
    reservation = reservations[0]
    reserved = reservation["registry"]
    require(reserved.get("installation") == report["installation_id"]
            and reserved.get("installation_profile") == carrier
            and sorted(reserved.get("targets", []), key=lambda row: row["path"])
                == sorted(registry.get("targets", []), key=lambda row: row["path"]),
            "private installation reservation differs from carrier/registry")
    registry_digest = sha(json.dumps(reserved, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode())
    journal_digest = sha(str((installation / "operations.sqlite").resolve()).encode())
    role_heads = [verify_delta_role(attempt, registry, carrier, role,
        report["installation_id"], registry_digest, journal_digest) for role in ROLES]
    require(report.get("profile") == "ashlar-configured-local-initialized/0.3"
            and report.get("operation") == "initialize" and report.get("published") is False
            and report.get("protected_ack") is False, "runtime outcome is not initialize-only")
    return {"profile": "ashlar-g03-r7-readonly-init-reconciliation/0.1",
        "attemptRoot": str(attempt), "packetTreeSha256": prepared["packetTreeSha256"],
        "originalExecutionStatus": "uncertain-after-dispatch",
        "classification": "runtime-initialization-completed; worker-receipt-verification-failed-after-effects",
        "workerFailure": {"type": status.get("errorType"),
            "stderrSha256": file_sha(attempt/"initialize.stderr.log"),
            "stdoutSha256": file_sha(attempt/"initialize.stdout.log"),
            "missingInitResult": True},
        "receiptSha256": {name: file_sha(attempt / path) for name, path in {
            "prepared": "prepared.json", "preparedSlot": "process-slot.json",
            "permit": "release-authorized-permit.json", "dispatch": "dispatch.json",
            "dispatchSlot": "dispatch-process-slot.json", "executionStatus": "execution-status.json",
            "processGroupCleanup": "process-group-cleanup.json",
            "sourceAdmissionPreflight": "source-admission-preflight.json",
            "timing": "r7-timing-receipt.json", "invocation": "initialize-invocation.json",
            "registry": "installation/installation.json", "carrier": "installation/carrier-profile.json",
            "runtimeReport": "initialize-output/runtime-report.json"}.items()},
        "processGroupEmpty": cleanup["empty"], "installationId": report["installation_id"],
        "installationSha256": file_sha(registry_path), "carrierProfileSha256": file_sha(carrier_path),
        "runtimeReportSha256": file_sha(report_path), "registryDigest": registry_digest,
        "journalSha256": journal_digest,
        "timingMs": {"source.open_sdk010": timing_events["source.open_sdk010"]["durationMs"],
                     "runtime.initialize": timing_events["runtime.initialize"]["durationMs"]},
        "runtimePolicyRevision": report["installation"]["scope"]["typed_graph_policy_revision"],
        "sourcePolicyRevision": report["installation"]["scope"]["typed_graph_policy_revision"],
        "carrierAdapterSourcePolicyRevision": carrier["source_policy_revision"],
        "nativeInitializationCompleted": True, "roleHeads": role_heads,
        "sixRoleHeadVector": role_heads, "installationUsableForTransaction1": True,
        "publication": False, "ack": False,
        "transaction1Receipt": "read-only independent reconciliation; original worker init-result absent"}

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("attempt")
    parser.add_argument("--receipt")
    args=parser.parse_args()
    result=reconcile(Path(args.attempt))
    encoded=(json.dumps(result,sort_keys=True,separators=(",", ":"))+"\n").encode()
    if args.receipt:
        out=Path(args.receipt)
        require(not out.exists(), "reconciliation output must be a fresh path")
        out.write_bytes(encoded)
    print(encoded.decode(),end="")
if __name__=="__main__": main()
