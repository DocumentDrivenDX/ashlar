#!/usr/bin/env python3
"""One call to the retained R3 `.4` manifest-only continuation API."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import subprocess


PACKET = Path(__file__).resolve().parent
REPO = Path("/private/tmp/ashlar-r3-manifest-continuation")
ATTEMPT = Path(
    "/private/tmp/ashlar-g03-sdk0101-lifecycle-r3-attempts-20261101/"
    "ashlar-g03-sdk0101-lifecycle-94c1991ea883463b88d938b126893d31")
INSTALL = ATTEMPT / "installation"
INVOCATION = PACKET / "invocation.json"
SOURCE_CODE_COMMIT = "ae07b9203591f309a0fee72d6740fa26d5da73bb"
REQUEST_DIGEST = "bebb23db9dee40dd09424cc071d4d80beb200f75b0c6487b5274b1d3d387fb77"
OUTPUT_RECEIPT_SHA256 = "eef82376bff8eafc281442aa8f7da2e424d0439ab942b5ccb0c5464c07f37b2c"
DATA_VERSION_VECTOR = {
    "local.configured.edge_current": 2,
    "local.configured.object_current": 2,
    "local.configured.tombstone": 1,
    "local.configured.whole_source_history": 2,
}
PRE_MANIFEST_HEADS = {
    "object_current": ("fae99e2d-2510-4c66-a463-c1b7111ea36f", 2),
    "edge_current": ("4db27b56-a042-442e-923c-c70a3734a308", 2),
    "tombstone": ("796a23fc-5030-4341-8115-1c5dd69daec7", 1),
    "whole_source_history": ("ac1749e9-2fe1-4e4f-bc6c-771c7e534ac3", 2),
    "publication_attempt_phase": ("54846b0f-2633-47c6-9984-958fcc51c523", 3),
    "publication_manifest": ("0796e532-30e7-4531-afed-93540b549458", 1),
}
POST_MANIFEST_HEADS = {**PRE_MANIFEST_HEADS,
                       "publication_manifest": ("0796e532-30e7-4531-afed-93540b549458", 2)}
PHYSICAL_ROLE_DIRS = {
    "object_current": "object_current", "edge_current": "edge_current",
    "tombstone": "tombstone", "whole_source_history": "whole_source_history",
    "publication_attempt_phase": "attempts", "publication_manifest": "manifest",
}
CARRIER_ROLE_BY_JOURNAL_ROLE = {
    "publication_attempt_phase": "attempts",
    "publication_manifest": "manifest",
}


def require(value: bool, message: str) -> None:
    if not value:
        raise RuntimeError(message)


def canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode()


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_selected_bytes(invocation: dict) -> None:
    source = invocation["source"]
    expected_files = {
        Path(source["configuration"]): source["configurationSha256"],
        ATTEMPT / "inputs/typed-graph-source.jsonl": source["sourceSha256"],
        ATTEMPT / "inputs/typed-graph-model.umf.json": source["modelSha256"],
        ATTEMPT / "inputs/typed-graph-binding.json": source["bindingSha256"],
        INSTALL / "installation.json": invocation["installationSha256"],
        INSTALL / "carrier-profile.json": invocation["carrierProfileSha256"],
        INSTALL / "operations.sqlite":
            invocation["priorContinuationRefusal"]["operationsSqliteSha256"],
        Path(invocation["priorR4ContinuationRefusal"]["attemptRoot"])
        / "worker.stderr.log": invocation["priorR4ContinuationRefusal"]["stderrSha256"],
        Path(invocation["priorR4ContinuationRefusal"]["attemptRoot"])
        / "execution-status.json":
            invocation["priorR4ContinuationRefusal"]["executionStatusSha256"],
    }
    for path, digest in expected_files.items():
        require(path.is_file() and not path.is_symlink() and sha(path) == digest,
                f"Selected input changed: {path.name}")
    module_hashes = {
        "src/ashlar_host/configured_local_runtime.py": invocation["sourceCode"]["runtimeSha256"],
        "src/ashlar_host/configured_local_delta4_publication.py": invocation["sourceCode"]["publicationSha256"],
        "src/ashlar_host/resources/typed_graph_values_sdk0101.ts":
            invocation["sourceCode"]["sdk0101CheckerSha256"],
        "src/ashlar_host/typed_graph_admission_sdk0101.py":
            invocation["sourceCode"]["sdk0101CustodySha256"],
    }
    for relative, digest in module_hashes.items():
        require(sha(REPO / relative) == digest, f"Selected source changed: {relative}")
    git = invocation["runtime"]["git"]
    tree = subprocess.run([git, "-C", source["producerSource"], "rev-parse", "HEAD^{tree}"],
                          check=True, capture_output=True, text=True, timeout=5).stdout.strip()
    commit = subprocess.run([git, "-C", source["producerSource"], "rev-parse", "HEAD"],
                            check=True, capture_output=True, text=True, timeout=5).stdout.strip()
    dirty = subprocess.run([git, "-C", source["producerSource"], "status", "--porcelain"],
                           check=True, capture_output=True, text=True, timeout=5).stdout
    require((commit, tree, dirty) == (source["producerCommit"], source["producerTree"], ""),
            "Selected exact clean SDK producer source required")


def read_invocation(path: Path) -> tuple[dict, str]:
    require(path.is_file() and not path.is_symlink(), "Regular invocation file required")
    raw = path.read_bytes()
    require(len(raw) <= 65536, "Invocation byte bound exceeded")
    value = json.loads(raw)
    require(type(value) is dict and value.get("profile") ==
            "ashlar-g03-r3-manifest-only-continuation-invocation/0.1"
            and value.get("dispatchEnabled") is True,
            "Exact dispatchable manifest continuation invocation required")
    require(value.get("installation") == str(INSTALL)
            and value.get("installationId") == "g03-sdk0101-a883463b88d938b126893d31"
            and value.get("installationSha256") ==
                "76ae97288b29564b56b6b1d45c1559a011f0269ad2f1656d60e1dbecd0863e58",
            "Original R3 installation custody required")
    require(value.get("sourceCode", {}).get("continuationCommit") == SOURCE_CODE_COMMIT,
            "Reviewed manifest-only continuation source commit required")
    effects = value.get("operation", {}).get("effects")
    require(value.get("operation", {}).get("expectedGraphPrepareApplyRecoverCalls") == 0
            and type(effects) is dict and effects.get("sourceAck") is False
            and effects.get("publicationArtifact") is False
            and effects.get("cloud") is False and effects.get("truss") is False,
            "Manifest-only effect envelope required")
    limits = value.get("bounds", {})
    require(limits.get("wallSeconds") == 900
            and limits.get("rssBytes") == 2 * 1024**3,
            "Exact process and cost bounds required")
    require(value["operation"].get("requestDigest") == REQUEST_DIGEST
            and value["operation"].get("outputReceiptSha256") == OUTPUT_RECEIPT_SHA256,
            "Exact retained transaction-one custody required")
    refusal = value.get("priorContinuationRefusal", {})
    require(refusal.get("elapsedMs") == 4455
            and refusal.get("classification") == "uncertain-after-dispatch"
            and refusal.get("processGroupEmpty") is True
            and refusal.get("nativeApiCalled") is False
            and refusal.get("operationsSqliteSha256") ==
                "10754c3d2b2e947b16b0770f5af13f90353925728a553290314fba403c00c016"
            and refusal.get("journalCounts") == {
                "committedGraphOperations": 3,
                "committedMetadataOperations": 2,
                "outputReceipts": 1,
                "publicationArtifacts": 0,
                "walFiles": 0,
            },
            "Exact preserved pre-call refusal reconciliation required")
    refusal_r4 = value.get("priorR4ContinuationRefusal", {})
    require(refusal_r4.get("elapsedMs") == 139348
            and refusal_r4.get("classification") == "uncertain-after-dispatch"
            and refusal_r4.get("processGroupEmpty") is True
            and refusal_r4.get("nativeApiCallStarted") is True
            and refusal_r4.get("manifestOperationAdded") is False
            and refusal_r4.get("manifestHeadAdvanced") is False
            and refusal_r4.get("headsUnchanged") is True
            and refusal_r4.get("operationsSqliteSha256") ==
                "10754c3d2b2e947b16b0770f5af13f90353925728a553290314fba403c00c016"
            and refusal_r4.get("stderrSha256") ==
                "8c3c9040765ef4d76f4783a791e10402778d3fbc56673aed90ff2d28c91cece6"
            and refusal_r4.get("executionStatusSha256") ==
                "71487ee93980bab4712e0b9ecb94a3846079094b37de93417e2185f9071d7899"
            and refusal_r4.get("journalCounts") == {
                "committedGraphOperations": 3,
                "committedMetadataOperations": 2,
                "outputReceipts": 1,
                "publicationArtifacts": 0,
                "walFiles": 0,
            },
            "Exact preserved R4 refusal reconciliation required")
    digest = hashlib.sha256(raw).hexdigest()
    return value, digest


def db_rows(db: sqlite3.Connection, query: str) -> list:
    return [list(row) for row in db.execute(query).fetchall()]


def journal_snapshot(*, immutable: bool) -> dict:
    suffix = "&immutable=1" if immutable else ""
    uri = f"file:{INSTALL / 'operations.sqlite'}?mode=ro{suffix}"
    db = sqlite3.connect(uri, uri=True)
    try:
        tables = {row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        required = {"local_source_plan", "local_plan", "local_operation",
                    "local_delta4_metadata_operation", "local_delta4_output_receipt",
                    "local_publication_artifact"}
        require(required <= tables, "Retained SQLite schema changed")
        return {
            "source": db_rows(db, "SELECT request_digest,original FROM local_source_plan"),
            "plan": db_rows(db, "SELECT operation,original,digest FROM local_plan"),
            "operations": db_rows(db, "SELECT operation,intent,intent_sha,before_version,state,receipt "
                                  "FROM local_operation ORDER BY operation"),
            "metadata": db_rows(db, "SELECT operation,lane,intent,intent_sha,before_version,state,receipt "
                                "FROM local_delta4_metadata_operation ORDER BY before_version"),
            "receipt": db_rows(db, "SELECT request_digest,original,receipt_sha256 "
                                "FROM local_delta4_output_receipt"),
            "artifacts": db_rows(db, "SELECT request_digest,request,artifact "
                                 "FROM local_publication_artifact"),
        }
    finally:
        db.close()


def _validate_initial_journal(snapshot: dict) -> None:
    require(len(snapshot["source"]) == 1
            and snapshot["source"][0][0] == REQUEST_DIGEST,
            "Exact original source plan required")
    require(len(snapshot["plan"]) == 1
            and snapshot["plan"][0][0] ==
                "effects:1b6684afdb17d315294c8cfa2dc61d3fe031016621b688e2af44167fb6dcec07"
            and snapshot["plan"][0][2] ==
                "69c633798f8ad2d439df53329739f4e6d8c90aea34b809dfc67336d3c70228d1",
            "Exact original graph plan required")
    require(len(snapshot["operations"]) == 3
            and all(row[4] == "committed" and type(row[5]) is str
                    for row in snapshot["operations"]),
            "Exactly three committed graph operation receipts required")
    require(len(snapshot["metadata"]) == 2
            and all(row[1] == "publication_attempt_phase" and row[5] == "committed"
                    for row in snapshot["metadata"])
            and [row[4] for row in snapshot["metadata"]] == [1, 2],
            "Exact prepared/applying metadata prefix required")
    require(len(snapshot["receipt"]) == 1
            and snapshot["receipt"][0][0] == REQUEST_DIGEST
            and snapshot["receipt"][0][2] == OUTPUT_RECEIPT_SHA256
            and snapshot["artifacts"] == [],
            "Exact output receipt and no publication artifact required")


def delta_heads() -> dict[str, tuple[str, int]]:
    result = {}
    for role, directory in PHYSICAL_ROLE_DIRS.items():
        logs = sorted((INSTALL / directory / "_delta_log").glob("[0-9]" * 20 + ".json"))
        require(logs, f"No Delta log for {role}")
        versions = [int(path.name[:20]) for path in logs]
        require(versions == list(range(versions[-1] + 1)),
                f"Noncontiguous Delta log versions for {role}")
        carrier_role = CARRIER_ROLE_BY_JOURNAL_ROLE.get(role, role)
        carrier = json.loads((INSTALL / "carrier-profile.json").read_text())
        role_profile = carrier["roles"][carrier_role]
        result[role] = (role_profile["uuid"], versions[-1])
    return result


def _verify_result(result: dict) -> dict:
    require(type(result) is dict and result.get("profile") ==
            "ashlar-configured-local-delta4-publication-result/0.1"
            and result.get("installation_id") == "g03-sdk0101-a883463b88d938b126893d31"
            and result.get("operation") == "publish"
            and result.get("batch_id") == "typed-graph-1"
            and result.get("publication") is True
            and result.get("protected_ack") is False,
            "Exact local manifest-only completion result required")
    require(result.get("completion_receipt") == {
        "profile": "ashlar-configured-local-delta4-output-receipt-ref/0.1",
        "request_digest": REQUEST_DIGEST,
        "receipt_sha256": OUTPUT_RECEIPT_SHA256},
        "Original transaction-one receipt reference changed")
    observation = result.get("native_observation")
    require(type(observation) is dict
            and observation.get("committed_prefix") == 3
            and observation.get("states") == ["committed"] * 3
            and observation.get("manifest_state") == "missing",
            "Original fully committed graph prefix was not reopened")
    observed_heads = {role: (item["uuid"], item["version"])
                      for role, item in observation.get("heads", {}).items()}
    require(observed_heads == PRE_MANIFEST_HEADS,
            "Graph/metadata heads changed before manifest finalization")

    manifest = result.get("manifest")
    from ashlar.manifest import validate_manifest_row
    validate_manifest_row(manifest)
    require(manifest["profile_version"] == "ashlar-delta/0.4-xxhash64"
            and json.loads(manifest["table_versions_json"]) == DATA_VERSION_VECTOR,
            "Final manifest row does not bind the retained four-role vector")
    vector = result.get("realized_vector")
    require(type(vector) is dict
            and vector.get("profile") == "ashlar-configured-local-delta4-realized-vector/0.1"
            and vector.get("carrierProfile") == "ashlar-delta/0.4-xxhash64"
            and vector.get("dataRoles") == {
                role.removeprefix("local.configured."): {
                    "name": role,
                    "uuid": PRE_MANIFEST_HEADS[role.removeprefix("local.configured.")][0],
                    "version": version,
                } for role, version in DATA_VERSION_VECTOR.items()},
            "Final realized vector changed retained data heads")
    inventory = vector.get("installationInventory")
    require(type(inventory) is dict and set(inventory) == set(POST_MANIFEST_HEADS)
            and {role: (item["uuid"], item["version"])
                 for role, item in inventory.items()} == POST_MANIFEST_HEADS,
            "Final six-role vector must advance only the manifest head")
    require(delta_heads() == POST_MANIFEST_HEADS,
            "Post-finalization native log heads differ from the exact expected vector")
    return {"manifestSha256": hashlib.sha256(canonical(manifest)).hexdigest(),
            "realizedVectorSha256": vector["canonicalSha256"],
            "terminalHeads": {role: {"uuid": pair[0], "version": pair[1]}
                              for role, pair in POST_MANIFEST_HEADS.items()}}


def _write(path: Path, value: dict) -> str:
    raw = canonical(value) + b"\n"
    require(len(raw) <= 1_048_576, "Continuation result exceeds 1 MiB")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        require(os.write(fd, raw) == len(raw), "Short durable continuation receipt")
    finally:
        os.close(fd)
    return hashlib.sha256(raw).hexdigest()


def _build_runtime_config(invocation: dict, output: Path):
    sys.path.insert(0, str(REPO / "src"))
    from ashlar_host.config import ProducerConfig
    from ashlar_host.configured_source_admission_config import ConfiguredSourceAdmissionConfig
    from ashlar_host.typed_graph_admission_sdk0101 import TypedGraphSdk0101AdmissionConfig
    from ashlar_host.configured_local_runtime import ConfiguredLocalRuntimeConfig

    runtime = invocation["runtime"]
    source = invocation["source"]
    producer = ProducerConfig(Path(source["producerSource"]), Path(runtime["bun"]),
                              Path(runtime["git"]), runtime["sdkProducerTimeoutSeconds"],
                              1_048_576, 4_194_304)
    admission = ConfiguredSourceAdmissionConfig(
        Path(source["configuration"]), producer, output / "admission", 4_194_304)
    config = ConfiguredLocalRuntimeConfig(
        installation=INSTALL, output=output, jars=Path(runtime["jars"]),
        java_home=Path(runtime["javaHome"]), admission=admission,
        stream=source["stream"], materialization_clock_us=source["materializationClockUs"],
        operation_capacity=None, installation_id=invocation["installationId"],
        installation_sha256=invocation["installationSha256"],
        jar_inventory={
            "delta-spark_4.2_2.13-4.4.1.jar":
                "2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be",
            "delta-storage-4.4.1.jar":
                "f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f",
        }, carrier_profile="ashlar-delta/0.4-xxhash64",
        sdk0101_admission=TypedGraphSdk0101AdmissionConfig(producer))
    config.__post_init__()
    return config


def _validate_invocation_copy(invocation_path: Path, attempt: Path,
                              digest: str, *, preflight: bool) -> dict:
    require(invocation_path.resolve() == (attempt / "invocation.json").resolve()
            and invocation_path.is_file() and not invocation_path.is_symlink()
            and invocation_path.read_bytes() == INVOCATION.read_bytes()
            and digest == hashlib.sha256(INVOCATION.read_bytes()).hexdigest(),
            "Exact durable packet invocation copy required")
    if preflight:
        require(attempt.is_dir() and not attempt.is_symlink()
                and stat_is_private(attempt)
                and not Path(read_invocation(INVOCATION)[0]["bounds"]["attemptParent"]).exists(),
                "Private disposable preflight attempt and absent dispatch path required")
    else:
        expected_attempt = Path(read_invocation(INVOCATION)[0]["bounds"]["attemptParent"])
        require(attempt == expected_attempt and attempt.is_dir()
                and not attempt.is_symlink() and stat_is_private(attempt),
                "Exact fresh private continuation attempt required")
    return read_invocation(invocation_path)[0]


def _preflight(invocation_path: Path, attempt: Path, digest: str) -> dict:
    invocation = _validate_invocation_copy(invocation_path, attempt, digest,
                                           preflight=True)
    verify_selected_bytes(invocation)
    output = attempt / "output"
    require(not output.exists()
            and not output.is_symlink(),
            "Disposable preflight output must remain fresh")
    config = _build_runtime_config(invocation, output)
    journal = journal_snapshot(immutable=True)
    _validate_initial_journal(journal)
    heads = delta_heads()
    require(heads == PRE_MANIFEST_HEADS,
            "Retained R3 six-role heads changed before worker preflight")
    sys.path.insert(0, str(REPO / "src"))
    from ashlar_host.configured_local_delta4_publication import (
        _delta4_output_receipt_observation)
    receipt_envelope = json.loads(journal["receipt"][0][1])
    retained_observation_json = receipt_envelope["receipt"]["snapshot_observation_json"]
    retained_observation = json.loads(retained_observation_json)
    current_observation_shape = {
        **retained_observation,
        "manifest_state": "missing",
        "manifest_bridge_fields": {"head": heads["publication_manifest"][1]},
    }
    projected = _delta4_output_receipt_observation(current_observation_shape)
    require(canonical(projected).decode("utf-8") == retained_observation_json,
            "Current observation projection does not reproduce retained R3 receipt bytes")
    require(not output.exists(), "Runtime config construction created output")
    return {
        "profile": "ashlar-g03-r5-worker-preflight/0.1",
        "invocationSha256": digest,
        "installationId": invocation["installationId"],
        "journal": {key: len(value) for key, value in journal.items()},
        "deltaHeads": {role: {"uuid": pair[0], "version": pair[1]}
                       for role, pair in heads.items()},
        "runtimeConfigConstructed": True,
        "sourceAndReceiptBindingsVerified": True,
        "receiptProjectionExact": True,
        "retainedSnapshotObservationSha256": hashlib.sha256(
            retained_observation_json.encode("utf-8")).hexdigest(),
        "nativeCall": False,
        "outputCreated": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--invocation", required=True, type=Path)
    parser.add_argument("--attempt", required=True, type=Path)
    parser.add_argument("--invocation-sha256", required=True)
    parser.add_argument("--preflight", action="store_true")
    args = parser.parse_args()
    invocation, digest = read_invocation(args.invocation)
    if args.preflight:
        require(digest == args.invocation_sha256,
                "Worker preflight invocation digest changed")
        report = _preflight(args.invocation, args.attempt, digest)
        print(json.dumps(report, sort_keys=True, separators=(",", ":")))
        return
    invocation = _validate_invocation_copy(args.invocation, args.attempt, digest,
                                           preflight=False)
    require(digest == args.invocation_sha256,
            "Immutable attempt invocation digest changed")
    verify_selected_bytes(invocation)
    output = Path(invocation["bounds"]["freshOutput"])
    require(output == args.attempt / "output" and not output.exists()
            and not output.is_symlink(), "Fresh output directory required")

    sys.path.insert(0, str(REPO / "src"))
    from ashlar_host.configured_local_runtime import (
        run_configured_local_delta4_transaction1)
    config = _build_runtime_config(invocation, output)

    before_journal = journal_snapshot(immutable=True)
    _validate_initial_journal(before_journal)
    before_heads = delta_heads()
    require(before_heads == PRE_MANIFEST_HEADS,
            "R3 heads changed since immutable preflight")

    # This is the single and only invocation of the native continuation API.
    result = run_configured_local_delta4_transaction1(config)
    postcheck = _verify_result(result)
    # Use SQLite's normal read-only WAL handling after runtime close. If the
    # journal cannot be reopened cleanly, fail uncertain instead of using an
    # immutable snapshot that could omit a committed manifest in a live WAL.
    after_journal = journal_snapshot(immutable=False)
    original_graph_rows = {row[0]: row for row in before_journal["operations"]}
    final_graph_rows = {row[0]: row for row in after_journal["operations"]
                        if row[0] in original_graph_rows}
    require(final_graph_rows == original_graph_rows,
            "Original graph operation custody changed during manifest finalization")
    require(after_journal["source"] == before_journal["source"]
            and after_journal["plan"] == before_journal["plan"]
            and after_journal["metadata"] == before_journal["metadata"]
            and after_journal["receipt"] == before_journal["receipt"]
            and after_journal["artifacts"] == before_journal["artifacts"],
            "Manifest-only continuation changed source/plan/metadata/receipt custody")
    require(len(after_journal["operations"]) == 4
            and len(final_graph_rows) == 3,
            "Exactly one committed manifest operation must extend the graph prefix")
    added_rows = [row for row in after_journal["operations"]
                  if row[0] not in original_graph_rows]
    require(len(added_rows) == 1 and added_rows[0][4] == "committed"
            and type(added_rows[0][5]) is str,
            "Exactly one committed durable manifest operation required")
    result_sha = _write(args.attempt / "continuation-result.json", result)
    postcheck.update({"profile": "ashlar-g03-r3-manifest-only-postcheck/0.1",
                      "invocationSha256": digest,
                      "continuationResultSha256": result_sha,
                      "graphOperationsReplayed": 0,
                      "metadataOperationsAdded": 0,
                      "ack": False, "cloud": False})
    _write(args.attempt / "postcheck.json", postcheck)


def stat_is_private(path: Path) -> bool:
    import stat
    info = path.lstat()
    return (stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and stat.S_IMODE(info.st_mode) == 0o700)


if __name__ == "__main__":
    main()
