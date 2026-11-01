#!/usr/bin/env python3
"""Effect-free pre-Spark inspection for the retained G03 R3 manifest prefix.

This deliberately traps the configured runtime immediately before Spark opens.
It runs the actual source, producer, installation-registry, carrier-profile,
Python/JDK/JAR and private-lease checks against R3 while writing admission
receipts only beneath a disposable temporary directory. It never dispatches
Delta work or writes to the retained installation.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import sys
import tempfile
from unittest.mock import patch


REPO = Path("/private/tmp/ashlar-r3-manifest-continuation")
ATTEMPT = Path(
    "/private/tmp/ashlar-g03-sdk0101-lifecycle-r3-attempts-20261101/"
    "ashlar-g03-sdk0101-lifecycle-94c1991ea883463b88d938b126893d31")
INSTALL = ATTEMPT / "installation"
UMF = Path("/private/tmp/umf-sdk0101-90cb")
JARS = Path("/private/tmp/ashlar-r16-delta4-native-smoke-20261029/"
            "attempt-84c7b6002712486495c5dd66bfa0b8d6/jars")
PYTHON = Path("/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/"
              "venv/bin/python3.12")
JAVA_HOME = Path("/opt/homebrew/Cellar/openjdk@21/21.0.13.0.2/"
                 "libexec/openjdk.jdk/Contents/Home")
BUN = Path("/opt/homebrew/Cellar/bun/1.4.3/bin/bun")
GIT = Path("/usr/bin/git")
SOURCE_CONFIG = ATTEMPT / "inputs/configured-typed-graph-source.json"
WHOLE_SECONDS = 900
RSS_LIMIT_BYTES = 2 * 1024**3
EXPECTED = {
    "execution-status.json": "e0adadeed8c9aa8a4c5e1963f5e17d345b7d4ebdfe96cf92ddf366b5cd5ef688",
    "lifecycle-completion.json": "68a4b40cb6ff45ffe6da92c31e2fba419a93728262f7ee14e277df1547abb157",
    "initialize-result.json": "5bdc1624cc1661aacc6d951bef515e065b08dd2d9cce55e486bdec7e73131d54",
    "transaction1-result.json": "f982e5e9ee5cb3297c4f459b1071f59d36e3ae2b140cc1f7ddf46d231e4c582a",
    "stage-timings.json": "83b002d3eaa09127cf44544073b5c0456b13c667a72e79d9a6a7ec8a295adfc1",
    "installation/installation.json": "76ae97288b29564b56b6b1d45c1559a011f0269ad2f1656d60e1dbecd0863e58",
    "installation/carrier-profile.json": "9b7f64fa3efbe71f5ecb601ad0b36185e1bd99a6def396ae83d0c03af2d38fd6",
    "inputs/configured-typed-graph-source.json": "c62cf82f2116e6b7630585597f20887ba0038e08f022c2e05f319f2827857f82",
    "inputs/typed-graph-binding.json": "1ab1416d5a36a082c4d5be38dc061377bdae22bb5395fb8cfbec8cee80910863",
    "inputs/typed-graph-model.umf.json": "19ab3cc44efd769346b88fb7c5dc6792b59fd0057ed8846e4a0eec744f8e716b",
    "inputs/typed-graph-source.jsonl": "9edb7566f82324fadb1e7cf1ba0eaf22691096a26209e938c83730d096d4882b",
}
SOURCE_FILES = {
    "src/ashlar_host/configured_local_runtime.py":
        "c452f2be4397d11baaf9e213c47dcaf08ab07a383e1ce1add637f4b65cbacea7",
    "src/ashlar_host/configured_local_delta4_publication.py":
        "fbfcda903d845a503bf43ca28fe6754edb8b2fe6d583d42bbd818c9a15856ae1",
    "src/ashlar_host/configured_local_runtime_profile.py":
        "082a69af610550bb0db19a8c8a4da09666e6031feb714eca3d07261d0a868033",
    "src/ashlar_host/configured_typed_graph_source_admission_sdk0101.py":
        "5bc5666da6cd3298afababb18661f7997df02dc2f36698ca0dc79b80f8761cb9",
    "src/ashlar_host/typed_graph_admission_sdk0101.py":
        "5e2ec2fa1dfd097ccf5d31fbc77b8309c566c981fa72b366d1bb24244d4fc1f4",
    "src/ashlar_host/resources/typed_graph_values_sdk0101.ts":
        "b006e36b48f05da4bdab60f2a784112383bfeb83eb2b0a3a67c19e0f842997dd",
}
ROLE_VERSIONS = {
    "edge_current": 2,
    "object_current": 2,
    "whole_source_history": 2,
    "attempts": 3,
    "manifest": 1,
    "tombstone": 1,
}
RESULT_ROLE_VERSIONS = {
    "edge_current": 2,
    "object_current": 2,
    "whole_source_history": 2,
    "publication_attempt_phase": 3,
    "publication_manifest": 1,
    "tombstone": 1,
}
PLANNED_ATTEMPT = Path(
    "/private/tmp/ashlar-g03-sdk0101-r3-manifest-continuation-attempts-20261101/"
    "continuation-r5-d24785a0-e9a9-4607-a7cc-88ef672bc35c")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def snapshot_installation() -> dict[str, str]:
    selected = [INSTALL / "installation.json", INSTALL / "carrier-profile.json",
                INSTALL / "operations.sqlite"]
    selected.extend(sorted(INSTALL.glob("*/_delta_log/*.json")))
    require(all(path.is_file() and not path.is_symlink() for path in selected),
            "Retained R3 installation inventory incomplete")
    return {str(path): sha(path) for path in selected}


def verify_delta_heads() -> None:
    carrier = json.loads((INSTALL / "carrier-profile.json").read_text())
    require(carrier.get("profile") == "ashlar-local-current-installation-profile/0.4"
            and carrier.get("physical_profile") == "ashlar-delta/0.4-xxhash64",
            "Retained carrier profile is not the exact `.4` profile")
    for role, version in ROLE_VERSIONS.items():
        logs = sorted((INSTALL / role / "_delta_log").glob("[0-9]" * 20 + ".json"))
        found = [int(path.name[:20]) for path in logs]
        require(found == list(range(version + 1)),
                f"Exact retained Delta log sequence changed for {role}")


def verify_retained_evidence() -> dict:
    for relative, digest in EXPECTED.items():
        path = ATTEMPT / relative
        require(path.is_file() and not path.is_symlink() and sha(path) == digest,
                f"Retained evidence digest changed: {relative}")
    status = json.loads((ATTEMPT / "execution-status.json").read_text())
    completion = json.loads((ATTEMPT / "lifecycle-completion.json").read_text())
    init = json.loads((ATTEMPT / "initialize-result.json").read_text())
    txn = json.loads((ATTEMPT / "transaction1-result.json").read_text())
    require(status.get("classification") == "completed-verified"
            and status.get("processGroupEmpty") is True,
            "R3 lifecycle status is not completed-verified and empty")
    require(completion.get("elapsedMs") == 617649
            and completion.get("ack") is False
            and completion.get("publication") is False,
            "R3 completion receipt differs from the retained result")
    require(any(event.get("stage") == "runtime.initialize"
                and event.get("durationMs") == 79389 for event in init.get("timings", []))
            and init.get("published") is False
            and init.get("ack") is False,
            "R3 initialize result differs from the retained result")
    timing = json.loads((ATTEMPT / "stage-timings.json").read_text())
    txn_elapsed = next((event["durationMs"] for event in timing["events"]
                        if event["stage"] == "runtime.transaction1"), None)
    require(txn_elapsed == 538102 and txn.get("manifest") is False
            and txn.get("publication") is False and txn.get("ack") is False,
            "R3 transaction-one result differs from the retained result")
    expected_heads = {row["role"]: row["version"] for row in txn["terminalRoleHeads"]}
    require(expected_heads == RESULT_ROLE_VERSIONS,
            "R3 six-role terminal head vector changed")
    return {"status": status, "completion": completion, "txn": txn}


def verify_sqlite_prefix() -> dict:
    uri = f"file:{INSTALL / 'operations.sqlite'}?mode=ro&immutable=1"
    db = sqlite3.connect(uri, uri=True)
    try:
        tables = {row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        require({"local_source_plan", "local_plan", "local_operation",
                 "local_delta4_metadata_operation", "local_delta4_output_receipt",
                 "local_publication_artifact"} <= tables,
                "Retained R3 journal schema is incomplete")
        require(not any("manifest" in name and name not in {
            "local_publication_artifact"} for name in tables),
            "Unexpected retained manifest intent table")
        source_rows = db.execute(
            "SELECT request_digest FROM local_source_plan").fetchall()
        plan_rows = db.execute(
            "SELECT operation,digest FROM local_plan").fetchall()
        ops = db.execute(
            "SELECT operation,before_version,state,receipt FROM local_operation "
            "ORDER BY operation").fetchall()
        metadata = db.execute(
            "SELECT lane,state,before_version FROM local_delta4_metadata_operation "
            "ORDER BY before_version").fetchall()
        receipts = db.execute(
            "SELECT request_digest,receipt_sha256 FROM local_delta4_output_receipt").fetchall()
        artifact_count = db.execute(
            "SELECT count(*) FROM local_publication_artifact").fetchone()[0]
        require(len(source_rows) == len(plan_rows) == 1
                and source_rows[0][0] == "bebb23db9dee40dd09424cc071d4d80beb200f75b0c6487b5274b1d3d387fb77",
                "Exact original retained source plan required")
        require(plan_rows[0] == (
                    "effects:1b6684afdb17d315294c8cfa2dc61d3fe031016621b688e2af44167fb6dcec07",
                    "69c633798f8ad2d439df53329739f4e6d8c90aea34b809dfc67336d3c70228d1"),
                "Exact original graph plan custody required")
        require(len(ops) == 3 and all(row[1] == 1 and row[2] == "committed"
                                     and type(row[3]) is str for row in ops),
                "Exactly three committed graph operation receipts required")
        require(metadata == [("publication_attempt_phase", "committed", 1),
                             ("publication_attempt_phase", "committed", 2)],
                "Exact prepared/applying metadata prefix required")
        require(receipts == [(source_rows[0][0],
                              "eef82376bff8eafc281442aa8f7da2e424d0439ab942b5ccb0c5464c07f37b2c")]
                and artifact_count == 0,
                "Exact retained output receipt and no publication artifact required")
        return {"operationCount": len(ops), "metadataCount": len(metadata),
                "outputReceiptSha256": receipts[0][1], "publicationArtifacts": artifact_count}
    finally:
        db.close()


class SparkBoundaryReached(Exception):
    pass


def exercise_real_pre_spark_path() -> None:
    sys.path.insert(0, str(REPO / "src"))
    from ashlar_host.config import ProducerConfig
    from ashlar_host.configured_source_admission_config import ConfiguredSourceAdmissionConfig
    from ashlar_host.typed_graph_admission_sdk0101 import TypedGraphSdk0101AdmissionConfig
    from ashlar_host.configured_local_runtime import (
        ConfiguredLocalRuntimeConfig, run_configured_local_delta4_transaction1)
    import ashlar_host.configured_local_runtime as runtime_module

    previous_java_home = os.environ.get("JAVA_HOME")
    os.environ["JAVA_HOME"] = str(JAVA_HOME)
    try:
        producer = ProducerConfig(UMF, BUN, GIT, 60, 1_048_576, 4_194_304)
        sdk = TypedGraphSdk0101AdmissionConfig(producer)
        with tempfile.TemporaryDirectory(prefix="ashlar-r3-preflight-") as directory:
            parent = Path(directory).resolve()
            parent.chmod(0o700)
            output = parent / "preflight-output"
            admission = ConfiguredSourceAdmissionConfig(
                SOURCE_CONFIG, producer, output / "admission", 4_194_304)
            config = ConfiguredLocalRuntimeConfig(
                installation=INSTALL,
                output=output,
                jars=JARS,
                java_home=JAVA_HOME,
                admission=admission,
                stream="g03-sdk0101-a883463b88d938b126893d31",
                materialization_clock_us="1793504419378646",
                operation_capacity=None,
                installation_id="g03-sdk0101-a883463b88d938b126893d31",
                installation_sha256=EXPECTED["installation/installation.json"],
                jar_inventory={
                    "delta-spark_4.2_2.13-4.4.1.jar":
                        "2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be",
                    "delta-storage-4.4.1.jar":
                        "f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f",
                },
                carrier_profile="ashlar-delta/0.4-xxhash64",
                sdk0101_admission=sdk)
            require(not output.exists(), "Preflight output must be fresh")
            with patch.object(runtime_module, "open_finite_spark",
                              side_effect=SparkBoundaryReached):
                try:
                    run_configured_local_delta4_transaction1(config)
                except SparkBoundaryReached:
                    pass
                else:
                    raise RuntimeError("Configured runtime did not reach Spark boundary")
            require("pyspark" not in sys.modules,
                    "Spark Python modules loaded before the trapped Spark boundary")
            require((output / "admission/report.json").is_file(),
                    "Real SDK 0.10.1 source admission did not complete")
    finally:
        if previous_java_home is None:
            os.environ.pop("JAVA_HOME", None)
        else:
            os.environ["JAVA_HOME"] = previous_java_home


def main() -> None:
    require(REPO.is_dir() and INSTALL.is_dir(), "Pinned source or retained R3 path missing")
    require(not PLANNED_ATTEMPT.exists() and not PLANNED_ATTEMPT.is_symlink(),
            "Planned continuation attempt parent must remain fresh")
    require(sys.version_info[:2] == (3, 12), "Python 3.12 required")
    require(stat.S_ISREG(BUN.lstat().st_mode) and os.access(BUN, os.X_OK),
            "Resolved regular Bun executable required")
    status = subprocess.run(["/usr/bin/git", "-C", str(REPO), "rev-parse", "HEAD"],
                            check=True, capture_output=True, text=True, timeout=5).stdout.strip()
    subprocess.run(["/usr/bin/git", "-C", str(REPO), "merge-base", "--is-ancestor",
                    "f16cc4ed0ba5704ba5055487097f29da1a452820", "HEAD"],
                   check=True, capture_output=True, timeout=5)
    require(subprocess.run(["/usr/bin/git", "-C", str(REPO), "status", "--porcelain"],
                           check=True, capture_output=True, text=True, timeout=5).stdout == "",
            "Candidate source checkout must be clean")
    for relative, digest in SOURCE_FILES.items():
        require(sha(REPO / relative) == digest, f"Selected runtime source changed: {relative}")
    evidence = verify_retained_evidence()
    verify_delta_heads()
    journal = verify_sqlite_prefix()
    before = snapshot_installation()
    exercise_real_pre_spark_path()
    after = snapshot_installation()
    require(before == after, "Effect-free preflight changed retained installation bytes")
    output = {
        "profile": "ashlar-g03-r3-manifest-only-preflight/0.1",
        "candidateCommit": status,
        "reachedSparkBoundary": True,
        "sparkStarted": False,
        "nativeDispatch": False,
        "retainedInstallationBytesUnchanged": True,
        "retainedR3ProcessGroupEmpty": evidence["status"]["processGroupEmpty"],
        "preflightSubprocessesSynchronousAndReaped": True,
        "R3Status": evidence["status"]["classification"],
        "R3ElapsedMs": evidence["completion"]["elapsedMs"],
        "SQLitePrefix": journal,
        "bounds": {"continuationWallSeconds": WHOLE_SECONDS,
                   "continuationRssBytes": RSS_LIMIT_BYTES,
                   "sparkMaster": "local[1]", "sparkDriverMemory": "512m",
                   "producerTimeoutSeconds": 60},
    }
    print(json.dumps(output, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
