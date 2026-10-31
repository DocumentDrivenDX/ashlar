#!/usr/bin/env python3
"""Effect-free R4 custody preflight. Never opens Spark or mutates an install."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[6]
PACKET = Path(__file__).resolve().parent
G03_INPUTS = (ROOT / "docs/helix/04-build/evidence/current-upstream-release-20261029/"
              "g03-sdk058-delta4-realization-candidate-20261030/inputs/typed-graph")
PYTHON = Path("/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python3.12")
NODE = Path("/opt/homebrew/Cellar/node/26.12.1/bin/node")
SDK_ROOT = Path("/private/tmp/ashlar-umf010-core104/sdk010/package")
SDK_ARCHIVE = Path("/private/tmp/ashlar-umf010-core104/umf-sdk-0.10.0.tgz")
R3_INSTALL = Path("/private/tmp/ashlar-g03-sdk058-delta4-txn1-attempts-20261030/"
                  "ashlar-g03-txn1-f66791eded7f449eb4b21ef60cd50bca/installation")
R4_ATTEMPT_PARENT = Path("/private/tmp/ashlar-g03-sdk010-delta4-txn1-r4-attempts-20261031")
SDK_ARCHIVE_SHA256 = "2fb63dea5ca85b32c6bad823703de526875ac79490234dbfea4a8863c7d5fe6c"
SDK_ENTRY_SHA256 = "b32bce2fdcf928bc5b9fd8376b4c7cab9beb1b63d90cb1d20b9ad0ec94595951"
SOURCE_COMMIT = "fe0ae9e0"


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=False, allow_nan=False) + "\n").encode()


def command(argv):
    return subprocess.run(argv, capture_output=True, text=True, check=True, timeout=10).stdout.strip()


def process_slot():
    raw = command(["/bin/ps", "-Ao", "pid=,ppid=,pgid=,comm="])
    selected = []
    for line in raw.splitlines():
        parts = line.split(None, 3)
        if len(parts) == 4 and Path(parts[3]).name.lower() in {"java", "java_home"}:
            selected.append({"pid": int(parts[0]), "ppid": int(parts[1]),
                             "pgid": int(parts[2]), "command": Path(parts[3]).name})
    return {"profile": "ashlar-g03-r4-process-slot/0.1",
            "capturedUtc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "inventorySha256": hashlib.sha256(raw.encode()).hexdigest(),
            "sparkJvmProcesses": selected, "empty": not selected}


def main():
    result = {"profile": "ashlar-g03-sdk010-r4-preflight/0.1", "effects": False,
              "checks": {}, "blockers": []}
    git = "/opt/homebrew/Cellar/git/2.56.0_1/bin/git"
    try:
        head = command([git, "-C", str(ROOT), "rev-parse", "HEAD"])
        subprocess.run([git, "-C", str(ROOT), "merge-base", "--is-ancestor",
                        SOURCE_COMMIT, head], check=True, timeout=10)
        dirty = subprocess.run([git, "-C", str(ROOT), "diff", "--quiet", SOURCE_COMMIT,
                                "--", "src"], timeout=10).returncode
        if dirty:
            raise ValueError("src differs from the admitted SDK010 source commit")
        result["checks"]["source"] = {"head": head, "admittedCommit": SOURCE_COMMIT,
                                      "srcMatchesCommit": True}
    except Exception as exc:
        result["blockers"].append("source custody: " + str(exc))

    try:
        sys.path.insert(0, str(ROOT / "src"))
        from ashlar.typed_graph_source import build_typed_graph_request
        from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration
        from ashlar_host.typed_graph_sdk010_admission import (
            TypedGraphSdk010AdmissionConfig, admit_typed_graph_sdk010,
            SDK010_PRODUCER_REVISION)
        config_path = G03_INPUTS / "configured-typed-graph-source.json"
        configuration = load_typed_graph_jsonl_configuration(config_path)
        model = configuration.model_source
        request = build_typed_graph_request(configuration)
        sdk_config = TypedGraphSdk010AdmissionConfig(NODE, SDK_ROOT, SDK_ARCHIVE)
        receipt = admit_typed_graph_sdk010(sdk_config, configuration, model, request)
        result["checks"]["sdk010SourceAdmission"] = {
            "producerRevision": SDK010_PRODUCER_REVISION,
            "configurationSha256": sha(config_path),
            "modelSha256": hashlib.sha256(model).hexdigest(),
            "requestSha256": hashlib.sha256(request).hexdigest(),
            "sdkReceiptSha256": hashlib.sha256(receipt).hexdigest(),
            "sdkArchiveSha256": sha(SDK_ARCHIVE), "sdkEntrySha256": sha(SDK_ROOT / "dist/umf.js"),
            "sourceSha256": hashlib.sha256(configuration.source_bytes).hexdigest(),
            "bindingSha256": hashlib.sha256(configuration.binding_source).hexdigest(),
            "node": str(NODE), "sdkArchive": str(SDK_ARCHIVE)}
        if result["checks"]["sdk010SourceAdmission"]["sdkArchiveSha256"] != SDK_ARCHIVE_SHA256:
            raise ValueError("official SDK archive digest differs")
        if result["checks"]["sdk010SourceAdmission"]["sdkEntrySha256"] != SDK_ENTRY_SHA256:
            raise ValueError("official SDK public entry digest differs")
    except Exception as exc:
        result["blockers"].append("SDK010 source admission: " + str(exc))

    try:
        if not PYTHON.is_file() or not os.access(PYTHON, os.X_OK):
            raise ValueError("pinned Python 3.12.14 executable is unavailable")
        probe = "import json,pyspark,delta,py_mini_racer; from importlib.metadata import version; print(json.dumps({'python':__import__('sys').version.split()[0],'pyspark':pyspark.__version__,'delta':version('delta-spark'),'miniRacer':version('mini-racer')}))"
        versions = json.loads(command([str(PYTHON), "-c", probe]))
        if versions != {"python": "3.12.14", "pyspark": "4.2.0", "delta": "4.4.1", "miniRacer": "0.14.1"}:
            raise ValueError("pinned local runtime versions differ: " + str(versions))
        result["checks"]["runtime"] = {"python": str(PYTHON), "pythonSha256": sha(PYTHON.resolve()),
            "versions": versions,
            "javaHome": "/opt/homebrew/Cellar/openjdk@21/21.0.13.0.2/libexec/openjdk.jdk/Contents/Home",
            "deltaJars": [
                "/private/tmp/ashlar-r16-delta4-native-smoke-20261029/attempt-84c7b6002712486495c5dd66bfa0b8d6/jars/delta-spark_4.2_2.13-4.4.1.jar",
                "/private/tmp/ashlar-r16-delta4-native-smoke-20261029/attempt-84c7b6002712486495c5dd66bfa0b8d6/jars/delta-storage-4.4.1.jar"]}
        if not all(Path(item).is_file() for item in result["checks"]["runtime"]["deltaJars"]):
            raise ValueError("pinned Delta runtime jars are incomplete")
    except Exception as exc:
        result["blockers"].append("runtime custody: " + str(exc))

    try:
        slot = process_slot()
        result["checks"]["processSlot"] = slot
        if not slot["empty"]:
            result["blockers"].append("Spark JVM process slot is occupied")
    except Exception as exc:
        result["blockers"].append("process slot: " + str(exc))

    try:
        carrier = json.loads((R3_INSTALL / "carrier-profile.json").read_bytes())
        registry = json.loads((R3_INSTALL / "installation.json").read_bytes())
        result["checks"]["r3InstallDisposition"] = {
            "carrierProfile": carrier.get("profile"),
            "sourcePolicyRevision": carrier.get("source_policy_revision"),
            "physicalProfile": carrier.get("physical_profile"),
            "installationSha256": sha(R3_INSTALL / "installation.json"),
            "eligibleForR4": False}
        if (carrier.get("source_policy_revision") != "umf-sdk-0.10.0/selected"
                or carrier.get("physical_profile") != "ashlar-delta/0.4-xxhash64"
                or len(registry.get("targets", [])) != 6):
            result["blockers"].append(
                "no fresh SDK010 `.4` installation receipt; retained r3 is SDK058 `.2` and must remain quarantined")
    except Exception as exc:
        result["blockers"].append("eligible installation receipt unavailable: " + str(exc))

    result["checks"]["newAttemptParent"] = {"path": str(R4_ATTEMPT_PARENT),
                                              "exists": R4_ATTEMPT_PARENT.exists()}
    result["readyForSolReview"] = not result["blockers"]
    sys.stdout.buffer.write(canonical(result))
    return 0 if not result["blockers"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
