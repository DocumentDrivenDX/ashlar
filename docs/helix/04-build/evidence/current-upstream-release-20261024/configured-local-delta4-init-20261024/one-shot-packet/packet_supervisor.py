#!/usr/bin/env python3
"""Prepare and single-dispatch the source-controlled `.4` initialization packet."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time
import uuid


PACKET_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PACKET_DIR))
from packet_common import (  # noqa: E402
    ALLOWED_ENV, canonical, json_file, load_manifest, packet_tree_digest,
    sha_bytes, sha_file, sol_review_path, verify_packet_assets,
)


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def write_new(path: Path, raw: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        require(os.write(fd, raw) == len(raw), "short bounded packet write")
        os.fsync(fd)
    finally:
        os.close(fd)


def private_dir(path: Path) -> None:
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and stat.S_IMODE(info.st_mode) == 0o700,
            "packet attempt directory must be private and user-owned")


def write_attempt(attempt: Path, manifest: dict) -> Path:
    inputs = attempt / "inputs"
    inputs.mkdir(mode=0o700)
    for name, expected in manifest["inputSha256"].items():
        source = PACKET_DIR / "inputs" / name
        require(sha_file(source) == expected, f"source-controlled input changed: {name}")
        write_new(inputs / name, source.read_bytes())
    (attempt / "tmp").mkdir(mode=0o700)
    (attempt / "spark-local").mkdir(mode=0o700)
    private_dir(attempt)
    private_dir(inputs)
    return inputs


def build_invocation(attempt: Path, manifest: dict) -> dict:
    runtime = manifest["runtime"]
    producer = manifest["umfProducer"]
    return {
        "profile": "ashlar-configured-local/0.3",
        "operation": "initialize",
        "ordinal": 0,
        "python": runtime["python"],
        "installation": str(attempt / "installation"),
        "installation_id": "delta4-init-" + attempt.name.rsplit("-", 1)[-1],
        "installation_sha256": None,
        "output": str(attempt / "output"),
        "jars": manifest["runtimePaths"]["jars"],
        "jar_inventory": runtime["jars"],
        "java_home": runtime["javaHome"],
        "source_configuration": str(attempt / "inputs/configured-typed-graph-source.json"),
        "producer": {
            "source": producer["source"], "bun": producer["bun"], "git": producer["git"],
            "timeout_seconds": producer["timeoutSeconds"],
            "maximum_output_bytes": producer["maximumOutputBytes"],
            "maximum_receipt_bytes": producer["maximumReceiptBytes"],
        },
        "stream": "ashlar-delta4-init-" + attempt.name.rsplit("-", 1)[-1],
        "materialization_clock_us": str(int(time.time() * 1_000_000)),
        "operation_capacity": None,
        "carrier_profile": "ashlar-delta/0.4-xxhash64",
        "bounds": {
            "whole_seconds": manifest["limits"]["wallSeconds"],
            "stream_bytes": manifest["limits"]["combinedStdoutStderrBytes"],
            "receipt_bytes": 262144,
            "maximum_admission_receipt_bytes": manifest["limits"]["admissionReceiptBytes"],
        },
    }


def create_prepared(attempt_parent: Path, packet_digest: str) -> Path:
    require(packet_tree_digest() == packet_digest, "packet digest does not match review value")
    manifest = load_manifest()
    verify_packet_assets()
    attempt_parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    private_dir(attempt_parent)
    attempt_id = uuid.uuid4().hex
    attempt = attempt_parent / f"ashlar-delta4-init-{attempt_id}"
    attempt.mkdir(mode=0o700)
    private_dir(attempt)
    inputs = write_attempt(attempt, manifest)
    invocation = build_invocation(attempt, manifest)
    invocation_raw = canonical(invocation)
    require(len(invocation_raw) <= manifest["limits"]["invocationBytes"],
            "versioned invocation exceeds its bound")
    invocation_path = attempt / "invocation.json"
    write_new(invocation_path, invocation_raw)

    python = Path(manifest["runtime"]["python"])
    python_real = python.resolve(strict=True)
    worker = PACKET_DIR / "packet_worker.py"
    prepared_path = attempt / "prepared.json"
    launch_path = attempt / "launch.json"
    permit_path = attempt / "execution-permit.json"
    environment = {
        "HOME": str(Path.home()),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
        "JAVA_HOME": manifest["runtime"]["javaHome"],
        "PYSPARK_PYTHON": str(python),
        "PYSPARK_DRIVER_PYTHON": str(python),
        "PYTHONDONTWRITEBYTECODE": "1",
        "SPARK_LOCAL_IP": "127.0.0.1",
        "TMPDIR": str(attempt / "tmp"),
        "SPARK_LOCAL_DIRS": str(attempt / "spark-local"),
    }
    require(set(environment) == set(ALLOWED_ENV), "child environment allowlist changed")
    template = [str(python), "-I", "-B", "-u", str(worker),
        "--prepared", str(prepared_path), "--permit", str(permit_path),
        "--prepared-sha256", "{PREPARED_SHA256}",
        "--launch-sha256", "{LAUNCH_SHA256}",
        "--packet-sha256", "{PACKET_SHA256}",
        "--permit-sha256", "{PERMIT_SHA256}"]
    descriptor = {
        "profile": "ashlar-local-child-launch/0.1",
        "packetTreeSha256": packet_digest,
        "attemptId": attempt_id,
        "preparedPath": str(prepared_path),
        "invocationSha256": sha_bytes(invocation_raw),
        "cwd": str(attempt),
        "interpreter": {
            "path": str(python), "realPath": str(python_real),
            "realPathSha256": sha_file(python_real),
        },
        "environment": environment,
        "environmentSha256": sha_bytes(canonical(environment)),
        "argvTemplate": template,
    }
    launch_sha = sha_bytes(canonical(descriptor))
    launch_value = {"profile": "ashlar-local-child-launch/0.1",
                    "descriptor": descriptor, "sha256": launch_sha}
    write_new(launch_path, canonical(launch_value))
    prepared = {
        "profile": "ashlar-delta4-init-prepared/0.1",
        "attemptId": attempt_id,
        "attemptRoot": str(attempt),
        "packetTreeSha256": packet_digest,
        "invocationPath": str(invocation_path),
        "invocationSha256": sha_bytes(invocation_raw),
        "launchPath": str(launch_path),
        "launchSha256": launch_sha,
        "executionPermitPath": str(permit_path),
        "limits": manifest["limits"],
    }
    write_new(prepared_path, canonical(prepared))
    prepared_sha = sha_file(prepared_path)
    return prepared_path


def validate_permit(prepared_path: Path, permit_path: Path, packet_digest: str) -> tuple[dict, str]:
    prepared_raw = prepared_path.read_bytes()
    permit_raw = permit_path.read_bytes()
    prepared = json.loads(prepared_raw)
    permit = json.loads(permit_raw)
    prepared_sha = sha_bytes(prepared_raw)
    permit_sha = sha_bytes(permit_raw)
    require(permit.get("profile") == "ashlar-delta4-init-execution-permit/0.1",
            "one-shot permit must bind reviewed packet and current slot")
    require(set(permit) == {"profile", "packetTreeSha256", "preparedSha256",
            "launchSha256", "invocationSha256", "processInventory", "solReview"},
            "execution permit contains unknown or missing members")
    require(permit.get("packetTreeSha256") == packet_digest
            and permit.get("preparedSha256") == prepared_sha
            and permit.get("launchSha256") == prepared["launchSha256"]
            and permit.get("invocationSha256") == prepared["invocationSha256"],
            "permit does not bind this packet and exact attempt")
    inventory = permit.get("processInventory")
    require(type(inventory) is dict and inventory.get("profile") == "ashlar-local-process-slot-inventory/0.1"
            and set(inventory) == {"profile", "capturedAt", "capturedBy", "command",
                                   "capturedOutputSha256", "sparkOrJavaProcesses"}
            and inventory.get("sparkOrJavaProcesses") == []
            and type(inventory.get("capturedBy")) is str and bool(inventory["capturedBy"])
            and type(inventory.get("command")) is str and bool(inventory["command"])
            and type(inventory.get("capturedOutputSha256")) is str
            and len(inventory["capturedOutputSha256"]) == 64,
            "fresh external Spark-slot inventory required")
    captured = dt.datetime.fromisoformat(inventory["capturedAt"].replace("Z", "+00:00"))
    age = (dt.datetime.now(dt.timezone.utc) - captured).total_seconds()
    require(0 <= age <= 60, "Spark-slot inventory must be no more than 60 seconds old")
    review = permit.get("solReview")
    require(type(review) is dict and set(review) == {"path", "sha256"}
            and review["path"] == str(sol_review_path(packet_digest))
            and type(review["sha256"]) is str and len(review["sha256"]) == 64,
            "Sol packet review receipt required")
    review_path = Path(review["path"])
    review_record = json.loads(review_path.read_bytes())
    require(sha_file(review_path) == review["sha256"]
            and review_record.get("profile") == "ashlar-delta4-init-sol-review/0.1"
            and review_record.get("reviewer") == "Sol"
            and review_record.get("decision") == "execute"
            and review_record.get("packetTreeSha256") == packet_digest,
            "Sol review receipt is stale, mismatched, or does not authorize this packet")
    return permit, permit_sha


def owned_bytes(root: Path) -> int:
    total = 0
    for path in root.rglob("*"):
        info = path.lstat()
        require(not stat.S_ISLNK(info.st_mode), "attempt symlink refused")
        if stat.S_ISREG(info.st_mode):
            total += info.st_size
    return total


def run_once(prepared_path: Path, permit_path: Path, packet_digest: str) -> dict:
    require(packet_tree_digest() == packet_digest, "packet differs from Sol-reviewed digest")
    prepared = json.loads(prepared_path.read_bytes())
    prepared_sha = sha_file(prepared_path)
    require(prepared.get("profile") == "ashlar-delta4-init-prepared/0.1"
            and prepared.get("packetTreeSha256") == packet_digest
            and prepared.get("executionPermitPath") == str(permit_path),
            "prepared attempt path or packet differs")
    permit, permit_sha = validate_permit(prepared_path, permit_path, packet_digest)
    attempt = Path(prepared["attemptRoot"])
    private_dir(attempt)
    dispatch = attempt / "dispatch.json"
    write_new(dispatch, canonical({
        "profile": "ashlar-delta4-init-dispatch/0.1",
        "attemptId": prepared["attemptId"],
        "packetTreeSha256": packet_digest,
        "preparedSha256": prepared_sha,
        "launchSha256": prepared["launchSha256"],
        "permitSha256": permit_sha,
        "dispatchedAt": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "retryAllowed": False,
    }))
    launch = json_file(Path(prepared["launchPath"]))
    descriptor = launch["descriptor"]
    argv = [part.replace("{PREPARED_SHA256}", prepared_sha)
                 .replace("{LAUNCH_SHA256}", prepared["launchSha256"])
                 .replace("{PACKET_SHA256}", packet_digest)
                 .replace("{PERMIT_SHA256}", permit_sha)
            for part in descriptor["argvTemplate"]]
    require(sha_bytes(canonical(descriptor)) == prepared["launchSha256"]
            and descriptor["environmentSha256"] == sha_bytes(canonical(descriptor["environment"])),
            "frozen child launch digest differs")

    manifest = load_manifest()
    max_seconds = manifest["limits"]["wallSeconds"]
    max_bytes = manifest["limits"]["ownedBytes"]
    max_stream = manifest["limits"]["combinedStdoutStderrBytes"]
    stdout_path, stderr_path = attempt / "worker.stdout", attempt / "worker.stderr"
    started = time.monotonic()
    process = None
    failure = None
    with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
        process = subprocess.Popen(argv, cwd=descriptor["cwd"], env=descriptor["environment"],
                                   stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                                   start_new_session=True)
        while process.poll() is None:
            total_stream = stdout_path.stat().st_size + stderr_path.stat().st_size
            if total_stream > max_stream:
                failure = "captured stdout/stderr cap exceeded"
                break
            if owned_bytes(attempt) > max_bytes:
                failure = "owned attempt byte cap exceeded"
                break
            if time.monotonic() - started > max_seconds:
                failure = "single-run wall limit exceeded"
                break
            time.sleep(0.05)
        if failure is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()
    elapsed = time.monotonic() - started
    total_stream = stdout_path.stat().st_size + stderr_path.stat().st_size
    if total_stream > max_stream:
        failure = failure or "captured stdout/stderr cap exceeded"
    final_bytes = owned_bytes(attempt)
    if final_bytes > max_bytes:
        failure = failure or "owned attempt byte cap exceeded"
    result_path = attempt / "result.json"
    if process.returncode != 0:
        failure = failure or f"child exit status {process.returncode}"
    result = {
        "profile": "ashlar-delta4-init-supervisor-result/0.1",
        "attemptId": prepared["attemptId"],
        "status": "complete" if failure is None and result_path.is_file() else "refused-or-failed",
        "failure": failure,
        "returnCode": process.returncode,
        "elapsedSeconds": round(elapsed, 3),
        "ownedBytes": final_bytes,
        "stdoutBytes": stdout_path.stat().st_size,
        "stderrBytes": stderr_path.stat().st_size,
        "stdoutSha256": sha_file(stdout_path),
        "stderrSha256": sha_file(stderr_path),
        "permitSha256": permit_sha,
        "attemptRoot": str(attempt),
        "retryAllowed": False,
    }
    write_new(attempt / "supervisor-result.json", canonical(result))
    if failure is not None or not result_path.is_file():
        raise RuntimeError(canonical(result).decode().strip())
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-packet-sha256", required=True)
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--attempt-parent", default="/private/tmp/ashlar-delta4-init-attempts")
    run = sub.add_parser("execute")
    run.add_argument("--prepared", required=True)
    run.add_argument("--permit", required=True)
    args = parser.parse_args()
    packet_digest = args.expected_packet_sha256
    require(len(packet_digest) == 64 and all(c in "0123456789abcdef" for c in packet_digest),
            "reviewed packet digest must be lowercase SHA-256")
    require(packet_tree_digest() == packet_digest, "packet files differ from supplied review digest")
    if args.command == "prepare":
        path = create_prepared(Path(args.attempt_parent).resolve(), packet_digest)
        prepared = json.loads(path.read_bytes())
        print(json.dumps({"preparedPath": str(path), "preparedSha256": sha_file(path),
                          "launchSha256": prepared["launchSha256"],
                          "invocationSha256": prepared["invocationSha256"],
                          "packetTreeSha256": packet_digest}, sort_keys=True))
        return 0
    result = run_once(Path(args.prepared).resolve(strict=True), Path(args.permit).resolve(strict=True), packet_digest)
    print(canonical(result).decode().strip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
