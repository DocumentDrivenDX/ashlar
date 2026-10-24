#!/usr/bin/env python3
"""One child: attest exact invocation/runtime/UMF, then initialize `.4` once."""
from __future__ import annotations

import argparse
from contextlib import redirect_stdout
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sys


PACKET_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PACKET_DIR))
sys.path.insert(0, "/Users/erik/Projects/ashlar/src")
from packet_common import (  # noqa: E402
    ALLOWED_ENV, canonical, json_file, load_manifest, packet_tree_digest,
    sha_bytes, sha_file, verify_inputs, verify_packet_assets,
    sol_review_path, verify_umf_bridge,
)


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def _load_bound_files(prepared_path: Path, permit_path: Path):
    prepared_raw = prepared_path.read_bytes()
    prepared = json.loads(prepared_raw)
    permit_raw = permit_path.read_bytes()
    permit = json.loads(permit_raw)
    return prepared, permit, sha_bytes(prepared_raw), sha_bytes(permit_raw)


def _verify_permit(prepared: dict, permit: dict, permit_sha: str, args) -> None:
    _require(permit.get("profile") == "ashlar-delta4-init-execution-permit/0.1",
             "explicit one-shot execution permit required")
    _require(set(permit) == {"profile", "packetTreeSha256", "preparedSha256",
             "launchSha256", "invocationSha256", "processInventory", "solReview"},
             "execution permit contains unknown or missing members")
    _require(permit.get("packetTreeSha256") == args.packet_sha256
             and permit.get("preparedSha256") == args.prepared_sha256
             and permit.get("launchSha256") == args.launch_sha256
             and permit.get("invocationSha256") == prepared["invocationSha256"],
             "execution permit does not bind this reviewed packet/attempt")
    inventory = permit.get("processInventory")
    _require(type(inventory) is dict
             and inventory.get("profile") == "ashlar-local-process-slot-inventory/0.1"
             and set(inventory) == {"profile", "capturedAt", "capturedBy", "command",
                                    "capturedOutputSha256", "sparkOrJavaProcesses"}
             and inventory.get("sparkOrJavaProcesses") == []
             and type(inventory.get("capturedOutputSha256")) is str
             and len(inventory["capturedOutputSha256"]) == 64,
             "fresh local Spark-slot inventory required")
    captured = dt.datetime.fromisoformat(inventory["capturedAt"].replace("Z", "+00:00"))
    now = dt.datetime.now(dt.timezone.utc)
    age = (now - captured).total_seconds()
    _require(0 <= age <= 60, "Spark-slot inventory is not fresh")
    review = permit.get("solReview")
    _require(type(review) is dict and set(review) == {"path", "sha256"}
             and review["path"] == str(sol_review_path(args.packet_sha256))
             and type(review["sha256"]) is str and len(review["sha256"]) == 64,
             "Sol review receipt required")
    review_path = Path(review["path"])
    review_record = json_file(review_path)
    _require(sha_file(review_path) == review["sha256"]
             and review_record.get("profile") == "ashlar-delta4-init-sol-review/0.1"
             and review_record.get("reviewer") == "Sol"
             and review_record.get("decision") == "execute"
             and review_record.get("packetTreeSha256") == args.packet_sha256,
             "Sol review receipt is stale, mismatched, or does not authorize this packet")
    _require(type(permit_sha) is str and len(permit_sha) == 64
             and permit_sha == args.permit_sha256,
             "execution permit digest is invalid")


def _verify_launch(prepared: dict, launch_sha: str, args) -> dict:
    launch_path = Path(prepared["launchPath"])
    launch = json_file(launch_path)
    descriptor = launch["descriptor"]
    _require(launch.get("profile") == "ashlar-local-child-launch/0.1"
             and launch.get("sha256") == launch_sha
             and sha_bytes(canonical(descriptor)) == launch_sha,
             "child launch descriptor hash differs")
    _require(descriptor.get("packetTreeSha256") == args.packet_sha256
             and descriptor.get("preparedPath") == str(Path(args.prepared).resolve())
             and descriptor.get("invocationSha256") == prepared["invocationSha256"],
             "child launch descriptor is bound to another attempt")
    expected_argv = [part.replace("{LAUNCH_SHA256}", launch_sha)
                          .replace("{PREPARED_SHA256}", args.prepared_sha256)
                          .replace("{PACKET_SHA256}", args.packet_sha256)
                          .replace("{PERMIT_SHA256}", args.permit_sha256)
                     for part in descriptor["argvTemplate"]]
    actual_argv = list(sys.orig_argv)
    _require(actual_argv == expected_argv, "actual child argv/interpreter differs from frozen launch")
    _require(os.getcwd() == descriptor["cwd"], "actual child working directory differs")
    env = {key: os.environ.get(key) for key in ALLOWED_ENV if key in os.environ}
    _require(env == descriptor["environment"]
             and sha_bytes(canonical(env)) == descriptor["environmentSha256"],
             "actual child environment differs from frozen allowlist/digest")
    _require(not any(key in os.environ for key in (
        "SPARK_REMOTE", "MASTER", "SPARK_CONNECT_MODE_ENABLED", "PYSPARK_SUBMIT_ARGS",
        "PYSPARK_GATEWAY_PORT", "PYSPARK_GATEWAY_SECRET", "PYTHONHOME", "PYTHONPATH")),
        "ambient Spark/Python override refused")
    interpreter = descriptor["interpreter"]
    path = Path(interpreter["path"])
    _require(sys.executable == str(path)
             and str(path.resolve(strict=True)) == interpreter["realPath"]
             and sha_file(path.resolve(strict=True)) == interpreter["realPathSha256"],
             "child interpreter identity changed")
    return launch


def _check_invocation(path: Path, prepared: dict, manifest: dict):
    from ashlar_host.configured_local_cli import parse_configured_local

    raw = path.read_bytes()
    _require(sha_bytes(raw) == prepared["invocationSha256"], "frozen invocation digest differs")
    _require(len(raw) <= manifest["limits"]["invocationBytes"], "invocation exceeds byte limit")
    parsed = parse_configured_local(raw)
    runtime = parsed.runtime
    attempt = Path(prepared["attemptRoot"])
    _require(parsed.operation == "initialize" and parsed.ordinal == 0
             and parsed.runtime.carrier_profile == "ashlar-delta/0.4-xxhash64"
             and parsed.runtime.installation == attempt / "installation"
             and parsed.runtime.output == attempt / "output"
             and parsed.runtime.admission.configuration == attempt / "inputs/configured-typed-graph-source.json"
             and parsed.python == Path(manifest["runtime"]["python"])
             and runtime.installation_sha256 is None,
             "versioned `.4` invocation differs from its frozen scope")
    return raw, parsed


def execute(prepared_path: Path, permit_path: Path, args) -> dict:
    prepared, permit, prepared_sha, permit_sha = _load_bound_files(prepared_path, permit_path)
    _require(prepared.get("profile") == "ashlar-delta4-init-prepared/0.1"
             and prepared_sha == args.prepared_sha256
             and prepared.get("launchSha256") == args.launch_sha256,
             "prepared attempt hash differs")
    _require(packet_tree_digest() == args.packet_sha256,
             "source-controlled packet digest changed")
    _verify_permit(prepared, permit, permit_sha, args)
    _verify_launch(prepared, args.launch_sha256, args)
    manifest = load_manifest()

    attempt = Path(prepared["attemptRoot"])
    _require(attempt.is_absolute() and attempt.resolve(strict=True) == attempt
             and attempt.is_dir() and not attempt.is_symlink(),
             "fresh private attempt directory required")
    attempt_info = attempt.lstat()
    _require(attempt_info.st_uid == os.getuid() and attempt_info.st_mode & 0o077 == 0,
             "attempt directory must be private and owned")
    verify_inputs(attempt / "inputs", manifest)
    runtime_receipt = verify_packet_assets(attempt / "inputs")

    invocation_path = Path(prepared["invocationPath"])
    invocation_raw, invocation = _check_invocation(invocation_path, prepared, manifest)
    bridge_receipt = verify_umf_bridge(invocation.runtime.installation, manifest)
    preflight = {
        "profile": "ashlar-local-delta4-init-effect-free-preflight/0.1",
        "packetTreeSha256": args.packet_sha256,
        "preparedSha256": prepared_sha,
        "launchSha256": args.launch_sha256,
        "invocationSha256": sha_bytes(invocation_raw),
        "environmentSha256": json_file(Path(prepared["launchPath"]))["descriptor"]["environmentSha256"],
        "runtime": runtime_receipt,
        "umfBridge": bridge_receipt,
        "sparkStarted": False,
        "publication": False,
        "ack": False,
    }
    _exclusive_write(attempt / "effect-free-preflight.json", canonical(preflight))

    # Recheck mutable authorities immediately before crossing the Spark boundary.
    _require(packet_tree_digest() == args.packet_sha256, "packet changed before Spark startup")
    verify_inputs(attempt / "inputs", manifest)
    verify_packet_assets(attempt / "inputs")
    _require(sha_bytes(invocation_path.read_bytes()) == prepared["invocationSha256"],
             "invocation changed before Spark startup")
    _require(sha_bytes(permit_path.read_bytes()) == permit_sha,
             "execution permit changed before Spark startup")

    from ashlar_host.configured_local_runtime import initialize_configured_local
    with redirect_stdout(sys.stderr):
        result = initialize_configured_local(invocation.runtime)
    installation = result.get("installation")
    role_names = set(manifest["scope"]["roles"])
    _require(result.get("profile") == "ashlar-configured-local-initialized/0.3"
             and result.get("operation") == "initialize"
             and result.get("published") is False and result.get("protected_ack") is False,
             "configured-local initialization result differs from `.4` contract")
    _require(type(installation) is dict
             and installation.get("profile") == "ashlar-configured-local-installation/0.3"
             and installation.get("physical_profile") == "ashlar-delta/0.4-xxhash64",
             "initialized six-role registry profile differs")
    carrier_path = invocation.runtime.installation / "carrier-profile.json"
    carrier_raw = carrier_path.read_bytes()
    _require(sha_bytes(carrier_raw) == installation.get("carrier_profile_sha256"),
             "carrier profile registry custody differs")
    carrier = json.loads(carrier_raw)
    _require(carrier.get("profile") == "ashlar-local-current-installation-profile/0.1"
             and carrier.get("physical_profile") == "ashlar-delta/0.4-xxhash64"
             and set(carrier.get("roles", {})) == role_names,
             "complete `.4` six-role carrier profile missing")
    targets = installation.get("targets")
    _require(type(targets) is list and len(targets) == 6,
             "six-role installation vector missing")
    by_role = {}
    for target in targets:
        table = target.get("table")
        role = table.rsplit(".", 1)[-1] if type(table) is str else None
        _require(role in role_names and role not in by_role
                 and table == "local.configured." + role
                 and target.get("path") == str(invocation.runtime.installation / role),
                 "initialized target role or path differs")
        _require(type(target.get("uuid")) is str and len(target["uuid"]) == 36,
                 "initialized native table UUID missing")
        delta_log = invocation.runtime.installation / role / "_delta_log"
        _require(delta_log.is_dir() and not delta_log.is_symlink(),
                 "initialized role Delta log missing")
        by_role[role] = target["uuid"]
    _require(set(by_role) == role_names and len(set(by_role.values())) == 6,
             "six-role target identity vector is incomplete or duplicated")
    final = {
        "profile": "ashlar-local-delta4-init-result/0.1",
        "status": "complete-local-initialization-only",
        "attemptId": prepared["attemptId"],
        "sourceInvocationSha256": prepared["invocationSha256"],
        "installationSha256": result["installation_sha256"],
        "carrierProfileSha256": sha_bytes(carrier_raw),
        "roleTableUuids": by_role,
        "publication": False,
        "query": False,
        "protectedAck": False,
        "preflightSha256": sha_file(attempt / "effect-free-preflight.json"),
    }
    _exclusive_write(attempt / "result.json", canonical(final))
    return final


def _exclusive_write(path: Path, raw: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        written = os.write(fd, raw)
        _require(written == len(raw), "short packet evidence write")
        os.fsync(fd)
    finally:
        os.close(fd)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prepared", required=True)
    parser.add_argument("--permit", required=True)
    parser.add_argument("--prepared-sha256", required=True)
    parser.add_argument("--launch-sha256", required=True)
    parser.add_argument("--packet-sha256", required=True)
    parser.add_argument("--permit-sha256", required=True)
    args = parser.parse_args()
    result = execute(Path(args.prepared), Path(args.permit), args)
    sys.stdout.buffer.write(canonical(result))
    sys.stdout.buffer.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
