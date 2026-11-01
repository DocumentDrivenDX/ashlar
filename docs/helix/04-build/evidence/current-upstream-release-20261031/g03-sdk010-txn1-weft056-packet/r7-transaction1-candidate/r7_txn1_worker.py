#!/usr/bin/env python3
"""Isolated SDK010 transaction-one child; inert until a separately reviewed rebind."""
from __future__ import annotations

from contextlib import redirect_stdout
from dataclasses import replace
import json
import os
from pathlib import Path
import stat
import sys

PACKET = Path(__file__).resolve().parent
sys.path.insert(0, str(PACKET))
sys.path.insert(0, str(PACKET.parent))
sys.path.insert(0, str(PACKET.parents[6] / "src"))
import r7_txn1_gate as gate  # noqa: E402


def require(ok: bool, message: str) -> None:
    if not ok:
        raise ValueError(message)


def canonical(value) -> bytes:
    return gate.canonical(value)


def _runtime_config(attempt: Path, reconciliation: dict):
    selected = gate.manifest()
    source_attempt = Path(selected["r7"]["attemptRoot"])
    invocation_path = source_attempt / "initialize-invocation.json"
    raw = invocation_path.read_bytes()
    from ashlar_host.configured_local_cli import parse_configured_local
    from ashlar_host.typed_graph_sdk010_admission import TypedGraphSdk010AdmissionConfig

    invocation = parse_configured_local(raw)
    runtime = invocation.runtime
    require(invocation.operation == "initialize"
            and runtime.carrier_profile == selected["r7"]["carrierProfile"]
            and runtime.installation == source_attempt / "installation"
            and runtime.installation_sha256 is None
            and runtime.installation_id == reconciliation["installationId"]
            and runtime.stream == reconciliation["installationId"],
            "retained R7 SDK010 invocation scope differs")
    runtime_output = attempt / "transaction1-output"
    parent_info = attempt.lstat()
    require(attempt.is_dir() and attempt.resolve() == attempt
            and stat.S_ISDIR(parent_info.st_mode) and parent_info.st_uid == os.getuid()
            and stat.S_IMODE(parent_info.st_mode) == 0o700
            and not runtime_output.exists() and not runtime_output.is_symlink(),
            "fresh private attempt and unclaimed transaction-one output path required")
    sdk = selected["sdk010"]
    sdk_admission = TypedGraphSdk010AdmissionConfig(
        Path(sdk["node"]), Path(sdk["sdkPackageRoot"]), Path(sdk["sdkArchive"]))
    runtime = replace(runtime, output=runtime_output,
                      admission=replace(runtime.admission, output=runtime_output / "admission"),
                      installation_sha256=reconciliation["installationSha256"],
                      sdk010_admission=sdk_admission)
    require(runtime.installation == source_attempt / "installation"
            and runtime.carrier_profile == "ashlar-delta/0.4-xxhash64"
            and runtime.sdk010_admission is not None,
            "SDK010 `.4` transaction-one runtime config differs")
    return runtime


def _expected_head_vector(reconciliation: dict) -> dict:
    local_to_runtime = {
        "object_current": "object_current", "edge_current": "edge_current",
        "tombstone": "tombstone", "whole_source_history": "whole_source_history",
        "attempts": "publication_attempt_phase", "manifest": "publication_manifest",
    }
    increments = {"object_current": 1, "edge_current": 1, "tombstone": 0,
                  "whole_source_history": 1, "attempts": 2, "manifest": 0}
    expected = {}
    for row in reconciliation["roleHeads"]:
        role = row["role"]
        runtime_role = local_to_runtime[role]
        expected[runtime_role] = {"uuid": row["uuid"],
                                  "version": row["headVersion"] + increments[role]}
    return expected


def execute(attempt: Path, permit_path: Path) -> dict:
    selected = gate.manifest()
    require(selected.get("dispatchEnabled") is True,
            "native transaction-one dispatch remains disabled pending Sol review")
    inspection = gate.inspect(complete_source_admission=False)
    require(inspection["solReviewComplete"] is True,
            "bound independent Sol review receipt required before worker startup")
    reconciliation = inspection["r7Initialization"]
    for name, digest in selected["inputs"].items():
        require(gate.sha_file(attempt / "inputs" / name) == digest,
                "attempt input digest differs: " + name)
    slot_path = attempt / "dispatch-process-slot.json"
    slot = json.loads(slot_path.read_bytes())
    require(slot.get("profile") == "ashlar-g03-r7-txn1-process-slot/0.1"
            and slot.get("empty") is True and slot.get("javaProcesses") == [],
            "fresh empty process slot must be bound before worker startup")
    prepared_path = attempt / "prepared.json"
    prepared_raw = prepared_path.read_bytes()
    prepared = json.loads(prepared_raw)
    packet_sha = packet_tree_sha256()
    require(prepared.get("profile") == "ashlar-g03-sdk010-r7-txn1-prepared/0.1"
            and prepared.get("packetTreeSha256") == packet_sha
            and prepared.get("attemptRoot") == str(attempt)
            and prepared.get("reconciliationSha256") == reconciliation["receiptSha256"],
            "transaction-one prepared identity differs")
    permit_raw = permit_path.read_bytes()
    permit = json.loads(permit_raw)
    require(permit.get("profile") == "ashlar-g03-sdk010-r7-txn1-execution-permit/0.1"
            and permit.get("execute") is True
            and permit.get("packetTreeSha256") == packet_sha
            and permit.get("preparedSha256") == gate.sha_bytes(prepared_raw)
            and permit.get("reconciliationSha256") == reconciliation["receiptSha256"]
            and permit.get("solReviewReceiptSha256") == selected["solReviewReceiptSha256"],
            "exact reviewed one-shot permit required")

    # This child is intentionally unreachable while the committed packet is in
    # candidate state. A reviewed rebind must add the exact Sol receipt first.
    runtime = _runtime_config(attempt, reconciliation)
    from ashlar_host.configured_local_runtime import run_configured_local_delta4_transaction1
    from r4_timing import StageTimings, install_sdk010_transaction_hooks
    timings = StageTimings()
    restore = install_sdk010_transaction_hooks(timings)
    failure = None
    result = None
    try:
        with timings.measure("runtime.transaction1"):
            with redirect_stdout(sys.stderr):
                result = run_configured_local_delta4_transaction1(runtime)
    except BaseException as exc:
        failure = exc
    finally:
        restore()
        (attempt / "transaction1-timing-receipt.json").write_bytes(timings.to_bytes())
    if failure is not None:
        raise failure
    require(type(result) is dict
            and result.get("profile") == "ashlar-configured-local-delta4-transaction1-result/0.1"
            and result.get("operation") == "transaction1-effects-validated"
            and result.get("batch_id") == "typed-graph-1"
            and result.get("publication") is False
            and result.get("protected_ack") is False,
            "SDK010 transaction-one effects-only runtime result differs")
    observed = result.get("native_observation", {}).get("heads")
    expected = _expected_head_vector(reconciliation)
    require(type(observed) is dict and set(observed) == set(expected),
            "complete six-role terminal head vector required")
    terminal = {}
    for role, head in expected.items():
        row = observed[role]
        require(type(row) is dict and row.get("uuid") == head["uuid"]
                and row.get("version") == head["version"],
                "transaction-one terminal head differs from the R7 vector plus exact effects")
        terminal[role] = {"uuid": row["uuid"], "version": row["version"]}
    report = runtime.output / "runtime-report.json"
    require(report.is_file() and json.loads(report.read_bytes()) == result,
            "bounded transaction-one runtime report differs")
    receipt = {"profile": "ashlar-g03-sdk010-r7-txn1-stage-result/0.1",
               "transaction": "typed-graph-1",
               "reconciliationSha256": reconciliation["receiptSha256"],
               "runtimeReportSha256": gate.sha_file(report),
               "timingReceiptSha256": gate.sha_file(attempt / "transaction1-timing-receipt.json"),
               "completionReceiptSha256": result["completion_receipt"]["receipt_sha256"],
               "terminalRoleHeads": terminal,
               "publication": False, "manifest": False, "ack": False,
               "consumer": False}
    gate.require(len(canonical(receipt)) <= selected["limits"]["receiptBytes"],
                 "transaction-one receipt exceeds bound")
    target = attempt / "transaction1-stage-result.json"
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(canonical(receipt))
        stream.flush()
        os.fsync(stream.fileno())
    return receipt


def packet_tree_sha256() -> str:
    return gate.packet_digest()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--attempt", type=Path, required=True)
    parser.add_argument("--permit", type=Path, required=True)
    args = parser.parse_args()
    print(canonical(execute(args.attempt.resolve(strict=True),
                            args.permit.resolve(strict=True))).decode(), end="")
