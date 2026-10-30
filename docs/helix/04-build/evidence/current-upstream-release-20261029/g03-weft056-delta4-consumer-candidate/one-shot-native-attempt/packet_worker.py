#!/usr/bin/env python3
"""One child stage of the supervised fresh `.4` G03 attempt."""
from __future__ import annotations

import argparse
from contextlib import redirect_stdout
from dataclasses import replace
import json
import os
from pathlib import Path
import sys


PACKET = Path(__file__).resolve().parent
sys.path.insert(0, str(PACKET))
from packet_common import (REPO, canonical, manifest, sha_bytes, sha_file,
                           verify_packet, verify_umf_bridge, write_new)  # noqa: E402

def require(condition, message):
    if not condition:
        raise ValueError(message)


require(REPO == Path(manifest()["ashlarSource"]["root"]).resolve(),
        "packet repository root differs from source manifest")
sys.path.insert(0, str(REPO / "src"))


def load(path):
    return json.loads(Path(path).read_bytes())


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", required=True)
    parser.add_argument("--stage", choices=("initialize", "transaction1"), required=True)
    parser.add_argument("--packet-sha256", required=True)
    parser.add_argument("--prepared-sha256", required=True)
    return parser.parse_args()


def execute(args):
    selected = manifest()
    attempt = Path(args.attempt)
    prepared_path = attempt / "prepared.json"
    prepared_raw = prepared_path.read_bytes()
    prepared = json.loads(prepared_raw)
    require(sha_bytes(prepared_raw) == args.prepared_sha256
            and prepared.get("packetTreeSha256") == args.packet_sha256
            and prepared.get("sourceCommit") == selected["sourceCommit"]
            and prepared.get("attemptRoot") == str(attempt),
            "worker/prepared/commit identity differs")
    packet_receipt = verify_packet(attempt / "inputs")
    require(packet_receipt['packetTreeSha256'] == args.packet_sha256
            and packet_receipt['currentCommit'] == prepared.get('currentCommit'),
            "packet or source inputs changed before worker startup")
    if args.stage == "initialize":
        invocation_path = attempt / "initialize-invocation.json"
        raw = invocation_path.read_bytes()
        require(sha_bytes(raw) == prepared['initializeInvocationSha256'],
                "initialized runtime invocation changed")
        from ashlar_host.configured_local_cli import parse_configured_local
        from ashlar_host.configured_local_runtime import initialize_configured_local
        invocation = parse_configured_local(raw)
        require(invocation.operation == "initialize" and invocation.ordinal == 0
                and invocation.runtime.carrier_profile == "ashlar-delta/0.4-xxhash64"
                and invocation.runtime.installation == attempt / "installation"
                and invocation.runtime.output == attempt / "initialize-output"
                and invocation.runtime.installation_sha256 is None,
                "fresh six-role `.4` initialization invocation refused")
        bridge = verify_umf_bridge(attempt / "installation")
        write_new(attempt / "umf-bridge-preflight.json", canonical(bridge))
        with redirect_stdout(sys.stderr):
            result = initialize_configured_local(invocation.runtime)
        require(result.get("profile") == "ashlar-configured-local-initialized/0.3"
                and result.get("operation") == "initialize"
                and result.get("published") is False
                and result.get("protected_ack") is False,
                "fresh local six-role initialization result differs")
        report = attempt / "initialize-output" / "runtime-report.json"
        require(report.is_file(),
                "initialized six-role result report missing")
        registry_path = attempt / "installation" / "installation.json"
        registry_raw = registry_path.read_bytes()
        require(sha_bytes(registry_raw) == result['installation_sha256'],
                "initialized installation registry digest differs")
        registry = json.loads(registry_raw)
        require(registry.get("physical_profile") == "ashlar-delta/0.4-xxhash64"
                and len(registry.get("targets", [])) == 6,
                "exact six-role `.4` installation profile required")
        summary = {"profile": "ashlar-g03-delta4-init-stage-result/0.1",
                   "installationId": result["installation_id"],
                   "installationSha256": result["installation_sha256"],
                   "runtimeReportSha256": sha_file(report),
                   "carrierProfileSha256": registry["carrier_profile_sha256"],
                   "published": False, "ack": False}
        write_new(attempt / "initialize-stage-result.json", canonical(summary))
        return summary

    init_result_path = attempt / "initialize-stage-result.json"
    init_result = load(init_result_path)
    require(init_result.get("profile") == "ashlar-g03-delta4-init-stage-result/0.1"
            and init_result.get("published") is False and init_result.get("ack") is False,
            "original fresh initialization receipt required")
    from ashlar_host.configured_local_cli import parse_configured_local
    from ashlar_host.configured_local_runtime import run_configured_local_delta4_transaction1
    invocation = parse_configured_local(
        (attempt / "initialize-invocation.json").read_bytes())
    require(invocation.operation == "initialize"
            and invocation.runtime.installation == attempt / "installation"
            and invocation.runtime.output == attempt / "initialize-output"
            and invocation.runtime.installation_sha256 is None,
            "original fresh initialization invocation required")
    output = attempt / "transaction1-output"
    runtime = replace(
        invocation.runtime, output=output,
        admission=replace(invocation.runtime.admission, output=output / "admission"),
        installation_sha256=init_result["installationSha256"])
    require(runtime.stream == invocation.runtime.stream
            and runtime.materialization_clock_us == invocation.runtime.materialization_clock_us
            and runtime.installation == attempt / "installation"
            and runtime.carrier_profile == "ashlar-delta/0.4-xxhash64",
            "transaction-1 source/install scope drifted after initialization")
    with redirect_stdout(sys.stderr):
        result = run_configured_local_delta4_transaction1(runtime)
    require(result.get("profile") == "ashlar-configured-local-delta4-transaction1-result/0.1"
            and result.get("operation") == "transaction1-effects-validated"
            and result.get("batch_id") == "typed-graph-1"
            and result.get("publication") is False
            and result.get("protected_ack") is False,
            "transaction-1 effects-only result differs")
    expected = selected["scope"]["expectedTerminalHeads"]
    observation = result.get("native_observation")
    require(type(observation) is dict and type(observation.get("heads")) is dict
            and {role: item.get("version") for role, item in observation["heads"].items()}
                == expected,
            "exact transaction-1 terminal six-role head vector differs")
    report = output / "runtime-report.json"
    require(report.is_file() and json.loads(report.read_bytes()) == result,
            "bounded effects-validated runtime report differs")
    summary = {"profile": "ashlar-g03-delta4-transaction1-stage-result/0.1",
               "batchId": "typed-graph-1",
               "runtimeReportSha256": sha_file(report),
               "completionReceiptSha256": result["completion_receipt"]["receipt_sha256"],
               "terminalHeads": expected,
               "publication": False, "manifest": False, "ack": False}
    write_new(attempt / "transaction1-stage-result.json", canonical(summary))
    return summary


def main():
    args = parse_args()
    result = execute(args)
    sys.stderr.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
