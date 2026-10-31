#!/usr/bin/env python3
"""Single initialized-only child; no transaction, query, publication, or ACK."""
from __future__ import annotations
import argparse
from contextlib import redirect_stdout
from dataclasses import replace
import json
import os
from pathlib import Path
import sys

PACKET = Path(__file__).resolve().parent
PARENT = PACKET.parent
REPO = PACKET.parents[6]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(PARENT))
from r4_packet import manifest, packet_digest, sha_bytes, sha_file, verify_effect_free, write_new, canonical, require, sdk_config
from r4_timing import StageTimings, install_sdk010_transaction_hooks


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", required=True)
    parser.add_argument("--prepared-sha256", required=True)
    args = parser.parse_args()
    selected = manifest()
    attempt = Path(args.attempt).resolve(strict=True)
    prepared_raw = (attempt / "prepared.json").read_bytes()
    prepared = json.loads(prepared_raw)
    require(sha_bytes(prepared_raw) == args.prepared_sha256
            and prepared["attemptRoot"] == str(attempt)
            and prepared["packetTreeSha256"] == packet_digest(), "worker/prepared identity differs")
    require((attempt / "process-slot.json").is_file(), "process-slot receipt missing")
    require(verify_effect_free()["packetTreeSha256"] == prepared["packetTreeSha256"],
            "SDK010/source/runtime preflight changed")
    invocation_raw = (attempt / "initialize-invocation.json").read_bytes()
    require(sha_file(attempt / "initialize-invocation.json") == prepared["invocationSha256"],
            "initialize invocation digest differs")
    from ashlar_host.configured_local_cli import parse_configured_local
    from ashlar_host.configured_local_runtime import initialize_configured_local
    runtime = parse_configured_local(invocation_raw).runtime
    require(Path(sys.executable) == Path(selected["runtime"]["python"]),
            "pinned Python executable required")
    require(runtime.carrier_profile == selected["carrierProfile"]
            and runtime.installation == attempt / "installation"
            and runtime.output == attempt / "initialize-output"
            and runtime.installation_sha256 is None, "fresh `.4` initialization invocation refused")
    runtime = replace(runtime, sdk010_admission=sdk_config(selected))
    timings = StageTimings()
    restore = install_sdk010_transaction_hooks(timings)
    failure = None
    result = None
    try:
        with timings.measure("runtime.initialize"):
            with redirect_stdout(sys.stderr):
                result = initialize_configured_local(runtime)
    except BaseException as exc:
        failure = exc
    finally:
        restore()
        write_new(attempt / "r4-timing-receipt.json", timings.to_bytes())
    if failure is not None:
        raise failure
    require(result.get("profile") == "ashlar-configured-local-initialized/0.3"
            and result.get("operation") == "initialize"
            and result.get("published") is False and result.get("protected_ack") is False,
            "SDK010 `.4` initialization result differs")
    registry_path = attempt / "installation/installation.json"
    carrier_path = attempt / "installation/carrier-profile.json"
    require(sha_file(registry_path) == result.get("installation_sha256"),
            "installation registry receipt digest differs")
    registry = json.loads(registry_path.read_bytes())
    carrier = json.loads(carrier_path.read_bytes())
    require(registry.get("physical_profile") == selected["carrierProfile"]
            and len(registry.get("targets", [])) == 6
            and registry.get("carrier_profile_sha256") == sha_file(carrier_path)
            and carrier.get("profile") == selected["installationProfile"]
            and carrier.get("source_policy_revision") == selected["sourcePolicyRevision"]
            and carrier.get("umf_realization_profile") ==
                "ashlar-local-current-delta4-umf-source-sdk010/0.1",
            "exact SDK010 six-role installation profile required")
    target_rows = registry["targets"]
    require(len({row["uuid"] for row in target_rows}) == 6
            and {Path(row["path"]).name for row in target_rows} == {
                "object_current", "edge_current", "tombstone", "whole_source_history",
                "attempts", "manifest"}, "six unique local role targets required")
    report = attempt / "initialize-output/runtime-report.json"
    require(report.is_file(), "initialized runtime report missing")
    receipt = {"profile": "ashlar-g03-sdk010-six-role-init-result/0.1",
        "sourcePolicyRevision": selected["sourcePolicyRevision"],
        "packetTreeSha256": prepared["packetTreeSha256"],
        "preparedSha256": args.prepared_sha256,
        "installationId": result["installation_id"],
        "installationSha256": result["installation_sha256"],
        "carrierProfileSha256": sha_file(carrier_path),
        "runtimeReportSha256": sha_file(report),
        "sdk010SourceAdmissionPreflightSha256": prepared["sourceAdmission"]["sourceAdmissionReceiptSha256"],
        "roleTargets": target_rows,
        "timingReceiptSha256": sha_file(attempt / "r4-timing-receipt.json"),
        "publication": False, "manifest": False, "ack": False,
        "transaction1": False, "consumer": False}
    write_new(attempt / "init-result.json", canonical(receipt))
    sys.stdout.write(canonical(receipt).decode())


if __name__ == "__main__":
    main()
