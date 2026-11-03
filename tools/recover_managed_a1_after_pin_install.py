#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Resume a committed managed A0 publication after pin migrations are installed.

This recovery skips schema/Volume creation, initialization, and A0 publication.
It reads and verifies their retained inputs, opens a fresh public Truss source
hold over the original stopped A/B clusters, then performs the missing A0
verification/ACK and the one authorized A1 seed/publication/ACK sequence.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
import uuid

import run_managed_a1_serverless as runner


MAX_READONLY_ADMISSION_SECONDS = 120
MAX_RECOVERY_SECONDS = 1230
ORIGINAL_CAMPAIGN_SECONDS = 1680
RETIRED_RECOVERY_WINDOWS = (
    {"attempt": "R18 retry01", "ceiling_seconds": 90,
     "observed_provider_seconds": 66.064, "ack_attempted": False,
     "a1_seed_attempted": False, "original_publication_mutated": False},
    {"attempt": "R19 retry01", "ceiling_seconds": 120,
     "observed_provider_seconds": 83.324, "ack_attempted": False,
     "a1_seed_attempted": False,
     "jobs_terminal_and_deleted": True,
     "provider_receipt_path": "/private/tmp/ashlar-managed-r19-failed-verifier-run.json",
     "provider_receipt_sha256": "6d74339b05ee6eb3017295e8746932ef832d382d25d1bdf604b4e271c497d855",
     "recovery_receipt_path": "/private/tmp/ashlar-a1-managed-recovery-r19-rc3-20261103/recovery.json",
     "recovery_receipt_sha256": "6dc0ae7f59d2fa6f36c69f93649e664336a28be2a5b81e7675e299e1bd81e025"},
    {"attempt": "R19 metadata inventory capture", "ceiling_seconds": 180,
     "observed_provider_seconds": 55.992, "read_only": True,
     "jobs_terminal_and_deleted": True,
     "provider_receipt_path": "/private/tmp/ashlar-native-metadata-capture-20261103/run.json",
     "provider_receipt_sha256": "b16f5d54f3c0b48c6a52ed0576dd73a897f2480dc963196e9139517755631208",
     "metadata_path": "/private/tmp/ashlar-native-metadata-capture-20261103/metadata.json",
     "metadata_sha256": "b26174e14eb8bb12a94ff4f7e6c56de292de8894c0b46a7fdceec88e8ee09128",
     "metadata_elapsed_seconds": 41.26997236,
     "inventory_counts": {"actions": 7, "column_rows": 66, "table_rows": 6,
         "detail_role_count": 6, "extended_role_count": 6, "grant_scope_count": 8,
         "history_role_count": 6, "retention_property_role_count": 4,
         "role_count": 6, "table_schema_role_count": 6}},
    {"attempt": "R20 RC5 failed A0 verifier", "ceiling_seconds": 150,
     "observed_provider_seconds": 111.312, "ack_attempted": False,
     "a1_seed_attempted": False, "source_ack_outcome": "not_attempted",
     "job_result_state": "FAILED", "job_create_state": "deleted",
     "provider_receipt_path": "/private/tmp/ashlar-managed-r20-failed-verifier-run.json",
     "provider_receipt_sha256": "5cd19fe9d0506c1850bbf5ffe2f4254fe0845f0885cbbc9f88a2d91f1462a25e",
     "recovery_receipt_path": "/private/tmp/ashlar-a1-managed-recovery-r20-rc5-20261103/recovery.json",
     "recovery_receipt_sha256": "1123025828ed403e380483e0241f70bc334bec953348bcdbc86c0e4f2edd27db"},
    {"attempt": "R20 read-only six-role versioned row capture", "ceiling_seconds": 180,
     "observed_provider_seconds": 41.536, "observed_body_seconds": 25.3466,
     "read_only": True, "jobs_terminal_and_deleted": True,
     "producer_receipt_sha256": "9b90f21685e92e8e236b632d779978e0c66840248bcff4da7545c23802d18b78",
     "provider_receipt_path": "/private/tmp/ashlar-native-versioned-rows-capture-20261103/run.json",
     "provider_receipt_sha256": "a7a37d75e928630f3312ba645fdf3f07ea4b0a9a7c5c2a55527e2a94b0af7f28",
     "rows_path": "/private/tmp/ashlar-native-versioned-rows-capture-20261103/rows.json",
     "rows_sha256": "ee19f82a17f439212f9b52b696c19c5cf72a2f0be39ea2580b512bd3cc5a23fc",
     "row_counts": {"object_current": 0, "edge_current": 0, "tombstone": 0,
         "whole_source_history": 0, "publication_manifest": 2,
         "publication_attempt_phase": 5}},
    {"attempt": "R21 RC6 failed A0 verifier", "ceiling_seconds": 210,
     "observed_provider_seconds": 161.042,
     "observed_local_recovery_seconds": 170.35985279199667,
     "ack_attempted": False, "source_ack_outcome": "not_attempted",
     "a1_seed_attempted": False, "job_result_state": "FAILED",
     "job_create_state": "deleted",
     "provider_receipt_path": "/private/tmp/ashlar-managed-r21-failed-verifier-run.json",
     "provider_receipt_sha256": "5024cbd4ee67c786238bb32784eba36282fe0e95c3aa8349127b4106fad3f6ae",
     "recovery_receipt_path": "/private/tmp/ashlar-a1-managed-recovery-r21-rc6-20261103/recovery.json",
     "recovery_receipt_sha256": "74f28a241c9ba452872879f0705f7728452dd35d8a710a64a177ae98cc259fea"},
)
PRIOR_ACTIVE_SECONDS_CEILING = 1380  # prior 1,170s + 210s R21 verifier window.
MAX_CUMULATIVE_ACTIVE_SECONDS = 2610  # prior 1,380s + remaining 1,230s.
PROFILE = "ashlar-managed-a1-pin-install-recovery/0.1"


def _require(value, message):
    if not value:
        raise ValueError(message)


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _json(raw: bytes, label: str) -> dict:
    value = json.loads(raw.decode("utf-8"))
    _require(type(value) is dict, f"Exact {label} object required")
    return value


def _read_private_artifact(receipt: dict, name: str, recovery_root: Path) -> bytes:
    record = receipt.get("private_artifacts", {}).get(name)
    _require(type(record) is dict and set(record) == {"path", "bytes", "sha256"},
             f"Exact retained {name} artifact record required")
    path = Path(record["path"])
    _require(path.is_absolute() and not path.is_symlink()
             and path.parent == recovery_root and path.is_file()
             and path.stat().st_uid == os.getuid() and path.stat().st_mode & 0o077 == 0,
             f"Owner-private retained {name} bytes required")
    raw = path.read_bytes()
    _require(len(raw) == record["bytes"] and _sha(raw) == record["sha256"],
             f"Retained {name} bytes differ from their receipt")
    return raw


def _read_volume_bytes(api, uri: str, expected: bytes) -> None:
    fd, local = tempfile.mkstemp(prefix="ashlar-a1-recovery-volume-")
    os.close(fd)
    try:
        api.copy("dbfs:" + uri, local, overwrite=True)
        observed = Path(local).read_bytes()
        _require(observed == expected and _sha(observed) == _sha(expected),
                 "Existing managed Volume artifact differs from retained exact bytes: "
                 + uri.rsplit("/", 1)[-1])
    finally:
        try:
            os.unlink(local)
        except FileNotFoundError:
            pass


def _write_state(path: Path, state: dict) -> None:
    runner._write_private_json(path, state)


def _validate_recovery_inputs(config_path: Path, recovery_receipt_path: Path,
                              output_root: Path) -> tuple[dict, dict, dict]:
    _require(config_path.is_absolute() and config_path.is_file()
             and not config_path.is_symlink(), "Exact original private trial config required")
    config = json.loads(config_path.read_text(encoding="utf-8"))
    payload, config = runner.build_payload(config)
    _require(config["maximum_wall_seconds"] == ORIGINAL_CAMPAIGN_SECONDS
             and config["trial_id"] == "a1-release-20261103-serverless-r1",
             "Recovery must retain the original 1,680-second campaign clock")
    cost = config.get("cost")
    _require(type(cost) is dict and config.get("trial", {}).get("cost") == cost
             and cost.get("estimated_total_dbu") == 140.0
             and cost.get("authorized_total_dbu") == 140.0
             and cost.get("estimated_total_usd") == 65.8
             and cost.get("cleanup_reserve_usd") == 20.0
             and cost.get("cleanup_reserve_dbu") == 0.0
             and cost.get("includes_startup_driver_verifier_handoff_and_cleanup") is True,
             "RC7 recovery cost envelope must bind 140 DBU plus the $20 cleanup reserve")
    _require(config["holder_bootstrap"]["python"] == sys.executable,
             "Run recovery under the exact original selected RC2 Python")
    _require(recovery_receipt_path.is_absolute() and recovery_receipt_path.is_file()
             and not recovery_receipt_path.is_symlink(),
             "Exact retained failed-run receipt required")
    receipt_root = recovery_receipt_path.parent.resolve(strict=True)
    _require(recovery_receipt_path.parent == receipt_root
             and recovery_receipt_path.stat().st_uid == os.getuid()
             and recovery_receipt_path.stat().st_mode & 0o077 == 0,
             "Owner-private original recovery receipt required")
    receipt = json.loads(recovery_receipt_path.read_text(encoding="utf-8"))
    _require(receipt.get("phase") == "recovery_required"
             and receipt.get("failure_type") == "ManagedA1LocalPinOwnerError"
             and receipt.get("failure") == "Committed Ashlar pin migrations required"
             and receipt.get("ack_attempted") is False,
             "Only the original pre-ACK missing-pin-migration failure is resumable")
    _require(receipt.get("schema") == config["catalog"] + "." + config["schema"]
             and receipt.get("volume") == config["volume_name"]
             and receipt.get("trial_id") == config["trial_id"]
             and receipt.get("workspace") == config["host"],
             "Recovery receipt does not bind the selected original trial")
    jobs = receipt.get("jobs")
    _require(type(jobs) is dict and set(jobs) == {"initializer", "a0-publisher"}
             and all(jobs[name].get("result_state") == "SUCCESS"
                     and jobs[name].get("run_now_state") == "terminal_success"
                     and jobs[name].get("job_create_state") == "deleted"
                     and type(jobs[name].get("job_id")) is int
                     and type(jobs[name].get("run_id")) is int
                     and type(jobs[name].get("task_run_id")) is int
                     for name in jobs),
             "Exact terminal initializer and A0 publisher jobs are required")
    _require(type(receipt.get("uc_genesis", {}).get("installation_inventory")) is dict
             and type(receipt.get("uc_table_id_baseline")) is dict
             and set(receipt["uc_table_id_baseline"]) == {
                 config["catalog"] + "." + config["schema"] + "." + role
                 for role in ("object_current", "edge_current", "tombstone",
                              "whole_source_history", "publication_manifest",
                              "publication_attempt_phase")},
             "Exact original six-role installation and UC identity baseline required")
    for name in ("a0_original_ab_genesis", "a0_held_source_snapshot",
                 "a0_frozen_request", "a0_publication_manifest",
                 "a0_destination_receipt", "a0_publisher_result",
                 "a0_source_prefix", "a0_selected_prefix", "uc_genesis_manifest"):
        _read_private_artifact(receipt, name, receipt_root)
    _require(not output_root.exists() and not output_root.is_symlink(),
             "Use one fresh private recovery output root")
    return config, payload, receipt


def _inert_preflight(config: dict, receipt: dict,
                     recovery_receipt_path: Path) -> dict:
    """Recompose exact retained A0 admission and retention bindings in memory."""
    import base64
    import inspect
    from ashlar_host.a1_ack_attempt import DurableA1AckAttemptStore
    from ashlar_host.managed_a1_detached_admission import admit_detached_a1_plan
    from ashlar_host.managed_current_a1_prefix_builder import build_managed_current_a1_prefix_evidence
    from ashlar_host.truss_detached_source_snapshot_codec import decode_detached_source_snapshot
    from ashlar_host.truss053_source_custody import detached_source_custody_digests
    from managed_a1_recovery_source_bootstrap import open_retained_source_hold

    _require(hasattr(DurableA1AckAttemptStore, "begin")
             and callable(open_retained_source_hold),
             "Durable ACK store and retained public-source recovery API required")
    _require(set(inspect.signature(open_retained_source_hold).parameters) == {
        "bootstrap_config_path", "expected_genesis_bytes", "expected_selection_bytes",
        "expected_request_bytes", "expected_publication_id", "expected_destination_id",
        "deadline_monotonic"}, "Closed retained-source recovery helper signature changed")
    root = recovery_receipt_path.parent.resolve(strict=True)
    genesis = _read_private_artifact(receipt, "a0_original_ab_genesis", root)
    snapshot = _read_private_artifact(receipt, "a0_held_source_snapshot", root)
    request = _read_private_artifact(receipt, "a0_frozen_request", root)
    initial_manifest = _read_private_artifact(receipt, "uc_genesis_manifest", root)
    manifest = _read_private_artifact(receipt, "a0_publication_manifest", root)
    destination_receipt = _read_private_artifact(receipt, "a0_destination_receipt", root)
    publisher_raw = _read_private_artifact(receipt, "a0_publisher_result", root)
    publisher = _json(publisher_raw, "retained A0 publisher result")
    actual_receipt, actual_request, actual_manifest, actual_receipt_bytes, vector, vector_sha = (
        runner._publisher_bundle(publisher,
            "managed-current-spark:" + config["expected_principal"], phase="a0"))
    _require((actual_request, actual_manifest, actual_receipt_bytes) ==
             (request, manifest, destination_receipt),
             "Retained A0 publisher output differs from exact frozen bindings")
    initial_value = _json(initial_manifest, "retained genesis manifest")
    phase_clock = runner._phase_clock_after_manifest(
        config["trial"]["materialized_at"], initial_value)
    plan = admit_detached_a1_plan(genesis_snapshot_bytes=genesis,
        genesis_manifest=initial_value, stream=config["trial"]["stream"],
        materialized_at=phase_clock, context=object())
    recomposed_request = json.dumps(plan.request, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False).encode("utf-8")
    _require(recomposed_request == request,
             "Retained A0 request does not recompose from the exact genesis and accepted manifest")
    detached = decode_detached_source_snapshot(snapshot)
    custody = detached_source_custody_digests(detached)
    _require(custody == receipt["source_hold"]["custody_digests"],
             "Retained A0 source snapshot custody digests differ from original receipt")
    prefix, prefix_sha, selected = build_managed_current_a1_prefix_evidence(
        genesis_bytes=genesis, source_snapshot_bytes=snapshot,
        initial_manifest_bytes=initial_manifest, request_bytes=request,
        publication_manifest_bytes=manifest,
        destination={"catalog": config["catalog"], "schema": config["schema"]},
        carrier_profile="ashlar-delta/0.4-xxhash64",
        materialization_clock_us=phase_clock, phase="a0")
    selected_raw = json.dumps(selected, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False).encode("utf-8")
    _require(prefix == _read_private_artifact(receipt, "a0_source_prefix", root)
             and selected_raw == _read_private_artifact(receipt, "a0_selected_prefix", root),
             "Retained A0 source-prefix evidence does not recompose byte-for-byte")
    task_trial = dict(config["trial"])
    task_trial["expected_runtime"] = config["expected_runtime"]
    task_trial["cost"] = config["cost"]
    task_trial["materialized_at"] = phase_clock
    task_trial["destination_id"] = config["trial"]["destination_id"]
    task_trial["source_custody_digests"] = custody
    bundle = runner._retention_bundle(trial=task_trial | {
            "verifier_timeout_seconds": config["verifier"]["timeout_seconds"]},
        protocol_profile=config["protocol_profile"], plan=plan,
        request_bytes=request, manifest_bytes=manifest,
        destination_receipt=destination_receipt, task_receipt=actual_receipt,
        prefix_bytes=prefix, prefix_sha=prefix_sha, selected_prefix=selected,
        source_snapshot=snapshot, custody_digests=custody)
    _require(_wire_vector(bundle.vector) == vector,
             "Retained A0 managed vector differs from exact retention selection")
    return {"status": "passed", "original_genesis_sha256": _sha(genesis),
        "original_source_selection_sha256": _sha(snapshot),
        "request_sha256": _sha(request), "manifest_sha256": _sha(manifest),
        "destination_receipt_sha256": _sha(destination_receipt),
        "vector_sha256": vector_sha, "source_prefix_sha256": prefix_sha,
        "catalog_only_recomposition": True, "source_or_delta_effects": False}


def _validate_terminal_predecessors(api, receipt: dict, config: dict,
                                    deadline: float, retained: dict) -> None:
    for name in ("initializer", "a0-publisher"):
        row = receipt["jobs"][name]
        run = runner._get_owned_run(api, row["run_id"], row["job_id"])
        _require(runner._terminal(run.get("state", {}))
                 and run.get("state", {}).get("result_state") == "SUCCESS",
                 f"Original {name} run no longer reads back as terminal success")
        result = runner._task_output(api, row["task_run_id"])
        if name == "a0-publisher":
            _require(json.dumps(result, sort_keys=True, separators=(",", ":"),
                                ensure_ascii=False, allow_nan=False).encode("utf-8")
                     == retained["publisher_result"],
                     "A0 publisher task output differs from retained successful output")
            runner._validate_task_runtime(result, config["expected_runtime"], "publisher")
        else:
            manifest = retained["genesis_manifest"]
            _require(result.get("profile") == "ashlar-managed-a1-initializer-result/0.1"
                     and result.get("predictiveOptimization") == "ENABLE"
                     and result.get("manifestSha256") == _sha(manifest)
                     and result.get("genesisManifestBytesBase64") ==
                         __import__("base64").b64encode(manifest).decode("ascii")
                     and result.get("installationInventory") ==
                         receipt["uc_genesis"]["installation_inventory"],
                     "Original initializer output differs from retained genesis and inventory")
            runner._validate_task_runtime(result, config["expected_runtime"], "initializer")
        _require(time.monotonic() < deadline,
                 "Recovery deadline expired while reconciling original successful Jobs")


def _volume_preflight(api, config, receipt, retained, deadline) -> tuple[str, list[str]]:
    from urllib.parse import quote
    volume_path = (f"/Volumes/{config['catalog']}/{config['schema']}/"
                   f"{config['volume_name']}")
    volume_name = f"{config['catalog']}.{config['schema']}.{config['volume_name']}"
    volume_api = "/api/2.1/unity-catalog/volumes/" + quote(volume_name, safe="")
    volume = api.call("get", volume_api)
    _require(volume.get("volume_id") == receipt["volume_id"]
             and volume.get("owner") == config["expected_principal"]
             and volume.get("volume_type") == "MANAGED"
             and volume.get("catalog_name") == config["catalog"]
             and volume.get("schema_name") == config["schema"],
             "Existing Volume ownership/identity differs from original receipt")
    schema = runner._list_schema(api, config["catalog"], config["schema"], deadline)
    _require(type(schema) is dict and schema.get("schema_id") == receipt["schema_id"]
             and schema.get("properties", {}).get("ashlar.trial_id") == config["trial_id"],
             "Existing schema identity/ownership marker differs from original receipt")
    inventory = receipt["uc_genesis"]["installation_inventory"]
    tables = runner._list_owned_tables(api, config, deadline)
    observed_ids = runner._admit_owned_uc_table_inventory(tables, inventory,
        catalog=config["catalog"], schema=config["schema"],
        principal=config["expected_principal"])
    _require(observed_ids == receipt["uc_table_id_baseline"],
             "Existing managed table IDs differ from the initializer baseline")
    objects = {
        "genesis.bin": retained["genesis"],
        "genesis-manifest.json": retained["genesis_manifest"],
        "a0-source-snapshot.bin": retained["a0_snapshot"],
        "a0-publisher-result.json": retained["publisher_result"],
        "source-prefix.json": retained["a0_prefix"],
        "selected-prefix.json": retained["a0_selected_prefix"],
    }
    for name, raw in objects.items():
        _read_volume_bytes(api, volume_path + "/" + name, raw)
    directory = api.call("get", "/api/2.0/fs/directories/" + quote(volume_path, safe="/"))
    contents = runner._directory_contents(directory, maximum_entries=16)
    actual_names = sorted(row["path"].rsplit("/", 1)[-1] for row in contents)
    _require(actual_names == sorted(objects),
             "Original Volume contains missing or unexpected files before recovery")
    _require(time.monotonic() < deadline, "Recovery deadline expired during Volume reconciliation")
    return volume_path, [volume_path + "/" + name for name in objects]


def _wire_vector(vector) -> dict:
    return {"authority": vector.authority, "scope_kind": vector.scope_kind,
            "scope_id": vector.scope_id, "custody_digest": vector.custody_digest,
            "targets": {name: list(target) for name, target in vector.targets.items()}}


def _require_snapshot_only_safe_watermark_drift(original: bytes, refreshed: bytes) -> None:
    """Require byte evidence to differ only in the provider's safe watermark.

    The bootstrap helper independently validates the closed genesis and
    selection binding. This second comparison makes the allowed difference
    explicit at the recovery boundary while retaining every page and catalog
    field used by publication and ACK custody.
    """
    from ashlar_host.truss_detached_source_snapshot_codec import decode_detached_source_snapshot

    before = decode_detached_source_snapshot(original)
    after = decode_detached_source_snapshot(refreshed)
    for name in ("catalog_revision", "catalog_registration_id", "catalog_registration_bytes",
                 "report_bytes", "view_bytes", "progress", "documents", "truss_version"):
        _require(getattr(before, name) == getattr(after, name),
                 "Fresh A0 snapshot changed non-page catalog evidence: " + name)
    left, right = before.source_page, after.source_page
    allowed_page_attributes = {"page_fields", "binding_sha256"}
    for name in ("source", "schedule_sha256", "publication_id", "destination_id",
                 "request_bytes", "request_sha256", "destination_registration_bytes",
                 "ack_scope_bytes", "identity", "maximum_facts", "maximum_bytes",
                 "page_content", "prior_bytes", "next_bytes"):
        _require(getattr(left, name) == getattr(right, name),
                 "Fresh A0 snapshot changed source page evidence: " + name)
    left_fields, right_fields = dict(left.page_fields), dict(right.page_fields)
    old_watermark = left_fields.pop("safe_watermark_xid", None)
    new_watermark = right_fields.pop("safe_watermark_xid", None)
    _require(old_watermark is not None and new_watermark is not None
             and type(old_watermark) is str and type(new_watermark) is str
             and old_watermark.isascii() and new_watermark.isascii()
             and old_watermark.isdecimal() and new_watermark.isdecimal()
             and str(int(old_watermark)) == old_watermark
             and str(int(new_watermark)) == new_watermark
             and int(new_watermark) >= int(old_watermark),
             "Fresh A0 source safe watermark must advance monotonically")
    _require(left_fields == right_fields,
             "Fresh A0 snapshot changed non-watermark source page fields")


def _require_genesis_only_safe_watermark_drift(original: bytes, refreshed: bytes) -> None:
    from ashlar_host.truss_detached_a1_genesis_codec import decode_detached_a1_genesis_snapshot

    decode_detached_a1_genesis_snapshot(original, expected_a_prior_cursor="0")
    decode_detached_a1_genesis_snapshot(refreshed, expected_a_prior_cursor="0")
    before, after = json.loads(original), json.loads(refreshed)
    old_sources = {item["source"]: item for item in before["sources"]}
    new_sources = {item["source"]: item for item in after["sources"]}
    _require(set(old_sources) == set(new_sources) == {"A", "B"},
             "Fresh genesis changed the source set")
    for source in ("A", "B"):
        old_page = old_sources[source]["page"]
        new_page = new_sources[source]["page"]
        old, new = old_page.get("safe_watermark_xid"), new_page.get("safe_watermark_xid")
        _require(type(old) is str and type(new) is str and old.isascii() and new.isascii()
                 and old.isdecimal() and new.isdecimal() and str(int(old)) == old
                 and str(int(new)) == new and int(new) >= int(old),
                 "Fresh genesis safe watermarks must advance monotonically")
        old_page["safe_watermark_xid"] = new_page["safe_watermark_xid"] = "<frontier>"
    before["binding_sha256"] = after["binding_sha256"] = "<verified>"
    _require(before == after,
             "Fresh genesis changed fields beyond monotonic safe watermarks")


def _run_verifier_phase(*, api, config, payload, state, state_path, recovery_root,
                        holder, pin_stages, phase, trial, volume_path, source_snapshot,
                        source_snapshot_uri,
                        custody_digests, request_bytes, manifest_bytes, receipt_bytes,
                        genesis_bytes, genesis_manifest_bytes,
                        publisher_result_raw, publisher_result_uri,
                        prefix_bytes, prefix_sha, selected_prefix_raw,
                        vector, vector_sha, deadline, work_deadline,
                        run_ids, ack_store_root):
    """Perform open/pre-ACK proof, durable exact ACK, then post-ACK close proof."""
    from ashlar_host.a1_ack_attempt import DurableA1AckAttemptStore

    prefix_name = "source-prefix.json" if phase == "a0" else "a1-source-prefix.json"
    selected_name = "selected-prefix.json" if phase == "a0" else "a1-selected-prefix.json"
    selected_prefix_bytes = selected_prefix_raw
    _read_volume_bytes(api, source_snapshot_uri, source_snapshot)
    if phase == "a1":
        _record_new_artifact(recovery_root, state_path, state, "a1_source_prefix", prefix_bytes)
        _put_new_volume(api, volume_path + "/" + prefix_name, prefix_bytes)
        _record_new_artifact(recovery_root, state_path, state, "a1_selected_prefix", selected_prefix_bytes)
        _put_new_volume(api, volume_path + "/" + selected_name, selected_prefix_bytes)
    prefix_artifact = {"sha256": _sha(prefix_bytes)}
    selected_artifact = {"sha256": _sha(selected_prefix_bytes)}
    interval_id = str(uuid.uuid4())
    interval_started = time.monotonic()
    params = {"trial": trial, "protocol_profile": config["protocol_profile"],
        "phase": phase,
        "genesis_volume_uri": volume_path + ("/genesis.bin" if phase == "a0" else "/a1-source-bundle.bin"),
        "genesis_sha256": _sha(genesis_bytes),
        "genesis_manifest_volume_uri": volume_path + ("/genesis-manifest.json" if phase == "a0" else "/a0-manifest.json"),
        "genesis_manifest_sha256": _sha(genesis_manifest_bytes),
        "a1_snapshot_volume_uri": source_snapshot_uri,
        "a1_snapshot_sha256": _sha(source_snapshot),
        "publisher_result_volume_uri": publisher_result_uri,
        "publisher_result_sha256": _sha(publisher_result_raw),
        "prefix_volume_uri": volume_path + "/" + prefix_name,
        "prefix_sha256": prefix_artifact["sha256"],
        "selected_prefix_volume_uri": volume_path + "/" + selected_name,
        "selected_prefix_sha256": selected_artifact["sha256"],
        "source_rpc_directory": volume_path, "post_ack_close": False,
        "operational_interval_id": interval_id}
    stage = phase + "-verifier"
    verifier_id, verifier_task_id, _, verifier_output = runner._run_stage(api, config,
        stage, payload["verifier"], {"task_parameters_json": json.dumps(params,
            sort_keys=True, separators=(",", ":"), ensure_ascii=False)}, work_deadline,
        on_poll=lambda: runner._service_source_observer_rpc(api, holder, volume_path,
            work_deadline, expected_vector_sha256=vector_sha,
            expected_custody_digests=custody_digests, pin_stage=pin_stages),
        cleanup_deadline=deadline, recovery_state=state, recovery_path=state_path,
        config_key="verifier")
    run_ids[stage] = verifier_id
    runner._validate_task_runtime(verifier_output, config["expected_runtime"], "verifier")
    pre_ack_received = time.monotonic()
    holder.renew(work_deadline)
    _require(pin_stages.stages == ["open", "pre_ack"] and pin_stages.handle is not None,
             "Native pin must remain open after open and pre-ACK observations")
    proof = runner._verify_terminal_proof(verifier_output, verifier_id, verifier_task_id,
        request_bytes, manifest_bytes, receipt_bytes, vector_sha, vector,
        phase=phase, observation_phase="pre_ack", interval_id=interval_id)
    proof_name = phase + "_pre_ack_proof"
    state[proof_name] = proof
    _record_new_artifact(recovery_root, state_path, state,
        phase + "_pre_ack_verifier_result", json.dumps(verifier_output, sort_keys=True,
            separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode())
    _write_state(state_path, state)

    holder.bind(manifest_bytes=manifest_bytes, vector=vector,
                receipt_bytes=receipt_bytes, deadline=deadline)
    attempt_dir = ack_store_root / phase / "attempts"
    attempt_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
    os.chmod(attempt_dir.parent, 0o700)
    os.chmod(attempt_dir, 0o700)
    store = DurableA1AckAttemptStore(attempt_dir)
    from ashlar_host.truss_detached_source_snapshot_codec import decode_detached_source_snapshot
    detached = decode_detached_source_snapshot(source_snapshot)
    page = detached.source_page
    store.begin(selection_bytes=source_snapshot, request_bytes=request_bytes,
        manifest_bytes=manifest_bytes, receipt_bytes=receipt_bytes,
        prior_checkpoint_bytes=page.prior_bytes, next_checkpoint_bytes=page.next_bytes,
        page_sha256=page.page_fields["sha256"], destination_id=trial["destination_id"],
        publication_id=json.loads(manifest_bytes)["publication_id"])
    prepared = store.load()
    _record_new_artifact(recovery_root, state_path, state, phase + "_ack_attempt",
                         store.path.read_bytes())
    state[phase + "_ack"] = {"state": "durable_attempt_prepared",
        "attempt_directory": str(store.directory),
        "attempt_sha256": _sha(store.path.read_bytes()),
        "selection_sha256": prepared.selection_sha256,
        "request_sha256": prepared.request_sha256,
        "manifest_sha256": prepared.manifest_sha256,
        "receipt_sha256": prepared.receipt_sha256,
        "publication_id": prepared.publication_id}
    _write_state(state_path, state)
    _require(deadline - time.monotonic() >= config["cleanup_reserve_seconds"],
             "Do not dispatch a protected ACK without the full cleanup reserve")
    pin_stages.renew_for_ack()
    state[phase + "_ack"]["state"] = "dispatch_intent"
    state["ack_attempted"] = True
    state["unresolved_ack"] = True
    _write_state(state_path, state)
    settlement = holder.acknowledge_once(store, attempt_directory=store.directory,
                                         deadline=deadline)
    state[phase + "_ack"]["state"] = "acknowledged_matched_post_ack_close_pending"
    state["ack_outcome"] = settlement.outcome
    _write_state(state_path, state)
    runner._validate_ack_settlement(settlement, request_bytes=request_bytes,
        manifest_bytes=manifest_bytes, snapshot_bytes=source_snapshot,
        next_checkpoint_bytes=page.next_bytes, receipt_bytes=receipt_bytes)
    close_id, close_task_id, close_output, close_proof = runner._run_post_ack_close(
        api, config, payload["verifier"], params, work_deadline,
        cleanup_deadline=deadline, stage=phase + "-post-ack-close", phase=phase,
        request_bytes=request_bytes, manifest_bytes=manifest_bytes,
        receipt_bytes=receipt_bytes, vector_sha256=vector_sha, vector=vector,
        holder=holder, rpc_dir=volume_path, custody_digests=custody_digests,
        pin_stages=pin_stages, recovery_state=state, recovery_path=state_path)
    runner._validate_task_runtime(close_output, config["expected_runtime"], "verifier")
    final_interval, terminal_proof = runner._compose_operational_interval(
        proof, close_proof, interval_started=interval_started,
        pre_ack_received=pre_ack_received, close_received=time.monotonic(),
        interval_id=interval_id)
    run_ids[phase + "-post-ack-close"] = close_id
    result = {"phase": phase, "verifier_run_id": verifier_id,
        "post_ack_close_run_id": close_id,
        "pre_ack_proof": proof, "post_ack_close_proof": close_proof,
        "operational_interval": final_interval, "terminal_proof": terminal_proof,
        "settlement": {"outcome": settlement.outcome,
            "selection_sha256": settlement.selection_sha256,
            "request_sha256": settlement.request_sha256,
            "manifest_sha256": settlement.manifest_sha256}}
    state[phase + "_ack"]["post_ack_close_proof"] = close_proof
    state[phase + "_ack"]["state"] = "matched_post_ack_close_observed"
    state["unresolved_ack"] = False
    state[phase + "_retention_interval"] = final_interval
    state[phase + "_terminal_proof"] = terminal_proof
    _write_state(state_path, state)
    pin_stages.close_after_ack()
    return result, settlement, store, attempt_dir, close_output


def _record_new_artifact(root: Path, state_path: Path, state: dict,
                         name: str, raw: bytes) -> dict:
    return runner._record_private_artifact(root, state_path, state, name, raw)


def _put_new_volume(api, uri: str, raw: bytes) -> dict:
    return runner._put_volume_bytes(api, uri, raw)


def _launch(config_path: Path, recovery_receipt_path: Path, output_root: Path,
            profile: str) -> dict:
    import threading
    from urllib.parse import quote

    config, payload, original = _validate_recovery_inputs(
        config_path, recovery_receipt_path, output_root)
    _require(profile == runner.PROFILE,
             "Recovery is pinned to the selected original Databricks profile")
    _require(threading.current_thread() is threading.main_thread(),
             "Bounded recovery must run on the main thread for hard deadline supervision")
    output_root.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    _require(not output_root.exists() and not output_root.is_symlink(),
             "Recovery output root must be new")
    output_root.mkdir(mode=0o700)
    os.chmod(output_root, 0o700)
    state_path = output_root / "recovery.json"
    state = {"profile": PROFILE, "status": "running",
        "phase": "read_only_reconciliation", "original_recovery_receipt":
            str(recovery_receipt_path),
        "original_recovery_receipt_sha256": _sha(recovery_receipt_path.read_bytes()),
        "original_trial_id": original["trial_id"],
        "original_schema_id": original["schema_id"],
        "original_volume_id": original["volume_id"],
        "ack_attempted": False, "source_ack_outcome": "not_attempted",
        "delta_replay_or_write": False, "initializer_replayed": False,
        "a0_publisher_replayed": False, "jobs": {}, "known_run_ids": {},
        "private_artifacts": {}, "cleanup_errors": [],
        "maximum_readonly_admission_seconds": MAX_READONLY_ADMISSION_SECONDS,
        "maximum_active_recovery_seconds": MAX_RECOVERY_SECONDS,
        "original_campaign_active_seconds_ceiling": ORIGINAL_CAMPAIGN_SECONDS,
        "retired_recovery_windows": [dict(item) for item in RETIRED_RECOVERY_WINDOWS],
        "cumulative_active_seconds_ceiling": MAX_CUMULATIVE_ACTIVE_SECONDS,
        "prior_active_seconds_ceiling": PRIOR_ACTIVE_SECONDS_CEILING,
        "cost_envelope": config["cost"]}
    _write_state(state_path, state)

    preflight_start = time.monotonic()
    preflight_deadline = preflight_start + MAX_READONLY_ADMISSION_SECONDS
    deadline = preflight_deadline
    work_deadline = preflight_deadline
    api = runner.DatabricksCLI(profile)
    api.deadline = deadline
    holder = None
    pin_owner = None
    pin_stages = None
    pin_pg_ctl_lock = None
    unresolved_ack = False
    a0_ack_done = False
    a1_ack_done = False
    active_start = None
    readonly_elapsed = None
    installation_inventory = original["uc_genesis"]["installation_inventory"]
    a0_pin_release = None
    run_ids = {}
    source_reselection = None
    result = {"profile": PROFILE, "status": "running",
        "original_recovery_receipt_sha256": state["original_recovery_receipt_sha256"],
        "original_trial_id": original["trial_id"], "schema": original["schema"],
        "schema_id": original["schema_id"], "volume_id": original["volume_id"],
        "a0_ack_replayed": False, "a0_publisher_replayed": False,
        "initializer_replayed": False, "delta_replay_or_write": False,
        "original_campaign_active_seconds_ceiling": ORIGINAL_CAMPAIGN_SECONDS,
        "retired_recovery_windows": [dict(item) for item in RETIRED_RECOVERY_WINDOWS],
        "prior_active_seconds_ceiling": PRIOR_ACTIVE_SECONDS_CEILING,
        "remaining_active_recovery_seconds": MAX_RECOVERY_SECONDS,
        "cumulative_active_seconds_ceiling": MAX_CUMULATIVE_ACTIVE_SECONDS,
        "cost_envelope": config["cost"],
        "source_ack": [], "jobs": {}, "cleanup_errors": []}

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    def expired(_signum, _frame):
        phase = state.get("phase", "read_only_reconciliation")
        if phase == "read_only_reconciliation":
            raise TimeoutError("Read-only managed A1 recovery admission exceeded 120 seconds")
        raise TimeoutError("Managed A1 active recovery exceeded its 1,230-second wall limit")
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, MAX_READONLY_ADMISSION_SECONDS)
    try:
        auth = api.auth_readback()
        runner._admit_auth_readback_host(auth, config["host"])
        runner._check_artifact_lock(api, config)
        _validate_terminal_predecessors(api, original, config, work_deadline, {
            "publisher_result": _read_private_artifact(original, "a0_publisher_result",
                recovery_receipt_path.parent.resolve(strict=True)),
            "genesis_manifest": _read_private_artifact(original, "uc_genesis_manifest",
                recovery_receipt_path.parent.resolve(strict=True))})
        schema = runner._list_schema(api, config["catalog"], config["schema"], work_deadline)
        _require(type(schema) is dict and schema.get("schema_id") == original["schema_id"]
                 and schema.get("properties", {}).get("ashlar.trial_id") == config["trial_id"],
                 "Original schema identity/ownership marker changed")

        original_root = recovery_receipt_path.parent.resolve(strict=True)
        retained = {
            "genesis": _read_private_artifact(original, "a0_original_ab_genesis", original_root),
            "a0_snapshot": _read_private_artifact(original, "a0_held_source_snapshot", original_root),
            "request": _read_private_artifact(original, "a0_frozen_request", original_root),
            "a0_manifest": _read_private_artifact(original, "a0_publication_manifest", original_root),
            "a0_receipt": _read_private_artifact(original, "a0_destination_receipt", original_root),
            "publisher_result": _read_private_artifact(original, "a0_publisher_result", original_root),
            "a0_prefix": _read_private_artifact(original, "a0_source_prefix", original_root),
            "a0_selected_prefix": _read_private_artifact(original, "a0_selected_prefix", original_root),
            "genesis_manifest": _read_private_artifact(original, "uc_genesis_manifest", original_root),
        }
        _require(_sha(retained["genesis"]) == original["source_bootstrap"]["genesis_sha256"]
                 and _sha(retained["request"]) == original["frozen_a0_metadata_request"]["request_sha256"]
                 and _sha(retained["a0_manifest"]) == original["private_artifacts"]["a0_publication_manifest"]["sha256"]
                 and _sha(retained["a0_receipt"]) == original["private_artifacts"]["a0_destination_receipt"]["sha256"]
                 and _sha(retained["publisher_result"]) == original["a0_publisher_result_artifact"]["sha256"],
                 "Exact A0 source/publication artifacts do not match original recovery anchors")
        publisher_output = _json(retained["publisher_result"], "A0 publisher result")
        runner._validate_task_runtime(publisher_output, config["expected_runtime"], "publisher")
        (a0_receipt, a0_request, a0_manifest, a0_receipt_bytes, a0_vector,
         a0_vector_sha) = runner._publisher_bundle(publisher_output,
            "managed-current-spark:" + config["expected_principal"], phase="a0")
        _require(a0_request == retained["request"]
                 and a0_manifest == retained["a0_manifest"]
                 and a0_receipt_bytes == retained["a0_receipt"],
                 "A0 exact publisher result differs from retained request/manifest/receipt")
        _require(a0_receipt.get("publicationId") ==
                 original["frozen_a0_metadata_request"]["publication_id"],
                 "A0 retained destination receipt publication identity changed")
        a0_manifest_value = _json(a0_manifest, "A0 publication manifest")
        genesis_manifest_value = _json(retained["genesis_manifest"], "genesis manifest")
        phase_clock = runner._phase_clock_after_manifest(
            config["trial"]["materialized_at"], genesis_manifest_value)
        from ashlar_host.managed_a1_detached_admission import admit_detached_a1_plan
        plan = admit_detached_a1_plan(genesis_snapshot_bytes=retained["genesis"],
            genesis_manifest=genesis_manifest_value, stream=config["trial"]["stream"],
            materialized_at=phase_clock, context=object())
        expected_request = json.dumps(plan.request, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False).encode("utf-8")
        _require(expected_request == retained["request"],
                 "Retained A0 request does not recompose from exact genesis and accepted manifest")
        state.update({"phase": "original_artifacts_reconciled",
            "a0_request_sha256": _sha(a0_request), "a0_manifest_sha256": _sha(a0_manifest),
            "a0_receipt_sha256": _sha(a0_receipt_bytes), "a0_vector_sha256": a0_vector_sha,
            "genesis_manifest_sha256": _sha(retained["genesis_manifest"]),
            "prior_terminal_jobs_reconciled": True})
        _write_state(state_path, state)

        tables = runner._list_owned_tables(api, config, work_deadline)
        observed_uc_ids = runner._admit_owned_uc_table_inventory(tables, installation_inventory,
            catalog=config["catalog"], schema=config["schema"],
            principal=config["expected_principal"])
        _require(observed_uc_ids == original["uc_table_id_baseline"],
                 "Original six-role UC IDs differ from retained genesis baseline")
        volume_path, original_volume_uris = _volume_preflight(
            api, config, original, retained, work_deadline)
        a0_recovery_snapshot_uri = volume_path + "/a0-recovery-source-snapshot.bin"
        state["existing_volume_files_verified"] = [uri.rsplit("/", 1)[-1]
                                                   for uri in original_volume_uris]
        _write_state(state_path, state)

        bootstrap_path = Path(original["source_bootstrap_config_path"])
        _require(bootstrap_path.is_file() and not bootstrap_path.is_symlink()
                 and bootstrap_path.stat().st_mode & 0o077 == 0,
                 "Original stopped source bootstrap config is not owner-private")
        readonly_elapsed = time.monotonic() - preflight_start
        _require(time.monotonic() < preflight_deadline,
                 "Read-only managed A1 recovery admission exceeded 120 seconds")
        active_start = time.monotonic()
        deadline = active_start + MAX_RECOVERY_SECONDS
        work_deadline = deadline - config["cleanup_reserve_seconds"]
        api.deadline = deadline
        state.update({"phase": "active_recovery", "readonly_admission_seconds": readonly_elapsed,
            "active_recovery_started_monotonic": active_start,
            "active_recovery_deadline_monotonic": deadline})
        _write_state(state_path, state)
        signal.setitimer(signal.ITIMER_REAL, MAX_RECOVERY_SECONDS)
        from managed_a1_recovery_source_bootstrap import open_retained_source_hold
        holder = open_retained_source_hold(bootstrap_config_path=bootstrap_path,
            expected_genesis_bytes=retained["genesis"],
            expected_selection_bytes=retained["a0_snapshot"],
            expected_request_bytes=a0_request,
            expected_publication_id=a0_receipt["publicationId"],
            expected_destination_id=config["trial"]["destination_id"],
            deadline_monotonic=work_deadline)
        reselection = holder.reselection_evidence
        fresh_a0_snapshot = holder.snapshot_bytes
        fresh_custody = holder.custody_digests
        _require(holder.phase == "a0"
                 and holder.original_genesis_bytes == retained["genesis"]
                 and reselection.get("original_genesis_sha256") == _sha(retained["genesis"])
                 and reselection.get("original_selection_sha256") == _sha(retained["a0_snapshot"])
                 and reselection.get("fresh_genesis_sha256") == _sha(holder.genesis_bytes)
                 and reselection.get("fresh_selection_sha256") == _sha(fresh_a0_snapshot)
                 and reselection.get("only_monotonic_safe_watermark_drift_admitted") is True,
                 "Fresh public source holder did not bind the retained genesis and snapshot")
        _require_genesis_only_safe_watermark_drift(retained["genesis"], holder.genesis_bytes)
        _require_snapshot_only_safe_watermark_drift(retained["a0_snapshot"], fresh_a0_snapshot)
        _record_new_artifact(output_root, state_path, state,
                             "a0_recovery_genesis", holder.genesis_bytes)
        old_custody = original["source_hold"]["custody_digests"]
        _require(type(old_custody) is dict and type(fresh_custody) is dict
                 and set(old_custody) == set(fresh_custody)
                 and all(old_custody[key] == fresh_custody[key]
                         for key in old_custody if key != "retained_page"),
                 "Fresh A0 custody differs beyond the permitted safe-watermark page digest")
        _record_new_artifact(output_root, state_path, state,
                             "a0_recovery_source_snapshot", fresh_a0_snapshot)
        state["source_hold"] = {"state": "fresh_same_page_hold_open",
            "original_genesis_sha256": _sha(retained["genesis"]),
            "fresh_genesis_sha256": _sha(holder.genesis_bytes),
            "original_selection_sha256": _sha(retained["a0_snapshot"]),
            "fresh_selection_sha256": _sha(fresh_a0_snapshot),
            "fresh_snapshot_volume_uri": a0_recovery_snapshot_uri,
            "original_custody_digests": old_custody,
            "fresh_custody_digests": fresh_custody,
            "reselection_evidence": reselection,
            "original_hold_gap": True}
        source_reselection = {"original_hold_gap": True,
            "original_genesis_sha256": _sha(retained["genesis"]),
            "fresh_genesis_sha256": _sha(holder.genesis_bytes),
            "original_selection_sha256": _sha(retained["a0_snapshot"]),
            "fresh_selection_sha256": _sha(fresh_a0_snapshot),
            "original_custody_digests": old_custody,
            "fresh_custody_digests": fresh_custody,
            "fresh_snapshot_volume_uri": a0_recovery_snapshot_uri,
            "watermark_drift_evidence": reselection}
        _write_state(state_path, state)

        from ashlar_host.managed_current_a1_prefix_builder import build_managed_current_a1_prefix_evidence
        prefix_bytes, prefix_sha, selected_prefix = build_managed_current_a1_prefix_evidence(
            genesis_bytes=retained["genesis"], source_snapshot_bytes=fresh_a0_snapshot,
            initial_manifest_bytes=retained["genesis_manifest"], request_bytes=a0_request,
            publication_manifest_bytes=a0_manifest,
            destination={"catalog": config["catalog"], "schema": config["schema"]},
            carrier_profile="ashlar-delta/0.4-xxhash64",
            materialization_clock_us=phase_clock, phase="a0")
        selected_raw = json.dumps(selected_prefix, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False).encode("utf-8")
        _require(prefix_bytes == retained["a0_prefix"]
                 and selected_raw == retained["a0_selected_prefix"],
                 "A0 source prefix differs from original exact selection/publication evidence")
        prefix_uri = volume_path + "/source-prefix.json"
        selected_uri = volume_path + "/selected-prefix.json"
        publisher_result_uri = volume_path + "/a0-publisher-result.json"
        _put_new_volume(api, a0_recovery_snapshot_uri, fresh_a0_snapshot)

        task_trial = dict(config["trial"])
        task_trial["expected_runtime"] = config["expected_runtime"]
        task_trial["cost"] = config["cost"]
        task_trial["materialized_at"] = phase_clock
        task_trial["source_custody_digests"] = fresh_custody
        task_trial["destination_id"] = config["trial"]["destination_id"]
        bundle = runner._retention_bundle(trial=task_trial | {
                "verifier_timeout_seconds": config["verifier"]["timeout_seconds"]},
            protocol_profile=config["protocol_profile"], plan=plan,
            request_bytes=a0_request, manifest_bytes=a0_manifest,
            destination_receipt=a0_receipt_bytes, task_receipt=a0_receipt,
            prefix_bytes=prefix_bytes, prefix_sha=prefix_sha,
            selected_prefix=selected_prefix, source_snapshot=fresh_a0_snapshot,
            custody_digests=fresh_custody)
        _require(_wire_vector(bundle.vector) == a0_vector,
                 "A0 retained publisher vector differs from the current retention selection")

        pin_root = output_root / "pin-owner"
        pin_root.mkdir(mode=0o700)
        os.chmod(pin_root, 0o700)
        pin_timeout = min(900, int(work_deadline - time.monotonic()
                                   - runner.POST_ACK_CLEANUP_RESERVE_SECONDS))
        _require(pin_timeout >= 31, "Recovery deadline cannot cover the bounded native pin")
        from importlib.resources import files
        from ashlar_host.managed_a1_postgres_cleanup import inspect_pg_ctl
        from ashlar_host.managed_a1_local_pin_owner import ManagedA1LocalPinOwner
        pin_pg_bin = Path(str(files("pgserver"))) / "pginstall" / "bin"
        pin_pg_ctl_lock = inspect_pg_ctl(pin_pg_bin / "pg_ctl")
        pin_owner = ManagedA1LocalPinOwner(pin_root / "pgdata", bundle.selection,
            bundle.vector, native_timeout_seconds=pin_timeout,
            deadline_monotonic=work_deadline)
        pin_owner.__enter__()
        pin_registration = pin_owner.register()
        state["pin_registry"] = {"a0_registration": pin_registration,
            "data_directory": str(pin_owner.data_directory), "state": "a0_registered"}
        _write_state(state_path, state)
        pin_stages = runner._LocalPinStages(pin_owner, lifetime_seconds=pin_timeout)

        # A0 publication already exists. Only its verifier and protected ACK run here.
        state["phase"] = "a0_reverification"
        _write_state(state_path, state)
        a0_result, a0_settlement, a0_store, a0_attempt_dir, _ = _run_verifier_phase(
            api=api, config=config, payload=payload, state=state, state_path=state_path,
            recovery_root=output_root, holder=holder, pin_stages=pin_stages,
            phase="a0", trial=task_trial, volume_path=volume_path,
            source_snapshot=fresh_a0_snapshot, source_snapshot_uri=a0_recovery_snapshot_uri,
            custody_digests=fresh_custody,
            request_bytes=a0_request, manifest_bytes=a0_manifest,
            receipt_bytes=a0_receipt_bytes, genesis_bytes=retained["genesis"],
            genesis_manifest_bytes=retained["genesis_manifest"],
            publisher_result_raw=retained["publisher_result"],
            publisher_result_uri=publisher_result_uri, prefix_bytes=prefix_bytes,
            prefix_sha=prefix_sha, selected_prefix_raw=selected_raw,
            vector=a0_vector, vector_sha=a0_vector_sha, deadline=deadline,
            work_deadline=work_deadline, run_ids=run_ids, ack_store_root=output_root / "ack")
        a0_pin_release = pin_owner.release_current_registration()
        state["pin_registry"].update({"a0_release": a0_pin_release,
            "state": "a0_released_a1_registration_pending"})
        _write_state(state_path, state)
        holder.finish_ack(a0_settlement, attempt_directory=a0_attempt_dir, deadline=deadline)
        a0_ack_done = True
        unresolved_ack = False
        state["unresolved_ack"] = False
        state["a0_ack"]["state"] = "acknowledged_exactly_reconciled"
        state["phase"] = "a0_acknowledged_advancing_to_a1"
        _write_state(state_path, state)

        a1_genesis = holder.advance_after_a0_ack(a0_settlement, work_deadline)
        _record_new_artifact(output_root, state_path, state, "a1_continuation_genesis", a1_genesis)
        a0_manifest_value = _json(a0_manifest, "A0 manifest")
        task_trial["materialized_at"] = runner._phase_clock_after_manifest(
            phase_clock, a0_manifest_value)
        from ashlar_host.managed_a1_detached_admission import admit_detached_a1_plan
        a1_plan = admit_detached_a1_plan(genesis_snapshot_bytes=a1_genesis,
            genesis_manifest=a0_manifest_value, stream=task_trial["stream"],
            materialized_at=task_trial["materialized_at"], context=object(), continuation=True)
        a1_request = json.dumps(a1_plan.request, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False).encode("utf-8")
        a1_publication_id = a1_plan.publication_id
        _record_new_artifact(output_root, state_path, state, "a1_frozen_request", a1_request)
        state["a1_request"] = {"publication_id": a1_publication_id,
            "request_sha256": _sha(a1_request)}
        _write_state(state_path, state)
        a1_snapshot, a1_custody = holder.open_a1(a1_request, a1_publication_id,
            task_trial["destination_id"], work_deadline)
        from ashlar_host.truss_detached_source_snapshot_codec import decode_detached_source_snapshot
        a1_detached = decode_detached_source_snapshot(a1_snapshot)
        _require(a1_detached.source_page.outcome == "transaction",
                 "A1 must be one actual public Truss transaction after the matched A0 ACK")
        holder.owner.start_heartbeat()
        _record_new_artifact(output_root, state_path, state, "a1_source_snapshot", a1_snapshot)
        a1_genesis_uri = volume_path + "/a1-source-bundle.bin"
        a1_manifest_uri = volume_path + "/a0-manifest.json"
        a1_snapshot_uri = volume_path + "/a1-source-snapshot.bin"
        a1_genesis_artifact = _put_new_volume(api, a1_genesis_uri, a1_genesis)
        a1_manifest_artifact = _put_new_volume(api, a1_manifest_uri, a0_manifest)
        a1_snapshot_artifact = _put_new_volume(api, a1_snapshot_uri, a1_snapshot)
        task_trial["source_custody_digests"] = a1_custody
        a1_common = {"trial": task_trial, "protocol_profile": config["protocol_profile"],
            "phase": "a1", "genesis_volume_uri": a1_genesis_uri,
            "genesis_sha256": a1_genesis_artifact["sha256"],
            "genesis_manifest_volume_uri": a1_manifest_uri,
            "genesis_manifest_sha256": a1_manifest_artifact["sha256"],
            "a1_snapshot_volume_uri": a1_snapshot_uri,
            "a1_snapshot_sha256": a1_snapshot_artifact["sha256"]}
        a1_pub_id, a1_pub_task_id, _, a1_output = runner._run_stage(api, config,
            "a1-publisher", payload["publisher"], {"task_parameters_json": json.dumps(
                a1_common, sort_keys=True, separators=(",", ":"), ensure_ascii=False)},
            work_deadline, cleanup_deadline=deadline, recovery_state=state,
            recovery_path=state_path, config_key="publisher")
        run_ids["a1-publisher"] = a1_pub_id
        runner._validate_task_runtime(a1_output, config["expected_runtime"], "publisher")
        a1_output_raw = json.dumps(a1_output, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False).encode("utf-8")
        _record_new_artifact(output_root, state_path, state, "a1_publisher_result", a1_output_raw)
        (a1_receipt, a1_request_actual, a1_manifest, a1_receipt_bytes, a1_vector,
         a1_vector_sha) = runner._publisher_bundle(a1_output,
            "managed-current-spark:" + config["expected_principal"], phase="a1")
        _require(a1_request_actual == a1_request,
                 "A1 publisher request differs from the current post-A0 continuation plan")
        _require(_json(a1_manifest, "A1 manifest").get("recorded_at") ==
                 task_trial["materialized_at"],
                 "A1 manifest clock differs from the frozen continuation plan")
        _record_new_artifact(output_root, state_path, state, "a1_publication_manifest", a1_manifest)
        _record_new_artifact(output_root, state_path, state, "a1_destination_receipt", a1_receipt_bytes)
        a1_result_uri = volume_path + "/a1-publisher-result.json"
        a1_result_artifact = _put_new_volume(api, a1_result_uri, a1_output_raw)
        from ashlar_host.managed_current_a1_prefix_builder import build_managed_current_a1_prefix_evidence
        a1_prefix, a1_prefix_sha, a1_selected = build_managed_current_a1_prefix_evidence(
            genesis_bytes=retained["genesis"], source_snapshot_bytes=a1_snapshot,
            initial_manifest_bytes=a0_manifest, request_bytes=a1_request,
            publication_manifest_bytes=a1_manifest,
            destination={"catalog": config["catalog"], "schema": config["schema"]},
            carrier_profile="ashlar-delta/0.4-xxhash64",
            materialization_clock_us=task_trial["materialized_at"])
        a1_selected_raw = json.dumps(a1_selected, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False).encode("utf-8")
        a1_bundle = runner._retention_bundle(trial=task_trial | {
                "verifier_timeout_seconds": config["verifier"]["timeout_seconds"]},
            protocol_profile=config["protocol_profile"], plan=a1_plan,
            request_bytes=a1_request, manifest_bytes=a1_manifest,
            destination_receipt=a1_receipt_bytes, task_receipt=a1_receipt,
            prefix_bytes=a1_prefix, prefix_sha=a1_prefix_sha,
            selected_prefix=a1_selected, source_snapshot=a1_snapshot,
            custody_digests=a1_custody)
        _require(_wire_vector(a1_bundle.vector) == a1_vector,
                 "A1 retained publisher vector differs from its retention selection")
        a1_pin_timeout = min(900, int(work_deadline - time.monotonic()
                                      - runner.POST_ACK_CLEANUP_RESERVE_SECONDS))
        _require(a1_pin_timeout >= 31, "Recovery deadline cannot cover A1 native pin interval")
        a1_pin_selection = pin_owner.select_next_phase(a1_bundle.selection,
            a1_bundle.vector, native_timeout_seconds=a1_pin_timeout)
        a1_registration = pin_owner.register()
        state["pin_registry"].update({"a1_phase": a1_pin_selection,
            "a1_registration": a1_registration, "state": "a1_registered"})
        _write_state(state_path, state)
        pin_stages = runner._LocalPinStages(pin_owner, lifetime_seconds=a1_pin_timeout)
        interval_trial = dict(task_trial)
        a1_result, a1_settlement, a1_store, a1_attempt_dir, _ = _run_verifier_phase(
            api=api, config=config, payload=payload, state=state, state_path=state_path,
            recovery_root=output_root, holder=holder, pin_stages=pin_stages,
            phase="a1", trial=interval_trial, volume_path=volume_path,
            source_snapshot=a1_snapshot, source_snapshot_uri=a1_snapshot_uri,
            custody_digests=a1_custody,
            request_bytes=a1_request, manifest_bytes=a1_manifest,
            receipt_bytes=a1_receipt_bytes, genesis_bytes=a1_genesis,
            genesis_manifest_bytes=a0_manifest, publisher_result_raw=a1_output_raw,
            publisher_result_uri=a1_result_uri, prefix_bytes=a1_prefix,
            prefix_sha=a1_prefix_sha, selected_prefix_raw=a1_selected_raw,
            vector=a1_vector, vector_sha=a1_vector_sha, deadline=deadline,
            work_deadline=work_deadline, run_ids=run_ids, ack_store_root=output_root / "ack")
        a1_release = pin_owner.release_current_registration()
        state["pin_registry"].update({"a1_release": a1_release, "state": "a1_released"})
        _write_state(state_path, state)
        pin_owner.__exit__(None, None, None)
        pin_cleanup = runner._cleanup_pin_pgdata(pin_owner, pin_pg_ctl_lock, deadline)
        pin_owner = None
        holder.finish_ack(a1_settlement, attempt_directory=a1_attempt_dir, deadline=deadline)
        a1_ack_done = True
        unresolved_ack = False
        state["unresolved_ack"] = False
        state["a1_ack"]["state"] = "acknowledged_exactly_reconciled"
        _write_state(state_path, state)
        holder.close(deadline, orderly=True)
        runner._wait_source_group_absent(holder, deadline)
        source_group_id = holder.owner.process_group_id
        source_config = json.loads(Path(original["source_bootstrap_config_path"]).read_bytes())
        source_root = Path(source_config["root"])
        source_state = {"state": "stopped_and_preserved",
            "pgdata": {source: {"path": str(source_root / f"pgdata-{source}"),
                "postmaster_pid_absent": not (source_root / f"pgdata-{source}" / "postmaster.pid").exists()}
                for source in ("A", "B")},
            "process_group_absent": runner._process_group_absent(source_group_id)}
        _require(all(item["postmaster_pid_absent"] for item in source_state["pgdata"].values())
                 and source_state["process_group_absent"],
                 "Original A/B source workers or PostgreSQL servers remain active")
        holder = None

        # Only after both ACKs are matched and the source worker has closed do we
        # remove the exact six owned tables and the thirteen known Volume artifacts.
        a1_volume_names = ["a1-source-bundle.bin", "a0-manifest.json",
            "a1-source-snapshot.bin", "a1-publisher-result.json",
            "a1-source-prefix.json", "a1-selected-prefix.json"]
        for name in a1_volume_names:
            _read_volume_bytes(api, volume_path + "/" + name,
                (a1_genesis if name == "a1-source-bundle.bin" else
                 a0_manifest if name == "a0-manifest.json" else
                 a1_snapshot if name == "a1-source-snapshot.bin" else
                 a1_output_raw if name == "a1-publisher-result.json" else
                 a1_prefix if name == "a1-source-prefix.json" else a1_selected_raw))
        _read_volume_bytes(api, a0_recovery_snapshot_uri, fresh_a0_snapshot)
        for uri in original_volume_uris + [a0_recovery_snapshot_uri] + [
                volume_path + "/" + name for name in a1_volume_names]:
            api.remove_file("dbfs:" + uri)
        directory = api.call("get", "/api/2.0/fs/directories/" + quote(volume_path, safe="/"))
        _require(runner._directory_contents(directory, maximum_entries=16) == [],
                 "Recovery Volume contains unexpected artifacts after known-file cleanup")
        volume_full = config["catalog"] + "." + config["schema"] + "." + config["volume_name"]
        volume_api = "/api/2.1/unity-catalog/volumes/" + quote(volume_full, safe="")
        live_volume = api.call("get", volume_api)
        _require(live_volume.get("volume_id") == original["volume_id"]
                 and live_volume.get("owner") == config["expected_principal"],
                 "Volume ownership changed before recovery cleanup")
        api.call("delete", volume_api)
        role_rows = runner._list_owned_tables(api, config, deadline)
        cleanup_ids = runner._admit_owned_uc_table_inventory(role_rows, installation_inventory,
            catalog=config["catalog"], schema=config["schema"],
            principal=config["expected_principal"])
        _require(cleanup_ids == original["uc_table_id_baseline"],
                 "Six table IDs changed before exact schema cleanup")
        current_schema = runner._list_schema(api, config["catalog"], config["schema"], deadline)
        _require(type(current_schema) is dict
                 and current_schema.get("schema_id") == original["schema_id"]
                 and current_schema.get("properties", {}).get("ashlar.trial_id") == config["trial_id"],
                 "Schema ownership changed before cleanup")
        schema_path = "/api/2.1/unity-catalog/schemas/" + quote(
            config["catalog"] + "." + config["schema"], safe="")
        api.call("delete", schema_path + "?force=true")
        _require(runner._list_schema(api, config["catalog"], config["schema"], deadline) is None,
                 "Owned A1 schema remains after successful recovery cleanup")
        result.update({"status": "acknowledged_and_cleaned", "a0": a0_result,
            "a1": a1_result, "a0_pin_release": a0_pin_release,
            "a1_pin_release": a1_release, "pin_postgres_cleanup": pin_cleanup,
            "source_postgres_cleanup": source_state,
            "source_reselection": source_reselection,
            "readonly_admission_seconds": readonly_elapsed,
            "active_recovery_seconds": time.monotonic() - active_start,
            "known_run_ids": dict(run_ids), "schema_absent_after_cleanup": True,
            "source_holder_closed": True,
            "volume_cleanup": "exact_original_and_recovery_files_removed",
            "native_table_cleanup": "six_original_managed_tables_and_schema_removed"})
        state.update({"status": "acknowledged_and_cleaned", "phase": "complete",
            "active_recovery_seconds": time.monotonic() - active_start,
            "a0_ack": {**state["a0_ack"], "state": "acknowledged_exactly_reconciled"},
            "a1_ack": {**state["a1_ack"], "state": "acknowledged_exactly_reconciled"},
            "known_run_ids": dict(run_ids), "result": result})
        _write_state(state_path, state)
        runner._write_private_json(output_root / "qualification.json", result)
        return result
    except BaseException as error:
        state["phase"] = "recovery_required"
        state["status"] = "recovery_required"
        state["failure_type"] = type(error).__name__
        state["failure"] = str(error)[:1200]
        state["readonly_admission_seconds"] = (None if readonly_elapsed is None
            else readonly_elapsed)
        state["active_recovery_seconds"] = (None if active_start is None
            else time.monotonic() - active_start)
        state["ack_attempted"] = bool(state.get("ack_attempted"))
        state["known_run_ids"] = dict(run_ids)
        state["a0_ack_matched"] = a0_ack_done
        state["a1_ack_matched"] = a1_ack_done
        result["status"] = "recovery_required"
        result["failure_type"] = type(error).__name__
        result["failure"] = str(error)[:1200]
        try:
            _write_state(state_path, state)
            runner._write_private_json(output_root / "qualification.json", result)
        except Exception:
            pass
        raise
    finally:
        if pin_stages is not None and pin_owner is not None:
            try:
                pin_stages.close_on_error()
            except Exception as error:
                state.setdefault("cleanup_errors", []).append(
                    {"owner": "pin", "stage": "native_handle_close",
                     "error": f"{type(error).__name__}: {error}"[:400]})
        if pin_owner is not None:
            try:
                pin_owner.__exit__(None, None, None)
            except Exception as error:
                state.setdefault("cleanup_errors", []).append(
                    {"owner": "pin", "stage": "owner_exit",
                     "error": f"{type(error).__name__}: {error}"[:400]})
            if pin_pg_ctl_lock is not None:
                try:
                    state.setdefault("postgres_cleanup", []).append(
                        runner._cleanup_pin_pgdata(pin_owner, pin_pg_ctl_lock, deadline))
                except Exception as error:
                    state.setdefault("cleanup_errors", []).append(
                        {"owner": "pin", "stage": "postgres_cleanup",
                         "error": f"{type(error).__name__}: {error}"[:400]})
        if holder is not None:
            try:
                unresolved_ack = unresolved_ack or bool(state.get("unresolved_ack"))
                holder.close(deadline, orderly=not unresolved_ack)
                runner._wait_source_group_absent(holder, deadline)
            except Exception as error:
                state.setdefault("cleanup_errors", []).append(
                    {"owner": "source", "stage": "close_or_reap",
                     "error": f"{type(error).__name__}: {error}"[:400]})
        try:
            _write_state(state_path, state)
        except Exception:
            pass
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer[0] > 0:
            signal.setitimer(signal.ITIMER_REAL, previous_timer[0], previous_timer[1])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--recovery-receipt", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--profile", default=runner.PROFILE)
    parser.add_argument("--execute", action="store_true",
                        help="Run the reviewed bounded recovery; default is effect-free preflight")
    args = parser.parse_args(argv)
    config_path, receipt_path, output_root = map(Path,
        (args.config, args.recovery_receipt, args.output_root))
    try:
        config, payload, receipt = _validate_recovery_inputs(
            config_path, receipt_path, output_root)
        inert = _inert_preflight(config, receipt, receipt_path)
        if not args.execute:
            print(json.dumps({"status": "preflight_passed", "profile": PROFILE,
                "trial_id": config["trial_id"], "schema": receipt["schema"],
                "read_only_admission_cap_seconds": MAX_READONLY_ADMISSION_SECONDS,
                "active_recovery_budget_seconds": MAX_RECOVERY_SECONDS,
                "maximum_finite_wall_seconds": MAX_READONLY_ADMISSION_SECONDS + MAX_RECOVERY_SECONDS,
                "original_campaign_active_seconds_ceiling": ORIGINAL_CAMPAIGN_SECONDS,
                "retired_recovery_windows": [dict(item) for item in RETIRED_RECOVERY_WINDOWS],
                "prior_active_seconds_ceiling": PRIOR_ACTIVE_SECONDS_CEILING,
                "cumulative_active_seconds_ceiling": MAX_CUMULATIVE_ACTIVE_SECONDS,
                "cost_envelope": config["cost"],
                "remaining_jobs": ["a0-verifier", "a0-post-ack-close", "a1-publisher",
                    "a1-verifier", "a1-post-ack-close"],
                "retained_input_recomposition": inert,
                "initializer_replayed": False, "a0_publisher_replayed": False,
                "source_or_delta_replay": False}, sort_keys=True))
            return 0
        value = _launch(config_path, receipt_path, output_root, args.profile)
        print(json.dumps(value, sort_keys=True, indent=2))
        return 0
    except Exception as error:
        print(f"managed A1 recovery refused: {type(error).__name__}: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())


