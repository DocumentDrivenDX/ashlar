#!/usr/bin/env python3
"""Recover the retained attempt14 A1 page without replaying its source group.

This one-use local worker only reopens attempt14's original Truss PostgreSQL
clusters and Spark Delta warehouse. It never initializes targets, installs
Truss, replays A0, reapplies the A1 group, or writes Delta. It performs one
protected A1 source ACK only after exact journal, manifest, vector, and
independent four-role readback checks pass.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import time
import traceback
from contextlib import ExitStack
from urllib.parse import unquote, urlsplit

REPO = Path(__file__).resolve().parents[1]
ORIGINAL = Path("/private/tmp/managed-current-local-native-20261103-attempt14")
ROOT = Path("/private/tmp/managed-current-local-native-20261103-attempt14-recovery05")
RUNTIME = Path("/private/tmp/ashlar-release-owned-runtime-manifest-20261103")
SCHEMA = "managed_current_local_20261103_attempt14"
MAX_WALL_SECONDS = 600
A0_PUBLICATION_ID = "ashlar-current-incremental:c919e38bc7c40c20168122d2fd5794cf0c0432759cae595a9702e37aea664305"
A0_MANIFEST_SHA256 = "4de05e1ff49d018d4d03c74b95fff8566f7ac01f06aa324c395f95ef8097c868"
A0_CHECKPOINT_SHA256 = "2a21b5785eb6aa61e686f09ead419827c0bf25b0d967b448f7df189171e5af70"
FROZEN_PUBLICATION_SHA256 = "5ac1022775e1f74c2c4245ee1ef1b33e749995e1417ef1584f8f487b68fede61"


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _private(path: Path, raw: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def _json(raw: bytes, label: str) -> dict:
    from ashlar.schema import decode_retained_json
    value = decode_retained_json(raw)
    if type(value) is not dict:
        raise RuntimeError(f"Exact {label} object required")
    return value


def _source_state_preflight() -> dict:
    if ORIGINAL.is_symlink() or not ORIGINAL.is_dir() or ORIGINAL.resolve() != ORIGINAL:
        raise RuntimeError("Exact retained attempt14 root required")
    if stat.S_IMODE(ORIGINAL.stat().st_mode) != 0o700 or ORIGINAL.stat().st_uid != os.getuid():
        raise RuntimeError("Owner-private retained attempt14 root required")
    worker = ORIGINAL / "qualification.json"
    supervisor = Path(str(ORIGINAL) + "-supervisor.json")
    for path in (worker, supervisor):
        if not path.is_file() or path.is_symlink() or path.stat().st_mode & 0o077:
            raise RuntimeError("Retained attempt14 completion/failure receipts required")
    result = json.loads(worker.read_bytes())
    supervised = json.loads(supervisor.read_bytes())
    if (result.get("status") != "failed"
            or result.get("source_ack", [{}])[-1].get("outcome") != "matched"
            or len(result.get("source_ack", [])) != 1
            or result.get("effect_state", {}).get("delta_publication_outcome")
                != "committed_receipt_reopened"
            or result.get("effect_state", {}).get("source_ack_outcome") != "matched"
            or supervised.get("process_group_empty") is not True
            or supervised.get("status") != "failed"):
        raise RuntimeError("Attempt14 must retain only matched A0 ACK and committed A1 publication")
    if (result.get("error", {}).get("type") != "ManagedCurrentA1PrefixBuildError"
            or "A/B source cursor differs from exact phase predecessor"
                not in result.get("error", {}).get("traceback", "")):
        raise RuntimeError("Attempt14 failure is not the reviewed A1 cursor-prefix refusal")
    if result.get("source_ack") and result["source_ack"][-1].get("outcome") != "matched":
        raise RuntimeError("Attempt14 A0 settlement is not exact and matched")
    if ((ORIGINAL / "pre-ack-ack-a1.json").exists()
            or (ORIGINAL / "ack-a1").exists()):
        raise RuntimeError("Attempt14 must have no A1 readback or ACK attempt")
    a1_result_path = ORIGINAL / "sources" / "a1-public-result.json"
    group_path = ORIGINAL / "sources" / "a1-group.json"
    if not a1_result_path.is_file() or not group_path.is_file():
        raise RuntimeError("Exact committed seven-operation A1 source group evidence required")
    return {"worker_sha256": _sha(worker.read_bytes()),
        "supervisor_sha256": _sha(supervisor.read_bytes()),
        "a0_ack_checkpoint_sha256": result["source_ack"][0]["checkpoint_sha256"],
        "a0_manifest_sha256": result["a0"]["manifest_sha256"],
        "a1_public_result_sha256": _sha(a1_result_path.read_bytes()),
        "a1_group_config_sha256": _sha(group_path.read_bytes()),
        "retained_source_root": str(ORIGINAL / "sources"),
        "retained_delta_warehouse": str(ORIGINAL / "spark-warehouse"),
        "attempt14_a1_ack_attempt_absent": True,
        "attempt14_supervisor_process_group_empty": True}


def _verify_source_group() -> dict:
    group = json.loads((ORIGINAL / "sources" / "a1-group.json").read_bytes())
    receipt = json.loads((ORIGINAL / "sources" / "a1-public-result.json").read_bytes())
    if (group.get("profile") != "ashlar-truss-record-group/0.1"
            or type(group.get("records")) is not list or len(group["records"]) != 4
            or type(group.get("edges")) is not list or len(group["edges"]) != 3
            or receipt.get("profile") != "ashlar-truss-record-group/0.1"
            or receipt.get("qualification") !=
                "One ordinary public atomic record/link group and exact readback/replay; no publication/feed/ACK/query or inferred required-edge enforcement qualification."):
        raise RuntimeError("Retained R28 seven-operation source group evidence changed")
    original = receipt.get("group_original", {})
    replayed = receipt.get("committed_receipt_replay", {})
    original_response = original.get("response", {})
    replay_response = replayed.get("response", {})
    if (original.get("outcome") != "success" or replayed.get("outcome") != "success"
            or original.get("group_id") != replayed.get("group_id")
            or original_response.get("disposition") != "applied"
            or original_response.get("durability") != "pending"
            or replay_response.get("disposition") != "replayed"
            or replay_response.get("durability") != "committed"
            or original_response.get("receipt_token") != replay_response.get("receipt_token")):
        raise RuntimeError("Exact committed public A1 group replay evidence required")
    token = original_response.get("receipt_token")
    if type(token) is not str:
        raise RuntimeError("Public A1 group receipt token required")
    try:
        token_raw = base64.urlsafe_b64decode(token + "=" * ((4 - len(token) % 4) % 4))
        token_doc = _json(token_raw, "A1 group receipt token")
    except (ValueError, TypeError) as error:
        raise RuntimeError("Public A1 group receipt token is malformed") from error
    if (token_doc.get("installationId") != "a50118c0-ad61-44b1-a0d3-c456fff9a724"
            or token_doc.get("sourceEpoch") != "0af4e10a-8cc9-4f8a-977a-e14ee4121d10"
            or token_doc.get("writerXid") != "797"):
        raise RuntimeError("Committed A1 group receipt differs from exact attempt14 source")
    return {"group_id": original["group_id"], "writer_xid": token_doc["writerXid"],
        "token_sha256": _sha(token.encode()),
        "group_config_sha256": _sha((ORIGINAL / "sources" / "a1-group.json").read_bytes()),
        "public_result_sha256": _sha((ORIGINAL / "sources" / "a1-public-result.json").read_bytes()),
        "objects": len(receipt.get("objects", [])), "edges": len(receipt.get("edges", [])),
        "committed_replay": True}


def _guard_read_only_spark(spark, original_worker, spark_metrics):
    original_sql = spark.sql
    metrics = {"spark_sql_statements": 0, "rejected_mutation_statements": 0}

    def guarded(statement, *args, **kwargs):
        compact = " ".join(statement.split())
        token = compact.split(" ", 1)[0].upper() if compact else ""
        if token not in {"SELECT", "SHOW", "DESCRIBE"}:
            metrics["rejected_mutation_statements"] += 1
            raise RuntimeError("Recovery Spark SQL allowlist refuses non-read statements")
        metrics["spark_sql_statements"] += 1
        return original_sql(statement, *args, **kwargs)

    spark.sql = guarded
    return metrics


def _warehouse_paths():
    warehouse = (ORIGINAL / "spark-warehouse").resolve(strict=True)
    database = warehouse / (SCHEMA + ".db")
    if database.is_symlink() or not database.is_dir() or database.resolve() != database:
        raise RuntimeError("Exact retained attempt14 Delta database directory required")
    paths = {}
    for role in ("object_current", "edge_current", "tombstone", "whole_source_history",
                 "publication_manifest", "publication_attempt_phase"):
        path = database / role
        if path.is_symlink() or not path.is_dir() or path.resolve() != path:
            raise RuntimeError(f"Exact retained native Delta path required for {role}")
        log = path / "_delta_log"
        if log.is_symlink() or not log.is_dir() or log.resolve() != log:
            raise RuntimeError(f"Exact retained Delta log required for {role}")
        paths[role] = path
    return paths


def _delta_log_state(path: Path):
    log = path / "_delta_log"
    files = sorted(log.glob("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].json"))
    if (not files or len(files) > 4096 or any(item.is_symlink() for item in files)
            or [int(item.stem) for item in files] != list(range(len(files)))):
        raise RuntimeError("Bounded contiguous original Delta JSON log required")
    digest_chain = []
    uuid = None
    for version, file in enumerate(files):
        raw = file.read_bytes()
        if len(raw) > 16 * 1024 * 1024:
            raise RuntimeError("Original Delta commit exceeds bounded log size")
        for line in raw.splitlines():
            action = _json(line, "Delta log action")
            metadata = action.get("metaData")
            if type(metadata) is dict:
                candidate = metadata.get("id")
                if type(candidate) is not str or not candidate:
                    raise RuntimeError("Original Delta metadata UUID is malformed")
                if uuid is not None and candidate != uuid:
                    raise RuntimeError("Original Delta UUID changed across retained log history")
                uuid = candidate
        digest_chain.append((version, _sha(raw)))
    if uuid is None:
        raise RuntimeError("Original Delta log lacks its metadata UUID")
    return {"head": len(files) - 1, "uuid": uuid, "history": digest_chain,
        "log_sha256": _sha(_canonical(digest_chain))}


def _read_pinned_frame(spark, path, version):
    if type(version) is not int or version < 0:
        raise RuntimeError("Exact nonnegative retained Delta version required")
    return spark.read.format("delta").option("versionAsOf", version).load(str(path))


def _pinned_manifest_row(spark, path, version, publication_id):
    from pyspark.sql import functions as F
    frame = _read_pinned_frame(spark, path, version)
    projected = frame.select("publication_id", "profile_version", "table_versions_json",
        "source_progress_json", "schema_revisions_json", "validation_report_json",
        F.unix_micros(F.col("recorded_at")).cast("string").alias("recorded_at"))
    rows = projected.where(F.col("publication_id") == publication_id).limit(2).collect()
    if len(rows) != 1:
        raise RuntimeError("Unique exact manifest row absent from pinned native Delta snapshot")
    return rows[0].asDict(recursive=True)


def _pinned_journal_rows(spark, path, version, maximum=160):
    rows = _read_pinned_frame(spark, path, version).select(
        "stream", "batch_id", "phase", "request_digest", "payload_json", "payload_digest"
    ).limit(maximum + 1).collect()
    if len(rows) > maximum:
        raise RuntimeError("Retained publication phase journal exceeds exact recovery bound")
    return [row.asDict(recursive=True) for row in rows]


def _a0_manifest(spark, paths, attempt14):
    from ashlar_host.managed_current_core import CARRIER_PROFILE_XXHASH64

    role = "publication_manifest"
    manifest = _pinned_manifest_row(spark, paths["publication_manifest"], 2,
                                    A0_PUBLICATION_ID)
    manifest_bytes = _canonical(manifest)
    if (_sha(manifest_bytes) != A0_MANIFEST_SHA256
            or manifest.get("publication_id") != A0_PUBLICATION_ID
            or manifest.get("profile_version") != CARRIER_PROFILE_XXHASH64):
        raise RuntimeError("Attempt14 A0 manifest identity or exact bytes changed")
    versions = _json(manifest["table_versions_json"].encode(), "A0 role versions")
    expected_data_tables = {f"spark_catalog.{SCHEMA}.{name}" for name in
        ("object_current", "edge_current", "tombstone", "whole_source_history")}
    if set(versions) != expected_data_tables or any(value != 0 for value in versions.values()):
        raise RuntimeError("A0 predecessor must be the exact empty data-role genesis")
    progress = _json(manifest["source_progress_json"].encode(), "A0 source progress")
    report = _json(manifest["validation_report_json"].encode(), "A0 validation report")
    revision_json = _json(manifest["schema_revisions_json"].encode(), "A0 schema revisions")
    if set(progress) != {"ashlar-managed-a1-a-feed", "ashlar-managed-a1-b-feed"}:
        raise RuntimeError("A0 metadata publication lacks exact A/B source progress")
    # A0 is an incremental metadata publication, so its report intentionally
    # contains no genesis catalog-acceptance fields. Recover those pins from
    # the exact original version-one genesis manifest row instead. Version
    # zero is the manifest table's CREATE TABLE commit, unlike data-role v0.
    genesis_rows = _read_pinned_frame(spark, paths[role], 1).select(
        "publication_id", "profile_version", "table_versions_json", "source_progress_json",
        "schema_revisions_json", "validation_report_json",
        __import__("pyspark.sql.functions", fromlist=["unix_micros"]).unix_micros(
            __import__("pyspark.sql.functions", fromlist=["col"]).col("recorded_at")
        ).cast("string").alias("recorded_at")).limit(16).collect()
    genesis_candidates = []
    for row in genesis_rows:
        candidate = row.asDict(recursive=True)
        candidate_report = _json(candidate["validation_report_json"].encode(),
                                  "original genesis report")
        if candidate_report.get("publication_plan_profile") == \
                "ashlar-managed-current-genesis-manifest/0.1":
            genesis_candidates.append((candidate, candidate_report))
    if len(genesis_candidates) != 1:
        raise RuntimeError("Unique exact original genesis manifest row required at version one")
    genesis_manifest, genesis_report = genesis_candidates[0]
    if (_sha(_canonical(genesis_manifest))
            != attempt14.get("native_genesis", {}).get("manifest_sha256")
            or type(genesis_report.get("catalog_acceptance_revisions")) is not dict
            or set(genesis_report["catalog_acceptance_revisions"]) != {"A", "B"}
            or type(genesis_report.get("sources")) is not dict
            or set(genesis_report["sources"]) != {"A", "B"}):
        raise RuntimeError("Exact original genesis digest/catalog pins differ from retained native receipt")
    genesis_progress = _json(genesis_manifest["source_progress_json"].encode(),
                             "original genesis source progress")
    if (set(genesis_progress) != set(progress)
            or progress["ashlar-managed-a1-a-feed"].get("previous")
                != genesis_progress["ashlar-managed-a1-a-feed"].get("position")
            or progress["ashlar-managed-a1-b-feed"]
                != genesis_progress["ashlar-managed-a1-b-feed"]):
        raise RuntimeError("A0 manifest does not continue exact genesis A cursor and untouched B marker")
    settlement_path = ORIGINAL / "ack-a0" / "worker-settlement.json"
    settlement = json.loads(settlement_path.read_bytes())
    checkpoint = base64.b64decode(settlement["checkpoint_bytes_base64"], validate=True)
    if (settlement.get("outcome") != "matched"
            or settlement.get("ack_retry_permitted") is not False
            or _sha(checkpoint) != A0_CHECKPOINT_SHA256):
        raise RuntimeError("Original A0 protected ACK is not durably matched")
    from ashlar_host.r04_schedule import R04Schedule
    a_binding = R04Schedule((ORIGINAL / "sources" / "r04-schedule.json").read_bytes()).document()["source_bindings"]["A"]
    progress_a = progress[a_binding["feed"]]
    cursor_bytes = base64.b64decode(progress_a["position"], validate=True)
    cursor = _json(cursor_bytes, "A0 source cursor")
    if (progress_a.get("epoch") != a_binding["epoch"]
            or progress_a.get("position") != base64.b64encode(checkpoint).decode("ascii")
            or cursor.get("sourceEpoch") != a_binding["epoch"]
            or cursor.get("installationId") != a_binding["installation_id"]
            or cursor.get("consumerId") != a_binding["consumer_id"]
            or cursor.get("registrationId") != a_binding["registration_id"]):
        raise RuntimeError("A0 ACK checkpoint differs from exact persisted A source progress")
    if (attempt14.get("a0", {}).get("ack", {}).get("outcome") != "matched"
            or attempt14["a0"]["ack"].get("checkpoint_sha256") != _sha(checkpoint)):
        raise RuntimeError("A0 retained native proof differs from exact matched settlement")
    return (manifest, manifest_bytes, progress, report, revision_json, checkpoint,
            a_binding, genesis_manifest, genesis_report)


def _rebuild_original_genesis(*, attempt14, genesis_manifest,
        genesis_report, schedule, current_selection_bytes):
    """Recover cursor-zero genesis bytes from the retained A0 hold plus live B."""
    from ashlar_host.truss_detached_source_snapshot_codec import decode_detached_source_snapshot
    from ashlar_host.truss_detached_a1_genesis_codec import (
        PROFILE as GENESIS_PROFILE, decode_detached_a1_genesis_snapshot,
    )

    ack_attempt = json.loads((ORIGINAL / "ack-a0" / "ack-attempt.json").read_bytes())
    a0_selection_bytes = base64.b64decode(ack_attempt["selection_bytes_base64"], validate=True)
    _private(ROOT / "retained-a0-source-selection.json", a0_selection_bytes)
    _private(ROOT / "live-post-a0-ab-selection.json", current_selection_bytes)
    a0_selection_sha = _sha(a0_selection_bytes)
    if (a0_selection_sha != ack_attempt.get("selection_sha256")
            or a0_selection_sha != attempt14.get("a0", {}).get("ack", {}).get("selection_sha256")):
        raise RuntimeError("Retained exact A0 public source selection digest differs")
    a0 = decode_detached_source_snapshot(a0_selection_bytes)
    page = a0.source_page
    checkpoint = base64.b64decode(
        json.loads((ORIGINAL / "ack-a0" / "worker-settlement.json").read_bytes())[
            "checkpoint_bytes_base64"], validate=True)
    original_cursor = (ORIGINAL / "sources" / "genesis-cursor-A.json").read_bytes()
    if (page.source != "A"
            or page.publication_id != A0_PUBLICATION_ID
            or page.request_bytes != base64.b64decode(ack_attempt["request_bytes_base64"], validate=True)
            or page.prior_bytes != original_cursor
            or page.next_bytes != checkpoint
            or page.page_fields.get("sha256") != ack_attempt.get("page_sha256")
            or a0.progress.cursor_bytes != original_cursor
            or a0.progress.position != base64.b64encode(original_cursor).decode("ascii")):
        raise RuntimeError("A0 retained public page does not bind original cursor-zero genesis")

    current_package = _json(current_selection_bytes, "live post-A0 A/B source selection")
    if (current_package.get("profile") != GENESIS_PROFILE
            or current_package.get("schedule_sha256") != schedule.sha256
            or type(current_package.get("sources")) is not list
            or len(current_package["sources"]) != 2):
        raise RuntimeError("Exact live public A/B source selection envelope required")
    current_b = [item for item in current_package["sources"]
                 if type(item) is dict and item.get("source") == "B"]
    if len(current_b) != 1:
        raise RuntimeError("Unique live public B source observation required")

    schedule_doc = schedule.document()
    a_binding = schedule_doc["source_bindings"]["A"]
    progress = a0.progress
    model_pin = schedule_doc["accepted_model_pins"]["A"]
    matching_models = [doc for doc in a0.documents
                       if doc.sha256 == model_pin.get("modelSha256")]
    native_schema_revisions = _json(genesis_manifest["schema_revisions_json"].encode(),
                                    "native genesis schema revisions")
    live_schema_revisions = current_package.get("schema_revisions")
    catalog_revision = genesis_report.get("catalog_acceptance_revisions", {}).get("A", {}).get("revision")
    if (len(matching_models) != 1
            or live_schema_revisions != native_schema_revisions
            or a0.catalog_revision != str(catalog_revision)
            or (progress.feed, progress.epoch, progress.installation_id,
                progress.consumer_id, progress.consumer_registration_id)
               != (a_binding["feed"], a_binding["epoch"], a_binding["installation_id"],
                   a_binding["consumer_id"], a_binding["registration_id"])):
        raise RuntimeError("Retained A0 source selection does not bind original A model and identity")
    a_source = {
        "source": "A",
        "identity": {"feed": progress.feed, "epoch": progress.epoch,
            "installation_id": progress.installation_id, "consumer_id": progress.consumer_id,
            "consumer_registration_id": progress.consumer_registration_id,
            "catalog_registration_id": a0.catalog_registration_id,
            "catalog_revision": a0.catalog_revision},
        "progress": {"feed": progress.feed, "epoch": progress.epoch,
            "position": progress.position, "position_sha256": progress.position_sha256,
            "catalog_registration_sha256": progress.catalog_registration_sha256,
            "catalog_registration_id": progress.catalog_registration_id,
            "catalog_registration_bytes_base64": base64.b64encode(
                progress.catalog_registration_bytes).decode("ascii"),
            "consumer_id": progress.consumer_id,
            "consumer_registration_id": progress.consumer_registration_id,
            "installation_id": progress.installation_id,
            "cursor_bytes_base64": base64.b64encode(progress.cursor_bytes).decode("ascii")},
        "catalog_revision": a0.catalog_revision,
        "report_bytes_base64": base64.b64encode(a0.report_bytes).decode("ascii"),
        "view_bytes_base64": base64.b64encode(a0.view_bytes).decode("ascii"),
        "page": {key: page.page_fields[key] for key in (
            "consumer_id", "registration_id", "prior_bytes_base64",
            "next_bytes_base64", "content_base64", "sha256", "writer_xid",
            "safe_watermark_xid", "outcome")},
        "documents": [{"document_id": doc.document_id,
            "document_revision": doc.document_revision,
            "artifact_identity": doc.artifact_identity,
            "registration_id": doc.registration_id,
            "catalog_revision": str(doc.catalog_revision), "sha256": doc.sha256,
            "content_base64": base64.b64encode(doc.content).decode("ascii")}
            for doc in a0.documents],
        "model": {"pin": model_pin,
            "content_base64": base64.b64encode(matching_models[0].content).decode("ascii")}}
    body = {"profile": GENESIS_PROFILE,
        "schedule_bytes_base64": base64.b64encode(schedule.raw).decode("ascii"),
        "schedule_sha256": schedule.sha256,
        "schema_revisions": current_package["schema_revisions"],
        "sources": [a_source, current_b[0]]}
    expected_sha = attempt14.get("source_genesis", {}).get("sha256")
    live_watermark = current_b[0].get("page", {}).get("safe_watermark_xid")
    if (type(live_watermark) is not str or not live_watermark.isascii()
            or not live_watermark.isdecimal() or str(int(live_watermark)) != live_watermark
            or int(live_watermark) > 4095):
        raise RuntimeError("Live B safe watermark exceeds the bounded genesis-recovery search")
    base_raw = _canonical({**body, "binding_sha256": _sha(_canonical(body))})
    _private(ROOT / "current-watermark-genesis-envelope.json", base_raw)
    matches = []
    for watermark in range(int(live_watermark) + 1):
        b_source = {**current_b[0], "page": {
            **current_b[0]["page"], "safe_watermark_xid": str(watermark)}}
        candidate_body = {**body, "sources": [a_source, b_source]}
        candidate = _canonical({**candidate_body,
            "binding_sha256": _sha(_canonical(candidate_body))})
        if _sha(candidate) == expected_sha:
            matches.append((watermark, candidate))
            if len(matches) > 1:
                break
    _private(ROOT / "genesis-reconstruction-check.json", _canonical({
        "profile": "ashlar-managed-current-local-original-genesis-reconstruction/0.1",
        "attempt14_original_sha256": expected_sha,
        "search": "canonical B safe-watermark integers from 0 through live public bound",
        "candidate_min": 0, "candidate_max": int(live_watermark),
        "candidates_checked": int(live_watermark) + 1 if len(matches) <= 1 else None,
        "unique_match_count": len(matches),
        "matching_b_safe_watermark_xid": matches[0][0] if len(matches) == 1 else None,
        "retained_a0_selection_sha256": a0_selection_sha,
        "live_post_a0_selection_sha256": _sha(current_selection_bytes),
        "a0_prior_cursor_sha256": _sha(original_cursor),
        "a0_next_cursor_sha256": _sha(checkpoint)}) + b"\n")
    if len(matches) != 1:
        raise RuntimeError("Reconstructed original cursor-zero A/B genesis bytes do not match attempt14")
    matched_watermark, raw = matches[0]
    _private(ROOT / "reconstructed-original-genesis.json", raw)
    decode_detached_a1_genesis_snapshot(raw, expected_a_prior_cursor="0")
    composition = {"original_genesis_recovered": True,
        "original_genesis_sha256": _sha(raw), "attempt14_source_genesis_sha256": expected_sha,
        "retained_a0_selection_sha256": a0_selection_sha,
        "live_post_a0_selection_sha256": _sha(current_selection_bytes),
        "a0_page_sha256": page.page_fields["sha256"],
        "a0_prior_cursor_sha256": _sha(original_cursor),
        "a0_next_cursor_sha256": _sha(checkpoint),
        "recovered_original_b_safe_watermark_xid": str(matched_watermark),
        "live_b_safe_watermark_xid": live_watermark,
        "live_b_source": "public issued A/B source observation",
        "bounded_full_sha256_watermark_search_unique": True,
        "cursor_zero_codec_validation": True}
    return raw, composition


def _start_source_runtimes(progress, genesis_report, schedule, checkpoint):
    import psycopg
    from truss import LocalPostgres
    from ashlar_host.managed_current_truss053_genesis_owner import ManagedCurrentTruss053SourceSelection

    group_config = json.loads((ORIGINAL / "sources" / "a1-group.json").read_bytes())
    group_login = psycopg.conninfo.conninfo_to_dict(group_config["connection_uri"])
    if group_login.get("user") != "ashlar_note_consumer":
        raise RuntimeError("Retained original consumer login is not exact")
    root = ORIGINAL / "sources"
    runtimes = {}
    selections = {}
    for source in ("A", "B"):
        pgdata = root / f"pgdata-{source}"
        if (not pgdata.is_dir() or pgdata.is_symlink() or pgdata.resolve() != pgdata
                or stat.S_IMODE(pgdata.stat().st_mode) != 0o700
                or (pgdata / "PG_VERSION").read_text().strip() != "18"
                or (pgdata / "postmaster.pid").exists()):
            raise RuntimeError(f"Exact stopped retained PostgreSQL 18 {source} cluster required")
        runtime_owner = LocalPostgres(pgdata)
        runtime = runtime_owner.__enter__()
        runtimes[source] = runtime_owner
        if (runtime.info.data_directory.resolve() != pgdata
                or runtime.info.server_version.split(".", 1)[0] != "18"):
            raise RuntimeError(f"Retained PostgreSQL {source} identity mismatch")
        with psycopg.connect(runtime.info.connection_uri, autocommit=True,
                             connect_timeout=5) as health:
            if health.execute("SHOW fsync").fetchone()[0] != "on":
                raise RuntimeError("PostgreSQL fsync must stay on during recovery")
        runtime_params = psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri)
        ordinary_uri = psycopg.conninfo.make_conninfo(
            runtime.info.connection_uri, user=group_login["user"])
        ordinary_params = psycopg.conninfo.conninfo_to_dict(ordinary_uri)
        if (ordinary_params.get("user") != "ashlar_note_consumer"
                or ordinary_params.get("host") != runtime_params.get("host")
                or ordinary_params.get("port") != runtime_params.get("port")):
            raise RuntimeError(f"Exact public {source} source endpoint/consumer login required")

        def factory(uri=ordinary_uri):
            return psycopg.connect(uri, autocommit=False, connect_timeout=5)

        binding = schedule.document()["source_bindings"][source]
        progress_item = progress[binding["feed"]]
        source_pin = genesis_report["sources"][source]
        catalog = genesis_report["catalog_acceptance_revisions"][source]
        if (source_pin.get("feed") != binding["feed"]
                or source_pin.get("epoch") != binding["epoch"]
                or source_pin.get("installation_id") != binding["installation_id"]
                or source_pin.get("consumer_id") != binding["consumer_id"]
                or source_pin.get("consumer_registration_id") != binding["registration_id"]
                or catalog.get("installation_id") != binding["installation_id"]
                or type(catalog.get("revision")) is not str
                or not catalog["revision"].isdecimal()):
            raise RuntimeError(f"Original genesis manifest does not bind public {source} pins")
        cursor_path = root / f"genesis-cursor-{source}.json"
        original_genesis_cursor = cursor_path.read_bytes()
        expected_position = (base64.b64encode(checkpoint).decode("ascii") if source == "A"
                             else base64.b64encode(original_genesis_cursor).decode("ascii"))
        if (progress_item.get("feed") != binding["feed"]
                or progress_item.get("epoch") != binding["epoch"]
                or progress_item.get("position") != expected_position):
            raise RuntimeError(f"A0 manifest does not preserve exact {source} cursor predecessor")
        selection = ManagedCurrentTruss053SourceSelection(
            source, binding["feed"], binding["epoch"], binding["installation_id"],
            binding["consumer_id"], binding["registration_id"],
            source_pin["catalog_registration_id"], str(catalog["revision"]), factory)
        selections[source] = selection
    # The new source owner independently checks each complete identity and
    # enforces A's exact matched-A0 predecessor; no phase is injected.
    context = object()
    return runtimes, selections, context


def _attempt_rows(spark, path, version, batch):
    rows = _pinned_journal_rows(spark, path, version, maximum=160)
    return [row for row in rows if row["batch_id"] == batch.batch_id], version


def _verify_pinned_zstd(spark, frame, table_path: Path, expected_rows):
    files = frame.inputFiles()
    if type(files) not in (list, tuple) or len(files) > 4096 or len(set(files)) != len(files):
        raise RuntimeError("Bounded complete pinned Parquet file list required")
    jvm = spark._jvm
    configuration = spark.sparkContext._jsc.hadoopConfiguration()
    reader_type = jvm.org.apache.parquet.hadoop.ParquetFileReader
    input_type = jvm.org.apache.parquet.hadoop.util.HadoopInputFile
    path_type = jvm.org.apache.hadoop.fs.Path
    rows = chunks = 0
    file_hashes = []
    for uri in sorted(files):
        parsed = urlsplit(uri)
        if parsed.scheme not in ("", "file") or parsed.netloc not in ("", "localhost"):
            raise RuntimeError("Nonlocal pinned Parquet path refused")
        local = Path(unquote(parsed.path if parsed.scheme else uri)).resolve()
        if (not local.is_file() or local.is_symlink()
                or not local.is_relative_to(table_path.resolve())):
            raise RuntimeError("Exact local pinned Parquet file required")
        reader = reader_type.open(input_type.fromPath(path_type(uri), configuration))
        try:
            groups = reader.getFooter().getBlocks()
            if groups is None or groups.size() > 4096:
                raise RuntimeError("Bounded native Parquet row groups required")
            for index in range(groups.size()):
                group = groups.get(index)
                rows += int(group.getRowCount())
                columns = group.getColumns()
                if columns is None or columns.size() > 16384:
                    raise RuntimeError("Bounded native Parquet column chunks required")
                for column in range(columns.size()):
                    if str(columns.get(column).getCodec().name()) != "ZSTD":
                        raise RuntimeError("Native `.4` recovery parquet codec differs from ZSTD")
                    chunks += 1
        finally:
            reader.close()
        file_hashes.append(_sha(uri.encode("utf-8")))
    if rows != expected_rows or (expected_rows and chunks == 0):
        raise RuntimeError("Pinned native Parquet footer rows differ from source oracle")
    return {"file_count": len(files), "file_uri_sha256": file_hashes,
        "row_count": rows, "zstd_column_chunks": chunks}


def _independent_path_readback(*, spark, paths, log_states, attempt14,
        plan, genesis_bytes, held_source_bytes, initial_manifest_bytes,
        request_bytes, manifest, manifest_bytes, vector, receipt_bytes):
    from ashlar.manifest import manifest_pin_vector
    from ashlar.protocol import validate_protocol_detail
    from ashlar_host.configured_local_runtime import MANAGED_SERVERLESS_PROFILE
    from ashlar_host.managed_current_core import (
        CARRIER_PROFILE_XXHASH64, ManagedCurrentCoreConfig,
    )
    from ashlar_host.managed_current_carriers import load_current_managed_carriers
    from ashlar_host.managed_current_spark_source_oracle import (
        build_managed_current_a1_source_oracle,
        compare_complete_managed_current_bags, managed_current_a1_lookup_keys,
    )
    from ashlar_host.managed_current_a1_prefix_builder import (
        build_managed_current_a1_prefix_evidence,
    )

    start = time.monotonic()
    roles = ("object_current", "edge_current", "tombstone", "whole_source_history")
    names = {role: f"spark_catalog.{SCHEMA}.{role}" for role in roles}
    versions = _json(manifest["table_versions_json"].encode(), "A1 four-role version vector")
    if set(versions) != set(names.values()):
        raise RuntimeError("A1 manifest does not pin exactly four native data roles")
    genesis_uuids = attempt14["native_genesis"]["six_role_uuid"]
    uuids = {names[role]: genesis_uuids[role] for role in roles}
    rebuilt = manifest_pin_vector(manifest, uuids,
        authority="managed-current-spark:local-native-qualification")
    if (rebuilt.targets != vector.targets or rebuilt.scope_id != vector.scope_id
            or rebuilt.custody_digest != vector.custody_digest):
        raise RuntimeError("Independent pinned path vector differs from source ACK vector")
    config = ManagedCurrentCoreConfig(catalog="spark_catalog", schema=SCHEMA,
        backend03=None, backend02=None, preferred_cost_usd=0, hard_cost_usd=0,
        carrier_profile=CARRIER_PROFILE_XXHASH64)
    lookup_hashes = {}
    for kind, source, type_id, entity_id in managed_current_a1_lookup_keys(plan, config=config):
        row = spark.sql(
            "SELECT xxhash64(CAST(:source AS STRING), CAST(:type AS BIGINT), "
            "CAST(:id AS BIGINT)) AS lookup_hash",
            args={"source": source, "type": str(type_id), "id": str(entity_id)}).collect()
        if len(row) != 1 or type(row[0]["lookup_hash"]) is not int:
            raise RuntimeError("Native source lookup hash did not produce one exact signed64 value")
        lookup_hashes[(kind, source, type_id, entity_id)] = row[0]["lookup_hash"]
    prefix_bytes, prefix_sha, selected = build_managed_current_a1_prefix_evidence(
        genesis_bytes=genesis_bytes, source_snapshot_bytes=held_source_bytes,
        initial_manifest_bytes=initial_manifest_bytes, request_bytes=request_bytes,
        publication_manifest_bytes=manifest_bytes,
        destination={"catalog": "spark_catalog", "schema": SCHEMA},
        carrier_profile=CARRIER_PROFILE_XXHASH64,
        materialization_clock_us=plan.driver_inputs["materialized_at"], phase="a1")
    oracle = build_managed_current_a1_source_oracle(plan, config=config,
        prefix_bytes=prefix_bytes, prefix_sha256=prefix_sha, selected=selected,
        manifest=manifest, lookup_hashes=lookup_hashes)
    expected = oracle.expected_rows
    carriers = load_current_managed_carriers(CARRIER_PROFILE_XXHASH64)
    actual = {}
    observations = {}
    native_inventory = {}
    all_sql_calls = 0
    for role in roles:
        path = paths[role]
        table = names[role]
        version = versions[table]
        state = log_states[role]
        if type(version) is not int or state["head"] != version:
            raise RuntimeError(f"Native Delta {role} head is not the exact A1 vector version")
        expected_uuid = genesis_uuids[role]
        if state["uuid"] != expected_uuid:
            raise RuntimeError(f"Native Delta UUID changed for {role}")
        frame = _read_pinned_frame(spark, path, version)
        schema = [[field.name, field.dataType.simpleString(), field.nullable]
                  for field in frame.schema.fields]
        type_names = {"STRING": "string", "BIGINT": "bigint",
                      "BOOLEAN": "boolean", "TIMESTAMP": "timestamp"}
        expected_schema = [[field, type_names[kind], nullable]
                           for field, kind, nullable in carriers.ordered_fields(role)]
        if schema != expected_schema:
            raise RuntimeError(f"Literal Delta-path schema differs for {role}")
        detail = spark.sql(f"DESCRIBE DETAIL delta.`{str(path).replace('`', '``')}`")
        detail_rows = detail.limit(2).collect()
        if len(detail_rows) != 1:
            raise RuntimeError(f"Literal Delta path lacks unique protocol detail for {role}")
        detail_doc = detail_rows[0].asDict(recursive=True)
        if detail_doc.get("id") != expected_uuid or detail_doc.get("format") != "delta":
            raise RuntimeError(f"Literal Delta path UUID/format differs for {role}")
        protocol_doc = dict(detail_doc)
        protocol_doc["minReaderVersion"] = str(protocol_doc["minReaderVersion"])
        protocol_doc["minWriterVersion"] = str(protocol_doc["minWriterVersion"])
        protocol_doc["tableFeatures"] = _canonical(list(protocol_doc.get("tableFeatures", []))).decode()
        protocol = validate_protocol_detail(protocol_doc, uuid=expected_uuid,
                                             profile=MANAGED_SERVERLESS_PROFILE)
        projections = []
        from pyspark.sql import functions as F
        for field, kind, _nullable in carriers.ordered_fields(role):
            column = F.col(field)
            if kind == "BIGINT":
                column = column.cast("string")
            elif kind == "TIMESTAMP":
                column = F.unix_micros(column).cast("string")
            projections.append(column.alias(field))
        rows = frame.select(*projections).limit(1025).collect()
        if len(rows) > 1024:
            raise RuntimeError("A1 native path row bag exceeds recovery bound")
        actual[role] = [row.asDict(recursive=True) for row in rows]
        footer = _verify_pinned_zstd(spark, frame, path, len(actual[role]))
        observations[role] = {"table": table, "path": str(path), "uuid": expected_uuid,
            "version": version, "head_version": state["head"],
            "delta_log_sha256": state["log_sha256"], "schema": schema,
            "protocol": protocol, "row_count": len(actual[role]),
            "rows_sha256": _sha(_canonical(actual[role])), "parquet_footer": footer}
        native_inventory[role] = {"name": table, "uuid": expected_uuid,
            "version": version, "schema": schema, "protocol": protocol}
    comparison = compare_complete_managed_current_bags(expected, actual,
        carrier_profile=CARRIER_PROFILE_XXHASH64)
    return {"phase": "A1 seven-operation seed", "publication_id": manifest["publication_id"],
        "manifest_sha256": _sha(manifest_bytes),
        "vector": {name: list(pin) for name, pin in vector.targets.items()},
        "roles": observations, "installation_inventory": native_inventory,
        "source_oracle_sha256": _sha(oracle.expected_rows_json.encode()),
        "comparison_sha256": comparison,
        "source_expected_rows": {role: len(expected[role]) for role in roles},
        "native_rows": {role: len(actual[role]) for role in roles},
        "elapsed_seconds": round(time.monotonic() - start, 6),
        "readback_passed": True}


def main():
    import inspect
    from ashlar.attempt_store import validate_source_qualified_phase_inventory
    from ashlar.manifest import manifest_pin_vector
    from ashlar.publication import descriptor_from_manifest_row
    from ashlar.stored_publisher import validate_applied_artifact
    from ashlar_host.a1_ack_attempt import A1AckSettlement, DurableA1AckAttemptStore
    from ashlar_host.configured_local_runtime import MANAGED_SERVERLESS_PROFILE, open_configured_local_spark
    from ashlar_host.configured_local_runtime_profile import attest
    from ashlar_host.managed_a1_detached_admission import admit_detached_a1_plan
    from ashlar_host.managed_current_core import CARRIER_PROFILE_XXHASH64
    from ashlar_host.managed_current_truss053_genesis_owner import (
        ManagedCurrentTruss053GenesisSourceOwner,
    )
    from ashlar_host.r04_schedule import R04Schedule
    from ashlar_host.truss053_source_custody import Truss053SourceCustodySession
    from ashlar_host.truss_feed_sessions import encode_truss_destination_receipt
    from ashlar_host.truss_source_custody_selection import select_truss_source_custody_anchor
    from ashlar_host.truss_detached_a1_genesis_codec import encode_detached_a1_genesis_snapshot
    from ashlar_host.truss_detached_source_snapshot_codec import encode_detached_source_snapshot
    import run_managed_current_local_native_attempt14 as original_worker
    original_worker.RUNTIME = RUNTIME

    started = time.monotonic()
    if ROOT.exists() or ROOT.is_symlink():
        raise RuntimeError("One-use attempt14 recovery output root already exists")
    custody = _source_state_preflight()
    group_receipt = _verify_source_group()
    timestamp = original_worker._configure_utc_timestamp_driver()
    preflight = original_worker._effect_free_preflight()
    ROOT.mkdir(mode=0o700)
    if stat.S_IMODE(ROOT.stat().st_mode) != 0o700 or ROOT.stat().st_uid != os.getuid():
        raise RuntimeError("Owner-private recovery output root required")
    evidence = {"profile": "ashlar-managed-current-local-native-a1-recovery/0.1",
        "status": "running", "attempt14_root": str(ORIGINAL),
        "scope": {"runtime": "local Spark 4.2 / Delta 4.4.1", "truss": "0.0.55",
            "catalog": "spark_catalog", "schema": SCHEMA, "UC": False,
            "databricks": False, "source_authority": "original attempt14 Truss .55 A/B only",
            "source_hold_gap": "attempt14 source holder closed after A1 Delta commit; this recovery reopens a fresh hold on the exact still-pending A1 page",
            "source_group_replay": False, "A0_ack_replay": False,
            "delta_write": False, "hard_wall_seconds": MAX_WALL_SECONDS},
        "custody_preflight": custody, "committed_a1_group": group_receipt,
        "timestamp_runtime": timestamp, "preflight": preflight,
        "source_ack_attempted": False, "source_ack_outcome": "not_attempted",
        "delta_write_attempted": False, "source_group_replay_attempted": False,
        "delta_publication_replay_attempted": False, "cleanup_errors": []}
    receipt_path = ROOT / "qualification.json"
    spark = None
    source_runtimes = {}
    session = None
    session_open = False
    ack_dispatched = False
    try:
        attempt14 = json.loads((ORIGINAL / "qualification.json").read_bytes())
        runtime_bytes = (RUNTIME / "r61-native-runtime-manifest.json").read_bytes()
        runtime = json.loads(runtime_bytes)
        java_home = Path(runtime["java_home"])
        os.environ["JAVA_HOME"] = str(java_home)
        os.environ["PATH"] = str(java_home / "bin") + os.pathsep + os.environ.get("PATH", "")
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ["PYSPARK_PYTHON"] = str(Path(sys.executable).resolve())
        os.environ["PYSPARK_DRIVER_PYTHON"] = str(Path(sys.executable).resolve())
        from ashlar_host.truss_resume_cli import _CurrentTrussRuntime
        local_runtime = _CurrentTrussRuntime(Path(runtime["jars_directory"]), java_home,
            runtime["jar_inventory"], ORIGINAL / "spark-warehouse")
        _, spark_identity, jars = attest(local_runtime)
        spark = open_configured_local_spark(local_runtime, jars,
            ORIGINAL / "spark-warehouse", app_name="Ashlar attempt14 A1 recovery",
            driver_memory="512m")
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
        readonly_metrics = {"spark_sql_statements": 0, "rejected_mutation_statements": 0}
        original_worker._local_table_type_shim(spark)
        shimmed_sql = spark.sql

        def readonly_sql(statement, *args, **kwargs):
            compact = " ".join(statement.split())
            token = compact.split(" ", 1)[0].upper() if compact else ""
            if token not in {"SELECT", "SHOW", "DESCRIBE"}:
                readonly_metrics["rejected_mutation_statements"] += 1
                raise RuntimeError("Recovery refuses a non-read-only Spark SQL operation")
            readonly_metrics["spark_sql_statements"] += 1
            return shimmed_sql(statement, *args, **kwargs)

        spark.sql = readonly_sql
        evidence["spark"] = {"version": spark.version, "identity": spark_identity,
            "runtime_manifest_sha256": _sha(runtime_bytes),
            "jar_inventory": runtime["jar_inventory"],
            "catalog_attachment": "none; process-local Spark catalog is not retained",
            "native_read_scope": "literal original Delta warehouse paths and VERSION AS OF",
            "read_only_sql_guard": "SELECT/SHOW/DESCRIBE only"}
        paths = _warehouse_paths()
        log_states = {role: _delta_log_state(path) for role, path in paths.items()}
        genesis_uuids = attempt14["native_genesis"]["six_role_uuid"]
        if set(log_states) != set(genesis_uuids) or any(
                log_states[role]["uuid"] != genesis_uuids[role] for role in log_states):
            raise RuntimeError("Original six-role Delta path UUIDs differ from attempt14 genesis")
        evidence["opening_native_paths"] = {role: {"path": str(paths[role]),
            "uuid": state["uuid"], "head": state["head"],
            "log_sha256": state["log_sha256"]} for role, state in sorted(log_states.items())}

        (a0_manifest, a0_manifest_bytes, a0_progress, a0_report, schema_revisions,
         a0_checkpoint, a_binding, genesis_manifest, genesis_report) = \
            _a0_manifest(spark, paths, attempt14)
        _private(ROOT / "a0-native-manifest.json", a0_manifest_bytes)
        _private(ROOT / "original-genesis-native-manifest.json", _canonical(genesis_manifest))
        _private(ROOT / "original-genesis-validation-report.json", _canonical(genesis_report))
        schedule = R04Schedule((ORIGINAL / "sources" / "r04-schedule.json").read_bytes())
        source_started = time.monotonic()
        source_runtimes, source_selections, context = _start_source_runtimes(
            a0_progress, genesis_report, schedule, a0_checkpoint)
        source_selection = ManagedCurrentTruss053GenesisSourceOwner(
            schedule, source_selections).admit_a1(
                expected_a_prior_cursor=json.loads(a0_checkpoint.decode("utf-8"))["nextXid"])
        a1_admission_bundle_bytes = encode_detached_a1_genesis_snapshot(source_selection)
        genesis_bytes, genesis_composition = _rebuild_original_genesis(
            attempt14=attempt14, genesis_manifest=genesis_manifest,
            genesis_report=genesis_report, schedule=schedule,
            current_selection_bytes=a1_admission_bundle_bytes)
        evidence["original_genesis_reconstruction"] = genesis_composition
        source_observation = source_selection.source_observations["A"]
        source_page = source_observation.page
        if (source_page.outcome != "transaction"
                or source_page.prior.content != a0_checkpoint
                or source_page.writer_xid != group_receipt["writer_xid"]
                or type(source_page.sha256) is not str or len(source_page.sha256) != 64
                or len(source_selection.admission.changes) != 7):
            raise RuntimeError("Current public A1 page differs from exact pending attempt14 group/cursor")
        for source in ("A", "B"):
            binding = schedule.document()["source_bindings"][source]
            selected_progress = source_selection.source_progress[source]
            current = a0_progress[binding["feed"]]
            if (selected_progress.feed != current.get("feed")
                    or selected_progress.epoch != current.get("epoch")
                    or selected_progress.position != current.get("position")
                    or selected_progress.installation_id != binding["installation_id"]
                    or selected_progress.consumer_id != binding["consumer_id"]
                    or selected_progress.consumer_registration_id != binding["registration_id"]):
                raise RuntimeError(f"Public {source} marker differs from exact A0 source position")
        evidence["source_open_and_selection"] = {"elapsed_seconds": round(
                time.monotonic() - source_started, 6),
            "A1_page_sha256": source_page.sha256,
            "A1_page_writer_xid": source_page.writer_xid,
            "A1_page_prior_sha256": _sha(source_page.prior.content),
            "A1_page_next_sha256": _sha(source_page.next.content),
            "A1_group_writer_xid": group_receipt["writer_xid"],
            "A1_changes": len(source_selection.admission.changes),
            "A_progress_matches_A0_manifest": True,
            "B_progress_matches_A0_manifest": True,
            "original_genesis_snapshot_sha256": _sha(genesis_bytes),
            "a1_admission_bundle_sha256": _sha(a1_admission_bundle_bytes)}

        batch_rows, attempt_table_version = _attempt_rows(
            spark, paths["publication_attempt_phase"],
            log_states["publication_attempt_phase"]["head"],
            source_selection.admission.batch)
        from ashlar.staging import batch_row
        source_batch_row = batch_row(source_selection.admission.batch)
        exact_groups = {}
        for row in batch_rows:
            payload = _json(row["payload_json"].encode(), "attempt phase payload")
            request = payload.get("request")
            if type(request) is not dict:
                raise RuntimeError("Original attempt phase lacks exact request")
            checkpoint_doc = _json(request.get("source_checkpoint_json", "").encode(),
                                   "attempt source checkpoint")
            if (request.get("source_batch_digest") == source_batch_row["batch_digest"]
                    and request.get("source_batch_json") == source_batch_row["batch_json"]
                    and request.get("predecessor") == A0_PUBLICATION_ID
                    and checkpoint_doc.get("feed") == a_binding["feed"]
                    and checkpoint_doc.get("epoch") == a_binding["epoch"]
                    and checkpoint_doc.get("previous") == base64.b64encode(a0_checkpoint).decode("ascii")):
                exact_groups.setdefault((row["stream"], row["batch_id"]), []).append(row)
        if len(exact_groups) != 1:
            raise RuntimeError("No unique retained committed A1 request matches the live pending source page")
        (stream, batch_id), retained_rows = next(iter(exact_groups.items()))
        _private(ROOT / "retained-a1-attempt-phase-rows.json", _canonical(retained_rows))
        if batch_id != source_selection.admission.batch.batch_id:
            raise RuntimeError("Retained attempt batch identity differs from current public source page")
        phase_order = ("prepared", "applying", "applied", "committing", "committed")
        phases = {row["phase"]: row for row in retained_rows}
        if (len(retained_rows) != len(phase_order)
                or len(phases) != len(phase_order)
                or set(phases) != set(phase_order)):
            raise RuntimeError("Original A1 attempt is not one fully committed five-phase journal")
        ordered_phase_rows = [phases[phase] for phase in phase_order]
        prepared_payload = _json(phases["prepared"]["payload_json"].encode(), "prepared A1 phase")
        original_request = prepared_payload["request"]
        request_bytes = _canonical(original_request)
        _private(ROOT / "retained-a1-original-request.json", request_bytes)
        original_plan_text = prepared_payload.get("plan_json")
        if type(original_plan_text) is not str:
            raise RuntimeError("Original prepared A1 plan bytes absent from retained journal")
        _private(ROOT / "retained-a1-original-plan.json", original_plan_text.encode("utf-8"))
        original_plan = _json(original_plan_text.encode(), "prepared A1 plan")
        materialized_at = original_plan.get("recordedAt")
        if type(materialized_at) is not str:
            raise RuntimeError("Original A1 plan lacks exact materialization clock")
        plan = admit_detached_a1_plan(genesis_snapshot_bytes=a1_admission_bundle_bytes,
            genesis_manifest=a0_manifest, stream=stream,
            materialized_at=materialized_at, context=context, continuation=True)
        if (dict(plan.request) != original_request
                or plan.publication_id != original_plan.get("publicationId")
                or plan.request["batch_id"] != batch_id):
            raise RuntimeError("Reconstructed A1 plan/request differs from retained committed journal")
        exact_records = validate_source_qualified_phase_inventory(stream, batch_id,
            [{key: row[key] for key in ("phase", "request_digest", "payload_json", "payload_digest")}
             for row in ordered_phase_rows], original_request)
        if tuple(record.phase for record in exact_records) != phase_order:
            raise RuntimeError("Original A1 publication phase journal is not complete and exact")
        committed_payload = _json(phases["committed"]["payload_json"].encode(),
                                  "committed A1 phase")
        applied, descriptor = validate_applied_artifact(
            committed_payload["result_json"], original_request)
        retained_descriptor = _json(committed_payload["descriptor_json"].encode(),
                                    "committed A1 descriptor")
        if retained_descriptor != dict(descriptor.raw):
            raise RuntimeError("Committed A1 descriptor differs from its original applied artifact")
        a1_manifest = dict(descriptor.raw)
        a1_manifest_bytes = _canonical(a1_manifest)
        _private(ROOT / "retained-a1-original-manifest.json", a1_manifest_bytes)
        if _canonical(retained_descriptor) != a1_manifest_bytes:
            raise RuntimeError("Exact original A1 manifest bytes do not recompose")
        evidence["committed_a1_journal"] = {"stream": stream, "batch_id": batch_id,
            "attempt_table_version": attempt_table_version,
            "phases": [record.phase for record in exact_records],
            "request_sha256": _sha(_canonical(original_request)),
            "request_digest": original_request["request_digest"],
            "prepared_plan_sha256": _sha(original_plan_text.encode()),
            "publication_id": descriptor.publication_id,
            "manifest_sha256": _sha(a1_manifest_bytes),
            "descriptor_json_sha256": _sha(committed_payload["descriptor_json"].encode()),
            "delta_replay_or_write": False}

        from ashlar.manifest import manifest_pin_vector
        role_names = ("object_current", "edge_current", "tombstone", "whole_source_history")
        role_uuids = {f"spark_catalog.{SCHEMA}.{role}":
                      attempt14["native_genesis"]["six_role_uuid"][role] for role in role_names}
        vector = manifest_pin_vector(a1_manifest, role_uuids,
            authority="managed-current-spark:local-native-qualification")
        vector_wire = {"authority": vector.authority, "scope_kind": vector.scope_kind,
            "scope_id": vector.scope_id, "custody_digest": vector.custody_digest,
            "targets": {name: list(pin) for name, pin in vector.targets.items()}}
        receipt_bytes = encode_truss_destination_receipt(
            request_bytes, a1_manifest_bytes, vector)
        _private(ROOT / "retained-a1-vector.json", _canonical(vector_wire))
        _private(ROOT / "retained-a1-destination-receipt.bin", receipt_bytes)
        manifest_head = log_states["publication_manifest"]["head"]
        exact_manifest_row = _pinned_manifest_row(spark, paths["publication_manifest"],
            manifest_head, descriptor.publication_id)
        if exact_manifest_row != a1_manifest:
            raise RuntimeError("Exact A1 manifest absent/different at retained native path version")
        anchor = select_truss_source_custody_anchor(schedule=schedule, source="A",
            page=source_page, request_bytes=_canonical(original_request),
            destination_id="spark_catalog." + SCHEMA,
            preselected_publication_id=descriptor.publication_id, incremental=True)
        open_selection = anchor.open_selection()
        selected_a = source_selections["A"]

        def connect_source(_context):
            return selected_a.connection_factory()

        session = Truss053SourceCustodySession(
            connection_factory=connect_source, selection=open_selection, context=context)
        session.open()
        session_open = True
        held_page = session.observe_selected_page()
        page_fields = ("content", "sha256", "prior", "next", "consumer_id",
                       "registration_id", "writer_xid", "outcome")
        if any(getattr(held_page, field) != getattr(source_page, field) for field in page_fields):
            raise RuntimeError("Reopened source hold differs from exact selected pending A1 page")
        selection_bytes = encode_detached_source_snapshot(anchor, source_observation)
        _private(ROOT / "live-a1-page-selection.bin", selection_bytes)
        from ashlar_host.truss_detached_source_snapshot_codec import decode_detached_source_snapshot
        detached_source = decode_detached_source_snapshot(selection_bytes)
        if (detached_source.source_page.prior_bytes != a0_checkpoint
                or detached_source.source_page.publication_id != descriptor.publication_id
                or detached_source.source_page.request_bytes != _canonical(original_request)):
            raise RuntimeError("Fresh A1 source selection does not bind exact ACK checkpoint/publication")

        readback = _independent_path_readback(spark=spark, paths=paths,
            log_states=log_states, attempt14=attempt14, plan=plan,
            genesis_bytes=genesis_bytes, held_source_bytes=selection_bytes,
            initial_manifest_bytes=a0_manifest_bytes,
            request_bytes=_canonical(original_request), manifest=a1_manifest,
            manifest_bytes=a1_manifest_bytes, vector=vector, receipt_bytes=receipt_bytes)
        if (readback.get("readback_passed") is not True
                or readback.get("source_expected_rows") != readback.get("native_rows")
                or readback.get("manifest_sha256") != _sha(a1_manifest_bytes)
                or readback.get("publication_id") != descriptor.publication_id):
            raise RuntimeError("Independent exact A1 source/path-native four-role bag proof failed")
        proof = {"profile": "ashlar-managed-current-local-a1-pre-ack-proof/0.1",
            "phase": "A1 seven-operation seed", "readback_passed": True,
            "publication_id": descriptor.publication_id,
            "manifest_sha256": _sha(a1_manifest_bytes),
            "request_sha256": _sha(_canonical(original_request)),
            "source_page_sha256": source_page.sha256,
            "source_selection_sha256": _sha(selection_bytes),
            "source_prior_cursor_sha256": _sha(detached_source.source_page.prior_bytes),
            "source_next_cursor_sha256": _sha(detached_source.source_page.next_bytes),
            "source_ack_outcome_before_dispatch": "not_attempted",
            "delta_write_state": "not_attempted",
            "source_group_replay": False,
            "roles": readback["roles"], "vector": readback["vector"],
            "source_expected_rows": readback["source_expected_rows"],
            "native_rows": readback["native_rows"],
            "source_oracle_sha256": readback["source_oracle_sha256"],
            "comparison_sha256": readback["comparison_sha256"],
        "readback_elapsed_seconds": readback["elapsed_seconds"],
            "native_catalog_claim": False,
        "readback_native_path_mode": "literal Delta paths with VERSION AS OF"}
        _private(ROOT / "pre-ack-ack-a1.json", _canonical(proof) + b"\n")
        evidence["independent_a1_readback"] = proof
        evidence["source_hold_gap"] = {
            "prior_attempt14_holder_closed": True,
            "new_public_hold_opened": True,
            "same_public_page_hash": source_page.sha256,
            "same_consumer_prior_checkpoint": _sha(a0_checkpoint),
            "no_source_group_or_delta_replay": True}

        closing_states = {role: _delta_log_state(path) for role, path in paths.items()}
        if closing_states != log_states:
            raise RuntimeError("Original native Delta JSON log changed during read-only recovery")
        evidence["native_pre_ack_close"] = {role: {"uuid": value["uuid"],
            "head": value["head"], "log_sha256": value["log_sha256"]}
            for role, value in sorted(closing_states.items())}
        binder_observation_type = __import__(
            "ashlar_host.managed_current_spark_receipt_read",
            fromlist=["ManagedCurrentSparkReceiptReopenObservation"]).ManagedCurrentSparkReceiptReopenObservation
        binder_observation = binder_observation_type(
            "ashlar-managed-current-spark-receipt-reopen/0.1",
            descriptor.publication_id, a1_manifest, _sha(a1_manifest_bytes),
            receipt_bytes, _sha(receipt_bytes), vector,
            readback["installation_inventory"])
        bound_selection = anchor.bind_publication(manifest_bytes=a1_manifest_bytes,
            vector=vector, receipt_bytes=receipt_bytes, observation=binder_observation)
        session.bind_publication(bound_selection)

        ack_dir = ROOT / "ack-a1"
        ack_dir.mkdir(mode=0o700)
        ack_store = DurableA1AckAttemptStore(ack_dir)
        page = detached_source.source_page
        ack_attempt = ack_store.begin(selection_bytes=selection_bytes,
            request_bytes=_canonical(original_request), manifest_bytes=a1_manifest_bytes,
            receipt_bytes=receipt_bytes, prior_checkpoint_bytes=page.prior_bytes,
            next_checkpoint_bytes=page.next_bytes,
            page_sha256=page.page_fields["sha256"],
            destination_id="spark_catalog." + SCHEMA,
            publication_id=descriptor.publication_id)
        generation = ack_store.record_worker_started(process_group_id=os.getpgrp(),
            selection_bytes=selection_bytes)
        ack_store.mark_ack_dispatched(process_group_id=os.getpgrp(), generation=generation)
        ack_dispatched = True
        evidence["source_ack_attempted"] = True
        evidence["source_ack_outcome"] = "unknown_after_dispatch"
        try:
            session.acknowledge()
        except BaseException:
            # An ACK command may have committed before a client-side refusal.
            # Reconcile only on this original session; never dispatch again.
            reconciled = session.reconcile()
        else:
            reconciled = session.reconcile()
        settlement = A1AckSettlement(reconciled.outcome,
            reconciled.checkpoint.content,
            reconciled.receipt.receipt_bytes if reconciled.outcome == "matched" else None,
            ack_attempt.request_sha256, ack_attempt.manifest_sha256,
            ack_attempt.selection_sha256)
        ack_store.record_settlement(settlement)
        if settlement.outcome != "matched":
            evidence["source_ack_outcome"] = settlement.outcome
            raise RuntimeError("Protected attempt14 A1 ACK did not reconcile as matched")
        session.close()
        session_open = False
        session.dispose()
        session = None
        evidence["source_ack_outcome"] = "matched"
        evidence["ack"] = {"outcome": "matched", "attempt_sha256": _sha(
                ack_store.path.read_bytes()), "selection_sha256": ack_attempt.selection_sha256,
            "request_sha256": settlement.request_sha256,
            "manifest_sha256": settlement.manifest_sha256,
            "receipt_sha256": _sha(receipt_bytes),
            "checkpoint_sha256": _sha(settlement.checkpoint_bytes),
            "ack_retry_permitted": False, "attempt14_group_replayed": False,
            "delta_write_attempted": False}
        evidence["status"] = "passed_attempt14_a1_ack_recovery"
    except BaseException as error:
        evidence["status"] = "failed"
        evidence["error"] = {"type": type(error).__name__,
            "message": str(error)[:2000],
            "traceback": traceback.format_exc(limit=8)[-6000:]}
        evidence["source_ack_outcome"] = ("unknown_after_dispatch" if ack_dispatched
            else evidence["source_ack_outcome"])
        raise
    finally:
        if session is not None:
            try:
                if session_open and not ack_dispatched:
                    session.close()
                elif session_open and ack_dispatched:
                    # Do not hide uncertainty; disposal releases the connection
                    # without issuing a second ACK or fabricating reconciliation.
                    pass
            except BaseException as error:
                evidence["cleanup_errors"].append(
                    "Truss custody close: " + type(error).__name__ + ": " + str(error)[:500])
            try:
                session.dispose()
            except BaseException as error:
                evidence["cleanup_errors"].append(
                    "Truss connection disposal: " + type(error).__name__ + ": " + str(error)[:500])
        for source, runtime in reversed(tuple(source_runtimes.items())):
            try:
                runtime.__exit__(None, None, None)
            except BaseException as error:
                evidence["cleanup_errors"].append(
                    f"PostgreSQL {source} close: " + type(error).__name__ + ": " + str(error)[:500])
        evidence["source_postgres_closed"] = all(not (ORIGINAL / "sources" / f"pgdata-{source}" / "postmaster.pid").exists()
            for source in ("A", "B"))
        if spark is not None:
            try:
                spark.stop()
                evidence["spark_closed"] = True
            except BaseException as error:
                evidence["cleanup_errors"].append(
                    "Spark close: " + type(error).__name__ + ": " + str(error)[:500])
        else:
            evidence["spark_closed"] = True
        evidence["elapsed_seconds"] = round(time.monotonic() - started, 3)
        evidence["read_only_sql_guard"] = readonly_metrics if "readonly_metrics" in locals() else None
        evidence["closed_before_receipt"] = evidence.get("spark_closed") is True \
            and evidence.get("source_postgres_closed") is True
        if evidence["status"] == "passed_attempt14_a1_ack_recovery" and evidence["cleanup_errors"]:
            evidence["status"] = "failed_cleanup"
        _private(receipt_path, _canonical(evidence) + b"\n")
    if time.monotonic() - started > MAX_WALL_SECONDS:
        raise RuntimeError("External 600-second hard cap exceeded; recovery success withheld")
    if evidence["status"] != "passed_attempt14_a1_ack_recovery":
        raise RuntimeError("Attempt14 A1 recovery did not fully pass and close")
    print(json.dumps({"status": evidence["status"], "receipt": str(receipt_path),
        "elapsed_seconds": evidence["elapsed_seconds"],
        "a1_manifest_sha256": evidence["committed_a1_journal"]["manifest_sha256"],
        "a1_ack_outcome": evidence["ack"]["outcome"]}, separators=(",", ":")))


if __name__ == "__main__":
    main()
