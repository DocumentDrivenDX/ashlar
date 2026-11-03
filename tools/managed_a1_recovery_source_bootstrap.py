# SPDX-License-Identifier: MIT OR Apache-2.0
"""Restore the exact retained public A/B Truss source for managed A1 recovery.

This recovery-only path opens existing stopped PostgreSQL 18 clusters. It does
not install Truss, register a consumer, mutate a source page, or ACK anything.
The ordinary source-holder worker continues to own the live session and ACK
process group after the read-only recovery comparison succeeds.
"""
from __future__ import annotations

import base64
from contextlib import ExitStack
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys
import time
import uuid


PROFILE = "ashlar-managed-a1-recovery-source-bootstrap/0.1"
MAX_EXPECTED_BYTES = 4 * 1024 * 1024
SOURCE_BOOTSTRAP = "tools.managed_a1_recovery_source_bootstrap:bootstrap_from_recovery_private_config"
SOURCE_A1_RESUME_BOOTSTRAP = (
    "tools.managed_a1_recovery_source_bootstrap:bootstrap_from_a1_resume_private_config")
SOURCE_A1_RESUME_PROFILE = "ashlar-managed-a1-recovery-a1-source-bootstrap/0.1"
ACK_REAP_RESERVE_SECONDS = 30
POST_ACK_CLEANUP_RESERVE_SECONDS = 30
MAX_TRANSITION_SECONDS = 90


def _need(condition, message):
    if not condition:
        raise ValueError(message)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("Duplicate recovery source configuration member")
        result[key] = value
    return result


def _decode_config(raw):
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (UnicodeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("Canonical recovery source configuration required") from error
    _need(type(value) is dict and _canonical(value) == raw
          and set(value) == {"profile", "source_bootstrap_config_path",
              "source_bootstrap_config_sha256", "expected_genesis_bytes_base64",
              "expected_selection_bytes_base64", "expected_request_bytes_base64",
              "expected_publication_id", "expected_destination_id"}
          and value["profile"] == PROFILE,
          "Closed canonical recovery source configuration required")
    path = Path(value["source_bootstrap_config_path"])
    _need(type(value["source_bootstrap_config_path"]) is str and path.is_absolute()
          and path.resolve(strict=True) == path and not path.is_symlink()
          and path.is_file(), "Canonical original private source config required")
    info = path.stat()
    _need(info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o600,
          "Original private source config ownership/mode required")
    config_bytes = path.read_bytes()
    _need(hashlib.sha256(config_bytes).hexdigest() == value["source_bootstrap_config_sha256"],
          "Original private source config changed")
    expected = {}
    for name in ("expected_genesis_bytes", "expected_selection_bytes",
                 "expected_request_bytes"):
        encoded = value[name + "_base64"]
        _need(type(encoded) is str and len(encoded) <= 4 * ((MAX_EXPECTED_BYTES + 2) // 3),
              f"Bounded {name} required")
        try:
            raw_value = base64.b64decode(encoded, validate=True)
        except (ValueError, TypeError) as error:
            raise ValueError(f"Canonical {name} base64 required") from error
        _need(0 < len(raw_value) <= MAX_EXPECTED_BYTES
              and base64.b64encode(raw_value).decode("ascii") == encoded,
              f"Bounded canonical {name} bytes required")
        expected[name] = raw_value
    for name in ("expected_publication_id", "expected_destination_id"):
        _need(type(value[name]) is str and value[name] and "\0" not in value[name],
              f"Exact {name} required")
    return value, config_bytes, expected


def _require_owned_file(path: Path, *, mode=None):
    info = path.lstat()
    _need(stat.S_ISREG(info.st_mode) and not stat.S_ISLNK(info.st_mode)
          and info.st_uid == os.getuid()
          and (mode is None or stat.S_IMODE(info.st_mode) == mode),
          f"Owned regular private file required: {path.name}")


def _require_existing_pgdata(path: Path):
    info = path.lstat()
    _need(stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode)
          and info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o700,
          "Exact retained owner-private PGDATA directory required")
    version = path / "PG_VERSION"
    _require_owned_file(version)
    _need(version.read_text(encoding="ascii").strip() == "18"
          and not (path / "postmaster.pid").exists()
          and not (path / "postmaster.pid").is_symlink(),
          "Stopped initialized PostgreSQL 18 source required")
    control = path / "global" / "pg_control"
    _require_owned_file(control)


def _b64(raw):
    return base64.b64encode(raw).decode("ascii")


def _existing_genesis_and_selection(expected):
    from ashlar_host.truss_detached_a1_genesis_codec import (
        decode_detached_a1_genesis_snapshot,
    )
    from ashlar_host.truss_detached_source_snapshot_codec import (
        decode_detached_source_snapshot,
    )
    genesis = decode_detached_a1_genesis_snapshot(expected["expected_genesis_bytes"])
    selected = decode_detached_source_snapshot(expected["expected_selection_bytes"])
    page = selected.source_page
    a = genesis.source_evidence("A")
    b = genesis.source_evidence("B")
    _need(a["progress"]["position"] == _b64(a["progress"]["cursor_bytes"])
          and b["progress"]["position"] == _b64(b["progress"]["cursor_bytes"]),
          "Exact original A/B genesis cursors required")
    _need(a["progress"]["position"] == _b64(page.prior_bytes)
          and page.source == "A"
          and page.schedule_sha256 == genesis.schedule_sha256
          and page.request_bytes == expected["expected_request_bytes"]
          and page.publication_id == expected["expected_publication_id"]
          and page.destination_id == expected["expected_destination_id"],
          "Retained A0 source snapshot, request, schedule, and destination must agree")
    _need(page.page_content == base64.b64decode(
              a["page"]["content_base64"], validate=True)
          and page.prior_bytes == base64.b64decode(
              a["page"]["prior_bytes_base64"], validate=True)
          and page.next_bytes == base64.b64decode(
              a["page"]["next_bytes_base64"], validate=True)
          and page.page_fields["sha256"] == a["page"]["sha256"],
          "Retained A0 held page differs from exact original A genesis")
    return genesis, selected


def _monotonic_watermark_match(original: bytes, refreshed: bytes, *,
                               kind: str, expected_a_prior_cursor="0") -> bool:
    """Compare closed source evidence, allowing only safe-watermark advance."""
    from ashlar_host.truss_detached_a1_genesis_codec import (
        decode_detached_a1_genesis_snapshot,
    )
    from ashlar_host.truss_detached_source_snapshot_codec import (
        decode_detached_source_snapshot,
    )
    if kind == "genesis":
        from ashlar_host.truss053_source_custody import (
            _genesis_matches_with_monotonic_watermarks,
        )
        return _genesis_matches_with_monotonic_watermarks(original, refreshed,
            expected_a_prior_cursor=expected_a_prior_cursor)
    if kind != "selection":
        return False
    decode_detached_source_snapshot(original)
    decode_detached_source_snapshot(refreshed)
    before, after = json.loads(original), json.loads(refreshed)
    old_page = json.loads(base64.b64decode(before["source_page_bytes_base64"], validate=True))
    new_page = json.loads(base64.b64decode(after["source_page_bytes_base64"], validate=True))
    old, new = old_page["page"]["safe_watermark_xid"], new_page["page"]["safe_watermark_xid"]
    if not _monotone_xid(old, new):
        return False
    old_page["page"]["safe_watermark_xid"] = new_page["page"]["safe_watermark_xid"] = "<frontier>"
    old_page["binding_sha256"] = new_page["binding_sha256"] = "<verified>"
    before["source_page_bytes_base64"] = _b64(_canonical(old_page))
    after["source_page_bytes_base64"] = _b64(_canonical(new_page))
    before["binding_sha256"] = after["binding_sha256"] = "<verified>"
    return before == after


def _monotone_xid(old, new):
    def valid(value):
        return (type(value) is str and value.isascii() and value.isdecimal()
            and len(value) <= 20 and str(int(value)) == value
            and int(value) <= 18446744073709551615)
    return valid(old) and valid(new) and int(new) >= int(old)


def _safe_watermarks(raw: bytes, *, kind: str) -> dict:
    value = json.loads(raw)
    if kind == "genesis":
        sources = {item["source"]: item for item in value["sources"]}
        return {source: sources[source]["page"]["safe_watermark_xid"]
                for source in ("A", "B")}
    if kind == "selection":
        page = json.loads(base64.b64decode(value["source_page_bytes_base64"], validate=True))
        return {page["source"]: page["page"]["safe_watermark_xid"]}
    raise ValueError("Closed source evidence kind required")


def _reopen_retained_source(config_bytes, expected, *, resume_a1=None):
    """Open old A/B PGDATA read-only and return the normal A0 holder."""
    from ashlar_host.managed_a1_source_bootstrap import (
        MODEL_ID, MODEL_REVISION, PUBLIC_SETUP_HELPER_SHA256,
        PUBLIC_SETUP_HELPER_RELATIVE_PATH, candidate_seed_group,
        canonical_json, public_group_seed, validate_private_bootstrap_config,
    )
    from ashlar_host.managed_current_truss053_genesis_owner import (
        ManagedCurrentTruss053GenesisSourceOwner,
        ManagedCurrentTruss053SourceSelection,
    )
    from ashlar_host.truss_detached_a1_genesis_codec import (
        decode_detached_a1_genesis_snapshot, encode_detached_a1_genesis_snapshot,
    )
    from ashlar_host.r04_schedule import R04Schedule
    from ashlar_host.truss_current_catalog import installed_fresh_truss_runtime_pins
    from ashlar_host.truss053_source_custody import Truss053A1SourceHolder
    from ashlar_host.truss_note_publication import read_page
    from ashlar_host.truss_record_group import run_create_truss_record_group_command
    from ashlar_host.truss_runtime import require_truss_psycopg_runtime
    from ashlar.truss_native_feed import native_feed_batch, typed_scalar_entity_changes
    from importlib.metadata import version as distribution_version
    import psycopg
    from truss import LocalPostgres
    import truss.local_runtime as local_runtime

    config = validate_private_bootstrap_config(config_bytes)
    root = Path(config["root"])
    _need(root.is_dir() and not root.is_symlink() and root.resolve(strict=True) == root
          and root.stat().st_uid == os.getuid()
          and stat.S_IMODE(root.stat().st_mode) == 0o700,
          "Original private retained A/B source root required")
    model_path = Path(config["model_path"])
    setup_path = Path(config["setup_helper_path"])
    _need(model_path == Path(config["repository_root"]) /
              "docs/helix/04-build/evidence/current-upstream-release-20261101/"
              "r21-truss052-fresh-source-custody/r28-page-sequence/r28-model.umf.json"
          and setup_path == Path(config["repository_root"]) / PUBLIC_SETUP_HELPER_RELATIVE_PATH
          and hashlib.sha256(setup_path.read_bytes()).hexdigest() == PUBLIC_SETUP_HELPER_SHA256,
          "Exact original R28 model and public setup helper paths required")
    _need(config["a1_group"] == candidate_seed_group("A"),
          "Exact already-frozen R28 A1 group required")

    genesis, _ = _existing_genesis_and_selection(expected)
    schedule = R04Schedule(genesis.schedule_bytes)
    schedule_path = root / "r04-schedule.json"
    _require_owned_file(schedule_path, mode=0o600)
    _need(schedule_path.read_bytes() == genesis.schedule_bytes,
          "Retained R04 schedule differs from original genesis bytes")
    for source in ("A", "B"):
        data = genesis.source_evidence(source)
        cursor_path = root / f"genesis-cursor-{source}.json"
        _require_owned_file(cursor_path, mode=0o600)
        _need(cursor_path.read_bytes() == base64.b64decode(
              data["progress"]["cursor_bytes_base64"], validate=True),
              f"Retained {source} genesis cursor differs from original source evidence")
        _require_existing_pgdata(root / f"pgdata-{source}")

    require_truss_psycopg_runtime()
    runtime_pins = installed_fresh_truss_runtime_pins()
    _need(runtime_pins["version"] == "0.0.55"
          and runtime_pins["source_custody_profile"] ==
              "ashlar-truss-055-fresh-baseline-source-custody/0.1"
          and distribution_version("umf-truss") == "0.0.55",
          "Exact installed fresh public Truss 0.0.55 runtime required")

    source_root = Path(__file__).resolve().parents[1]
    _need(config["repository_root"] == str(source_root.resolve(strict=True)),
          "Retained source repository root differs from recovery tool checkout")
    helper = importlib.util.spec_from_file_location("_ashlar_recovery_r61_setup", setup_path)
    _need(helper is not None and helper.loader is not None,
          "Exact public Truss setup helper import required")
    setup_module = importlib.util.module_from_spec(helper)
    helper.loader.exec_module(setup_module)
    _need(hashlib.sha256(Path(local_runtime.__file__).read_bytes()).hexdigest()
          == setup_module.LOCAL_RUNTIME_SHA256,
          "Exact selected public Truss PostgreSQL 18 runtime source required")

    data_directories = {source: root / f"pgdata-{source}" for source in ("A", "B")}
    with ExitStack() as stack:
        runtimes = {source: stack.enter_context(LocalPostgres(data_directories[source]))
                    for source in ("A", "B")}
        selections = {}
        for source in ("A", "B"):
            runtime = runtimes[source]
            _need(runtime.info.data_directory.resolve(strict=True) == data_directories[source]
                  and runtime.info.server_version.split(".", 1)[0] == "18"
                  and runtime.info.socket_directory is not None,
                  f"Exact original owned PostgreSQL 18 {source} runtime required")
            with psycopg.connect(runtime.info.connection_uri, autocommit=True,
                                 connect_timeout=5) as health:
                _need(health.execute("SHOW fsync").fetchone()[0] == "on",
                      "Source PostgreSQL fsync must remain enabled")
            identity = genesis.source_evidence(source)["identity"]
            parameters = psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri)
            ordinary_uri = psycopg.conninfo.make_conninfo(
                **dict(parameters, user="ashlar_note_consumer"))
            def ordinary_factory(uri=ordinary_uri):
                return psycopg.connect(uri, autocommit=False, connect_timeout=5,
                    options="-c statement_timeout=10000 -c lock_timeout=2000")
            selections[source] = ManagedCurrentTruss053SourceSelection(
                source, identity["feed"], identity["epoch"], identity["installation_id"],
                identity["consumer_id"], identity["consumer_registration_id"],
                identity["catalog_registration_id"], identity["catalog_revision"],
                ordinary_factory)

        # Read-only re-admission checks the exact original installations,
        # registration IDs, accepted catalog bytes, cursors, and A0 pending page.
        if resume_a1 is None:
            fresh_selection = ManagedCurrentTruss053GenesisSourceOwner(
                schedule, selections).admit_a1(expected_a_prior_cursor="0")
            fresh_genesis_bytes = encode_detached_a1_genesis_snapshot(fresh_selection)
            _need(_monotonic_watermark_match(expected["expected_genesis_bytes"],
                  fresh_genesis_bytes, kind="genesis", expected_a_prior_cursor="0"),
                  "Retained public A/B genesis changed beyond monotonic safe watermarks")

        ids = config["source_ids"]
        origin = b'{"operator":"ashlar-managed-current-a1-fresh-local-source"}\n'
        for source in ("A", "B"):
            origin_path = root / f"origin-{source}.json"
            _require_owned_file(origin_path, mode=0o600)
            _need(origin_path.read_bytes() == origin,
                  f"Original {source} Truss source origin bytes differ")

        # Revalidate the persisted exact A1 group transport against the original
        # plain semantic group and the restarted source's ordinary URI.
        a_uri = psycopg.conninfo.make_conninfo(**dict(
            psycopg.conninfo.conninfo_to_dict(runtimes["A"].info.connection_uri),
            user="ashlar_note_consumer"))
        group_path = root / "a1-group.json"
        _require_owned_file(group_path, mode=0o600)
        group_value = json.loads(group_path.read_bytes())
        expected_group = {"profile": "ashlar-truss-record-group/0.1",
            "connection_uri": a_uri, "bun": config["bun_path"],
            "document_path": config["model_path"], "document_revision": MODEL_REVISION,
            "document_id": MODEL_ID, "source_version": "0.10.0",
            "origin_path": str(root / "origin-A.json"),
            "request_key": "ashlar-managed-a1-a1-seven-operation-group",
            "records": public_group_seed(config["a1_group"])["records"],
            "edges": public_group_seed(config["a1_group"])["edges"]}
        _need(group_value == expected_group
              and group_path.read_bytes() == canonical_json(expected_group),
              "Exact original frozen seven-operation A1 group transport required")

        holder = Truss053A1SourceHolder(schedule=schedule,
            source_selections=selections, context=object())
        if resume_a1 is not None:
            fresh_a1_genesis = holder.resume_a1_after_matched_a0(
                resume_a1["attempt"], resume_a1["settlement"])
            _need(_monotonic_watermark_match(resume_a1["expected_genesis_bytes"],
                  fresh_a1_genesis, kind="genesis",
                  expected_a_prior_cursor=resume_a1["expected_a_prior_cursor"]),
                  "Fresh A1 public genesis differs beyond safe-watermark advance")
            _need(holder.phase == "a1",
                  "Matched A0 recovery must enter the explicit A1 continuation phase")
            _need(holder.request_bytes is None and holder.session is None,
                  "Fresh resumed A1 genesis must precede prepublication open")
            holder._bootstrap_runtimes = stack.pop_all()
            return holder
        holder.phase = "a0"

        def apply_frozen_a1_after_a0(checkpoint_bytes):
            current_a = read_page(selections["A"].connection_factory,
                ids["A"]["consumer_id"], maximum_bytes=1048576)
            current_b = read_page(selections["B"].connection_factory,
                ids["B"]["consumer_id"], maximum_bytes=1048576)
            b_cursor = base64.b64decode(
                genesis.source_evidence("B")["progress"]["cursor_bytes_base64"],
                validate=True)
            _need(current_a.prior.content == checkpoint_bytes
                  and current_a.outcome == "empty_interval"
                  and current_b.prior.content == b_cursor,
                  "Matched A0 checkpoint and untouched B genesis cursor are required")
            result_path = root / "a1-public-result.json"
            freeze_path = root / "a1-truss-request-freeze.json"
            _need(not result_path.exists() and not result_path.is_symlink()
                  and not freeze_path.exists() and not freeze_path.is_symlink(),
                  "Fresh single-use A1 public group result and freeze targets required")
            run_create_truss_record_group_command(group_path, result_path,
                require_fresh=True, freeze_request_path=freeze_path,
                schedule_sha256=schedule.sha256, slot="A1", group_timeout_ms=60000)
            page = read_page(selections["A"].connection_factory,
                ids["A"]["consumer_id"], maximum_bytes=1048576)
            _need(page.outcome == "transaction"
                  and page.registration_id == selections["A"].consumer_registration_id
                  and page.prior.content == checkpoint_bytes,
                  "Exact public A1 transaction page must continue matched A0 cursor")
            batch = native_feed_batch(page.content, sha256=page.sha256,
                prior=page.prior.content, next=page.next.content, feed=ids["A"]["feed"])
            parsed = typed_scalar_entity_changes(page.content, sha256=page.sha256,
                prior=page.prior.content, next=page.next.content, feed=ids["A"]["feed"])
            _need(batch.epoch == selections["A"].epoch and len(parsed.changes) == 7,
                  "Exact seven-operation accepted A1 page required")
            holder._a0_page_next = checkpoint_bytes
            holder.genesis = None
            holder.genesis_bytes = None

        holder._advance_a1_callback = apply_frozen_a1_after_a0
        holder._bootstrap_runtimes = stack.pop_all()
        return holder


def bootstrap_from_recovery_private_config(config_bytes: bytes):
    """Worker bootstrap that opens stopped public sources without source writes."""
    value, source_config_bytes, expected = _decode_config(config_bytes)
    expected["expected_publication_id"] = value["expected_publication_id"]
    expected["expected_destination_id"] = value["expected_destination_id"]
    return _reopen_retained_source(source_config_bytes, expected)


def _decode_a1_resume_config(raw: bytes):
    """Decode the closed private A1 continuation bootstrap envelope."""
    try:
        envelope = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (UnicodeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("Closed A1 resume source bootstrap JSON required") from error
    required = {"profile", "a0_bootstrap_config_base64", "a0_attempt_directory",
        "expected_a1_genesis_bytes_base64", "expected_a1_request_bytes_base64",
        "a1_publication_id", "a1_destination_id"}
    _need(type(envelope) is dict and set(envelope) == required
          and envelope["profile"] == SOURCE_A1_RESUME_PROFILE
          and _canonical(envelope) == raw,
          "Closed canonical A1 resume bootstrap envelope required")
    try:
        a0_config_bytes = base64.b64decode(envelope["a0_bootstrap_config_base64"],
                                           validate=True)
    except (ValueError, TypeError) as error:
        raise ValueError("Canonical A0 bootstrap config bytes required") from error
    _need(base64.b64encode(a0_config_bytes).decode("ascii")
          == envelope["a0_bootstrap_config_base64"],
          "Canonical A0 bootstrap configuration encoding required")
    a0_envelope, source_config_bytes, expected = _decode_config(a0_config_bytes)
    # The nested A0 bootstrap envelope owns the original A0 publication and
    # destination identities. Keep them distinct from the A1 resume target.
    expected["expected_publication_id"] = a0_envelope["expected_publication_id"]
    expected["expected_destination_id"] = a0_envelope["expected_destination_id"]
    expected_a1 = {}
    for field, limit in (("expected_a1_genesis_bytes_base64", 4 * 1024 * 1024),
                         ("expected_a1_request_bytes_base64", 4 * 1024 * 1024)):
        encoded = envelope[field]
        _need(type(encoded) is str and len(encoded) <= 4 * ((limit + 2) // 3),
              "Bounded A1 resume artifact encoding required")
        try:
            value = base64.b64decode(encoded, validate=True)
        except (ValueError, TypeError) as error:
            raise ValueError("Canonical A1 resume artifact bytes required") from error
        _need(0 < len(value) <= limit and base64.b64encode(value).decode("ascii") == encoded,
              "Canonical bounded A1 resume artifact required")
        expected_a1[field.removesuffix("_base64")] = value
    _need(type(envelope["a0_attempt_directory"]) is str
          and Path(envelope["a0_attempt_directory"]).is_absolute()
          and type(envelope["a1_publication_id"]) is str
          and envelope["a1_publication_id"]
          and type(envelope["a1_destination_id"]) is str
          and envelope["a1_destination_id"],
          "Exact A0 attempt and A1 publication target required")
    return envelope, source_config_bytes, expected, a0_envelope, expected_a1


def bootstrap_from_a1_resume_private_config(config_bytes: bytes):
    """Restore a matched A0 settlement into a fresh A1 source holder.

    The durable A0 attempt is freshly re-reconciled by the holder's explicit
    recovery API. This path has no group callback and cannot replay the seed.
    """
    (envelope, source_config_bytes, expected, a0_envelope,
     expected_a1) = _decode_a1_resume_config(config_bytes)
    from ashlar_host.a1_ack_attempt import (
        A1AckSettlement, DurableA1AckAttemptStore, MAX_SETTLEMENT_RECORD_BYTES,
    )
    from ashlar_host.truss_detached_a1_genesis_codec import decode_detached_a1_genesis_snapshot
    attempt_directory = Path(envelope["a0_attempt_directory"])
    _need(attempt_directory.is_absolute() and attempt_directory.resolve(strict=True)
          == attempt_directory and not attempt_directory.is_symlink(),
          "Exact durable matched A0 attempt directory required")
    store = DurableA1AckAttemptStore(attempt_directory)
    attempt = store.load()
    record = store._read_record("worker-settlement.json", MAX_SETTLEMENT_RECORD_BYTES)
    dispatch = store._read_record("ack-command-dispatched.json")
    _need(record.get("profile") == "ashlar-a1-protected-ack-attempt/0.1"
          and record.get("attempt_id") == attempt.attempt_id
          and record.get("outcome") == "matched"
          and record.get("ack_retry_permitted") is False
          and dispatch.get("attempt_id") == attempt.attempt_id
          and dispatch.get("status") == "ack_command_may_have_been_sent"
          and dispatch.get("ack_retry_permitted") is False,
          "Exact durable matched A0 settlement and dispatch required")
    import base64
    checkpoint = base64.b64decode(record["checkpoint_bytes_base64"], validate=True)
    receipt = (None if record["receipt_bytes_base64"] is None else
               base64.b64decode(record["receipt_bytes_base64"], validate=True))
    settlement = A1AckSettlement(record["outcome"], checkpoint, receipt,
        record["request_sha256"], record["manifest_sha256"],
        record["selection_sha256"])
    store._validate_settlement(attempt, settlement)
    checkpoint_value = json.loads(checkpoint.decode("utf-8"))
    cursor = checkpoint_value.get("nextXid")
    _need(type(cursor) is str and cursor.isascii() and cursor.isdecimal()
          and str(int(cursor)) == cursor,
          "Canonical A0 settlement next cursor required")
    decode_detached_a1_genesis_snapshot(expected_a1["expected_a1_genesis_bytes"],
        expected_a_prior_cursor=cursor)
    resume = {"attempt": attempt, "settlement": settlement,
        "expected_genesis_bytes": expected_a1["expected_a1_genesis_bytes"],
        "expected_request_bytes": expected_a1["expected_a1_request_bytes"],
        "publication_id": envelope["a1_publication_id"],
        "destination_id": envelope["a1_destination_id"],
        "expected_a_prior_cursor": cursor}
    _need(attempt.selection_bytes == expected["expected_selection_bytes"]
          and attempt.request_bytes == expected["expected_request_bytes"]
          and attempt.publication_id == expected["expected_publication_id"]
          and attempt.destination_id == expected["expected_destination_id"],
          "A0 durable attempt differs from original retained source anchors")
    return _reopen_retained_source(source_config_bytes, expected, resume_a1=resume)


class RetainedA0SourceHold:
    """Parent adapter over the existing same-session source-holder worker."""

    def __init__(self, owner):
        self.owner = owner

    @property
    def phase(self):
        return self.owner.phase

    @property
    def genesis_bytes(self):
        return self.owner.genesis_bytes

    @property
    def original_genesis_bytes(self):
        return self._original_genesis_bytes

    @property
    def reselection_evidence(self):
        return self._reselection_evidence

    @property
    def snapshot_bytes(self):
        return self.owner.snapshot_bytes

    @property
    def custody_digests(self):
        return self.owner.custody_digests

    def _bounded_timeout(self, deadline, reserve=POST_ACK_CLEANUP_RESERVE_SECONDS):
        remaining = deadline - time.monotonic()
        _need(remaining >= reserve + 1,
              "Recovery source operation lacks exact cleanup reserve")
        previous = self.owner.operation_timeout_seconds
        timeout = max(1, min(previous, int(remaining - reserve)))
        self.owner.operation_timeout_seconds = timeout
        self.owner.channel.settimeout(timeout)
        return previous

    def renew(self, deadline):
        previous = self._bounded_timeout(deadline)
        try:
            return self.owner.renew()
        finally:
            if not self.owner._closed:
                self.owner.operation_timeout_seconds = previous
                self.owner.channel.settimeout(previous)

    def open_a1(self, request_bytes, publication_id, destination_id, deadline):
        previous = self._bounded_timeout(deadline)
        try:
            snapshot = self.owner.open_prepublication(request_bytes=request_bytes,
                destination_id=destination_id, publication_id=publication_id)
            if self.owner.phase == "a0":
                _need(_monotonic_watermark_match(self._expected_selection_bytes,
                      snapshot, kind="selection"),
                      "Fresh opened A0 page differs beyond monotonic safe watermark")
            self.owner.operation_timeout_seconds = min(30, previous)
            self.owner.channel.settimeout(self.owner.operation_timeout_seconds)
            return snapshot, self.owner.custody_digests
        except BaseException:
            if not self.owner._closed:
                self.owner.close_before_ack()
            raise

    def bind(self, *, manifest_bytes, vector, receipt_bytes, deadline):
        previous = self._bounded_timeout(deadline)
        try:
            self.owner.bind_publication(manifest_bytes=manifest_bytes,
                vector_payload=vector, receipt_bytes=receipt_bytes)
        finally:
            if not self.owner._closed:
                self.owner.operation_timeout_seconds = min(30, previous)
                self.owner.channel.settimeout(self.owner.operation_timeout_seconds)

    def acknowledge_once(self, store, *, attempt_directory, deadline):
        remaining = deadline - time.monotonic()
        _need(remaining >= ACK_REAP_RESERVE_SECONDS + POST_ACK_CLEANUP_RESERVE_SECONDS + 1,
              "Recovery ACK dispatch lacks bounded ACK/reap/cleanup reserves")
        previous = self.owner.operation_timeout_seconds
        timeout = max(1, min(15, int((remaining - POST_ACK_CLEANUP_RESERVE_SECONDS) / 2)))
        self.owner.operation_timeout_seconds = timeout
        self.owner.channel.settimeout(timeout)
        return self.owner.acknowledge_once(store, attempt_directory=attempt_directory)

    def finish_ack(self, settlement, *, attempt_directory, deadline):
        return self.owner.finish_ack(settlement, attempt_directory=attempt_directory,
            deadline=deadline)

    def advance_after_a0_ack(self, settlement, deadline):
        remaining = deadline - time.monotonic()
        _need(remaining >= POST_ACK_CLEANUP_RESERVE_SECONDS + 1,
              "Recovery A1 source transition lacks cleanup reserve")
        previous_timeout = self.owner.operation_timeout_seconds
        previous_channel_timeout = self.owner.channel.gettimeout()
        timeout = max(1, min(MAX_TRANSITION_SECONDS,
            int(remaining - POST_ACK_CLEANUP_RESERVE_SECONDS)))
        self.owner.operation_timeout_seconds = timeout
        self.owner.channel.settimeout(timeout)
        try:
            return self.owner.advance_after_a0_ack(settlement, deadline=deadline)
        finally:
            if not self.owner._closed:
                self.owner.operation_timeout_seconds = previous_timeout
                self.owner.channel.settimeout(previous_channel_timeout)

    def close(self, deadline, *, orderly):
        remaining = deadline - time.monotonic()
        _need(remaining >= 1, "Recovery source close deadline expired")
        result = None
        if not self.owner._closed:
            previous = self.owner.operation_timeout_seconds
            self.owner.operation_timeout_seconds = max(1, min(previous, int(remaining)))
            self.owner.channel.settimeout(self.owner.operation_timeout_seconds)
            if orderly:
                result = self.owner.close_before_ack()
            else:
                self.owner.terminate()
        _wait_worker_group_absent(self.owner.process_group_id, deadline)
        bootstrap_bytes = Path(self.worker_config_path).read_bytes()
        if _json_profile(bootstrap_bytes) == SOURCE_A1_RESUME_PROFILE:
            _, source_config_bytes, _, _, _ = _decode_a1_resume_config(bootstrap_bytes)
        else:
            _, source_config_bytes, _ = _decode_config(bootstrap_bytes)
        source_config = json.loads(source_config_bytes.decode("utf-8"),
                                   object_pairs_hook=_pairs)
        if orderly:
            if any((Path(source_config["root"]) / f"pgdata-{source}" / "postmaster.pid").exists()
                    for source in ("A", "B")):
                _cleanup_escaped_source_processes(source_config_bytes, deadline)
            for source in ("A", "B"):
                _require_existing_pgdata(Path(source_config["root"]) / f"pgdata-{source}")
        else:
            _cleanup_escaped_source_processes(source_config_bytes, deadline)
        _remove_worker_config(self.worker_config_path)
        return result


def _write_private_config(path: Path, value: dict) -> None:
    raw = _canonical(value)
    _need(len(raw) <= 4 * 1024 * 1024,
          "Recovery source worker configuration exceeds bound")
    info = path.parent.lstat()
    _need(stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode)
          and info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o700,
          "Owner-private original recovery root required for worker config")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        view = memoryview(raw)
        while view:
            count = os.write(fd, view)
            if count <= 0:
                raise OSError("Recovery source config write made no progress")
            view = view[count:]
        os.fsync(fd)
    finally:
        os.close(fd)
    directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _json_profile(raw: bytes) -> str | None:
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs)
    except (UnicodeError, ValueError, json.JSONDecodeError):
        return None
    return value.get("profile") if type(value) is dict else None


def _remove_worker_config(path: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    _require_owned_file(path, mode=0o600)
    path.unlink()
    directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _cleanup_escaped_source_processes(source_config_bytes: bytes, deadline: float) -> None:
    """After worker reap, stop only the two exact retained owner PGDATA paths."""
    from ashlar_host.managed_a1_postgres_cleanup import (
        cleanup_owned_postgres, inspect_pg_ctl,
    )
    config = json.loads(source_config_bytes.decode("utf-8"), object_pairs_hook=_pairs)
    initdb = Path(config["pg18_initdb"])
    pg_ctl_lock = inspect_pg_ctl(initdb.with_name("pg_ctl"))
    for source in ("A", "B"):
        remaining = deadline - time.monotonic()
        _need(remaining > 1, "No bounded cleanup time remains for retained sources")
        cleanup_owned_postgres(Path(config["root"]) / f"pgdata-{source}", pg_ctl_lock,
            deadline=deadline, pg_controldata=initdb.with_name("pg_controldata"))


def _wait_worker_group_absent(process_group_id: int, deadline: float) -> None:
    _need(type(process_group_id) is int and process_group_id > 1,
          "Exact owned source worker process group required")
    while time.monotonic() < deadline:
        try:
            os.killpg(process_group_id, 0)
        except ProcessLookupError:
            return
        except PermissionError as error:
            raise RuntimeError("Cannot prove source worker process group is absent") from error
        time.sleep(min(0.05, max(0.0, deadline-time.monotonic())))
    raise TimeoutError("Source worker process group remains after bounded reap")


def open_retained_source_hold(*, bootstrap_config_path: Path,
        expected_genesis_bytes: bytes, expected_selection_bytes: bytes,
        expected_request_bytes: bytes, expected_publication_id: str,
        expected_destination_id: str, deadline_monotonic: float):
    """Reopen retained A/B public sources and hold the exact pending A0 page.

    The original A0 snapshot, request, publication, destination, and full A/B
    genesis must match byte-for-byte before the returned worker begins its
    heartbeat. No registration, installation, page mutation, or ACK occurs.
    """
    from ashlar_host.a1_source_holder_worker import A1Truss053SourceHolderProcess
    from ashlar_host.truss_detached_a1_genesis_codec import decode_detached_a1_genesis_snapshot
    from ashlar_host.truss_detached_source_snapshot_codec import decode_detached_source_snapshot

    path = Path(bootstrap_config_path)
    _need(path.is_absolute() and path.resolve(strict=True) == path
          and not path.is_symlink() and path.is_file(),
          "Canonical retained source-bootstrap config path required")
    info = path.stat()
    _need(info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o600,
          "Owner-private retained source-bootstrap config required")
    _need(type(deadline_monotonic) in (int, float)
          and deadline_monotonic - time.monotonic() > POST_ACK_CLEANUP_RESERVE_SECONDS + 1,
          "Recovery source open deadline lacks cleanup reserve")
    _need(type(expected_genesis_bytes) is bytes and type(expected_selection_bytes) is bytes
          and type(expected_request_bytes) is bytes
          and type(expected_publication_id) is str and expected_publication_id
          and type(expected_destination_id) is str and expected_destination_id,
          "Exact retained source recovery bindings required")
    genesis = decode_detached_a1_genesis_snapshot(expected_genesis_bytes)
    selected = decode_detached_source_snapshot(expected_selection_bytes)
    page = selected.source_page
    _need(page.source == "A" and page.request_bytes == expected_request_bytes
          and page.publication_id == expected_publication_id
          and page.destination_id == expected_destination_id
          and page.schedule_sha256 == genesis.schedule_sha256,
          "Retained A0 held selection differs from request/publication/destination")

    source_config_bytes = path.read_bytes()
    from ashlar_host.managed_a1_source_bootstrap import validate_private_bootstrap_config
    source_config = validate_private_bootstrap_config(source_config_bytes)
    root = Path(source_config["root"])
    worker_config_path = root / ("recovery-source-holder-" + uuid.uuid4().hex + ".json")
    worker_config = {"profile": PROFILE,
        "source_bootstrap_config_path": str(path),
        "source_bootstrap_config_sha256": hashlib.sha256(source_config_bytes).hexdigest(),
        "expected_genesis_bytes_base64": _b64(expected_genesis_bytes),
        "expected_selection_bytes_base64": _b64(expected_selection_bytes),
        "expected_request_bytes_base64": _b64(expected_request_bytes),
        "expected_publication_id": expected_publication_id,
        "expected_destination_id": expected_destination_id}
    _write_private_config(worker_config_path, worker_config)
    owner = None
    try:
        remaining = deadline_monotonic - time.monotonic()
        _need(remaining > POST_ACK_CLEANUP_RESERVE_SECONDS + 1,
              "Recovery source startup deadline lacks cleanup reserve")
        operation_timeout = max(1, min(90,
            int(remaining - POST_ACK_CLEANUP_RESERVE_SECONDS)))
        owner = A1Truss053SourceHolderProcess.spawn(bootstrap=SOURCE_BOOTSTRAP,
            config_path=worker_config_path, python=sys.executable,
            operation_timeout_seconds=operation_timeout, renewal_interval_seconds=15)
        fresh_genesis = owner.wait_genesis()
        _need(_monotonic_watermark_match(expected_genesis_bytes, fresh_genesis,
              kind="genesis"),
              "Reopened public A/B genesis changed beyond monotonic safe watermarks")
        fresh_genesis_watermarks = _safe_watermarks(fresh_genesis, kind="genesis")
        fresh_selection = owner.open_prepublication(
            request_bytes=expected_request_bytes,
            destination_id=expected_destination_id,
            publication_id=expected_publication_id)
        _need(_monotonic_watermark_match(expected_selection_bytes, fresh_selection,
              kind="selection"),
              "Reopened public A0 held page differs beyond monotonic safe watermark")
        fresh_selection_watermarks = _safe_watermarks(fresh_selection, kind="selection")
        owner.operation_timeout_seconds = min(30, owner.operation_timeout_seconds)
        owner.channel.settimeout(owner.operation_timeout_seconds)
        owner.start_heartbeat()
        adapter = RetainedA0SourceHold(owner)
        adapter._expected_selection_bytes = expected_selection_bytes
        adapter._original_genesis_bytes = expected_genesis_bytes
        adapter._reselection_evidence = {
            "original_genesis_sha256": hashlib.sha256(expected_genesis_bytes).hexdigest(),
            "fresh_genesis_sha256": hashlib.sha256(fresh_genesis).hexdigest(),
            "original_selection_sha256": hashlib.sha256(expected_selection_bytes).hexdigest(),
            "fresh_selection_sha256": hashlib.sha256(fresh_selection).hexdigest(),
            "original_genesis_safe_watermarks": _safe_watermarks(
                expected_genesis_bytes, kind="genesis"),
            "fresh_genesis_safe_watermarks": fresh_genesis_watermarks,
            "original_selection_safe_watermarks": _safe_watermarks(
                expected_selection_bytes, kind="selection"),
            "fresh_selection_safe_watermarks": fresh_selection_watermarks,
            "only_monotonic_safe_watermark_drift_admitted": True,
        }
        adapter.worker_config_path = worker_config_path
        return adapter
    except BaseException as primary:
        if owner is not None and not owner._closed:
            try:
                owner.close_before_ack()
            except BaseException as close_error:
                try:
                    owner.terminate()
                    _wait_worker_group_absent(owner.process_group_id,
                        time.monotonic() + min(10, max(1, deadline_monotonic-time.monotonic())))
                    _cleanup_escaped_source_processes(source_config_bytes,
                        deadline_monotonic)
                except BaseException as terminate_error:
                    add_note = getattr(primary, "add_note", None)
                    if callable(add_note):
                        add_note("Recovery source-holder reap was incomplete: "
                                 + type(terminate_error).__name__)
                add_note = getattr(primary, "add_note", None)
                if callable(add_note):
                    add_note("Recovery source close also failed: " + type(close_error).__name__)
        if owner is None or owner._closed:
            try:
                _cleanup_escaped_source_processes(source_config_bytes,
                    deadline_monotonic)
                for source in ("A", "B"):
                    _require_existing_pgdata(root / f"pgdata-{source}")
                _remove_worker_config(worker_config_path)
            except BaseException as cleanup_error:
                add_note = getattr(primary, "add_note", None)
                if callable(add_note):
                    add_note("Private worker config cleanup failed: "
                             + type(cleanup_error).__name__)
        raise


def open_retained_a1_source_hold(*, bootstrap_config_path: Path,
        a0_genesis_bytes: bytes, a0_selection_bytes: bytes, a0_request_bytes: bytes,
        a0_publication_id: str, a1_genesis_bytes: bytes, a1_request_bytes: bytes,
        a1_publication_id: str, destination_id: str, a0_attempt_directory: Path,
        deadline_monotonic: float):
    """Open the already-committed A1 page after a durable matched A0 settlement.

    This path only reconstructs the durable A0 settlement and asks the public
    A/B owner to re-admit the pending A1 page. It has no source group callback.
    """
    from ashlar_host.a1_ack_attempt import (
        A1AckSettlement, DurableA1AckAttemptStore, MAX_SETTLEMENT_RECORD_BYTES,
    )
    from ashlar_host.a1_source_holder_worker import A1Truss053SourceHolderProcess
    from ashlar_host.truss_detached_a1_genesis_codec import decode_detached_a1_genesis_snapshot
    from ashlar_host.truss_detached_source_snapshot_codec import decode_detached_source_snapshot

    config_path = Path(bootstrap_config_path)
    _need(config_path.is_absolute() and config_path.resolve(strict=True) == config_path
          and not config_path.is_symlink() and config_path.is_file(),
          "Canonical retained A/B source configuration required")
    _require_owned_file(config_path, mode=0o600)
    _need(type(deadline_monotonic) in (int, float)
          and deadline_monotonic - time.monotonic() > POST_ACK_CLEANUP_RESERVE_SECONDS + 1,
          "Fresh A1 source holder lacks bounded cleanup time")
    attempt_directory = Path(a0_attempt_directory)
    _need(attempt_directory.is_absolute() and attempt_directory.resolve(strict=True)
          == attempt_directory and not attempt_directory.is_symlink(),
          "Exact original A0 durable ACK attempt directory required")
    store = DurableA1AckAttemptStore(attempt_directory)
    attempt = store.load()
    record = store._read_record("worker-settlement.json", MAX_SETTLEMENT_RECORD_BYTES)
    dispatch = store._read_record("ack-command-dispatched.json")
    _need(record.get("profile") == "ashlar-a1-protected-ack-attempt/0.1"
          and record.get("attempt_id") == attempt.attempt_id
          and record.get("outcome") == "matched"
          and record.get("ack_retry_permitted") is False
          and dispatch.get("profile") == "ashlar-a1-protected-ack-attempt/0.1"
          and dispatch.get("attempt_id") == attempt.attempt_id
          and dispatch.get("status") == "ack_command_may_have_been_sent"
          and dispatch.get("ack_retry_permitted") is False,
          "Durable matched A0 ACK settlement required")
    checkpoint = base64.b64decode(record["checkpoint_bytes_base64"], validate=True)
    ack_receipt = (None if record["receipt_bytes_base64"] is None else
                   base64.b64decode(record["receipt_bytes_base64"], validate=True))
    settlement = A1AckSettlement(record["outcome"], checkpoint, ack_receipt,
        record["request_sha256"], record["manifest_sha256"],
        record["selection_sha256"])
    store._validate_settlement(attempt, settlement)
    _need(attempt.selection_bytes == a0_selection_bytes
          and attempt.request_bytes == a0_request_bytes
          and attempt.publication_id == a0_publication_id
          and attempt.destination_id == destination_id
          and settlement.outcome == "matched",
          "A0 attempt/settlement differs from retained original A0 selection")
    a0_genesis, _ = _existing_genesis_and_selection({
        "expected_genesis_bytes": a0_genesis_bytes,
        "expected_selection_bytes": a0_selection_bytes,
        "expected_request_bytes": a0_request_bytes,
        "expected_publication_id": a0_publication_id,
        "expected_destination_id": destination_id})
    checkpoint_value = json.loads(checkpoint.decode("utf-8"))
    cursor = checkpoint_value.get("nextXid")
    _need(type(cursor) is str and cursor.isascii() and cursor.isdecimal()
          and str(int(cursor)) == cursor,
          "Exact matched A0 next cursor required for A1 resume")
    a1_genesis = decode_detached_a1_genesis_snapshot(a1_genesis_bytes,
        expected_a_prior_cursor=cursor)
    _need(type(a1_request_bytes) is bytes and a1_request_bytes
          and type(a1_publication_id) is str and a1_publication_id
          and type(destination_id) is str and destination_id
          and a1_genesis.schedule_sha256 == a0_genesis.schedule_sha256,
          "Exact retained A1 request and original schedule required")
    _need(type(a1_genesis_bytes) is bytes and type(a0_genesis_bytes) is bytes
          and type(a0_selection_bytes) is bytes and type(a0_request_bytes) is bytes,
          "Exact original A0 and continuation A1 byte evidence required")

    a0_config_bytes = _canonical({"profile": PROFILE,
        "source_bootstrap_config_path": str(config_path),
        "source_bootstrap_config_sha256": hashlib.sha256(config_path.read_bytes()).hexdigest(),
        "expected_genesis_bytes_base64": _b64(a0_genesis_bytes),
        "expected_selection_bytes_base64": _b64(a0_selection_bytes),
        "expected_request_bytes_base64": _b64(a0_request_bytes),
        "expected_publication_id": a0_publication_id,
        "expected_destination_id": destination_id})
    envelope = {"profile": SOURCE_A1_RESUME_PROFILE,
        "a0_bootstrap_config_base64": _b64(a0_config_bytes),
        "a0_attempt_directory": str(attempt_directory),
        "expected_a1_genesis_bytes_base64": _b64(a1_genesis_bytes),
        "expected_a1_request_bytes_base64": _b64(a1_request_bytes),
        "a1_publication_id": a1_publication_id,
        "a1_destination_id": destination_id}
    from ashlar_host.managed_a1_source_bootstrap import validate_private_bootstrap_config
    source_config = validate_private_bootstrap_config(config_path.read_bytes())
    source_root = Path(source_config["root"])
    worker_config = source_root / ("recovery-a1-source-holder-" + uuid.uuid4().hex + ".json")
    _write_private_config(worker_config, envelope)
    owner = None
    try:
        remaining = deadline_monotonic - time.monotonic()
        _need(remaining > POST_ACK_CLEANUP_RESERVE_SECONDS + 1,
              "A1 source worker startup lacks bounded cleanup time")
        timeout = max(1, min(90, int(remaining - POST_ACK_CLEANUP_RESERVE_SECONDS)))
        owner = A1Truss053SourceHolderProcess.spawn(bootstrap=SOURCE_A1_RESUME_BOOTSTRAP,
            config_path=worker_config, python=sys.executable,
            operation_timeout_seconds=timeout, renewal_interval_seconds=15)
        fresh_genesis = owner.wait_genesis(expected_phase="a1",
            expected_a_prior_cursor=cursor)
        _need(_monotonic_watermark_match(a1_genesis_bytes, fresh_genesis,
              kind="genesis", expected_a_prior_cursor=cursor),
              "Fresh public A1 genesis differs beyond safe watermark movement")
        snapshot = owner.open_prepublication(request_bytes=a1_request_bytes,
            destination_id=destination_id, publication_id=a1_publication_id)
        detached = decode_detached_source_snapshot(snapshot)
        page = detached.source_page
        _need(page.source == "A" and page.page_fields["outcome"] == "transaction"
              and page.request_bytes == a1_request_bytes
              and page.publication_id == a1_publication_id
              and page.destination_id == destination_id
              and page.prior_bytes == checkpoint,
              "Fresh public A1 page does not match the committed A0 checkpoint/request")
        owner.start_heartbeat()
        adapter = RetainedA0SourceHold(owner)
        adapter._expected_selection_bytes = None
        adapter._original_genesis_bytes = a1_genesis_bytes
        adapter._reselection_evidence = {
            "a0_attempt_id": attempt.attempt_id,
            "a0_settlement_outcome": settlement.outcome,
            "a0_checkpoint_sha256": hashlib.sha256(checkpoint).hexdigest(),
            "original_a1_genesis_sha256": hashlib.sha256(a1_genesis_bytes).hexdigest(),
            "fresh_a1_genesis_sha256": hashlib.sha256(fresh_genesis).hexdigest(),
            "fresh_a1_selection_sha256": hashlib.sha256(snapshot).hexdigest(),
            "expected_a_prior_cursor": cursor,
            "only_monotonic_safe_watermark_drift_admitted": True,
            "seed_callback_invoked": False}
        adapter.worker_config_path = worker_config
        return adapter
    except BaseException as primary:
        if owner is not None and not owner._closed:
            try:
                owner.close_before_ack()
            except BaseException as close_error:
                try:
                    owner.terminate()
                    _wait_worker_group_absent(owner.process_group_id,
                        time.monotonic() + min(10, max(1, deadline_monotonic-time.monotonic())))
                    _cleanup_escaped_source_processes(config_path.read_bytes(), deadline_monotonic)
                except BaseException as terminate_error:
                    add_note = getattr(primary, "add_note", None)
                    if callable(add_note):
                        add_note("A1 resume source worker reap incomplete: "
                                 + type(terminate_error).__name__)
                add_note = getattr(primary, "add_note", None)
                if callable(add_note):
                    add_note("A1 resume source close also failed: " + type(close_error).__name__)
        if owner is None or owner._closed:
            try:
                _cleanup_escaped_source_processes(config_path.read_bytes(), deadline_monotonic)
                for source in ("A", "B"):
                    _require_existing_pgdata(source_root / f"pgdata-{source}")
                _remove_worker_config(worker_config)
            except BaseException as cleanup_error:
                add_note = getattr(primary, "add_note", None)
                if callable(add_note):
                    add_note("A1 resume bootstrap cleanup incomplete: "
                             + type(cleanup_error).__name__)
        raise
