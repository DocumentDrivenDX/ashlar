"""Held, bounded local paired R05 writer discriminator.

This module is inert on import. It is a worker component for a separately
reviewed supervisor and owner authorization; it does not start Spark itself.
The fixed profile uses the same deterministic 45M/225M UMF-generated range
SELECT/INSERT plan for fresh sequential and parallel Delta outputs.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import time
import os

from full_delta4_generator import ROLES, build_generation_plan
from full_delta4_trial import (
    _host_limits, _row_value_scan, _table_detail, _validate_table,
)

PROFILE = "ashlar-r05-paired-writer-packet/0.1"
NODE_ROWS = 45_000_000
EDGE_ROWS = 225_000_000
TOTAL_SECONDS = 900
WORKER_SECONDS = 780
ROOT_BYTES = 32 * 1024**3
ACTIVE_DELTA_BYTES = 16 * 1024**3
MIN_FREE_BYTES = 64 * 1024**3
MODES = ("sequential", "parallel-node-edge")


class PacketRefused(PermissionError):
    pass


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _closed_approval(value: object, packet_sha256: str) -> dict:
    """Validate the complete fixed trial authorization; no default approval."""
    fields = {
        "profile", "status", "packetSha256", "attempts", "nodeRows", "edgeRows",
        "totalSeconds", "workerSeconds", "rootBytes", "activeDeltaBytes",
        "minimumFreeBytes", "predictiveOptimizationDisabled", "sourcePinsSha256",
        "runtimePinsSha256", "ownerDecision",
    }
    if type(value) is not dict or set(value) != fields:
        raise PacketRefused("closed exact paired-trial authorization required")
    expected = {
        "profile": "ashlar-r05-paired-writer-authorization/0.1",
        "status": "approved", "packetSha256": packet_sha256,
        "attempts": 1, "nodeRows": NODE_ROWS, "edgeRows": EDGE_ROWS,
        "totalSeconds": TOTAL_SECONDS, "workerSeconds": WORKER_SECONDS,
        "rootBytes": ROOT_BYTES, "activeDeltaBytes": ACTIVE_DELTA_BYTES,
        "minimumFreeBytes": MIN_FREE_BYTES,
        "predictiveOptimizationDisabled": False,
    }
    if any(value.get(key) != expected_value for key, expected_value in expected.items()):
        raise PacketRefused("authorization differs from fixed paired-trial bounds")
    decision = value.get("ownerDecision")
    if type(decision) is not str or not decision.strip():
        raise PacketRefused("explicit owner decision required")
    for key in ("sourcePinsSha256", "runtimePinsSha256"):
        if type(value[key]) is not str or re.fullmatch(r"[0-9a-f]{64}", value[key]) is None:
            raise PacketRefused(f"exact {key} digest required")
    return value


def packet_sha256() -> str:
    """Hash executable packet components and the fixed profile note."""
    here = Path(__file__).resolve().parent
    names = ("README.md", "oracle.json", "offline-generation-check.json",
             "r05_paired_writer_packet.py", "full_delta4_generator.py",
             "full_delta4_trial.py", "r05_full_delta4_supervisor.py",
             "r05_paired_writer_supervisor.py",
             "test_r05_paired_writer_packet.py")
    digest = hashlib.sha256()
    for name in names:
        raw = (here / name).read_bytes()
        digest.update(name.encode() + b"\0" + len(raw).to_bytes(8, "big") + raw)
    return digest.hexdigest()


def _write_receipt(path: Path, value: dict) -> None:
    raw = _canonical(value) + b"\n"
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    dir_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _source_select(role_plan) -> str:
    match = re.fullmatch(r"INSERT INTO delta\.`[^`]+`\n(SELECT[\s\S]+)", role_plan.insert_sql)
    if not match:
        raise PacketRefused("generated insert does not match the bounded source projection")
    return match.group(1)


def run_pair(*, spark, root: Path, started: float, packet_digest: str) -> dict:
    """Run the same deterministic input plan against two fresh output pairs."""
    if packet_digest != packet_sha256():
        raise PacketRefused("paired packet digest drift")
    if (not root.is_absolute() or root.is_symlink() or not root.is_dir()
            or root.stat().st_uid != os.getuid() or root.stat().st_mode & 0o077):
        raise PacketRefused("supervisor-created private attempt root required")
    if (spark.sparkContext.master != "local[18]"
            or spark.conf.get("spark.sql.session.timeZone") != "UTC"
            or spark.conf.get("spark.sql.shuffle.partitions") != "18"):
        raise PacketRefused("exact local[18], UTC, and 18-partition runtime required")
    from ashlar_host.pinned_delta_graphframes import _admit_canonical_graphframes_runtime
    runtime = _admit_canonical_graphframes_runtime(spark)
    host = _host_limits(spark, root)
    result = {"profile": PROFILE, "status": "running", "nodeRows": NODE_ROWS,
              "edgeRows": EDGE_ROWS, "modeOrder": list(MODES),
              "runtime": runtime, "host": host, "logicalInputPlans": {}, "modes": {}}
    root_st = root.stat()
    ownership = {"profile": "ashlar-r05-paired-ownership/0.1",
                 "root": [root_st.st_dev, root_st.st_ino], "status": "building",
                 "tables": {}}
    _write_receipt(root / "r05-paired-ownership-building.json", ownership)
    select_digests = {}
    for mode in MODES:
        if time.monotonic() - started > WORKER_SECONDS:
            raise TimeoutError("paired worker deadline reached before second write mode")
        mode_root = root / mode
        if not mode_root.is_dir():
            raise PacketRefused("supervisor-prepared mode root required")
        generation = build_generation_plan(
            str(mode_root / "object_current"), str(mode_root / "edge_current"),
            node_rows=NODE_ROWS, edge_rows=EDGE_ROWS)
        role_plans = {item.role: item for item in generation.roles}
        for role in ROLES:
            select_sql = _source_select(role_plans[role])
            digest = hashlib.sha256(select_sql.encode("utf-8")).hexdigest()
            if role in select_digests and select_digests[role] != digest:
                raise PacketRefused("deterministic input SELECT changed between write schedules")
            select_digests[role] = digest
            result["logicalInputPlans"][role] = {
                "sha256": digest, "source": "pinned UMF `.4-xxhash64` range projection",
            }
        mode_started = time.monotonic()
        for role in ROLES:
            if (mode_root / role).exists() or (mode_root / role).is_symlink():
                raise PacketRefused("fresh Delta output path required")
            for setting in role_plans[role].writer_settings:
                spark.conf.set(setting["key"], setting["value"])
            spark.sql(role_plans[role].ddl)
        before = {role: _table_detail(spark, mode_root / role)["id"] for role in ROLES}
        for role in ROLES:
            path = mode_root / role
            st = path.stat()
            ownership["tables"][f"{mode}/{role}"] = {
                "uuid": before[role], "pathIdentity": [st.st_dev, st.st_ino],
                "path": str(path),
            }
        ownership["status"] = "tables-created"
        _write_receipt(root / f"r05-paired-ownership-{mode}.json", ownership)
        write_started = time.monotonic()
        role_seconds = {}
        if mode == "sequential":
            for role in ROLES:
                role_started = time.monotonic()
                spark.sql(role_plans[role].insert_sql)
                role_seconds[role] = round(time.monotonic() - role_started, 6)
        else:
            from full_delta4_trial import _parallel_roles
            group_id = f"r05-paired-{int(started * 1000)}"
            def execute(role):
                spark.sparkContext.setJobGroup(group_id, f"R05 paired {role}", interruptOnCancel=True)
                role_started = time.monotonic()
                try:
                    spark.sql(role_plans[role].insert_sql)
                finally:
                    role_seconds[role] = round(time.monotonic() - role_started, 6)
                    spark.sparkContext.clearJobGroup()
            _parallel_roles(ROLES, execute, lambda: spark.sparkContext.cancelJobGroup(group_id))
        mode_result = {"writeWallSeconds": round(time.monotonic() - write_started, 6),
                       "roleWriteSeconds": role_seconds, "tables": {}}
        frames = {}
        for role in ROLES:
            table = _validate_table(spark, mode_root, role, role_plans[role])
            if table["uuid"] != before[role]:
                raise PacketRefused("Delta UUID changed during write")
            frames[role] = spark.read.format("delta").option("versionAsOf", table["version"]).load(str(mode_root / role))
            mode_result["tables"][role] = table
        mode_result["rowValueWitness"] = _row_value_scan(
            spark, frames, node_rows=NODE_ROWS, edge_rows=EDGE_ROWS,
            include_endpoint_formula=True)
        for role in ROLES:
            table = mode_result["tables"][role]
            table["rowCount"] = mode_result["rowValueWitness"]["roles"][role]["rows"]
        mode_result["endpointWitness"] = {
            key: mode_result["rowValueWitness"]["roles"]["edge_current"][key]
            for key in ("rows", "invalid_source", "invalid_target",
                        "source_out_of_range", "target_out_of_range")}
        mode_result["modeTotalSeconds"] = round(time.monotonic() - mode_started, 6)
        for role in ROLES:
            ownership["tables"][f"{mode}/{role}"].update({
                "version": mode_result["tables"][role]["version"],
                "protocolSha256": mode_result["tables"][role]["protocolSha256"],
            })
        ownership["status"] = "mode-validated:" + mode
        _write_receipt(root / f"r05-paired-ownership-{mode}-validated.json", ownership)
        result["modes"][mode] = mode_result
    result["status"] = "paired-comparison-passed"
    _write_receipt(root / "r05-paired-worker-result.json", result)
    ownership["status"] = "validated-awaiting-supervisor-quiescence"
    _write_receipt(root / "r05-paired-ownership-final.json", ownership)
    return result
