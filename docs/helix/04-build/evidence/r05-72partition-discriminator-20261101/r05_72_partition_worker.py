"""Held, bounded local R05 72-partition writer discriminator.

This module is inert on import. It is a worker component for a separately
reviewed supervisor and owner authorization; it does not start Spark itself.
The fixed profile uses the deterministic 45M/225M UMF-generated `.4` plan
with an explicit 72-slice range source and one fresh sequential Delta candidate.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import time
import os

from dataclasses import replace
from full_delta4_generator import ROLES, build_generation_plan
from full_delta4_trial import (
    _host_limits, _row_value_scan, _table_detail, _validate_table,
)

PROFILE = "ashlar-r05-72-partition-writer/0.1"
NODE_ROWS = 45_000_000
EDGE_ROWS = 225_000_000
TOTAL_SECONDS = 900
WORKER_SECONDS = 780
ROOT_BYTES = 32 * 1024**3
ACTIVE_DELTA_BYTES = 16 * 1024**3
MIN_FREE_BYTES = 64 * 1024**3
MODES = ("candidate-72",)
PARTITIONS = 72


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
        raise PacketRefused("closed exact 72-partition trial authorization required")
    expected = {
        "profile": "ashlar-r05-72-partition-authorization/0.1",
        "status": "approved", "packetSha256": packet_sha256,
        "attempts": 1, "nodeRows": NODE_ROWS, "edgeRows": EDGE_ROWS,
        "totalSeconds": TOTAL_SECONDS, "workerSeconds": WORKER_SECONDS,
        "rootBytes": ROOT_BYTES, "activeDeltaBytes": ACTIVE_DELTA_BYTES,
        "minimumFreeBytes": MIN_FREE_BYTES,
        "predictiveOptimizationDisabled": False,
    }
    if any(value.get(key) != expected_value for key, expected_value in expected.items()):
        raise PacketRefused("authorization differs from fixed 72-partition trial bounds")
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
             "r05_72_partition_worker.py", "full_delta4_generator.py",
             "full_delta4_trial.py", "r05_72_runtime.py",
             "r05_72_partition_supervisor.py", "test_r05_72_partition.py",
             "writer-only-runtime-receipt.json")
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


def _partitioned_plan(role_plan):
    if role_plan.role not in ("object_current", "edge_current"):
        raise PacketRefused("exact current graph role required")
    count = NODE_ROWS if role_plan.role == "object_current" else EDGE_ROWS
    source = f"FROM range({count})"
    target = f"FROM range(0, {count}, 1, {PARTITIONS})"
    if role_plan.insert_sql.count(source) != 1 or role_plan.insert_sql.count("range(") != 1:
        raise PacketRefused("one exact generated range source required")
    sql = role_plan.insert_sql.replace(source, target)
    if sql.count(target) != 1 or source in sql:
        raise PacketRefused("explicit 72-slice range rewrite failed")
    return replace(role_plan, insert_sql=sql)


def _admit_delta_runtime(spark, runtime_identity: dict) -> dict:
    """Verify exact Spark and loaded Delta classes; this profile does not load GraphFrames."""
    import pyspark
    if (spark.sparkContext.master != "local[18]" or spark.version != "4.2.0"
            or pyspark.__version__ != "4.2.0"):
        raise PacketRefused("exact local[18] Spark/PySpark 4.2.0 required")
    loader = spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
    observed = {}
    classes = (("io.delta.sql.DeltaSparkSessionExtension", "delta-spark_4.2_2.13-4.4.1.jar"),
               ("org.apache.spark.sql.delta.catalog.DeltaCatalog", "delta-spark_4.2_2.13-4.4.1.jar"),
               ("io.delta.storage.LogStore", "delta-storage-4.4.1.jar"))
    for name, jar in classes:
        klass = loader.loadClass(name)
        uri = klass.getProtectionDomain().getCodeSource().getLocation().toURI()
        path = Path(uri.getPath()).resolve(strict=True)
        expected = Path(runtime_identity["jars"][jar]).resolve(strict=True)
        if path != expected:
            raise PacketRefused("loaded Delta class does not match admitted JAR: " + name)
        observed[name] = {"jar": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    return {"spark": spark.version, "pyspark": pyspark.__version__, "delta": "4.4.1",
            "jdkBuild": runtime_identity["jdkBuild"],
            "master": spark.sparkContext.master, "loadedDeltaClasses": observed}


def _clear_spark_job_group(spark_context) -> None:
    """Clear the per-thread job-group properties through PySpark's public API.

    Spark 4.2.0 exposes Scala SparkContext.clearJobGroup, but the selected
    PySpark 4.2.0 wrapper does not. These are the three properties set by the
    Spark 4.2.0 JVM setJobGroup implementation.
    """
    for key in ("spark.job.description", "spark.jobGroup.id",
                "spark.job.interruptOnCancel"):
        spark_context.setLocalProperty(key, None)


def run_candidate(*, spark, root: Path, started: float, packet_digest: str,
                  authorization: dict, runtime_identity: dict) -> dict:
    """Run one sequential 45M/225M candidate using 72 explicit range slices."""
    if packet_digest != packet_sha256():
        raise PacketRefused("72-partition packet digest drift")
    if (not root.is_absolute() or root.is_symlink() or not root.is_dir()
            or root.stat().st_uid != os.getuid() or root.stat().st_mode & 0o077):
        raise PacketRefused("supervisor-created private attempt root required")
    if (spark.sparkContext.master != "local[18]"
            or spark.conf.get("spark.sql.session.timeZone") != "UTC"
            or spark.conf.get("spark.sql.shuffle.partitions") != str(PARTITIONS)):
        raise PacketRefused("exact local[18], UTC, and 72-partition runtime required")
    _closed_approval(authorization, packet_digest)
    runtime = _admit_delta_runtime(spark, runtime_identity)
    host = _host_limits(spark, root)
    range_slice_witness = {}
    for role, count in (("object_current", NODE_ROWS), ("edge_current", EDGE_ROWS)):
        observed = spark.range(0, count, 1, PARTITIONS).rdd.getNumPartitions()
        if observed != PARTITIONS:
            raise PacketRefused("Spark range did not expose exactly 72 source slices")
        range_slice_witness[role] = observed
    result = {"profile": PROFILE, "status": "running", "nodeRows": NODE_ROWS,
              "edgeRows": EDGE_ROWS, "partitionCount": PARTITIONS, "modeOrder": list(MODES),
              "rangeSliceWitness": range_slice_witness, "runtime": runtime, "host": host,
              "logicalInputPlans": {}, "modes": {}}
    root_st = root.stat()
    ownership = {"profile": "ashlar-r05-72-partition-ownership/0.1",
                 "root": [root_st.st_dev, root_st.st_ino], "status": "building",
                 "tables": {}}
    _write_receipt(root / "r05-72-ownership-building.json", ownership)
    select_digests = {}
    for mode in MODES:
        if time.monotonic() - started > WORKER_SECONDS:
            raise TimeoutError("72-partition worker deadline reached")
        mode_root = root / mode
        if not mode_root.is_dir():
            raise PacketRefused("supervisor-prepared mode root required")
        generation = build_generation_plan(
            str(mode_root / "object_current"), str(mode_root / "edge_current"),
            node_rows=NODE_ROWS, edge_rows=EDGE_ROWS)
        role_plans = {item.role: _partitioned_plan(item) for item in generation.roles}
        for role in ROLES:
            select_sql = _source_select(role_plans[role])
            count = NODE_ROWS if role == "object_current" else EDGE_ROWS
            if f"FROM range(0, {count}, 1, {PARTITIONS})" not in select_sql:
                raise PacketRefused("explicit 72-slice range SQL required")
            digest = hashlib.sha256(select_sql.encode("utf-8")).hexdigest()
            if role in select_digests and select_digests[role] != digest:
                raise PacketRefused("deterministic input SELECT changed between write schedules")
            select_digests[role] = digest
            result["logicalInputPlans"][role] = {
                "sha256": digest, "source": "pinned UMF `.4-xxhash64` range projection",
                "partitions": PARTITIONS,
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
        _write_receipt(root / f"r05-72-ownership-{mode}.json", ownership)
        write_started = time.monotonic()
        role_seconds = {}
        for role, phase in (("object_current", "node_write"), ("edge_current", "edge_write")):
            group_id = f"r05-full-{phase}-{PARTITIONS}"
            spark.sparkContext.setJobGroup(group_id, f"R05 72-slice {role} write",
                                           interruptOnCancel=True)
            role_started = time.monotonic()
            try:
                spark.sql(role_plans[role].insert_sql)
            finally:
                role_seconds[role] = round(time.monotonic() - role_started, 6)
                _clear_spark_job_group(spark.sparkContext)
        mode_result = {"writeWallSeconds": round(time.monotonic() - write_started, 6),
                       "roleWriteSeconds": role_seconds, "tables": {}}
        frames = {}
        for role in ROLES:
            table = _validate_table(spark, mode_root, role, role_plans[role])
            if table["uuid"] != before[role]:
                raise PacketRefused("Delta UUID changed during write")
            frames[role] = spark.read.format("delta").option("versionAsOf", table["version"]).load(str(mode_root / role))
            mode_result["tables"][role] = table
        scan_started = time.monotonic()
        scan_group = f"r05-full-row_value_scan-{PARTITIONS}"
        spark.sparkContext.setJobGroup(scan_group, "R05 72-slice persisted row/value/endpoint witness",
                                       interruptOnCancel=True)
        try:
            mode_result["rowValueWitness"] = _row_value_scan(
                spark, frames, node_rows=NODE_ROWS, edge_rows=EDGE_ROWS,
                include_endpoint_formula=True)
        finally:
            _clear_spark_job_group(spark.sparkContext)
        mode_result["rowValueWitnessSeconds"] = round(time.monotonic() - scan_started, 6)
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
        _write_receipt(root / f"r05-72-ownership-{mode}-validated.json", ownership)
        result["modes"][mode] = mode_result
    result["status"] = "single-candidate-witnesses-passed"
    _write_receipt(root / "r05-72-worker-result.json", result)
    ownership["status"] = "validated-awaiting-supervisor-quiescence"
    _write_receipt(root / "r05-72-ownership-final.json", ownership)
    return result
