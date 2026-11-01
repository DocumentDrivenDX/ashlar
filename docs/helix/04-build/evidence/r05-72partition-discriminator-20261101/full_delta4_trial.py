"""In-process worker invoked only by the held R05 local process supervisor.

This module never starts Spark on import. Its public run function requires an
already admitted local Spark session, the supervisor-created private attempt
root, exact authorization, and the supervisor's child-start monotonic time.
Phase timers request cancellation; the parent supervisor owns hard termination
and post-exit cleanup.
"""
from __future__ import annotations

from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import threading
import time
from typing import Any

from full_delta4_generator import (
    EDGE_ROWS, NODE_ROWS, ROLES, SOURCE_SYSTEM, build_generation_plan,
    endpoint_oracle, validate_offline_oracle,
)

PROFILE = "ashlar-r05-full-delta4-local-trial/0.1"
HERE = Path(__file__).resolve().parent
TOTAL_SECONDS = 1800
ROOT_BYTES = 256 * 1024**3
ACTIVE_PARQUET_BYTES = 128 * 1024**3
MIN_FREE_BYTES = 512 * 1024**3
MAX_WORKERS = 18
MAX_JVM_HEAP_BYTES = 24 * 1024**3
MAX_HOST_RAM_BYTES = 128 * 1024**3
GRAPH_RESULT_CAP = 10_000
PHASE_SECONDS = {
    "admission_and_ddl": 60,
    "node_write": 360,
    "edge_write": 840,
    "concurrent_writes": 1200,
    "point_reads": 60,
    "row_value_scan": 120,
    "endpoint_scan": 120,
    "graphframes": 120,
    "validation_and_cleanup": 120,
}
OWNERSHIP = ".ashlar-r05-full-ownership.json"
RESULT = "r05-full-delta4-result.json"
PHASE_STATE = "r05-full-phase-state.json"


class TrialRefused(PermissionError):
    pass


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def selected_packet_sha256() -> str:
    """Bind approval to the reviewed proposal, oracle and executable sources."""
    names = ("README.md", "oracle.json", "full_delta4_generator.py",
             "full_delta4_trial.py", "r05_full_delta4_supervisor.py",
             "test_r05_full_delta4_supervisor.py", "test_r05_parallel_writes.py")
    h = hashlib.sha256()
    for name in names:
        raw = (HERE / name).read_bytes()
        h.update(name.encode("utf-8") + b"\0" + len(raw).to_bytes(8, "big") + raw)
    outer_packet = HERE.parent / "r05-one-shot-full-scale-run-packet-20261029.md"
    raw = outer_packet.read_bytes()
    name = "docs/helix/04-build/evidence/r05-one-shot-full-scale-run-packet-20261029.md"
    h.update(name.encode("utf-8") + b"\0" + len(raw).to_bytes(8, "big") + raw)
    return h.hexdigest()


def _read_approval(path: str | Path, expected_packet_sha256: str) -> dict:
    """Require explicit approval bound to this packet and every hard ceiling."""
    raw = Path(path).read_bytes()
    if len(raw) > 65536:
        raise TrialRefused("bounded full R05 owner authorization required")
    approval = json.loads(raw)
    if (type(approval) is not dict
            or approval.get("profile") != "ashlar-r05-full-delta4-authorization/0.1"
            or approval.get("status") != "approved"
            or approval.get("packetSha256") != expected_packet_sha256
            or approval.get("attempts") != 1
            or approval.get("totalSeconds") != TOTAL_SECONDS
            or approval.get("attemptRootBytes") != ROOT_BYTES
            or approval.get("activeParquetBytes") != ACTIVE_PARQUET_BYTES
            or approval.get("minimumFreeBytes") != MIN_FREE_BYTES
            or approval.get("phaseSeconds") != PHASE_SECONDS
            or approval.get("nodeRows") != NODE_ROWS
            or approval.get("edgeRows") != EDGE_ROWS
            or approval.get("predictiveOptimizationDisabled") is not False
            or not isinstance(approval.get("ownerDecision"), str)
            or not approval["ownerDecision"].strip()):
        raise TrialRefused("authorization is absent or does not approve this exact bounded profile")
    schedule = approval.get("writeSchedule", "sequential")
    if schedule not in ("sequential", "parallel-node-edge"):
        raise TrialRefused("approved R05 writeSchedule must be sequential or parallel-node-edge")
    return {"sha256": _sha(raw), "ownerDecision": approval["ownerDecision"],
            "writeSchedule": schedule}


def _private_attempt_root(path: str | Path) -> Path:
    supplied = Path(path)
    if not supplied.is_absolute() or supplied.is_symlink() or not supplied.exists():
        raise TrialRefused("caller-prepared absolute private attempt root required")
    parent = supplied.parent.resolve(strict=True)
    if parent != supplied.parent or not parent.is_dir():
        raise TrialRefused("canonical existing attempt parent required")
    usage = shutil.disk_usage(parent)
    fs = os.statvfs(parent)
    if usage.free < MIN_FREE_BYTES or fs.f_favail < 1000:
        raise TrialRefused("512 GiB free disk and at least 1000 free inodes required")
    info = supplied.lstat()
    if (not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode)
            or info.st_mode & 0o077 or info.st_uid != os.getuid()):
        raise TrialRefused("fresh mode-0700 attempt root required")
    return supplied


def _acquire_lock(root: Path) -> int:
    path = root / ".ashlar-r05-full.lock"
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_mode & 0o077:
        os.close(fd)
        raise TrialRefused("stable private full-trial lock required")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as error:
        os.close(fd)
        raise TrialRefused("full-trial root already in use") from error
    return fd


def _private_scratch_path(root: Path, item: Path) -> bool:
    try:
        relative = item.relative_to(root)
    except ValueError:
        return False
    return bool(relative.parts) and relative.parts[0] in {"spark-local", "tmp", "spark-events"}


def _tree_bytes(root: Path) -> int:
    total = 0
    def walk_error(error):
        if isinstance(error, FileNotFoundError) and error.filename:
            if _private_scratch_path(root, Path(error.filename)):
                return
        raise error

    for directory, dirs, files in os.walk(root, followlinks=False, onerror=walk_error):
        for name in dirs + files:
            path = Path(directory) / name
            try:
                info = path.lstat()
            except FileNotFoundError:
                if _private_scratch_path(root, path):
                    continue
                raise
            if stat.S_ISLNK(info.st_mode):
                raise TrialRefused("symlink appeared under attempt root")
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
    return total


def _parquet_bytes(root: Path) -> int:
    total = 0
    for role in ROLES:
        role_root = root / role
        if not role_root.exists():
            continue
        for directory, dirs, files in os.walk(role_root, followlinks=False):
            if any((Path(directory) / item).is_symlink() for item in dirs + files):
                raise TrialRefused("symlink appeared under Delta table")
            for name in files:
                if name.endswith(".parquet"):
                    total += (Path(directory) / name).stat().st_size
    return total


def _capacity_check(root: Path) -> None:
    if _tree_bytes(root) > ROOT_BYTES:
        raise TrialRefused("attempt root exceeded 256 GiB cap")
    if _parquet_bytes(root) > ACTIVE_PARQUET_BYTES:
        raise TrialRefused("active Parquet exceeded 128 GiB cap")


@contextmanager
def _phase(spark, root: Path, name: str, deadline: float, ledger: dict):
    if name not in PHASE_SECONDS:
        raise ValueError("unknown R05 phase")
    started = time.monotonic()
    started_ns = time.monotonic_ns()
    phase_deadline = min(deadline, started + PHASE_SECONDS[name])
    group = f"r05-full-{name}-{time.monotonic_ns()}"
    sc = spark.sparkContext
    sc.setJobGroup(group, f"Ashlar R05 {name}", interruptOnCancel=True)
    timer = threading.Timer(max(0.0, phase_deadline - started),
                            lambda: sc.cancelJobGroup(group))
    timer.daemon = True
    timer.start()
    stop_monitor = threading.Event()
    monitor_error = []

    def monitor_capacity():
        while not stop_monitor.wait(1.0):
            try:
                _capacity_check(root)
            except BaseException as error:
                monitor_error.append(error)
                sc.cancelJobGroup(group)
                return

    monitor = threading.Thread(target=monitor_capacity, daemon=True)
    monitor.start()
    try:
        if time.monotonic() >= phase_deadline:
            sc.cancelJobGroup(group)
            raise TimeoutError(f"R05 phase deadline reached before phase start: {name}")
        ledger[name] = {"startedMonotonicNs": started_ns,
                        "deadlineSeconds": PHASE_SECONDS[name]}
        _write_json(root / PHASE_STATE, {"profile": PROFILE,
                                        "activePhase": name,
                                        "phases": ledger})
        yield group
        if monitor_error:
            raise monitor_error[0]
        if time.monotonic() >= phase_deadline:
            sc.cancelJobGroup(group)
            raise TimeoutError(f"R05 phase deadline reached: {name}")
        _capacity_check(root)
    finally:
        stop_monitor.set()
        monitor.join(timeout=1)
        timer.cancel()
        _clear_spark_job_group(sc)
        elapsed = time.monotonic() - started
        ledger[name] = {"seconds": round(elapsed, 6),
                        "deadlineSeconds": PHASE_SECONDS[name],
                        "startedMonotonicNs": started_ns,
                        "endedMonotonicNs": time.monotonic_ns()}
        _write_json(root / PHASE_STATE, {"profile": PROFILE,
                                        "activePhase": None,
                                        "phases": ledger})


def _parallel_roles(roles, execute, cancel):
    """Run two independent role writes together; cancel and join peers on failure."""
    if type(roles) is not tuple or len(roles) != 2 or len(set(roles)) != 2:
        raise ValueError("Exactly two distinct Delta roles required for parallel writes")
    executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="r05-delta-write")
    futures = {}
    results = {}
    try:
        for role in roles:
            futures[executor.submit(execute, role)] = role
        for future in as_completed(futures):
            results[futures[future]] = future.result()
        return results
    except BaseException:
        try:
            cancel()
        except BaseException:
            pass
        for future in futures:
            future.cancel()
        wait(futures)
        raise
    finally:
        executor.shutdown(wait=True, cancel_futures=True)


def _write_schedule(authorization):
    schedule = authorization.get("writeSchedule", "sequential")
    if schedule not in ("sequential", "parallel-node-edge"):
        raise TrialRefused("unsupported R05 write schedule")
    return schedule


def _row_dict(row) -> dict:
    return row.asDict(recursive=True)


def _json_safe_row(row: dict) -> dict:
    return {key: (value.isoformat() + ("Z" if value.tzinfo is None else "")
                  if isinstance(value, datetime) else value)
            for key, value in row.items()}


def _table_detail(spark, path: Path) -> dict:
    from ashlar_host.pinned_delta_graphframes import _quote_path
    rows = spark.sql(f"DESCRIBE DETAIL {_quote_path(path)}").limit(2).collect()
    if len(rows) != 1:
        raise TrialRefused("one Delta detail row required")
    detail = _row_dict(rows[0])
    if detail.get("format") != "delta" or not isinstance(detail.get("id"), str):
        raise TrialRefused("exact local Delta table and UUID required")
    return detail


def _current_version(spark, path: Path) -> int:
    rows = spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1").limit(2).collect()
    if len(rows) != 1 or type(_row_dict(rows[0]).get("version")) is not int:
        raise TrialRefused("one exact current Delta version required")
    return _row_dict(rows[0])["version"]


def _validate_table(spark, root: Path, role: str, role_plan) -> dict:
    from ashlar_host.canonical_graphframes_materializer import (
        _protocol, _verify_detail_layout,
    )
    from ashlar_host.pinned_delta_graphframes import (
        CANONICAL_ACTION_LOG_PROTOCOL, _canonical_action_log_protocol_matches,
        _read_canonical_delta_log_protocol,
    )
    from ashlar_host.managed_current_xxhash64_carriers import (
        load_current_managed_xxhash64_carriers,
    )
    path = root / role
    detail = _table_detail(spark, path)
    _protocol(detail)
    _verify_detail_layout(detail, role_plan.layout)
    version = _current_version(spark, path)
    protocol, protocol_version, protocol_sha = _read_canonical_delta_log_protocol(path, version)
    if (not _canonical_action_log_protocol_matches(protocol)
            or protocol_version > version
            or set(protocol.get("writerFeatures", ())) !=
            set(CANONICAL_ACTION_LOG_PROTOCOL["writerFeatures"])):
        raise TrialRefused("exact Delta writer-7 protocol action required")
    frame = spark.read.format("delta").option("versionAsOf", version).load(str(path))
    expected_fields = load_current_managed_xxhash64_carriers().ordered_fields(role)
    observed_fields = tuple((field.name, field.dataType.simpleString().upper(), field.nullable)
                            for field in frame.schema.fields)
    if observed_fields != expected_fields:
        raise TrialRefused("pinned table schema differs from selected UMF `.4` carrier")
    from urllib.parse import unquote, urlparse
    location = urlparse(detail.get("location", ""))
    if location.scheme not in ("", "file") or location.netloc:
        raise TrialRefused("local Delta URI required")
    observed_path = Path(unquote(location.path or detail["location"])).resolve(strict=True)
    if observed_path != path.resolve(strict=True):
        raise TrialRefused("Delta location differs from exact owned path")
    expected = tuple(role_plan.layout.get("clusterBy", ()))
    if not expected or detail.get("clusteringColumns") != list(expected):
        raise TrialRefused("selected UMF clustering layout missing")
    return {"uuid": detail["id"], "version": version,
            "schemaSha256": hashlib.sha256(frame.schema.json().encode()).hexdigest(),
            "protocolSha256": protocol_sha, "protocolVersion": protocol_version,
            "protocol": protocol, "location": detail.get("location"),
            "numFiles": detail.get("numFiles"),
            "sizeInBytes": detail.get("sizeInBytes"),
            "activeParquetBytes": _parquet_bytes(root)}


def _write_json(path: Path, value: dict) -> None:
    raw = _canonical(value) + b"\n"
    if len(raw) > 8 * 1024 * 1024:
        raise TrialRefused("bounded R05 receipt exceeded 8 MiB")
    temporary = path.with_name(path.name + ".tmp")
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(temporary, flags, 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        dir_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except BaseException:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise


def _host_limits(spark, root: Path) -> dict:
    master = spark.sparkContext.master
    match = __import__("re").fullmatch(r"local\[(\d+)\]", master)
    if match is None or int(match.group(1)) > MAX_WORKERS:
        raise TrialRefused("dedicated local Spark master with at most 18 workers required")
    heap = int(spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory())
    if heap > MAX_JVM_HEAP_BYTES:
        raise TrialRefused("JVM maximum heap exceeds 24 GiB")
    physical = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    if physical < MAX_HOST_RAM_BYTES:
        raise TrialRefused("host must have at least 128 GiB physical RAM")
    local_dirs = spark.sparkContext.getConf().get("spark.local.dir", "")
    if not local_dirs:
        raise TrialRefused("Spark scratch must be configured entirely under the fresh attempt root")
    for part in local_dirs.split(","):
        try:
            Path(part).resolve(strict=True).relative_to(root.resolve(strict=True))
        except (OSError, ValueError) as error:
            raise TrialRefused("Spark scratch must be configured under the attempt root") from error
    conf = spark.conf.get("spark.databricks.delta.predictiveOptimization.enabled", "true")
    if conf.strip().lower() in {"false", "0", "no"}:
        raise TrialRefused("predictive optimization must remain enabled")
    return {"master": master, "jvmMaxHeapBytes": heap,
            "physicalMemoryBytes": physical, "predictiveOptimization": conf,
            "scratchRootCount": len(local_dirs.split(","))}


def _point_lookup(spark, frame, role: str) -> dict:
    from pyspark.sql import functions as F
    identity_col = "type_id" if role == "object_current" else "rel_type_id"
    identity_value = 1 if role == "object_current" else 7
    hash_expr = F.xxhash64(F.lit(SOURCE_SYSTEM).cast("string"),
                           F.lit(identity_value).cast("long"), F.lit(0).cast("long"))
    query = frame.where(
        (F.col("source_system") == SOURCE_SYSTEM)
        & (F.col(identity_col) == identity_value)
        & (F.col("id") == 0)
        & (F.col("lookup_hash") == hash_expr)
    )
    plan = query._jdf.queryExecution().executedPlan().toString()
    rows = query.limit(2).collect()
    if len(rows) != 1:
        raise TrialRefused(f"exact full-identity {role} point lookup failed")
    result = _row_dict(rows[0])
    if result["source_system"] != SOURCE_SYSTEM or result["id"] != 0 \
            or result[identity_col] != identity_value:
        raise TrialRefused("full point identity fields differ")
    expected = {
        "source_system": SOURCE_SYSTEM,
        identity_col: identity_value,
        "id": 0,
        "schema_revision": "r05-full-delta4-v1",
        "entity_version": 1,
        "props_json": "{}",
        "retained_json": "{}",
        "source_feed": "r05-local-synthetic",
        "source_epoch": "1",
        "source_position": 0,
        "published_at": datetime(2026, 10, 23),
        "apply_batch_id": "r05-full-delta4-v1",
        "source_cursor_json": "{}",
        "source_delivery_id": "0",
    }
    if role == "object_current":
        expected.update({"logical_key_json": "[]", "root_id": None})
    else:
        expected.update({"source_type": 1, "source_id": 0,
                         "target_type": 1, "target_id": 0, "order_key": None})
    if any(result.get(name) != value for name, value in expected.items()):
        raise TrialRefused("complete point row differs from frozen `.4` value oracle")
    return {"row": _json_safe_row(result), "plan": plan}


def _row_value_scan(spark, frames, *, node_rows=NODE_ROWS, edge_rows=EDGE_ROWS,
                    include_endpoint_formula=False) -> dict:
    from pyspark.sql import functions as F
    checks = []
    for role in ROLES:
        frame = frames[role]
        identity_col = "type_id" if role == "object_current" else "rel_type_id"
        identity_value = 1 if role == "object_current" else 7
        expected_count = node_rows if role == "object_current" else edge_rows
        bad = (
            (F.col("source_system") != SOURCE_SYSTEM)
            | (F.col(identity_col) != identity_value)
            | (F.col("schema_revision") != "r05-full-delta4-v1")
            | (F.col("entity_version") != 1)
            | (F.col("props_json") != "{}")
            | (F.col("retained_json") != "{}")
            | (F.col("source_feed") != "r05-local-synthetic")
            | (F.col("source_epoch") != "1")
            | (F.col("source_position") != F.col("id"))
            | (F.col("source_delivery_id") != F.col("id").cast("string"))
            | (F.col("apply_batch_id") != "r05-full-delta4-v1")
            | (F.col("source_cursor_json") != "{}")
            | (F.col("lookup_hash") != F.xxhash64(
                F.col("source_system").cast("string"),
                F.col(identity_col).cast("long"), F.col("id").cast("long")))
        )
        if role == "object_current":
            bad = (bad | (F.col("logical_key_json") != "[]") | F.col("root_id").isNotNull()
                   | (F.col("published_at") != F.to_timestamp(
                       F.lit("2026-10-23T00:00:00.000Z"))))
        else:
            bad = (bad | (F.col("source_type") != 1) | (F.col("target_type") != 1)
                   | F.col("order_key").isNotNull()
                   | (F.col("published_at") != F.to_timestamp(
                       F.lit("2026-10-23T00:00:00.000Z"))))
            if include_endpoint_formula:
                expected_source = F.floor(F.col("id") / F.lit(5)).cast("long")
                expected_target = F.pmod(
                    expected_source + F.floor(F.pmod(F.col("id"), F.lit(5)) / F.lit(2)),
                    F.lit(node_rows),
                ).cast("long")
                bad = (bad | (F.col("source_id") != expected_source)
                       | (F.col("target_id") != expected_target)
                       | (F.col("source_id") < 0) | (F.col("source_id") >= node_rows)
                       | (F.col("target_id") < 0) | (F.col("target_id") >= node_rows))
        bad = F.coalesce(bad, F.lit(True))
        aggregates = [
            F.count(F.lit(1)).alias("rows"),
            F.min("id").alias("min_id"), F.max("id").alias("max_id"),
            F.sum(F.col("id").cast("decimal(38,0)")).alias("sum_id"),
            F.sum("bad_row").alias("bad_rows"),
        ]
        selected_columns = [
            F.lit(role).alias("role"), F.col("id"),
            F.when(bad, 1).otherwise(0).alias("bad_row"),
        ]
        if include_endpoint_formula:
            if role == "edge_current":
                invalid_source = F.coalesce(
                    (F.col("source_type") != 1) | (F.col("source_id") != expected_source), F.lit(True))
                invalid_target = F.coalesce(
                    (F.col("target_type") != 1) | (F.col("target_id") != expected_target), F.lit(True))
                source_out = F.coalesce((F.col("source_id") < 0)
                                        | (F.col("source_id") >= node_rows), F.lit(True))
                target_out = F.coalesce((F.col("target_id") < 0)
                                        | (F.col("target_id") >= node_rows), F.lit(True))
                selected_columns.extend([
                    F.when(invalid_source, 1).otherwise(0).alias("invalid_source"),
                    F.when(invalid_target, 1).otherwise(0).alias("invalid_target"),
                    F.when(source_out, 1).otherwise(0).alias("source_out_of_range"),
                    F.when(target_out, 1).otherwise(0).alias("target_out_of_range"),
                ])
            else:
                selected_columns.extend([
                    F.lit(0).alias("invalid_source"), F.lit(0).alias("invalid_target"),
                    F.lit(0).alias("source_out_of_range"), F.lit(0).alias("target_out_of_range"),
                ])
            aggregates.extend([
                F.sum("invalid_source").alias("invalid_source"),
                F.sum("invalid_target").alias("invalid_target"),
                F.sum("source_out_of_range").alias("source_out_of_range"),
                F.sum("target_out_of_range").alias("target_out_of_range"),
            ])
        selected = frame.select(*selected_columns)
        checks.append(selected.groupBy("role").agg(*aggregates)
                      .withColumn("expected_rows", F.lit(expected_count)))
    combined = checks[0].unionByName(checks[1])
    plan = combined._jdf.queryExecution().executedPlan().toString()
    rows = combined.collect()
    if len(rows) != 2:
        raise TrialRefused("combined exact row/value witness missing role aggregate")
    result = {row["role"]: _row_dict(row) for row in rows}
    for witness in result.values():
        if witness.get("sum_id") is not None:
            witness["sum_id"] = int(witness["sum_id"])
    for role, expected in (("object_current", node_rows), ("edge_current", edge_rows)):
        witness = result[role]
        expected_sum = expected * (expected - 1) // 2
        if (witness["rows"] != expected
                or witness["min_id"] != 0 or witness["max_id"] != expected - 1
                or int(witness["sum_id"]) != expected_sum
                or witness["bad_rows"] != 0):
            raise TrialRefused(f"persisted {role} row/value/range/sum witness failed")
    if include_endpoint_formula:
        endpoint = result["edge_current"]
        if (endpoint["rows"] != edge_rows
                or any(endpoint[name] != 0 for name in (
                    "invalid_source", "invalid_target", "source_out_of_range",
                    "target_out_of_range"))):
            raise TrialRefused("persisted edge endpoints differ from independent formula")
    return {"roles": result, "plan": plan,
            "identityMultiplicityWitness": "count+range+exact-decimal-sum",
            "exactUniquenessProved": False,
            "limitation": "Linear aggregates detect many duplicate/missing substitutions but cannot prove every multiset is unique; exact distinct counting is intentionally omitted because its global shuffle is outside the two-minute scan budget."}


def _endpoint_scan(spark, edges, *, node_rows=NODE_ROWS, edge_rows=EDGE_ROWS) -> dict:
    """Run an independent full scan of the persisted edge Delta frame."""
    from pyspark.sql import functions as F
    expected_source = F.floor(F.col("id") / F.lit(5)).cast("long")
    expected_target = F.pmod(
        expected_source + F.floor(F.pmod(F.col("id"), F.lit(5)) / F.lit(2)),
        F.lit(node_rows),
    ).cast("long")
    invalid_source = F.coalesce(
        (F.col("source_type") != 1) | (F.col("source_id") != expected_source), F.lit(True))
    invalid_target = F.coalesce(
        (F.col("target_type") != 1) | (F.col("target_id") != expected_target), F.lit(True))
    source_out = F.coalesce((F.col("source_id") < 0)
                            | (F.col("source_id") >= node_rows), F.lit(True))
    target_out = F.coalesce((F.col("target_id") < 0)
                            | (F.col("target_id") >= node_rows), F.lit(True))
    query = edges.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(invalid_source, 1).otherwise(0)).alias("invalid_source"),
        F.sum(F.when(invalid_target, 1).otherwise(0)).alias("invalid_target"),
        F.sum(F.when(source_out, 1).otherwise(0)).alias("source_out_of_range"),
        F.sum(F.when(target_out, 1).otherwise(0)).alias("target_out_of_range"),
    )
    plan = query._jdf.queryExecution().executedPlan().toString()
    rows = query.collect()
    if len(rows) != 1:
        raise TrialRefused("independent endpoint scan aggregate missing")
    result = _row_dict(rows[0])
    names = ("invalid_source", "invalid_target", "source_out_of_range", "target_out_of_range")
    if (result.get("rows") != edge_rows
            or any(type(result.get(name)) is not int or result[name] != 0 for name in names)):
        raise TrialRefused("persisted edge endpoints differ from independent formula")
    return {**result, "plan": plan, "logicalScan": "endpointScan"}


def _graph_action(spark, frames) -> dict:
    from pyspark.sql import functions as F
    from graphframes import GraphFrame
    from ashlar_host.pinned_delta_graphframes import _graphframes_views
    vertices, edges = _graphframes_views(frames["object_current"], frames["edge_current"], F)
    graph = GraphFrame(vertices, edges)
    seed = F.struct(F.lit(SOURCE_SYSTEM).cast("string").alias("source_system"),
                    F.lit(1).cast("long").alias("type_id"),
                    F.lit(0).cast("long").alias("id"))
    query = (graph.find("(a)-[e]->(b)").where(F.col("a.id") == seed).select(
        F.col("a.id.source_system").alias("src_source"),
        F.col("a.id.type_id").alias("src_type"), F.col("a.id.id").alias("src_id"),
        F.col("e.edge_id.source_system").alias("edge_source"),
        F.col("e.edge_id.rel_type_id").alias("edge_type"),
        F.col("e.edge_id.id").alias("edge_id"),
        F.col("b.id.source_system").alias("dst_source"),
        F.col("b.id.type_id").alias("dst_type"), F.col("b.id.id").alias("dst_id"),
    ))
    plan = query._jdf.queryExecution().executedPlan().toString()
    rows = query.limit(GRAPH_RESULT_CAP + 1).collect()
    if len(rows) > GRAPH_RESULT_CAP:
        raise TrialRefused("GraphFrames edge occurrence result overflow")
    actual = tuple(sorted(tuple(_row_dict(row)[name] for name in (
        "src_source", "src_type", "src_id", "edge_source", "edge_type", "edge_id",
        "dst_source", "dst_type", "dst_id")) for row in rows))
    expected = tuple(sorted((SOURCE_SYSTEM, 1, 0, SOURCE_SYSTEM, 7, edge_id,
                             SOURCE_SYSTEM, 1, endpoint_oracle(edge_id)[1])
                            for edge_id in range(5)))
    if actual != expected:
        raise TrialRefused("actual GraphFrames source-qualified edge bag differs from independent oracle")
    return {"resultRows": len(actual), "result": [list(row) for row in actual],
            "cap": GRAPH_RESULT_CAP, "plan": plan,
            "input": "transient-pinned-delta-dataframes",
            "persistence": "none",
            "graphX": "not-used", "wholeFrameCollect": "not-used"}


def _clear_spark_job_group(spark_context) -> None:
    """Reset the three Spark 4.2.0 job-group local properties via public API."""
    for key in ("spark.job.description", "spark.jobGroup.id",
                "spark.job.interruptOnCancel"):
        spark_context.setLocalProperty(key, None)


def _ensure_no_overlap(phase_times: dict) -> None:
    if time.monotonic() >= phase_times["deadline"]:
        raise TimeoutError("R05 aggregate 1,800 second deadline reached")


def run_full_delta4_trial(*, spark, attempt_root: str | Path,
                          authorization_path: str | Path,
                          packet_sha256: str,
                          supervised_child_started: float) -> dict:
    """Run one fully authorized local attempt; never starts Spark itself."""
    now = time.monotonic()
    if (type(supervised_child_started) not in (float, int)
            or not 0 < supervised_child_started <= now
            or now - supervised_child_started >= TOTAL_SECONDS):
        raise TrialRefused("supervisor monotonic child-start timestamp required")
    started = float(supervised_child_started)
    deadline = started + TOTAL_SECONDS
    selected = selected_packet_sha256()
    if packet_sha256 != selected:
        raise TrialRefused("selected packet digest does not match reviewed local artifacts")
    authorization = _read_approval(authorization_path, selected)
    validate_offline_oracle()
    root = _private_attempt_root(attempt_root)
    lock_fd = _acquire_lock(root)
    ledger: dict[str, dict] = {}
    ownership = {"profile": PROFILE, "status": "running",
                 "root": [root.stat().st_dev, root.stat().st_ino], "tables": {}}
    result = {"profile": PROFILE, "status": "started", "authorization": authorization,
              "tables": {}, "phases": ledger}
    _write_json(root / OWNERSHIP, ownership)
    try:
        with _phase(spark, root, "admission_and_ddl", deadline, ledger):
            host = _host_limits(spark, root)
            configured_dirs = [Path(item).resolve(strict=True) for item in
                               spark.sparkContext.getConf().get("spark.local.dir", "").split(",")]
            if not configured_dirs:
                raise TrialRefused("Spark local scratch roots must be configured under attempt root")
            allowed_entries = set()
            for item in configured_dirs:
                try:
                    relative = item.relative_to(root.resolve(strict=True))
                except ValueError as error:
                    raise TrialRefused("Spark scratch is outside attempt root") from error
                if not relative.parts:
                    raise TrialRefused("Spark scratch must use a dedicated child directory")
                allowed_entries.add(relative.parts[0])
            allowed_entries.update({".ashlar-r05-full.lock", OWNERSHIP,
                                    PHASE_STATE, "owner-authorization.json",
                                    "runtime-pins.json",
                                    "r05-full-supervisor-admission.json",
                                    "spark-warehouse", "tmp", "spark-events",
                                    "r05-full-worker-diagnostics.log"})
            if not {item.name for item in root.iterdir()}.issubset(allowed_entries):
                raise TrialRefused("prepared attempt root contains unrecognized preexisting entries")
            result["hostAdmission"] = host
            from ashlar_host.pinned_delta_graphframes import _admit_canonical_graphframes_runtime
            runtime = _admit_canonical_graphframes_runtime(spark)
            result["runtime"] = runtime
            if spark.conf.get("spark.sql.session.timeZone") != "UTC":
                raise TrialRefused("Spark session timezone must be UTC")
            plan = build_generation_plan(str(root / ROLES[0]), str(root / ROLES[1]))
            result["generator"] = {"profile": plan.profile,
                                    "nodeRows": plan.node_rows, "edgeRows": plan.edge_rows,
                                    "endpointOracle": plan.endpoint_oracle,
                                    "bridge":"LocalUmfDelta4Bridge"}
            for role_plan in plan.roles:
                for setting in role_plan.writer_settings:
                    spark.conf.set(setting["key"], setting["value"])
                spark.sql(role_plan.ddl)
            table_plans = {item.role: item for item in plan.roles}
            for role in ROLES:
                detail = _table_detail(spark, root / role)
                ownership["tables"][role] = {
                    "uuid": detail["id"],
                    "pathIdentity": [ (root / role).stat().st_dev,
                                      (root / role).stat().st_ino ],
                }
            _write_json(root / OWNERSHIP, ownership)
            for role in ROLES:
                result["tables"][role] = _validate_table(
                    spark, root, role, table_plans[role])

        plan_by_role = {item.role: item for item in plan.roles}
        write_schedule = _write_schedule(authorization)
        if write_schedule == "sequential":
            role_durations = {}
            for role, phase_name in (("object_current", "node_write"),
                                     ("edge_current", "edge_write")):
                with _phase(spark, root, phase_name, deadline, ledger):
                    role_started = time.monotonic()
                    spark.sql(plan_by_role[role].insert_sql)
                    role_durations[role] = round(time.monotonic() - role_started, 6)
                    result["tables"][role]["postWrite"] = _validate_table(
                        spark, root, role, plan_by_role[role])
                    result["tables"][role]["postWrite"]["attemptRootBytes"] = _tree_bytes(root)
                    result["tables"][role]["postWrite"]["activeParquetBytes"] = _parquet_bytes(root)
            if ledger["node_write"]["seconds"] + ledger["edge_write"]["seconds"] > 1200:
                raise TimeoutError("combined write cap of 1,200 seconds reached")
            result["writeSchedule"] = {"mode": write_schedule,
                                        "roleSeconds": role_durations,
                                        "wallSeconds": round(
                                            ledger["node_write"]["seconds"]
                                            + ledger["edge_write"]["seconds"], 6)}
        else:
            if write_schedule != "parallel-node-edge":
                raise TrialRefused("parallel Delta write was not explicitly selected")
            group_id = None
            role_started = {}
            role_durations = {}
            with _phase(spark, root, "concurrent_writes", deadline, ledger) as group_id:
                def execute_role(role):
                    sc = spark.sparkContext
                    sc.setJobGroup(group_id, f"Ashlar R05 concurrent {role} write",
                                   interruptOnCancel=True)
                    role_started[role] = time.monotonic()
                    try:
                        spark.sql(plan_by_role[role].insert_sql)
                    finally:
                        role_durations[role] = round(time.monotonic() - role_started[role], 6)
                        _clear_spark_job_group(sc)

                _parallel_roles(
                    ROLES, execute_role,
                    lambda: spark.sparkContext.cancelJobGroup(group_id))
                for role in ROLES:
                    result["tables"][role]["postWrite"] = _validate_table(
                        spark, root, role, plan_by_role[role])
                    result["tables"][role]["postWrite"]["attemptRootBytes"] = _tree_bytes(root)
                    result["tables"][role]["postWrite"]["activeParquetBytes"] = _parquet_bytes(root)
            result["writeSchedule"] = {
                "mode": write_schedule,
                "roleSeconds": role_durations,
                "wallSeconds": ledger["concurrent_writes"]["seconds"],
                "atomicAcrossTables": False,
                "publicationVectorGate": "both-role-postWrite-verifications-required",
            }

        frames = {role: spark.read.format("delta").option(
            "versionAsOf", result["tables"][role]["postWrite"]["version"]
        ).load(str(root / role)) for role in ROLES}
        for role in ROLES:
            if _table_detail(spark, root / role)["id"] != result["tables"][role]["uuid"]:
                raise TrialRefused("Delta UUID changed after writer")

        with _phase(spark, root, "point_reads", deadline, ledger):
            point_rows = {role: _point_lookup(spark, frames[role], role) for role in ROLES}
        result["pointLookups"] = point_rows
        with _phase(spark, root, "row_value_scan", deadline, ledger):
            result["rowValueScan"] = _row_value_scan(spark, frames)
        with _phase(spark, root, "endpoint_scan", deadline, ledger):
            result["endpointScan"] = _endpoint_scan(
                spark, frames["edge_current"])
        with _phase(spark, root, "graphframes", deadline, ledger):
            from ashlar_host.pinned_delta_graphframes import _snapshot_file_inventory
            runtime_close = _admit_canonical_graphframes_runtime(spark)
            if runtime_close != result["runtime"]:
                raise TrialRefused("runtime/JAR identity changed after table creation")
            before = {role: _snapshot_file_inventory(
                frames[role], root, root / role, int(result["tables"][role]["postWrite"]["numFiles"])
            )[1] for role in ROLES}
            result["graphframes"] = _graph_action(spark, frames)
            after = {role: _snapshot_file_inventory(
                frames[role], root, root / role, int(result["tables"][role]["postWrite"]["numFiles"])
            )[1] for role in ROLES}
            if before != after:
                raise TrialRefused("pinned Delta active-file inventory changed during GraphFrames action")
            result["graphframes"]["activeFileInventorySha256"] = before

        with _phase(spark, root, "validation_and_cleanup", deadline, ledger):
            active_jobs = spark.sparkContext.statusTracker().getActiveJobIds()
            if active_jobs:
                raise TrialRefused("Spark jobs remain active; preserving owned Delta tables")
            for role in ROLES:
                if (_table_detail(spark, root / role)["id"] != result["tables"][role]["uuid"]
                        or _current_version(spark, root / role)
                        != result["tables"][role]["postWrite"]["version"]):
                    raise TrialRefused("table UUID changed before cleanup")
            result["status"] = "bounded-synthetic-checks-passed"
            result["acceptance"] = {
                "boundedOracleChecksPassed": True,
                "identityDerivedFromFrozenRangeGeneratorAndPersistedCounts": True,
                "persistedDistinctProof": False,
                "fullAcceptance": "bounded-synthetic-trial-passed",
                "supportClaim": "absent",
            }
            result["elapsedSeconds"] = round(time.monotonic() - started, 6)
            ownership["status"] = "validated-awaiting-supervisor-quiescence"
            _write_json(root / OWNERSHIP, ownership)
            result["cleanup"] = {"status": "deferred-until-supervisor-confirms-child-quiescence"}
    except BaseException as error:
        result["status"] = "failed-or-held"
        result["error"] = f"{type(error).__name__}: {error}"
        result["elapsedSeconds"] = round(time.monotonic() - started, 6)
        # Preserve failed-attempt data and receipts for diagnosis. The external
        # supervisor owns all cleanup after this child process group has exited.
        result["cleanup"] = {"status": "preserved-on-failure",
                              "ownedTableRoles": sorted(ownership["tables"])}
        raise
    finally:
        result["phases"] = ledger
        result["elapsedSeconds"] = round(time.monotonic() - started, 6)
        if result.get("elapsedSeconds", 0) > TOTAL_SECONDS:
            result["status"] = "failed-or-held"
            result["aggregateDeadline"] = "exceeded"
        try:
            _capacity_check(root)
            result["attemptRootBytesAtClose"] = _tree_bytes(root)
            result["activeParquetBytesAtClose"] = _parquet_bytes(root)
            # Keep receipts aggregate-only: local path names, inodes and
            # process/filesystem inventories are not copied into evidence.
            result["finalFilesystemSummary"] = {
                "entryCount": sum(1 for _ in root.iterdir()),
                "attemptRootBytes": result["attemptRootBytesAtClose"],
                "activeParquetBytes": result["activeParquetBytesAtClose"],
            }
            _write_json(root / RESULT, result)
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)
    if result["status"] == "bounded-synthetic-checks-passed":
        return result
    raise TrialRefused("R05 attempt did not pass")
