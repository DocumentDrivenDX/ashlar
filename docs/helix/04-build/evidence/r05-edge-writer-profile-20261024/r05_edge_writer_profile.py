"""Held, one-attempt local R05 edge-writer profiling packet.

Import is inert. The native path requires an exact authorization, reviewed
R14 runtime pins, an existing exclusive slot lock, and the paired R05 process
supervisor's local admission checks. It writes one 225M-row edge Delta table.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
from typing import Any

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[4]
SRC = REPO / "src"
EVIDENCE = REPO / "docs/helix/04-build/evidence"
PARENT_PACKET = EVIDENCE / "r05-full-delta4-scale-packet-20261023"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(PARENT_PACKET) not in sys.path:
    sys.path.insert(0, str(PARENT_PACKET))

import r05_full_delta4_supervisor as full
import r05_paired_writer_supervisor as paired
from full_delta4_generator import EDGE_ROWS, NODE_ROWS, ROLES, build_generation_plan
from full_delta4_trial import _host_limits, _table_detail, _validate_table

PROFILE = "ashlar-r05-edge-writer-profile/0.1"
AUTH_PROFILE = "ashlar-r05-edge-writer-authorization/0.1"
NODE_ROWS = 45_000_000
EDGE_ROWS = 225_000_000
TOTAL_SECONDS = 900
WORKER_SECONDS = 780
ACTIVE_DELTA_BYTES = 16 * 1024**3
ROOT_BYTES = 32 * 1024**3
MIN_FREE_BYTES = 64 * 1024**3
MAX_EVENT_BYTES = 512 * 1024**2
MAX_DIAGNOSTICS_BYTES = 16 * 1024**2
MAX_JFR_BYTES = 64 * 1024**2
ROLE = "edge_current"
AUTH_FILE = "owner-authorization.json"
RUNTIME_FILE = "runtime-pins.json"
RESULT_FILE = "r05-edge-writer-result.json"
OWNERSHIP_FILE = "r05-edge-writer-ownership.json"
SUPERVISOR_FILE = "r05-edge-writer-supervisor-result.json"
EVENT_DIR = "spark-events"
LOG_FILE = "r05-edge-writer-diagnostics.log"
JFR_FILE = "r05-edge-writer.jfr"


class ProfileRefused(PermissionError):
    pass


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def packet_sha256() -> str:
    names = ("r05_edge_writer_profile.py", "packet-profile.json",
             "full_delta4_generator.py", "full_delta4_trial.py",
             "r05_full_delta4_supervisor.py", "r05_paired_writer_supervisor.py")
    h = hashlib.sha256()
    for name in names:
        path = HERE / name if name in names[:2] else PARENT_PACKET / name
        raw = path.read_bytes()
        h.update(name.encode() + b"\0" + len(raw).to_bytes(8, "big") + raw)
    return h.hexdigest()


def source_pins_sha256() -> str:
    h = hashlib.sha256()
    base = full.source_pins_sha256()
    h.update(base.encode("ascii"))
    for path in (HERE / "r05_edge_writer_profile.py",
                 PARENT_PACKET / "full_delta4_generator.py",
                 PARENT_PACKET / "full_delta4_trial.py",
                 PARENT_PACKET / "r05_full_delta4_supervisor.py",
                 PARENT_PACKET / "r05_paired_writer_supervisor.py",
                 PARENT_PACKET / "paired-current-runtime-receipt.json"):
        raw = path.read_bytes()
        h.update(path.name.encode() + b"\0" + len(raw).to_bytes(8, "big") + raw)
    return h.hexdigest()


def _runtime_document() -> dict:
    receipt = json.loads((PARENT_PACKET / paired.CURRENT_RUNTIME_RECEIPT).read_text())
    return {
        "profile": "ashlar-r05-reviewed-local-runtime/0.1",
        "python": receipt["python"]["entryPath"],
        "java": receipt["java"]["path"],
        "jars": {name: item["path"] for name, item in receipt["jars"].items()},
        "pythonPackagesSha256": receipt["python"]["packagesSha256"],
    }


def review_pins() -> dict:
    digest = packet_sha256()
    sources = source_pins_sha256()
    runtime = paired._paired_runtime_identity(_runtime_document())
    return {"profile": "ashlar-r05-edge-writer-review-pins/0.1",
            "packetSha256": digest, "sourcePinsSha256": sources,
            "runtimePinsSha256": full.runtime_pins_sha256(runtime),
            "runtime": runtime}


def _validate_authorization(value: Any, pins: dict) -> None:
    fields = {"profile", "status", "packetSha256", "sourcePinsSha256",
              "runtimePinsSha256", "attempts", "nodeRows", "edgeRows",
              "totalSeconds", "workerSeconds", "activeDeltaBytes", "rootBytes",
              "minimumFreeBytes", "predictiveOptimizationDisabled", "ownerDecision"}
    if type(value) is not dict or set(value) != fields:
        raise ProfileRefused("closed exact edge-profile authorization required")
    expected = {
        "profile": AUTH_PROFILE, "status": "approved",
        "packetSha256": pins["packetSha256"],
        "sourcePinsSha256": pins["sourcePinsSha256"],
        "runtimePinsSha256": pins["runtimePinsSha256"],
        "attempts": 1, "nodeRows": NODE_ROWS, "edgeRows": EDGE_ROWS,
        "totalSeconds": TOTAL_SECONDS, "workerSeconds": WORKER_SECONDS,
        "activeDeltaBytes": ACTIVE_DELTA_BYTES, "rootBytes": ROOT_BYTES,
        "minimumFreeBytes": MIN_FREE_BYTES,
        "predictiveOptimizationDisabled": False,
    }
    if any(value.get(key) != wanted for key, wanted in expected.items()):
        raise ProfileRefused("authorization differs from exact one-write bounds or pins")
    if type(value.get("ownerDecision")) is not str or not value["ownerDecision"].strip():
        raise ProfileRefused("explicit owner decision required")


def _write_private(path: Path, value: dict) -> None:
    raw = _canonical(value) + b"\n"
    if len(raw) > 8 * 1024 * 1024:
        raise ProfileRefused("bounded result receipt exceeded 8 MiB")
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def _jfr_command(java: Path, pid: int, command: tuple[str, ...], timeout: int = 15) -> dict:
    jcmd = java.parent / "jcmd"
    if not jcmd.is_file() or not os.access(jcmd, os.X_OK):
        return {"status": "unavailable", "reason": "pinned-jcmd-missing"}
    try:
        result = subprocess.run((str(jcmd), str(pid), *command),
                                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"status": "unavailable", "reason": type(error).__name__}
    output = result.stdout[:4096].decode("utf-8", "replace")
    return {"status": "ok" if result.returncode == 0 else "unavailable",
            "exitCode": result.returncode, "output": output}


def _witness(frame, node_rows: int) -> dict:
    from pyspark.sql import functions as F
    expected_source = F.floor(F.col("id") / F.lit(5)).cast("long")
    expected_target = F.pmod(
        expected_source + F.floor(F.pmod(F.col("id"), F.lit(5)) / F.lit(2)),
        F.lit(node_rows)).cast("long")
    bad = F.coalesce(
        (F.col("source_system") != "r05-full-scale-source")
        | (F.col("rel_type_id") != 7)
        | (F.col("schema_revision") != "r05-full-delta4-v1")
        | (F.col("entity_version") != 1)
        | (F.col("props_json") != "{}")
        | (F.col("retained_json") != "{}")
        | (F.col("source_type") != 1) | (F.col("target_type") != 1)
        | (F.col("source_id") != expected_source)
        | (F.col("target_id") != expected_target)
        | F.col("order_key").isNotNull()
        | (F.col("source_feed") != "r05-local-synthetic")
        | (F.col("source_epoch") != "1")
        | (F.col("source_position") != F.col("id"))
        | (F.col("published_at") != F.to_timestamp(F.lit("2026-10-23T00:00:00.000Z")))
        | (F.col("apply_batch_id") != "r05-full-delta4-v1")
        | (F.col("source_cursor_json") != "{}")
        | (F.col("source_delivery_id") != F.col("id").cast("string"))
        | (F.col("lookup_hash") != F.xxhash64(
            F.col("source_system").cast("string"), F.col("rel_type_id").cast("long"),
            F.col("id").cast("long")))
        | (F.col("source_id") < 0) | (F.col("source_id") >= node_rows)
        | (F.col("target_id") < 0) | (F.col("target_id") >= node_rows), F.lit(True))
    row = frame.agg(
        F.count(F.lit(1)).alias("rows"), F.min("id").alias("min_id"),
        F.max("id").alias("max_id"),
        F.sum(F.col("id").cast("decimal(38,0)")).alias("sum_id"),
        F.sum(F.when(bad, 1).otherwise(0)).alias("bad_rows"),
    ).first().asDict()
    if row["sum_id"] is not None:
        row["sum_id"] = str(row["sum_id"])
    return row


def run_worker(*, spark, root: Path, started: float, digest: str) -> dict:
    if packet_sha256() != digest:
        raise ProfileRefused("packet digest drift")
    if (spark.sparkContext.master != "local[18]"
            or spark.conf.get("spark.sql.session.timeZone") != "UTC"
            or spark.conf.get("spark.sql.shuffle.partitions") != "18"):
        raise ProfileRefused("exact local[18], UTC and 18-partition runtime required")
    from ashlar_host.pinned_delta_graphframes import _admit_canonical_graphframes_runtime
    runtime = _admit_canonical_graphframes_runtime(spark)
    plan = build_generation_plan(str(root / "unused-object-current"),
                                 str(root / ROLE), node_rows=NODE_ROWS,
                                 edge_rows=EDGE_ROWS)
    role_plan = next(item for item in plan.roles if item.role == ROLE)
    for setting in role_plan.writer_settings:
        spark.conf.set(setting["key"], setting["value"])
    spark.sql(role_plan.ddl)
    detail = _table_detail(spark, root / ROLE)
    table_path = root / ROLE
    st = table_path.stat()
    ownership = {"profile": "ashlar-r05-edge-writer-ownership/0.1",
                 "rootIdentity": [root.stat().st_dev, root.stat().st_ino],
                 "table": {"uuid": detail["id"], "path": str(table_path),
                           "pathIdentity": [st.st_dev, st.st_ino]},
                 "status": "created-awaiting-single-write"}
    _write_private(root / OWNERSHIP_FILE, ownership)

    jvm_pid = getattr(spark.sparkContext._gateway.proc, "pid", None)
    jfr_start = {"status": "unavailable", "reason": "gateway-pid-unavailable"}
    jfr_stop = {"status": "unavailable", "reason": "not-started"}
    if type(jvm_pid) is int and jvm_pid > 0:
        java = Path(os.environ["JAVA_HOME"]) / "bin" / "java"
        jfr_start = _jfr_command(java, jvm_pid, (
            "JFR.start", "name=R05EdgeWriter", "settings=profile",
            f"duration={WORKER_SECONDS}s", f"filename={root / JFR_FILE}", "maxsize=64m"))
    group_id = f"r05-full-edge_write-{int(started * 1000)}"
    context = spark.sparkContext
    context.setJobGroup(group_id, "R05 single edge writer profile", interruptOnCancel=True)
    write_started = time.monotonic()
    try:
        spark.sql(role_plan.insert_sql)
    finally:
        write_wall = round(time.monotonic() - write_started, 6)
        for key in ("spark.job.description", "spark.jobGroup.id", "spark.job.interruptOnCancel"):
            context.setLocalProperty(key, None)
        if type(jvm_pid) is int and jvm_pid > 0 and jfr_start.get("status") == "ok":
            java = Path(os.environ["JAVA_HOME"]) / "bin" / "java"
            jfr_stop = _jfr_command(java, jvm_pid, ("JFR.stop", "name=R05EdgeWriter"))

    validation_started = time.monotonic()
    table = _validate_table(spark, root, ROLE, role_plan)
    frame = (spark.read.format("delta").option("versionAsOf", table["version"])
             .load(str(root / ROLE)))
    witness = _witness(frame, NODE_ROWS)
    validation_wall = round(time.monotonic() - validation_started, 6)
    expected = EDGE_ROWS * (EDGE_ROWS - 1) // 2
    if (witness.get("rows") != EDGE_ROWS or witness.get("min_id") != 0
            or witness.get("max_id") != EDGE_ROWS - 1
            or int(witness.get("sum_id", -1)) != expected
            or witness.get("bad_rows") != 0):
        raise ProfileRefused("edge value witness differs from frozen generator oracle")
    ownership["status"] = "validated-awaiting-supervisor-quiescence"
    ownership["table"].update({"version": table["version"],
                               "protocolSha256": table["protocolSha256"]})
    _write_private(root / "r05-edge-writer-ownership-final.json", ownership)
    result = {
        "profile": PROFILE, "status": "edge-write-profile-passed",
        "nodeRows": NODE_ROWS, "edgeRows": EDGE_ROWS,
        "writeWallSeconds": write_wall, "roleWriteSeconds": {ROLE: write_wall},
        "validationWallSeconds": validation_wall,
        "runtime": runtime, "host": _host_limits(spark, root),
        "logicalInputPlan": {"sha256": _sha(role_plan.insert_sql.encode()),
                             "source": "pinned UMF `.4-xxhash64` range projection"},
        "table": table, "witness": witness,
        "jfr": {"start": jfr_start, "stop": jfr_stop,
                "path": JFR_FILE, "capBytes": MAX_JFR_BYTES},
        "qualification": "One local synthetic 225M-edge Delta write and compact post-write witness only.",
    }
    _write_private(root / RESULT_FILE, result)
    return result


def _verify_and_cleanup(root: Path) -> dict:
    result = json.loads((root / RESULT_FILE).read_text())
    ownership = json.loads((root / "r05-edge-writer-ownership-final.json").read_text())
    table_path = root / ROLE
    if result.get("status") != "edge-write-profile-passed" or ownership.get("status") != "validated-awaiting-supervisor-quiescence":
        raise ProfileRefused("validated one-table result required before cleanup")
    st = table_path.lstat()
    record = ownership["table"]
    from ashlar_host.pinned_delta_graphframes import _canonical_delta_uuid
    if (ownership.get("rootIdentity") != [root.stat().st_dev, root.stat().st_ino]
            or record.get("path") != str(table_path)
            or record.get("pathIdentity") != [st.st_dev, st.st_ino]
            or _canonical_delta_uuid(table_path) != record.get("uuid")
            or result["table"].get("uuid") != record.get("uuid")
            or result["table"].get("version") != record.get("version")):
        raise ProfileRefused("table ownership, UUID, or version changed; preserve root")
    shutil.rmtree(table_path)
    if table_path.exists() or table_path.is_symlink():
        raise ProfileRefused("owned Delta table cleanup could not be verified")
    for directory in ("tmp", "spark-local", "spark-warehouse"):
        path = root / directory
        if path.exists() and not path.is_symlink() and path.is_dir():
            shutil.rmtree(path)
    return {"status": "removed-owned-delta-and-scratch", "retained": [
        EVENT_DIR, JFR_FILE, LOG_FILE, RESULT_FILE, OWNERSHIP_FILE,
        "r05-edge-writer-ownership-final.json", AUTH_FILE, RUNTIME_FILE]}


def _run_child(root: Path, runtime: dict, digest: str, started: float,
               source_digest: str) -> tuple[int, bool, str | None]:
    output = root / LOG_FILE
    fd = os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    env = full._child_environment(runtime, root)
    try:
        process = subprocess.Popen(
            (runtime["python"], "-I", "-B", str(HERE / "r05_edge_writer_profile.py"),
             "--worker-child", "--root", str(root), "--packet-sha256", digest,
             "--child-started", str(started)), cwd=root, env=env,
            stdin=subprocess.DEVNULL, stdout=fd, stderr=subprocess.STDOUT,
            close_fds=True, start_new_session=True)
    finally:
        os.close(fd)
    reason, quiescent = None, False
    deadline = started + WORKER_SECONDS
    try:
        while process.poll() is None:
            if time.monotonic() >= deadline:
                reason = "worker-780-second-deadline-exceeded"
            elif full._tree_bytes(root) > ROOT_BYTES:
                reason = "attempt-root-32-GiB-cap-exceeded"
            elif full._parquet_bytes(root) > ACTIVE_DELTA_BYTES:
                reason = "active-delta-16-GiB-cap-exceeded"
            elif shutil.disk_usage(root.parent).free < MIN_FREE_BYTES:
                reason = "free-disk-64-GiB-floor-breached"
            elif output.stat().st_size > MAX_DIAGNOSTICS_BYTES:
                reason = "diagnostics-16-MiB-cap-exceeded"
            elif full._named_tree_bytes(root / EVENT_DIR, tolerate_disappearing=True) > MAX_EVENT_BYTES:
                reason = "spark-event-log-512-MiB-cap-exceeded"
            elif full._named_tree_bytes(root / JFR_FILE, tolerate_disappearing=True) > MAX_JFR_BYTES:
                reason = "jfr-64-MiB-cap-exceeded"
            elif source_pins_sha256() != source_digest:
                reason = "source-pin-drift"
            if reason:
                break
            time.sleep(1)
    finally:
        if reason and process.poll() is None:
            quiescent = full._kill_reap(process, wait_seconds=15)
        else:
            try:
                process.wait(timeout=max(1, deadline - time.monotonic()))
                quiescent = paired._wait_for_group_quiescence(process.pid)
            except subprocess.TimeoutExpired:
                reason = reason or "worker-deadline-exceeded"
                quiescent = full._kill_reap(process, wait_seconds=15)
    if not quiescent:
        reason = reason or "worker-process-group-not-quiescent"
    return process.returncode if process.returncode is not None else -signal.SIGKILL, quiescent, reason


def _bounded_auxiliary(runtime: dict, root: Path, mode: str, timeout: float) -> tuple[int, bytes, bool]:
    fd = os.open(root / ("." + mode + ".log"), os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        process = subprocess.Popen((runtime["python"], "-I", "-B",
                                     str(HERE / "r05_edge_writer_profile.py"),
                                     "--" + mode + "-child", "--root", str(root)),
                                   cwd=root, env=full._child_environment(runtime, root),
                                   stdin=subprocess.DEVNULL, stdout=fd,
                                   stderr=subprocess.STDOUT, close_fds=True,
                                   start_new_session=True)
    finally:
        os.close(fd)
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        quiescent = full._kill_reap(process, wait_seconds=10)
        return -signal.SIGKILL, b"auxiliary-child-timeout", quiescent
    quiescent = paired._wait_for_group_quiescence(process.pid)
    log_path = root / ("." + mode + ".log")
    raw = log_path.read_bytes()[:8 * 1024 * 1024]
    return process.returncode or 0, raw, quiescent


def _parse_events_child(root: Path) -> int:
    event = full._spark_event_summary(root)
    output_by_stage = {}
    directory = root / EVENT_DIR
    files = full._event_log_files(directory)
    for item in files:
        with item.open("rb") as stream:
            for raw in stream:
                record = json.loads(raw)
                if not record.get("Event", "").endswith("SparkListenerTaskEnd"):
                    continue
                stage = record.get("Stage ID")
                metrics = (record.get("Task Metrics") or {}).get("Output Metrics") or {}
                row = output_by_stage.setdefault(str(stage), {
                    "tasksWithOutputMetrics": 0, "outputBytesWritten": 0,
                    "outputRecordsWritten": 0})
                if metrics:
                    row["tasksWithOutputMetrics"] += 1
                    row["outputBytesWritten"] += int(metrics.get("Bytes Written", 0) or 0)
                    row["outputRecordsWritten"] += int(metrics.get("Records Written", 0) or 0)
    for stage in event.get("stages", []):
        stage.update(output_by_stage.get(str(stage["stageId"]), {
            "tasksWithOutputMetrics": 0, "outputBytesWritten": 0,
            "outputRecordsWritten": 0}))
    event["outputMetricsSource"] = "SparkListenerTaskEnd Task Metrics.Output Metrics when supplied by Spark 4.2"
    usable = [stage for stage in event.get("stages", [])
              if "edge_write" in stage.get("phaseNames", [])
              and stage.get("tasks", 0) > 0
              and stage.get("executorRunTimeMs", 0) > 0
              and stage.get("executorCpuTimeNs", 0) > 0]
    event["profileStatus"] = "usable" if event.get("status") == "parsed" and usable else "unusable"
    if not usable:
        event["profileIssue"] = "no attributed edge_write stage with tasks, executor runtime, and CPU metrics"
    _write_private(root / "r05-edge-writer-event-summary.json", event)
    return 0 if event["profileStatus"] == "usable" else 2


def run_supervised(*, authorization_path: Path, attempt_parent: Path,
                    native_slot_lock: Path) -> dict:
    started = time.monotonic()
    pins = review_pins()
    raw_auth = full._read_regular(authorization_path, 65536)
    auth = json.loads(raw_auth)
    _validate_authorization(auth, pins)
    lock_fd = full._lock_existing(native_slot_lock)
    try:
        if full._spark_process_count() != 0:
            raise ProfileRefused("local Spark process slot is occupied")
        root, created = paired._prepare_root(attempt_parent)
        # Paired root preparation includes mode directories; they are empty owned scratch.
        for mode in paired.MODE_DIRS:
            shutil.rmtree(root / mode)
        _write_private(root / AUTH_FILE, auth)
        _write_private(root / RUNTIME_FILE, _runtime_document())
        status, quiescent, reason = _run_child(root, pins["runtime"], pins["packetSha256"],
                                               started, pins["sourcePinsSha256"])
        event = {"status": "unavailable", "reason": "worker-failed-or-not-quiescent"}
        if reason is None and status == 0 and quiescent:
            code, parse_log, parse_quiescent = _bounded_auxiliary(
                pins["runtime"], root, "parse-events", 30)
            if code == 0 and parse_quiescent:
                event = json.loads((root / "r05-edge-writer-event-summary.json").read_text())
            else:
                reason = "bounded-event-log-parse-failed"
                event = {"status": "incomplete", "diagnostic": parse_log.decode("utf-8", "replace")[:4096]}
        jfr_path = root / JFR_FILE
        jfr = full._private_file_evidence(jfr_path, MAX_JFR_BYTES)
        elapsed_before_cleanup = time.monotonic() - started
        cleanup = {"status": "preserved-after-failure-or-uncertainty"}
        if reason is None and status == 0 and quiescent:
            remaining = TOTAL_SECONDS - (time.monotonic() - started) - 35
            if remaining <= 0:
                reason = "aggregate-900-second-hard-stop-reserve-reached"
            else:
                code, cleanup_log, cleanup_quiescent = _bounded_auxiliary(
                    pins["runtime"], root, "cleanup", min(60, remaining))
                if code == 0 and cleanup_quiescent:
                    cleanup = json.loads((root / "r05-edge-writer-cleanup-result.json").read_text())
                else:
                    reason = "bounded-owned-cleanup-failed-or-uncertain"
        elapsed_total = time.monotonic() - started
        if elapsed_total > TOTAL_SECONDS:
            reason = reason or "aggregate-900-second-hard-stop-exceeded"
        supervisor = {
            "profile": "ashlar-r05-edge-writer-supervisor/0.1",
            "status": "edge-writer-profile-passed" if reason is None and cleanup.get("status") == "removed-owned-delta-and-scratch" else "failed-or-held",
            "packetSha256": pins["packetSha256"],
            "sourcePinsSha256": pins["sourcePinsSha256"],
            "runtimePinsSha256": pins["runtimePinsSha256"],
            "childExitStatus": status, "childGroupQuiescent": quiescent,
            "elapsedBeforeCleanupSeconds": round(elapsed_before_cleanup, 6),
            "elapsedSeconds": round(elapsed_total, 6),
            "caps": {"attempts": 1, "workerSeconds": WORKER_SECONDS,
                     "totalSeconds": TOTAL_SECONDS, "activeDeltaBytes": ACTIVE_DELTA_BYTES,
                     "rootBytes": ROOT_BYTES, "minimumFreeBytes": MIN_FREE_BYTES,
                     "eventLogBytes": MAX_EVENT_BYTES, "jfrBytes": MAX_JFR_BYTES},
            "eventLog": event, "jfrFile": jfr,
            "cleanup": cleanup, "reason": reason,
        }
        _write_private(root / SUPERVISOR_FILE, supervisor)
        return supervisor
    finally:
        os.close(lock_fd)


def _child(root: Path, digest: str, started: float) -> int:
    pins = review_pins()
    if pins["packetSha256"] != digest:
        raise ProfileRefused("child packet digest mismatch")
    runtime_doc = json.loads((root / RUNTIME_FILE).read_text())
    runtime = paired._paired_runtime_identity(runtime_doc)
    if runtime["spark"] != "4.2.0" or runtime["jdkBuild"] != "21.0.13.0.2":
        raise ProfileRefused("reviewed R14 Spark 4.2/JDK 21 runtime required")
    from pyspark.sql import SparkSession
    jars = [runtime["jars"][name] for name in sorted(runtime["jars"])]
    spark = (SparkSession.builder.master("local[18]")
             .appName("Ashlar R05 one-edge writer profile")
             .config("spark.driver.memory", "24g").config("spark.executor.memory", "24g")
             .config("spark.ui.enabled", "false").config("spark.ui.bindAddress", "127.0.0.1")
             .config("spark.local.dir", str(root / "spark-local"))
             .config("spark.jars", ",".join(jars))
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.sql.warehouse.dir", str(root / "spark-warehouse"))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "18")
             .config("spark.eventLog.enabled", "true")
             .config("spark.eventLog.dir", (root / EVENT_DIR).as_uri())
             .config("spark.eventLog.compress", "false")
             .config("spark.eventLog.rolling.enabled", "true")
             .config("spark.eventLog.rolling.maxFileSize", "32m").getOrCreate())
    try:
        result = run_worker(spark=spark, root=root, started=started, digest=digest)
        return 0 if result["status"] == "edge-write-profile-passed" else 2
    finally:
        spark.stop()


def _cleanup_child(root: Path) -> int:
    result = _verify_and_cleanup(root)
    _write_private(root / "r05-edge-writer-cleanup-result.json", result)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--print-review-pins", action="store_true")
    parser.add_argument("--authorization")
    parser.add_argument("--attempt-parent")
    parser.add_argument("--native-slot-lock")
    parser.add_argument("--worker-child", action="store_true")
    parser.add_argument("--root")
    parser.add_argument("--packet-sha256")
    parser.add_argument("--child-started")
    parser.add_argument("--parse-events-child", action="store_true")
    parser.add_argument("--cleanup-child", action="store_true")
    args = parser.parse_args(argv)
    if args.print_review_pins:
        print(json.dumps(review_pins(), indent=2, sort_keys=True))
        return 0
    if args.worker_child:
        return _child(Path(args.root), args.packet_sha256, float(args.child_started))
    if args.parse_events_child:
        return _parse_events_child(Path(args.root))
    if args.cleanup_child:
        return _cleanup_child(Path(args.root))
    if not all((args.authorization, args.attempt_parent, args.native_slot_lock)):
        raise ProfileRefused("authorization, attempt parent, and pre-existing native slot lock required")
    result = run_supervised(authorization_path=Path(args.authorization),
                            attempt_parent=Path(args.attempt_parent),
                            native_slot_lock=Path(args.native_slot_lock))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "edge-writer-profile-passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
