"""Held one-pair R05 supervisor with a 15-minute aggregate hard stop.

Import is inert. Execution requires a separate exact owner authorization,
reviewed local runtime pins, a pre-existing exclusive local slot lock, and a
fresh local Spark process scan. Only receipt-owned paths are eligible for
post-child cleanup; uncertainty preserves the attempt root.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
from typing import Any

# `python -I` removes the script directory and PYTHONPATH. Establish the two
# pinned local import roots before loading packet modules that import Ashlar.
HERE = Path(__file__).resolve().parent
REPO = HERE.parents[4]
SRC = REPO / "src"
for import_root in (str(HERE), str(SRC)):
    if import_root not in sys.path:
        sys.path.insert(0, import_root)

import r05_full_delta4_supervisor as full
from r05_paired_writer_packet import (
    ACTIVE_DELTA_BYTES, EDGE_ROWS, MIN_FREE_BYTES, NODE_ROWS, ROOT_BYTES,
    TOTAL_SECONDS, WORKER_SECONDS, PacketRefused, _canonical,
    _closed_approval, packet_sha256,
)

PROFILE = "ashlar-r05-paired-writer-supervisor/0.1"
AUTH_PROFILE = "ashlar-r05-paired-writer-authorization/0.1"
MODE_DIRS = ("sequential", "parallel-node-edge")
PAIRED_JAR_NAMES = {
    "delta-spark_4.2_2.13-4.4.1.jar", "delta-storage-4.4.1.jar",
    "graphframes-graphx-spark4_2.13-0.12.3.jar",
    "graphframes-spark4_2.13-0.12.3.jar", "datasketches-java-6.2.0.jar",
    "datasketches-memory-3.0.2.jar",
}
SCRATCH_DIRS = ("tmp", "spark-local", "spark-warehouse", "spark-events")
AUTH_FILE = "owner-authorization.json"
RUNTIME_FILE = "runtime-pins.json"
WORKER_FILE = "r05-paired-worker-result.json"
OWNERSHIP_FILE = "r05-paired-ownership-final.json"
SUPERVISOR_FILE = "r05-paired-supervisor-result.json"
CLEANUP_FILE = "r05-paired-cleanup-result.json"
DIAGNOSTICS_FILE = "r05-paired-worker-diagnostics.log"
CLEANUP_DIAGNOSTICS_FILE = "r05-paired-cleanup-diagnostics.log"
MAX_AUTH_BYTES = 65536
MAX_DIAGNOSTICS_BYTES = 16 * 1024**2
CURRENT_RUNTIME_RECEIPT = "paired-current-runtime-receipt.json"


class PairedSupervisorRefused(PermissionError):
    pass


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _write_private(path: Path, raw: bytes) -> None:
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    dirfd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(dirfd)
    finally:
        os.close(dirfd)


def _read_regular(path: Path, limit: int) -> bytes:
    if not path.is_absolute() or path.is_symlink():
        raise PairedSupervisorRefused("absolute regular non-symlink file required")
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_size > limit:
            raise PairedSupervisorRefused("bounded regular file required")
        chunks, remaining = [], st.st_size
        while remaining:
            block = os.read(fd, min(1024 * 1024, remaining))
            if not block:
                raise PairedSupervisorRefused("file changed while being read")
            chunks.append(block)
            remaining -= len(block)
        if os.fstat(fd).st_size != st.st_size:
            raise PairedSupervisorRefused("file changed while being read")
        return b"".join(chunks)
    finally:
        os.close(fd)


def source_pins_sha256() -> str:
    """Bind executable packet plus pinned Ashlar runtime/source implementation."""
    h = hashlib.sha256()
    h.update(full.source_pins_sha256().encode("ascii"))
    for name in ("r05_paired_writer_packet.py", "r05_paired_writer_supervisor.py",
                 "r05_full_delta4_supervisor.py",
                 "full_delta4_generator.py", "full_delta4_trial.py",
                 CURRENT_RUNTIME_RECEIPT):
        raw = (HERE / name).read_bytes()
        h.update(name.encode() + b"\0" + len(raw).to_bytes(8, "big") + raw)
    return h.hexdigest()


def _paired_runtime_identity(document: Any, *, receipt: dict | None = None,
                             package_probe=None, python_version_probe=None,
                             java_version_probe=None) -> dict:
    """Admit the paired packet's current R14 runtime without altering full-run pins.

    The current selected Python entry is a reviewed venv symlink chain. This
    selector admits only that exact chain and binds its terminal executable.
    The historical full-scale selector remains unchanged.
    """
    if receipt is None:
        receipt = json.loads((HERE / CURRENT_RUNTIME_RECEIPT).read_text(encoding="utf-8"))
    if type(document) is not dict or set(document) != {
            "profile", "python", "java", "jars", "pythonPackagesSha256"}:
        raise PairedSupervisorRefused("closed paired runtime pin document required")
    if (receipt.get("profile") != "ashlar-r05-paired-current-runtime/0.1"
            or receipt.get("status") != "candidate-from-reviewed-r14"):
        raise PairedSupervisorRefused("reviewed paired current-runtime receipt required")
    if (receipt.get("spark") != "4.2.0" or receipt.get("delta") != "4.4.1"
            or receipt.get("graphframes") != "0.12.3"
            or receipt.get("scala") != "2.13.18"
            or receipt.get("jdkBuild") != "21.0.13.0.2"):
        raise PairedSupervisorRefused("paired runtime versions differ from reviewed cohort")
    if document.get("profile") != "ashlar-r05-reviewed-local-runtime/0.1":
        raise PairedSupervisorRefused("paired reviewed-local-runtime profile required")
    python_record = receipt.get("python", {})
    java_record = receipt.get("java", {})
    jar_records = receipt.get("jars", {})
    python = Path(document["python"])
    java = Path(document["java"])
    if str(python) != python_record.get("entryPath"):
        raise PairedSupervisorRefused("Python venv entry path differs from paired receipt")
    if str(java) != java_record.get("path"):
        raise PairedSupervisorRefused("Java executable path differs from paired receipt")
    if (type(document.get("pythonPackagesSha256")) is not str
            or document["pythonPackagesSha256"] != python_record.get("packagesSha256")):
        raise PairedSupervisorRefused("Python package closure differs from paired receipt")
    if not python.is_absolute() or not python.is_symlink():
        raise PairedSupervisorRefused("exact reviewed venv Python link entry required")
    for hop in python_record.get("linkChain", []):
        link = Path(hop["path"])
        if not link.is_symlink() or os.readlink(link) != hop["target"]:
            raise PairedSupervisorRefused("Python venv symlink chain differs from paired receipt")
    try:
        resolved_python = python.resolve(strict=True)
    except OSError as error:
        raise PairedSupervisorRefused("Python venv link chain is broken") from error
    if resolved_python != Path(python_record.get("terminalPath", "")):
        raise PairedSupervisorRefused("Python link chain resolves to another executable")
    terminal = Path(python_record["terminalPath"])
    if (terminal.is_symlink() or not terminal.is_file()
            or not os.access(terminal, os.X_OK)
            or terminal.stat().st_size != python_record.get("terminalBytes")
            or _sha(terminal.read_bytes()) != python_record.get("terminalSha256")):
        raise PairedSupervisorRefused("Python terminal executable differs from paired receipt")
    if (not java.is_absolute() or java.is_symlink() or not java.is_file()
            or not os.access(java, os.X_OK)
            or java.resolve(strict=True) != java
            or java.stat().st_size != java_record.get("bytes")
            or _sha(java.read_bytes()) != java_record.get("sha256")):
        raise PairedSupervisorRefused("Java executable differs from paired receipt")
    if (type(jar_records) is not dict or set(jar_records) != PAIRED_JAR_NAMES
            or type(document.get("jars")) is not dict
            or set(document["jars"]) != PAIRED_JAR_NAMES):
        raise PairedSupervisorRefused("exact six-JAR paired runtime closure required")
    jars = {}
    for name, record in jar_records.items():
        path = Path(document["jars"][name])
        if (str(path) != record.get("path") or not path.is_absolute()
                or path.is_symlink() or not path.is_file()
                or path.resolve(strict=True) != path
                or path.stat().st_size != record.get("bytes")
                or _sha(path.read_bytes()) != record.get("sha256")):
            raise PairedSupervisorRefused(f"JAR differs from paired runtime receipt: {name}")
        jars[name] = str(path)
    package_probe = package_probe or full._probe_python_package_closure
    observed_packages = package_probe({"python": str(python), "java": str(java)})
    if observed_packages != python_record["packagesSha256"]:
        raise PairedSupervisorRefused("selected Python package closure changed")
    if python_version_probe is None:
        python_version_probe = lambda: subprocess.run(
            (str(python), "-I", "-B", "-c", "import platform; print(platform.python_version())"),
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=15, check=True).stdout.decode("ascii").strip()
    if python_version_probe() != python_record.get("version"):
        raise PairedSupervisorRefused("selected Python version differs from paired receipt")
    if java_version_probe is None:
        def java_version_probe():
            result = subprocess.run((str(java), "-version"), stdin=subprocess.DEVNULL,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=15, check=False)
            if result.returncode != 0 or len(result.stderr) > 4096:
                raise PairedSupervisorRefused("bounded Java version probe failed")
            return result.stderr.decode("utf-8", "replace").splitlines()[0]
    if java_version_probe() != java_record.get("versionLine"):
        raise PairedSupervisorRefused("selected Java version differs from paired receipt")
    java_version = re.search(r'^openjdk version "([^"]+)"', java_record["versionLine"])
    if (java_record.get("version") != receipt["jdkBuild"]
            or java_version is None or java_version.group(1) != receipt["jdkBuild"]):
        raise PairedSupervisorRefused("Java version line and reviewed JDK build disagree")
    receipt_sha = _sha(_canonical(receipt))
    return {
        "profile": document["profile"], "python": str(python),
        "pythonEntryPath": str(python), "pythonLinkChain": python_record["linkChain"],
        "pythonTerminalPath": str(terminal),
        "pythonTerminalSha256": python_record["terminalSha256"],
        "pythonVersion": python_record["version"],
        "java": str(java), "javaSha256": java_record["sha256"],
        "javaVersionLine": java_record["versionLine"], "jars": jars,
        "jarsSha256": {name: record["sha256"] for name, record in jar_records.items()},
        "pythonPackagesSha256": observed_packages,
        "pairedRuntimeReceiptSha256": receipt_sha,
    }


def _mkdir_owned(root: Path, relative: str, created: dict[str, list[int]]) -> Path:
    path = root / relative
    path.mkdir(mode=0o700, parents=True, exist_ok=False)
    st = path.lstat()
    if not stat.S_ISDIR(st.st_mode) or stat.S_ISLNK(st.st_mode):
        raise PairedSupervisorRefused("fresh private directory required")
    created[relative] = [st.st_dev, st.st_ino]
    return path


def _prepare_root(parent: Path) -> tuple[Path, dict[str, list[int]]]:
    if (not parent.is_absolute() or parent.is_symlink() or not parent.is_dir()
            or parent.resolve(strict=True) != parent
            or parent.stat().st_uid != os.getuid()
            or any(char in str(parent) for char in ("`", "\n", "\r", "\x00"))):
        raise PairedSupervisorRefused("canonical owner-controlled attempt parent required")
    fs = os.statvfs(parent)
    if shutil.disk_usage(parent).free < MIN_FREE_BYTES or fs.f_favail < 1000:
        raise PairedSupervisorRefused("64 GiB free disk and 1000 free inodes required")
    root = parent / ("ashlar-r05-paired-" + os.urandom(12).hex())
    root.mkdir(mode=0o700)
    os.chmod(root, 0o700)
    created: dict[str, list[int]] = {}
    for name in SCRATCH_DIRS:
        _mkdir_owned(root, name, created)
    for name in MODE_DIRS:
        _mkdir_owned(root, name, created)
    return root, created


def _child_environment(runtime: dict, root: Path) -> dict[str, str]:
    return full._child_environment(runtime, root)


def _worker_argv(runtime: dict, root: Path, started: float, digest: str) -> list[str]:
    return [runtime["python"], "-I", "-B", str(HERE / "r05_paired_writer_supervisor.py"),
            "--worker-child", "--root", str(root), "--packet-sha256", digest,
            "--child-started", repr(started)]


def _active_delta_bytes(root: Path) -> int:
    total = 0
    for mode in MODE_DIRS:
        mode_root = root / mode
        if mode_root.exists():
            total += full._parquet_bytes(mode_root)
    return total


def _run_child(root: Path, runtime: dict, digest: str,
               started: float, source_digest: str) -> tuple[int, bool, str | None]:
    child_deadline = started + WORKER_SECONDS
    fd = os.open(root / DIAGNOSTICS_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        process = subprocess.Popen(
            _worker_argv(runtime, root, started, digest), cwd=root,
            env=_child_environment(runtime, root), stdin=subprocess.DEVNULL,
            stdout=fd, stderr=subprocess.STDOUT, close_fds=True, start_new_session=True)
    finally:
        os.close(fd)
    reason = None
    next_source_check = started
    try:
        while process.poll() is None:
            now = time.monotonic()
            try:
                if now >= child_deadline:
                    reason = "worker-780-second-deadline-exceeded"
                elif full._tree_bytes(root) > ROOT_BYTES:
                    reason = "attempt-root-32-GiB-cap-exceeded"
                elif _active_delta_bytes(root) > ACTIVE_DELTA_BYTES:
                    reason = "active-delta-16-GiB-cap-exceeded"
                elif shutil.disk_usage(root.parent).free < MIN_FREE_BYTES:
                    reason = "free-disk-64-GiB-floor-breached"
                elif (root / DIAGNOSTICS_FILE).stat().st_size > MAX_DIAGNOSTICS_BYTES:
                    reason = "worker-diagnostics-16-MiB-cap-exceeded"
                elif now >= next_source_check:
                    if source_pins_sha256() != source_digest:
                        reason = "source-pin-drift"
                    next_source_check = now + 5.0
            except BaseException as error:
                reason = "surveillance-failed:" + type(error).__name__
            if reason:
                break
            time.sleep(1.0)
    finally:
        if reason and process.poll() is None:
            quiescent = full._kill_reap(process, wait_seconds=15)
        else:
            try:
                process.wait(timeout=max(1.0, child_deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                reason = reason or "worker-deadline-exceeded"
                quiescent = full._kill_reap(process, wait_seconds=15)
            else:
                quiescent = not full._group_exists(process.pid)
    if not quiescent:
        reason = reason or "worker-process-group-not-quiescent"
    return process.returncode if process.returncode is not None else -signal.SIGKILL, quiescent, reason


def _read_json(path: Path, limit: int) -> Any:
    raw = _read_regular(path, limit)
    return json.loads(raw)


def _verify_and_cleanup(root: Path, created: dict[str, list[int]]) -> dict:
    worker = _read_json(root / WORKER_FILE, 4 * 1024 * 1024)
    ownership = _read_json(root / OWNERSHIP_FILE, 1024 * 1024)
    if worker.get("status") != "paired-comparison-passed":
        raise PairedSupervisorRefused("paired semantic checks did not pass")
    if set(worker.get("modes", {})) != set(MODE_DIRS):
        raise PairedSupervisorRefused("both ordered write schedules required")
    if worker.get("modeOrder") != list(MODE_DIRS):
        raise PairedSupervisorRefused("sequential-then-parallel order receipt required")
    runtime = worker.get("runtime", {})
    if (runtime.get("spark") != "4.2.0" or runtime.get("delta") != "4.4.1"
            or runtime.get("graphframes") != "0.12.3"
            or runtime.get("jdkBuild") != "21.0.13.0.2"):
        raise PairedSupervisorRefused("actual loaded Spark/Delta/JDK runtime receipt required")
    host = worker.get("host", {})
    if (host.get("master") != "local[18]"
            or type(host.get("jvmMaxHeapBytes")) is not int
            or host["jvmMaxHeapBytes"] > 24 * 1024**3
            or type(host.get("physicalMemoryBytes")) is not int
            or host["physicalMemoryBytes"] < 128 * 1024**3
            or host.get("predictiveOptimization", "true").strip().lower()
            in {"false", "0", "no"}):
        raise PairedSupervisorRefused("fresh local host/cost-control receipt required")
    expected_counts = {"object_current": NODE_ROWS, "edge_current": EDGE_ROWS}
    if set(worker.get("logicalInputPlans", {})) != set(expected_counts):
        raise PairedSupervisorRefused("two deterministic input plan digests required")
    for digest in worker["logicalInputPlans"].values():
        if (digest.get("source") != "pinned UMF `.4-xxhash64` range projection"
                or type(digest.get("sha256")) is not str
                or re.fullmatch(r"[0-9a-f]{64}", digest["sha256"]) is None):
            raise PairedSupervisorRefused("pinned source plan digest required")
    for mode in MODE_DIRS:
        mode_result = worker["modes"][mode]
        if set(mode_result.get("tables", {})) != set(expected_counts):
            raise PairedSupervisorRefused("both output table receipts required per mode")
        for field in ("writeWallSeconds", "modeTotalSeconds"):
            value = mode_result.get(field)
            if type(value) not in (int, float) or value < 0:
                raise PairedSupervisorRefused("finite nonnegative mode duration required")
        if set(mode_result.get("roleWriteSeconds", {})) != set(expected_counts):
            raise PairedSupervisorRefused("per-role write timing required")
        if any(type(seconds) not in (int, float) or seconds < 0
               for seconds in mode_result["roleWriteSeconds"].values()):
            raise PairedSupervisorRefused("finite nonnegative per-role timings required")
        for role, expected in expected_counts.items():
            table = mode_result["tables"][role]
            if (table.get("rowCount") != expected
                    or type(table.get("version")) is not int
                    or not table.get("uuid") or not table.get("protocolSha256")):
                raise PairedSupervisorRefused("validated table row/version receipt mismatch")
        row_scan = mode_result.get("rowValueWitness", {})
        if (set(row_scan.get("roles", {})) != set(expected_counts)
                or row_scan.get("exactUniquenessProved") is not False):
            raise PairedSupervisorRefused("both exact row/value witnesses required")
        for role, count in expected_counts.items():
            witness = row_scan["roles"][role]
            if (witness.get("rows") != count or witness.get("bad_rows") != 0
                    or witness.get("min_id") != 0 or witness.get("max_id") != count - 1
                    or int(witness.get("sum_id", -1)) != count * (count - 1) // 2):
                raise PairedSupervisorRefused("row/value witness differs from frozen oracle")
        endpoint = mode_result.get("endpointWitness", {})
        if (endpoint.get("rows") != EDGE_ROWS
                or any(endpoint.get(name) != 0 for name in (
                    "invalid_source", "invalid_target", "source_out_of_range",
                    "target_out_of_range"))):
            raise PairedSupervisorRefused("endpoint witness differs from frozen oracle")
    if (ownership.get("status") != "validated-awaiting-supervisor-quiescence"
            or ownership.get("root") != [root.stat().st_dev, root.stat().st_ino]
            or set(ownership.get("tables", {})) != {
                f"{mode}/{role}" for mode in MODE_DIRS
                for role in ("object_current", "edge_current")}):
        raise PairedSupervisorRefused("complete exact pair ownership receipt required")
    from ashlar_host.pinned_delta_graphframes import (
        CANONICAL_ACTION_LOG_PROTOCOL, _canonical_action_log_protocol_matches,
        _canonical_delta_uuid, _read_canonical_delta_log_protocol,
    )
    delta_paths = []
    for mode in MODE_DIRS:
        for role in ("object_current", "edge_current"):
            key = f"{mode}/{role}"
            table = root / mode / role
            record = ownership["tables"][key]
            table_result = worker["modes"][mode]["tables"][role]
            st = table.lstat()
            if (record.get("path") != str(table)
                    or record.get("pathIdentity") != [st.st_dev, st.st_ino]
                    or _canonical_delta_uuid(table) != record.get("uuid")
                    or table_result.get("uuid") != record.get("uuid")
                    or table_result.get("version") != record.get("version")
                    or table_result.get("protocolSha256") != record.get("protocolSha256")):
                raise PairedSupervisorRefused("Delta path identity or UUID differs from child receipt")
            version = record.get("version")
            logs = [int(p.name[:20]) for p in (table / "_delta_log").iterdir()
                    if re.fullmatch(r"[0-9]{20}\.json", p.name)]
            if type(version) is not int or not logs or max(logs) != version:
                raise PairedSupervisorRefused("Delta head changed after child exit")
            protocol, protocol_version, protocol_sha = _read_canonical_delta_log_protocol(table, version)
            if (protocol_sha != record.get("protocolSha256")
                    or protocol_version > version
                    or not _canonical_action_log_protocol_matches(protocol)
                    or set(protocol.get("writerFeatures", ())) !=
                    set(CANONICAL_ACTION_LOG_PROTOCOL["writerFeatures"])):
                raise PairedSupervisorRefused("Delta writer protocol differs from ownership receipt")
            full._validate_removable_tree(table)
            delta_paths.append(table)

    # Validate every private scratch directory before any deletion.
    remove_paths = list(delta_paths)
    for relative, identity in created.items():
        path = root / relative
        if path.is_symlink() or not path.is_dir():
            raise PairedSupervisorRefused("supervisor-owned directory missing or replaced")
        st = path.lstat()
        if [st.st_dev, st.st_ino] != identity:
            raise PairedSupervisorRefused("supervisor-owned directory identity changed")
        full._validate_removable_tree(path)
        remove_paths.append(path)
    # Delta tables first, then the supervisor-owned mode and scratch directories.
    for path in remove_paths:
        full._safe_remove_tree(path)
    for path in remove_paths:
        if path.exists() or path.is_symlink():
            raise PairedSupervisorRefused("owned path remained after cleanup")
    return {"status": "removed-after-child-group-quiescence",
            "removedDeltaTables": len(delta_paths),
            "removedSupervisorDirectories": len(created),
            "ownershipReceiptPreserved": True}


def _run_cleanup_child(root: Path, runtime: dict, deadline: float) -> tuple[dict, bool, str | None]:
    remaining = deadline - time.monotonic()
    if remaining <= 1:
        return {}, True, "aggregate-deadline-left-no-cleanup-window"
    fd = os.open(root / CLEANUP_DIAGNOSTICS_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    argv = [runtime["python"], "-I", "-B", str(HERE / "r05_paired_writer_supervisor.py"),
            "--cleanup-child", "--root", str(root)]
    env = {"PATH": str(Path(runtime["java"]).parent) + ":/usr/bin:/bin",
           "JAVA_HOME": str(Path(runtime["java"]).parent.parent),
           "PYTHONDONTWRITEBYTECODE": "1", "PYTHONUTF8": "1", "TMPDIR": "/private/tmp"}
    try:
        process = subprocess.Popen(argv, cwd=root, env=env, stdin=subprocess.DEVNULL,
                                   stdout=fd, stderr=subprocess.STDOUT, close_fds=True,
                                   start_new_session=True)
    finally:
        os.close(fd)
    while process.poll() is None and time.monotonic() < deadline - 1:
        time.sleep(0.1)
    if process.poll() is None:
        quiescent = full._kill_reap(process, wait_seconds=9)
        return {}, quiescent, "cleanup-child-deadline-exceeded"
    if not full._group_exists(process.pid):
        quiescent = True
    else:
        quiescent = full._kill_reap(process, wait_seconds=10)
    if process.returncode != 0 or not quiescent:
        return {}, quiescent, "cleanup-child-failed-or-not-quiescent"
    try:
        result = _read_json(root / CLEANUP_FILE, 65536)
    except BaseException:
        return {}, quiescent, "cleanup-result-missing-or-invalid"
    if result.get("status") != "removed-after-child-group-quiescence":
        return result, quiescent, "cleanup-result-not-successful"
    return result, quiescent, None


def run_supervised_attempt(*, authorization_path: Path, runtime_pins_path: Path,
                           attempt_parent: Path, native_slot_lock: Path) -> dict:
    digest = packet_sha256()
    source_digest = source_pins_sha256()
    auth_raw = _read_regular(authorization_path, MAX_AUTH_BYTES)
    auth_doc = json.loads(auth_raw)
    _closed_approval(auth_doc, digest)
    runtime_doc = json.loads(_read_regular(runtime_pins_path, 65536))
    runtime = _paired_runtime_identity(runtime_doc)
    if auth_doc.get("sourcePinsSha256") != source_digest:
        raise PairedSupervisorRefused("authorization source pins differ")
    runtime_digest = full.runtime_pins_sha256(runtime)
    if auth_doc.get("runtimePinsSha256") != runtime_digest:
        raise PairedSupervisorRefused("authorization runtime pins differ")
    lock_fd = full._lock_existing(native_slot_lock)
    lock_st = os.fstat(lock_fd)
    started = time.monotonic()
    try:
        if full._spark_process_count() != 0:
            raise PairedSupervisorRefused("local Spark process slot is occupied")
        if packet_sha256() != digest or source_pins_sha256() != source_digest:
            raise PairedSupervisorRefused("packet/source changed before launch")
        root, created = _prepare_root(attempt_parent)
        root_st = root.stat()
        _write_private(root / AUTH_FILE, auth_raw)
        runtime_private = dict(runtime)
        runtime_private["sourcePinsSha256"] = source_digest
        runtime_private["runtimePinsSha256"] = runtime_digest
        _write_private(root / RUNTIME_FILE, _canonical(runtime_private))
        _write_private(root / "r05-paired-admission.json", _canonical({
            "profile": PROFILE, "rootIdentity": [root_st.st_dev, root_st.st_ino],
            "createdDirectories": created, "packetSha256": digest,
            "sourcePinsSha256": source_digest, "runtimePinsSha256": runtime_digest,
        }))
        status, quiescent, reason = _run_child(root, runtime, digest, started, source_digest)
        elapsed = time.monotonic() - started
        result = {"profile": PROFILE, "packetSha256": digest,
                  "sourcePinsSha256": source_digest, "runtimePinsSha256": runtime_digest,
                  "childExitStatus": status, "childGroupQuiescent": quiescent,
                  "elapsedBeforeCleanupSeconds": round(elapsed, 6),
                  "totalSecondsCap": TOTAL_SECONDS, "workerSecondsCap": WORKER_SECONDS,
                  "attemptRootBytesCap": ROOT_BYTES,
                  "activeDeltaBytesCap": ACTIVE_DELTA_BYTES,
                  "cleanup": {"status": "preserved-after-failure-or-uncertainty"},
                  "reason": reason}
        if (reason is None and status == 0 and quiescent
                and elapsed <= WORKER_SECONDS + 15):
            try:
                cleanup, cleanup_quiescent, cleanup_reason = _run_cleanup_child(
                    root, runtime, started + TOTAL_SECONDS - 40)
                result["cleanupChildGroupQuiescent"] = cleanup_quiescent
                result["cleanup"] = cleanup or {
                    "status": "preserved-after-failed-or-uncertain-cleanup"}
                result["reason"] = cleanup_reason
            except BaseException as error:
                result["cleanup"] = {"status": "preserved-after-failed-or-uncertain-cleanup"}
                result["reason"] = "cleanup-launch-failed:" + type(error).__name__
        result["elapsedSeconds"] = round(time.monotonic() - started, 6)
        result["status"] = (
            "bounded-paired-comparison-passed"
            if result["reason"] is None
            and result["cleanup"].get("status") == "removed-after-child-group-quiescence"
            and result["elapsedSeconds"] <= TOTAL_SECONDS else "failed-or-held")
        if time.monotonic() - started > TOTAL_SECONDS:
            result["status"] = "failed-or-held"
            result["reason"] = "aggregate-900-second-hard-stop-exceeded"
        _write_private(root / SUPERVISOR_FILE, _canonical(result))
        current_lock = os.stat(native_slot_lock, follow_symlinks=False)
        if (lock_st.st_dev, lock_st.st_ino) != (current_lock.st_dev, current_lock.st_ino):
            raise PairedSupervisorRefused("native slot lock identity changed")
        return result
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)


def _worker_child(root: Path, digest: str, child_started: float) -> int:
    if packet_sha256() != digest:
        raise PairedSupervisorRefused("paired packet digest mismatch")
    runtime = json.loads(_read_regular(root / RUNTIME_FILE, 65536))
    if source_pins_sha256() != runtime.get("sourcePinsSha256"):
        raise PairedSupervisorRefused("paired worker source digest mismatch")
    identity = _paired_runtime_identity({key: runtime[key] for key in (
        "profile", "python", "java", "jars", "pythonPackagesSha256")})
    from pyspark.sql import SparkSession
    jars = [runtime["jars"][name] for name in sorted(runtime["jars"])]
    spark = (SparkSession.builder.master("local[18]")
             .appName("Ashlar R05 bounded paired Delta writer trial")
             .config("spark.driver.memory", "24g")
             .config("spark.executor.memory", "24g")
             .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={root / 'tmp'}")
             .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={root / 'tmp'}")
             .config("spark.ui.enabled", "false")
             .config("spark.ui.bindAddress", "127.0.0.1")
             .config("spark.local.dir", str(root / "spark-local"))
             .config("spark.jars", ",".join(jars))
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.sql.warehouse.dir", str(root / "spark-warehouse"))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "18")
             .config("spark.eventLog.enabled", "true")
             .config("spark.eventLog.dir", (root / "spark-events").as_uri())
             .config("spark.eventLog.compress", "false")
             .getOrCreate())
    try:
        if time.monotonic() - child_started > WORKER_SECONDS:
            raise TimeoutError("paired worker startup exceeded 780 seconds")
        from r05_paired_writer_packet import run_pair
        outcome = run_pair(spark=spark, root=root, started=child_started,
                           packet_digest=digest)
        return 0 if outcome.get("status") == "paired-comparison-passed" else 2
    finally:
        spark.stop()


def _cleanup_child(root: Path) -> int:
    admission = _read_json(root / "r05-paired-admission.json", 65536)
    root_st = root.stat()
    if admission.get("rootIdentity") != [root_st.st_dev, root_st.st_ino]:
        raise PairedSupervisorRefused("cleanup root identity differs from admission receipt")
    outcome = _verify_and_cleanup(root, admission.get("createdDirectories", {}))
    _write_private(root / CLEANUP_FILE, _canonical(outcome))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker-child", action="store_true")
    parser.add_argument("--cleanup-child", action="store_true")
    parser.add_argument("--root")
    parser.add_argument("--packet-sha256")
    parser.add_argument("--child-started")
    parser.add_argument("--authorization")
    parser.add_argument("--runtime-pins")
    parser.add_argument("--attempt-parent")
    parser.add_argument("--native-slot-lock")
    parser.add_argument("--print-review-pins", action="store_true")
    args = parser.parse_args(argv)
    if args.print_review_pins:
        if not args.runtime_pins:
            raise PairedSupervisorRefused("reviewed runtime pins file required")
        runtime = _paired_runtime_identity(json.loads(
            _read_regular(Path(args.runtime_pins), 65536)))
        print(json.dumps({
            "profile": AUTH_PROFILE, "packetSha256": packet_sha256(),
            "sourcePinsSha256": source_pins_sha256(),
            "runtimePinsSha256": full.runtime_pins_sha256(runtime),
            "attempts": 1, "nodeRows": NODE_ROWS, "edgeRows": EDGE_ROWS,
            "totalSeconds": TOTAL_SECONDS, "workerSeconds": WORKER_SECONDS,
            "rootBytes": ROOT_BYTES, "activeDeltaBytes": ACTIVE_DELTA_BYTES,
            "minimumFreeBytes": MIN_FREE_BYTES,
            "predictiveOptimizationDisabled": False,
        }, sort_keys=True))
        return 0
    if args.cleanup_child:
        return _cleanup_child(Path(args.root))
    if args.worker_child:
        return _worker_child(Path(args.root), args.packet_sha256, float(args.child_started))
    if not all((args.authorization, args.runtime_pins, args.attempt_parent,
                args.native_slot_lock)):
        raise PairedSupervisorRefused("all exact authorization/runtime/slot inputs required")
    outcome = run_supervised_attempt(
        authorization_path=Path(args.authorization), runtime_pins_path=Path(args.runtime_pins),
        attempt_parent=Path(args.attempt_parent), native_slot_lock=Path(args.native_slot_lock))
    print(json.dumps(outcome, sort_keys=True))
    return 0 if outcome.get("status") == "bounded-paired-comparison-passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
