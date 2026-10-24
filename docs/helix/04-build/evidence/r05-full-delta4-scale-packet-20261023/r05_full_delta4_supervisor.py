"""Proposed one-attempt local R05 process supervisor; importing is inert.

The command path is deliberately unreachable without an owner authorization
bound to the final packet, source closure, and exact local runtime pins. It
starts one isolated Python child process group, monitors hard limits, and only
removes receipt-owned data after the entire child group has exited.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import importlib.metadata
import inspect
import json
import os
from pathlib import Path
import platform
import re
import secrets
import shutil
import signal
import stat
import subprocess
import sys
import time
from typing import Any

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[4]
SRC = REPO / "src"
PROFILE = "ashlar-r05-full-delta4-supervisor/0.1"
AUTH_PROFILE = "ashlar-r05-full-delta4-authorization/0.1"
TOTAL_SECONDS = 1800
ROOT_BYTES = 256 * 1024**3
PARQUET_BYTES = 128 * 1024**3
MIN_FREE_BYTES = 512 * 1024**3
MIN_FREE_INODES = 1000
PHASE_SECONDS = {
    "admission_and_ddl": 60, "node_write": 360, "edge_write": 840,
    "concurrent_writes": 840,
    "point_reads": 60, "row_value_scan": 120, "endpoint_scan": 120,
    "graphframes": 120, "validation_and_cleanup": 120,
}
ROLES = ("object_current", "edge_current")
OWNER_FILE = "owner-authorization.json"
RUNTIME_FILE = "runtime-pins.json"
PHASE_FILE = "r05-full-phase-state.json"
WORKER_RESULT = "r05-full-delta4-result.json"
OWNER_TABLES = ".ashlar-r05-full-ownership.json"
SUPERVISOR_RESULT = "r05-full-supervisor-result.json"
ADMISSION_FILE = "r05-full-supervisor-admission.json"
CLEANUP_RESULT = "r05-full-cleanup-result.json"
WORKER_DIAGNOSTICS = "r05-full-worker-diagnostics.log"
CLEANUP_DIAGNOSTICS = "r05-full-cleanup-diagnostics.log"
SPARK_EVENT_DIR = "spark-events"
DIAGNOSTICS_BYTES = 32 * 1024 * 1024
EVENT_LOG_BYTES = 512 * 1024 * 1024
MAX_EVENT_LINE_BYTES = 256 * 1024
MAX_EVENT_RECORDS = 1_000_000
MAX_SQL_EXECUTIONS = 2048
MAX_SQL_DESCRIPTIONS = 2
MAX_SQL_DESCRIPTION_BYTES = 512
MAX_JOBS = 8192
MAX_STAGES = 2048
MAX_STAGE_LINKS = 10_000
MAX_JOB_GROUP_CHARS = 128
LOCK_FILE = ".ashlar-r05-supervisor.lock"
_MAX_AUTH_BYTES = 65536


class SupervisorRefused(PermissionError):
    pass


class EventLogIncomplete(ValueError):
    pass


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def source_pins_sha256() -> str:
    """Hash the packet and implementation files used by this child."""
    names = [
        "src/ashlar_host/local_current_delta4_umf.py",
        "src/ashlar_host/canonical_graphframes_materializer.py",
        "src/ashlar_host/pinned_delta_graphframes.py",
        "src/ashlar_host/managed_current_xxhash64_carriers.py",
        "src/ashlar_host/managed_current_carriers.py",
        "src/ashlar_host/resources/local-current-delta4/realization.json",
        "src/ashlar_host/resources/local-current-delta4/role-semantics.json",
        "src/ashlar_host/resources/local-current-delta4/build-inputs.json",
        "src/ashlar_host/resources/local-current-delta4/umf-spark-delta4.iife.js",
        "src/ashlar_host/resources/local-current-delta4/bridge-entry.ts",
        "src/ashlar_host/resources/local-current-delta4/THIRD_PARTY_NOTICES.txt",
        "docs/helix/04-build/evidence/current-upstream-release-20261021/local-current/pinned-delta-graphframes-smoke-20261021.json",
    ]
    for resource_root in ("managed-current-core-010", "managed-current-core-010-xxhash64"):
        names.append(f"src/ashlar_host/resources/{resource_root}/runtime-carriers.generated.json")
        names.append(f"src/ashlar_host/resources/{resource_root}/index.json")
        names.extend(f"src/ashlar_host/resources/{resource_root}/{role}.umf.json"
                     for role in ("object_current", "edge_current", "tombstone",
                                  "whole_source_history", "publication_manifest",
                                  "publication_attempt_phase"))
    h = hashlib.sha256()
    for name in names:
        path = REPO / name
        raw = path.read_bytes()
        h.update(name.encode("utf-8") + b"\0" + len(raw).to_bytes(8, "big") + raw)
    return h.hexdigest()


def packet_sha256() -> str:
    """Use the worker's one canonical packet digest implementation."""
    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    if str(HERE) not in sys.path:
        sys.path.insert(0, str(HERE))
    from full_delta4_trial import selected_packet_sha256
    return selected_packet_sha256()


def _runtime_expected() -> dict:
    receipt_path = (REPO / "docs/helix/04-build/evidence/current-upstream-release-20261021/local-current"
                    / "pinned-delta-graphframes-smoke-20261021.json")
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    pins = receipt["dependencyPins"]
    names = {}
    for item in pins["jars"]:
        _, artifact, version = item["coordinate"].split(":", 2)
        name = artifact + "-" + version + ".jar"
        if name in names:
            raise SupervisorRefused("duplicate reviewed dependency JAR pin")
        names[name] = item["sha256"]
    if len(names) != 6:
        raise SupervisorRefused("six exact reviewed Delta/GraphFrames dependency JAR pins required")
    return {
        "profile": "ashlar-r05-reviewed-local-runtime/0.1",
        "pythonSha256": pins["pythonExecutableSha256"],
        "javaSha256": pins["javaExecutableSha256"],
        "spark": "4.2.0", "delta": "4.4.1", "graphframes": "0.12.3",
        "jdkBuild": "21.0.13.0.2", "jars": names,
    }


def _read_regular(path: str | Path, maximum: int) -> bytes:
    supplied = Path(path)
    if not supplied.is_absolute() or supplied.is_symlink():
        raise SupervisorRefused("absolute regular non-symlink file required")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(supplied, flags)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > maximum:
            raise SupervisorRefused("bounded regular input file required")
        chunks = []
        remaining = info.st_size
        while remaining:
            block = os.read(fd, min(1024 * 1024, remaining))
            if not block:
                raise SupervisorRefused("input file changed while reading")
            chunks.append(block)
            remaining -= len(block)
        if os.fstat(fd).st_size != info.st_size:
            raise SupervisorRefused("input file changed while reading")
        return b"".join(chunks)
    finally:
        os.close(fd)


def _write_private(path: Path, raw: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        dir_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except BaseException:
        try:
            path.unlink()
        except OSError:
            pass
        raise


def _runtime_identity(document: Any) -> dict:
    if type(document) is not dict or set(document) != {
            "profile", "python", "java", "jars", "pythonPackagesSha256"}:
        raise SupervisorRefused("closed exact local runtime pin document required")
    expected = _runtime_expected()
    if document["profile"] != expected["profile"]:
        raise SupervisorRefused("reviewed runtime profile required")
    if (type(document["pythonPackagesSha256"]) is not str
            or re.fullmatch(r"[0-9a-f]{64}", document["pythonPackagesSha256"]) is None):
        raise SupervisorRefused("exact selected Python package closure digest required")
    python = Path(document["python"])
    java = Path(document["java"])
    if any(not p.is_absolute() or p.is_symlink() or not p.is_file()
           or p.resolve(strict=True) != p for p in (python, java)):
        raise SupervisorRefused("absolute pinned Python and Java executables required")
    if not os.access(python, os.X_OK) or not os.access(java, os.X_OK):
        raise SupervisorRefused("pinned Python and Java executables must be executable")
    if _sha(python.read_bytes()) != expected["pythonSha256"]:
        raise SupervisorRefused("Python executable differs from reviewed runtime receipt")
    if _sha(java.read_bytes()) != expected["javaSha256"]:
        raise SupervisorRefused("Java executable differs from reviewed runtime receipt")
    jars = document["jars"]
    if type(jars) is not dict or set(jars) != set(expected["jars"]):
        raise SupervisorRefused("exact six-JAR dependency closure required")
    resolved_jars = {}
    for name, value in jars.items():
        if type(value) is not str:
            raise SupervisorRefused("absolute jar paths required")
        path = Path(value)
        if (not path.is_absolute() or path.is_symlink() or not path.is_file()
                or path.resolve(strict=True) != path):
            raise SupervisorRefused("regular absolute pinned JAR required")
        digest = _sha(path.read_bytes())
        if digest != expected["jars"][name]:
            raise SupervisorRefused("JAR bytes differ from reviewed runtime receipt")
        resolved_jars[name] = str(path.resolve(strict=True))
    return {
        "profile": expected["profile"], "python": str(python.resolve(strict=True)),
        "pythonSha256": expected["pythonSha256"], "java": str(java.resolve(strict=True)),
        "javaSha256": expected["javaSha256"], "jars": resolved_jars,
        "pythonPackagesSha256": document["pythonPackagesSha256"],
        "jarsSha256": expected["jars"], "spark": expected["spark"],
        "delta": expected["delta"], "graphframes": expected["graphframes"],
        "jdkBuild": expected["jdkBuild"],
    }


def python_package_closure_sha256() -> str:
    """Hash bounded distributions used by the pinned Spark/UMF/GraphFrames worker."""
    packages = ("pyspark", "py4j", "graphframes-py", "mini-racer", "numpy")
    manifest = []
    total_bytes = 0
    for name in packages:
        distribution = importlib.metadata.distribution(name)
        files = distribution.files
        if files is None or len(files) > 100_000:
            raise SupervisorRefused("bounded installed Python distribution inventory required")
        records = []
        for entry in sorted(files, key=str):
            path = Path(distribution.locate_file(entry))
            if path.is_symlink() or not path.exists():
                raise SupervisorRefused("selected Python distribution file missing or symlinked")
            st = path.stat()
            if not stat.S_ISREG(st.st_mode):
                continue
            total_bytes += st.st_size
            if total_bytes > 3 * 1024**3:
                raise SupervisorRefused("selected Python package closure exceeds 3 GiB")
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
            records.append([str(entry), st.st_size, digest.hexdigest()])
        manifest.append({"name": distribution.metadata["Name"].lower(),
                         "version": distribution.version, "files": records})
    manifest.append({"python": platform.python_version()})
    return _sha(_canonical(manifest))


def _verify_python_runtime_modules() -> None:
    import graphframes
    from graphframes.graphframe import GraphFrame as FacadeGraphFrame
    from graphframes.classic.graphframe import GraphFrame as ClassicGraphFrame
    import py_mini_racer  # noqa: F401 - selected UMF bridge dependency

    receipt_path = (REPO / "docs/helix/04-build/evidence/current-upstream-release-20261021/local-current"
                    / "pinned-delta-graphframes-smoke-20261021.json")
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    provenance = receipt["runtimeClassProvenance"]["graphframesPython"]
    observed = {
        "packageSha256": _sha(Path(graphframes.__file__).resolve(strict=True).read_bytes()),
        "facadeSha256": _sha(Path(inspect.getsourcefile(FacadeGraphFrame)).resolve(strict=True).read_bytes()),
        "classicSha256": _sha(Path(inspect.getsourcefile(ClassicGraphFrame)).resolve(strict=True).read_bytes()),
    }
    expected = {name: provenance[name] for name in observed}
    if (platform.python_version() != receipt["runtime"]["python"]
            or importlib.metadata.version("pyspark") != receipt["runtime"]["pyspark"]
            or importlib.metadata.version("graphframes-py") != provenance["distribution"]["version"]
            or importlib.metadata.version("mini-racer") != "0.14.1"
            or observed != expected
            or ClassicGraphFrame.__module__ != "graphframes.classic.graphframe"):
        raise SupervisorRefused("Python/GraphFrames/UMF bridge module identity differs from reviewed pins")


def _probe_python_package_closure(runtime: dict) -> str:
    script = str(HERE / "r05_full_delta4_supervisor.py")
    result = subprocess.run(
        (runtime["python"], "-I", "-B", script, "--print-python-package-pin"),
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        close_fds=True, timeout=120, check=False,
        env={"PATH": str(Path(runtime["java"]).parent) + ":/usr/bin:/bin",
             "JAVA_HOME": str(Path(runtime["java"]).parent.parent),
             "PYTHONDONTWRITEBYTECODE": "1", "PYTHONUTF8": "1"})
    if result.returncode != 0 or len(result.stdout) != 65 or not result.stdout.endswith(b"\n"):
        raise SupervisorRefused("selected Python package closure probe failed")
    digest = result.stdout[:-1].decode("ascii")
    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise SupervisorRefused("selected Python package closure probe malformed")
    return digest


def runtime_pins_sha256(identity: dict) -> str:
    return _sha(_canonical(identity))


def _validate_approval(document: Any, *, packet_digest: str,
                       source_digest: str, runtime_digest: str) -> None:
    if type(document) is not dict:
        raise SupervisorRefused("owner authorization object required")
    from full_delta4_trial import (EDGE_ROWS, NODE_ROWS, PHASE_SECONDS as WORKER_PHASES,
                                   ROOT_BYTES as WORKER_ROOT, ACTIVE_PARQUET_BYTES,
                                   MIN_FREE_BYTES, TOTAL_SECONDS)
    required = {
        "profile": AUTH_PROFILE, "status": "approved", "packetSha256": packet_digest,
        "sourcePinsSha256": source_digest, "runtimePinsSha256": runtime_digest,
        "attempts": 1, "totalSeconds": TOTAL_SECONDS,
        "attemptRootBytes": WORKER_ROOT,
        "activeParquetBytes": ACTIVE_PARQUET_BYTES,
        "minimumFreeBytes": MIN_FREE_BYTES,
        "phaseSeconds": WORKER_PHASES, "nodeRows": NODE_ROWS,
        "edgeRows": EDGE_ROWS, "predictiveOptimizationDisabled": False,
    }
    if any(document.get(key) != value for key, value in required.items()):
        raise SupervisorRefused("authorization does not approve the exact final packet/runtime/resource profile")
    if (type(document.get("ownerDecision")) is not str
            or not document["ownerDecision"].strip()):
        raise SupervisorRefused("explicit non-empty owner decision required")


def _capacity_reason(root_bytes: int, parquet_bytes: int) -> str | None:
    if type(root_bytes) is not int or type(parquet_bytes) is not int:
        return "invalid-capacity-observation"
    if root_bytes > ROOT_BYTES:
        return "attempt-root-byte-cap-exceeded"
    if parquet_bytes > PARQUET_BYTES:
        return "active-parquet-byte-cap-exceeded"
    return None


def _phase_reason(state: Any, now_ns: int, child_started_ns: int) -> str | None:
    if type(state) is not dict or state.get("profile") != "ashlar-r05-full-delta4-local-trial/0.1":
        return "phase-state-missing-or-invalid"
    phases = state.get("phases")
    if type(phases) is not dict:
        return "phase-ledger-invalid"
    for name, record in phases.items():
        if name not in PHASE_SECONDS or type(record) is not dict:
            return "unknown-phase-ledger-entry"
        seconds = record.get("seconds")
        if seconds is not None and (type(seconds) not in (int, float)
                                    or seconds > PHASE_SECONDS[name]):
            return "phase-cap-exceeded:" + name
        if name == "admission_and_ddl" and record.get("endedMonotonicNs") is not None:
            if type(record["endedMonotonicNs"]) is not int:
                return "phase-end-witness-invalid"
            if record["endedMonotonicNs"] - child_started_ns > PHASE_SECONDS[name] * 1_000_000_000:
                return "phase-cap-exceeded:" + name
    active = state.get("activePhase")
    if active is not None:
        started = phases.get(active, {}).get("startedMonotonicNs")
        if active not in PHASE_SECONDS or type(started) is not int or started <= 0:
            return "active-phase-witness-invalid"
        phase_anchor = child_started_ns if active == "admission_and_ddl" else started
        if now_ns - phase_anchor > PHASE_SECONDS[active] * 1_000_000_000:
            return "phase-cap-exceeded:" + active
    writes = sum(float(phases.get(name, {}).get("seconds", 0.0) or 0.0)
                 for name in ("node_write", "edge_write", "concurrent_writes"))
    if active in ("node_write", "edge_write", "concurrent_writes"):
        writes += max(0.0, (now_ns - phases[active]["startedMonotonicNs"]) / 1e9)
    if writes > 1200:
        return "combined-write-cap-exceeded"
    return None


def _spark_process_count() -> int:
    """Read a fresh local process view but return only a candidate count."""
    result = subprocess.run(("ps", "-axo", "pid=,command="),
                            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, check=False, timeout=10)
    if result.returncode != 0:
        raise SupervisorRefused("fresh local Spark process admission unavailable")
    candidates = 0
    patterns = ("org.apache.spark", "spark-class", "pyspark-shell", "SparkSubmit")
    for line in result.stdout.decode("utf-8", "replace").splitlines():
        if any(pattern.lower() in line.lower() for pattern in patterns):
            candidates += 1
    return candidates


def _lock_existing(path: str | Path) -> int:
    supplied = Path(path)
    if not supplied.is_absolute() or supplied.is_symlink() or not supplied.is_file():
        raise SupervisorRefused("pre-existing shared local native-slot lock file required")
    fd = os.open(supplied, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0))
    info = os.fstat(fd)
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
            or info.st_mode & 0o077):
        os.close(fd)
        raise SupervisorRefused("private owner-controlled native-slot lock required")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as error:
        os.close(fd)
        raise SupervisorRefused("shared local native slot is occupied") from error
    return fd


def _prepare_root(parent: str | Path) -> tuple[Path, dict]:
    base = Path(parent)
    if (not base.is_absolute() or base.is_symlink() or not base.is_dir()
            or base.resolve(strict=True) != base):
        raise SupervisorRefused("canonical existing attempt parent required")
    info = base.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise SupervisorRefused("owner-controlled attempt parent required")
    usage = shutil.disk_usage(base)
    fs = os.statvfs(base)
    if usage.free < MIN_FREE_BYTES or fs.f_favail < MIN_FREE_INODES:
        raise SupervisorRefused("fresh admission requires 512 GiB and 1000 free inodes")
    root = base / ("ashlar-r05-full-" + secrets.token_hex(16))
    root.mkdir(mode=0o700)
    os.chmod(root, 0o700)
    created = {}
    for name in ("spark-local", "spark-warehouse", "tmp", SPARK_EVENT_DIR):
        child = root / name
        child.mkdir(mode=0o700)
        os.chmod(child, 0o700)
        st = child.stat()
        created[name] = [st.st_dev, st.st_ino]
    return root, created


def _is_private_scratch_path(root: Path, item: Path) -> bool:
    try:
        relative = item.relative_to(root)
    except ValueError:
        return False
    return bool(relative.parts) and relative.parts[0] in {"spark-local", "tmp", SPARK_EVENT_DIR}


def _capacity_stat(root: Path, item: Path, stat_fn=os.lstat):
    """Read a live capacity entry; Spark scratch rename races may vanish mid-walk."""
    try:
        return stat_fn(item)
    except FileNotFoundError:
        if _is_private_scratch_path(root, item):
            return None
        raise


def _tree_bytes(root: Path) -> int:
    total = 0
    def walk_error(error):
        if isinstance(error, FileNotFoundError) and error.filename:
            if _is_private_scratch_path(root, Path(error.filename)):
                return
        raise error

    for directory, dirs, files in os.walk(root, followlinks=False, onerror=walk_error):
        for name in dirs + files:
            item = Path(directory) / name
            st = _capacity_stat(root, item)
            if st is None:
                continue
            if stat.S_ISLNK(st.st_mode):
                raise SupervisorRefused("symlink appeared under attempt root")
            if stat.S_ISREG(st.st_mode):
                total += st.st_size
    return total


def _parquet_bytes(root: Path) -> int:
    total = 0
    for role in ROLES:
        table = root / role
        if not table.exists():
            continue
        def table_walk_error(error):
            raise error

        for directory, dirs, files in os.walk(table, followlinks=False, onerror=table_walk_error):
            for name in dirs + files:
                item = Path(directory) / name
                st = item.lstat()
                if stat.S_ISLNK(st.st_mode):
                    raise SupervisorRefused("symlink appeared under Delta table")
                if stat.S_ISREG(st.st_mode) and name.endswith(".parquet"):
                    total += st.st_size
    return total


def _named_tree_bytes(path: Path, *, tolerate_disappearing: bool = False) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        st = path.lstat()
        if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
            raise SupervisorRefused("bounded private output is not a regular file")
        return st.st_size
    total = 0
    def walk_error(error):
        if tolerate_disappearing and isinstance(error, FileNotFoundError):
            return
        raise error

    for directory, dirs, files in os.walk(path, followlinks=False, onerror=walk_error):
        for name in dirs + files:
            item = Path(directory) / name
            try:
                st = item.lstat()
            except FileNotFoundError:
                if tolerate_disappearing:
                    continue
                raise
            if stat.S_ISLNK(st.st_mode):
                raise SupervisorRefused("symlink appeared under bounded private output")
            if stat.S_ISREG(st.st_mode):
                total += st.st_size
    return total


def _diagnostic_reason(root: Path) -> str | None:
    if _named_tree_bytes(root / WORKER_DIAGNOSTICS) > DIAGNOSTICS_BYTES:
        return "worker-diagnostics-byte-cap-exceeded"
    if _named_tree_bytes(root / CLEANUP_DIAGNOSTICS) > DIAGNOSTICS_BYTES:
        return "cleanup-diagnostics-byte-cap-exceeded"
    if _named_tree_bytes(root / SPARK_EVENT_DIR, tolerate_disappearing=True) > EVENT_LOG_BYTES:
        return "spark-event-log-byte-cap-exceeded"
    return None


def _private_file_evidence(path: Path, maximum: int) -> dict:
    try:
        st = path.lstat()
        if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
            raise SupervisorRefused("private diagnostics changed file type")
        digest = hashlib.sha256()
        read_bytes = 0
        with path.open("rb") as stream:
            while read_bytes < min(st.st_size, maximum * 2):
                chunk = stream.read(min(1024 * 1024, maximum * 2 - read_bytes))
                if not chunk:
                    break
                digest.update(chunk)
                read_bytes += len(chunk)
    except FileNotFoundError:
        return {"status": "absent", "bytes": 0, "sha256": None}
    result = {"status": "present" if st.st_size <= maximum else "cap-exceeded",
              "bytes": st.st_size, "sha256": digest.hexdigest()}
    if read_bytes != st.st_size:
        result["hashCoverage"] = "prefix-64MiB"
    return result


def _spark_event_summary(root: Path) -> dict:
    """Stream SQL/job/stage/task lineage into bounded aggregates; never retain task rows."""
    directory = root / SPARK_EVENT_DIR
    totals: dict[str, dict[str, int]] = {}
    executions: dict[str, dict[str, Any]] = {}
    jobs: dict[str, dict[str, Any]] = {}
    stage_links: dict[str, dict[str, set[str]]] = {}
    files = []
    event_hash = hashlib.sha256()
    event_bytes = 0
    event_records = 0
    stage_links_count = 0
    try:
        for item in sorted(directory.iterdir()):
            item_stat = item.lstat()
            if stat.S_ISLNK(item_stat.st_mode):
                raise SupervisorRefused("symlink appeared under Spark event log directory")
            if not stat.S_ISREG(item_stat.st_mode) or item.name.endswith((".crc", ".inprogress")):
                continue
            files.append(item)
        event_bytes = sum(item.stat().st_size for item in files)
        if event_bytes > EVENT_LOG_BYTES:
            return {"status": "event-log-cap-exceeded", "eventLogBytes": event_bytes, "stages": []}
        for item in files:
            event_hash.update(item.name.encode("utf-8") + b"\0")
            with item.open("rb") as stream:
                for raw_line in stream:
                    event_records += 1
                    if len(raw_line) > MAX_EVENT_LINE_BYTES:
                        raise EventLogIncomplete("event-line-byte-cap-exceeded")
                    if event_records > MAX_EVENT_RECORDS:
                        raise EventLogIncomplete("event-record-count-cap-exceeded")
                    event_hash.update(raw_line)
                    event = json.loads(raw_line)
                    if (type(event) is not dict or type(event.get("Event")) is not str
                            or not event.get("Event")):
                        raise ValueError("malformed-event-record")
                    kind = event["Event"]
                    if kind.endswith("SparkListenerSQLExecutionStart"):
                        execution_id = event.get("executionId")
                        if (type(execution_id) is not int or execution_id < 0
                                or execution_id > 2**63 - 1):
                            raise ValueError("malformed-sql-execution-id")
                        root_execution_id = event.get("rootExecutionId")
                        if (root_execution_id is not None and (type(root_execution_id) is not int
                                or root_execution_id < 0 or root_execution_id > 2**63 - 1)):
                            raise ValueError("malformed-root-sql-execution-id")
                        description = event.get("description", "")
                        if (type(description) is not str
                                or len(description.encode("utf-8")) > MAX_SQL_DESCRIPTION_BYTES):
                            raise EventLogIncomplete("sql-description-byte-cap-exceeded")
                        if str(execution_id) not in executions and len(executions) >= MAX_SQL_EXECUTIONS:
                            raise EventLogIncomplete("sql-execution-count-cap-exceeded")
                        execution = executions.setdefault(str(execution_id), {
                                "executionId": int(execution_id),
                                "rootExecutionId": root_execution_id,
                                "descriptions": set(), "jobIds": set(),
                                "stageIds": set(), "phaseNames": set(),
                            })
                        if description not in execution["descriptions"]:
                            if len(execution["descriptions"]) >= MAX_SQL_DESCRIPTIONS:
                                raise EventLogIncomplete("sql-description-count-cap-exceeded")
                            execution["descriptions"].add(description)
                    elif kind.endswith("SparkListenerJobStart"):
                        job_id = event.get("Job ID")
                        if type(job_id) is not int or job_id < 0 or job_id > 2**31 - 1:
                            raise ValueError("malformed-job-id")
                        if str(job_id) not in jobs and len(jobs) >= MAX_JOBS:
                            raise EventLogIncomplete("job-count-cap-exceeded")
                        props = event.get("Properties")
                        if type(props) is not dict:
                            raise ValueError("malformed-job-properties")
                        execution_id = props.get("spark.sql.execution.id")
                        group_id = props.get("spark.jobGroup.id")
                        if execution_id is not None and (type(execution_id) not in (str, int)
                                or not str(execution_id).isdigit()):
                            raise ValueError("malformed-job-sql-execution-id")
                        if group_id is not None and (type(group_id) is not str
                                or len(group_id.encode("utf-8")) > MAX_JOB_GROUP_CHARS):
                            raise EventLogIncomplete("job-group-id-byte-cap-exceeded")
                        phase = _phase_from_job_group(group_id)
                        infos = event.get("Stage Infos")
                        if type(infos) is not list:
                            raise ValueError("malformed-job-stage-list")
                        stage_ids = []
                        for info in infos:
                            if type(info) is not dict or type(info.get("Stage ID")) is not int:
                                raise ValueError("malformed-job-stage-info")
                            stage_id = info["Stage ID"]
                            if stage_id < 0 or stage_id > 2**31 - 1:
                                raise ValueError("malformed-job-stage-id")
                            stage_ids.append(stage_id)
                        stage_links_count += len(stage_ids)
                        if stage_links_count > MAX_STAGE_LINKS:
                            raise EventLogIncomplete("stage-link-count-cap-exceeded")
                        if any(str(stage) not in stage_links for stage in stage_ids):
                            new_stages = sum(1 for stage in stage_ids if str(stage) not in stage_links)
                            if len(stage_links) + new_stages > MAX_STAGES:
                                raise EventLogIncomplete("stage-count-cap-exceeded")
                        job = {"jobId": str(job_id),
                               "executionId": str(execution_id) if execution_id is not None else None,
                               "phase": phase, "stageIds": stage_ids}
                        jobs[str(job_id)] = job
                        for stage_id in stage_ids:
                            link = stage_links.setdefault(str(stage_id), {
                                "jobIds": set(), "executionIds": set(), "phaseNames": set()})
                            link["jobIds"].add(str(job_id))
                            if execution_id is not None:
                                link["executionIds"].add(str(execution_id))
                            if phase is not None:
                                link["phaseNames"].add(phase)
                        if execution_id is not None and str(execution_id) in executions:
                            execution = executions[str(execution_id)]
                            execution["jobIds"].add(str(job_id))
                            execution["stageIds"].update(str(stage) for stage in stage_ids)
                            if phase:
                                execution["phaseNames"].add(phase)
                    elif kind.endswith("SparkListenerTaskEnd"):
                        stage = event.get("Stage ID")
                        if type(stage) is not int:
                            raise ValueError("malformed-task-stage-id")
                        if stage < 0 or stage > 2**31 - 1:
                            raise ValueError("malformed-task-stage-id")
                        metrics = event.get("Task Metrics") or {}
                        if type(metrics) is not dict:
                            raise ValueError("malformed-task-metrics")
                        for metrics_key in ("Input Metrics", "Shuffle Read Metrics", "Shuffle Write Metrics"):
                            if metrics.get(metrics_key) is not None and type(metrics[metrics_key]) is not dict:
                                raise ValueError("malformed-task-metric-subobject")
                        if str(stage) not in totals and len(totals) >= MAX_STAGES:
                            raise EventLogIncomplete("task-stage-count-cap-exceeded")
                        row = totals.setdefault(str(stage), {
                            "tasks": 0, "executorRunTimeMs": 0, "executorCpuTimeNs": 0,
                            "jvmGcTimeMs": 0, "inputBytes": 0, "shuffleReadBytes": 0,
                            "shuffleWriteBytes": 0, "memorySpillBytes": 0, "diskSpillBytes": 0,
                        })
                        row["tasks"] += 1
                        row["executorRunTimeMs"] += int(metrics.get("Executor Run Time", 0) or 0)
                        row["executorCpuTimeNs"] += int(metrics.get("Executor CPU Time", 0) or 0)
                        row["jvmGcTimeMs"] += int(metrics.get("JVM GC Time", 0) or 0)
                        row["inputBytes"] += int((metrics.get("Input Metrics") or {}).get("Bytes Read", 0) or 0)
                        read = metrics.get("Shuffle Read Metrics") or {}
                        row["shuffleReadBytes"] += int(read.get("Remote Bytes Read", 0) or 0) + int(read.get("Local Bytes Read", 0) or 0)
                        write = metrics.get("Shuffle Write Metrics") or {}
                        row["shuffleWriteBytes"] += int(write.get("Shuffle Bytes Written", 0) or 0)
                        row["memorySpillBytes"] += int(metrics.get("Memory Bytes Spilled", 0) or 0)
                        row["diskSpillBytes"] += int(metrics.get("Disk Bytes Spilled", 0) or 0)
                    else:
                        continue
        for job in jobs.values():
            execution_id = job["executionId"]
            if execution_id is not None and execution_id in executions:
                execution = executions[execution_id]
                execution["jobIds"].add(job["jobId"])
                execution["stageIds"].update(str(stage) for stage in job["stageIds"])
                if job["phase"]:
                    execution["phaseNames"].add(job["phase"])
        stage_rows = []
        for stage in sorted(totals, key=int):
            links = stage_links.get(stage, {"jobIds": set(), "executionIds": set(), "phaseNames": set()})
            stage_rows.append({"stageId": int(stage), **totals[stage],
                               "jobIds": sorted(links["jobIds"], key=int),
                               "sqlExecutionIds": sorted(links["executionIds"], key=int),
                               "phaseNames": sorted(links["phaseNames"])})
        sql_rows = []
        for execution in sorted(executions.values(), key=lambda item: item["executionId"]):
            sql_rows.append({**execution,
                             "descriptions": sorted(execution["descriptions"]),
                             "jobIds": sorted(execution["jobIds"], key=int),
                             "stageIds": sorted(execution["stageIds"], key=int),
                             "phaseNames": sorted(execution["phaseNames"])})
        untagged_jobs = sum(1 for job in jobs.values() if job["phase"] is None)
        unmapped_task_stages = sum(
            1 for stage in totals
            if len(stage_links.get(stage, {}).get("phaseNames", set())) != 1)
        return {"status": "parsed", "files": len(files), "eventRecords": event_records,
                "eventLogBytes": event_bytes,
                "eventLogSha256": event_hash.hexdigest(), "sqlExecutions": sql_rows,
                "jobs": len(jobs), "untaggedJobs": untagged_jobs,
                "unmappedTaskStages": unmapped_task_stages,
                "stagePhaseAttribution": "complete" if totals and jobs
                and untagged_jobs == 0 and unmapped_task_stages == 0 else "incomplete",
                "stages": stage_rows}
    except FileNotFoundError as error:
        return {"status": "incomplete", "reason": "event-log-file-disappeared",
                "eventLogBytes": event_bytes, "stages": []}
    except EventLogIncomplete as error:
        return {"status": "incomplete", "reason": str(error),
                "eventLogBytes": event_bytes, "eventRecords": event_records,
                "stages": []}
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError):
        return {"status": "parse-failed", "eventLogBytes": event_bytes, "stages": []}


def _phase_from_job_group(group_id: Any) -> str | None:
    if type(group_id) is not str:
        return None
    for phase in PHASE_SECONDS:
        prefix = f"r05-full-{phase}-"
        suffix = group_id.removeprefix(prefix) if group_id.startswith(prefix) else ""
        if suffix.isdigit():
            return phase
    return None


def _read_json(path: Path, maximum: int = 8 * 1024 * 1024) -> Any:
    return json.loads(_read_regular(path, maximum))


def _group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _kill_reap(process: subprocess.Popen, wait_seconds: float = 10.0) -> bool:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=wait_seconds)
    except subprocess.TimeoutExpired:
        return False
    end = time.monotonic() + wait_seconds
    while time.monotonic() < end:
        if not _group_exists(process.pid):
            return True
        time.sleep(0.05)
    return not _group_exists(process.pid)


def _safe_remove_tree(path: Path) -> None:
    if path.is_symlink() or not path.is_dir():
        raise SupervisorRefused("exact owned directory required for cleanup")
    for directory, dirs, files in os.walk(path, followlinks=False):
        for name in dirs + files:
            item = Path(directory) / name
            st = item.lstat()
            if not (stat.S_ISDIR(st.st_mode) or stat.S_ISREG(st.st_mode)):
                raise SupervisorRefused("owned path contains symlink or special file")
    shutil.rmtree(path)


def _validate_removable_tree(path: Path) -> None:
    if path.is_symlink() or not path.is_dir():
        raise SupervisorRefused("exact owned directory required for cleanup")
    for directory, dirs, files in os.walk(path, followlinks=False):
        for name in dirs + files:
            item = Path(directory) / name
            st = item.lstat()
            if not (stat.S_ISDIR(st.st_mode) or stat.S_ISREG(st.st_mode)):
                raise SupervisorRefused("owned path contains symlink or special file")


def _verify_and_cleanup(root: Path, created: dict, worker: dict,
                        ownership: dict) -> dict:
    if worker.get("status") != "bounded-synthetic-checks-passed":
        raise SupervisorRefused("bounded semantic gates did not pass")
    acceptance = worker.get("acceptance", {})
    if (acceptance.get("boundedOracleChecksPassed") is not True
            or acceptance.get("identityDerivedFromFrozenRangeGeneratorAndPersistedCounts") is not True
            or acceptance.get("persistedDistinctProof") is not False
            or acceptance.get("supportClaim") != "absent"):
        raise SupervisorRefused("synthetic generator/count acceptance receipt mismatch")
    row_scan = worker.get("rowValueScan", {})
    roles = row_scan.get("roles", {})
    if set(roles) != set(ROLES):
        raise SupervisorRefused("both persisted role witnesses required")
    expected_rows = {"object_current": 1_000_000_000,
                     "edge_current": 5_000_000_000}
    for role, count in expected_rows.items():
        witness = roles[role]
        expected_sum = count * (count - 1) // 2
        if (witness.get("rows") != count or witness.get("bad_rows") != 0
                or witness.get("min_id") != 0 or witness.get("max_id") != count - 1
                or int(witness.get("sum_id")) != expected_sum):
            raise SupervisorRefused("persisted row/value/range witness failed")
    endpoints = worker.get("endpointScan", {})
    if (endpoints.get("rows") != expected_rows["edge_current"]
            or any(endpoints.get(name) != 0 for name in (
                "invalid_source", "invalid_target", "source_out_of_range", "target_out_of_range"))):
        raise SupervisorRefused("persisted endpoint formula witness failed")
    if set(worker.get("pointLookups", {})) != set(ROLES):
        raise SupervisorRefused("exactly two point lookup receipts required")
    graph = worker.get("graphframes", {})
    if graph.get("resultRows") != 5 or len(graph.get("result", [])) != 5:
        raise SupervisorRefused("complete bounded GraphFrames occurrence bag required")
    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    if str(HERE) not in sys.path:
        sys.path.insert(0, str(HERE))
    from full_delta4_generator import SOURCE_SYSTEM, endpoint_oracle
    actual_graph_bag = tuple(sorted(tuple(row) for row in graph["result"]))
    expected_graph_bag = tuple(sorted(
        (SOURCE_SYSTEM, 1, 0, SOURCE_SYSTEM, 7, edge_id,
         SOURCE_SYSTEM, 1, endpoint_oracle(edge_id)[1])
        for edge_id in range(5)))
    if actual_graph_bag != expected_graph_bag:
        raise SupervisorRefused("GraphFrames occurrence bag differs from frozen oracle")
    if ownership.get("status") != "validated-awaiting-supervisor-quiescence":
        raise SupervisorRefused("child did not defer cleanup under exact ownership receipt")
    root_st = root.stat()
    if ownership.get("root") != [root_st.st_dev, root_st.st_ino]:
        raise SupervisorRefused("private attempt root identity changed")
    tables = ownership.get("tables", {})
    if set(tables) != set(ROLES):
        raise SupervisorRefused("exact two-role ownership receipt required")

    from ashlar_host.pinned_delta_graphframes import (
        _canonical_delta_uuid, _read_canonical_delta_log_protocol,
        _canonical_action_log_protocol_matches, CANONICAL_ACTION_LOG_PROTOCOL,
    )
    candidates = []
    for role in ROLES:
        table = root / role
        record = tables[role]
        st = table.lstat()
        if record.get("pathIdentity") != [st.st_dev, st.st_ino]:
            raise SupervisorRefused("table directory identity changed")
        if _canonical_delta_uuid(table) != record.get("uuid"):
            raise SupervisorRefused("Delta UUID differs from ownership receipt")
        version = worker.get("tables", {}).get(role, {}).get("postWrite", {}).get("version")
        log_root = table / "_delta_log"
        versions = []
        for name in os.listdir(log_root):
            if re.fullmatch(r"[0-9]{20}\.json", name):
                versions.append(int(name[:20]))
        if not versions or max(versions) != version:
            raise SupervisorRefused("Delta head moved after child exit")
        protocol, protocol_version, _ = _read_canonical_delta_log_protocol(table, version)
        if (protocol_version > version
                or not _canonical_action_log_protocol_matches(protocol)
                or set(protocol.get("writerFeatures", ())) !=
                set(CANONICAL_ACTION_LOG_PROTOCOL["writerFeatures"])):
            raise SupervisorRefused("owned Delta writer protocol custody changed")
        candidates.append((role, table))

    # Validate every owned Delta table before deleting any of them.
    for role, table in candidates:
        _validate_removable_tree(table)

    removed_scratch = []
    for name, identity in created.items():
        path = root / name
        if not path.exists():
            continue
        st = path.lstat()
        if [st.st_dev, st.st_ino] != identity:
            raise SupervisorRefused("supervisor-owned scratch identity changed")
        _validate_removable_tree(path)
        removed_scratch.append(name)
    for _, table in candidates:
        _safe_remove_tree(table)
    for name in removed_scratch:
        _safe_remove_tree(root / name)
    return {"status": "removed-after-child-group-quiescence",
            "ownedTableRoles": [role for role, _ in candidates],
            "ownedScratchDirectories": removed_scratch,
            "ownershipReceiptPreserved": True}


def _worker_argv(runtime: dict, root: Path, child_started: float,
                 packet_digest: str) -> list[str]:
    script = HERE / "r05_full_delta4_supervisor.py"
    return [runtime["python"], "-I", "-B", str(script), "--worker-child",
            "--root", str(root), "--packet-sha256", packet_digest,
            "--child-started", repr(child_started)]


def _child_environment(runtime: dict, root: Path) -> dict[str, str]:
    java_home = str(Path(runtime["java"]).parent.parent)
    return {
        "PATH": str(Path(runtime["java"]).parent) + ":/usr/bin:/bin",
        "JAVA_HOME": java_home,
        "PYSPARK_PYTHON": runtime["python"],
        "PYSPARK_DRIVER_PYTHON": runtime["python"],
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONUTF8": "1",
        "SPARK_LOCAL_IP": "127.0.0.1",
        "TMPDIR": str(root / "tmp"),
    }


def _run_child(root: Path, runtime: dict, packet_digest: str,
               started: float, started_ns: int) -> tuple[int, dict, bool, str | None]:
    deadline = started + TOTAL_SECONDS
    argv = _worker_argv(runtime, root, started, packet_digest)
    diagnostic_fd = os.open(root / WORKER_DIAGNOSTICS,
                            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0),
                            0o600)
    try:
        process = subprocess.Popen(argv, cwd=str(root), env=_child_environment(runtime, root),
                                   stdin=subprocess.DEVNULL, stdout=diagnostic_fd,
                                   stderr=subprocess.STDOUT, close_fds=True,
                                   start_new_session=True)
    finally:
        os.close(diagnostic_fd)
    killed = False
    reason = None
    last_state = {}
    next_source_check = time.monotonic()
    try:
        while process.poll() is None:
            now = time.monotonic()
            if now >= deadline:
                reason = "aggregate-deadline-exceeded"
            if reason is None:
                try:
                    root_size = _tree_bytes(root)
                    parquet_size = _parquet_bytes(root)
                    reason = _capacity_reason(root_size, parquet_size)
                    if reason is None:
                        reason = _diagnostic_reason(root)
                    phase_path = root / PHASE_FILE
                    if reason is None:
                        if phase_path.exists():
                            last_state = _read_json(phase_path, 2 * 1024 * 1024)
                        elif now - started > PHASE_SECONDS["admission_and_ddl"]:
                            reason = "phase-cap-exceeded:admission_and_ddl"
                        if reason is None and last_state:
                            reason = _phase_reason(last_state, time.monotonic_ns(), started_ns)
                        if (reason is None and now - started > PHASE_SECONDS["admission_and_ddl"]
                                and last_state.get("activePhase") == "admission_and_ddl"):
                            reason = "phase-cap-exceeded:admission_and_ddl"
                except BaseException as error:
                    reason = "surveillance-failed:" + type(error).__name__
            if reason is None and now >= next_source_check:
                try:
                    if source_pins_sha256() != runtime["sourcePinsSha256"]:
                        reason = "source-pin-drift"
                    # Runtime executables, Python distributions and all six JARs
                    # were byte-pinned before child start. The child also verifies
                    # its in-process package closure before opening Spark.
                except BaseException:
                    reason = "source-pin-surveillance-failed"
                next_source_check = now + 2.0
            if reason is not None:
                killed = True
                _kill_reap(process)
                break
            time.sleep(min(1.0, max(0.01, deadline - now)))
    except BaseException:
        _kill_reap(process)
        raise
    status = process.poll()
    try:
        worker = _read_json(root / WORKER_RESULT)
    except BaseException:
        worker = {}
    if reason is None:
        try:
            reason = _capacity_reason(_tree_bytes(root), _parquet_bytes(root))
            if reason is None:
                reason = _diagnostic_reason(root)
        except BaseException as error:
            reason = "post-exit-surveillance-failed:" + type(error).__name__
    quiescent = not _group_exists(process.pid)
    if not quiescent and not killed:
        reason = "unexpected-descendant-after-worker-exit"
        _kill_reap(process)
        quiescent = not _group_exists(process.pid)
    return int(status if status is not None else -signal.SIGKILL), worker, quiescent, reason


def _cleanup_child(root: Path) -> int:
    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    admission = _read_json(root / ADMISSION_FILE, 65536)
    st = root.stat()
    if (admission.get("profile") != PROFILE
            or admission.get("rootIdentity") != [st.st_dev, st.st_ino]):
        raise SupervisorRefused("supervisor-created root receipt changed")
    worker = _read_json(root / WORKER_RESULT)
    ownership = _read_json(root / OWNER_TABLES, 1024 * 1024)
    cleanup = _verify_and_cleanup(
        root, admission["createdScratchDirectories"], worker, ownership)
    _write_private(root / CLEANUP_RESULT, _canonical(cleanup))
    return 0


def _run_cleanup(root: Path, runtime: dict, *, started: float,
                 consumed_phase_seconds: float) -> tuple[dict, float, bool, str | None]:
    budget = min(max(0.0, PHASE_SECONDS["validation_and_cleanup"] - consumed_phase_seconds),
                 max(0.0, TOTAL_SECONDS - (time.monotonic() - started)))
    if budget <= 0:
        return {}, 0.0, True, "validation_cleanup_deadline-exceeded"
    argv = [runtime["python"], "-I", "-B", str(HERE / "r05_full_delta4_supervisor.py"),
            "--cleanup-child", "--root", str(root)]
    env = _child_environment(runtime, root)
    child_started = time.monotonic()
    diagnostic_fd = os.open(root / CLEANUP_DIAGNOSTICS,
                            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0),
                            0o600)
    try:
        process = subprocess.Popen(argv, cwd=str(root), env=env, stdin=subprocess.DEVNULL,
                                   stdout=diagnostic_fd, stderr=subprocess.STDOUT,
                                   close_fds=True, start_new_session=True)
    finally:
        os.close(diagnostic_fd)
    deadline = child_started + budget
    reason = None
    while process.poll() is None:
        now = time.monotonic()
        if now >= deadline:
            reason = "validation_cleanup_deadline-exceeded"
            _kill_reap(process)
            break
        try:
            reason = _capacity_reason(_tree_bytes(root), _parquet_bytes(root))
            if reason is None:
                reason = _diagnostic_reason(root)
        except BaseException as error:
            reason = "cleanup-surveillance-failed:" + type(error).__name__
        if reason is not None:
            _kill_reap(process)
            break
        time.sleep(min(0.5, max(0.01, deadline - now)))
    elapsed = time.monotonic() - child_started
    quiescent = not _group_exists(process.pid)
    if not quiescent:
        reason = reason or "cleanup-child-group-not-quiescent"
        _kill_reap(process)
        quiescent = not _group_exists(process.pid)
    try:
        cleanup = _read_json(root / CLEANUP_RESULT, 65536)
    except BaseException:
        cleanup = {}
    if process.poll() != 0 and reason is None:
        reason = "cleanup-child-failed"
    return cleanup, elapsed, quiescent, reason


def run_supervised_attempt(*, authorization_path: str | Path,
                           runtime_pins_path: str | Path,
                           attempt_parent: str | Path,
                           native_slot_lock: str | Path) -> dict:
    """Run one authorized child; this function is the only mutating entrypoint."""
    packet_digest = packet_sha256()
    source_digest = source_pins_sha256()
    auth_raw = _read_regular(authorization_path, _MAX_AUTH_BYTES)
    runtime_raw = _read_regular(runtime_pins_path, 65536)
    auth_doc = json.loads(auth_raw)
    runtime_doc = json.loads(runtime_raw)
    runtime = _runtime_identity(runtime_doc)
    runtime_digest = runtime_pins_sha256(runtime)
    observed_packages = _probe_python_package_closure(runtime)
    if observed_packages != runtime["pythonPackagesSha256"]:
        raise SupervisorRefused("selected Python package bytes differ from approved closure")
    _validate_approval(auth_doc, packet_digest=packet_digest,
                       source_digest=source_digest, runtime_digest=runtime_digest)
    slot_fd = _lock_existing(native_slot_lock)
    slot_path = Path(native_slot_lock)
    slot_identity = os.fstat(slot_fd)
    try:
        if _spark_process_count() != 0:
            raise SupervisorRefused("fresh process scan found an occupied local Spark slot")
        if packet_sha256() != packet_digest or source_pins_sha256() != source_digest:
            raise SupervisorRefused("reviewed packet or source pin changed before child launch")
        root, created = _prepare_root(attempt_parent)
        _write_private(root / OWNER_FILE, auth_raw)
        runtime_private = dict(runtime)
        runtime_private["sourcePinsSha256"] = source_digest
        runtime_private["runtimePinsSha256"] = runtime_digest
        _write_private(root / RUNTIME_FILE, _canonical(runtime_private))
        _write_private(root / ADMISSION_FILE, _canonical({
            "profile": PROFILE, "createdScratchDirectories": created,
            "rootIdentity": [root.stat().st_dev, root.stat().st_ino],
        }))
        runtime["sourcePinsSha256"] = source_digest
        runtime["runtimePinsSha256"] = runtime_digest
        started_ns = time.monotonic_ns()
        started = started_ns / 1_000_000_000
        if shutil.disk_usage(root.parent).free < MIN_FREE_BYTES:
            raise SupervisorRefused("512 GiB free disk admission changed before child start")
        result = {
            "profile": PROFILE, "status": "child-starting",
            "packetSha256": packet_digest, "sourcePinsSha256": source_digest,
            "runtimePinsSha256": runtime_digest, "authorizationSha256": _sha(auth_raw),
            "totalSecondsCap": TOTAL_SECONDS, "attemptRootBytesCap": ROOT_BYTES,
            "activeParquetBytesCap": PARQUET_BYTES,
            "phaseSeconds": PHASE_SECONDS,
        }
        # The root path and local runtime paths remain private to this directory;
        # they are not copied into any checked-in receipt.
        try:
            status, worker, quiescent, reason = _run_child(
                root, runtime, packet_digest, started, started_ns)
            result["childExitStatus"] = status
            result["childGroupQuiescent"] = quiescent
            result["reason"] = reason
            result["elapsedSeconds"] = round(time.monotonic() - started, 6)
            result["workerStatus"] = worker.get("status")
            result["sparkStageSummary"] = _spark_event_summary(root)
            phase_evidence = dict(worker.get("phases", {}))
            admission_phase = phase_evidence.get("admission_and_ddl", {})
            if type(admission_phase.get("endedMonotonicNs")) is int:
                admission_phase["supervisedSeconds"] = round(
                    (admission_phase["endedMonotonicNs"] - started_ns) / 1e9, 6)
            result["phaseEvidence"] = phase_evidence
            if (reason is None and status == 0 and quiescent
                    and result["elapsedSeconds"] <= TOTAL_SECONDS):
                phase_seconds = worker.get("phases", {}).get(
                    "validation_and_cleanup", {}).get("seconds", 0.0)
                cleanup, cleanup_seconds, cleanup_quiescent, cleanup_reason = _run_cleanup(
                    root, runtime, started=started,
                    consumed_phase_seconds=float(phase_seconds))
                result["cleanupSeconds"] = round(cleanup_seconds, 6)
                result["cleanupChildGroupQuiescent"] = cleanup_quiescent
                validation_phase = result["phaseEvidence"].get("validation_and_cleanup", {})
                validation_phase["cleanupSeconds"] = round(cleanup_seconds, 6)
                validation_phase["supervisedSeconds"] = round(
                    float(phase_seconds) + cleanup_seconds, 6)
                result["cleanup"] = cleanup or {
                    "status": "preserved-after-failed-or-uncertain-cleanup"}
                result["reason"] = cleanup_reason
                result["elapsedSeconds"] = round(time.monotonic() - started, 6)
                if (cleanup_reason is None and cleanup_quiescent
                        and cleanup.get("status") == "removed-after-child-group-quiescence"
                        and result["elapsedSeconds"] <= TOTAL_SECONDS):
                    result["status"] = "bounded-synthetic-trial-passed"
                else:
                    result["status"] = "failed-or-held"
            else:
                result["cleanup"] = {"status": "preserved-after-failure-or-uncertainty"}
                result["status"] = "failed-or-held"
        except BaseException as error:
            result["status"] = "failed-or-held"
            result["reason"] = type(error).__name__
            result["cleanup"] = {"status": "preserved-after-failure-or-uncertainty"}
        result["activeParquetBytesAtClose"] = _parquet_bytes(root)
        result["privateDiagnostics"] = {
            "worker": _private_file_evidence(root / WORKER_DIAGNOSTICS, DIAGNOSTICS_BYTES),
            "cleanup": _private_file_evidence(root / CLEANUP_DIAGNOSTICS, DIAGNOSTICS_BYTES),
        }
        result["receiptFinality"] = "supervisor-after-child-exit"
        base_bytes = _tree_bytes(root)
        for _ in range(4):
            result["attemptRootBytesAtClose"] = base_bytes + len(_canonical(result))
        result_bytes = _canonical(result)
        _write_private(root / SUPERVISOR_RESULT, result_bytes)
        actual_bytes = _tree_bytes(root)
        if actual_bytes != base_bytes + len(result_bytes):
            raise SupervisorRefused("final root byte accounting did not reconcile")
        final_slot = os.stat(slot_path, follow_symlinks=False)
        if (slot_identity.st_dev, slot_identity.st_ino) != (final_slot.st_dev, final_slot.st_ino):
            raise SupervisorRefused("shared native-slot lock identity changed during attempt")
        return result
    finally:
        fcntl.flock(slot_fd, fcntl.LOCK_UN)
        os.close(slot_fd)


def _open_child(root: Path, packet_digest: str, child_started: float) -> int:
    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    if str(HERE) not in sys.path:
        sys.path.insert(0, str(HERE))
    from full_delta4_trial import (PHASE_STATE, _read_approval,
                                   run_full_delta4_trial)
    runtime = _read_json(root / RUNTIME_FILE, 65536)
    if (packet_sha256() != packet_digest
            or source_pins_sha256() != runtime.get("sourcePinsSha256")):
        raise SupervisorRefused("child packet/source pin mismatch")
    if python_package_closure_sha256() != runtime.get("pythonPackagesSha256"):
        raise SupervisorRefused("child Python package closure differs from approved pin")
    auth_path = root / OWNER_FILE
    runtime_doc = {key: runtime[key] for key in (
        "profile", "python", "java", "jars", "pythonPackagesSha256")}
    identity = _runtime_identity(runtime_doc)
    runtime_digest = runtime_pins_sha256(identity)
    auth_doc = json.loads(_read_regular(auth_path, _MAX_AUTH_BYTES))
    _validate_approval(auth_doc, packet_digest=packet_digest,
                       source_digest=runtime["sourcePinsSha256"],
                       runtime_digest=runtime_digest)
    _read_approval(auth_path, packet_digest)
    # Load the selected Python/JVM-facing packages before Spark startup. The
    # worker later reuses these exact imported modules for its bounded action.
    _verify_python_runtime_modules()
    jars = [runtime["jars"][name] for name in sorted(runtime["jars"])]
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[18]")
             .appName("Ashlar R05 one-attempt full Delta4 trial")
             .config("spark.driver.memory", "24g")
             .config("spark.executor.memory", "24g")
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
             .config("spark.eventLog.dir", (root / SPARK_EVENT_DIR).as_uri())
             .config("spark.eventLog.compress", "false")
             .config("spark.eventLog.rolling.enabled", "true")
             .config("spark.eventLog.rolling.maxFileSize", "32m")
             .getOrCreate())
    try:
        outcome = run_full_delta4_trial(
            spark=spark, attempt_root=root, authorization_path=auth_path,
            packet_sha256=packet_digest, supervised_child_started=child_started)
        return 0 if outcome.get("status") == "bounded-synthetic-checks-passed" else 2
    finally:
        spark.stop()


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
    parser.add_argument("--print-python-package-pin", action="store_true")
    parser.add_argument("--print-review-pins", action="store_true")
    parser.add_argument("--print-runtime-pin", action="store_true")
    args = parser.parse_args(argv)
    if args.print_python_package_pin:
        print(python_package_closure_sha256())
        return 0
    if args.cleanup_child:
        if not args.root:
            raise SupervisorRefused("private attempt root required for cleanup child")
        return _cleanup_child(Path(args.root))
    if args.print_review_pins:
        print(json.dumps({"profile": PROFILE, "packetSha256": packet_sha256(),
                          "sourcePinsSha256": source_pins_sha256(),
                          "runtimeProfile": _runtime_expected()["profile"]},
                         sort_keys=True, separators=(",", ":")))
        return 0
    if args.print_runtime_pin:
        if not args.runtime_pins:
            raise SupervisorRefused("runtime pin document required")
        document = json.loads(_read_regular(args.runtime_pins, 65536))
        identity = _runtime_identity(document)
        observed = _probe_python_package_closure(identity)
        if observed != identity["pythonPackagesSha256"]:
            raise SupervisorRefused("selected Python package bytes differ from supplied closure digest")
        print(runtime_pins_sha256(identity))
        return 0
    if args.worker_child:
        if not all((args.root, args.packet_sha256, args.child_started)):
            raise SupervisorRefused("exact child invocation parameters required")
        return _open_child(Path(args.root), args.packet_sha256, float(args.child_started))
    if not all((args.authorization, args.runtime_pins, args.attempt_parent,
                args.native_slot_lock)):
        raise SupervisorRefused("authorization, runtime pins, parent root, and native-slot lock required")
    result = run_supervised_attempt(
        authorization_path=args.authorization, runtime_pins_path=args.runtime_pins,
        attempt_parent=args.attempt_parent, native_slot_lock=args.native_slot_lock)
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result.get("status") == "bounded-synthetic-trial-passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
