#!/usr/bin/env python3
"""Bounded, approval-gated CLI for the R18 one-start local lifecycle candidate.

The CLI never constructs approval. It admits only the exact scope and approval
bytes and code hashes, then delegates the one PostgreSQL start/stop to the
custody worker. Native effects remain impossible without the owner approval.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import secrets
import signal
import subprocess
import sys
import time
from pathlib import Path

PROFILE = "ashlar-truss051-r18-full-lifecycle-supervisor/0.1"
MAX_SCOPE_BYTES = 256 * 1024
MAX_CHILD_DEADLINE_SECONDS = 900
SOURCE_ARCHIVE_SHA256 = "cb57fdb7d338f345cfad157463fdbe94a2be7dcf69e728c474954c2939c14a0b"
SOURCE_MANIFEST_SHA256 = "fb79cebc5b1388b49d0687c9f77c61187125299758ee7388fd64ba2c9d533348"
SOURCE_HELPER_SHA256 = "62707a3febf1f67b7a06edc1cc7ccba1a843c1dc3540fae2ac8b4406e09ea13d"
PG_CTL_SHA256 = "3021087af95fd33bfd83106c85de47c4756e44aa7edde78446e7593604d1b451"


def _load_module(name: str, path: Path):
    if path.is_symlink() or not path.is_file() or path.resolve() != path:
        raise ValueError(f"canonical worker file required: {path}")
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError(f"worker module cannot be loaded: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _verify_frozen_source_inputs(source: dict) -> Path:
    """Verify exact frozen inputs before any source-root import."""
    archive = Path(source.get("archive_path", ""))
    manifest = Path(source.get("manifest_path", ""))
    helper = Path(source.get("helper_path", ""))
    source_root = Path(source.get("source_root_path", ""))
    for path in (archive, manifest, helper, source_root):
        if not path.is_absolute() or path.resolve() != path or path.is_symlink():
            raise ValueError("canonical frozen source input paths required")
    if (not archive.is_file() or archive.stat().st_size > 16 * 1024 * 1024
            or hashlib.sha256(archive.read_bytes()).hexdigest() != SOURCE_ARCHIVE_SHA256
            or source.get("archive_sha256") != SOURCE_ARCHIVE_SHA256):
        raise ValueError("exact frozen source archive bytes required")
    if (manifest != helper.with_name("source-inputs-manifest-r16.json")
            or not manifest.is_file() or manifest.stat().st_size > 2 * 1024 * 1024
            or hashlib.sha256(manifest.read_bytes()).hexdigest() != SOURCE_MANIFEST_SHA256
            or source.get("manifest_sha256") != SOURCE_MANIFEST_SHA256):
        raise ValueError("exact frozen source manifest bytes required")
    if (not helper.is_file() or helper.stat().st_size > 2 * 1024 * 1024
            or hashlib.sha256(helper.read_bytes()).hexdigest() != SOURCE_HELPER_SHA256):
        raise ValueError("exact frozen .48 source verifier required")
    verifier = _load_module("r18_supervisor_frozen_source_verifier", helper)
    previous = os.environ.get("ASHLAR_R16_SOURCE_SRC")
    os.environ["ASHLAR_R16_SOURCE_SRC"] = str(source_root)
    try:
        result = verifier.require_frozen_source_root()
    finally:
        if previous is None:
            os.environ.pop("ASHLAR_R16_SOURCE_SRC", None)
        else:
            os.environ["ASHLAR_R16_SOURCE_SRC"] = previous
    if result.get("source_root") != str(source_root):
        raise ValueError("frozen source verifier resolved a different source root")
    return source_root


def _failure_receipt(scope: dict | None, scope_raw: bytes | None,
                     error: BaseException, prefix_worker=None) -> dict:
    effect_state = (dict(prefix_worker._EFFECT_STATE) if prefix_worker is not None else
                    {"phase": "none", "native_effects": "none", "effects_may_remain": False})
    receipt = {"profile": PROFILE, "status": "failed_or_blocked",
        "reason": f"{type(error).__name__}: {error}"[:1500],
        "native_effects": effect_state.get("native_effects", "unknown"),
        "effects_may_remain": effect_state.get("effects_may_remain", True),
        "effect_state": effect_state,
        "scope_sha256": hashlib.sha256(scope_raw).hexdigest() if scope_raw else None,
        "durable_stop_receipt_sha256": None,
        "failure_receipt_persisted": False}
    if scope is None:
        return receipt
    root_value = scope.get("output_root")
    if type(root_value) is not str:
        return receipt
    root = Path(root_value)
    if not root.is_absolute() or root.resolve() != root or root.is_symlink() or not root.is_dir():
        return receipt
    if effect_state.get("phase") in (None, "none"):
        return receipt
    stop_path = root / "lifecycle-stop-receipt.json"
    if stop_path.is_file() and not stop_path.is_symlink() and stop_path.stat().st_size <= MAX_SCOPE_BYTES:
        receipt["durable_stop_receipt_sha256"] = hashlib.sha256(stop_path.read_bytes()).hexdigest()
    failure_path = root / "supervisor-failure.json"
    persisted_receipt = {**receipt, "failure_receipt_persisted": True}
    raw = (json.dumps(persisted_receipt, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
    try:
        fd = os.open(failure_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        receipt["failure_receipt_persisted"] = True
    except (FileExistsError, OSError):
        receipt["failure_receipt_persisted"] = False
    return receipt


def _process_group_members(group_id: int) -> list[int]:
    """Bounded macOS libproc inventory copied from the reviewed R15 supervisor."""
    import ctypes
    libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    list_pids = libproc.proc_listallpids
    list_pids.argtypes = (ctypes.c_void_p, ctypes.c_int)
    list_pids.restype = ctypes.c_int
    pid_info = libproc.proc_pidinfo
    pid_info.argtypes = (ctypes.c_int, ctypes.c_int, ctypes.c_uint64,
                         ctypes.c_void_p, ctypes.c_int)
    pid_info.restype = ctypes.c_int
    capacity = 65536
    pids = (ctypes.c_int * capacity)()
    count = list_pids(pids, ctypes.sizeof(pids))
    if count < 0 or count >= capacity:
        raise RuntimeError("bounded process-group inventory failed")
    members = []
    record = ctypes.create_string_buffer(256)
    for pid in pids[:count]:
        if pid <= 0:
            continue
        size = pid_info(pid, 3, 0, record, 256)  # PROC_PIDTBSDINFO
        if size < 104:
            continue
        observed_pid = int.from_bytes(record.raw[12:16], sys.byteorder)
        observed_pgid = int.from_bytes(record.raw[100:104], sys.byteorder)
        if observed_pid == pid and observed_pgid == group_id:
            members.append(pid)
    return sorted(set(members))


def _terminate_process_group(child: subprocess.Popen, *, grace_seconds: float = 2.0,
                             kill_seconds: float = 3.0) -> list[int]:
    """TERM, then KILL, and reap the exact child session and its descendants."""
    group_id = child.pid
    try:
        members = _process_group_members(group_id)
    except BaseException:
        members = [child.pid]
    if members:
        try:
            os.killpg(group_id, signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + grace_seconds
        while time.monotonic() < deadline:
            child.poll()  # reap the leader promptly while descendants drain
            try:
                members = _process_group_members(group_id)
            except BaseException:
                members = [child.pid] if child.poll() is None else []
            if not members:
                break
            time.sleep(0.05)
        if members:
            try:
                os.killpg(group_id, signal.SIGKILL)
            except ProcessLookupError:
                pass
    try:
        child.wait(timeout=kill_seconds)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(group_id, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=kill_seconds)
        except subprocess.TimeoutExpired:
            pass
    try:
        return _process_group_members(group_id)
    except BaseException:
        return [-1]


def _monitor_child(child: subprocess.Popen, *, deadline_seconds: float,
                   sample, poll_seconds: float = 1.0) -> dict:
    """Monitor child-group lifetime; kill/reap on deadline or resource failure."""
    started = time.monotonic()
    cause = None
    samples = {}
    while child.poll() is None:
        elapsed = time.monotonic() - started
        if elapsed >= deadline_seconds:
            cause = "child_execution_deadline_exceeded"
            break
        try:
            samples = sample()
        except BaseException as error:
            cause = f"live_resource_monitor_failed:{type(error).__name__}: {error}"[:1000]
            break
        if samples.get("limit_exceeded"):
            cause = str(samples["limit_exceeded"])
            break
        time.sleep(min(poll_seconds, max(0.0, deadline_seconds - elapsed)))
    inventory_failed = False
    try:
        members = _process_group_members(child.pid)
    except BaseException:
        members = [child.pid]
        inventory_failed = True
    if inventory_failed and cause is None:
        cause = "process_group_inventory_failed_after_child_exit"
    if members and cause is None:
        cause = "worker_process_group_descendants_remained_after_leader_exit"
    remaining = _terminate_process_group(child) if cause or members else []
    try:
        returncode = child.wait(timeout=3)
    except subprocess.TimeoutExpired:
        remaining = _terminate_process_group(child)
        returncode = child.poll()
        cause = cause or "worker_leader_not_reaped"
    return {"cause": cause, "returncode": returncode,
            "child_runtime_seconds": round(time.monotonic() - started, 3),
            "process_group_remaining": remaining, "last_live_sample": samples}


def _tree_bytes(path: Path, *, maximum: int) -> int:
    if not path.exists():
        return 0
    if path.is_symlink() or not path.is_dir():
        raise RuntimeError(f"bounded output root is not a real directory: {path}")
    total = 0
    entries = 0
    for base, dirs, files in os.walk(path, followlinks=False):
        for name in dirs + files:
            entries += 1
            if entries > 10000:
                raise RuntimeError("live attempt output entry-count limit exceeded")
            item = Path(base) / name
            try:
                if item.is_symlink():
                    raise RuntimeError(f"symlink in bounded output: {item}")
                if item.is_file():
                    total += item.stat().st_size
                    if total > maximum:
                        raise RuntimeError("live attempt output byte limit exceeded")
            except FileNotFoundError:
                # Scratch files can disappear between os.walk and stat. They
                # are absent from this sample; the limit is observed-only.
                continue
    return total


def _rss_bytes_for_group(group_id: int) -> int:
    """Sample resident bytes from all members of the isolated child process group."""
    result = subprocess.run(["/bin/ps", "-axo", "pid=,pgid=,rss="],
                            stdin=subprocess.DEVNULL, capture_output=True,
                            text=True, timeout=3, check=False)
    if result.returncode:
        raise RuntimeError("live RSS process inventory unavailable")
    total_kib = 0
    found = False
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) != 3:
            continue
        try:
            _pid, pgid, rss = map(int, fields)
        except ValueError:
            continue
        if pgid == group_id:
            found = True
            total_kib += rss
    if not found:
        raise RuntimeError("isolated child process group absent from live RSS inventory")
    return total_kib * 1024


def _watchdog_failure_receipt(scope: dict, scope_raw: bytes, cause: str,
                              result: dict, log_paths: dict) -> dict:
    root = Path(scope["output_root"])
    start_intent = root / "postgres-start-intent.json"
    started_maybe = start_intent.is_file() and not start_intent.is_symlink()
    stop_path = root / "lifecycle-stop-receipt.json"
    stop_digest = (hashlib.sha256(stop_path.read_bytes()).hexdigest()
                   if stop_path.is_file() and not stop_path.is_symlink()
                   and stop_path.stat().st_size <= MAX_SCOPE_BYTES else None)
    attempt_artifacts_may_remain = root.exists()
    effect_state = {"phase": "watchdog-terminated-after-postgres-start-intent" if started_maybe
                    else "watchdog-terminated-before-postgres-start-intent",
                    "native_effects": "unknown" if started_maybe else "none",
                    "effects_may_remain": bool(started_maybe)}
    cleanup = {"attempted": False, "status": "not_required_or_start_not_proven"}
    if started_maybe:
        try:
            intent = json.loads(start_intent.read_bytes())
            restored = Path(scope["state_custody"]["restored_pgdata_path"])
            pg_ctl = Path(scope["runtime"]["pg_ctl_path"])
            if (intent.get("pgdata_path") != str(restored)
                    or not restored.is_relative_to(root)
                    or pg_ctl.resolve() != pg_ctl or pg_ctl.is_symlink()
                    or hashlib.sha256(pg_ctl.read_bytes()).hexdigest() != PG_CTL_SHA256):
                raise ValueError("start-intent target or pinned pg_ctl mismatch")
            pid_file = restored / "postmaster.pid"
            if pid_file.is_file() and not pid_file.is_symlink():
                lines = pid_file.read_text().splitlines()
                if len(lines) >= 2 and Path(lines[1]).resolve() == restored.resolve():
                    cleanup["attempted"] = True
                    stopped = subprocess.run([str(pg_ctl), "-D", str(restored), "-m", "fast",
                                              "-w", "-t", "20", "stop"],
                        stdin=subprocess.DEVNULL, capture_output=True, text=True,
                        timeout=25, check=False)
                    cleanup.update(returncode=stopped.returncode,
                                   status="stopped" if stopped.returncode == 0
                                   and not pid_file.exists() else "stop_unverified",
                                   diagnostic=(stopped.stderr or stopped.stdout)[-1000:])
        except BaseException as error:
            cleanup.update(status="stop_unverified",
                           diagnostic=f"{type(error).__name__}: {error}"[:1000])
    receipt = {"profile": PROFILE, "status": "watchdog_terminated_child",
        "cause": cause, "scope_sha256": hashlib.sha256(scope_raw).hexdigest(),
        "child": result, "effect_state": effect_state,
        "native_effects": effect_state["native_effects"],
        "effects_may_remain": effect_state["effects_may_remain"],
        "attempt_artifacts_may_remain": attempt_artifacts_may_remain,
        "lifecycle_stop_receipt_sha256": stop_digest,
        "postgres_cleanup": cleanup, "logs": {key: str(value) for key, value in log_paths.items()},
        "failure_receipt_persisted": False}
    if root.is_dir() and not root.is_symlink() and started_maybe:
        attempt_path = root / "supervisor-failure.json"
        try:
            raw = (json.dumps({**receipt, "failure_receipt_persisted": True},
                              sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
            fd = os.open(attempt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(raw); stream.flush(); os.fsync(stream.fileno())
            receipt["failure_receipt_persisted"] = True
        except (FileExistsError, OSError):
            pass
    return receipt


def _child_main(argv: list[str] | None = None) -> int:
    child_nonce = os.environ.pop("ASHLAR_R18_CHILD_NONCE", "")
    if len(child_nonce) != 64 or any(ch not in "0123456789abcdef" for ch in child_nonce):
        print(json.dumps({"profile": PROFILE, "status": "supervisor_required",
                          "reason": "internal child mode requires a parent watchdog nonce",
                          "native_effects": "none", "effects_may_remain": False},
                         sort_keys=True, separators=(",", ":")), file=sys.stderr)
        return 2
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", required=True, type=Path)
    parser.add_argument("--approval", required=True, type=Path)
    args = parser.parse_args(argv)
    scope = None
    scope_raw = None
    prefix_worker = None
    try:
        scope_path = args.scope
        if (not scope_path.is_absolute() or scope_path.resolve() != scope_path
                or scope_path.is_symlink() or not scope_path.is_file()
                or scope_path.stat().st_size > MAX_SCOPE_BYTES):
            raise ValueError("canonical bounded populated lifecycle-scope file required")
        scope_raw = scope_path.read_bytes()
        scope = json.loads(scope_raw)
        supervisor_path = Path(__file__).resolve()
        if scope.get("supervisor_path") != str(supervisor_path):
            raise ValueError("scope supervisor path must identify this exact CLI file")
        runtime = scope.get("runtime", {})
        selected_python = Path(runtime.get("python_path", ""))
        if (not selected_python.is_absolute() or not selected_python.is_file()
                or Path(sys.executable).resolve() != selected_python.resolve()):
            raise ValueError("invoke with the exact approved Truss .51 Python runtime")
        source = scope.get("source", {})
        source_root = _verify_frozen_source_inputs(source)
        helper_path = Path(source["helper_path"])
        # The verified helper inserts source_root only after checking every
        # selected source file against the frozen manifest.
        directory = Path(__file__).resolve().parent
        prefix_path = directory / "r18_pre_migration_custody_worker.py"
        worker_path = directory / "r18_post_migration_admission.py"
        prefix_worker = _load_module("r18_pre_migration_custody_worker", prefix_path)
        worker = _load_module("r18_post_migration_admission", worker_path)
        result = worker.run_one_start_prefix_and_migration(
            prefix_worker, scope_path, args.approval, worker_path,
            prefix_path, source_root, helper_path, supervisor_path=supervisor_path)
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    except BaseException as error:
        result = _failure_receipt(scope, scope_raw, error, prefix_worker)
        print(json.dumps(result, sort_keys=True, separators=(",", ":")), file=sys.stderr)
        return 2


def _write_new_json(path: Path, value: dict) -> None:
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())


def _validate_child_success(value: object, scope_sha256: str, output_root: Path) -> dict:
    """Require both the child result and durable final-stop evidence."""
    stages = ("migration_invoked", "publication_invoked", "genesis_acknowledged",
              "seed_invoked", "continuation_invoked", "continuation_acknowledged")
    if (not isinstance(value, dict)
            or value.get("profile") != "ashlar-truss-r18-full-native-lifecycle/0.1"
            or value.get("status") != "one_start_full_lifecycle_complete_and_stopped"
            or value.get("scope_sha256") != scope_sha256
            or value.get("single_postgres_start") is not True
            or value.get("postgres_stopped_by_prefix_finally") is not True
            or any(value.get(stage) is not True for stage in stages)):
        raise ValueError("child success result is missing full-lifecycle stage or scope evidence")
    stop_path = output_root / "lifecycle-stop-receipt.json"
    if stop_path.is_symlink() or not stop_path.is_file() or stop_path.stat().st_size > MAX_SCOPE_BYTES:
        raise ValueError("durable lifecycle stop receipt is absent or invalid")
    stop_raw = stop_path.read_bytes()
    stopped = json.loads(stop_raw)
    if (stopped.get("pg_stopped") is not True
            or any(stopped.get(stage) is not True for stage in stages)):
        raise ValueError("durable lifecycle stop receipt does not confirm completed stages and stopped PostgreSQL")
    result_path = output_root / "one-start-migration-readback.json"
    if result_path.is_symlink() or not result_path.is_file() or result_path.stat().st_size > MAX_SCOPE_BYTES:
        raise ValueError("durable full-lifecycle result is absent or invalid")
    durable_result = json.loads(result_path.read_bytes())
    if durable_result != value:
        raise ValueError("child success result differs from durable full-lifecycle result")
    value = dict(value)
    value["lifecycle_stop_receipt_sha256"] = hashlib.sha256(stop_raw).hexdigest()
    return value


def _completed_supervisor_fields(child_result: dict) -> dict:
    """Truthful parent fields after a validated, stopped native lifecycle."""
    return {"status": "child_completed", "child_result": child_result,
            "native_effects": "completed", "native_effects_occurred": True,
            "effects_may_remain": True, "postgres_stopped": True}


def _supervisor_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", required=True, type=Path)
    parser.add_argument("--approval", required=True, type=Path)
    args = parser.parse_args(argv)
    child = None
    scope = None
    scope_raw = None
    worker_hash = None
    output_root = None
    stdout_path = stderr_path = receipt_path = None
    log_root = None
    try:
        scope_path = args.scope
        if (not scope_path.is_absolute() or scope_path.resolve() != scope_path
                or scope_path.is_symlink() or not scope_path.is_file()
                or scope_path.stat().st_size > MAX_SCOPE_BYTES):
            raise ValueError("canonical bounded lifecycle scope required")
        scope_raw = scope_path.read_bytes()
        scope = json.loads(scope_raw)
        supervisor_path = Path(__file__).resolve()
        if scope.get("supervisor_path") != str(supervisor_path):
            raise ValueError("scope must bind this exact supervisor path")
        worker_hash = hashlib.sha256(supervisor_path.read_bytes()).hexdigest()
        if scope.get("workers", {}).get("supervisor_sha256") != worker_hash:
            raise ValueError("scope must bind the exact current supervisor bytes")
        runtime = scope.get("runtime", {})
        python_path = Path(runtime.get("python_path", ""))
        if (not python_path.is_absolute() or not python_path.is_file()
                or python_path.resolve() != Path(sys.executable).resolve()):
            raise ValueError("invoke supervisor with the exact approved Truss .51 Python runtime")
        output_root = Path(scope.get("output_root", ""))
        log_root = Path(scope.get("supervisor_output_directory", ""))
        approval_path = args.approval
        if (not approval_path.is_absolute() or approval_path.resolve() != approval_path
                or approval_path.is_symlink() or not approval_path.is_file()
                or approval_path.stat().st_size > 64 * 1024):
            raise ValueError("canonical bounded owner-approval file required")
        if (not output_root.is_absolute() or output_root.resolve() != output_root
                or not log_root.is_absolute() or log_root.resolve() != log_root
                or log_root == output_root or log_root in output_root.parents
                or output_root in log_root.parents or log_root.is_symlink()
                or log_root.exists()):
            raise ValueError("fresh disjoint canonical supervisor log directory required")
        limits = scope.get("limits", {})
        deadline = limits.get("child_deadline_seconds")
        attempt_cap = limits.get("attempt_root_bytes_max")
        log_cap = limits.get("combined_log_bytes_max")
        rss_cap = limits.get("process_group_rss_bytes_max")
        if (type(deadline) is not int or not 1 <= deadline <= MAX_CHILD_DEADLINE_SECONDS
                or type(attempt_cap) is not int or attempt_cap < 1
                or type(log_cap) is not int or log_cap < 1
                or type(rss_cap) is not int or rss_cap < 1):
            raise ValueError("bounded child deadline, output, log, and RSS limits required")
        state = scope["state_custody"]
        selected_path = Path(state["selected_incomplete_pgdata_path"])
        cold_path = Path(state["cold_backup_path"])
        aggregate_cap = limits.get("aggregate_retained_plus_attempt_bytes_max")
        if type(aggregate_cap) is not int or aggregate_cap < 1:
            raise ValueError("aggregate retained-plus-attempt byte limit required")
        retained_bytes = (_tree_bytes(selected_path, maximum=aggregate_cap)
                          + _tree_bytes(cold_path, maximum=aggregate_cap))
        if retained_bytes > aggregate_cap:
            raise ValueError("retained inputs exceed aggregate byte limit")

        # Confirm process inventory and RSS sampling work before the child can
        # create its attempt directory or start PostgreSQL.
        _rss_bytes_for_group(os.getpgid(os.getpid()))
        log_root.mkdir(mode=0o700, parents=False, exist_ok=False)
        if log_root.stat().st_mode & 0o077:
            raise ValueError("supervisor output directory must be private mode 0700")
        stdout_path = log_root / "child.stdout.log"
        stderr_path = log_root / "child.stderr.log"
        receipt_path = log_root / "supervisor-receipt.json"
        child_stdout = os.open(stdout_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        child_stderr = os.open(stderr_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        child_env = {
            "PATH": f"{python_path.parent}:{runtime['java_home']}/bin:/usr/bin:/bin:/opt/homebrew/bin",
            "HOME": os.environ.get("HOME", "/Users/erik"),
            "JAVA_HOME": runtime["java_home"],
            "PYSPARK_PYTHON": str(python_path),
            "PYSPARK_DRIVER_PYTHON": str(python_path),
            "PYTHONNOUSERSITE": "1", "PYTHONDONTWRITEBYTECODE": "1",
            "TMPDIR": "/private/tmp",
            "ASHLAR_R18_CHILD_NONCE": secrets.token_hex(32),
        }
        child = subprocess.Popen([str(python_path), "-I", "-B", str(supervisor_path),
                "--child", "--scope", str(scope_path), "--approval", str(args.approval)],
            cwd=str(supervisor_path.parent), env=child_env, stdin=subprocess.DEVNULL,
            stdout=child_stdout, stderr=child_stderr, start_new_session=True, close_fds=True)
        os.close(child_stdout); os.close(child_stderr)

        def sample():
            log_bytes = 0
            for path in (stdout_path, stderr_path):
                if path.is_symlink():
                    raise RuntimeError("symlink in supervisor logs")
                if path.exists():
                    log_bytes += path.stat().st_size
            attempt_bytes = _tree_bytes(output_root, maximum=attempt_cap)
            rss_bytes = _rss_bytes_for_group(child.pid)
            result = {"attempt_root_bytes": attempt_bytes,
                      "combined_log_bytes": log_bytes,
                      "process_group_rss_bytes": rss_bytes,
                      "retained_plus_attempt_bytes": retained_bytes + attempt_bytes + log_bytes}
            if log_bytes > log_cap:
                result["limit_exceeded"] = "combined_log_bytes_limit_exceeded"
            elif rss_bytes > rss_cap:
                result["limit_exceeded"] = "process_group_rss_limit_exceeded"
            elif result["retained_plus_attempt_bytes"] > aggregate_cap:
                result["limit_exceeded"] = "aggregate_retained_plus_attempt_bytes_exceeded"
            return result

        previous_handlers = {}
        def interrupt_watchdog(signum, _frame):
            raise KeyboardInterrupt(f"parent watchdog interrupted by signal {signum}")
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                previous_handlers[sig] = signal.signal(sig, interrupt_watchdog)
            monitored = _monitor_child(child, deadline_seconds=deadline, sample=sample)
        except BaseException as error:
            remaining = _terminate_process_group(child)
            monitored = {"cause": f"parent_watchdog_interrupted:{type(error).__name__}: {error}"[:1000],
                "returncode": child.poll(), "child_runtime_seconds": None,
                "process_group_remaining": remaining, "last_live_sample": {}}
        finally:
            for sig, handler in previous_handlers.items():
                signal.signal(sig, handler)
        for fd in (child_stdout, child_stderr):
            try: os.close(fd)
            except OSError: pass
        try:
            final_attempt_bytes = _tree_bytes(output_root, maximum=attempt_cap)
            final_log_bytes = stdout_path.stat().st_size + stderr_path.stat().st_size
            if final_log_bytes > log_cap:
                monitored["cause"] = monitored["cause"] or "combined_log_bytes_limit_exceeded_post_exit"
            if retained_bytes + final_attempt_bytes + final_log_bytes > aggregate_cap:
                monitored["cause"] = monitored["cause"] or "aggregate_retained_plus_attempt_bytes_exceeded_post_exit"
        except BaseException as error:
            final_attempt_bytes = -1
            final_log_bytes = -1
            monitored["cause"] = monitored["cause"] or f"post_exit_resource_check_failed:{type(error).__name__}"
        watchdog_receipt = {"profile": PROFILE,
            "status": ("child_terminated_by_watchdog" if monitored["cause"] else
                       "child_completed" if monitored["returncode"] == 0 else "child_failed"),
            "scope_sha256": hashlib.sha256(scope_raw).hexdigest(),
            "supervisor_sha256": worker_hash, "child": monitored,
            "logs": {"stdout": str(stdout_path), "stderr": str(stderr_path)},
            "attempt_root": str(output_root), "native_effects": "none",
            "effects_may_remain": False, "failure_receipt_persisted": False}
        if monitored["cause"]:
            watchdog_receipt = _watchdog_failure_receipt(
                scope, scope_raw, monitored["cause"], monitored,
                {"stdout": stdout_path, "stderr": stderr_path})
            watchdog_receipt["supervisor_sha256"] = worker_hash
        elif monitored["returncode"] == 0:
            lines = stdout_path.read_text().splitlines()
            try:
                child_result = json.loads(lines[-1]) if len(lines) == 1 else None
                child_result = _validate_child_success(
                    child_result, hashlib.sha256(scope_raw).hexdigest(), output_root)
                watchdog_receipt.update(_completed_supervisor_fields(child_result))
            except BaseException as error:
                watchdog_receipt = _watchdog_failure_receipt(
                    scope, scope_raw,
                    f"child_success_receipt_invalid:{type(error).__name__}: {error}"[:1000],
                    monitored, {"stdout": stdout_path, "stderr": stderr_path})
                watchdog_receipt["supervisor_sha256"] = worker_hash
                watchdog_receipt["status"] = "child_success_receipt_invalid"
                monitored["cause"] = "child_success_receipt_invalid"
        else:
            lines = stderr_path.read_text().splitlines()
            child_failure = json.loads(lines[-1]) if len(lines) == 1 else None
            watchdog_receipt["child_failure"] = child_failure
            watchdog_receipt["native_effects"] = "unknown"
            watchdog_receipt["effects_may_remain"] = True
            if isinstance(child_failure, dict):
                watchdog_receipt["native_effects"] = child_failure.get("native_effects", "unknown")
                watchdog_receipt["effects_may_remain"] = child_failure.get("effects_may_remain", True)
        watchdog_receipt["attempt_root_bytes_final"] = final_attempt_bytes
        watchdog_receipt["combined_log_bytes_final"] = final_log_bytes
        _write_new_json(receipt_path, watchdog_receipt)
        print(json.dumps(watchdog_receipt, sort_keys=True, separators=(",", ":")))
        if monitored["cause"] is not None or monitored["returncode"] != 0:
            return 1
        return 0
    except BaseException as error:
        if child is not None:
            remaining = _terminate_process_group(child)
            if scope is not None and scope_raw is not None and output_root is not None:
                emergency = _watchdog_failure_receipt(
                    scope, scope_raw,
                    f"parent_exception_after_child_spawn:{type(error).__name__}: {error}"[:1000],
                    {"returncode": child.poll(), "process_group_remaining": remaining},
                    {"stdout": stdout_path, "stderr": stderr_path})
                emergency["supervisor_sha256"] = worker_hash
                if receipt_path is not None and not receipt_path.exists():
                    try: _write_new_json(receipt_path, emergency)
                    except OSError: pass
                print(json.dumps(emergency, sort_keys=True, separators=(",", ":")), file=sys.stderr)
                return 1
        print(json.dumps({"profile": PROFILE, "status": "supervisor_admission_failed",
                          "reason": f"{type(error).__name__}: {error}"[:1200],
                          "native_effects": "none", "effects_may_remain": False},
                         sort_keys=True, separators=(",", ":")), file=sys.stderr)
        return 2


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if args and args[0] == "--child":
        return _child_main(args[1:])
    return _supervisor_main(args)


if __name__ == "__main__":
    raise SystemExit(main())
