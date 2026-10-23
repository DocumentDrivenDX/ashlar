#!/usr/bin/env python3
"""One-use, bounded B source install supervisor; current packet is inert."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
WORKER = HERE / "b_install_worker.py"
CONFIG = HERE / "b-install-config.json"
APPROVAL = HERE / "execution-approval.json"
MAX_RESULT = 65_536
MAX_CHILD_SECONDS = 600
MAX_BYTES = 1_073_741_824
MAX_LOG_BYTES = 16 * 1024 * 1024
POLL_SECONDS = 0.25


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def file_sha(path: Path, maximum=1_048_576) -> str:
    if path.is_symlink() or not path.is_file():
        raise ValueError("pinned regular file required: " + str(path))
    raw = path.read_bytes()
    if len(raw) > maximum:
        raise ValueError("pinned file exceeds byte bound")
    return sha(raw)


def large_file_sha(path: Path, maximum=134_217_728) -> str:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > maximum:
        raise ValueError("bounded executable pin required")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def strict_object(raw: bytes) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON member")
            result[key] = value
        return result
    value = json.loads(raw, object_pairs_hook=pairs)
    if type(value) is not dict:
        raise ValueError("JSON object required")
    return value


def write_new(path: Path, value: dict) -> None:
    raw = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        if stream.write(raw) != len(raw):
            raise OSError("incomplete supervisor marker write")
        stream.flush()
        os.fsync(stream.fileno())
    dfd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)


def tree_bytes(root: Path) -> int:
    if not root.exists():
        return 0
    if root.is_symlink():
        raise RuntimeError("symlink at bounded B root")
    total = 0
    for base, dirs, files in os.walk(root, followlinks=False):
        for name in dirs + files:
            item = Path(base) / name
            if item.is_symlink():
                raise RuntimeError("symlink within bounded B root")
            try:
                stat = item.stat()
            except FileNotFoundError:
                continue
            if item.is_file():
                total += stat.st_size
            if total > MAX_BYTES:
                raise RuntimeError("combined B tree byte cap exceeded")
    return total


def check_logs(logs: tuple[Path, ...]) -> None:
    for path in logs:
        if path.is_symlink():
            raise RuntimeError("worker log symlink")
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            continue
        if size > MAX_LOG_BYTES:
            raise RuntimeError("per-log byte cap exceeded: " + path.name)


def process_port_gate(*, run=subprocess.run, socket_root=None) -> dict:
    """Read-only inventory: no PostgreSQL start, connection, or IPC mutation."""
    result = run(["/bin/ps", "-axo", "pid=,command="], stdin=subprocess.DEVNULL,
                 capture_output=True, text=True, timeout=5)
    if result.returncode or len(result.stdout.encode()) > MAX_RESULT:
        raise RuntimeError("bounded process inventory unavailable")
    postgres = []
    own_pid = os.getpid()
    for line in result.stdout.splitlines():
        fields = line.strip().split(None, 1)
        if len(fields) != 2 or not fields[0].isdigit() or int(fields[0]) == own_pid:
            continue
        command = fields[1].lower()
        if "postgres" in command or "postmaster" in command:
            postgres.append(int(fields[0]))
    if socket_root is None:
        socket_root = Path(f"/private/tmp/truss-pg18-{os.getuid()}")
    sockets = []
    if socket_root.exists():
        if socket_root.is_symlink() or not socket_root.is_dir():
            raise RuntimeError("unexpected LocalPostgres socket-root type")
        for base, _, files in os.walk(socket_root, followlinks=False):
            for name in files:
                item = Path(base) / name
                if item.is_symlink():
                    raise RuntimeError("symlink in LocalPostgres socket inventory")
                if name == ".s.PGSQL.5432":
                    sockets.append(str(item))
    return {"status": "ready" if not postgres and not sockets else "occupied",
            "active_postgres_pids": sorted(postgres), "nominal_5432_socket_files": sorted(sockets),
            "process_inventory_bytes": len(result.stdout.encode())}


def admission_digest(gate: dict) -> str:
    stable = {"status": gate.get("status"),
              "active_postgres_pids": gate.get("active_postgres_pids"),
              "nominal_5432_socket_files": gate.get("nominal_5432_socket_files")}
    return sha(json.dumps(stable, sort_keys=True, separators=(",", ":")).encode())


def expected_approval(config: dict, config_raw: bytes, gate_hash: str,
                      worker_preparation: dict) -> dict:
    prep = config["preparation"]
    runtime = config["runtime"]
    source = config["source"]
    origin = config["origin"]
    lock_sha256 = file_sha(Path(config["dependency_lock"]["path"]))
    if lock_sha256 != config["dependency_lock"]["sha256"]:
        raise RuntimeError("selected R4 dependency lock digest mismatch")
    if (large_file_sha(Path(runtime["pg_ctl_path"])) != runtime["pg_ctl_sha256"]
            or large_file_sha(Path(prep["bun_path"])) != prep["bun_sha256"]):
        raise RuntimeError("pinned cleanup/preparation executable digest mismatch")
    return {"profile": "ashlar-r04-b-install-source-execution-approval/0.1",
            "scope": "one B-local Truss install, one exact source acceptance, one genesis consumer; no feed read, destination, Spark or ACK",
            "worker_sha256": file_sha(WORKER),
            "supervisor_sha256": file_sha(Path(__file__)),
            "config_sha256": sha(config_raw),
            "dependency_lock_sha256": lock_sha256,
            "preparation_sha256": file_sha(HERE / prep["assessment_path"]),
            "prepared_input_sha256": file_sha(Path(prep["prepared_input_path"])),
            "prepared_report_sha256": file_sha(Path(prep["prepared_report_path"])),
            "runtime_receipt_sha256": file_sha(Path(runtime["receipt_path"])),
            "catalog_manifest_sha256": runtime["catalog_manifest_sha256"],
            "pg_ctl_path": runtime["pg_ctl_path"],
            "pg_ctl_sha256": runtime["pg_ctl_sha256"],
            "bun_path": prep["bun_path"],
            "bun_sha256": prep["bun_sha256"],
            "source_sha256": file_sha(Path(source["path"])),
            "origin_sha256": file_sha(Path(origin["path"])),
            "admission_gates_sha256": gate_hash}


def worker_approval_payload(config: dict, config_raw: bytes, worker_result: dict,
                            gate_hash: str) -> dict:
    # Supervisor independently constructs the contract, then checks parity
    # against the worker's separately implemented expected_approval object.
    payload = expected_approval(config, config_raw, gate_hash,
                                worker_result["preparation"])
    pins = worker_result["preparation"]
    if (pins.get("pg_ctl_path") != config["runtime"]["pg_ctl_path"]
            or pins.get("pg_ctl_sha256") != config["runtime"]["pg_ctl_sha256"]
            or pins.get("bun_path") != config["preparation"]["bun_path"]
            or pins.get("bun_sha256") != config["preparation"]["bun_sha256"]):
        raise RuntimeError("worker executable pins differ from frozen config")
    if worker_result.get("expected_approval") != payload:
        raise RuntimeError("worker/supervisor approval payload parity mismatch")
    return payload


def preflight(run=subprocess.run, gate_reader=process_port_gate) -> dict:
    config_raw = CONFIG.read_bytes()
    if len(config_raw) > MAX_RESULT or CONFIG.is_symlink():
        raise ValueError("bounded regular B configuration required")
    config = strict_object(config_raw)
    gate = gate_reader(run=run)
    gate_hash = admission_digest(gate)
    python = Path(config["runtime"]["venv"]) / "bin" / "python"
    if not python.is_file():
        raise RuntimeError("pinned Truss interpreter unavailable")
    env = {"PATH": "/usr/bin:/bin", "PYTHONNOUSERSITE": "1",
           "PYTHONDONTWRITEBYTECODE": "1",
           "ASHLAR_B_ADMISSION_GATES_SHA256": gate_hash}
    completed = run([str(python), "-I", "-B", str(WORKER), "--preflight"],
                    cwd=str(HERE), env=env, stdin=subprocess.DEVNULL,
                    capture_output=True, text=True, timeout=120)
    if completed.returncode != 0 or len(completed.stdout.encode()) > MAX_RESULT:
        raise RuntimeError("bounded effect-free B worker preflight failed")
    lines = completed.stdout.splitlines()
    if len(lines) != 1:
        raise RuntimeError("one bounded worker JSON result required")
    worker = strict_object(lines[0].encode())
    if any(worker.get(flag) is not False for flag in
           ("pg_started", "spark_started", "acceptance_invoked", "consumer_registered")):
        raise RuntimeError("worker preflight reported native or source effects")
    expected = worker_approval_payload(config, config_raw, worker, gate_hash)
    readiness = {
        "origin_approved": config["origin"].get("approved") is True,
        "arras_window_released": config["scope"].get("arras_window_released") is True,
        "all_b_execution_pins_set": all(config["installation"].get(k) is not None for k in
            ("root_path", "pgdata_path", "attempt_path", "owner_role", "registrar_role",
             "registrar_login", "reader_login", "consumer_id", "request_key",
             "exclusive_port_admission", "retention_owner")),
        "port_gate_ready": gate.get("status") == "ready",
        "approval_file_present": APPROVAL.is_file() and not APPROVAL.is_symlink(),
    }
    return {"profile": "ashlar-r04-b-supervisor-preflight/0.2",
            "status": "effect_free_preflight_passed", "gate": gate,
            "admission_gates_sha256": gate_hash, "worker": worker,
            "expected_approval": expected, "readiness": readiness,
            "execution_approval": config["execution"].get("approval"),
            "native_effects": 0}


def process_group_members(group_id: int, *, run=subprocess.run) -> list[int]:
    result = run(["/bin/ps", "-axo", "pid=,pgid="], stdin=subprocess.DEVNULL,
                 capture_output=True, text=True, timeout=5)
    if result.returncode or len(result.stdout.encode()) > MAX_RESULT:
        raise RuntimeError("bounded process-group inventory failed")
    members = []
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[0].isdigit() and fields[1].isdigit() and int(fields[1]) == group_id:
            members.append(int(fields[0]))
    return sorted(set(members))


def reap_group(group_id: int, *, members=process_group_members, killpg=os.killpg,
               sleep=time.sleep, clock=time.monotonic) -> list[int]:
    remaining = members(group_id)
    if not remaining:
        return []
    try: killpg(group_id, signal.SIGTERM)
    except ProcessLookupError: return []
    deadline = clock() + 2
    while clock() < deadline:
        remaining = members(group_id)
        if not remaining: return []
        sleep(0.1)
    try: killpg(group_id, signal.SIGKILL)
    except ProcessLookupError: return []
    deadline = clock() + 2
    while clock() < deadline:
        remaining = members(group_id)
        if not remaining: return []
        sleep(0.1)
    return members(group_id)


def supervise_child(command, *, root: Path, logs: tuple[Path, Path],
                    popen=subprocess.Popen, clock=time.monotonic, sleep=time.sleep,
                    killpg=os.killpg, reap=reap_group, deadline=MAX_CHILD_SECONDS,
                    env=None) -> dict:
    for path in logs:
        if path.exists() or path.is_symlink():
            raise RuntimeError("fresh worker logs required")
    fds = [os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600) for p in logs]
    streams = [os.fdopen(fd, "wb", buffering=0) for fd in fds]
    child = None
    started = clock()
    stop_cause = None
    supervisor_error = None
    remaining = []
    try:
        child = popen(command, stdin=subprocess.DEVNULL, stdout=streams[0], stderr=streams[1],
                      cwd=str(root), env=env, start_new_session=True, close_fds=True)
        while child.poll() is None:
            try:
                used = tree_bytes(root)
                check_logs(logs)
            except BaseException as error:
                stop_cause = "bounded_tree_integrity_failure:" + type(error).__name__
                break
            if used > MAX_BYTES:
                stop_cause = "combined_tree_byte_limit_exceeded"
                break
            if clock() - started > deadline:
                stop_cause = "worker_deadline_exceeded"
                break
            sleep(POLL_SECONDS)
    except BaseException as error:
        supervisor_error = type(error).__name__
        stop_cause = stop_cause or "supervisor_exception:" + supervisor_error
    finally:
        code = None
        if child is not None:
            try:
                if child.poll() is None:
                    try: killpg(child.pid, signal.SIGTERM)
                    except ProcessLookupError: pass
                    try: child.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        try: killpg(child.pid, signal.SIGKILL)
                        except ProcessLookupError: pass
                        child.wait(timeout=10)
                code = child.wait()
            except BaseException as error:
                supervisor_error = supervisor_error or type(error).__name__
                stop_cause = stop_cause or "supervisor_reap_exception:" + type(error).__name__
                try: killpg(child.pid, signal.SIGKILL)
                except BaseException: pass
                try: child.wait(timeout=10)
                except BaseException: pass
            try:
                remaining = reap(child.pid, killpg=killpg, sleep=sleep, clock=clock)
            except BaseException as error:
                supervisor_error = supervisor_error or type(error).__name__
                stop_cause = stop_cause or "process_group_reap_exception:" + type(error).__name__
        elapsed = round(clock() - started, 3)
        if elapsed > deadline:
            stop_cause = stop_cause or "worker_deadline_exceeded"
        if remaining:
            stop_cause = stop_cause or "worker_process_group_not_reaped"
        for stream in streams:
            try: stream.flush(); os.fsync(stream.fileno()); stream.close()
            except BaseException as error:
                supervisor_error = supervisor_error or type(error).__name__
                stop_cause = stop_cause or "worker_log_close_exception:" + type(error).__name__
    return {"exit_code": code, "runtime_seconds": elapsed,
            "stop_cause": stop_cause, "supervisor_error": supervisor_error,
            "remaining_process_group": remaining}


def validate_paths(config: dict) -> tuple[Path, Path, Path]:
    install = config["installation"]
    keys = ("root_path", "pgdata_path", "attempt_path")
    if any(type(install.get(k)) is not str or not install[k] for k in keys):
        raise PermissionError("B root, PGDATA and attempt paths are unresolved")
    root, pgdata, attempt = (Path(install[k]) for k in keys)
    if any(not p.is_absolute() or p.resolve() != p or p.is_symlink() for p in (root, pgdata, attempt)):
        raise PermissionError("canonical absolute non-symlink B paths required")
    if (not root.is_dir() or root.stat().st_uid != os.getuid()
            or stat.S_IMODE(root.stat().st_mode) != 0o700):
        raise PermissionError("precreated private mode-0700 B root required")
    if not attempt.is_relative_to(root) or not pgdata.is_relative_to(root) or attempt == pgdata:
        raise PermissionError("attempt and PGDATA must be distinct descendants of B root")
    if attempt.is_relative_to(pgdata) or pgdata.is_relative_to(attempt):
        raise PermissionError("attempt and PGDATA trees must not overlap")
    if (not attempt.is_dir() or attempt.stat().st_uid != os.getuid()
            or stat.S_IMODE(attempt.stat().st_mode) != 0o700 or any(attempt.iterdir())):
        raise PermissionError("fresh empty private attempt directory required")
    if {entry.name for entry in root.iterdir()} != {attempt.name}:
        raise PermissionError("fresh B root may contain only its empty attempt directory")
    if pgdata.exists() or pgdata.is_symlink():
        raise PermissionError("fresh B PGDATA required; retained/existing data refuses retry")
    origin_path = Path(config["origin"]["path"])
    if not origin_path.is_absolute() or origin_path.resolve() != origin_path or origin_path.is_symlink():
        raise PermissionError("external canonical B origin custody path required")
    for input_path in (Path(config["source"]["path"]), origin_path):
        if input_path.is_relative_to(root):
            raise PermissionError("B-owned output root must not contain source/origin custody inputs")
    if tree_bytes(root) > MAX_BYTES:
        raise PermissionError("fresh B root already exceeds its combined byte bound")
    return root, pgdata, attempt


def stop_owned_postgres(pgdata: Path, started_marker: Path, pg_ctl_path: Path,
                        pg_ctl_sha256: str, *, run=subprocess.run) -> str:
    marker = pgdata / "postmaster.pid"
    if not marker.exists():
        return "postmaster_pid_absent"
    if marker.is_symlink() or not started_marker.is_file() or started_marker.is_symlink():
        return "unowned_or_unmarked_postmaster_preserved"
    lines = marker.read_text().splitlines()
    if len(lines) < 2 or Path(lines[1]).resolve() != pgdata.resolve():
        return "postmaster_pid_data_path_mismatch"
    if file_sha(pg_ctl_path, maximum=134_217_728) != pg_ctl_sha256:
        return "pinned_pg_ctl_digest_mismatch"
    result = run([str(pg_ctl_path), "-D", str(pgdata), "-m", "fast", "-w", "-t", "20", "stop"],
                 stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=25,
                 env={"PATH": "/usr/bin:/bin", "HOME": str(Path.home()), "LC_ALL": "C"})
    return "owned_postmaster_stopped_fast" if result.returncode == 0 and not marker.exists() else "owned_postmaster_stop_uncertain"


def execute(*, run=subprocess.run, popen=subprocess.Popen, gate_reader=process_port_gate,
            clock=time.monotonic, sleep=time.sleep, killpg=os.killpg, reap=reap_group) -> dict:
    # Preflight is read-only. It verifies approval parity and resource admission
    # before a marker or child process can exist.
    pre = preflight(run=run, gate_reader=gate_reader)
    config_raw = CONFIG.read_bytes()
    config = strict_object(config_raw)
    root, pgdata, attempt = validate_paths(config)
    if config["execution"].get("attempt_count") != 0 or config["execution"].get("automatic_retries") != 0:
        raise PermissionError("exact one-use/no-retry execution pins required")
    if config["origin"].get("approved") is not True or config["origin"].get("approval_record") is None:
        raise PermissionError("approved origin record required")
    if config["scope"].get("arras_window_released") is not True or config["scope"].get("source_acceptance") is not True:
        raise PermissionError("released Arras slot and source acceptance scope required")
    if pre["gate"].get("status") != "ready":
        raise PermissionError("pre-start LocalPostgres process/socket gate is occupied")
    if config["execution"].get("approval") is not None:
        raise PermissionError("approval must be provided only as exact sidecar")
    expected = pre["expected_approval"]
    approval_raw = APPROVAL.read_bytes() if APPROVAL.is_file() and not APPROVAL.is_symlink() else b""
    if not approval_raw or strict_object(approval_raw) != expected:
        raise PermissionError("exact worker/supervisor/config/preparation/runtime/source/origin approval required")
    started_marker = root / "execution-started-b.json"
    result_path = root / "supervisor-result-b.json"
    if started_marker.exists() or started_marker.is_symlink() or result_path.exists() or result_path.is_symlink():
        raise PermissionError("B started/result marker exists; one-use attempt refuses replay")
    # Repeat the read-only process/socket gate immediately before marker/child.
    live_gate = gate_reader(run=run)
    if admission_digest(live_gate) != pre["admission_gates_sha256"] or live_gate.get("status") != "ready":
        raise PermissionError("live shared LocalPostgres admission changed")
    write_new(started_marker, {"profile": "ashlar-r04-b-execution-started/0.1",
        "approval_sha256": sha(approval_raw), "worker_sha256": expected["worker_sha256"],
        "supervisor_sha256": expected["supervisor_sha256"],
        "admission_gates_sha256": pre["admission_gates_sha256"],
        "retained_pgdata_preexisting": False, "automatic_retries": 0})
    logs = (attempt / "worker.stdout.log", attempt / "worker.stderr.log")
    python = Path(config["runtime"]["venv"]) / "bin" / "python"
    env = {"PATH": str(python.parent) + ":" + str(Path(expected["bun_path"]).parent) + ":/usr/bin:/bin",
           "HOME": str(Path.home()), "PYTHONNOUSERSITE": "1",
           "PYTHONDONTWRITEBYTECODE": "1",
           "ASHLAR_B_ADMISSION_GATES_SHA256": pre["admission_gates_sha256"]}
    command = [str(python), "-I", "-B", str(WORKER), "--execute"]
    try:
        child = supervise_child(command, root=root, logs=logs, popen=popen,
                                clock=clock, sleep=sleep, killpg=killpg, reap=reap, env=env)
    except BaseException as error:
        # The started marker is already durable. Do not retry; still attempt
        # owned-server cleanup and record the exceptional settlement path.
        child = {"exit_code": None, "runtime_seconds": None,
                 "stop_cause": "supervisor_exception:" + type(error).__name__,
                 "supervisor_error": type(error).__name__,
                 "remaining_process_group": []}
    try:
        cleanup = stop_owned_postgres(pgdata, started_marker, Path(expected["pg_ctl_path"]),
                                      expected["pg_ctl_sha256"], run=run)
    except BaseException as error:
        cleanup = "owned_postgres_cleanup_exception:" + type(error).__name__
        child["stop_cause"] = child["stop_cause"] or cleanup
    try:
        post_gate = gate_reader(run=run)
    except BaseException as error:
        post_gate = {"status": "unknown", "error": type(error).__name__}
        child["stop_cause"] = child["stop_cause"] or "post_run_admission_check_exception"
    try:
        total = tree_bytes(root)
        check_logs(logs)
    except BaseException as error:
        total = None
        child["stop_cause"] = child["stop_cause"] or "post_run_bounds_check_exception:" + type(error).__name__
    worker_summary = None
    if child["exit_code"] == 0 and child["stop_cause"] is None:
        try:
            stdout = logs[0].read_bytes()
            lines = stdout.splitlines()
            if len(lines) != 1 or len(lines[0]) > MAX_RESULT:
                child["stop_cause"] = "worker_summary_shape_invalid"
            else:
                worker_summary = strict_object(lines[0])
                if worker_summary.get("status") != "installed_accepted_genesis_registered_stopped":
                    child["stop_cause"] = "worker_summary_outcome_invalid"
        except BaseException as error:
            child["stop_cause"] = "worker_summary_read_exception:" + type(error).__name__
    if cleanup not in ("postmaster_pid_absent", "owned_postmaster_stopped_fast"):
        child["stop_cause"] = child["stop_cause"] or "owned_postgres_cleanup_uncertain"
    if post_gate.get("status") != "ready":
        child["stop_cause"] = child["stop_cause"] or "post-run_shared_localpostgres_admission_occupied"
    if child["exit_code"] != 0:
        child["stop_cause"] = child["stop_cause"] or "worker_exit_nonzero_or_unknown"
    result = {"profile": "ashlar-r04-b-supervisor-result/0.1", **child,
              "worker_summary": worker_summary,
              "cleanup_status": cleanup, "post_run_gate": post_gate,
              "combined_tree_bytes": total,
              "combined_tree_byte_limit": MAX_BYTES, "per_log_byte_limit": MAX_LOG_BYTES,
              "deadline_seconds": MAX_CHILD_SECONDS, "poll_seconds": POLL_SECONDS,
              "automatic_retries": 0, "pgdata_retained": True,
              "feed_reads": 0, "acks": 0}
    write_new(result_path, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if args.preflight:
        print(json.dumps(preflight(), sort_keys=True))
        return 0
    result = execute()
    print(json.dumps(result, sort_keys=True))
    return 0 if result["exit_code"] == 0 and result["stop_cause"] is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
