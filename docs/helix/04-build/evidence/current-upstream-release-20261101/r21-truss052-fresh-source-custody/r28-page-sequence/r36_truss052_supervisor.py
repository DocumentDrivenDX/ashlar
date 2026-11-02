#!/usr/bin/env python3
"""One-attempt bounded supervisor for the local R36 Truss 0.0.52 lifecycle.

This supervisor is an execution guard, not approval. Preflight performs no
native effects. Dispatch requires a separate receipt binding the exact scope,
worker and supervisor bytes; limits and scope are rechecked before launch.
"""
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

PROFILE = "ashlar-r36-truss052-supervisor/0.1"
DISPATCH_PROFILE = "ashlar-r36-truss052-local-dispatch/0.1"
MAX_SCOPE_BYTES = 256 * 1024
MAX_LOG_BYTES = 16 * 1024 * 1024
MAX_ATTEMPT_BYTES = 512 * 1024 * 1024
MAX_RSS_BYTES = 2 * 1024 * 1024 * 1024
MAX_WALL_SECONDS = 900
# Reserve bounded worker/process and cluster teardown inside the 900s total cap.
WORKER_DEADLINE_SECONDS = MAX_WALL_SECONDS - 45
POST_WORKER_PROCESS_DRAIN_GRACE_SECONDS = 10.0
MAX_TREE_ENTRIES = 200_000
MAX_PROCESSES = 512


class SupervisorRefusal(RuntimeError):
    pass


def _need(value: bool, message: str) -> None:
    if not value:
        raise SupervisorRefusal(message)


def _create_private_child_logs(supervisor_root: Path) -> tuple[int, int]:
    """Create fresh, owner-only logs independent of the caller's umask."""
    descriptors = []
    try:
        for name in ("child.stdout.log", "child.stderr.log"):
            path = supervisor_root / name
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            descriptors.append(descriptor)
            os.fchmod(descriptor, 0o600)
            info = os.fstat(descriptor)
            _need(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                  and (info.st_mode & 0o777) == 0o600,
                  "R36 child logs must be owner-only regular files")
        return descriptors[0], descriptors[1]
    except BaseException:
        for descriptor in descriptors:
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise


def _strict_json(raw: bytes, label: str) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise SupervisorRefusal(f"duplicate {label} member")
            result[key] = value
        return result

    def constant(_value):
        raise SupervisorRefusal(f"nonfinite {label} value")

    try:
        value = json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as error:
        raise SupervisorRefusal(f"malformed {label}") from error
    _need(type(value) is dict, f"exact {label} object required")
    return value


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _canonical_path(value: object, label: str, *, must_exist: bool) -> Path:
    _need(type(value) is str and value.startswith("/"), f"absolute {label} required")
    path = Path(value)
    _need(not path.is_symlink(), f"non-symlink {label} required")
    if must_exist:
        _need(path.exists() and path.resolve(strict=True) == path, f"canonical existing {label} required")
    else:
        _need(not path.exists() and path.parent.exists()
              and path.parent.resolve(strict=True) == path.parent,
              f"canonical absent {label} with existing parent required")
    _need(all(not parent.is_symlink() for parent in (path.parent, *path.parent.parents)),
          f"{label} ancestors must not be symlinks")
    return path


def _selected_python(value: object) -> Path:
    """Admit the exact selected venv entrypoint, including its normal symlink."""
    _need(type(value) is str and value.startswith("/"),
          "absolute selected runtime Python path required")
    path = Path(value)
    _need(path.is_file() and os.access(path, os.X_OK)
          and all(not parent.is_symlink() for parent in (path.parent, *path.parent.parents)),
          "selected runtime Python executable and canonical parent chain required")
    target = path.resolve(strict=True)
    _need(target.is_file() and os.access(target, os.X_OK) and not target.is_symlink(),
          "selected runtime Python symlink must resolve to an executable file")
    return path


def _scope_admit(scope_path: Path, worker_argument: Path,
                 supervisor_path: Path) -> tuple[dict, bytes, Path, Path, Path, Path]:
    _need(scope_path.is_absolute() and scope_path.is_file() and not scope_path.is_symlink()
          and scope_path.resolve(strict=True) == scope_path
          and scope_path.stat().st_size <= MAX_SCOPE_BYTES,
          "canonical bounded R36 scope file required")
    scope_raw = scope_path.read_bytes()
    scope = _strict_json(scope_raw, "R36 scope")
    _need(scope.get("profile") == "ashlar-r36-truss052-fresh-source-custody-lifecycle/0.1"
          and scope.get("authorization") == "standing_user_local_release_authorization"
          and scope.get("cloud") is False and scope.get("production_authority") is False,
          "exact local-development R36 scope required")
    limits = scope.get("limits")
    _need(type(limits) is dict
          and limits.get("wall_seconds") == MAX_WALL_SECONDS
          and limits.get("process_rss_bytes") == MAX_RSS_BYTES
          and limits.get("attempt_root_bytes") == MAX_ATTEMPT_BYTES
          and limits.get("attempts") == 1
          and limits.get("automatic_retries") == 0
          and limits.get("cloud") is False
          and limits.get("network_listen") is False
          and limits.get("cleanup") is False
          and limits.get("parallelism") == "sequential local only",
          "R36 scope limits differ from the one-attempt local supervisor")
    paths = scope.get("paths")
    _need(type(paths) is dict and paths.get("must_be_absent_before_native_run") is True,
          "R36 fresh output path gate required")
    output_root = _canonical_path(paths.get("output_root"), "attempt root", must_exist=False)
    supervisor_root = _canonical_path(paths.get("supervisor_root"), "supervisor root", must_exist=False)
    data_directory = Path(paths.get("data_directory", ""))
    _need(data_directory == output_root / "pgdata",
          "R36 PostgreSQL data directory must be inside the bounded attempt root")
    _need(supervisor_root != output_root and output_root not in supervisor_root.parents
          and supervisor_root not in output_root.parents,
          "R36 attempt and supervisor roots must be distinct")

    runtime = scope.get("runtime_candidate")
    _need(type(runtime) is dict and runtime.get("version") == "0.0.52"
          and runtime.get("distribution") == "umf-truss"
          and runtime.get("release_state") == "published GitHub asset; wheel SHA-256 verified against public release readback"
          and runtime.get("wheel_sha256") == "00d8b11abc83d9c84fa131ae6f3eef23ecad9d321d4b04a13641ed31c77e9747",
          "Exact published Truss 0.0.52 wheel runtime candidate required")
    python = _selected_python(runtime.get("local_runtime_base_python"))

    worker_entry = scope.get("worker")
    _need(type(worker_entry) is dict and set(worker_entry) == {"path", "sha256"}
          and type(worker_entry.get("sha256")) is str
          and len(worker_entry["sha256"]) == 64,
          "scope-bound R36 worker path and SHA-256 required")
    worker = _canonical_path(worker_entry.get("path"), "R36 worker", must_exist=True)
    _need(worker == worker_argument and worker.is_file()
          and _sha(worker.read_bytes()) == worker_entry["sha256"],
          "CLI worker differs from the exact scope-bound worker bytes")
    supervisor_entry = scope.get("supervisor")
    _need(type(supervisor_entry) is dict and set(supervisor_entry) == {"path", "sha256"}
          and supervisor_entry.get("path") == str(supervisor_path)
          and supervisor_entry.get("sha256") == _sha(supervisor_path.read_bytes()),
          "R36 scope does not bind this exact supervisor file")
    return scope, scope_raw, worker, python, output_root, supervisor_root


def _dispatch_admit(dispatch_path: Path, *, scope_raw: bytes, worker: Path,
                    supervisor_path: Path, scope: dict) -> dict:
    _need(scope.get("status") == "sol_reviewed_and_dispatchable",
          "R36 scope is not marked dispatchable after Sol review")
    _need(dispatch_path.is_absolute() and dispatch_path.is_file()
          and not dispatch_path.is_symlink()
          and dispatch_path.resolve(strict=True) == dispatch_path
          and dispatch_path.stat().st_size <= 64 * 1024,
          "canonical bounded one-run dispatch receipt required")
    _need(all(not parent.is_symlink() for parent in (dispatch_path.parent,
              *dispatch_path.parent.parents)),
          "dispatch receipt ancestors must not be symlinks")
    receipt = _strict_json(dispatch_path.read_bytes(), "dispatch receipt")
    expected = {
        "profile": DISPATCH_PROFILE,
        "status": "sol_reviewed_and_dispatchable",
        "scope_sha256": _sha(scope_raw),
        "worker_sha256": _sha(worker.read_bytes()),
        "supervisor_sha256": _sha(supervisor_path.read_bytes()),
    }
    _need(receipt == expected, "dispatch receipt does not bind exact reviewed R36 inputs")
    return receipt


def _structural_worker_admit(scope_path: Path, worker: Path, python: Path,
                             output_root: Path, supervisor_root: Path,
                             scope_raw: bytes) -> dict:
    """Run the exact worker's deterministic native setup before any output effect."""
    _need(not output_root.exists() and not supervisor_root.exists(),
          "R24 structural admission requires both attempt roots absent")
    command = [str(python), str(worker), "--scope", str(scope_path),
               "--structural-preflight-only"]
    result = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, check=False, timeout=60,
        env={**os.environ, "ASHLAR_R36_CLOUD": "0"})
    _need(result.returncode == 0,
          "R24 worker structural preflight refused: " + result.stderr.decode("utf-8", "replace")[:1200])
    receipt = _strict_json(result.stdout, "R24 worker structural preflight")
    _need(receipt.get("status") == "passed_effect_free_native_structural_preflight"
          and receipt.get("native_effects") is False
          and receipt.get("database_opened") is False
          and receipt.get("spark_started") is False
          and receipt.get("delta_written") is False
          and receipt.get("source_acknowledgement") is False
          and receipt.get("cloud") is False,
          "R24 worker structural preflight did not prove its effect-free result")
    _need(not output_root.exists() and not supervisor_root.exists(),
          "R24 worker structural preflight created an attempt root")
    return receipt


def _process_inventory() -> dict[int, dict]:
    """Return one bounded process snapshot with parentage and start identity."""
    result = subprocess.run(
        ["/bin/ps", "-axo", "pid=,ppid=,pgid=,rss=,lstart=,command="],
        check=True, capture_output=True, text=True, timeout=2)
    processes = {}
    for line in result.stdout.splitlines():
        fields = line.split(None, 9)
        if len(fields) != 10:
            continue
        try:
            pid, ppid, pgid, rss_kib = map(int, fields[:4])
        except ValueError:
            continue
        processes[pid] = {"ppid": ppid, "pgid": pgid, "rss": rss_kib * 1024,
                          "started": " ".join(fields[4:9]), "command": fields[9]}
    _need(len(processes) <= 20_000, "bounded host process inventory required")
    return processes


def _descendants(root_pid: int, processes: dict[int, dict]) -> list[int]:
    """Return the observed root and all descendants from one process snapshot."""
    selected = {root_pid} if root_pid in processes else set()
    changed = True
    while changed:
        changed = False
        for pid, process in processes.items():
            if pid not in selected and process["ppid"] in selected:
                selected.add(pid)
                changed = True
                _need(len(selected) <= MAX_PROCESSES,
                      "bounded PostgreSQL postmaster process tree required")
    return sorted(selected)


def _process_holds_pgdata(pid: int, data_directory: Path) -> bool:
    result = subprocess.run(["/usr/sbin/lsof", "-nP", "-a", "-p", str(pid), "-F", "n"],
                            check=False, capture_output=True, text=True, timeout=2)
    _need(result.returncode in (0, 1), "postmaster PGDATA open-file witness unavailable")
    expected_prefix = str(data_directory) + "/"
    return any(line.startswith("n") and line[1:].startswith(expected_prefix)
               for line in result.stdout.splitlines())


POSTMASTER_PIDFILE_STARTUP_GRACE_SECONDS = 10.0


def _postmaster_pid(data_directory: Path, processes: dict[int, dict], *,
                    allow_startup_partial: bool = False,
                    startup_state: dict | None = None,
                    now: float | None = None) -> int | None:
    """Prove postmaster.pid names a live postgres process holding this PGDATA."""
    _need(data_directory.is_absolute() and not data_directory.is_symlink()
          and all(not parent.is_symlink()
                  for parent in (data_directory.parent, *data_directory.parent.parents)),
          "canonical non-symlink owned R36 PGDATA required")
    if not data_directory.exists():
        return None
    _need(data_directory.is_dir() and data_directory.resolve(strict=True) == data_directory,
          "canonical existing R36 PGDATA directory required")
    pid_file = data_directory / "postmaster.pid"
    if not pid_file.exists():
        _need(not pid_file.is_symlink(), "R36 postmaster.pid cannot be a symlink")
        return None
    _need(pid_file.is_file() and not pid_file.is_symlink()
          and pid_file.stat().st_size <= 4096,
          "bounded regular R36 postmaster.pid required")
    raw_lines = pid_file.read_text(encoding="utf-8").splitlines(keepends=True)
    lines = [line.rstrip("\r\n") for line in raw_lines]
    expected_path = str(data_directory)

    def startup_pending(reason: str) -> None:
        state = startup_state if startup_state is not None else {}
        current = time.monotonic() if now is None else now
        first_seen = state.setdefault("partial_pidfile_first_seen", current)
        _need(current - first_seen <= POSTMASTER_PIDFILE_STARTUP_GRACE_SECONDS,
              f"{reason} exceeded bounded startup grace")

    if len(lines) >= 2 and lines[0].isdecimal():
        observed_path = lines[1]
        path_line_complete = raw_lines[1].endswith(("\n", "\r"))
        _need((not path_line_complete and expected_path.startswith(observed_path))
              or observed_path == expected_path,
              "postmaster.pid must bind exact owned R36 PGDATA")
        if not path_line_complete:
            if allow_startup_partial:
                startup_pending("partial postmaster.pid path line")
                return None
            raise SupervisorRefusal("postmaster.pid path line is incomplete during strict verification")

    incomplete = not lines or not lines[0].isdecimal() or len(lines) < 4
    if incomplete and allow_startup_partial:
        startup_pending("partial postmaster.pid")
        return None
    _need(not incomplete and len(lines) >= 4 and lines[1] == expected_path,
          "postmaster.pid must bind exact owned R36 PGDATA")

    pid = int(lines[0])
    if pid <= 1:
        _need(False, "postmaster.pid PID must identify a live PostgreSQL server process")
    if pid not in processes:
        if allow_startup_partial:
            startup_pending("postmaster process inventory lag")
            return None
        _need(False, "R36 postmaster PID must be live and inventoried")
    if "postgres" not in processes[pid]["command"].lower():
        if allow_startup_partial:
            startup_pending("postmaster command identity lag")
            return None
        _need(False, "postmaster.pid PID must identify a PostgreSQL server process")
    if not _process_holds_pgdata(pid, data_directory):
        if allow_startup_partial:
            startup_pending("postmaster PGDATA open-file witness lag")
            return None
        _need(False, "postmaster PID does not hold files under the exact R36 PGDATA")
    if startup_state is not None:
        startup_state.pop("partial_pidfile_first_seen", None)
    return pid


def _process_group_snapshot(group_id: int, data_directory: Path, *,
                            allow_startup_partial: bool = False,
                            startup_state: dict | None = None) -> tuple[list[int], int, int | None]:
    processes = _process_inventory()
    selected = {pid for pid, process in processes.items() if process["pgid"] == group_id}
    postmaster_pid = _postmaster_pid(data_directory, processes,
        allow_startup_partial=allow_startup_partial, startup_state=startup_state)
    if postmaster_pid is not None:
        selected.update(_descendants(postmaster_pid, processes))
    _need(len(selected) <= MAX_PROCESSES,
          "bounded supervised process and PostgreSQL trees required")
    rss_bytes = sum(processes[pid]["rss"] for pid in selected)
    return sorted(selected), rss_bytes, postmaster_pid


def _wait_for_process_group_drain(initial_snapshot, take_snapshot, *,
                                  grace_seconds: float = POST_WORKER_PROCESS_DRAIN_GRACE_SECONDS,
                                  poll_interval: float = 0.2,
                                  monotonic=time.monotonic,
                                  sleep=time.sleep):
    """Allow already-exiting worker descendants a short, bounded natural drain."""
    _need(0 < grace_seconds <= POST_WORKER_PROCESS_DRAIN_GRACE_SECONDS
          and 0 < poll_interval <= 1.0,
          "bounded post-worker process-drain limits required")
    snapshot = initial_snapshot
    deadline = monotonic() + grace_seconds
    while snapshot[0]:
        remaining = deadline - monotonic()
        if remaining <= 0:
            break
        sleep(min(poll_interval, remaining))
        snapshot = take_snapshot()
    return snapshot


def _group_pids(group_id: int) -> list[int]:
    processes = _process_inventory()
    pids = sorted(pid for pid, process in processes.items() if process["pgid"] == group_id)
    _need(len(pids) <= MAX_PROCESSES, "bounded supervised process group required")
    return pids


def _check_external_listeners(pids: list[int]) -> None:
    if not pids:
        return
    command = ["/usr/sbin/lsof", "-nP", "-a", "-p", ",".join(map(str, pids)),
               "-iTCP", "-sTCP:LISTEN"]
    result = subprocess.run(command, check=False, capture_output=True, text=True, timeout=10)
    _need(result.returncode in (0, 1), "local listener inventory command failed")
    for line in result.stdout.splitlines():
        if "TCP " not in line or "(LISTEN)" not in line:
            continue
        endpoint = line.split("TCP ", 1)[1].rsplit(" (LISTEN)", 1)[0]
        address = endpoint.rsplit(":", 1)[0].strip("[]")
        if address not in ("127.0.0.1", "::1", "localhost"):
            raise SupervisorRefusal("externally reachable TCP listener refused: " + endpoint)


def _tree_bytes(root: Path) -> int:
    if not root.exists():
        return 0
    _need(root.is_dir() and not root.is_symlink(), "attempt root must remain a non-symlink directory")
    total = 0
    entries_seen = 0
    pending = [root]
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                entries_seen += 1
                _need(entries_seen <= MAX_TREE_ENTRIES,
                      "bounded R36 attempt tree entry inventory required")
                _need(not entry.is_symlink(), "symlink in R36 attempt tree refused")
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    total += entry.stat(follow_symlinks=False).st_size
                    _need(total <= MAX_ATTEMPT_BYTES,
                          "R36 attempt root exceeded 512 MiB")
                else:
                    raise SupervisorRefusal("special file in R36 attempt tree refused")
    return total


def _write_json_exclusive(path: Path, value: dict) -> None:
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def _terminate_group(child: subprocess.Popen, grace: float = 2.0) -> list[int]:
    group_id = child.pid
    try:
        pids = _group_pids(group_id)
    except BaseException:
        pids = [child.pid]
    if pids:
        try:
            os.killpg(group_id, signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline:
            child.poll()
            try:
                pids = _group_pids(group_id)
            except BaseException:
                pids = [child.pid] if child.poll() is None else []
            if not pids:
                break
            time.sleep(0.1)
        if pids:
            try:
                os.killpg(group_id, signal.SIGKILL)
            except ProcessLookupError:
                pass
    try:
        child.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(group_id, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
    try:
        return _group_pids(group_id)
    except BaseException:
        return [-1]


def _stop_owned_postmaster(data_directory: Path, grace: float = 15.0) -> dict:
    """Stop only the live postmaster proven to own the selected fresh PGDATA."""
    processes = _process_inventory()
    pid = _postmaster_pid(data_directory, processes)
    if pid is None:
        return {"status": "absent", "postmaster_pid": None, "remaining": []}
    owned = _descendants(pid, processes)
    identities = {process_pid: processes[process_pid]["started"] for process_pid in owned}
    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + grace
    remaining = []
    while time.monotonic() < deadline:
        current = _process_inventory()
        remaining = [process_pid for process_pid, started in identities.items()
                     if process_pid in current and current[process_pid]["started"] == started]
        if not remaining:
            return {"status": "stopped_gracefully", "postmaster_pid": pid,
                    "process_tree": owned, "remaining": []}
        time.sleep(0.2)

    # Revalidate the exact PID file and open-file witness before a bounded hard
    # stop. Descendants are signalled only when their PID and start identity
    # still match the tree observed beneath this verified postmaster.
    current = _process_inventory()
    current_pid = _postmaster_pid(data_directory, current)
    if current_pid == pid:
        if current[pid]["started"] != identities[pid]:
            raise SupervisorRefusal("R36 postmaster PID was reused during owned-cluster cleanup")
        for process_pid in reversed(_descendants(pid, current)):
            if (process_pid in identities
                    and current[process_pid]["started"] == identities[process_pid]):
                latest = _process_inventory().get(process_pid)
                if (latest is not None and latest["started"] == identities[process_pid]
                        and "postgres" in latest["command"].lower()
                        and _process_holds_pgdata(process_pid, data_directory)):
                    os.kill(process_pid, signal.SIGKILL)
    else:
        for process_pid in remaining:
            current_process = _process_inventory().get(process_pid)
            if (current_process is not None
                    and current_process["started"] == identities[process_pid]
                    and "postgres" in current_process["command"].lower()
                    and _process_holds_pgdata(process_pid, data_directory)):
                os.kill(process_pid, signal.SIGKILL)
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        current = _process_inventory()
        remaining = [process_pid for process_pid, started in identities.items()
                     if process_pid in current and current[process_pid]["started"] == started]
        if not remaining:
            return {"status": "stopped_after_bounded_force", "postmaster_pid": pid,
                    "process_tree": owned, "remaining": []}
        time.sleep(0.1)
    return {"status": "stop_unconfirmed", "postmaster_pid": pid,
            "process_tree": owned, "remaining": remaining}


def _dispatch(scope: dict, scope_raw: bytes, worker: Path, python: Path,
              output_root: Path, data_directory: Path, supervisor_root: Path,
              supervisor_path: Path, scope_path: Path) -> int:
    _need(not output_root.exists() and not supervisor_root.exists(),
          "R36 output roots must both be absent before the one attempt")
    supervisor_root.mkdir(mode=0o700)
    _need(supervisor_root.stat().st_mode & 0o077 == 0,
          "private R36 supervisor output directory required")
    stdout_path = supervisor_root / "child.stdout.log"
    stderr_path = supervisor_root / "child.stderr.log"
    started = time.monotonic()
    status = "child_failed"
    cause = None
    returncode = None
    peak_rss = 0
    peak_attempt_bytes = 0
    samples = 0
    remaining = []
    peak_postmaster_pid = None
    postmaster_startup_state = {}
    postgres_cleanup = {"status": "not_checked", "postmaster_pid": None,
                        "process_tree": [], "remaining": []}
    clean_exit = False
    env = dict(os.environ)
    for key in tuple(env):
        if key.startswith(("AWS_", "AZURE_", "DATABRICKS_", "GOOGLE_", "GCP_")):
            env.pop(key, None)
    env.update({"ASHLAR_R36_SUPERVISED": "1",
                "ASHLAR_R36_SCOPE_SHA256": _sha(scope_raw),
                "ASHLAR_R36_ATTEMPT_ROOT": str(output_root),
                "ASHLAR_R36_SUPERVISOR_ROOT": str(supervisor_root),
                "ASHLAR_R36_CLOUD": "0"})
    command = [str(python), str(worker), "--scope", str(scope_path),
               "--attempt-root", str(output_root), "--supervisor-root", str(supervisor_root)]
    try:
        stdout_fd, stderr_fd = _create_private_child_logs(supervisor_root)
        with os.fdopen(stdout_fd, "wb") as stdout, os.fdopen(stderr_fd, "wb") as stderr:
            child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=stdout,
                stderr=stderr, env=env, start_new_session=True, close_fds=True)
            while child.poll() is None:
                elapsed = time.monotonic() - started
                if elapsed >= WORKER_DEADLINE_SECONDS:
                    cause = "855-second worker deadline reached to reserve teardown within 900s total"
                    break
                pids, rss, postmaster_pid = _process_group_snapshot(child.pid, data_directory,
                    allow_startup_partial=True, startup_state=postmaster_startup_state)
                if postmaster_pid is not None:
                    peak_postmaster_pid = postmaster_pid
                peak_rss = max(peak_rss, rss)
                attempt_bytes = _tree_bytes(output_root)
                peak_attempt_bytes = max(peak_attempt_bytes, attempt_bytes)
                log_bytes = stdout_path.stat().st_size + stderr_path.stat().st_size
                _need(log_bytes <= MAX_LOG_BYTES,
                      "combined R36 worker logs exceeded 16 MiB")
                _check_external_listeners(pids)
                samples += 1
                if rss > MAX_RSS_BYTES:
                    cause = "2 GiB worker-plus-owned-PostgreSQL RSS cap exceeded"
                    break
                time.sleep(min(1.0, max(0.0, WORKER_DEADLINE_SECONDS - elapsed)))
            if cause is not None:
                remaining = _terminate_group(child)
            else:
                returncode = child.wait()
                try:
                    pids, rss, postmaster_pid = _process_group_snapshot(child.pid, data_directory)
                    if postmaster_pid is not None:
                        peak_postmaster_pid = postmaster_pid
                    if pids and returncode == 0:
                        def drain_snapshot():
                            nonlocal peak_rss, peak_attempt_bytes, peak_postmaster_pid
                            current = _process_group_snapshot(child.pid, data_directory)
                            current_pids, current_rss, current_postmaster = current
                            peak_rss = max(peak_rss, current_rss)
                            if current_postmaster is not None:
                                peak_postmaster_pid = current_postmaster
                            _need(current_rss <= MAX_RSS_BYTES,
                                  "2 GiB worker-plus-owned-PostgreSQL RSS cap exceeded during natural drain")
                            current_bytes = _tree_bytes(output_root)
                            peak_attempt_bytes = max(peak_attempt_bytes, current_bytes)
                            _need(current_bytes <= MAX_ATTEMPT_BYTES,
                                  "R36 attempt-root cap exceeded during natural process drain")
                            current_logs = stdout_path.stat().st_size + stderr_path.stat().st_size
                            _need(current_logs <= MAX_LOG_BYTES,
                                  "combined R36 worker logs exceeded 16 MiB during natural drain")
                            _check_external_listeners(current_pids)
                            return current

                        pids, rss, postmaster_pid = _wait_for_process_group_drain(
                            (pids, rss, postmaster_pid), drain_snapshot)
                        if postmaster_pid is not None:
                            peak_postmaster_pid = postmaster_pid
                    if pids:
                        cause = "worker exited with a supervised process or owned database still alive"
                        remaining = _terminate_group(child)
                    peak_rss = max(peak_rss, rss)
                except BaseException as error:
                    cause = f"final process-group inventory failed: {type(error).__name__}: {error}"
                    remaining = _terminate_group(child)
            if cause is None:
                returncode = child.returncode
                clean_exit = not remaining
                status = "succeeded" if returncode == 0 and clean_exit else "child_failed"
                if returncode != 0:
                    cause = f"worker exited {returncode}"
                elif remaining:
                    cause = "supervised worker process group did not drain"
            else:
                status = "supervisor_stopped_child"
    except BaseException as error:
        if cause is None:
            cause = f"{type(error).__name__}: {error}"[:1500]
        status = "supervisor_failed_or_stopped"
        try:
            if "child" in locals() and child.poll() is None:
                remaining = _terminate_group(child)
                returncode = child.returncode
        except BaseException:
            remaining = [-1]

    # LocalPostgres launches its postmaster outside the worker's process group.
    # Stop only a PID proved by postmaster.pid, host process inventory, and an
    # open file under this exact fresh PGDATA. Never remove PGDATA or receipts.
    try:
        pre_cleanup_processes = _process_inventory()
        live_postmaster = _postmaster_pid(data_directory, pre_cleanup_processes)
        if live_postmaster is not None:
            peak_postmaster_pid = live_postmaster
            if cause is None:
                cause = "worker left its owned PostgreSQL cluster running after exit"
                status = "child_failed"
            postgres_cleanup = _stop_owned_postmaster(data_directory)
            if postgres_cleanup["status"] == "stop_unconfirmed":
                cause = (cause or "") + "; owned PostgreSQL postmaster stop is unconfirmed"
                status = "supervisor_failed_or_stopped"
        else:
            postgres_cleanup = {"status": "absent", "postmaster_pid": None,
                                "process_tree": [], "remaining": []}
    except BaseException as error:
        postgres_cleanup = {"status": "ownership_or_stop_unverified",
                            "error": f"{type(error).__name__}: {error}"[:1000],
                            "postmaster_pid": peak_postmaster_pid,
                            "remaining": []}
        cause = (cause or "") + "; owned PostgreSQL cleanup could not be verified"
        status = "supervisor_failed_or_stopped"

    ended = time.monotonic()
    if ended - started > MAX_WALL_SECONDS and cause is None:
        cause = "900-second hard wall deadline including teardown exceeded"
        status = "supervisor_failed_or_stopped"
    try:
        final_attempt_bytes = _tree_bytes(output_root)
        peak_attempt_bytes = max(peak_attempt_bytes, final_attempt_bytes)
    except BaseException:
        final_attempt_bytes = None
        if cause is None:
            cause = "final attempt-root inventory failed or exceeded its cap"
            status = "supervisor_failed_or_stopped"
    try:
        final_logs = stdout_path.stat().st_size + stderr_path.stat().st_size
    except OSError:
        final_logs = None
        if cause is None:
            cause = "final worker-log inventory failed"
            status = "supervisor_failed_or_stopped"
    if final_logs is not None and final_logs > MAX_LOG_BYTES and cause is None:
        cause = "combined R36 worker logs exceeded 16 MiB"
        status = "supervisor_failed_or_stopped"
    receipt = {"profile": PROFILE, "status": status, "attempts": 1,
        "automatic_retries": 0, "scope_sha256": _sha(scope_raw),
        "worker_sha256": _sha(worker.read_bytes()),
        "supervisor_sha256": _sha(supervisor_path.read_bytes()),
        "wall_seconds": round(ended - started, 3), "wall_limit_seconds": MAX_WALL_SECONDS,
        "worker_deadline_seconds": WORKER_DEADLINE_SECONDS,
        "peak_supervised_process_rss_bytes": peak_rss, "rss_limit_bytes": MAX_RSS_BYTES,
        "postgres_postmaster_pid_observed": peak_postmaster_pid,
        "postgres_cluster_cleanup": postgres_cleanup,
        "attempt_root_bytes_peak": peak_attempt_bytes,
        "attempt_root_bytes_final": final_attempt_bytes,
        "attempt_root_limit_bytes": MAX_ATTEMPT_BYTES,
        "combined_log_bytes_final": final_logs, "combined_log_limit_bytes": MAX_LOG_BYTES,
        "process_group_remaining": remaining, "returncode": returncode,
        "native_effects": "local-only; inspect worker receipt for phase",
        "effects_may_remain": (status != "succeeded"),
        "failure_reason": cause,
        "logs": {"stdout": str(stdout_path), "stderr": str(stderr_path)}}
    _write_json_exclusive(supervisor_root / "supervisor-receipt.json", receipt)
    print(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
    return 0 if status == "succeeded" else 1


def _log_self_test(scope: dict, scope_raw: bytes, worker: Path, python: Path,
                   output_root: Path, supervisor_path: Path, scope_path: Path,
                   test_root: Path) -> int:
    """Exercise secure log creation and the worker's real supervisor handshake only."""
    _need(test_root.is_absolute() and not test_root.exists() and not test_root.is_symlink()
          and test_root.parent.exists() and test_root.parent.resolve() == test_root.parent,
          "fresh canonical R22 log self-test root required")
    _need(test_root != output_root and output_root not in test_root.parents
          and test_root not in output_root.parents,
          "R22 log self-test root must be separate from attempt output")
    _need(not output_root.exists(), "R22 output root must remain absent during log self-test")
    previous_umask = os.umask(0)
    try:
        test_root.mkdir(mode=0o700)
        os.chmod(test_root, 0o700)
        _need(test_root.stat().st_uid == os.getuid()
              and (test_root.stat().st_mode & 0o777) == 0o700,
              "R22 log self-test root must be owned and private")
        stdout_fd, stderr_fd = _create_private_child_logs(test_root)
    finally:
        os.umask(previous_umask)
    env = dict(os.environ)
    for key in tuple(env):
        if key.startswith(("AWS_", "AZURE_", "DATABRICKS_", "GOOGLE_", "GCP_")):
            env.pop(key, None)
    env.update({"ASHLAR_R36_SUPERVISED": "1",
        "ASHLAR_R36_SCOPE_SHA256": _sha(scope_raw),
        "ASHLAR_R36_ATTEMPT_ROOT": str(output_root),
        "ASHLAR_R36_SUPERVISOR_ROOT": str(test_root),
        "ASHLAR_R36_CLOUD": "0", "ASHLAR_R36_HANDSHAKE_TEST": "1"})
    command = [str(python), str(worker), "--scope", str(scope_path),
        "--attempt-root", str(output_root), "--supervisor-root", str(test_root),
        "--supervisor-handshake-only"]
    with os.fdopen(stdout_fd, "wb") as stdout, os.fdopen(stderr_fd, "wb") as stderr:
        child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=stdout,
            stderr=stderr, env=env, start_new_session=True, close_fds=True)
        try:
            returncode = child.wait(timeout=60)
        except subprocess.TimeoutExpired:
            child.terminate()
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)
            raise SupervisorRefusal("R22 effect-free worker handshake exceeded 60 seconds")
    stdout_path = test_root / "child.stdout.log"
    child_result = _strict_json(stdout_path.read_bytes(), "R22 worker handshake result")
    _need(child_result.get("status") == "passed_effect_free_supervisor_handshake"
          and all(child_result.get(key) is False for key in
                  ("native_effects", "database_opened", "spark_started", "delta_written",
                   "source_acknowledgement", "cloud")),
          "R22 worker handshake did not return exact effect-free success")
    logs = {}
    for name in ("child.stdout.log", "child.stderr.log"):
        path = test_root / name
        info = path.lstat()
        _need(path.is_file() and not path.is_symlink() and info.st_uid == os.getuid()
              and (info.st_mode & 0o777) == 0o600,
              "R22 secure child log postcondition failed")
        logs[name] = {"sha256": _sha(path.read_bytes()), "bytes": info.st_size,
                      "mode": oct(info.st_mode & 0o777)}
    _need(returncode == 0 and not output_root.exists(),
          "R22 worker supervisor-handshake preflight failed or created output")
    receipt = {"profile": "ashlar-r22-log-handshake-self-test/0.1",
        "status": "passed_effect_free", "scope_sha256": _sha(scope_raw),
        "worker_sha256": _sha(worker.read_bytes()),
        "supervisor_sha256": _sha(supervisor_path.read_bytes()),
        "worker_returncode": returncode, "worker_result": child_result,
        "test_umask": "000", "logs": logs,
        "attempt_output_created": False, "database_opened": False,
        "spark_started": False, "delta_written": False,
        "source_acknowledgement": False, "cloud": False,
        "self_test_root": str(test_root)}
    _write_json_exclusive(test_root / "log-handshake-self-test.json", receipt)
    print(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", required=True, type=Path)
    parser.add_argument("--worker", required=True, type=Path)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--dispatch-receipt", type=Path)
    parser.add_argument("--log-self-test-root", type=Path)
    args = parser.parse_args(argv)
    scope_path = args.scope.resolve(strict=True)
    worker_argument = args.worker.resolve(strict=True)
    try:
        supervisor_path = Path(__file__).resolve(strict=True)
        scope, scope_raw, worker, python, output_root, supervisor_root = _scope_admit(
            scope_path, worker_argument, supervisor_path)
        if args.log_self_test_root is not None:
            _need(args.dispatch_receipt is None and not args.preflight_only,
                  "R22 log self-test cannot include dispatch or preflight flags")
            return _log_self_test(scope, scope_raw, worker, python, output_root,
                supervisor_path, scope_path, args.log_self_test_root.resolve())
        if args.preflight_only:
            _need(args.dispatch_receipt is None, "preflight cannot include dispatch receipt")
            structural = _structural_worker_admit(scope_path, worker, python,
                output_root, supervisor_root, scope_raw)
            result = {"profile": PROFILE, "status": "preflight_ok",
                "scope_sha256": _sha(scope_raw), "worker_sha256": _sha(worker.read_bytes()),
                "supervisor_sha256": _sha(supervisor_path.read_bytes()),
                "python": str(python), "worker": str(worker),
                "attempt_root": str(output_root), "supervisor_root": str(supervisor_root),
                "native_effects": "none", "native_structural_preflight": structural}
            print(json.dumps(result, sort_keys=True, separators=(",", ":")))
            return 0
        _need(args.dispatch_receipt is not None,
              "dispatch requires a separate Sol-reviewed exact-hash receipt")
        _dispatch_admit(args.dispatch_receipt.resolve(strict=True), scope_raw=scope_raw,
                        worker=worker, supervisor_path=supervisor_path, scope=scope)
        # Re-run against the exact dispatch scope and worker immediately before
        # supervisor-root creation or child launch; this is not a skippable
        # advisory checker.
        _structural_worker_admit(scope_path, worker, python,
            output_root, supervisor_root, scope_raw)
        return _dispatch(scope, scope_raw, worker, python, output_root,
                         Path(scope["paths"]["data_directory"]), supervisor_root,
                         supervisor_path, scope_path)
    except BaseException as error:
        result = {"profile": PROFILE, "status": "admission_refused",
            "reason": f"{type(error).__name__}: {error}"[:1500],
            "native_effects": "none", "attempts": 0, "automatic_retries": 0}
        print(json.dumps(result, sort_keys=True, separators=(",", ":")), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
