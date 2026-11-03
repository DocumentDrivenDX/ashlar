#!/usr/bin/env python3
"""Hard wall/RSS/output supervisor for the one-shot local managed-current run."""
from __future__ import annotations

import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import time

WORKER = Path(__file__).with_name("run_managed_current_local_native.py")
ROOT = Path("/private/tmp/managed-current-local-native-20261103-attempt14")
LOG = Path("/private/tmp/managed-current-local-native-20261103-attempt14-supervisor.log")
RECEIPT = Path("/private/tmp/managed-current-local-native-20261103-attempt14-supervisor.json")
HARD_WALL_SECONDS = 900
WORK_DEADLINE_SECONDS = 870
RSS_CAP_BYTES = 4 * 1024**3
OUTPUT_CAP_BYTES = 512 * 1024**2
CAPTURE_CAP_BYTES = 16 * 1024**2
TERM_GRACE_SECONDS = 10


def _private_new_file(path: Path):
    if path.exists() or path.is_symlink() or not path.parent.is_dir():
        raise RuntimeError("Fresh private supervisor output path required")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    return os.fdopen(fd, "wb")


def _kill_group(pgid: int, sig: int):
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        pass


def _group_census(psutil, pgid: int, root_pid: int):
    pids = set()
    failures = []
    try:
        root = psutil.Process(root_pid)
        pids.update(child.pid for child in root.children(recursive=True))
    except psutil.NoSuchProcess:
        pass
    except psutil.Error as error:
        failures.append(type(error).__name__)
    for process in psutil.process_iter(attrs=["pid", "status"]):
        pid = process.info["pid"]
        try:
            if os.getpgid(pid) == pgid:
                if process.info.get("status") != psutil.STATUS_ZOMBIE:
                    pids.add(pid)
        except ProcessLookupError:
            continue
        except PermissionError:
            failures.append(f"permission:{pid}")
    if failures:
        raise RuntimeError("Incomplete owned process census: " + ",".join(failures[:8]))
    rss = 0
    live = []
    for pid in sorted(pids):
        try:
            process = psutil.Process(pid)
            if process.status() == psutil.STATUS_ZOMBIE:
                continue
            rss += process.memory_info().rss
            live.append(pid)
        except psutil.NoSuchProcess:
            continue
        except psutil.Error as error:
            raise RuntimeError(f"Incomplete RSS observation for owned pid {pid}") from error
    return rss, live


def _postgres_roots():
    return tuple(ROOT / "sources" / f"pgdata-{slot}" for slot in ("A", "B"))


def _owned_postgres_processes(psutil, postgres_binary: Path, worker_pgid: int | None = None):
    result = {}
    for pgdata in _postgres_roots():
        if not pgdata.exists():
            continue
        if pgdata.is_symlink() or not pgdata.is_dir() or pgdata.resolve() != pgdata:
            raise RuntimeError("Owned PostgreSQL data root is not canonical")
        pidfile = pgdata / "postmaster.pid"
        if not pidfile.exists():
            continue
        if pidfile.is_symlink():
            raise RuntimeError("Owned PostgreSQL pidfile is a symlink")
        lines = pidfile.read_text(encoding="ascii").splitlines()
        if not lines or not lines[0].lstrip("-").isdigit():
            raise RuntimeError("Owned PostgreSQL pidfile is malformed")
        recorded = int(lines[0])
        pid = abs(recorded)
        if pid <= 1:
            raise RuntimeError("Owned PostgreSQL pidfile PID is invalid")
        try:
            process = psutil.Process(pid)
            exe = Path(process.exe()).resolve()
            argv = process.cmdline()
            environ = process.environ()
        except psutil.NoSuchProcess:
            # initdb's ordinary standalone backend exits while initdb itself
            # continues; it stores the negative backend PID in postmaster.pid.
            # Count the initdb worker in its process group and wait for the
            # daemon's positive postmaster PID instead of treating this normal
            # transition as a stale server owner.
            if recorded < 0:
                try:
                    current = (pgdata / "postmaster.pid").read_text(
                        encoding="ascii").splitlines()[0]
                except (FileNotFoundError, IndexError):
                    continue
                if current != lines[0]:
                    continue
                continue
            try:
                current = (pgdata / "postmaster.pid").read_text(
                    encoding="ascii").splitlines()[0]
            except (FileNotFoundError, IndexError):
                continue
            if current != lines[0]:
                continue
            raise RuntimeError("Stale positive PostgreSQL pidfile requires reconciliation")
        except (psutil.AccessDenied, PermissionError) as error:
            # Reconcile an access-denied sample against the exact pidfile before
            # failing. A vanished/replaced PID is a normal pg_ctl transition;
            # an unchanged positive owner remains fail-closed. The negative PID
            # form is initdb's standalone backend, which is already in the
            # supervised worker group when its exact PGDATA is being initialized.
            try:
                current = (pgdata / "postmaster.pid").read_text(
                    encoding="ascii").splitlines()[0]
            except (FileNotFoundError, IndexError):
                continue
            if current != lines[0]:
                continue
            if recorded < 0 and worker_pgid is not None:
                try:
                    process_group = os.getpgid(pid)
                except ProcessLookupError:
                    continue
                if process_group == worker_pgid:
                    continue
            raise RuntimeError(
                "Access-denied PostgreSQL identity remains bound to unchanged pidfile"
            ) from error
        if exe != postgres_binary.resolve() or Path(argv[0]).name != "postgres":
            raise RuntimeError("PostgreSQL PID does not identify the selected installed binary")
        has_data_arg = any(argv[index] == "-D" and index + 1 < len(argv)
                           and Path(argv[index + 1]).resolve() == pgdata
                           for index in range(len(argv)))
        exact_env = environ.get("PGDATA") == str(pgdata)
        if not (has_data_arg or exact_env):
            raise RuntimeError("PostgreSQL process is not bound to its exact fresh PGDATA")
        if recorded < 0:
            # PostgreSQL writes the negative PID for its standalone initdb
            # backend. Its process title may replace the original --single/
            # --boot arguments, so bind it by exact executable, exact PGDATA,
            # and the supervised worker process group instead.
            if worker_pgid is None:
                raise RuntimeError("Negative PostgreSQL PID lacks a supervised worker group")
            try:
                process_group = os.getpgid(pid)
            except ProcessLookupError:
                continue
            if process_group != worker_pgid:
                raise RuntimeError("Negative PostgreSQL PID escaped the supervised worker group")
        result[pgdata] = pid
    return result


def _owned_rss(psutil, pgid: int, root_pid: int, postgres_binary: Path):
    rss, pids = _group_census(psutil, pgid, root_pid)
    for pid in _owned_postgres_processes(psutil, postgres_binary, pgid).values():
        if pid not in pids:
            try:
                rss += psutil.Process(pid).memory_info().rss
                pids.append(pid)
            except psutil.NoSuchProcess:
                continue
    return rss, pids


def _stop_owned_postgres(psutil, pg_ctl: Path, postgres_binary: Path, deadline: float, worker_pgid: int | None = None):
    stopped = []
    for pgdata in _postgres_roots():
        owned = _owned_postgres_processes(psutil, postgres_binary, worker_pgid)
        pid = owned.get(pgdata)
        if pid is None:
            continue
        remaining = deadline - time.monotonic()
        if remaining <= 0.1:
            raise TimeoutError("No cleanup reserve remains for owned PostgreSQL shutdown")
        completed = subprocess.run([str(pg_ctl), "-D", str(pgdata), "-m", "fast", "-w", "stop"],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            timeout=min(remaining, 20), check=False, close_fds=True)
        stopped.append({"pgdata": str(pgdata), "pid": pid, "returncode": completed.returncode,
            "output_sha256": __import__("hashlib").sha256(completed.stdout).hexdigest()})
        if pgdata.joinpath("postmaster.pid").exists():
            raise RuntimeError("Exact owned PostgreSQL cluster did not stop cleanly")
    for pgdata in _postgres_roots():
        if pgdata.exists() and pgdata.joinpath("postmaster.pid").exists():
            raise RuntimeError("PostgreSQL pidfile remains after owned cleanup")
    return stopped


def _output_bytes(root: Path) -> int:
    if not root.exists():
        return 0
    if root.is_symlink() or not root.is_dir() or root.resolve() != root:
        raise RuntimeError("Disposable worker output root is not canonical")
    total = entries = 0
    for base, dirs, files in os.walk(root, followlinks=False):
        entries += len(dirs) + len(files)
        if entries > 100000:
            raise RuntimeError("Disposable output file count exceeds supervisor bound")
        for name in dirs + files:
            path = Path(base) / name
            if path.is_symlink():
                raise RuntimeError("Symlink found under disposable output root")
            if path.is_file():
                try:
                    total += path.stat().st_size
                except FileNotFoundError:
                    continue
    return total


def run():
    import psutil
    if (psutil.__version__ != "7.2.2"
            or not Path(psutil.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())):
        raise RuntimeError("Selected supervisor psutil origin/version changed")
    if not WORKER.is_file() or WORKER.is_symlink() or ROOT.exists() or ROOT.is_symlink():
        raise RuntimeError("Exact worker and fresh attempt14 root required")
    if time.monotonic() + HARD_WALL_SECONDS <= time.monotonic():
        raise RuntimeError("Invalid hard wall")
    process = None
    postgres_cleanup = []
    pgserver_root = Path(sys.prefix) / "lib/python3.12/site-packages/pgserver/pginstall/bin"
    pg_ctl = pgserver_root / "pg_ctl"
    postgres_binary = pgserver_root / "postgres"
    if not all(path.is_file() and not path.is_symlink() for path in (pg_ctl, postgres_binary)):
        raise RuntimeError("Exact installed PostgreSQL 18 binaries are required")
    binary_hashes = {path.name: __import__("hashlib").sha256(path.read_bytes()).hexdigest()
                     for path in (pg_ctl, postgres_binary)}
    selector = selectors.DefaultSelector()
    used = peak_rss = output_size = 0
    peak_output = 0
    last_output_check = 0.0
    timed_out = rss_exceeded = output_exceeded = False
    started = time.monotonic()
    with _private_new_file(LOG) as log:
        process = subprocess.Popen([sys.executable, str(WORKER)], cwd=str(WORKER.parents[1]),
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            start_new_session=True, close_fds=True)
        if process.stdout is None:
            raise RuntimeError("Worker output pipe unavailable")
        selector.register(process.stdout, selectors.EVENT_READ)
        work_deadline = started + WORK_DEADLINE_SECONDS
        hard_deadline = started + HARD_WALL_SECONDS
        try:
            while process.poll() is None or selector.get_map():
                now = time.monotonic()
                if now >= work_deadline:
                    timed_out = True
                    break
                rss, _ = _owned_rss(psutil, process.pid, process.pid, postgres_binary)
                peak_rss = max(peak_rss, rss)
                if rss > RSS_CAP_BYTES:
                    rss_exceeded = True
                    break
                if now - last_output_check >= 2.0:
                    output_size = _output_bytes(ROOT)
                    peak_output = max(peak_output, output_size)
                    last_output_check = now
                    if output_size + used > OUTPUT_CAP_BYTES:
                        output_exceeded = True
                        break
                for key, _ in selector.select(min(0.25, max(0.0, work_deadline - now))):
                    log_available = min(CAPTURE_CAP_BYTES - used,
                        OUTPUT_CAP_BYTES - used - output_size)
                    if log_available <= 0:
                        output_exceeded = True
                        break
                    block = os.read(key.fd, min(65536, log_available + 1))
                    if not block:
                        selector.unregister(key.fileobj)
                        continue
                    keep = min(len(block), log_available)
                    if keep:
                        log.write(block[:keep])
                        used += keep
                    if keep != len(block):
                        output_exceeded = True
                        break
                if output_exceeded:
                    break
            if process.poll() is None:
                _kill_group(process.pid, signal.SIGINT)
                try:
                    process.wait(timeout=TERM_GRACE_SECONDS)
                except subprocess.TimeoutExpired:
                    _kill_group(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=TERM_GRACE_SECONDS)
                    except subprocess.TimeoutExpired:
                        _kill_group(process.pid, signal.SIGKILL)
                        process.wait()
            else:
                _, survivors = _owned_rss(psutil, process.pid, process.pid, postgres_binary)
                if survivors:
                    _kill_group(process.pid, signal.SIGTERM)
                    survivor_deadline = min(hard_deadline, time.monotonic() + TERM_GRACE_SECONDS)
                    while survivors and time.monotonic() < survivor_deadline:
                        time.sleep(0.1)
                        _, survivors = _owned_rss(psutil, process.pid, process.pid, postgres_binary)
                    if survivors:
                        _kill_group(process.pid, signal.SIGKILL)
            postgres_cleanup = _stop_owned_postgres(psutil, pg_ctl, postgres_binary, hard_deadline, process.pid)
            remaining = max(0, hard_deadline - time.monotonic())
            _, group_pids = _owned_rss(psutil, process.pid, process.pid, postgres_binary)
            while remaining and group_pids:
                time.sleep(min(0.1, remaining))
                remaining = max(0, hard_deadline - time.monotonic())
                _, group_pids = _owned_rss(psutil, process.pid, process.pid, postgres_binary)
            _, group_pids = _owned_rss(psutil, process.pid, process.pid, postgres_binary)
            if group_pids:
                _kill_group(process.pid, signal.SIGKILL)
                raise RuntimeError("Owned Spark/worker descendants remained after bounded cleanup")
            if time.monotonic() >= hard_deadline:
                timed_out = True
        finally:
            if process is not None and process.poll() is None:
                _kill_group(process.pid, signal.SIGINT)
                try:
                    process.wait(timeout=TERM_GRACE_SECONDS)
                except subprocess.TimeoutExpired:
                    _kill_group(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=TERM_GRACE_SECONDS)
                    except subprocess.TimeoutExpired:
                        _kill_group(process.pid, signal.SIGKILL)
                        process.wait()
                try:
                    _stop_owned_postgres(psutil, pg_ctl, postgres_binary,
                        started + HARD_WALL_SECONDS, process.pid)
                except BaseException:
                    pass
            log.flush()
            os.fsync(log.fileno())
    worker_receipt = ROOT / "qualification.json"
    worker_result = None
    if worker_receipt.is_file() and not worker_receipt.is_symlink():
        try:
            worker_result = json.loads(worker_receipt.read_bytes())
        except (OSError, ValueError):
            worker_result = None
    status = "passed" if process.returncode == 0 and not any(
        (timed_out, rss_exceeded, output_exceeded)) and type(worker_result) is dict \
        and worker_result.get("status") == "passed_local_native_a0_a1" \
        and worker_result.get("closed_before_receipt") is True \
        and not worker_result.get("cleanup_errors") else "failed"
    result = {"profile": "ashlar-managed-current-local-native-supervisor/0.1",
        "status": status, "returncode": process.returncode,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "hard_wall_seconds": HARD_WALL_SECONDS,
        "work_deadline_seconds": WORK_DEADLINE_SECONDS,
        "aggregate_rss_cap_bytes": RSS_CAP_BYTES, "peak_aggregate_rss_bytes": peak_rss,
        "disposable_output_cap_bytes": OUTPUT_CAP_BYTES,
        "peak_disposable_output_bytes": peak_output,
        "output_cap_bytes": OUTPUT_CAP_BYTES, "captured_output_cap_bytes": CAPTURE_CAP_BYTES,
        "captured_output_bytes": used,
        "timed_out": timed_out, "rss_exceeded": rss_exceeded,
        "output_exceeded": output_exceeded, "worker_log": str(LOG),
        "worker_receipt": str(worker_receipt),
        "worker_status": None if worker_result is None else worker_result.get("status"),
        "worker_closed_before_receipt": None if worker_result is None else
            worker_result.get("closed_before_receipt"),
        "postgres_binaries_sha256": binary_hashes,
        "postgres_stop_receipts": postgres_cleanup,
        "psutil": {"version": psutil.__version__, "path": str(Path(psutil.__file__).resolve()),
            "module_sha256": __import__("hashlib").sha256(
                Path(psutil.__file__).read_bytes()).hexdigest()},
        "process_group_empty": not group_pids}
    with _private_new_file(RECEIPT) as output:
        output.write(json.dumps(result, sort_keys=True, separators=(",", ":")).encode() + b"\n")
        output.flush()
        os.fsync(output.fileno())
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if status == "passed" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except BaseException as error:
        import traceback
        if not RECEIPT.exists() and not RECEIPT.is_symlink():
            try:
                result = {"profile": "ashlar-managed-current-local-native-supervisor/0.1",
                    "status": "failed", "effects_possible": bool(ROOT.exists()),
                    "error_type": type(error).__name__, "error": str(error)[:1200],
                    "traceback": traceback.format_exc()[-8000:],
                    "worker_receipt": str(ROOT / "qualification.json"),
                    "hard_wall_seconds": HARD_WALL_SECONDS,
                    "aggregate_rss_cap_bytes": RSS_CAP_BYTES}
                with _private_new_file(RECEIPT) as output:
                    output.write(json.dumps(result, sort_keys=True,
                        separators=(",", ":")).encode() + b"\n")
                    output.flush()
                    os.fsync(output.fileno())
            except BaseException:
                pass
        raise
