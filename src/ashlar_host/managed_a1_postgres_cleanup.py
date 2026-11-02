# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bounded, PGDATA-bound shutdown for Truss runtimes that escape worker groups."""
from __future__ import annotations

import fcntl
import errno
import ctypes
import hashlib
import os
from pathlib import Path
import re
import shlex
import signal
import stat
import subprocess
import sys
import time


class ManagedA1PostgresCleanupError(RuntimeError):
    """A private PostgreSQL runtime could not be proven stopped."""


def inspect_pg_ctl(path: Path) -> dict:
    """Freeze the exact executable bytes and PostgreSQL major before startup."""
    path = Path(path)
    if (not path.is_absolute() or path.is_symlink() or path.resolve() != path
            or not path.is_file() or not os.access(path, os.X_OK)):
        raise ManagedA1PostgresCleanupError("Canonical executable PostgreSQL pg_ctl required")
    tools = {}
    for name in ("initdb", "pg_ctl", "postgres", "pg_controldata"):
        executable = path.parent / name
        if (executable.is_symlink() or not executable.is_file()
                or not os.access(executable, os.X_OK)):
            raise ManagedA1PostgresCleanupError(f"Exact bundled PostgreSQL {name} required")
        tools[name] = {"path": str(executable),
            "sha256": hashlib.sha256(executable.read_bytes()).hexdigest()}
    try:
        probe = subprocess.run([str(path), "--version"], stdin=subprocess.DEVNULL,
            capture_output=True, text=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ManagedA1PostgresCleanupError("Pinned pg_ctl version probe failed") from error
    if (probe.returncode != 0
            or re.fullmatch(r"pg_ctl \(PostgreSQL\) 18\.[0-9]+(?:\s+.*)?",
                            probe.stdout.strip()) is None):
        raise ManagedA1PostgresCleanupError("Pinned PostgreSQL 18 pg_ctl required")
    return {"path": str(path), "sha256": tools["pg_ctl"]["sha256"],
            "version": probe.stdout.strip(), "tools": tools}


def inspect_pg_control(pgdata: Path, *, expected_pg_controldata: Path) -> dict:
    """Read the owned control file with the matching bundled PostgreSQL tools."""
    pgdata = Path(pgdata)
    binary = Path(expected_pg_controldata)
    version = pgdata / "PG_VERSION"
    pg_control = pgdata / "global" / "pg_control"
    if (not version.is_file() or version.is_symlink()
            or version.read_text(encoding="ascii").strip() != "18"
            or binary.resolve() != binary
            or binary.is_symlink() or not binary.is_file() or not os.access(binary, os.X_OK)):
        raise ManagedA1PostgresCleanupError("Exact initialized PostgreSQL 18 control state required")
    if (not pg_control.is_file() or pg_control.is_symlink()
            or pg_control.stat().st_uid != os.getuid()):
        raise ManagedA1PostgresCleanupError("Owned regular PostgreSQL control file required")
    try:
        result = subprocess.run([str(binary), "-D", str(pgdata)], stdin=subprocess.DEVNULL,
            capture_output=True, text=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ManagedA1PostgresCleanupError("pg_controldata owner-bound probe failed") from error
    if (result.returncode != 0
            or re.search(r"(?m)^pg_control version number:\s*1800\s*$", result.stdout) is None):
        raise ManagedA1PostgresCleanupError("PGDATA is not exact PostgreSQL 18 control format")
    return {"pgdata": str(pgdata), "pg_version": "18",
            "pg_control_sha256": hashlib.sha256(pg_control.read_bytes()).hexdigest(),
            "pg_controldata_sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}


def _read_pidfile(pgdata: Path, *, stable_seconds: float = 0.15) -> int | None:
    pidfile = pgdata / "postmaster.pid"
    if not pidfile.exists() and not pidfile.is_symlink():
        return None
    info = pidfile.lstat()
    if (not stat.S_ISREG(info.st_mode) or pidfile.is_symlink()
            or info.st_uid != os.getuid() or info.st_mode & 0o077):
        raise ManagedA1PostgresCleanupError("Owned private regular postmaster.pid required")
    deadline = time.monotonic() + stable_seconds * 12
    previous = None
    while time.monotonic() < deadline:
        try:
            before = pidfile.stat()
            raw = pidfile.read_text(encoding="ascii")
            after = pidfile.stat()
        except FileNotFoundError:
            return None
        if ((before.st_ino, before.st_size, before.st_mtime_ns)
                != (after.st_ino, after.st_size, after.st_mtime_ns)):
            previous = None
            time.sleep(0.025)
            continue
        lines = raw.splitlines()
        if len(lines) < 2 or Path(lines[1]).resolve() != pgdata.resolve():
            raise ManagedA1PostgresCleanupError("postmaster.pid does not bind to exact PGDATA")
        try:
            pid = int(lines[0])
        except ValueError as error:
            raise ManagedA1PostgresCleanupError("Stable PostgreSQL PID required") from error
        if abs(pid) <= 1:
            raise ManagedA1PostgresCleanupError("Unsafe PostgreSQL PID refused")
        marker = (after.st_ino, after.st_size, after.st_mtime_ns, pid, lines[1])
        if marker == previous:
            return pid
        previous = marker
        time.sleep(0.025)
    raise ManagedA1PostgresCleanupError("postmaster.pid did not stabilize for owned PGDATA")


def _process_executable(pid: int) -> Path:
    if sys.platform.startswith("linux"):
        try:
            return Path(os.readlink(f"/proc/{pid}/exe")).resolve(strict=True)
        except OSError as error:
            raise ManagedA1PostgresCleanupError("PostgreSQL executable identity is unreadable") from error
    if sys.platform == "darwin":
        try:
            libproc = ctypes.CDLL("/usr/lib/libproc.dylib")
            buffer = ctypes.create_string_buffer(4096)
            libproc.proc_pidpath.argtypes = (ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32)
            libproc.proc_pidpath.restype = ctypes.c_int
            size = libproc.proc_pidpath(pid, buffer, len(buffer))
            if size <= 0 or size >= len(buffer):
                raise OSError("proc_pidpath refused")
            return Path(os.fsdecode(buffer.raw[:size])).resolve(strict=True)
        except (OSError, AttributeError) as error:
            raise ManagedA1PostgresCleanupError("PostgreSQL executable identity is unreadable") from error
    raise ManagedA1PostgresCleanupError("Supported process executable inspection is unavailable")


def _process_command(pid: int, *, pgdata: Path, standalone: bool,
                     postgres_path: Path) -> str:
    try:
        result = subprocess.run(["ps", "-p", str(pid), "-o", "command="],
            stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=3, check=False)
        environment = subprocess.run(["ps", "eww", "-p", str(pid), "-o", "command="],
            stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=3, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ManagedA1PostgresCleanupError("PostgreSQL process identity probe failed") from error
    line = result.stdout.strip()
    environment_line = environment.stdout.strip()
    if (result.returncode != 0 or environment.returncode != 0 or not line
            or not environment_line):
        raise ManagedA1PostgresCleanupError("postmaster.pid process is not live/readable")
    try:
        fields = shlex.split(line)
    except ValueError as error:
        raise ManagedA1PostgresCleanupError("PostgreSQL process argv is malformed") from error
    if (not fields or _process_executable(pid) != postgres_path
            or not postgres_path.is_absolute() or postgres_path.is_symlink()
            or not postgres_path.is_file()):
        raise ManagedA1PostgresCleanupError("PID executable differs from the pinned PostgreSQL binary")
    argv_bound = any((fields[index] == "-D" and index + 1 < len(fields)
                 and Path(fields[index + 1]).resolve() == pgdata.resolve())
                or fields[index] == f"-D{pgdata}" for index in range(1, len(fields)))
    try:
        environment_fields = shlex.split(environment_line)
    except ValueError as error:
        raise ManagedA1PostgresCleanupError("PostgreSQL process environment is malformed") from error
    # ps eww includes the command followed by its environment. Only inspect the
    # PGDATA token and never retain or report the other environment values.
    env_pgdata = next((item.split("=", 1)[1] for item in environment_fields
                       if item.startswith("PGDATA=")), None)
    bound = argv_bound or env_pgdata == str(pgdata)
    single = "--single" in fields or "--boot" in fields
    if not bound or single != standalone:
        raise ManagedA1PostgresCleanupError("PostgreSQL process argv does not prove exact PGDATA/mode")
    return line


def _lease_state(path: Path) -> dict:
    if not path.exists() and not path.is_symlink():
        return {"present": False, "unlocked": False}
    info = path.lstat()
    parent = path.parent.lstat()
    if (not stat.S_ISREG(info.st_mode) or path.is_symlink() or info.st_uid != os.getuid()
            or not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid()
            or stat.S_IMODE(parent.st_mode) != 0o700):
        raise ManagedA1PostgresCleanupError("Owned private Truss runtime lease required")
    fd = os.open(path, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0))
    try:
        try:
            fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            if error.errno in (errno.EAGAIN, errno.EACCES):
                return {"present": True, "unlocked": False}
            raise
        fcntl.lockf(fd, fcntl.LOCK_UN)
        return {"present": True, "unlocked": True}
    finally:
        os.close(fd)


def cleanup_owned_postgres(pgdata: Path, pg_ctl_lock: dict, *, deadline: float,
                           pg_controldata: Path) -> dict:
    """Stop one exact private PGDATA and prove pid, socket and lease closure.

    The caller must first reap the holder/owner process group. PGDATA is never
    removed, including after an uncertain ACK.
    """
    pgdata = Path(pgdata)
    if (not pgdata.is_absolute() or pgdata.is_symlink() or pgdata.resolve() != pgdata
            or not pgdata.is_dir() or pgdata.stat().st_uid != os.getuid()
            or stat.S_IMODE(pgdata.stat().st_mode) != 0o700):
        raise ManagedA1PostgresCleanupError("Canonical private owned PGDATA required")
    locked_tools = inspect_pg_ctl(Path(pg_ctl_lock["path"]))
    if locked_tools != pg_ctl_lock:
        raise ManagedA1PostgresCleanupError("Pinned PostgreSQL runtime tools changed before cleanup")
    expected_control_tool = Path(pg_ctl_lock["tools"]["pg_controldata"]["path"])
    if Path(pg_controldata) != expected_control_tool:
        raise ManagedA1PostgresCleanupError("Exact locked pg_controldata executable required")
    control = inspect_pg_control(pgdata, expected_pg_controldata=expected_control_tool)
    pidfile = pgdata / "postmaster.pid"
    base = Path("/private/tmp" if sys.platform == "darwin" else "/tmp")
    socket_root = base / f"truss-pg18-{os.getuid()}"
    socket_dir = socket_root / hashlib.sha256(os.fsencode(pgdata)).hexdigest()[:24]
    socket_file = socket_dir / ".s.PGSQL.5432"
    lease_file = Path(str(pgdata) + ".truss-runtime.lock")
    signed_pid = _read_pidfile(pgdata)
    stop_returncode = None
    if signed_pid is not None:
        lock = inspect_pg_ctl(Path(pg_ctl_lock["path"]))
        if lock != pg_ctl_lock:
            raise ManagedA1PostgresCleanupError("Pinned pg_ctl changed after runtime startup")
        if time.monotonic() + 2 >= deadline:
            raise TimeoutError("Insufficient deadline for owned PostgreSQL stop")
        process_pid = abs(signed_pid)
        postgres_path = Path(pg_ctl_lock["tools"]["postgres"]["path"])
        _process_command(process_pid, pgdata=pgdata, standalone=signed_pid < 0,
                         postgres_path=postgres_path)
        if signed_pid < 0:
            # Negative PID is PostgreSQL's standalone/initdb state marker.
            # Never pass a negative value to kill/killpg; only signal the
            # verified positive PID from its postmaster.pid binding.
            try:
                os.kill(process_pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            stop_returncode = "verified_standalone_sigterm_sent"
        else:
            timeout = max(1, min(15, int(deadline - time.monotonic() - 1)))
            try:
                stopped = subprocess.run([str(pg_ctl_lock["path"]), "-D", str(pgdata),
                    "-m", "fast", "-w", "-t", str(timeout), "stop"],
                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL, cwd=str(pgdata.parent), timeout=timeout,
                    check=False)
                stop_returncode = stopped.returncode
            except subprocess.TimeoutExpired:
                stop_returncode = "timeout"
        while time.monotonic() < deadline:
            if not pidfile.exists() and not pidfile.is_symlink():
                break
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))
    pid_absent = not pidfile.exists() and not pidfile.is_symlink()
    socket_absent = not socket_file.exists() and not socket_file.is_symlink()
    lease = _lease_state(lease_file)
    if socket_dir.exists() or socket_dir.is_symlink():
        info = socket_dir.lstat()
        if (not stat.S_ISDIR(info.st_mode) or socket_dir.is_symlink()
                or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700):
            raise ManagedA1PostgresCleanupError("Truss socket directory ownership/mode changed")
    if not (pid_absent and socket_absent and (not lease["present"] or lease["unlocked"])):
        raise ManagedA1PostgresCleanupError("Owned PostgreSQL PID/socket/runtime lease remains live")
    control_after = inspect_pg_control(pgdata, expected_pg_controldata=expected_control_tool)
    return {"pgdata": str(pgdata), "pg_ctl": pg_ctl_lock,
            "control_before_stop": control, "control_after_stop": control_after,
            "pg_ctl_returncode": stop_returncode,
            "postmaster_pid_absent": pid_absent, "socket_absent": socket_absent,
            "runtime_lease": lease, "postgres_stopped": True,
            "pgdata_preserved": True}
