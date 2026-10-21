#!/usr/bin/env python3
"""Bounded private process-group supervisor for the internal Spark smoke."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import selectors
import signal
import stat
import subprocess
import sys
import time

MAX_SECONDS = 600
TERM_GRACE_SECONDS = 12
MAX_CAPTURE_BYTES = 1 * 1024 * 1024


def _regular_owned(path: Path, *, mode: int | None = None) -> None:
    if not path.is_absolute() or path.resolve() != path or path.is_symlink():
        raise ValueError("Canonical absolute nonsymlink path required")
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
        raise PermissionError("Owned regular file required")
    if mode is not None and stat.S_IMODE(info.st_mode) != mode:
        raise PermissionError("Exact private file mode required")


def _private_directory(path: Path) -> None:
    if not path.is_absolute() or path.resolve() != path or path.is_symlink():
        raise ValueError("Canonical absolute private directory required")
    info = path.stat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) & 0o077):
        raise PermissionError("Caller-owned private directory required")


def _stop_group(process: subprocess.Popen, grace: float) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        if process.poll() is None:
            process.wait()
        return
    if process.poll() is None:
        try:
            process.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            pass
    deadline = time.monotonic() + grace
    while True:
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            break
        if time.monotonic() >= deadline:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            break
        time.sleep(0.1)
    if process.poll() is None:
        process.wait()


def supervise(worker: Path, config: Path, log_path: Path, python: Path,
              java_home: Path) -> int:
    _regular_owned(worker)
    _regular_owned(config, mode=0o600)
    if (not python.is_absolute() or not python.resolve().is_file()
            or not os.access(python, os.X_OK)):
        raise PermissionError("Pinned executable Python interpreter required")
    if (not java_home.is_absolute() or java_home.resolve() != java_home
            or not (java_home / "bin" / "java").is_file()
            or not os.access(java_home / "bin" / "java", os.X_OK)):
        raise PermissionError("Pinned Java home required")
    _private_directory(log_path.parent)
    if log_path.exists() or log_path.is_symlink():
        raise ValueError("Fresh private capture path required")

    environment = {
        "PATH": str(python.parent) + os.pathsep + str(Path("/usr/bin")) + os.pathsep + str(Path("/bin")),
        "JAVA_HOME": str(java_home),
        "HOME": str(log_path.parent),
        "SPARK_LOCAL_IP": "127.0.0.1",
        "LC_ALL": "C.UTF-8",
    }
    environment["PATH"] = str(java_home / "bin") + os.pathsep + environment["PATH"]

    fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    process = None
    used = 0
    timed_out = False
    exceeded = False
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb", closefd=True) as log:
            process = subprocess.Popen(
                [str(python), str(worker), str(config)],
                cwd=str(log_path.parent), env=environment,
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, start_new_session=True, close_fds=True,
            )
            assert process.stdout is not None
            selector = selectors.DefaultSelector()
            selector.register(process.stdout, selectors.EVENT_READ)
            deadline = time.monotonic() + MAX_SECONDS
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    break
                events = selector.select(min(remaining, 1.0))
                if not events:
                    if process.poll() is not None:
                        break
                    continue
                for key, _ in events:
                    block = os.read(key.fd, min(65536, MAX_CAPTURE_BYTES - used + 1))
                    if not block:
                        selector.unregister(key.fileobj)
                        continue
                    if used + len(block) > MAX_CAPTURE_BYTES:
                        allowed = MAX_CAPTURE_BYTES - used
                        if allowed > 0:
                            log.write(block[:allowed])
                            used += allowed
                        exceeded = True
                        break
                    log.write(block)
                    used += len(block)
                if exceeded:
                    break
            if timed_out or exceeded:
                _stop_group(process, TERM_GRACE_SECONDS)
            else:
                try:
                    process.wait(timeout=max(0.0, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    timed_out = True
                finally:
                    # Spark's gateway can outlive its Python leader. Always
                    # close this dedicated group after normal or timed exit.
                    _stop_group(process, TERM_GRACE_SECONDS)
            log.flush()
            os.fsync(log.fileno())
    except BaseException:
        if process is not None:
            _stop_group(process, TERM_GRACE_SECONDS)
        raise
    if timed_out:
        return 124
    if exceeded:
        return 125
    return int(process.returncode)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--log", required=True, type=Path)
    parser.add_argument("--python", required=True, type=Path)
    parser.add_argument("--java-home", required=True, type=Path)
    args = parser.parse_args(argv)
    return supervise(args.worker, args.config, args.log, args.python, args.java_home)


if __name__ == "__main__":
    raise SystemExit(main())
