#!/usr/bin/env python3
"""One-shot bounded local supervisor for the reviewed R61 worker."""

from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import selectors
import stat
import subprocess
import sys
import time
import zipfile

REPO = Path(__file__).resolve().parents[1]
SCOPE = REPO / "docs/helix/04-build/evidence/current-upstream-release-20261102/r61-fresh-truss053-umf102/scope.json"
PENDING = (
    "truss_wheel", "truss_wheel_sha256", "truss_release_manifest_sha256",
    "umf_sdk_artifact", "umf_sdk_sha256", "umf_sdk_tag_commit",
    "umf_core_platform_artifact", "umf_core_sha256",
    "truss_adapter_profile", "python_executable", "python_sha256",
    "runtime_manifest", "runtime_manifest_sha256",
    "bun_executable", "bun_sha256", "postgresql_18_runtime",
    "postgresql_18_runtime_manifest", "postgresql_18_runtime_manifest_sha256",
    "ashlar_candidate_wheel",
    "ashlar_candidate_wheel_sha256", "graphframes_runtime_manifest",
    "graphframes_runtime_manifest_sha256", "worker_path", "worker_sha256",
)
TERM_GRACE = 8
KILL_GRACE = 4
PG_STOP_SECONDS = 22
FINAL_RESERVE_SECONDS = 8
RSS_POLL = 0.25
MAX_LOG_BYTES = 64 * 1024**2
RECEIPT_NAME = "supervisor-result.json"
RUN_ONCE_STARTED = False
RUN_RECEIPT_PATH: str | None = None


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _pinned_psutil(scope: dict):
    pin = scope["supervisor_runtime"]
    module_path = Path(pin["psutil_module_path"])
    if (_sha(module_path) != pin["psutil_module_sha256"]
            or not module_path.is_absolute() or module_path.is_symlink()):
        raise RuntimeError("pinned supervisor psutil module path/hash differs")
    name = "r61_pinned_psutil"
    spec = importlib.util.spec_from_file_location(
        name, module_path, submodule_search_locations=[str(module_path.parent)])
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load pinned supervisor psutil module")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    if module.__version__ != pin["psutil_version"] or Path(module.__file__).resolve() != module_path:
        raise RuntimeError("loaded supervisor psutil origin/version differs")
    return module


def _tree_size(root: Path, *, stop_after: int, exclude: Path | None = None,
               deadline: float | None = None) -> int:
    size = 0
    entries = 0
    for base, dirs, files in os.walk(root, followlinks=False):
        for name in dirs + files:
            entries += 1
            if entries > 100000:
                return stop_after + 1
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("supervisor wall deadline reached during output census")
            path = Path(base) / name
            if exclude is not None and path == exclude:
                continue
            if path.is_symlink():
                raise RuntimeError("symlink found in disposable output")
            if path.is_file():
                size += path.stat().st_size
                if size > stop_after:
                    return size
    return size


def _pgids(psutil) -> set[int]:
    result = set()
    for proc in psutil.process_iter(attrs=["pid"], ad_value=None):
        try:
            if proc.info.get("pid") is not None:
                result.add(proc.pid)
        except psutil.NoSuchProcess:
            continue
        except (psutil.AccessDenied, PermissionError) as exc:
            raise RuntimeError("process census denied; RSS bound cannot be proven") from exc
    return result


def _postmaster_pid(pgdata: Path) -> int | None:
    pidfile = pgdata / "postmaster.pid"
    if not pidfile.exists() and not pidfile.is_symlink():
        return None
    info = pidfile.lstat()
    if (not pidfile.is_file() or pidfile.is_symlink() or info.st_uid != os.getuid()
            or info.st_mode & 0o077):
        raise RuntimeError("owned regular private PostgreSQL pidfile required for RSS census")
    lines = pidfile.read_text().splitlines()
    if len(lines) < 2 or Path(lines[1]).resolve() != pgdata.resolve():
        raise RuntimeError("PostgreSQL pidfile does not bind to R61 PGDATA")
    try:
        pid = int(lines[0])
    except ValueError as exc:
        raise RuntimeError("invalid owned PostgreSQL PID") from exc
    if pid <= 1:
        raise RuntimeError("unsafe PostgreSQL PID")
    return pid


def _group_snapshot(psutil, pgid: int, pgdata: Path) -> tuple[int, list[int], list[int]]:
    rss = 0
    group_pids: set[int] = set()
    for pid in _pgids(psutil):
        try:
            if os.getpgid(pid) == pgid:
                group_pids.add(pid)
        except ProcessLookupError:
            continue
        except (PermissionError, psutil.AccessDenied) as exc:
            raise RuntimeError("process-group census denied; refusing incomplete RSS") from exc
    owned_pg_pids: set[int] = set()
    postmaster_pid = _postmaster_pid(pgdata)
    if postmaster_pid is not None:
        try:
            postmaster = psutil.Process(postmaster_pid)
            owned_pg_pids.add(postmaster.pid)
            owned_pg_pids.update(child.pid for child in postmaster.children(recursive=True))
        except psutil.NoSuchProcess:
            pass
        except (psutil.AccessDenied, PermissionError) as exc:
            raise RuntimeError("owned PostgreSQL process tree inaccessible; refusing incomplete RSS") from exc
    all_pids = group_pids | owned_pg_pids
    for pid in all_pids:
        try:
            rss += psutil.Process(pid).memory_info().rss
        except psutil.NoSuchProcess:
            continue
        except (psutil.AccessDenied, PermissionError) as exc:
            raise RuntimeError("owned process RSS inaccessible; refusing incomplete bound") from exc
    return rss, sorted(group_pids), sorted(owned_pg_pids)


def _group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError as exc:
        raise RuntimeError("cannot verify owned worker process group absence") from exc


def _terminate_group(process: subprocess.Popen, deadline: float) -> dict:
    pgid = process.pid
    sent_term = False
    try:
        os.killpg(pgid, signal.SIGTERM)
        sent_term = True
    except ProcessLookupError:
        pass
    term_until = min(deadline, time.monotonic() + TERM_GRACE)
    while time.monotonic() < term_until:
        if not _group_exists(pgid):
            break
        time.sleep(min(RSS_POLL, max(0, term_until - time.monotonic())))
    sent_kill = False
    if _group_exists(pgid):
        try:
            os.killpg(pgid, signal.SIGKILL)
            sent_kill = True
        except ProcessLookupError:
            pass
        kill_until = min(deadline, time.monotonic() + KILL_GRACE)
        while time.monotonic() < kill_until:
            if not _group_exists(pgid):
                break
            time.sleep(min(RSS_POLL, max(0, kill_until - time.monotonic())))
    if process.poll() is None:
        try:
            process.wait(timeout=max(0.01, min(1.0, deadline - time.monotonic())))
        except subprocess.TimeoutExpired:
            pass
    group_empty = not _group_exists(pgid)
    return {"sigterm_sent": sent_term, "sigkill_sent": sent_kill,
            "process_group_empty": group_empty, "group_empty": group_empty}


def _probe_runtime_lease(path: Path) -> dict:
    if not path.exists() and not path.is_symlink():
        return {"present": False, "unlocked": False}
    info = path.lstat()
    if not path.is_file() or path.is_symlink() or info.st_uid != os.getuid():
        raise RuntimeError("Truss runtime lease must be an owned regular file")
    parent_info = path.parent.lstat()
    if (not stat.S_ISDIR(parent_info.st_mode) or parent_info.st_uid != os.getuid()
            or stat.S_IMODE(parent_info.st_mode) != 0o700):
        raise RuntimeError("Truss runtime lease must remain in the private run root")
    fd = os.open(path, os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0))
    try:
        try:
            fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                return {"present": True, "unlocked": False}
            raise
        fcntl.lockf(fd, fcntl.LOCK_UN)
        return {"present": True, "unlocked": True}
    finally:
        os.close(fd)


def _write_reserved_receipt(fd: int, path: Path, result: dict) -> None:
    info = os.fstat(fd)
    try:
        current = path.lstat()
    except FileNotFoundError:
        current = None
    if (current is None or current.st_dev != info.st_dev or current.st_ino != info.st_ino
            or current.st_uid != os.getuid() or stat.S_IMODE(current.st_mode) != 0o600):
        result["status"] = "failed_receipt_path_replaced_state_uncertain"
        result["receipt_path_replaced"] = True
    encoded = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
    if len(encoded) > 65536:
        result["status"] = "failed_receipt_size_limit_state_uncertain"
        result["peak_process_group_pids"] = result.get("peak_process_group_pids", [])[:32]
        result["peak_postgres_tree_pids"] = result.get("peak_postgres_tree_pids", [])[:32]
        result["receipt_truncated"] = True
        encoded = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
    os.ftruncate(fd, 0)
    os.lseek(fd, 0, os.SEEK_SET)
    view = memoryview(encoded)
    while view:
        view = view[os.write(fd, view):]
    os.fsync(fd)
    if result.get("receipt_path_replaced"):
        fallback = path.with_name("supervisor-result-fallback.json")
        try:
            fallback_fd = os.open(fallback, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                                  getattr(os, "O_NOFOLLOW", 0), 0o600)
        except OSError:
            return
        try:
            remaining = memoryview(encoded)
            while remaining:
                remaining = remaining[os.write(fallback_fd, remaining):]
            os.fsync(fallback_fd)
        finally:
            os.close(fallback_fd)


def _close_owned_postgres(scope: dict, artifacts: dict, deadline: float) -> dict:
    """Stop only the PGDATA pinned to this run, then verify pid/socket absence."""
    pgdata = Path(scope["fresh_local_state"]["postgres_data"])
    base = Path("/private/tmp" if sys.platform == "darwin" else "/tmp")
    runtime_root = base / f"truss-pg18-{os.getuid()}"
    socket_dir = runtime_root / hashlib.sha256(os.fsencode(pgdata)).hexdigest()[:24]
    pidfile = pgdata / "postmaster.pid"
    socket_file = socket_dir / ".s.PGSQL.5432"
    lease_file = Path(str(pgdata) + ".truss-runtime.lock")
    stop_attempted = False
    stop_returncode = None
    if pidfile.exists() or pidfile.is_symlink():
        info = pidfile.lstat()
        if (not pidfile.is_file() or pidfile.is_symlink() or info.st_uid != os.getuid()
                or info.st_mode & 0o077):
            raise RuntimeError("owned regular private PostgreSQL pidfile required")
        lines = pidfile.read_text().splitlines()
        if len(lines) < 2 or Path(lines[1]).resolve() != pgdata.resolve():
            raise RuntimeError("refusing to stop PostgreSQL with a foreign postmaster.pid")
        if time.monotonic() + PG_STOP_SECONDS + FINAL_RESERVE_SECONDS >= deadline:
            raise TimeoutError("insufficient supervisor deadline reserved for PostgreSQL close")
        runtime = Path(artifacts["postgresql_18_runtime"])
        pg_ctl = runtime / "bin" / "pg_ctl" if runtime.is_dir() else runtime
        if not pg_ctl.is_file() or not os.access(pg_ctl, os.X_OK):
            raise RuntimeError("pinned PostgreSQL 18 pg_ctl unavailable for owned cleanup")
        stop_attempted = True
        try:
            stopped = subprocess.run([str(pg_ctl), "-D", str(pgdata), "-m", "fast", "-w",
                                      "-t", str(PG_STOP_SECONDS), "stop"],
                                     stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL, cwd=str(pgdata.parent),
                                     timeout=PG_STOP_SECONDS, check=False)
            stop_returncode = stopped.returncode
        except subprocess.TimeoutExpired:
            stop_returncode = "timeout"
    # A nonzero pg_ctl may mean another owner already stopped it; only the
    # exact post-state determines closure.
    pid_absent = not pidfile.exists() and not pidfile.is_symlink()
    socket_absent = not socket_file.exists() and not socket_file.is_symlink()
    lease = _probe_runtime_lease(lease_file)
    if socket_dir.exists() or socket_dir.is_symlink():
        info = socket_dir.lstat()
        if (not socket_dir.is_dir() or socket_dir.is_symlink()
                or info.st_uid != os.getuid() or info.st_mode & 0o077):
            raise RuntimeError("Truss socket directory owner/mode changed during run")
    return {"stop_attempted": stop_attempted, "pg_ctl_returncode": stop_returncode,
            "postmaster_pid_absent": pid_absent, "socket_absent": socket_absent,
            "runtime_lease": lease,
            "postgres_closed": pid_absent and socket_absent and (not lease["present"] or lease["unlocked"])}


def _validate_scope(scope: dict) -> tuple[list[str], dict]:
    source = scope["source"]
    if subprocess.run(["git", "merge-base", "--is-ancestor", source["base_commit"], "HEAD"],
                      cwd=REPO, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
        raise RuntimeError("pinned source base is not an ancestor of HEAD")
    if subprocess.run(["git", "status", "--porcelain"], cwd=REPO,
                      capture_output=True, text=True, check=True).stdout:
        raise RuntimeError("source worktree is not clean")
    supervisor_runtime = scope["supervisor_runtime"]
    selected_manager = Path(supervisor_runtime["python_executable"])
    if (not selected_manager.is_absolute() or not selected_manager.exists()
            or Path(sys.executable).resolve() != selected_manager.resolve()
            or _sha(selected_manager.resolve()) != supervisor_runtime["python_sha256"]
            or ".".join(map(str, sys.version_info[:3])) != supervisor_runtime["python_version"]):
        raise RuntimeError("exact pinned supervisor Python runtime required")
    checks = ((source["runner_path"], source["runner_sha256"]),
              (source["setup_stage_path"], source["setup_stage_sha256"]),
              (source["supervisor_path"], source["supervisor_sha256"]))
    for relative, expected in checks:
        if _sha(REPO / relative) != expected:
            raise RuntimeError(f"source hash differs from scope: {relative}")
    artifacts = scope["artifacts"]
    if (artifacts.get("truss_version") != "0.0.53"
            or artifacts.get("umf_sdk_version") != "0.10.2"
            or artifacts.get("umf_core_version") != "0.10.6"
            or artifacts.get("umf_model_version") != "0.10.0"):
        raise RuntimeError("R61 Truss/UMF declared versions differ from reviewed cohort")
    if (artifacts.get("umf_sdk_tag_commit") != "e35cd92b45e16816be2bdbf28e83ef9c53ac99a2"
            or artifacts.get("truss_adapter_profile") != "ashlar-truss-current-catalog-evidence/0.8-fresh-baseline"
            or artifacts.get("truss_source_custody_profile") != "ashlar-truss-053-fresh-baseline-source-custody/0.1"
            or artifacts.get("delta_installation_profile") != "ashlar-local-current-installation-profile/0.5"):
        raise RuntimeError("R61 Truss/UMF adapter profiles or SDK tag differ from candidate")
    platform_hashes = ({
        "truss_wheel_sha256": "a6c8568d676cd69675dc27d92fdfed2b85b69089ff0007d2b5bd2f5f48be6d89",
        "truss_release_manifest_sha256": "5ecb599f65d64e6656bf6c83c30cea6e2aefd488cb3fe821b749458d5e9b3522",
        "umf_sdk_sha256": "785de2c4db18b030c9462c7f55f52d012db50a4d4b3c608a4dc3aa6f5fa0d39b",
        "umf_core_sha256": "e6219539dabcdfe9657bc09485ad991b3380ae306c658d0a51a35caac0196e36"}
        if sys.platform == "darwin" else {
        "truss_wheel_sha256": "a6c8568d676cd69675dc27d92fdfed2b85b69089ff0007d2b5bd2f5f48be6d89",
        "truss_release_manifest_sha256": "5ecb599f65d64e6656bf6c83c30cea6e2aefd488cb3fe821b749458d5e9b3522",
        "umf_sdk_sha256": "0e210969dc6d585d73b0e70d35660421b40c141746389118f7181c54bfcff943",
        "umf_core_sha256": "9a4fc6e739fb9c0c80e22916490484fb41b6f00985afc53fb81dcca2d72029b9"})
    for key, expected in platform_hashes.items():
        if artifacts.get(key) != expected:
            raise RuntimeError(f"R61 platform artifact pin differs: {key}")
    missing = [key for key in PENDING if not artifacts.get(key)]
    for path_key, hash_key in (("truss_wheel", "truss_wheel_sha256"),
                               ("umf_sdk_artifact", "umf_sdk_sha256"),
                               ("umf_core_platform_artifact", "umf_core_sha256"),
                               ("bun_executable", "bun_sha256"),
                               ("ashlar_candidate_wheel", "ashlar_candidate_wheel_sha256"),
                               ("graphframes_runtime_manifest", "graphframes_runtime_manifest_sha256")):
        selected = artifacts.get(path_key)
        if selected:
            path = Path(selected)
            if (not path.is_absolute() or path.resolve() != path or path.is_symlink()
                    or not path.is_file() or _sha(path) != artifacts.get(hash_key)):
                raise RuntimeError(f"artifact path/hash mismatch: {path_key}")
            if path_key == "truss_wheel":
                with zipfile.ZipFile(path) as wheel:
                    runtime_sha = hashlib.sha256(wheel.read("truss/local_runtime.py")).hexdigest()
                if runtime_sha != scope["qualification_stage"]["truss_local_runtime_member_sha256"]:
                    raise RuntimeError("Truss .53 deterministic socket runtime hash differs")
    if artifacts.get("bun_executable"):
        bun = Path(artifacts["bun_executable"])
        if not bun.is_file() or not os.access(bun, os.X_OK):
            raise RuntimeError("selected Bun executable unavailable")
        observed_version = subprocess.run([str(bun), "--version"], check=True, capture_output=True,
                                          text=True, timeout=5).stdout.strip()
        if observed_version != artifacts.get("bun_version"):
            raise RuntimeError("selected Bun version differs from scope")
    python_path = artifacts.get("python_executable")
    if python_path:
        python_file = Path(python_path)
        if (not python_file.is_absolute() or not python_file.exists()
                or not python_file.resolve().is_file() or not os.access(python_file, os.X_OK)
                or _sha(python_file.resolve()) != artifacts.get("python_sha256")):
            raise RuntimeError("selected worker Python path/hash mismatch")
    for key, hash_key in (("runtime_manifest", "runtime_manifest_sha256"),
                          ("postgresql_18_runtime_manifest", "postgresql_18_runtime_manifest_sha256")):
        selected = artifacts.get(key)
        if selected:
            path = Path(selected)
            if (not path.is_absolute() or path.resolve() != path or path.is_symlink()
                    or not path.is_file() or _sha(path) != artifacts.get(hash_key)):
                raise RuntimeError(f"runtime manifest path/hash mismatch: {key}")
    postgres_runtime = artifacts.get("postgresql_18_runtime")
    if postgres_runtime:
        runtime = Path(postgres_runtime)
        manifest = json.loads(Path(artifacts["postgresql_18_runtime_manifest"]).read_text())
        if not runtime.is_absolute() or runtime.resolve() != runtime or runtime.is_symlink() or not runtime.is_dir():
            raise RuntimeError("canonical selected PostgreSQL 18 runtime directory required")
        for binary, key in (("pg_ctl", "pg_ctl_sha256"), ("postgres", "postgres_sha256")):
            binary_path = runtime / "bin" / binary
            if (not binary_path.is_file() or not os.access(binary_path, os.X_OK)
                    or _sha(binary_path) != manifest.get(key)):
                raise RuntimeError(f"selected PostgreSQL runtime binary hash mismatch: {binary}")
        if not str(manifest.get("postgresql_version", "")).startswith("18."):
            raise RuntimeError("selected PostgreSQL runtime manifest is not major 18")
    if artifacts.get("upstream_candidate_review") != "accepted":
        missing.append("accepted_upstream_candidate_review")
    if artifacts.get("upstream_candidate_commits") != [
            "0f109814091aa6da54f4278e7683a8c82338dd93"]:
        raise RuntimeError("reviewed upstream candidate commit differs")
    for candidate_commit in artifacts["upstream_candidate_commits"]:
        integrated = subprocess.run(["git", "merge-base", "--is-ancestor", candidate_commit, "HEAD"],
                                    cwd=REPO, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if integrated.returncode:
            missing.append("upstream_candidate_not_integrated:" + candidate_commit[:12])
    limits = scope["limits"]
    if (limits.get("wall_seconds_including_cleanup") != 900
            or limits.get("aggregate_rss_bytes") != 4 * 1024**3
            or limits.get("disposable_output_bytes") != 512 * 1024**2
            or limits.get("attempts") != 1):
        raise RuntimeError("R61 hard bounds differ from reviewed limits")
    state = scope["fresh_local_state"]
    for key in ("root", "postgres_data", "delta_root", "evidence_root"):
        candidate = Path(state[key])
        if not candidate.is_absolute() or candidate.resolve() != candidate:
            raise RuntimeError(f"noncanonical fresh path: {key}")
        for forbidden in state["forbidden_state"]:
            denied = Path(forbidden).resolve()
            if candidate == denied or candidate in denied.parents or denied in candidate.parents:
                raise RuntimeError("fresh run path overlaps retained state")
        if candidate.exists() or candidate.is_symlink():
            raise RuntimeError(f"fresh output path already exists: {key}")
    pgdata = Path(state["postgres_data"])
    runtime_root = Path("/private/tmp" if sys.platform == "darwin" else "/tmp") / f"truss-pg18-{os.getuid()}"
    socket_id = hashlib.sha256(os.fsencode(pgdata)).hexdigest()[:24]
    socket_dir = runtime_root / socket_id
    lease = Path(str(pgdata) + ".truss-runtime.lock")
    if runtime_root.exists() or runtime_root.is_symlink():
        root_info = runtime_root.lstat()
        if (not stat.S_ISDIR(root_info.st_mode) or stat.S_ISLNK(root_info.st_mode)
                or root_info.st_uid != os.getuid() or stat.S_IMODE(root_info.st_mode) != 0o700):
            raise RuntimeError("Truss runtime socket root ownership/mode differs")
    if socket_dir.exists() or socket_dir.is_symlink() or lease.exists() or lease.is_symlink():
        raise RuntimeError("fresh Truss deterministic socket/lease is already present")
    return missing, scope


def preflight() -> dict:
    scope = json.loads(SCOPE.read_text())
    missing, _ = _validate_scope(scope)
    psutil = _pinned_psutil(scope)
    psutil_path = Path(psutil.__file__).resolve()
    return {"status": "blocked_pending_candidate" if missing else "review_gate_required",
            "effect_free": True, "native_dispatch_available": False,
            "missing_bindings": sorted(set(missing)),
            "bounds_enforced_by_supervisor": True,
            "limits": scope["limits"], "output_root_absent": True,
            "truss_socket_directory_absent": True,
            "supervisor_psutil": {"version": psutil.__version__,
                                  "module_path": str(psutil_path),
                                  "module_sha256": _sha(psutil_path)},
            "postgres_and_spark_started": False}


def _verify_dispatch(path: Path, scope: dict, worker: Path) -> dict:
    if (not path.is_absolute() or path.resolve() != path or path.is_symlink()
            or not path.is_file() or path.stat().st_size > 32768
            or path.stat().st_uid != os.getuid()):
        raise RuntimeError("canonical reviewed dispatch receipt required")
    receipt = json.loads(path.read_text())
    expected = {"status": "accepted", "scope_sha256": _sha(SCOPE),
                "supervisor_sha256": _sha(Path(__file__).resolve()),
                "worker_sha256": _sha(worker), "attempts": 1,
                "wall_seconds": 900, "aggregate_rss_bytes": 4 * 1024**3,
                "disposable_output_bytes": 512 * 1024**2,
                "fresh_root": scope["fresh_local_state"]["root"]}
    expected["artifact_hashes"] = {
        key: scope["artifacts"].get(key) for key in (
            "truss_wheel_sha256", "umf_sdk_sha256", "umf_core_sha256",
            "python_sha256", "runtime_manifest_sha256", "ashlar_candidate_wheel_sha256",
            "graphframes_runtime_manifest_sha256", "bun_sha256",
            "postgresql_18_runtime_manifest_sha256")}
    psutil = _pinned_psutil(scope)
    expected["supervisor_psutil"] = {"version": psutil.__version__,
                                     "module_sha256": _sha(Path(psutil.__file__).resolve())}
    for key, value in expected.items():
        if receipt.get(key) != value:
            raise RuntimeError(f"dispatch receipt binding mismatch: {key}")
    return receipt


def run_once(dispatch_path: Path) -> int:
    scope = json.loads(SCOPE.read_text())
    missing, _ = _validate_scope(scope)
    if missing:
        raise RuntimeError(f"native dispatch blocked; missing reviewed bindings: {sorted(set(missing))}")
    artifacts = scope["artifacts"]
    worker = Path(artifacts["worker_path"])
    if (not worker.is_absolute() or worker.resolve() != worker or worker.is_symlink()
            or not worker.is_file() or not worker.is_relative_to(REPO)
            or _sha(worker) != artifacts["worker_sha256"]):
        raise RuntimeError("bound one-shot worker does not match scope")
    _verify_dispatch(dispatch_path, scope, worker)
    psutil = _pinned_psutil(scope)
    root = Path(scope["fresh_local_state"]["root"])
    if root.exists() or root.is_symlink():
        raise RuntimeError("fresh disposable root must be absent")
    selected_python = Path(artifacts["python_executable"])
    pg_runtime = Path(artifacts["postgresql_18_runtime"])
    pg_bin = pg_runtime / "bin"
    bun_parent = str(Path(artifacts["bun_executable"]).parent)
    env = {"PATH": os.pathsep.join((str(selected_python.parent), bun_parent,
                                    str(pg_bin), "/usr/bin", "/bin")),
           "HOME": str(root / "home"), "TMPDIR": str(root / "tmp"),
           "PYTHONNOUSERSITE": "1", "PYTHONDONTWRITEBYTECODE": "1",
           "LC_ALL": "C.UTF-8", "R61_SCOPE": str(SCOPE),
           "R61_OUTPUT_ROOT": str(root), "R61_DISPATCH_RECEIPT": str(dispatch_path)}
    for key in ("truss_wheel", "umf_sdk_artifact", "umf_core_platform_artifact",
                "runtime_manifest", "python_executable", "bun_executable",
                "postgresql_18_runtime", "postgresql_18_runtime_manifest",
                "ashlar_candidate_wheel", "graphframes_runtime_manifest"):
        env["R61_" + key.upper()] = str(artifacts[key])
    started = time.monotonic()
    deadline = started + 900
    log = root / "worker.log"
    receipt_path = root / RECEIPT_NAME
    receipt_fd = None
    global RUN_ONCE_STARTED, RUN_RECEIPT_PATH
    RUN_ONCE_STARTED = True
    RUN_RECEIPT_PATH = str(receipt_path)
    process = None
    peak_rss = 0
    result = {"status": "failed", "scope_sha256": _sha(SCOPE),
              "dispatch_receipt_sha256": _sha(dispatch_path),
              "worker_sha256": _sha(worker), "native_started": False}
    try:
        root.mkdir(mode=0o700)
        # Keep this descriptor open so every post-dispatch outcome has a
        # durable failure/success receipt even after the worker deadline.
        receipt_fd = os.open(receipt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                             getattr(os, "O_NOFOLLOW", 0), 0o600)
        (root / "home").mkdir(mode=0o700)
        (root / "tmp").mkdir(mode=0o700)
        log_fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(log_fd, "wb") as stream:
            process = subprocess.Popen([str(selected_python), str(worker)], cwd=REPO, env=env,
                                       stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, start_new_session=True, close_fds=True)
            result["native_started"] = True
            timed_out = rss_exceeded = output_exceeded = False
            captured = 0
            selector = selectors.DefaultSelector()
            assert process.stdout is not None
            selector.register(process.stdout, selectors.EVENT_READ)
            while process.poll() is None or selector.get_map():
                rss, group_pids, pg_pids = _group_snapshot(
                    psutil, process.pid, Path(scope["fresh_local_state"]["postgres_data"]))
                peak_rss = max(peak_rss, rss)
                if rss >= peak_rss:
                    result["peak_process_group_pids"] = group_pids[:256]
                    result["peak_process_group_pid_count"] = len(group_pids)
                    result["peak_postgres_tree_pids"] = pg_pids[:256]
                    result["peak_postgres_tree_pid_count"] = len(pg_pids)
                if rss > 4 * 1024**3:
                    rss_exceeded = True
                    break
                # The stream is capped independently so a fast writer cannot
                # overshoot the aggregate output bound between filesystem polls.
                other_bytes = _tree_size(root, stop_after=512 * 1024**2,
                                         exclude=log, deadline=deadline)
                if other_bytes + captured > 512 * 1024**2 - 65536:
                    output_exceeded = True
                    break
                remaining = deadline - time.monotonic()
                if remaining <= TERM_GRACE + KILL_GRACE + PG_STOP_SECONDS + FINAL_RESERVE_SECONDS:
                    timed_out = True
                    break
                for key, _ in selector.select(min(RSS_POLL, remaining)):
                    block = os.read(key.fd, min(65536, MAX_LOG_BYTES - captured + 1))
                    if not block:
                        selector.unregister(key.fileobj)
                        continue
                    allowed = min(len(block), MAX_LOG_BYTES - captured)
                    if allowed:
                        stream.write(block[:allowed])
                        captured += allowed
                    if allowed != len(block):
                        output_exceeded = True
                        break
                if output_exceeded:
                    break
            try:
                cleanup = _terminate_group(process, deadline)
            except BaseException as error:
                cleanup = {"group_empty": False, "cleanup_error": f"{type(error).__name__}: {error}"}
            try:
                pg_cleanup = _close_owned_postgres(scope, artifacts, deadline)
            except BaseException as error:
                pg_cleanup = {"postgres_closed": False,
                              "cleanup_error": f"{type(error).__name__}: {error}"}
            result.update({"worker_returncode": process.returncode,
                           "timed_out": timed_out, "rss_exceeded": rss_exceeded,
                           "output_exceeded": output_exceeded, "peak_group_rss_bytes": peak_rss,
                           "captured_log_bytes": captured,
                           "cleanup": cleanup, "elapsed_seconds": round(time.monotonic() - started, 3)})
            result["postgres_cleanup"] = pg_cleanup
            if time.monotonic() > deadline:
                result["status"] = "failed_cleanup_deadline_exceeded_state_uncertain"
            elif not cleanup["group_empty"] or not pg_cleanup["postgres_closed"]:
                result["status"] = "failed_owner_cleanup_incomplete_state_uncertain"
            elif timed_out or rss_exceeded or output_exceeded:
                result["status"] = "failed_bound_exceeded"
            elif process.returncode != 0:
                result["status"] = "failed_worker"
            else:
                worker_result_path = root / "worker-completion.json"
                if (not worker_result_path.is_file() or worker_result_path.is_symlink()
                        or worker_result_path.stat().st_uid != os.getuid()):
                    result["status"] = "failed_worker_completion_receipt_missing"
                elif worker_result_path.stat().st_size > 32768:
                    result["status"] = "failed_worker_reconciliation_receipt_oversized"
                else:
                    info = worker_result_path.stat()
                    if (not worker_result_path.is_file() or info.st_uid != os.getuid()
                            or stat.S_IMODE(info.st_mode) != 0o600):
                        raise RuntimeError("worker completion receipt must be a private owned file")
                    worker_result = json.loads(worker_result_path.read_text())
                    required_true = ("all_owners_closed", "source_state_reconciled",
                                     "publication_vector_reconciled", "ack_reconciled",
                                     "no_delta_write_after_close")
                    graph_hashes = (worker_result.get("graph_release_sha256"),
                                    worker_result.get("graph_custody_sha256"),
                                    worker_result.get("graphframes_action_sha256")) if isinstance(worker_result, dict) else ()
                    worker_artifact_hashes = worker_result.get("artifact_hashes", {}) if isinstance(worker_result, dict) else {}
                    bound_artifact_keys = ("truss_wheel_sha256", "truss_release_manifest_sha256",
                                           "umf_sdk_sha256", "umf_core_sha256", "ashlar_candidate_wheel_sha256",
                                           "runtime_manifest_sha256", "graphframes_runtime_manifest_sha256",
                                           "bun_sha256", "postgresql_18_runtime_manifest_sha256")
                    if (not isinstance(worker_result, dict)
                            or worker_result.get("status") != "completed"
                            or any(worker_result.get(key) is not True for key in required_true)
                            or not all(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value)
                                       for value in graph_hashes)
                            or worker_result.get("installed_ashlar_wheel_sha256") != artifacts["ashlar_candidate_wheel_sha256"]
                            or worker_result.get("graphframes_ashlar_wheel_sha256") != artifacts["ashlar_candidate_wheel_sha256"]
                            or worker_result.get("truss_wheel_sha256") != artifacts["truss_wheel_sha256"]
                            or worker_result.get("runtime_manifest_sha256") != artifacts["runtime_manifest_sha256"]
                            or any(worker_artifact_hashes.get(key) != artifacts.get(key)
                                   for key in bound_artifact_keys)):
                        result["status"] = "failed_worker_reconciliation_receipt"
                    else:
                        result["status"] = "worker_completed_reconciliation_required"
                        result["worker_completion_sha256"] = _sha(worker_result_path)
            result["log_sha256"] = _sha(log)
            result["log_bytes"] = log.stat().st_size
            result["output_bytes_before_receipt"] = _tree_size(root, stop_after=512 * 1024**2,
                                                               exclude=receipt_path)
    except BaseException as exc:
        result["status"] = "failed_supervisor_exception"
        result["failure"] = f"{type(exc).__name__}: {exc}"
        if process is not None:
            try:
                result["cleanup"] = _terminate_group(process, deadline)
            except BaseException as cleanup_error:
                result["cleanup"] = {"group_empty": False,
                                      "cleanup_error": f"{type(cleanup_error).__name__}: {cleanup_error}"}
            try:
                result["postgres_cleanup"] = _close_owned_postgres(scope, artifacts, deadline)
            except BaseException as cleanup_error:
                result["postgres_cleanup"] = {"postgres_closed": False,
                                              "cleanup_error": f"{type(cleanup_error).__name__}: {cleanup_error}"}
        if log.exists():
            result["log_sha256"] = _sha(log)
            result["log_bytes"] = log.stat().st_size
    finally:
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)
        try:
            if root.exists() and time.monotonic() < deadline:
                result["final_output_bytes"] = _tree_size(root, stop_after=512 * 1024**2,
                                                          exclude=receipt_path, deadline=deadline)
                if result["final_output_bytes"] > 512 * 1024**2 - 65536:
                    result["status"] = "failed_output_limit_receipt_reserve"
            elif root.exists():
                result["final_output_census"] = "skipped_after_deadline"
                if result["status"] == "worker_completed_reconciliation_required":
                    result["status"] = "failed_deadline_during_final_census"
        except BaseException as error:
            result["final_output_census_error"] = f"{type(error).__name__}: {error}"
            if result["status"] == "worker_completed_reconciliation_required":
                result["status"] = "failed_final_census_indeterminate"
        if receipt_fd is not None:
            try:
                _write_reserved_receipt(receipt_fd, receipt_path, result)
            finally:
                os.close(receipt_fd)
        elif root.exists():
            result["receipt_unavailable"] = True
            fallback = root / "supervisor-result-unreserved.json"
            try:
                fallback_fd = os.open(fallback, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                                      getattr(os, "O_NOFOLLOW", 0), 0o600)
                try:
                    encoded = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
                    remaining = memoryview(encoded)
                    while remaining:
                        remaining = remaining[os.write(fallback_fd, remaining):]
                    os.fsync(fallback_fd)
                finally:
                    os.close(fallback_fd)
            except OSError:
                result["receipt_unavailable"] = True
    return 0 if result["status"] == "worker_completed_reconciliation_required" else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--preflight-only", action="store_true")
    mode.add_argument("--run-once", action="store_true")
    parser.add_argument("--dispatch-receipt", type=Path)
    args = parser.parse_args()
    if args.preflight_only:
        print(json.dumps(preflight(), sort_keys=True, separators=(",", ":")))
        return 0
    if args.dispatch_receipt is None:
        raise RuntimeError("--run-once requires a separately reviewed dispatch receipt")
    return run_once(args.dispatch_receipt)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"status": "refused" if not RUN_ONCE_STARTED else "indeterminate_after_dispatch_gate",
                          "effect_free": not RUN_ONCE_STARTED, "reason": str(exc),
                          "durable_receipt_expected": RUN_ONCE_STARTED,
                          "receipt_path": RUN_RECEIPT_PATH},
                         sort_keys=True, separators=(",", ":")), file=sys.stderr)
        raise SystemExit(2)
