#!/usr/bin/env python3
"""One-child wall/RSS/storage supervisor for the reviewed r6 .51 worker."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
SCOPE_PATH = HERE / "r7-truss051-migration-scope.json"
WORKER = HERE / "r7_truss051_local_worker.py"
PYTHON = "/private/tmp/ashlar-r19-truss051-runtime-cohort-20261031/bin/python"
PYTHONPATH = "/private/tmp/ashlar-r19-truss051-runtime-cohort-20261031/lib/python3.12/site-packages"
EXPECTED_PATHS = {
    "output_root": "/private/tmp/ashlar-r7-truss051-continuation-20261031",
    "supervisor_root": "/private/tmp/ashlar-r7-truss051-continuation-20261031-supervisor",
    "cold_backup_pgdata": "/private/tmp/ashlar-r7-truss051-continuation-20261031/cold-backup-pgdata",
    "migration_clone_pgdata": "/private/tmp/ashlar-r7-truss051-continuation-20261031/migration-clone-pgdata",
    "recovery_registry": "/private/tmp/ashlar-r7-truss051-continuation-20261031/truss-recovery-registry.sqlite3",
    "receipt_root": "/private/tmp/ashlar-r7-truss051-continuation-20261031/receipts",
}
MAX_SECONDS = 900
MAX_RSS = 2 * 1024 * 1024 * 1024
MAX_ATTEMPT = 512 * 1024 * 1024
MAX_AGGREGATE = 2 * 1024 * 1024 * 1024
MAX_LOGS = 16 * 1024 * 1024


class StopSupervisor(RuntimeError):
    pass


def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise StopSupervisor(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def size_tree(root: Path) -> int:
    if not root.exists():
        return 0
    total = 0
    for base, dirs, files in os.walk(root, followlinks=False):
        dirs.sort(); files.sort()
        for name in dirs + files:
            path = Path(base) / name
            if path.is_symlink():
                raise StopSupervisor(f"symlink appeared in attempt tree: {path}")
            if path.is_file():
                total += path.stat().st_size
            elif not path.is_dir():
                raise StopSupervisor(f"special file appeared in attempt tree: {path}")
            if total > MAX_ATTEMPT:
                raise StopSupervisor("attempt tree exceeded 512 MiB")
    return total


def rss_group(pgid: int) -> tuple[int, list[int]]:
    result = subprocess.run(["/bin/ps", "-axo", "pid=,pgid=,rss="], check=True,
                            text=True, capture_output=True, timeout=5)
    total = 0; pids = []
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) != 3:
            continue
        pid, group, rss_kib = map(int, fields)
        if group == pgid:
            pids.append(pid); total += rss_kib * 1024
    return total, pids


def pgdata_processes(path: str) -> list[str]:
    result = subprocess.run(["/bin/ps", "-Ao", "pid=,command="], check=True,
                            text=True, capture_output=True, timeout=5)
    return [line.strip() for line in result.stdout.splitlines()
            if path in line and ("postgres" in line.lower() or "pg_ctl" in line.lower())]


def terminate_group(pgid: int) -> list[int]:
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return []
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            _, pids = rss_group(pgid)
            if not pids:
                return []
        except BaseException:
            pass
        time.sleep(0.2)
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        return []
    time.sleep(0.2)
    try:
        _, pids = rss_group(pgid)
        return pids
    except BaseException:
        return [-1]


def main() -> int:
    scope_raw = SCOPE_PATH.read_bytes()
    scope = json.loads(scope_raw, object_pairs_hook=unique_pairs)
    if (scope.get("paths") != EXPECTED_PATHS
            or scope.get("source", {}).get("existing_pgdata") != "/private/tmp/ashlar-truss048-fresh-local-20261031-r6/pgdata"):
        raise StopSupervisor("exact reviewed source and attempt paths required")
    if (scope.get("runtime", {}).get("python_path") != PYTHON
            or scope["runtime"].get("pythonpath") != PYTHONPATH):
        raise StopSupervisor("exact pinned .51 Python executable/site-packages required")
    if (scope.get("status") != "approved_one_shot_native"
            or scope.get("authorization") != "one_run_local_native_lifecycle"):
        raise StopSupervisor("review gate closed: no native dispatch")
    if scope.get("limits") != {
        "attempts":1,"automatic_retries":0,"wall_seconds":900,
        "process_group_rss_bytes":2147483648,"retained_plus_attempt_bytes":2147483648,
        "attempt_root_bytes":536870912,"migration_preservation_bytes":268435456,
        "combined_log_bytes":16777216,"parallelism":"sequential local only",
        "cloud":False,"cleanup":False}:
        raise StopSupervisor("exact reviewed wall/RSS/storage limits required")
    approval_path = HERE / "r7-truss051-native-approval.json"
    approval = json.loads(approval_path.read_text(), object_pairs_hook=unique_pairs)
    if (approval.get("scope_sha256") != sha(scope_raw)
            or approval.get("worker_sha256") != sha(WORKER.read_bytes())
            or approval.get("supervisor_sha256") != sha(Path(__file__).read_bytes())
            or approval.get("attempts") != 1 or approval.get("cloud") is not False):
        raise StopSupervisor("exact scope/worker/supervisor approval binding required")
    if os.environ.get("PYTHONPATH") != PYTHONPATH:
        raise StopSupervisor("exact pinned .51 Python environment required")
    supervisor_root = Path(scope["paths"]["supervisor_root"])
    output_root = Path(scope["paths"]["output_root"])
    if any(path.exists() or path.is_symlink() for path in (supervisor_root, output_root)):
        raise StopSupervisor("fresh supervisor and worker output roots must be absent")
    # The immediate complete PGDATA owner inventory is repeated inside the
    # worker directly before the cold copy. This preflight also refuses to run
    # if the OS process inventory itself is unavailable.
    subprocess.run(["/bin/ps", "-Ao", "pid=,command="], check=True,
                   text=True, capture_output=True, timeout=5)
    supervisor_root.mkdir(mode=0o700)
    stdout_path = supervisor_root / "child.stdout.log"
    stderr_path = supervisor_root / "child.stderr.log"
    start = time.monotonic()
    receipt = {"status": "running", "scope_sha256": sha(scope_raw),
               "worker_sha256": sha(WORKER.read_bytes()), "child_started": False,
               "cloud": False, "publication": False, "acknowledgement": False}
    try:
        with stdout_path.open("xb") as out, stderr_path.open("xb") as err:
            child = subprocess.Popen([PYTHON, str(WORKER)], env=os.environ.copy(),
                                     stdout=out, stderr=err, start_new_session=True,
                                     close_fds=True)
            receipt["child_started"] = True
            receipt["child_pid"] = child.pid
            max_rss = max_tree = max_logs = 0
            while child.poll() is None:
                if time.monotonic() - start > MAX_SECONDS:
                    raise StopSupervisor("900-second wall limit exceeded")
                rss, pids = rss_group(child.pid)
                max_rss = max(max_rss, rss)
                if rss > MAX_RSS:
                    raise StopSupervisor("process-group RSS exceeded 2 GiB")
                current_tree = size_tree(output_root)
                max_tree = max(max_tree, current_tree)
                logs = stdout_path.stat().st_size + stderr_path.stat().st_size
                max_logs = max(max_logs, logs)
                if logs > MAX_LOGS:
                    raise StopSupervisor("combined child logs exceeded 16 MiB")
                if current_tree + size_tree(supervisor_root) > MAX_ATTEMPT:
                    raise StopSupervisor("combined attempt and supervisor output exceeded 512 MiB")
                retained = size_tree(Path(scope["source"]["existing_pgdata"]))
                if (retained + size_tree(Path(scope["paths"]["cold_backup_pgdata"]))
                        + size_tree(Path(scope["paths"]["migration_clone_pgdata"]))
                        + current_tree + size_tree(supervisor_root) > MAX_AGGREGATE):
                    raise StopSupervisor("retained plus attempt storage exceeded 2 GiB")
                time.sleep(1)
            code = child.wait(timeout=5)
        rss, remaining = rss_group(child.pid)
        if remaining:
            raise StopSupervisor(f"child exited with process-group members remaining: {remaining!r}")
        owners = pgdata_processes(scope["paths"]["migration_clone_pgdata"])
        if owners:
            raise StopSupervisor(f"PostgreSQL process still references migration clone: {owners[:4]!r}")
        if code != 0:
            raise StopSupervisor(f"continuation worker exited {code}")
        receipt.update(status="completed", child_exit_code=code, maximum_rss_bytes=max_rss,
                       maximum_attempt_tree_bytes=max_tree, maximum_logs_bytes=max_logs,
                       elapsed_seconds=round(time.monotonic() - start, 3))
    except BaseException as error:
        if receipt.get("child_started"):
            remaining = terminate_group(receipt["child_pid"])
        else:
            remaining = []
        try:
            owners = pgdata_processes(scope["paths"]["migration_clone_pgdata"])
        except BaseException as custody_error:
            owners = [f"process inventory failed: {type(custody_error).__name__}: {custody_error}"]
        receipt.update(status="failed_or_interrupted", failure_type=type(error).__name__,
                       failure=str(error)[:1500], remaining_process_group_pids=remaining,
                       migration_clone_owners=owners,
                       elapsed_seconds=round(time.monotonic() - start, 3),
                       effects_may_remain=receipt.get("child_started", False))
        if remaining:
            receipt["custody_status"] = "unresolved_processes_remain_do_not_retry"
        (supervisor_root / "supervisor-receipt.json").write_text(json.dumps(receipt, sort_keys=True) + "\n")
        raise
    (supervisor_root / "supervisor-receipt.json").write_text(json.dumps(receipt, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
