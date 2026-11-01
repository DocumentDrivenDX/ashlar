#!/usr/bin/env python3
"""R8-derived bounded supervisor for R12 continuation of committed R10 migration."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import signal
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
SCOPE_PATH = HERE / "r12-truss051-resume-scope.json"
WORKER = HERE / "r12_truss051_local_worker.py"
PYTHON = "/private/tmp/ashlar-r19-truss051-runtime-cohort-20261031/bin/python"
PYTHONPATH = "/private/tmp/ashlar-r9-truss051-current-runtime-20261101/site-packages:/private/tmp/ashlar-r19-truss051-runtime-cohort-20261031/lib/python3.12/site-packages"
EXPECTED_PATHS = {
    "output_root": "/private/tmp/ashlar-r12-truss051-current-cohort-20261101",
    "supervisor_root": "/private/tmp/ashlar-r12-truss051-current-cohort-20261101-supervisor",
    "cold_backup_pgdata": "/private/tmp/ashlar-r12-truss051-current-cohort-20261101/r10-migrated-backup-pgdata",
    "migration_clone_pgdata": "/private/tmp/ashlar-r12-truss051-current-cohort-20261101/resume-clone-pgdata",
    "recovery_registry": "/private/tmp/ashlar-r12-truss051-current-cohort-20261101/truss-recovery-registry.sqlite3",
    "receipt_root": "/private/tmp/ashlar-r12-truss051-current-cohort-20261101/receipts",
}
MAX_SECONDS = 900
MAX_RSS = 2 * 1024 * 1024 * 1024
MAX_ATTEMPT = 512 * 1024 * 1024
MAX_AGGREGATE = 2 * 1024 * 1024 * 1024
MAX_LOGS = 16 * 1024 * 1024
STORAGE_SAMPLE_SECONDS = 15.0
PG_CTL = "/private/tmp/ashlar-r9-truss051-current-runtime-20261101/site-packages/pgserver/pginstall/bin/pg_ctl"


class StopSupervisor(RuntimeError):
    pass


def validate_standing_authorization(scope: dict, scope_raw: bytes, *, receipt=None,
                                     worker_raw=None, supervisor_raw=None) -> None:
    """Require standing user authority and the exact Sol-reviewed dispatch binding."""
    if (scope.get("status") != "standing_local_native_authorization"
            or scope.get("authorization") != "standing_user_local_release_authorization"
            or scope.get("authorization_receipt") != "r12-standing-authorization-dispatch-receipt.json"):
        raise StopSupervisor("standing local authorization scope identity mismatch")
    if receipt is None:
        receipt_path = HERE / scope["authorization_receipt"]
        receipt = json.loads(receipt_path.read_text(), object_pairs_hook=unique_pairs)
    worker_raw = worker_raw if worker_raw is not None else WORKER.read_bytes()
    supervisor_raw = supervisor_raw if supervisor_raw is not None else Path(__file__).read_bytes()
    expected = {
        "schema": "ashlar.standing-local-dispatch-binding/1",
        "authorization_basis": "standing_user_local_release_authorization",
        "user_authorization_statement": "User authorized fresh Truss source creation, a local seed, and a tested release; bounded local R12 resume execution is within that standing authorization and requires no per-attempt user approval.",
        "scope_sha256": sha(scope_raw),
        "worker_sha256": sha(worker_raw),
        "supervisor_sha256": sha(supervisor_raw),
        "attempts": 1,
        "cloud": False,
    }
    for key, value in expected.items():
        if receipt.get(key) != value:
            raise StopSupervisor(f"standing authorization binding mismatch: {key}")
    if set(receipt) != (set(expected) | {"sol_review_status", "dispatch_enabled"}):
        raise StopSupervisor("standing authorization receipt has unexpected fields")
    if receipt.get("sol_review_status") != "sol_reviewed" or receipt.get("dispatch_enabled") is not True:
        raise StopSupervisor("Sol review receipt is pending; native dispatch remains disabled")


def validate_integration_binding(release: dict) -> None:
    """Bind reviewed SDK0101 commit plus the reviewed source-tree correction."""
    repo = HERE.parents[5]
    integration = "afeb78c41679454730a3bf0129db6f438191953a"
    integration_tree = "08862e9d2f3482f505cec40ed266f5e77cad0192"
    correction = "64f28c3b04803eadcac0f920cd2b8bb3f84a6f43"
    correction_tree = "28138503d679a480f7af9de8aac25b8ab0884f14"
    try:
        trees = [subprocess.run(["git", "-C", str(repo), "rev-parse", f"{commit}^{{tree}}"],
                                check=True, text=True, capture_output=True, timeout=10).stdout.strip()
                 for commit in (integration, correction)]
        ancestors = [subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", commit, "HEAD"],
                                    text=True, capture_output=True, timeout=10)
                    for commit in (integration, correction)]
        original_paths = subprocess.run(["git", "-C", str(repo), "show", "--format=", "--name-only", integration],
                                        check=True, text=True, capture_output=True, timeout=10).stdout.splitlines()
        corrected_paths = subprocess.run(["git", "-C", str(repo), "show", "--format=", "--name-only", correction],
                                         check=True, text=True, capture_output=True, timeout=10).stdout.splitlines()
    except (OSError, subprocess.SubprocessError) as error:
        raise StopSupervisor("cannot prove reviewed SDK0101/source-custody commit ancestry") from error
    expected_paths = sorted(path for path in original_paths if path and not path.startswith("docs/"))
    corrected_paths = {path for path in corrected_paths if path and not path.startswith("docs/")}
    bound = release.get("integration_files")
    if (trees != [integration_tree, correction_tree] or any(item.returncode != 0 for item in ancestors)
            or not isinstance(bound, dict)):
        raise StopSupervisor("reviewed SDK0101/source-custody commit/tree is not in current checkout ancestry")
    if not corrected_paths.issubset(expected_paths) or sorted(bound) != expected_paths:
        raise StopSupervisor("SDK0101 integration file set differs from reviewed commit")
    for relative in expected_paths:
        path = (repo / relative).resolve()
        blob_commit = correction if relative in corrected_paths else integration
        try:
            path.relative_to(repo.resolve())
            actual = hashlib.sha256(path.read_bytes()).hexdigest()
            committed_bytes = subprocess.run(
                ["git", "-C", str(repo), "show", f"{blob_commit}:{relative}"],
                check=True, capture_output=True, timeout=10).stdout
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            raise StopSupervisor(f"cannot verify SDK0101 integration file: {relative}") from error
        if actual != bound[relative] or hashlib.sha256(committed_bytes).hexdigest() != bound[relative]:
            raise StopSupervisor(f"SDK0101 integration file hash mismatch: {relative}")
    dirty = subprocess.run(["git", "-C", str(repo), "status", "--porcelain", "--untracked-files=no", "--", *expected_paths],
                           check=True, text=True, capture_output=True, timeout=10).stdout
    if dirty.strip():
        raise StopSupervisor("SDK0101 integration paths have local modifications")

def validate_release_scope(scope: dict) -> None:
    expected = {
        "sdk_version": "0.10.1",
        "sdk_archive_sha256": "3d95fff822f4f44b3d1a8e56185c3697e94ee66f5315cda7389f5301f3d16d26",
        "sdk_public_entry_sha256": "7577c4efe5269d9f98187265b31be71f2be69b67cab357ce9bf07dabcd758f27",
        "source_commit_declared": "90cb61eb63aec3f5a77b5991a56e3dfc3d10bd27",
        "source_tree_object": "24d2531cbe9025fdc07fb1321a82a81ae31ff12a",
        "source_custody_correction_commit": "64f28c3b04803eadcac0f920cd2b8bb3f84a6f43",
        "source_custody_correction_tree": "28138503d679a480f7af9de8aac25b8ab0884f14",
        "ashlar_integration_commit": "afeb78c41679454730a3bf0129db6f438191953a",
        "ashlar_integration_tree": "08862e9d2f3482f505cec40ed266f5e77cad0192",
        "ashlar_runtime_profile": "ashlar-local-current-installation-profile/0.4",
        "semantic_core_version": "0.10.0",
        "python_core_distribution": "umf-core",
        "python_core_version": "0.10.5",
        "python_core_wheel_sha256": "9213ff4b87649f551abcfdff40cdb6b0d75d619260988fa8ea4fe26cac7f6788",
    }
    release = scope.get("release_cohort", {})
    for key, value in expected.items():
        if release.get(key) != value:
            raise StopSupervisor(f"current UMF cohort mismatch: {key}")
    validate_integration_binding(release)
    if release.get("runtime_integration") != "Sol-reviewed SDK0101 runtime integration bound":
        raise StopSupervisor("Sol-reviewed SDK0101 runtime integration binding mismatch")
    if release.get("runtime_proof") != "r12-runtime-proof.json":
        raise StopSupervisor("exact R12 current-runtime proof required")
    proof_path = (HERE / release["runtime_proof"]).resolve()
    if (proof_path.parent != HERE
            or hashlib.sha256(proof_path.read_bytes()).hexdigest() != release.get("runtime_proof_sha256")):
        raise StopSupervisor("R12 current-runtime proof digest mismatch")
    proof = json.loads(proof_path.read_text(), object_pairs_hook=unique_pairs)
    if (proof.get("result") != "passed"
            or proof.get("r9_overlay", {}).get("core_version") != "0.10.5"
            or proof.get("r9_overlay", {}).get("core_wheel_sha256") != expected["python_core_wheel_sha256"]
            or proof.get("truss", {}).get("version") != "0.0.51"
            or proof.get("ashlar_integration", {}).get("upstream_source_tree_object") != expected["source_tree_object"]
            or proof.get("ashlar_integration", {}).get("source_custody_correction_commit") != expected["source_custody_correction_commit"]
            or proof.get("ashlar_integration", {}).get("source_custody_correction_tree") != expected["source_custody_correction_tree"]
            or proof.get("effects", {}).get("native_postgres_started") is not False):
        raise StopSupervisor("R12 current-runtime proof fields mismatch")
    accepted = scope.get("source", {}).get("catalog_accepted", {})
    if (accepted.get("document_id") != "ashlar-local-dev-typed-graph-r6-20261031"
            or accepted.get("document_revision") != "1"
            or accepted.get("document_sha256") != "2c9b295a8232cae4770255650bf634941ab223e582b79ae2bb78f6acf0b8a880"
            or scope.get("release_cohort", {}).get("semantic_core_version") != "0.10.0"):
        raise StopSupervisor("accepted R6 .48 source identity/semantic version changed")
    migration = scope.get("migration", {})
    if (migration.get("r6_truss048_accepted_document_retained") is not True
            or not migration.get("truss051_accepted_document_readback_state", "").startswith("pending;")):
        raise StopSupervisor("R6 retention or pending .51 public readback state changed")


def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise StopSupervisor(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def du_bytes(root: Path) -> int:
    """Use one bounded C du scan per phase sample, never full Python tree walks per second."""
    if not root.exists():
        return 0
    result = subprocess.run(["/usr/bin/du", "-sk", str(root)], check=True,
                            text=True, capture_output=True, timeout=15)
    fields = result.stdout.split()
    if len(fields) < 2 or int(fields[0]) < 0:
        raise StopSupervisor(f"unexpected du output for {root}")
    return int(fields[0]) * 1024


def storage_sample_due(now: float, last_sample: float | None,
                       interval: float = STORAGE_SAMPLE_SECONDS) -> bool:
    return last_sample is None or now - last_sample >= interval


def process_snapshot() -> list[dict]:
    result = subprocess.run(["/bin/ps", "-Ao", "pid=,ppid=,uid=,pgid=,rss=,command="], check=True,
                            text=True, capture_output=True, timeout=5)
    records = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        fields = line.split(maxsplit=5)
        if len(fields) != 6:
            raise StopSupervisor(f"unexpected nonempty ps process row: {line[:300]!r}")
        try:
            pid, ppid, uid, group, rss_kib = map(int, fields[:5])
        except ValueError as error:
            raise StopSupervisor(f"unparseable ps process identity row: {line[:300]!r}") from error
        if pid <= 0 or ppid < 0 or group < 0 or rss_kib < 0:
            raise StopSupervisor(f"invalid ps process numeric field: {line[:300]!r}")
        records.append({"pid":pid,"ppid":ppid,"uid":uid,"pgid":group,"rss_bytes":rss_kib*1024,
                        "command":fields[5]})
    return records


def rss_group(pgid: int, records: list[dict] | None = None) -> tuple[int, list[int]]:
    records = process_snapshot() if records is None else records
    owned = [item for item in records if item["pgid"] == pgid and item["uid"] == os.getuid()]
    return sum(item["rss_bytes"] for item in owned), [item["pid"] for item in owned]


def pgdata_processes(path: str) -> list[str]:
    records = process_snapshot()
    owners = {item["pid"] for item in pgdata_owner_records(path, records)}
    return [f"pid={item['pid']} uid={item['uid']} rss={item['rss_bytes']} command={item['command']}"
            for item in records if item["pid"] in owners or (path in item["command"]
            and "pg_ctl" in item["command"].lower())]


def pgdata_owner_records(path: str, records: list[dict] | None = None) -> list[dict]:
    records = process_snapshot() if records is None else records
    data = Path(path)
    pidfile = data / "postmaster.pid"
    try:
        lines = pidfile.read_text().splitlines()
        postmaster_pid = int(lines[0]) if len(lines) >= 2 and Path(lines[1]).resolve() == data.resolve() else None
    except (OSError, ValueError):
        postmaster_pid = None
    by_pid = {item["pid"]: item for item in records}
    root = by_pid.get(postmaster_pid)
    if (root is None or path not in root["command"] or "postgres" not in root["command"].lower()):
        return [item for item in records if path in item["command"] and "postgres" in item["command"].lower()]
    family = {postmaster_pid}
    changed = True
    while changed:
        changed = False
        for item in records:
            if item.get("ppid") in family and item["pid"] not in family:
                family.add(item["pid"]); changed = True
    return [item for item in records if item["pid"] in family]


def combined_owned_rss(pgid: int, pgdata_path: str,
                       records: list[dict] | None = None) -> tuple[int, list[int], list[dict]]:
    records = process_snapshot() if records is None else records
    group_rss, pids = rss_group(pgid, records)
    group_ids = set(pids)
    pg_owners = [item for item in pgdata_owner_records(pgdata_path, records)
                 if item["pid"] not in group_ids]
    return group_rss + sum(item["rss_bytes"] for item in pg_owners), pids, pg_owners


def stop_owned_postgres(path: str, *, run=subprocess.run, snapshot=process_snapshot) -> dict:
    """Stop only a current-user PG daemon proven by its exact PGDATA pidfile and argv."""
    data = Path(path)
    info = data.lstat()
    if (data.is_symlink() or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()):
        raise StopSupervisor("refusing PostgreSQL fallback: clone directory ownership changed")
    pidfile = data / "postmaster.pid"
    if not pidfile.exists():
        owners = pgdata_owner_records(path, snapshot())
        if owners:
            raise StopSupervisor("PostgreSQL process references clone without its postmaster.pid")
        return {"attempted":False,"stopped":True,"reason":"no pidfile or clone owners"}
    pidinfo = pidfile.lstat()
    if pidfile.is_symlink() or not stat.S_ISREG(pidinfo.st_mode) or pidinfo.st_uid != os.getuid():
        raise StopSupervisor("refusing PostgreSQL fallback: unsafe postmaster.pid")
    lines = pidfile.read_text().splitlines()
    if len(lines) < 2 or Path(lines[1]).resolve() != data.resolve():
        raise StopSupervisor("refusing PostgreSQL fallback: pidfile does not bind exact clone")
    try:
        postmaster_pid = int(lines[0])
    except ValueError as error:
        raise StopSupervisor("refusing PostgreSQL fallback: invalid postmaster pid") from error
    records = snapshot()
    owner = next((item for item in records if item["pid"] == postmaster_pid), None)
    pg_owners = pgdata_owner_records(path, records)
    if (owner is None or owner["uid"] != os.getuid() or path not in owner["command"]
            or "postgres" not in owner["command"].lower()
            or not any(item["pid"] == postmaster_pid for item in pg_owners)):
        raise StopSupervisor("refusing PostgreSQL fallback: postmaster process identity is unproven")
    if not Path(PG_CTL).is_file():
        raise StopSupervisor("pinned R12 pg_ctl binary is missing")
    env = {key:value for key,value in os.environ.items() if not key.startswith("PG")}
    result = run([PG_CTL,"-D",str(data),"-m","fast","-w","-t","15","stop"],
                 check=False,text=True,capture_output=True,timeout=25,env=env)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if not pidfile.exists() and not pgdata_owner_records(path, snapshot()):
            return {"attempted":True,"stopped":True,"pg_ctl_exit_code":result.returncode,
                    "postmaster_pid":postmaster_pid}
        time.sleep(0.2)
    raise StopSupervisor("owned PostgreSQL fallback stop was not confirmed; custody unresolved")


def terminate_group(pgid: int, *, killpg=None, snapshot=None, clock=None,
                    sleep_fn=None, grace_seconds: float = 5.0) -> dict:
    """Bounded best-effort stop; absence and OS refusal are returned, never raised."""
    killpg = killpg or os.killpg
    snapshot = snapshot or process_snapshot
    clock = clock or time.monotonic
    sleep_fn = sleep_fn or time.sleep

    def group_records():
        return [item for item in snapshot() if item["pgid"] == pgid]

    try:
        initial = group_records()
    except BaseException as error:
        return {"status":"process_inventory_failed","remaining_pids":[-1],
                "error":f"{type(error).__name__}: {error}"[:1000],"signals_sent":[]}
    if not initial:
        return {"status":"process_group_absent","remaining_pids":[],"signals_sent":[]}
    foreign = [item["pid"] for item in initial if item["uid"] != os.getuid()]
    if foreign:
        return {"status":"foreign_uid_members","remaining_pids":foreign,"signals_sent":[]}
    signals_sent = []
    try:
        killpg(pgid, signal.SIGTERM)
        signals_sent.append("SIGTERM")
    except ProcessLookupError:
        return {"status":"process_group_absent","remaining_pids":[],"signals_sent":signals_sent}
    except PermissionError as error:
        return {"status":"permission_denied","remaining_pids":[item["pid"] for item in initial],
                "error":f"{type(error).__name__}: {error}"[:1000],"signals_sent":signals_sent}
    except OSError as error:
        return {"status":"signal_error","remaining_pids":[item["pid"] for item in initial],
                "error":f"{type(error).__name__}: {error}"[:1000],"signals_sent":signals_sent}
    deadline = clock() + grace_seconds
    while clock() < deadline:
        try:
            members = group_records()
        except BaseException as error:
            return {"status":"process_inventory_failed","remaining_pids":[item["pid"] for item in initial],
                    "error":f"{type(error).__name__}: {error}"[:1000],"signals_sent":signals_sent}
        if not members:
            return {"status":"terminated","remaining_pids":[],"signals_sent":signals_sent}
        foreign = [item["pid"] for item in members if item["uid"] != os.getuid()]
        if foreign:
            return {"status":"foreign_uid_members","remaining_pids":foreign,"signals_sent":signals_sent}
        sleep_fn(0.2)
    try:
        killpg(pgid, signal.SIGKILL)
        signals_sent.append("SIGKILL")
    except ProcessLookupError:
        return {"status":"process_group_absent","remaining_pids":[],"signals_sent":signals_sent}
    except PermissionError as error:
        return {"status":"permission_denied","remaining_pids":[item["pid"] for item in members],
                "error":f"{type(error).__name__}: {error}"[:1000],"signals_sent":signals_sent}
    except OSError as error:
        return {"status":"signal_error","remaining_pids":[item["pid"] for item in members],
                "error":f"{type(error).__name__}: {error}"[:1000],"signals_sent":signals_sent}
    try:
        remaining = [item["pid"] for item in group_records()]
    except BaseException as error:
        return {"status":"process_inventory_failed","remaining_pids":[-1],
                "error":f"{type(error).__name__}: {error}"[:1000],"signals_sent":signals_sent}
    return {"status":"terminated" if not remaining else "processes_remain",
            "remaining_pids":remaining,"signals_sent":signals_sent}


def persist_terminal_receipt(path: Path, receipt: dict) -> None:
    """Durably create the terminal receipt without overwriting prior evidence."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        stream.write(json.dumps(receipt, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def terminal_failure_receipt(receipt: dict, error: BaseException,
                             termination: dict, pg_stop: dict,
                             owners: list[str], elapsed: float) -> dict:
    """Build a terminal, do-not-retry receipt even when cleanup cannot signal."""
    result = dict(receipt)
    remaining = termination.get("remaining_pids", [-1])
    result.update(status="failed_or_interrupted", failure_type=type(error).__name__,
                  failure=str(error)[:1500], remaining_process_group_pids=remaining,
                  process_group_termination=termination,
                  postgres_fallback_stop=pg_stop, migration_clone_owners=owners,
                  elapsed_seconds=round(elapsed, 3))
    safe_stops = {"terminated", "process_group_absent"}
    if not receipt.get("child_started") and termination.get("status") == "not_started":
        safe_stops.add("not_started")
    unresolved = (termination.get("status") not in safe_stops or remaining
                  or pg_stop.get("stopped") is not True or owners)
    result["effects_may_remain"] = bool(unresolved or (receipt.get("child_started") and remaining))
    if unresolved:
        result["custody_status"] = "unresolved_processes_remain_do_not_retry"
    else:
        result["custody_status"] = "no_owned_processes_remain"
    return result


def main() -> int:
    scope_raw = SCOPE_PATH.read_bytes()
    scope = json.loads(scope_raw, object_pairs_hook=unique_pairs)
    if (scope.get("paths") != EXPECTED_PATHS
            or scope.get("source", {}).get("existing_pgdata") != "/private/tmp/ashlar-r10-truss051-current-cohort-20261101/migration-clone-pgdata"):
        raise StopSupervisor("exact reviewed source and attempt paths required")
    if (scope.get("runtime", {}).get("python_path") != PYTHON
            or scope["runtime"].get("pythonpath") != PYTHONPATH):
        raise StopSupervisor("exact pinned .51 Python executable/site-packages required")
    validate_standing_authorization(scope, scope_raw)
    validate_release_scope(scope)
    if scope.get("limits") != {
        "attempts":1,"automatic_retries":0,"wall_seconds":900,
        "process_group_rss_bytes":2147483648,"retained_plus_attempt_bytes":2147483648,
        "attempt_root_bytes":536870912,"migration_preservation_bytes":268435456,
        "combined_log_bytes":16777216,"storage_sample_seconds":15,"parallelism":"sequential local only",
        "cloud":False,"cleanup":False}:
        raise StopSupervisor("exact reviewed wall/RSS/storage limits required")
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
            max_rss = max_pg_rss = max_tree = max_logs = 0
            last_storage_sample = None
            retained_bytes = scope["source"]["pgdata_bytes_at_packet_review"]
            while child.poll() is None:
                now = time.monotonic()
                if now - start > MAX_SECONDS:
                    raise StopSupervisor("900-second wall limit exceeded")
                rss, pids, pg_owners = combined_owned_rss(
                    child.pid, scope["paths"]["migration_clone_pgdata"])
                pg_rss = sum(item["rss_bytes"] for item in pg_owners)
                max_rss = max(max_rss, rss)
                max_pg_rss = max(max_pg_rss, pg_rss)
                if rss > MAX_RSS:
                    raise StopSupervisor("worker process-group plus detached clone PostgreSQL RSS exceeded 2 GiB")
                logs = stdout_path.stat().st_size + stderr_path.stat().st_size
                max_logs = max(max_logs, logs)
                if logs > MAX_LOGS:
                    raise StopSupervisor("combined child logs exceeded 16 MiB")
                if storage_sample_due(now, last_storage_sample):
                    current_tree = du_bytes(output_root)
                    supervisor_bytes = du_bytes(supervisor_root)
                    max_tree = max(max_tree, current_tree)
                    if current_tree + supervisor_bytes > MAX_ATTEMPT:
                        raise StopSupervisor("combined attempt and supervisor output exceeded 512 MiB")
                    if retained_bytes + current_tree + supervisor_bytes > MAX_AGGREGATE:
                        raise StopSupervisor("retained plus attempt storage exceeded 2 GiB")
                    last_storage_sample = now
                time.sleep(1)
            code = child.wait(timeout=5)
            current_tree = du_bytes(output_root)
            supervisor_bytes = du_bytes(supervisor_root)
            max_tree = max(max_tree, current_tree)
            if current_tree + supervisor_bytes > MAX_ATTEMPT:
                raise StopSupervisor("combined attempt and supervisor output exceeded 512 MiB")
            if retained_bytes + current_tree + supervisor_bytes > MAX_AGGREGATE:
                raise StopSupervisor("retained plus attempt storage exceeded 2 GiB")
        rss, remaining = rss_group(child.pid)
        if remaining:
            raise StopSupervisor(f"child exited with process-group members remaining: {remaining!r}")
        owners = pgdata_processes(scope["paths"]["migration_clone_pgdata"])
        if owners:
            stopped = stop_owned_postgres(scope["paths"]["migration_clone_pgdata"])
            receipt["postgres_fallback_stop"] = stopped
            raise StopSupervisor(f"worker exited with PostgreSQL still owned outside its group: {owners[:4]!r}")
        if code != 0:
            raise StopSupervisor(f"continuation worker exited {code}")
        receipt.update(status="completed", child_exit_code=code, maximum_rss_bytes=max_rss,
                       maximum_detached_postgres_rss_bytes=max_pg_rss,
                       maximum_attempt_tree_bytes=max_tree, maximum_logs_bytes=max_logs,
                       elapsed_seconds=round(time.monotonic() - start, 3))
    except BaseException as error:
        if receipt.get("child_started"):
            try:
                termination = terminate_group(receipt["child_pid"])
            except BaseException as cleanup_error:
                termination = {"status":"cleanup_error","remaining_pids":[-1],
                               "error":f"{type(cleanup_error).__name__}: {cleanup_error}"[:1000],
                               "signals_sent":[]}
        else:
            termination = {"status":"not_started","remaining_pids":[],"signals_sent":[]}
        try:
            pg_stop = stop_owned_postgres(scope["paths"]["migration_clone_pgdata"])
        except FileNotFoundError:
            pg_stop = {"attempted":False,"stopped":True,"reason":"clone not created"}
        except BaseException as postgres_error:
            pg_stop = {"attempted":True,"stopped":False,
                       "failure":f"{type(postgres_error).__name__}: {postgres_error}"[:1000]}
        try:
            owners = pgdata_processes(scope["paths"]["migration_clone_pgdata"])
        except BaseException as custody_error:
            owners = [f"process inventory failed: {type(custody_error).__name__}: {custody_error}"]
        terminal = terminal_failure_receipt(receipt, error, termination, pg_stop, owners,
                                            time.monotonic() - start)
        receipt_path = supervisor_root / "supervisor-receipt.json"
        try:
            persist_terminal_receipt(receipt_path, terminal)
        except BaseException as persist_error:
            # Keep the original exception while making terminal-receipt loss explicit in stderr.
            sys.stderr.write(f"terminal supervisor receipt write failed: {type(persist_error).__name__}: {persist_error}\n")
            sys.stderr.flush()
        raise
    (supervisor_root / "supervisor-receipt.json").write_text(json.dumps(receipt, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
