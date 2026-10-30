#!/usr/bin/env python3
"""R16 stopped-backup restore and public .48 custody prefix; stops before migration."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import signal
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Callable

PROFILE = "ashlar-truss051-r16-pre-migration-custody/0.1"
ARCHIVE_SHA = "cb57fdb7d338f345cfad157463fdbe94a2be7dcf69e728c474954c2939c14a0b"
SOURCE_MANIFEST_SHA = "fb79cebc5b1388b49d0687c9f77c61187125299758ee7388fd64ba2c9d533348"
HELPER_SHA = "62707a3febf1f67b7a06edc1cc7ccba1a843c1dc3540fae2ac8b4406e09ea13d"
OLD48_RUNTIME_SHA = "1ae2d48fcda993c5841f99d1f9f45a689f1f778007d39826224206a474f8bb74"
OLD48_PYTHON = "/private/tmp/ashlar-r16-truss048-readonly-compat-312/bin/python"
PG_CTL_SHA = "3021087af95fd33bfd83106c85de47c4756e44aa7edde78446e7593604d1b451"
SEED_REQUEST_SHA = "56aed4d7a4789f45cdaa7af55a834ff38ca2b4bcb75f44658b3031ba5f8aa771"
ORIGIN_SHA = "c19540a4c1cc3561365cc4ec38a71da44fdc4648b8f27a6540c81eb5233badc5"
FROZEN_SOURCE_CUSTODY = {
    "expected_accepted_source_document_id": "ashlar-current-045-lifecycle-20261021",
    "expected_source_revision": "ashlar-current-045-lifecycle-r1",
    "expected_source_registration_id": "ccfcd423-5893-4df9-8f88-6504e086bd4d",
    "expected_source_sha256": "498a82f313fd3ceaeb6195e133b403aa829acfcda7cec676a33a5790a57d456b",
    "expected_catalog_revision": 1,
    "expected_source_report_sha256": "c797191c90c516b0997801a63605d297e5e429b50e922cd7da2d39640d570c9a",
    "expected_complete_view_sha256": "cc36fe489326f73c2eaf153ba2e025feeb76eaa88ffa0d6b675a1f4772264dc7",
    "expected_feed_id": "ashlar-current-045-lifecycle",
    "expected_consumer_id": "ashlar-current-048-lifecycle-20261022",
    "expected_consumer_registration_id": "9f03eb5d-ce72-437f-8b32-0112c130e724",
    "expected_genesis_checkpoint_sha256": "c74e4c2dd697421f26a378454ec489bcb83501a4997235b670bdb011e4e7be8e",
}
MAX_TREE_BYTES = 512 * 1024 * 1024
MAX_HELPER_OUTPUT = 2 * 1024 * 1024


class StopRun(RuntimeError):
    pass


_EFFECT_STATE = {"phase": "none", "native_effects": "none", "effects_may_remain": False}


def _mark_effect(phase: str, *, native: str | None = None, may_remain: bool | None = None) -> None:
    _EFFECT_STATE["phase"] = phase
    if native is not None:
        _EFFECT_STATE["native_effects"] = native
    if may_remain is not None:
        _EFFECT_STATE["effects_may_remain"] = may_remain


def file_sha(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise StopRun(f"regular file required: {path}")
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def write_new(path: Path, value: object) -> None:
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def tree(root: Path) -> dict:
    if root.is_symlink() or not root.is_dir():
        raise StopRun(f"real directory required: {root}")
    digest = hashlib.sha256()
    count = total = 0
    for base, dirs, files in os.walk(root, followlinks=False):
        dirs.sort(); files.sort()
        for name in dirs + files:
            path = Path(base) / name
            if path.is_symlink() or not (path.is_dir() or path.is_file()):
                raise StopRun("symlink or special object in PGDATA")
        for name in files:
            path = Path(base) / name
            size = path.stat().st_size
            total += size; count += 1
            if total > MAX_TREE_BYTES:
                raise StopRun("PGDATA exceeds 512 MiB inventory cap")
            digest.update((path.relative_to(root).as_posix() + "\0" + str(size) + "\0" + file_sha(path) + "\n").encode())
    return {"tree_sha256": digest.hexdigest(), "bytes": total, "file_count": count}


def _paths(scope: dict) -> dict[str, Path]:
    state = scope["state_custody"]
    names = ("output_root", "selected_incomplete_pgdata_path", "discrepancy_preservation_path",
             "cold_backup_path", "restored_pgdata_path", "registry_path", "registry_backup_receipt_path", "genesis_page_path")
    values = {key: Path(state[key] if key != "output_root" else scope[key]) for key in names}
    values["socket_directory"] = Path(scope["runtime"]["socket_directory"])
    for key, path in values.items():
        if not path.is_absolute() or path.resolve() != path:
            raise StopRun(f"canonical absolute path required: {key}")
    root = values["output_root"]
    for key in ("discrepancy_preservation_path", "restored_pgdata_path", "registry_path", "registry_backup_receipt_path", "genesis_page_path", "socket_directory"):
        if not values[key].is_relative_to(root):
            raise StopRun(f"{key} must be under the one-run output root")
    sources = (values["selected_incomplete_pgdata_path"], values["cold_backup_path"])
    targets = tuple(values[key] for key in ("output_root", "discrepancy_preservation_path", "restored_pgdata_path",
                                             "registry_path", "registry_backup_receipt_path", "genesis_page_path", "socket_directory"))
    def overlaps(left: Path, right: Path) -> bool:
        return left == right or left in right.parents or right in left.parents
    for source in sources:
        if any(overlaps(source, target) for target in targets):
            raise StopRun("output root/attempt paths/socket may not overlap either PGDATA source")
    if overlaps(*sources):
        raise StopRun("selected and cold-backup PGDATA sources must not overlap")
    leaf_targets = tuple(values[key] for key in ("discrepancy_preservation_path", "restored_pgdata_path",
                                                  "registry_path", "registry_backup_receipt_path", "genesis_page_path", "socket_directory"))
    if any(overlaps(left, right) for index, left in enumerate(leaf_targets) for right in leaf_targets[index + 1:]):
        raise StopRun("attempt outputs and socket directory must be mutually disjoint")
    if len({values[key] for key in ("selected_incomplete_pgdata_path", "discrepancy_preservation_path", "cold_backup_path", "restored_pgdata_path")}) != 4:
        raise StopRun("selected, preserved, cold-backup and restored paths must be distinct")
    return values


def verify_scope(scope: dict) -> None:
    """Pre-effect admission check. The present template fails here on its nulls."""
    if scope.get("profile") != "ashlar-truss-r16-custody-prefix-scope/0.1":
        raise StopRun("exact R16 custody-prefix scope required")
    if scope.get("authorization") != "one_run_local_custody_prefix":
        raise StopRun("fresh custody-prefix-only approval required")
    if scope.get("limits", {}).get("attempts") != 1 or scope.get("limits", {}).get("automatic_retries") != 0:
        raise StopRun("one attempt and zero automatic retries required")
    state = scope.get("state_custody", {})
    runtime = scope.get("runtime", {})
    source = scope.get("source", {})
    required = [scope.get("output_root"), state.get("selected_incomplete_pgdata_path"),
                state.get("discrepancy_preservation_path"), state.get("cold_backup_path"),
                state.get("restored_pgdata_path"), state.get("registry_backup_receipt_path"),
                state.get("genesis_page_path"), runtime.get("socket_directory"),
                runtime.get("port"), runtime.get("database"), runtime.get("bootstrap_user"),
                runtime.get("pg_ctl_path"), runtime.get("truss048_python"),
                runtime.get("truss048_runtime_receipt_path"), runtime.get("truss048_runtime_receipt_sha256"),
                source.get("archive_sha256"), source.get("worker_sha256"),
                state.get("fresh_process_inventory_rule"),
                state.get("cold_backup_tree_sha256"), state.get("selected_incomplete_pgdata_tree_sha256"),
                state.get("new_registry_pre_effect_backup_sha256_or_exact_creation_rule"),
                state.get("new_registry_pre_effect_backup_sha256"), state.get("registry_path")]
    if any(value in (None, "", "TBD") for value in required):
        raise StopRun("required scope paths, runtime pins, inventory receipt or approval hashes are unresolved")
    if source["archive_sha256"] != ARCHIVE_SHA:
        raise StopRun("selected frozen source archive hash mismatch")
    if scope.get("seed_custody", {}).get("request_sha256") != SEED_REQUEST_SHA:
        raise StopRun("original exact seed request hash required")
    origin = scope.get("seed_custody", {})
    if origin.get("origin_map_expected_sha256") != ORIGIN_SHA:
        raise StopRun("proposed explicit local-trial origin-map hash required")
    if state.get("fresh_process_inventory_rule") != "fresh /bin/ps -Ao pid=,command= immediately before start; require successful output and no selected/restored PGDATA references":
        raise StopRun("exact immediate pre-start process-inventory rule required")
    limits = scope["limits"]
    if (limits.get("child_deadline_seconds") != 600 or limits.get("attempt_root_bytes_max") != MAX_TREE_BYTES
            or limits.get("combined_log_bytes_max") != 16 * 1024 * 1024
            or limits.get("aggregate_retained_plus_attempt_bytes_max") != 2 * 1024 * 1024 * 1024
            or limits.get("parallelism") != "sequential local only" or limits.get("cloud_resources") is not False):
        raise StopRun("bounded single-host custody-prefix resource limits required")
    paths = _paths(scope)
    if any(paths[key].exists() or paths[key].is_symlink() for key in ("output_root", "discrepancy_preservation_path", "restored_pgdata_path", "registry_path", "registry_backup_receipt_path", "genesis_page_path", "socket_directory")):
        raise StopRun("all attempt outputs must be fresh and absent")
    if scope.get("source_custody") != FROZEN_SOURCE_CUSTODY:
        raise StopRun("source/feed/consumer/checkpoint pins differ from the frozen R16 packet")


def verify_approval(scope_path: Path, approval_path: Path, worker_path: Path) -> dict:
    """Bind one new custody-prefix run to this exact worker and complete scope."""
    for path, maximum in ((scope_path, 256 * 1024), (approval_path, 64 * 1024)):
        if path.is_symlink() or not path.is_file() or path.resolve() != path:
            raise StopRun("canonical regular scope and approval files required")
        if path.stat().st_size > maximum:
            raise StopRun("scope or approval exceeds its bounded input size")
    raw = scope_path.read_bytes()
    approval_raw = approval_path.read_bytes()
    scope = json.loads(raw); approval = json.loads(approval_raw)
    verify_scope(scope)
    actual_worker = file_sha(worker_path)
    if scope["source"]["worker_sha256"] != actual_worker:
        raise StopRun("worker code hash differs from reviewed scope")
    expected = {"profile": "ashlar-truss-r16-custody-prefix-approval/0.1",
                "scope_sha256": hashlib.sha256(raw).hexdigest(),
                "worker_sha256": actual_worker,
                "source_archive_sha256": ARCHIVE_SHA,
                "source_manifest_sha256": SOURCE_MANIFEST_SHA,
                "source_helper_sha256": HELPER_SHA,
                "truss048_runtime_receipt_sha256": OLD48_RUNTIME_SHA,
                "pg_ctl_sha256": PG_CTL_SHA,
                "seed_request_sha256": SEED_REQUEST_SHA,
                "origin_map_sha256": ORIGIN_SHA,
                "scope": "one stopped-backup restore plus .48 source/feed/page custody only; stop before migration"}
    if approval != expected:
        raise StopRun("fresh owner approval does not exactly bind this worker/scope/input set")
    return scope


def _verify_static_inputs(scope: dict, source_root: Path, helper: Path) -> Path:
    if sys.version_info[:3] != (3, 12, 14) or platform.system() != "Darwin" or platform.machine() != "arm64":
        raise StopRun("selected macOS arm64 / CPython 3.12.14 worker runtime required")
    if file_sha(helper) != HELPER_SHA:
        raise StopRun("preserved public .48 helper hash mismatch")
    if not source_root.is_dir() or source_root.is_symlink() or source_root.resolve() != source_root:
        raise StopRun("canonical extracted frozen R16 source src directory required")
    manifest = helper.with_name("source-inputs-manifest-r16.json")
    if file_sha(manifest) != SOURCE_MANIFEST_SHA:
        raise StopRun("frozen source-input manifest hash mismatch")
    runtime_receipt = Path(scope["runtime"]["truss048_runtime_receipt_path"])
    helper_python = Path(scope["runtime"]["truss048_python"])
    if (file_sha(runtime_receipt) != OLD48_RUNTIME_SHA
            or scope["runtime"]["truss048_runtime_receipt_sha256"] != OLD48_RUNTIME_SHA):
        raise StopRun("exact effect-free .48 compatibility-runtime receipt required")
    if str(helper_python) != OLD48_PYTHON or not helper_python.is_file() or not helper_python.resolve().is_file():
        raise StopRun("canonical .48 compatibility interpreter required")
    import importlib.util
    spec = importlib.util.spec_from_file_location("r16_frozen_source_helper", helper)
    if spec is None or spec.loader is None:
        raise StopRun("frozen source verifier unavailable")
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    os.environ["ASHLAR_R16_SOURCE_SRC"] = str(source_root)
    try:
        verifier.require_frozen_source_root()
    except Exception as error:
        raise StopRun("frozen source tree differs from exact R16 manifest") from error
    selected_archive = Path(scope["source"]["archive_path"])
    if file_sha(selected_archive) != ARCHIVE_SHA:
        raise StopRun("frozen selected source archive hash mismatch")
    pg_ctl = Path(scope["runtime"]["pg_ctl_path"])
    if pg_ctl.resolve() != pg_ctl or pg_ctl.is_symlink() or file_sha(pg_ctl) != PG_CTL_SHA:
        raise StopRun("hash-bound external PostgreSQL 18.6 pg_ctl required")
    version = subprocess.run([str(pg_ctl), "--version"], stdin=subprocess.DEVNULL,
                              text=True, capture_output=True, timeout=5, check=False)
    if version.returncode or version.stdout.strip() not in ("pg_ctl (PostgreSQL) 18.6", "pg_ctl (PostgreSQL) 18.6 (Homebrew)"):
        raise StopRun("external PostgreSQL tool must identify as 18.6")
    old48 = subprocess.run([str(helper_python), "-I", "-B", str(helper), "--preflight"],
                           stdin=subprocess.DEVNULL, text=True, capture_output=True, timeout=30, check=False)
    if old48.returncode or len(old48.stdout.splitlines()) != 1:
        raise StopRun("effect-free .48 compatibility helper preflight failed")
    old48_result = json.loads(old48.stdout)
    if old48_result.get("truss") != "0.0.48" or old48_result.get("pg_started") is not False or old48_result.get("spark_started") is not False:
        raise StopRun("exact no-native .48 helper profile required")
    return pg_ctl


def _copy(source: Path, target: Path, expected: str) -> dict:
    before = tree(source)
    if before["tree_sha256"] != expected:
        raise StopRun("cold-backup source differs from frozen inventory")
    if target.exists() or target.is_symlink():
        raise StopRun("fresh copy target required")
    shutil.copytree(source, target, symlinks=False, copy_function=shutil.copy2)
    after = tree(source); copied = tree(target)
    if before != copied or before != after:
        raise StopRun("copy inventory mismatch; stop before server start")
    if (target / "PG_VERSION").read_text().strip() != "18" or (target / "postmaster.pid").exists():
        raise StopRun("restored target must be stopped PostgreSQL 18")
    return copied


def _run_helper(helper_python: Path, helper: Path, source_root: Path, conninfo: str, attempt: Path, *args: str) -> dict:
    env = {"PATH": str(helper_python.parent) + ":/usr/bin:/bin", "HOME": os.environ.get("HOME", "/Users/erik"),
           "PYTHONDONTWRITEBYTECODE": "1", "ASHLAR_R16_SOURCE_SRC": str(source_root),
           "ASHLAR_LOCAL_PG_CONNINFO": conninfo}
    result = subprocess.run([str(helper_python), "-I", "-B", str(helper), *args], cwd=attempt,
                            env=env, stdin=subprocess.DEVNULL, text=True, capture_output=True,
                            timeout=90, check=False)
    if len(result.stdout.encode()) + len(result.stderr.encode()) > MAX_HELPER_OUTPUT:
        raise StopRun(".48 helper output exceeds 2 MiB")
    if result.returncode or len(result.stdout.splitlines()) != 1:
        raise StopRun("public .48 helper failed: " + (result.stderr or result.stdout)[-1000:])
    return json.loads(result.stdout)


def verify_custody(observed: dict, expected: dict) -> None:
    pairs = (("document_id", "expected_accepted_source_document_id"),
             ("document_revision", "expected_source_revision"),
             ("source_registration_id", "expected_source_registration_id"),
             ("source_sha256", "expected_source_sha256"),
             ("catalog_revision", "expected_catalog_revision"),
             ("report_sha256", "expected_source_report_sha256"),
             ("view_sha256", "expected_complete_view_sha256"))
    for actual, frozen in pairs:
        if observed.get(actual) != expected.get(frozen):
            raise StopRun("fresh .48 source/view custody mismatch: " + actual)


def run_prefix(scope: dict, source_root: Path, helper: Path, *, stage: Callable[[str], None] | None = None,
               operations: dict[str, Callable] | None = None) -> dict:
    """Run only preserve → restore → start → .48 custody/page → stop; never migrate."""
    _EFFECT_STATE.update(phase="none", native_effects="none", effects_may_remain=False)
    verify_scope(scope)
    ops = operations or {}
    paths = _paths(scope)
    runtime = scope["runtime"]
    expected = scope["source_custody"]
    pg_ctl = _verify_static_inputs(scope, source_root, helper)
    # Hash all input PGDATA before creating an output directory or copying any
    # bytes. These inventories are admission gates, not post-copy diagnostics.
    selected = paths["selected_incomplete_pgdata_path"]
    preselected = ops.get("tree", tree)(selected)
    if preselected["tree_sha256"] != scope["state_custody"]["selected_incomplete_pgdata_tree_sha256"]:
        raise StopRun("fresh selected 11-file tree differs from frozen observation")
    cold = ops.get("tree", tree)(paths["cold_backup_path"])
    if cold["tree_sha256"] != scope["state_custody"]["cold_backup_tree_sha256"]:
        raise StopRun("cold-backup source differs from frozen inventory")
    aggregate = 2 * (preselected["bytes"] + cold["bytes"])
    if aggregate > scope["limits"]["aggregate_retained_plus_attempt_bytes_max"]:
        raise StopRun("retained source plus preserved/restored copies exceed aggregate byte cap")
    attempt = paths["output_root"]
    if shutil.disk_usage(attempt.parent).free < aggregate + 64 * 1024 * 1024:
        raise StopRun("insufficient free space for bounded copies and 64 MiB reserve")
    _mark_effect("attempt-directory-creation", may_remain=True)
    attempt.mkdir(mode=0o700, parents=True, exist_ok=False)
    if stage: stage("attempt-created")

    discrepancy = paths["discrepancy_preservation_path"]
    discrepancy.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    _mark_effect("discrepancy-copy-in-progress", may_remain=True)
    ops.get("copytree", shutil.copytree)(selected, discrepancy, symlinks=False, copy_function=shutil.copy2)
    if ops.get("tree", tree)(discrepancy) != preselected or ops.get("tree", tree)(selected) != preselected:
        raise StopRun("discrepancy tree preservation mismatch")
    write_new(attempt / "discrepancy-preserved.json", preselected)
    if stage: stage("discrepancy-preserved")

    restored = paths["restored_pgdata_path"]
    _mark_effect("cold-backup-restore-in-progress", may_remain=True)
    backup = (ops.get("copy", _copy)(paths["cold_backup_path"], restored,
                                      scope["state_custody"]["cold_backup_tree_sha256"]))
    write_new(attempt / "cold-backup-restored.json", backup)
    if stage: stage("cold-backup-restored")

    receipt = paths["registry_backup_receipt_path"]
    rule = scope["state_custody"]["new_registry_pre_effect_backup_sha256_or_exact_creation_rule"]
    write_new(receipt, {"profile": "ashlar-r16-registry-absence-before-migration/0.1",
                        "registry_path": str(Path(scope["state_custody"]["registry_path"])),
                        "exact_creation_rule": rule, "registry_created": False})
    if file_sha(receipt) != scope["state_custody"]["new_registry_pre_effect_backup_sha256"]:
        raise StopRun("registry absence receipt differs from approved hash")
    if stage: stage("registry-absence-receipt")

    process_check = ops.get("process_inventory", lambda: subprocess.run(["/bin/ps", "-Ao", "pid=,command="], stdin=subprocess.DEVNULL,
                                   text=True, capture_output=True, timeout=10, check=False)
                                  )()
    if process_check.returncode:
        raise StopRun("fresh local process inventory unavailable")
    process_bytes = process_check.stdout.encode()
    if len(process_bytes) > 1024 * 1024:
        raise StopRun("fresh process inventory exceeds 1 MiB bound")
    if any(str(path) in process_check.stdout for path in (selected, restored)):
        raise StopRun("live PostgreSQL process references selected/restored PGDATA")
    write_new(attempt / "pre-start-process-inventory.json", {
        "profile": "ashlar-r16-process-inventory/0.1", "command": "/bin/ps -Ao pid=,command=",
        "output_sha256": hashlib.sha256(process_bytes).hexdigest(),
        "output_bytes": len(process_bytes), "returncode": process_check.returncode,
        "selected_pgdata_reference": False, "restored_pgdata_reference": False,
        "process_inventory_complete": True})

    socket_dir = Path(runtime["socket_directory"])
    socket_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    port = runtime["port"]
    if type(port) is not int or not 1024 <= port <= 65535:
        raise StopRun("approved local port in range 1024..65535 required")
    log = attempt / "postgres.log"
    started = False
    result = None
    deadline = time.monotonic() + scope["limits"]["child_deadline_seconds"]
    try:
        _mark_effect("postgres-start-attempted", native="possible", may_remain=True)
        # A timed-out or non-zero pg_ctl start may still have spawned the
        # postmaster. Enter the owned-target cleanup path before invoking it.
        started = True
        start = ops.get("start", lambda: subprocess.run([str(pg_ctl), "-D", str(restored), "-l", str(log), "-o",
                                f"-p {port} -k {socket_dir}", "-w", "-t", "30", "start"],
                               stdin=subprocess.DEVNULL, text=True, capture_output=True, timeout=40, check=False))()
        if start.returncode:
            raise StopRun("fresh restored PostgreSQL start failed: " + (start.stderr or start.stdout)[-1000:])
        _mark_effect("postgres-running", native="running", may_remain=True)
        if stage: stage("postgres-started")
        import psycopg
        params = {"host": str(socket_dir), "port": port, "dbname": runtime["database"],
                  "user": runtime["bootstrap_user"], "connect_timeout": 5}
        if "connect" in ops:
            connected = Path(ops["connect"](params)).resolve()
        else:
            with psycopg.connect(**params, autocommit=True, connect_timeout=5) as connection:
                connected = Path(connection.execute("SHOW data_directory").fetchone()[0]).resolve()
        if connected != restored:
            raise StopRun("connected server is not the exact fresh restored PGDATA")
        if time.monotonic() > deadline:
            raise StopRun("600-second custody-prefix deadline exceeded")
        conninfo = psycopg.conninfo.make_conninfo(**params)
        helper_run = ops.get("helper", _run_helper)
        source = helper_run(Path(runtime["truss048_python"]), helper, source_root, conninfo,
                            attempt, "--custody")
        verify_custody(source, expected)
        if stage: stage("source-custody-verified")
        page = helper_run(Path(runtime["truss048_python"]), helper, source_root, conninfo,
                          attempt, "--read-page", str(paths["genesis_page_path"]))
        if (page.get("consumer_id") != expected["expected_consumer_id"]
                or page.get("registration_id") != expected["expected_consumer_registration_id"]
                or page.get("next_checkpoint_sha256") != expected["expected_genesis_checkpoint_sha256"]
                or ("page_file_sha256" in page and page["page_file_sha256"] != page.get("archive_sha256"))
                or ("page_file_sha256" not in page and file_sha(paths["genesis_page_path"]) != page.get("archive_sha256"))):
            raise StopRun("fresh pending genesis page identity/checkpoint/archive mismatch")
        if time.monotonic() > deadline:
            raise StopRun("600-second custody-prefix deadline exceeded")
        result = {"profile": PROFILE, "status": "custody_prefix_verified_migration_not_invoked",
                  "source_custody": source,
                  "genesis_page": {key: page[key] for key in ("consumer_id", "registration_id", "writer_xid",
                      "page_sha256", "archive_sha256", "prior_checkpoint_sha256", "next_checkpoint_sha256")},
                  "migration_invoked": False, "spark_started": False,
                  "publication_invoked": False, "acknowledgement_invoked": False,
                  "seed_invoked": False, "continuation_invoked": False}
        if stage: stage("custody-prefix-complete")
    finally:
        if started:
            prior_stage_error = sys.exc_info()[1]
            pid_file = restored / "postmaster.pid"
            if not pid_file.is_file() or pid_file.is_symlink():
                suffix = f"; prior failure: {prior_stage_error}" if prior_stage_error else ""
                _mark_effect("postgres-stop-unverified-pid-file-missing", native="server_may_be_running", may_remain=True)
                raise StopRun("owned postmaster PID file is missing; refusing cleanup" + suffix)
            lines = pid_file.read_text().splitlines()
            if len(lines) < 2 or Path(lines[1]).resolve() != restored:
                suffix = f"; prior failure: {prior_stage_error}" if prior_stage_error else ""
                _mark_effect("postgres-stop-unverified-pid-file-mismatch", native="server_may_be_running", may_remain=True)
                raise StopRun("postmaster PID file does not bind exact restored PGDATA" + suffix)
            try:
                stopped = ops.get("stop", lambda: subprocess.run([str(pg_ctl), "-D", str(restored), "-m", "fast", "-w", "-t", "20", "stop"],
                                         stdin=subprocess.DEVNULL, text=True, capture_output=True, timeout=25, check=False))()
            except Exception as error:
                _mark_effect("postgres-stop-threw", native="server_may_be_running", may_remain=True)
                raise StopRun(f"owned PostgreSQL stop raised {type(error).__name__}: {str(error)[:700]}; "
                              f"inspect retained attempt {attempt}; prior stage failure: {prior_stage_error or 'none'}") from error
            if stopped.returncode or pid_file.exists():
                primary_note = f"; prior failure: {prior_stage_error}" if prior_stage_error else ""
                stop_output = (stopped.stderr or stopped.stdout or "").strip()[-1000:]
                _mark_effect("postgres-stop-failed", native="server_may_be_running", may_remain=True)
                raise StopRun(f"owned PostgreSQL stop failed (returncode={stopped.returncode}, pid_file_remains={pid_file.exists()}); "
                              f"inspect retained attempt {attempt}; pg_ctl: {stop_output}{primary_note}")
            _mark_effect("postgres-stopped", native="stopped", may_remain=True)
    if result is None:
        raise StopRun("pre-migration custody result missing")
    retained_logs = sum(path.stat().st_size for path in (log,) if path.exists())
    if retained_logs > scope["limits"]["combined_log_bytes_max"]:
        raise StopRun("retained PostgreSQL log exceeds 16 MiB combined-log cap")
    result["pg_stopped"] = True
    result["effect_state"] = dict(_EFFECT_STATE)
    write_new(attempt / "pre-migration-custody.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scope", required=True, type=Path)
    parser.add_argument("--approval", required=True, type=Path)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--helper", required=True, type=Path)
    args = parser.parse_args()
    _EFFECT_STATE.update(phase="none", native_effects="none", effects_may_remain=False)
    def deadline(_signum, _frame):
        raise StopRun("600-second custody-prefix deadline exceeded")
    try:
        scope = verify_approval(args.scope, args.approval, Path(__file__).resolve())
        signal.signal(signal.SIGALRM, deadline)
        signal.alarm(scope["limits"]["child_deadline_seconds"])
        result = run_prefix(scope, args.source_root, args.helper)
    except Exception as error:
        print(json.dumps({"profile": PROFILE, "status": "blocked",
                          "native_effects": _EFFECT_STATE["native_effects"] != "none",
                          "reason": str(error)[:1500], "effect_state": dict(_EFFECT_STATE)},
                         sort_keys=True, separators=(",", ":")))
        return 2
    finally:
        signal.alarm(0)
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
