#!/usr/bin/env python3
"""Single-use bounded supervisor for the read-only R15 view-reason capture."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path('/private/tmp/ashlar-truss050-current-source-2eeecf40-r15')
R15_RESULT = Path('/Users/erik/Projects/ashlar/docs/helix/04-build/evidence/current-upstream-release-20261024/truss050-r15-selected-source-lifecycle-20261024/execution-result.json')
RUNTIME = Path('/private/tmp/ashlar-truss050-current-cohort-9c745278-r13/runtime/venv')
RUNTIME_RECEIPT = ROOT / 'runtime-receipt.json'
OLD48_VENV = Path('/private/tmp/ashlar-truss048-weft052-umf054-runtime-20261022/venv')
OLD48_RUNTIME = Path('/private/tmp/ashlar-truss048-weft052-umf054-runtime-20261022/runtime-preflight.json')
DATA = Path('/private/tmp/ashlar-truss048-source-acceptance-attempt-01-r4/postgres/data')
RUNTIME_LOCK = Path(str(DATA) + '.truss-runtime.lock')
SOCKET_ROOT = Path('/private/tmp/truss-pg18-501')
SOCKET_DIRECTORY = SOCKET_ROOT / '31d9238e0397bd8473c8bea3'
ATTEMPT = ROOT / 'catalog-view-reason-r1'
BACKUP = ATTEMPT / 'cold-backup' / 'pgdata'
LOGS = ATTEMPT / 'logs'
APPROVAL = ATTEMPT / 'approval.json'
STARTED = ATTEMPT / 'started.json'
RESULT = ATTEMPT / 'result.json'
PACKET = Path(__file__).resolve().parent
WORKER = PACKET / 'read_catalog_view_reason.py'
CLASSIFIER = Path('/Users/erik/Projects/ashlar/docs/helix/04-build/evidence/current-upstream-release-20261024/truss050-r15-selected-source-lifecycle-20261024/diagnostic-candidate/catalog_view_diagnostic.py')
PG_CTL = RUNTIME / 'lib/python3.12/site-packages/pgserver/pginstall/bin/pg_ctl'
PG_CTL_SHA256 = '59b641aabae44b43acfc34fd0bdeb9af9480338d39b25f3088d7c486fdd8b999'
R15_RESULT_SHA256 = '16e453fd14bc4ef355c299809d7c3f16b50cc8cf00a0e28436eb0ff95bd091bf'
R15_ORIGINAL_HELPER_SHA256 = 'cd9a39b76490db46e93d68edb44f5bc2b9cb978f4a152ded359a04ec66d3efcf'
R15_RUNTIME_RECEIPT_SHA256 = 'fe48796cead974bc06821bd296428f47b389e1016a7b43ff186ec15cb03f72e5'
OLD48_RUNTIME_RECEIPT_SHA256 = 'bbc590badfa88ea7c950a40f867ddd62f409df078aa461575a3b7db6c7288b7b'
OLD48_CATALOG_RUNTIME = OLD48_VENV / 'lib/python3.12/site-packages/truss/catalog_runtime.py'
OLD48_CATALOG_VIEW = OLD48_VENV / 'lib/python3.12/site-packages/truss/catalog_view.py'
R15_LOCAL_RUNTIME = RUNTIME / 'lib/python3.12/site-packages/truss/local_runtime.py'
OLD48_CATALOG_RUNTIME_SHA256 = 'c3090a85ae7a3d4b210509defc31514729670f62fb2755bd78dbc7852a3e31b0'
OLD48_CATALOG_VIEW_SHA256 = '167fc8fd2e304a9aaf9d485d957b0d124542617caa2f16c82f3d7aee42ec890c'
R15_LOCAL_RUNTIME_SHA256 = '7a0fce2fd418a3c239e7f6e847ac952d152d1967638251a4ebba2f5f29b90728'
DATA_TREE_SHA256 = 'abb464cc096dfce6bc6bd2720f6f6a17aa92c08af5a601146736ca71a308e6d2'
DATA_BYTES = 63998352
DATA_FILE_COUNT = 1248
MAX_CHILD_SECONDS = 120
MAX_BYTES = 1073741824
MAX_LOG_BYTES = 16777216


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def tree_manifest(root: Path) -> dict[str, object]:
    total = 0
    rows = []
    hasher = hashlib.sha256()
    for base, dirs, files in os.walk(root, followlinks=False):
        dirs.sort()
        files.sort()
        base_path = Path(base)
        for name in dirs + files:
            path = base_path / name
            if path.is_symlink():
                raise RuntimeError('Symlink in retained/attempt data tree')
        for name in files:
            path = base_path / name
            size = path.stat().st_size
            item_hash = digest(path)
            row = {'path': path.relative_to(root).as_posix(), 'bytes': size, 'sha256': item_hash}
            rows.append(row)
            total += size
            hasher.update((row['path'] + '\0' + str(size) + '\0' + item_hash + '\n').encode())
            if total > MAX_BYTES:
                raise RuntimeError('1 GiB retained/attempt tree bound exceeded')
    return {'bytes': total, 'file_count': len(rows), 'tree_sha256': hasher.hexdigest(), 'files': rows}


def tree_bytes(root: Path) -> int:
    total = 0
    for base, dirs, files in os.walk(root, followlinks=False):
        base_path = Path(base)
        for name in dirs + files:
            path = base_path / name
            if path.is_symlink():
                raise RuntimeError('Symlink in retained/attempt data tree')
            if path.is_file():
                total += path.stat().st_size
                if total > MAX_BYTES:
                    raise RuntimeError('1 GiB retained/attempt byte cap exceeded')
    return total


def write_new(path: Path, value: dict[str, object]) -> None:
    raw = (json.dumps(value, sort_keys=True, indent=2) + '\n').encode()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def build_approval_payload() -> dict[str, object]:
    r15 = json.loads(R15_RESULT.read_bytes())
    if digest(R15_RESULT) != R15_RESULT_SHA256 or r15['implementation']['source_helper_sha256'] != R15_ORIGINAL_HELPER_SHA256:
        raise RuntimeError('Failed R15 receipt or original helper differs from the reviewed record')
    if digest(RUNTIME_RECEIPT) != R15_RUNTIME_RECEIPT_SHA256 or digest(OLD48_RUNTIME) != OLD48_RUNTIME_RECEIPT_SHA256:
        raise RuntimeError('Pinned R15/.48 runtime receipt differs from the reviewed record')
    for path, expected_hash in (
        (OLD48_CATALOG_RUNTIME, OLD48_CATALOG_RUNTIME_SHA256),
        (OLD48_CATALOG_VIEW, OLD48_CATALOG_VIEW_SHA256),
        (R15_LOCAL_RUNTIME, R15_LOCAL_RUNTIME_SHA256),
        (PG_CTL, PG_CTL_SHA256),
    ):
        if digest(path) != expected_hash:
            raise RuntimeError('Pinned runtime implementation file changed: ' + path.name)
    return {
        'profile': 'ashlar-truss050-r15-catalog-view-reason-readonly-approval/0.1',
        'scope': 'one local PostgreSQL startup from the exact stopped post-R15 retained PGDATA; cold-copy and verify that current state first; execute only the pinned read-only catalog-view reason helper as the existing registrar; capture CatalogViewUnavailable.reason or available status; cleanly stop PostgreSQL and preserve all custody; no migration/source/feed/publication/ACK/seed/continuation operations',
        'selected_source': {
            'commit': '2eeecf409033c6c7c92ded72ec7e43c569dffaf9',
            'tree': 'a97e9e1e275294e840690bbe792e795afbeffff7',
        },
        'retained_state': {
            'data_path': str(DATA),
            'postgres_major': '18',
            'post_attempt_tree_sha256': DATA_TREE_SHA256,
            'post_attempt_bytes': DATA_BYTES,
            'post_attempt_file_count': DATA_FILE_COUNT,
            'cold_backup_path': str(BACKUP),
            'runtime_lock_path': str(RUNTIME_LOCK),
            'runtime_lock_bytes': 0,
            'runtime_lock_sha256': hashlib.sha256(b'').hexdigest(),
            'runtime_socket_directory': str(SOCKET_DIRECTORY),
            'runtime_socket_directory_must_be_empty_mode_0700': True,
            'postgres_log_path': str(DATA / 'log'),
            'postgres_log_cap_bytes': MAX_LOG_BYTES,
            'expected_external_runtime_effects': [
                'LocalPostgres reuses the existing empty runtime lock and empty private socket directory; transient socket files are removed by shutdown.',
                'PostgreSQL may change its internal data/WAL/control files and append to the bounded PGDATA log; the pre-start cold copy preserves the exact state.',
            ],
        },
        'inputs': {
            'failed_r15_result_sha256': digest(R15_RESULT),
            'r15_original_readonly_helper_sha256': R15_ORIGINAL_HELPER_SHA256,
            'truss050_runtime_receipt_sha256': R15_RUNTIME_RECEIPT_SHA256,
            'truss048_runtime_receipt_sha256': OLD48_RUNTIME_RECEIPT_SHA256,
            'truss048_runtime_wheel_sha256': '1aedf614ff3d3ff7a61bd4d91887ede650a7135d1239e8564c1c297c0ddc37c3',
            'truss048_catalog_runtime_sha256': OLD48_CATALOG_RUNTIME_SHA256,
            'truss048_catalog_view_sha256': OLD48_CATALOG_VIEW_SHA256,
            'truss050_local_runtime_sha256': R15_LOCAL_RUNTIME_SHA256,
            'reason_helper_sha256': digest(WORKER),
            'classifier_sha256': digest(CLASSIFIER),
            'supervisor_sha256': digest(Path(__file__).resolve()),
            'pg_ctl_sha256': PG_CTL_SHA256,
        },
        'limits': {
            'attempts': 1,
            'automatic_retries': 0,
            'child_deadline_seconds': MAX_CHILD_SECONDS,
            'combined_retained_and_attempt_bytes': MAX_BYTES,
            'log_bytes': MAX_LOG_BYTES,
            'parallelism': 'sequential local only',
            'cloud_resources': False,
        },
        'read_boundary': {
            'database_role': 'ashlar048_accept_consumer_r1',
            'transaction': 'READ COMMITTED READ ONLY',
            'statement_timeout_ms': 2000,
            'query': 'one Truss catalog_view_in_transaction request with complete scope and existing 65536-definition/16777216-byte bounds',
            'result_output': 'availability/reason only; no source bytes, report bytes, catalog-view bytes, or connection data',
            'forbidden_stages': ['migration', 'source mutation', 'feed mutation', 'publication', 'ACK', 'seed submission', 'continuation'],
        },
    }


def clean_environment() -> dict[str, str]:
    return {
        'PATH': str(RUNTIME / 'bin') + ':/usr/bin:/bin:/opt/homebrew/bin',
        'HOME': '/Users/erik',
        'PYTHONNOUSERSITE': '1',
        'PYTHONDONTWRITEBYTECODE': '1',
    }


def pg_owned() -> bool:
    marker = DATA / 'postmaster.pid'
    if not marker.exists():
        return False
    if marker.is_symlink():
        raise RuntimeError('Unexpected retained PostgreSQL PID symlink')
    lines = marker.read_text().splitlines()
    if len(lines) < 2 or Path(lines[1]).resolve() != DATA.resolve():
        raise RuntimeError('Retained PostgreSQL PID marker does not bind the selected data directory')
    return True


def stop_owned_pg() -> str:
    if not pg_owned():
        return 'postmaster_pid_absent'
    if PG_CTL.is_symlink() or not PG_CTL.is_file() or digest(PG_CTL) != PG_CTL_SHA256:
        return 'pinned_pg_ctl_unavailable'
    stopped = subprocess.run(
        [str(PG_CTL), '-D', str(DATA), '-m', 'fast', '-w', '-t', '20', 'stop'],
        cwd=ROOT, env=clean_environment(), stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=25,
    )
    return 'owned_postmaster_stopped_fast' if stopped.returncode == 0 and not pg_owned() else 'owned_postmaster_stop_failed'


def stop_worker_group(child: subprocess.Popen) -> None:
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        child.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        child.wait(timeout=5)


def verify_runtime_auxiliaries() -> None:
    if not DATA.parent.is_dir() or DATA.parent.is_symlink():
        raise RuntimeError('Retained PostgreSQL parent directory must already exist')
    if DATA.stat().st_mode & 0o777 != 0o700:
        raise RuntimeError('Retained PostgreSQL directory must already be mode 0700')
    expected_socket = SOCKET_ROOT / hashlib.sha256(os.fsencode(DATA.resolve())).hexdigest()[:24]
    if expected_socket != SOCKET_DIRECTORY:
        raise RuntimeError('Pinned Truss private socket-directory identity changed')
    if RUNTIME_LOCK.is_symlink() or not RUNTIME_LOCK.is_file() or RUNTIME_LOCK.stat().st_size != 0 or RUNTIME_LOCK.stat().st_uid != os.getuid():
        raise RuntimeError('Existing empty Truss runtime lock file required')
    if digest(RUNTIME_LOCK) != hashlib.sha256(b'').hexdigest():
        raise RuntimeError('Truss runtime lock file content changed')
    if SOCKET_ROOT.is_symlink() or not SOCKET_ROOT.is_dir() or SOCKET_ROOT.stat().st_mode & 0o777 != 0o700 or SOCKET_ROOT.stat().st_uid != os.getuid():
        raise RuntimeError('Existing Truss runtime socket root with mode 0700 required')
    if SOCKET_DIRECTORY.is_symlink() or not SOCKET_DIRECTORY.is_dir() or SOCKET_DIRECTORY.stat().st_mode & 0o777 != 0o700 or SOCKET_DIRECTORY.stat().st_uid != os.getuid():
        raise RuntimeError('Existing Truss runtime socket directory with mode 0700 required')
    if any(SOCKET_DIRECTORY.iterdir()):
        raise RuntimeError('Truss runtime socket directory must be empty before startup')
    pg_log = DATA / 'log'
    if pg_log.is_symlink() or (pg_log.exists() and pg_log.stat().st_size > MAX_LOG_BYTES):
        raise RuntimeError('Existing PostgreSQL log exceeds the 16 MiB bound')


def bounded_worker_environment(conninfo: str) -> dict[str, str]:
    import psycopg
    if 'password' in psycopg.conninfo.conninfo_to_dict(conninfo):
        raise RuntimeError('Passwordless local retained connection required')
    return {
        'PATH': str(OLD48_VENV / 'bin') + ':/usr/bin:/bin',
        'HOME': '/Users/erik',
        'PYTHONNOUSERSITE': '1',
        'PYTHONDONTWRITEBYTECODE': '1',
        'ASHLAR_LOCAL_PG_CONNINFO': conninfo,
    }


def execute() -> int:
    expected = build_approval_payload()
    if APPROVAL.is_symlink() or not APPROVAL.is_file() or APPROVAL.stat().st_size > 65536 or APPROVAL.stat().st_mode & 0o077:
        raise RuntimeError('Private bounded exact approval file required')
    approval = json.loads(APPROVAL.read_bytes())
    if approval != expected:
        raise RuntimeError('Fresh exact hash-bound read-only diagnostic approval required')
    if ATTEMPT.is_symlink():
        raise RuntimeError('Diagnostic output path must not be a symlink')
    if ATTEMPT.exists():
        allowed = {APPROVAL.name}
        if not ATTEMPT.is_dir() or any(p.name not in allowed for p in ATTEMPT.iterdir()):
            raise RuntimeError('Single-use diagnostic path already has output; no retry')
    if not DATA.is_dir() or DATA.is_symlink() or (DATA / 'PG_VERSION').read_text().strip() != '18' or pg_owned():
        raise RuntimeError('Exact stopped PostgreSQL 18 retained directory required')
    verify_runtime_auxiliaries()
    before = tree_manifest(DATA)
    if (before['tree_sha256'], before['bytes'], before['file_count']) != (DATA_TREE_SHA256, DATA_BYTES, DATA_FILE_COUNT):
        raise RuntimeError('Stopped retained PGDATA differs from approved post-R15 state')
    if BACKUP.exists() or BACKUP.is_symlink():
        raise RuntimeError('Fresh diagnostic backup path required; no retry')
    if Path(sys.executable).resolve() != (RUNTIME / 'bin/python').resolve():
        raise RuntimeError('Exact R15 Truss 0.0.50 runtime interpreter required')

    ATTEMPT.mkdir(mode=0o700, parents=True, exist_ok=True)
    write_new(STARTED, {
        'profile': 'ashlar-truss050-r15-catalog-view-reason-started/0.1',
        'approval_payload_sha256': hashlib.sha256(json.dumps(expected, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
        'supervisor_sha256': digest(Path(__file__).resolve()),
        'reason_helper_sha256': digest(WORKER),
        'prestart_retained_pgdata': {k: before[k] for k in ('bytes', 'file_count', 'tree_sha256')},
        'no_migration_or_source_feed_publication_stage': True,
    })
    start = time.monotonic()
    BACKUP.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    shutil.copytree(DATA, BACKUP, symlinks=False, copy_function=shutil.copy2)
    copied = tree_manifest(BACKUP)
    after_copy = tree_manifest(DATA)
    if (before['tree_sha256'], before['bytes']) != (copied['tree_sha256'], copied['bytes']) or before['tree_sha256'] != after_copy['tree_sha256']:
        raise RuntimeError('Cold diagnostic backup is not byte-identical to stopped retained PGDATA')
    if time.monotonic() - start >= MAX_CHILD_SECONDS:
        raise RuntimeError('Read-only diagnostic reached its 120-second bound before PostgreSQL startup')

    stdout = LOGS / 'reason.stdout.log'
    stderr = LOGS / 'reason.stderr.log'
    LOGS.mkdir(mode=0o700, parents=True, exist_ok=True)
    worker_result = None
    worker_exit = None
    cleanup_status = 'postmaster_not_started'
    child = None
    from truss import LocalPostgres
    try:
        with LocalPostgres(DATA) as runtime:
            remaining = MAX_CHILD_SECONDS - (time.monotonic() - start)
            if remaining <= 0:
                raise RuntimeError('Read-only diagnostic deadline elapsed during PostgreSQL startup')
            conninfo = runtime.info.connection_uri
            stdout_fd = os.open(stdout, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            stderr_fd = os.open(stderr, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            with os.fdopen(stdout_fd, 'wb') as stdout_stream, os.fdopen(stderr_fd, 'wb') as stderr_stream:
                child = subprocess.Popen(
                    [str(OLD48_VENV / 'bin/python'), '-I', '-B', str(WORKER)],
                    cwd=ROOT, env=bounded_worker_environment(conninfo), stdin=subprocess.DEVNULL,
                    stdout=stdout_stream, stderr=stderr_stream, start_new_session=True, close_fds=True,
                )
                while child.poll() is None:
                    if any(p.exists() and p.stat().st_size > MAX_LOG_BYTES for p in (stdout, stderr, DATA / 'log')):
                        stop_worker_group(child)
                        raise RuntimeError('Read-only diagnostic log cap exceeded')
                    if tree_bytes(DATA) + tree_bytes(ATTEMPT) > MAX_BYTES:
                        stop_worker_group(child)
                        raise RuntimeError('Combined retained/attempt byte cap exceeded')
                    remaining = MAX_CHILD_SECONDS - (time.monotonic() - start)
                    if remaining <= 0:
                        stop_worker_group(child)
                        raise RuntimeError('Read-only diagnostic exceeded 120-second deadline')
                    try:
                        child.wait(timeout=min(0.25, remaining))
                    except subprocess.TimeoutExpired:
                        continue
                worker_exit = child.returncode
    finally:
        if child is not None and child.poll() is None:
            stop_worker_group(child)
        cleanup_status = stop_owned_pg()
    verify_runtime_auxiliaries()

    for log in (stdout, stderr, DATA / 'log'):
        if log.is_symlink() or (log.exists() and log.stat().st_size > MAX_LOG_BYTES):
            raise RuntimeError('Diagnostic log cap exceeded')
    total = tree_manifest(DATA)['bytes'] + tree_manifest(ATTEMPT)['bytes']
    if total > MAX_BYTES:
        raise RuntimeError('Combined retained/attempt byte cap exceeded')
    if cleanup_status not in ('postmaster_pid_absent', 'owned_postmaster_stopped_fast') or pg_owned():
        raise RuntimeError('Retained PostgreSQL cleanup did not confirm stopped')
    if worker_exit == 0:
        lines = stdout.read_bytes().splitlines()
        if len(lines) == 1 and len(lines[0]) <= 65536:
            worker_result = json.loads(lines[0])
    if time.monotonic() - start > MAX_CHILD_SECONDS:
        raise RuntimeError('Read-only diagnostic exceeded 120-second attempt bound')
    write_new(RESULT, {
        'profile': 'ashlar-truss050-r15-catalog-view-reason-result/0.1',
        'worker_exit_code': worker_exit,
        'worker_result': worker_result,
        'cleanup_status': cleanup_status,
        'postgres_postmaster_pid_absent': not pg_owned(),
        'cold_backup_verified': True,
        'combined_retained_and_attempt_bytes': total,
        'limits': expected['limits'],
        'migration_or_source_feed_publication_ack_seed_continuation': 'not invoked',
    })
    return 0 if worker_exit == 0 and worker_result is not None else 1


if __name__ == '__main__':
    if len(sys.argv) != 2 or sys.argv[1] != '--execute':
        raise SystemExit('Only --execute is supported; it requires the exact approval file')
    raise SystemExit(execute())
