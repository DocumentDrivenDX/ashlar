from __future__ import annotations

import argparse
import ast
import hashlib
import json
import shutil
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
PRIVATE = Path('/private/tmp/ashlar-truss049-current-cohort-1f8c3fc2-r7')
REPO = Path('/Users/erik/Projects/ashlar')
COMMIT = '7c4006e880b037e2de318d04b45a8aeae9dfea12'
TREE = '65377e9c97bab62b50a1eef7d0314cb390f8f2e4'
OBJECTS = {
    'src': '6678e0d1cff4f8c43b0a28af618253559d4de1ce',
    'sql': '3cfe250fadd42ef4b25597f0eec7d10a426ce8ff',
    'pyproject.toml': 'ba55a552124b69cfc927cefc5d309af9c76e6ded',
}
ARCHIVE_SHA = '49f1eea46cbfcf8ddc33ce841669fd9de4e41ad427863e4598f30bf92b6dfaba'
SOURCE_MANIFEST_SHA = '00d1902bda5821e478c76d8b1a264b11e59e88d3d978d7b5d34ea7a6691cb9d5'
RUNTIME_SHA = '517ac6892280e911060688afc75528907e62aa5d6d290abf6d9b166f5da5617a'
LOCK_SHA = 'fce1c31381495aea07ae356dd1f54d35210b48e37c69ff8a7ae43dfa48687703'
RUNTIME_PROVENANCE_SHA = 'eb3d7da5d8197acca902225f0215ad461b10ac49573d05b4f097bea1060d759e'


def unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'duplicate JSON key: {key}')
        result[key] = value
    return result


def load(path: Path):
    return json.loads(path.read_text(), object_pairs_hook=unique_pairs)


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def git_blob(path: Path) -> str:
    payload = path.read_bytes()
    return hashlib.sha1(f'blob {len(payload)}\0'.encode() + payload).hexdigest()


def git_output(*args: str) -> bytes:
    executable = shutil.which('git')
    require(executable is not None, 'git executable required for immutable source-tree check')
    result = subprocess.run(
        [executable, '-C', str(REPO), *args], cwd=REPO,
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        check=False, timeout=10,
    )
    require(result.returncode == 0, 'pinned Git object lookup failed')
    return result.stdout


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--allow-cleanup-pending", action="store_true", help="run static binding checks while redundant setup snapshots are still being removed; does not grant root-size admission")
    args = parser.parse_args()
    require(sha(HERE / 'source-inputs-manifest.json') == SOURCE_MANIFEST_SHA,
            'tracked source manifest digest')
    require(sha(HERE / 'runtime-provenance.json') == RUNTIME_PROVENANCE_SHA,
            'tracked runtime provenance digest')
    require(PRIVATE.is_dir() and not PRIVATE.is_symlink(), 'private R7 root missing or symlinked')
    archive = PRIVATE / 'source-inputs-7c4006e.tar'
    source = PRIVATE / 'source-inputs-7c4006e'
    manifest_path = PRIVATE / 'source-inputs-manifest.json'
    runtime_path = PRIVATE / 'runtime-receipt.json'
    lock_path = PRIVATE / 'runtime-wheel-lock.json'
    require(sha(archive) == ARCHIVE_SHA, 'source archive digest')
    require(sha(manifest_path) == SOURCE_MANIFEST_SHA, 'source manifest digest')
    manifest = load(manifest_path)
    require(manifest['profile'] == 'ashlar-selected-source-inputs/1', 'source manifest profile')
    require(manifest['commit'] == COMMIT and manifest['tree'] == TREE, 'selected source commit/tree')
    require(manifest['objects'] == OBJECTS, 'selected source object map')
    require(manifest['archive_sha256'] == ARCHIVE_SHA, 'manifest/archive binding')
    require(git_output('rev-parse', f'{COMMIT}^{{tree}}').decode().strip() == TREE,
            'selected commit tree object')
    for path, expected_object in OBJECTS.items():
        observed = git_output('rev-parse', f'{COMMIT}:{path}').decode().strip()
        require(observed == expected_object, f'pinned Git object: {path}')
    rows = manifest['files']
    require(type(rows) is list and len(rows) == 587, 'source manifest row count')
    require(all(type(row) is dict and set(row) == {'path', 'bytes', 'sha256', 'git_blob'} for row in rows),
            'closed source manifest row schema')
    row_paths = [row['path'] for row in rows]
    require(len(set(row_paths)) == len(row_paths), 'unique source manifest paths')
    require(all(path == 'pyproject.toml' or path.startswith(('src/', 'sql/')) for path in row_paths),
            'source manifest path scope')
    expected_paths = {row['path'] for row in rows}
    require(source.is_dir() and not source.is_symlink(), 'selected source extraction root')
    entries = list(source.rglob('*'))
    require(not any(path.is_symlink() for path in entries), 'source extraction contains no symlinks')
    require(all(path.is_file() or path.is_dir() for path in entries), 'source extraction contains only files and directories')
    actual_paths = {p.relative_to(source).as_posix() for p in entries if p.is_file()}
    require(actual_paths == expected_paths, 'source archive file inventory')
    expected_dirs = {
        parent.as_posix()
        for name in expected_paths
        for parent in Path(name).parents
        if parent.as_posix() != '.'
    }
    actual_dirs = {p.relative_to(source).as_posix() for p in entries if p.is_dir()}
    require(actual_dirs == expected_dirs, 'source archive directory inventory')
    git_rows = git_output('ls-tree', '-r', '-z', '--full-tree', COMMIT, '--', 'src', 'sql', 'pyproject.toml')
    git_paths: dict[str, tuple[str, str]] = {}
    for item in git_rows.split(b'\0'):
        if not item:
            continue
        metadata, raw_path = item.split(b'\t', 1)
        mode, kind, object_id = metadata.decode('ascii').split(' ')
        path = raw_path.decode('utf-8')
        require(mode in {'100644', '100755'} and kind == 'blob', f'regular Git file required: {path}')
        require(path not in git_paths, f'duplicate Git tree path: {path}')
        git_paths[path] = (mode, object_id)
    require(set(git_paths) == expected_paths, 'manifest path set differs from pinned Git tree')
    for row in rows:
        path = source / row['path']
        require(not path.is_symlink(), f'symlinked selected source: {row["path"]}')
        require(git_rows and git_paths[row['path']][1] == row['git_blob'],
                f'manifest Git blob differs from pinned commit: {row["path"]}')
        payload = path.read_bytes()
        require(len(payload) == row['bytes'], f'selected source size: {row["path"]}')
        require(hashlib.sha256(payload).hexdigest() == row['sha256'], f'selected source SHA-256: {row["path"]}')
        require(git_blob(path) == row['git_blob'], f'selected Git blob: {row["path"]}')

    require(sha(runtime_path) == RUNTIME_SHA, 'runtime receipt digest')
    require(sha(lock_path) == LOCK_SHA, 'runtime wheel lock digest')
    runtime = load(runtime_path)
    require(runtime['runtime']['python'] == '3.12.14', 'runtime Python')
    require(runtime['runtime']['platform'] == 'macOS arm64', 'runtime platform')
    require(runtime['runtime']['include_system_site_packages'] is False, 'runtime isolation')
    require(runtime['install']['pip_check'] == 'No broken requirements found.', 'pip check')
    require(runtime['install']['wheel_count'] == 27, 'wheel count')
    require(runtime['install']['verified_record_payload_count'] == 4001, 'installed RECORD payload count')
    expected_versions = {
        'truss-toolkit': '0.0.49', 'umf-core': '0.10.1', 'umf_access': '0.1.0.dev7',
        'weft-sql': '0.4.53', 'pyspark': '4.2.0', 'delta-spark': '4.4.1',
        'pgserver-pg18': '18.6.1', 'psycopg': '3.3.6',
    }
    require(runtime['install']['versions'] == expected_versions, 'runtime version tuple')
    require(runtime['runtime']['size_cap_bytes'] == 1024 ** 3, 'runtime size cap')
    require(runtime['runtime']['size_bytes_including_install_log'] < 1024 ** 3, 'runtime measured size cap')

    prep_path = PRIVATE / 'preparation.json'
    worker_path = PRIVATE / 'campaign_worker.py'
    supervisor_path = PRIVATE / 'run_supervisor.py'
    bridge_path = PRIVATE / 'bridge-readiness.json'
    require(sha(bridge_path) == sha(HERE / 'bridge-readiness.json'), 'private/tracked bridge pin')
    for path in (prep_path, worker_path, supervisor_path, bridge_path, PRIVATE / 'seed-request.json',
                 PRIVATE / 'source_048_readonly_helper.py', PRIVATE / 'weft053-compatibility-receipt.md'):
        require(path.is_file() and not path.is_symlink(), f'packet file missing: {path.name}')
    require(sha(HERE / 'preparation.json') == sha(prep_path), 'tracked/private preparation snapshot')
    require(sha(HERE / 'campaign_worker.py') == sha(worker_path), 'tracked/private worker bytes')
    require(sha(HERE / 'run_supervisor.py') == sha(supervisor_path), 'tracked/private supervisor bytes')
    prep = load(prep_path)
    bridge = load(bridge_path)
    require(prep['profile'] == 'ashlar-truss-0.0.49-current-cohort-preparation/0.1', 'preparation profile')
    require(prep['attempt'] == 'r7', 'attempt label')
    require(prep['source_checkout']['commit'] == COMMIT and prep['source_checkout']['tree'] == TREE,
            'preparation source commit/tree')
    require(prep['source_checkout']['runtime_input_objects'] == OBJECTS, 'preparation source objects')
    require(prep['runtime_receipt']['sha256'] == RUNTIME_SHA, 'preparation runtime receipt')
    require(prep['runtime_receipt']['path'] == str(runtime_path), 'preparation runtime path')
    require(prep['execution_authorization']['authorized'] is False, 'execution authorization')
    require(prep['execution_authorization']['approval_file_created'] is False, 'approval marker state')
    require(all(value is False for value in prep['execution'].values()), 'execution effect flags')
    require(prep['future_preflight']['status'] == 'not_run', 'preflight state')
    require(bridge['ashlar_commit'] == COMMIT and bridge['runtime_input_objects'] == OBJECTS,
            'bridge source binding')
    require(bridge['status'] == 'ready' and bridge['umf_commit'] == '259bf1ed91466ff7e9782d7517c14f2112282026',
            'published UMF bridge binding')
    ast.parse(worker_path.read_text(), filename=str(worker_path))
    ast.parse(supervisor_path.read_text(), filename=str(supervisor_path))
    require(prep['worker']['sha256'] == sha(worker_path), 'worker digest pin')
    require(prep['supervisor']['sha256'] == sha(supervisor_path), 'supervisor digest pin')
    require(not (PRIVATE / 'execution-approval.json').exists(), 'execution approval must be absent')
    require(not (PRIVATE / 'execution-started-r7.json').exists(), 'execution marker must be absent')
    require(not (PRIVATE / 'offline-preflight-r7.json').exists(), 'preflight result must be absent')
    attempt = PRIVATE / 'attempt-r7'
    require(attempt.is_dir() and not attempt.is_symlink() and not any(attempt.iterdir()),
            'fresh empty attempt directory required')
    require(attempt.stat().st_mode & 0o077 == 0, 'attempt directory must be private')
    require(prep['resource_limits']['child_execution_deadline_seconds'] == 600, 'child time cap')
    require(prep['resource_limits']['maximum_combined_retained_and_new_bytes'] == 1024 ** 3,
            'combined data/log cap')
    require(prep['resource_limits']['max_log_bytes'] == 16 * 1024 * 1024, 'worker log cap')
    require(prep['resource_limits']['automatic_retries'] == 0, 'retry cap')

    owned_bytes = sum(p.stat().st_size for p in PRIVATE.rglob('*') if p.is_file() and not p.is_symlink())
    if args.allow_cleanup_pending:
        print(f'R7 static bindings valid; cleanup pending; root_bytes={owned_bytes}; size admission not granted')
    else:
        require(owned_bytes < 1024 ** 3, 'R7 root must remain below 1 GiB')
        print(f'R7 static packet valid; source/runtime bound; no preflight/native effects; root_bytes={owned_bytes}')


if __name__ == '__main__':
    main()
