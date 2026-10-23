"""Explicitly gated, sequential one-pair local ZSTD/SNAPPY supervisor.

This runner does not target Databricks or a selected Ashlar warehouse. It must
be invoked with --execute-reviewed-pair after owner review; importing or
running without that flag is inert.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

REPO = Path('/Users/erik/Projects/ashlar')
SCRIPT = REPO / 'docs/helix/04-build/evidence/edge-write-attribution-20261023/run.py'
PYTHON = Path('/private/tmp/ashlar-release-20261021-m3rde5p2/venv/bin/python')
WHEEL = Path('/private/tmp/ashlar-release-20261021-m3rde5p2/wheelhouse-bc2accd8/ashlar_graph_toolkit-0.1.0.dev1-py3-none-any.whl')
COMMIT = 'bc2accd883a49d34c282dc002af98b4b05ea9440'
RUNNER_SHA256 = 'b132dcf5d4d80fdaec1c47cdaee9980b3ba4a1ee3291ecf95f8d73b201f74003'
INSERT_SQL_SHA256 = '088d365593b311cd9c75f14b9129cf4beaab24657e126a0d538de3fa99f2ff5e'
ROOT = Path('/private/tmp/ashlar-r05-codec-pair-20261023-02')
OUTPUT_CAP = 4_000_000_000
DIAGNOSTIC_CAP = 128_000_000
MIN_FREE = 12_000_000_000
WALL_CAP = 300
TERMINATE_AT = 290


class PairInterrupted(Exception):
    pass


def _interrupt_handler(signum, _frame):
    raise PairInterrupted(f'received signal {signum}')


def tree_bytes(path: Path) -> int:
    total = 0
    if path.exists():
        for item in path.rglob('*'):
            if item.is_file():
                try:
                    total += item.stat().st_size
                except FileNotFoundError:
                    pass
    return total


def process_group_members(pgid: int, *, runner=subprocess.run) -> list[dict]:
    """Return an unambiguous bounded OS inventory for one process group."""
    result = runner(['ps', '-axo', 'pid=,pgid=,command='], check=True,
                    capture_output=True, text=True, timeout=10)
    if result.returncode != 0 or not isinstance(result.stdout, str):
        raise RuntimeError('process inventory did not return successful text output')
    members = []
    seen_pids = set()
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        fields = line.strip().split(None, 2)
        if len(fields) < 2:
            raise RuntimeError(f'process inventory returned ambiguous row: {line[:200]}')
        try:
            pid, observed_pgid = int(fields[0]), int(fields[1])
        except ValueError as exc:
            raise RuntimeError(f'process inventory returned malformed PID/PGID: {line[:200]}') from exc
        if pid <= 0 or observed_pgid <= 0 or pid in seen_pids:
            raise RuntimeError(f'process inventory returned invalid or duplicate PID/PGID: {line[:200]}')
        seen_pids.add(pid)
        if observed_pgid == pgid:
            members.append({'pid': pid, 'pgid': observed_pgid,
                            'command': fields[2] if len(fields) > 2 else ''})
    return members


def _group_alive(pgid: int, killpg=os.killpg) -> bool:
    # killpg is accepted to preserve the test seam; membership is determined
    # from the OS process inventory, never from a permission-sensitive signal 0.
    return bool(process_group_members(pgid))


def terminate_and_reap(proc, pgid: int, stop_cause: str, *, killpg=os.killpg,
                       group_alive=_group_alive, sleep=time.sleep,
                       monotonic=time.monotonic, inventory=process_group_members) -> dict:
    """Terminate the full child group, reap its leader, and report uncertainty."""
    old_handlers = {}
    if __name__ == '__main__':
        for sig in (signal.SIGINT, signal.SIGTERM):
            old_handlers[sig] = signal.signal(sig, signal.SIG_IGN)
    receipt = {
        'stop_cause': stop_cause, 'process_group_id': pgid,
        'sigterm_sent': False, 'sigkill_sent': False,
        'leader_reaped': False, 'remaining_process_group_uncertain': False,
    }
    try:
        killpg(pgid, signal.SIGTERM)
        receipt['sigterm_sent'] = True
    except ProcessLookupError:
        pass
    except PermissionError as exc:
        # A denied signal does not establish that the group is alive. Require
        # the independent bounded ps inventory; inability/ambiguity stays fail-closed.
        receipt['termination_error'] = f'PermissionError: {exc}'[:500]
        try:
            receipt['post_sigterm_process_inventory'] = inventory(pgid)
            receipt['permission_fallback_inventory_used'] = True
        except BaseException as inventory_exc:
            receipt['remaining_process_group_uncertain'] = True
            receipt['group_probe_error'] = f'{type(inventory_exc).__name__}: {inventory_exc}'[:500]
    except BaseException as exc:
        receipt['remaining_process_group_uncertain'] = True
        receipt['termination_error'] = f'{type(exc).__name__}: {exc}'[:500]

    deadline = monotonic() + 8
    while monotonic() < deadline:
        try:
            proc.wait(timeout=0.1)
        except subprocess.TimeoutExpired:
            pass
        except BaseException as exc:
            receipt['remaining_process_group_uncertain'] = True
            receipt['wait_error'] = f'{type(exc).__name__}: {exc}'[:500]
        try:
            alive = group_alive(pgid, killpg=killpg)
        except BaseException as exc:
            alive = True
            receipt['remaining_process_group_uncertain'] = True
            receipt['group_probe_error'] = f'{type(exc).__name__}: {exc}'[:500]
        if not alive and proc.poll() is not None:
            break
        sleep(0.25)

    try:
        alive = group_alive(pgid, killpg=killpg)
    except BaseException as exc:
        alive = True
        receipt['remaining_process_group_uncertain'] = True
        receipt['group_probe_error'] = f'{type(exc).__name__}: {exc}'[:500]
    if alive:
        try:
            killpg(pgid, signal.SIGKILL)
            receipt['sigkill_sent'] = True
        except ProcessLookupError:
            alive = False
        except BaseException as exc:
            receipt['remaining_process_group_uncertain'] = True
            receipt['kill_error'] = f'{type(exc).__name__}: {exc}'[:500]
    try:
        proc.wait(timeout=2)
        receipt['leader_reaped'] = True
    except subprocess.TimeoutExpired:
        try:
            killpg(pgid, signal.SIGKILL)
            receipt['sigkill_sent'] = True
        except BaseException as exc:
            receipt['remaining_process_group_uncertain'] = True
            receipt['kill_error'] = f'{type(exc).__name__}: {exc}'[:500]
        try:
            proc.wait(timeout=2)
            receipt['leader_reaped'] = True
        except BaseException as exc:
            receipt['remaining_process_group_uncertain'] = True
            receipt['wait_error'] = f'{type(exc).__name__}: {exc}'[:500]
    except BaseException as exc:
        receipt['remaining_process_group_uncertain'] = True
        receipt['wait_error'] = f'{type(exc).__name__}: {exc}'[:500]
    try:
        receipt['post_cleanup_process_inventory'] = inventory(pgid)
        receipt['remaining_process_group_uncertain'] |= bool(receipt['post_cleanup_process_inventory'])
    except BaseException as exc:
        receipt['remaining_process_group_uncertain'] = True
        receipt['group_probe_error'] = f'{type(exc).__name__}: {exc}'[:500]
    for sig, handler in old_handlers.items():
        signal.signal(sig, handler)
    return receipt


def monitor_child(proc, pgid: int, check, *, terminate=terminate_and_reap,
                  sleep=time.sleep, group_alive=_group_alive) -> dict:
    """Monitor one child and ensure process-group cleanup on all exit paths."""
    receipt = {'stop_cause': None, 'limit_triggered': None,
               'monitor_exception': None, 'cleanup': None, 'cleanup_required': False,
               'returncode': None}
    try:
        while proc.poll() is None:
            stop_cause = check()
            if stop_cause:
                receipt['stop_cause'] = stop_cause
                receipt['limit_triggered'] = stop_cause
                receipt['cleanup'] = terminate(proc, pgid, stop_cause)
                break
            sleep(0.5)
        receipt['returncode'] = proc.wait()
    except BaseException as exc:
        receipt['stop_cause'] = f'{type(exc).__name__}: {exc}'[:500]
        receipt['limit_triggered'] = 'monitor-exception'
        receipt['monitor_exception'] = receipt['stop_cause']
        receipt['cleanup'] = terminate(proc, pgid, receipt['stop_cause'])
    finally:
        try:
            still_running = proc.poll() is None
            group_remains = group_alive(pgid)
        except BaseException as exc:
            still_running = True
            group_remains = True
            receipt['stop_cause'] = receipt['stop_cause'] or f'process-group probe failed: {type(exc).__name__}: {exc}'[:500]
            receipt['limit_triggered'] = receipt['limit_triggered'] or 'monitor-exception'
            receipt['monitor_exception'] = receipt['monitor_exception'] or receipt['stop_cause']
        if still_running or group_remains:
            receipt['cleanup_required'] = receipt['stop_cause'] is None
            receipt['stop_cause'] = receipt['stop_cause'] or 'child leader exited with process-group members remaining'
            receipt['cleanup'] = terminate(proc, pgid, receipt['stop_cause'])
        if proc.poll() is None:
            receipt['returncode'] = None
        else:
            receipt['returncode'] = proc.returncode
        cleanup = receipt['cleanup'] or {}
        receipt['remaining_process_group_uncertain'] = bool(cleanup.get('remaining_process_group_uncertain', False))
        if still_running or group_remains:
            receipt['remaining_process_group_uncertain'] |= receipt['returncode'] is None
    return receipt


def launch_monitored_child(command, env, log, check, *, popen=subprocess.Popen,
                           terminate=terminate_and_reap, group_alive=_group_alive) -> dict:
    """Close the Popen-to-monitor interruption window and always clean its group."""
    proc = None
    monitor = {'stop_cause': None, 'limit_triggered': None,
               'monitor_exception': None, 'cleanup': None,
               'cleanup_required': False, 'remaining_process_group_uncertain': False,
               'returncode': None}
    try:
        previous_mask = None
        if hasattr(signal, 'pthread_sigmask'):
            previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM})
        try:
            proc = popen(command, env=env, stdout=log, stderr=subprocess.STDOUT,
                         start_new_session=True)
        finally:
            if previous_mask is not None:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        monitor = monitor_child(proc, proc.pid, check, terminate=terminate,
                                group_alive=group_alive)
    except BaseException as exc:
        if proc is None:
            raise
        cause = f'{type(exc).__name__}: {exc}'[:500]
        monitor.update({'stop_cause': cause, 'limit_triggered': 'monitor-exception',
                        'monitor_exception': cause})
        try:
            monitor['cleanup'] = terminate(proc, proc.pid, cause)
        except BaseException as cleanup_exc:
            monitor['remaining_process_group_uncertain'] = True
            monitor['cleanup'] = {'stop_cause': cause, 'remaining_process_group_uncertain': True,
                                  'cleanup_error': f'{type(cleanup_exc).__name__}: {cleanup_exc}'[:500]}
    finally:
        if proc is not None:
            try:
                active = proc.poll() is None or group_alive(proc.pid)
            except BaseException as probe_exc:
                active = True
                monitor['remaining_process_group_uncertain'] = True
                monitor['monitor_exception'] = monitor['monitor_exception'] or f'{type(probe_exc).__name__}: {probe_exc}'[:500]
                monitor['limit_triggered'] = monitor['limit_triggered'] or 'monitor-exception'
            if active:
                cause = monitor['stop_cause'] or 'Popen/monitor finalizer found child process-group members'
                monitor['stop_cause'] = cause
                monitor['cleanup_required'] = True
                try:
                    monitor['cleanup'] = terminate(proc, proc.pid, cause)
                except BaseException as cleanup_exc:
                    monitor['remaining_process_group_uncertain'] = True
                    monitor['cleanup'] = {'stop_cause': cause, 'remaining_process_group_uncertain': True,
                                          'cleanup_error': f'{type(cleanup_exc).__name__}: {cleanup_exc}'[:500]}
            try:
                monitor['returncode'] = proc.wait(timeout=2) if proc.poll() is None else proc.returncode
            except BaseException as wait_exc:
                monitor['remaining_process_group_uncertain'] = True
                monitor['monitor_exception'] = monitor['monitor_exception'] or f'{type(wait_exc).__name__}: {wait_exc}'[:500]
                monitor['limit_triggered'] = monitor['limit_triggered'] or 'monitor-exception'
            cleanup = monitor.get('cleanup') or {}
            monitor['remaining_process_group_uncertain'] |= bool(cleanup.get('remaining_process_group_uncertain', False))
    return monitor


def load_progress(case_root: Path) -> list[dict]:
    path = case_root / 'out' / 'progress.jsonl'
    if not path.is_file():
        raise RuntimeError(f'missing progress receipt: {path}')
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def stage(rows: list[dict], name: str) -> dict:
    found = [row for row in rows if row.get('stage') == name]
    if len(found) != 1:
        raise RuntimeError(f'expected one {name} receipt, found {len(found)}')
    return found[0]


def ensure_pair_deadline(pair_started: float, phase: str) -> None:
    elapsed = time.monotonic() - pair_started
    if elapsed >= WALL_CAP:
        raise RuntimeError(f'pair hard deadline reached before {phase}: {elapsed:.3f}s')
    if elapsed >= TERMINATE_AT:
        raise RuntimeError(f'pair termination threshold reached before {phase}: {elapsed:.3f}s')


def ensure_runner_pin() -> None:
    observed = hashlib.sha256(SCRIPT.read_bytes()).hexdigest()
    if observed != RUNNER_SHA256:
        raise RuntimeError(f'runner source drift: expected {RUNNER_SHA256}, observed {observed}')


def run_case(codec: str, pair_started: float) -> dict:
    ensure_pair_deadline(pair_started, f'{codec} case')
    ensure_runner_pin()
    case_root = ROOT / codec
    out = case_root / 'out'
    case_root.mkdir(mode=0o700)
    free = shutil.disk_usage(ROOT).free
    if free < MIN_FREE:
        raise RuntimeError(f'{codec}: need at least {MIN_FREE} free bytes, observed {free}')

    env = os.environ.copy()
    env.update({
        'JAVA_HOME': '/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home',
        'PATH': '/opt/homebrew/opt/openjdk@21/bin:' + os.environ.get('PATH', ''),
        'PYSPARK_PYTHON': str(PYTHON),
        'PYSPARK_SUBMIT_ARGS': '--driver-memory 24g pyspark-shell',
    })
    command = [
        str(PYTHON), '-B', str(SCRIPT), '--repo', str(REPO), '--wheel', str(WHEEL),
        '--commit', COMMIT, '--out', str(out), '--codec', codec,
        '--minimum-free-bytes', str(MIN_FREE),
    ]
    log_path = case_root / 'runner.log'
    started = time.monotonic()
    with log_path.open('xb') as log:
        def check_limits():
            elapsed = time.monotonic() - pair_started
            output = tree_bytes(case_root)
            warehouse = tree_bytes(out / 'warehouse')
            diagnostics = output - warehouse
            if elapsed >= TERMINATE_AT:
                return 'pair-wall-300s-termination-threshold-290s'
            if output > OUTPUT_CAP:
                return 'case-output-4GB'
            if diagnostics > DIAGNOSTIC_CAP:
                return 'case-diagnostics-128MB'
            return None
        monitor = launch_monitored_child(command, env, log, check_limits)

    ensure_runner_pin()

    accounting_error = None
    try:
        output = tree_bytes(case_root)
        warehouse = tree_bytes(out / 'warehouse')
        diagnostics = output - warehouse
    except BaseException as exc:
        output = warehouse = diagnostics = None
        accounting_error = f'{type(exc).__name__}: {exc}'[:500]
    limit = monitor['limit_triggered']
    if limit is None and output is not None and output > OUTPUT_CAP:
        limit = 'case-output-4GB-observed-after-exit'
    if limit is None and diagnostics is not None and diagnostics > DIAGNOSTIC_CAP:
        limit = 'case-diagnostics-128MB-observed-after-exit'
    (case_root / 'supervisor-result.json').write_text(json.dumps({
        'codec': codec.upper(), 'command': command, 'returncode': monitor['returncode'],
        'elapsed_seconds': round(time.monotonic() - started, 3),
        'pair_elapsed_seconds': round(time.monotonic() - pair_started, 3),
        'limit_triggered': limit, 'stop_cause': monitor['stop_cause'],
        'monitor_exception': monitor['monitor_exception'], 'cleanup': monitor['cleanup'],
        'cleanup_required': monitor['cleanup_required'], 'accounting_error': accounting_error,
        'remaining_process_group_uncertain': monitor['remaining_process_group_uncertain'],
        'case_output_bytes': output,
        'case_diagnostic_bytes': diagnostics, 'output_cap_bytes': OUTPUT_CAP,
        'diagnostic_cap_bytes': DIAGNOSTIC_CAP, 'minimum_free_bytes': MIN_FREE,
        'pair_wall_cap_seconds': WALL_CAP, 'termination_trigger_seconds': TERMINATE_AT,
        'monitor_interval_seconds': 0.5,
        'bounds_are_polled_thresholds_not_filesystem_quotas': True,
    }, sort_keys=True, separators=(',', ':')) + '\n')
    if monitor['monitor_exception']:
        raise RuntimeError(f"{codec}: monitor exception: {monitor['monitor_exception']}; cleanup={monitor['cleanup']}")
    if accounting_error:
        raise RuntimeError(f'{codec}: post-cleanup output accounting failed: {accounting_error}')
    if monitor['remaining_process_group_uncertain']:
        raise RuntimeError(f'{codec}: child process-group cleanup is uncertain; refusing to continue pair')
    if monitor['cleanup_required']:
        raise RuntimeError(f"{codec}: unexpected process-group members required cleanup: {monitor['cleanup']}")
    if monitor['returncode'] != 0 or limit:
        raise RuntimeError(f'{codec}: child failed or a resource limit fired; inspect {log_path}')

    progress = load_progress(case_root)
    insert = stage(progress, 'insert_completed')
    footer = stage(progress, 'footer_verified')
    metadata = stage(progress, 'carrier_table_ready')
    actual_sql_sha = stage(progress, 'plan_captured')['insert_sql_sha256']
    if actual_sql_sha != INSERT_SQL_SHA256:
        raise RuntimeError(f'{codec}: executed INSERT digest drift: {actual_sql_sha}')
    if footer['rows'] != 225_000_000 or footer['files'] != 18 or footer['codecs'] != [codec.upper()]:
        raise RuntimeError(f'{codec}: row, file, or footer codec receipt differs')
    if metadata['codec'] != codec.upper():
        raise RuntimeError(f'{codec}: table property did not read back')
    return {
        'codec': codec.upper(), 'experiment_profile': stage(progress, 'admitted')['experiment_profile'],
        'insert_seconds': insert['write_seconds'], 'insert_sql_sha256': actual_sql_sha,
        'rows': footer['rows'], 'files': footer['files'], 'parquet_bytes': footer['parquet_bytes'],
        'codecs': footer['codecs'], 'schema_sha256': metadata['schema_sha256'],
        'metadata_schema_sha256': footer['metadata_schema_sha256'],
        'protocol': footer['protocol'], 'metadata_configuration': footer['metadata_configuration'],
        'persisted_witnesses': footer['persisted_witnesses'],
        'table_path': metadata['table_path'],
        'case_root': str(case_root),
    }


def remove_owned_warehouse(result: dict) -> None:
    case_root = Path(result['case_root']).resolve()
    table_path = Path(result['table_path']).resolve()
    warehouse = (case_root / 'out' / 'warehouse').resolve()
    if table_path != (warehouse / 'edge_current').resolve() or not table_path.is_relative_to(warehouse):
        raise RuntimeError('refusing cleanup: table location is not the exact owned warehouse')
    if warehouse.exists():
        shutil.rmtree(warehouse)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute-reviewed-pair', action='store_true',
                        help='explicitly enable the two bounded local Spark cases')
    args = parser.parse_args()
    if not args.execute_reviewed_pair:
        parser.error('inert by default; pass --execute-reviewed-pair only after owner review')
    if ROOT.exists():
        raise SystemExit(f'pair root already exists; refusing reuse: {ROOT}')
    if not SCRIPT.is_file() or not PYTHON.is_file() or not WHEEL.is_file():
        raise SystemExit('pinned runner, Python environment, or wheel is unavailable')
    signal.signal(signal.SIGINT, _interrupt_handler)
    signal.signal(signal.SIGTERM, _interrupt_handler)
    ROOT.mkdir(mode=0o700, parents=True)
    pair_started = time.monotonic()
    ensure_pair_deadline(pair_started, 'ZSTD case')
    baseline = run_case('zstd', pair_started)
    remove_owned_warehouse(baseline)
    ensure_pair_deadline(pair_started, 'SNAPPY case')
    candidate = run_case('snappy', pair_started)

    if baseline['insert_sql_sha256'] != candidate['insert_sql_sha256']:
        raise RuntimeError('pair invalid: executed INSERT SQL digests differ')
    for key in ('rows', 'files', 'schema_sha256', 'metadata_schema_sha256', 'protocol', 'persisted_witnesses'):
        if baseline[key] != candidate[key]:
            raise RuntimeError(f'pair invalid: {key} differs')
    base_config = dict(baseline['metadata_configuration'])
    candidate_config = dict(candidate['metadata_configuration'])
    if base_config.pop('delta.parquet.compression.codec', None) != 'zstd':
        raise RuntimeError('pair invalid: ZSTD baseline table property differs')
    if candidate_config.pop('delta.parquet.compression.codec', None) != 'snappy':
        raise RuntimeError('pair invalid: SNAPPY candidate table property differs')
    if base_config != candidate_config:
        raise RuntimeError('pair invalid: table configurations differ beyond codec')
    ensure_runner_pin()
    ensure_pair_deadline(pair_started, 'final acceptance')

    pair = {
        'status': 'valid-one-pair-observation', 'baseline': baseline, 'candidate': candidate,
        'candidate_below_22_72s': candidate['insert_seconds'] < 22.72,
        'elapsed_seconds': round(time.monotonic() - pair_started, 3),
        'full_end_to_end_few_minute_target': 'open; this pair is edge-write-only',
        'release_or_scale_qualification': False,
    }
    (ROOT / 'pair-result.json').write_text(json.dumps(pair, sort_keys=True, separators=(',', ':')) + '\n')
    remove_owned_warehouse(candidate)
    print(json.dumps(pair, sort_keys=True, separators=(',', ':')))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
