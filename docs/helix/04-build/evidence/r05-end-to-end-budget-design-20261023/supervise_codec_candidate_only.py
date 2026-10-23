"""Explicitly gated SNAPPY continuation using the immutable -02 ZSTD receipt.

The selected ZSTD case is never rerun. Baseline files are hash-pinned and
read-only; the single candidate writes only beneath the fresh -03 root.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import supervise_codec_pair as pair

BASE = Path('/private/tmp/ashlar-r05-codec-pair-20261023-02/zstd')
ROOT = Path('/private/tmp/ashlar-r05-codec-pair-20261023-03')
BASE_HASHES = {
    'out/progress.jsonl': '1a74ba8ace61f9bc61fd8adf7830dbb175fe4545afa7c4766b9c6455fc847338',
    'runner.log': 'b18b42323f9609389b7f7ba64b8a5154ae4e8eba21550819c11af6cab433d653',
    'supervisor-result.json': '811f42525572f4d4e7f4f6b27b044d533d87d8a6e45d36f47c8c569f244c5bfe',
}
EXPECTED_SCHEMA_SHA256 = 'bf6657de5fd7839e327c10e94b155dc2faa9e302c741cc351fd1bfa97ae59ce1'
EXPECTED_METADATA_SCHEMA_SHA256 = 'eb17bbf49525de56c8faac2f9005541f711119949db30441ccb087c715eede00'
EXPECTED_PROTOCOL = {
    'minReaderVersion': 1, 'minWriterVersion': 7,
    'writerFeatures': ['domainMetadata', 'clustering', 'invariants', 'appendOnly'],
}
EXPECTED_CONFIG_WITHOUT_CODEC = {
    'delta.dataSkippingStatsColumns': 'lookup_hash,source_system,rel_type_id,id',
    'delta.targetFileSize': '67108864',
}


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def verify_baseline_pins() -> tuple[list[dict], dict, list[dict]]:
    for relative, expected in BASE_HASHES.items():
        path = BASE / relative
        if not path.is_file() or file_sha256(path) != expected:
            raise RuntimeError(f'baseline evidence missing or changed: {path}')
    progress = pair.load_progress(BASE)
    receipt = json.loads((BASE / 'supervisor-result.json').read_text())
    if receipt.get('codec') != 'ZSTD' or receipt.get('returncode') != 0 or receipt.get('limit_triggered') is not None:
        raise RuntimeError('pinned ZSTD runner result is not a complete successful case')
    if receipt.get('case_output_bytes', 0) > pair.OUTPUT_CAP or receipt.get('case_diagnostic_bytes', 0) > pair.DIAGNOSTIC_CAP:
        raise RuntimeError('pinned ZSTD case exceeded a retained resource cap')
    cleanup = receipt.get('cleanup') or {}
    permission_refusal = str(cleanup.get('group_probe_error', '')).startswith('PermissionError:')
    if not cleanup.get('leader_reaped') or not receipt.get('remaining_process_group_uncertain') or not permission_refusal:
        raise RuntimeError('pinned ZSTD receipt does not match the reviewed process-probe cleanup refusal')

    admitted = pair.stage(progress, 'admitted')
    preflight = pair.stage(progress, 'pre_spark_environment')
    spark = pair.stage(progress, 'spark_started')
    table = pair.stage(progress, 'carrier_table_ready')
    plan = pair.stage(progress, 'plan_captured')
    insert = pair.stage(progress, 'insert_completed')
    footer = pair.stage(progress, 'footer_verified')
    stopped = pair.stage(progress, 'spark_stopped')
    if admitted.get('codec') != 'ZSTD' or admitted.get('experiment_profile') != 'ashlar-local-current/.4-xxhash64':
        raise RuntimeError('pinned ZSTD admission profile differs')
    if (preflight.get('python'), preflight.get('pyspark'), preflight.get('delta'), preflight.get('master'), preflight.get('driver_memory')) != ('3.12.14', '4.2.0', '4.4.1', 'local[18]', '24g'):
        raise RuntimeError('pinned ZSTD runtime identity differs')
    if spark.get('spark') != '4.2.0' or spark.get('master') != 'local[18]' or spark.get('heap_max_bytes') != 25_769_803_776:
        raise RuntimeError('pinned ZSTD actual Spark runtime differs')
    if table.get('schema_sha256') != EXPECTED_SCHEMA_SHA256 or footer.get('metadata_schema_sha256') != EXPECTED_METADATA_SCHEMA_SHA256:
        raise RuntimeError('pinned ZSTD schema receipt differs')
    if plan.get('insert_sql_sha256') != pair.INSERT_SQL_SHA256:
        raise RuntimeError('pinned ZSTD INSERT digest differs')
    if insert.get('write_seconds') != 25.962255041929893:
        raise RuntimeError('pinned ZSTD measured INSERT timing differs')
    if (footer.get('rows'), footer.get('files'), footer.get('codecs'), footer.get('parquet_bytes')) != (225_000_000, 18, ['ZSTD'], 2_569_031_961):
        raise RuntimeError('pinned ZSTD footer receipt differs')
    if footer.get('protocol') != EXPECTED_PROTOCOL or table.get('table_configuration') != {**EXPECTED_CONFIG_WITHOUT_CODEC, 'delta.parquet.compression.codec': 'zstd'}:
        raise RuntimeError('pinned ZSTD Delta protocol/configuration differs')
    if not stopped or stopped.get('stage') != 'spark_stopped':
        raise RuntimeError('pinned ZSTD Spark stop receipt missing')
    return progress, receipt, footer['persisted_witnesses']


def spark_process_inventory() -> dict:
    result = subprocess.run(['ps', '-axo', 'pid=,pgid=,command='], check=True,
                            capture_output=True, text=True, timeout=10)
    matches = []
    terms = ('org.apache.spark', 'pyspark', 'py4j.java_gateway')
    target_roots = (str(BASE.parent), str(ROOT))
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        fields = line.strip().split(None, 2)
        if len(fields) < 2:
            raise RuntimeError(f'process inventory returned ambiguous row: {line[:200]}')
        try:
            pid, pgid = int(fields[0]), int(fields[1])
        except ValueError as exc:
            raise RuntimeError(f'process inventory returned malformed PID/PGID: {line[:200]}') from exc
        if pid <= 0 or pgid <= 0:
            raise RuntimeError(f'process inventory returned invalid PID/PGID: {line[:200]}')
        command = fields[2] if len(fields) > 2 else ''
        lowered = command.lower()
        if any(term in lowered for term in terms) or any(root in command for root in target_roots):
            matches.append({'pid': pid, 'pgid': pgid, 'command': command})
    return {
        'command': ['ps', '-axo', 'pid=,pgid=,command='],
        'captured_at_utc': datetime.now(timezone.utc).isoformat(),
        'matching_processes': matches,
        'clear': not matches,
    }


def require_clear_inventory(phase: str) -> dict:
    inventory = spark_process_inventory()
    (ROOT / f'process-inventory-{phase}.json').write_text(json.dumps(
        inventory, sort_keys=True, separators=(',', ':')) + '\n')
    if not inventory['clear']:
        raise RuntimeError(f'{phase}: Spark/experiment processes remain; refusing candidate')
    return inventory


def hash_and_remove_baseline_warehouse(progress: list[dict]) -> dict:
    table = pair.stage(progress, 'carrier_table_ready')
    footer = pair.stage(progress, 'footer_verified')
    commit = pair.stage(progress, 'delta_commit_observed')
    table_path = Path(table['table_path']).resolve()
    owned_warehouse = (BASE / 'out' / 'warehouse').resolve()
    expected_table = (owned_warehouse / 'edge_current').resolve()
    if table_path != expected_table or not table_path.is_dir() or not table_path.is_relative_to(owned_warehouse):
        raise RuntimeError('refusing baseline cleanup: table path is not the exact retained owned warehouse')
    log_dir = table_path / '_delta_log'
    names = footer.get('delta_log_files')
    if not isinstance(names, list) or not names or any(Path(name).name != name for name in names):
        raise RuntimeError('refusing baseline cleanup: Delta log file list is missing or ambiguous')
    delta_log_hashes = {}
    for name in names:
        log_path = log_dir / name
        if not log_path.is_file():
            raise RuntimeError(f'refusing baseline cleanup: retained Delta log is missing: {name}')
        delta_log_hashes[name] = file_sha256(log_path)
    commit_hashes = {row['name']: row['sha256'] for row in commit.get('commit_files', [])}
    if not commit_hashes or set(delta_log_hashes) != set(commit_hashes) or any(
            delta_log_hashes.get(name) != digest for name, digest in commit_hashes.items()):
        raise RuntimeError('refusing baseline cleanup: retained Delta log hashes differ from the progress receipt')
    retained = {
        'baseline_root': str(BASE), 'table_path': str(table_path),
        'progress_sha256': BASE_HASHES['out/progress.jsonl'],
        'runner_log_sha256': BASE_HASHES['runner.log'],
        'supervisor_result_sha256': BASE_HASHES['supervisor-result.json'],
        'delta_log_sha256': delta_log_hashes,
        'cleanup_scope': 'exact owned attempt-02 warehouse only; progress, runner log, supervisor receipt, and this Delta-log hash receipt retained',
    }
    receipt_path = ROOT / 'baseline-retention-receipt.json'
    receipt_path.write_text(json.dumps(retained, sort_keys=True, separators=(',', ':')) + '\n')
    retained['retention_receipt_path'] = str(receipt_path)
    retained['retention_receipt_sha256'] = file_sha256(receipt_path)
    pair.remove_owned_warehouse({'case_root': str(BASE), 'table_path': str(table_path)})
    if (BASE / 'out' / 'warehouse').exists():
        raise RuntimeError('owned baseline warehouse remains after requested cleanup')
    return retained


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute-reviewed-candidate', action='store_true',
                        help='explicitly enable the single bounded SNAPPY local case')
    args = parser.parse_args()
    if not args.execute_reviewed_candidate:
        parser.error('inert by default; pass --execute-reviewed-candidate only after Sol review')
    if ROOT.exists():
        raise SystemExit(f'candidate root already exists; refusing reuse: {ROOT}')
    pair.ensure_runner_pin()
    ROOT.mkdir(mode=0o700, parents=True)
    continuation_started = time.monotonic()
    pair.ensure_pair_deadline(continuation_started, 'baseline verification')
    base_progress, base_receipt, base_witnesses = verify_baseline_pins()
    preflight_inventory = require_clear_inventory('before-candidate')
    baseline_retention = hash_and_remove_baseline_warehouse(base_progress)

    pair.ROOT = ROOT
    pair.ensure_pair_deadline(continuation_started, 'SNAPPY-only candidate')
    candidate = pair.run_case('snappy', continuation_started)
    candidate_progress = pair.load_progress(Path(candidate['case_root']))
    candidate_preflight = pair.stage(candidate_progress, 'pre_spark_environment')
    candidate_spark = pair.stage(candidate_progress, 'spark_started')
    if (candidate_preflight.get('python'), candidate_preflight.get('pyspark'),
            candidate_preflight.get('delta'), candidate_preflight.get('master'),
            candidate_preflight.get('driver_memory')) != ('3.12.14', '4.2.0', '4.4.1', 'local[18]', '24g'):
        raise RuntimeError('candidate declared runtime differs from pinned ZSTD baseline')
    if (candidate_spark.get('spark'), candidate_spark.get('master'),
            candidate_spark.get('heap_max_bytes')) != ('4.2.0', 'local[18]', 25_769_803_776):
        raise RuntimeError('candidate observed Spark runtime differs from pinned ZSTD baseline')
    if candidate['insert_sql_sha256'] != pair.INSERT_SQL_SHA256:
        raise RuntimeError('candidate SQL digest differs from pinned attempt-02 source')
    if candidate['experiment_profile'] != 'ashlar-local-current/.4-xxhash64-snappy-candidate-20261023':
        raise RuntimeError('candidate was not labeled as the isolated SNAPPY profile')

    baseline_footer = pair.stage(base_progress, 'footer_verified')
    baseline_table = pair.stage(base_progress, 'carrier_table_ready')
    for key, expected in (
        ('rows', 225_000_000), ('files', 18), ('schema_sha256', EXPECTED_SCHEMA_SHA256),
        ('metadata_schema_sha256', EXPECTED_METADATA_SCHEMA_SHA256), ('protocol', EXPECTED_PROTOCOL),
        ('persisted_witnesses', base_witnesses),
    ):
        observed = candidate.get(key)
        if observed != expected:
            raise RuntimeError(f'candidate differs from pinned ZSTD baseline for {key}')
    if candidate['codecs'] != ['SNAPPY']:
        raise RuntimeError('candidate Parquet footers do not all report SNAPPY')
    if candidate['metadata_configuration'].get('delta.parquet.compression.codec') != 'snappy':
        raise RuntimeError('candidate Delta table codec property did not read back as SNAPPY')
    candidate_config = dict(candidate['metadata_configuration'])
    candidate_config.pop('delta.parquet.compression.codec', None)
    if candidate_config != EXPECTED_CONFIG_WITHOUT_CODEC:
        raise RuntimeError('candidate Delta table configuration differs beyond codec')
    if baseline_footer['persisted_witnesses'] != candidate['persisted_witnesses']:
        raise RuntimeError('candidate exact persisted witnesses differ from pinned baseline')
    pair.ensure_runner_pin()
    pair.ensure_pair_deadline(continuation_started, 'candidate final acceptance')
    verify_baseline_pins()
    post_inventory = require_clear_inventory('after-candidate')

    pair_result = {
        'status': 'valid-one-pair-observation',
        'baseline_attempt': 'attempt-02/ZSTD reused from immutable retained receipts; not rerun',
        'baseline_paths': {relative: str(BASE / relative) for relative in BASE_HASHES},
        'baseline_sha256': BASE_HASHES,
        'baseline_delta_log_sha256': baseline_retention['delta_log_sha256'],
        'baseline_retention_receipt_sha256': baseline_retention['retention_receipt_sha256'],
        'baseline_write_seconds': pair.stage(base_progress, 'insert_completed')['write_seconds'],
        'baseline_process_group_probe': {
            'pgid': (base_receipt.get('cleanup') or {}).get('process_group_id'),
            'permission_denied_on_signal_probe': True,
            'leader_reaped': True,
            'independent_inventory': 'parent task reported successful read-only ps inventory with no Spark/Java child before candidate preparation',
            'baseline_hash_pins_rechecked_after_candidate': True,
        },
        'candidate': candidate,
        'candidate_below_22_72s': candidate['insert_seconds'] < 22.72,
        'preflight_process_inventory': preflight_inventory,
        'post_candidate_process_inventory': post_inventory,
        'elapsed_seconds': round(time.monotonic() - continuation_started, 3),
        'few_minute_end_to_end_target': 'open; no broader pipeline operations measured',
        'release_or_scale_qualification': False,
    }
    (ROOT / 'candidate-result.json').write_text(json.dumps(
        pair_result, sort_keys=True, separators=(',', ':')) + '\n')
    pair.remove_owned_warehouse(candidate)
    print(json.dumps(pair_result, sort_keys=True, separators=(',', ':')))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
