#!/usr/bin/env python3
"""Read-only verifier for retained R20 publication and stopped-clone receipts."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess

HERE = Path(__file__).resolve().parent
ROOT = Path('/private/tmp/ashlar-r20-truss051-registrar-catalog-20261101')
SUPERVISOR_ROOT = Path(str(ROOT) + '-supervisor')
R19_SOURCE = Path('/private/tmp/ashlar-r19-truss051-delta-publication-20261101/r16b-stopped-source-backup-pgdata')
AUDIT_CLONE = ROOT / 'audit-readonly-pgdata'
PAGE_SHA = 'de5c6d50f03082e5650126db86b4b50871a70f3a74f183a06a1f645a31e51760'
CHECKPOINT_SHA = '802f677f84076a2deaa1da682c836550a38654e2e1b67494b9cd2acf01299725'
CONSUMER = 'ashlar-local-dev-typed-graph-consumer-r6-20261031'
FEED = 'ashlar-local-dev-typed-graph-feed-r19-20261101'
DOCUMENT = 'ashlar-local-dev-typed-graph-r6-20261031'
DOCUMENT_SHA = '2c9b295a8232cae4770255650bf634941ab223e582b79ae2bb78f6acf0b8a880'


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def read_json(path: Path):
    return json.loads(path.read_text())


def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'duplicate JSON key: {key}')
        result[key] = value
    return result


def tree_snapshot(root: Path):
    digest = hashlib.sha256(); total = count = 0
    for base, dirs, files in os.walk(root, followlinks=False):
        dirs.sort(); files.sort()
        for name in dirs:
            path = Path(base) / name
            if path.is_symlink() or not path.is_dir():
                raise ValueError(f'non-directory or symlink in PGDATA: {path}')
            digest.update(b'D\0' + path.relative_to(root).as_posix().encode() + b'\0')
        for name in files:
            path = Path(base) / name
            if path.is_symlink() or not path.is_file():
                raise ValueError(f'special file or symlink in PGDATA: {path}')
            raw = path.read_bytes(); total += len(raw); count += 1
            digest.update(b'F\0' + path.relative_to(root).as_posix().encode() + b'\0'
                          + str(len(raw)).encode() + b'\0' + hashlib.sha256(raw).digest())
    return {'bytes': total, 'tree_sha256': digest.hexdigest(), 'files': count}


def process_rows():
    raw = subprocess.run(['/bin/ps', '-Ao', 'pid=,ppid=,pgid=,command='],
                         text=True, capture_output=True, check=True, timeout=10).stdout
    rows = []
    for line in raw.splitlines():
        fields = line.strip().split(maxsplit=3)
        if len(fields) != 4:
            if line.strip():
                raise RuntimeError(f'unparseable process inventory row: {line!r}')
            continue
        rows.append({'pid': int(fields[0]), 'ppid': int(fields[1]),
                     'pgid': int(fields[2]), 'command': fields[3]})
    return rows


def active_data_bag(table_path: Path, expected_uuid: str, expected_version: int):
    logs = sorted((table_path / '_delta_log').glob('[0-9]' * 20 + '.json'))
    if [int(path.stem) for path in logs] != list(range(expected_version + 1)):
        raise ValueError(f'{table_path.name}: Delta history is not contiguous through vector version')
    active = {}; metadata = None; counts = {}; history = []
    for log in logs:
        raw = log.read_bytes()
        actions = [json.loads(line, object_pairs_hook=unique_pairs) for line in raw.decode().splitlines()]
        for action in actions:
            kind, body = next(iter(action.items()))
            counts[kind] = counts.get(kind, 0) + 1
            if kind == 'metaData': metadata = body
            elif kind == 'add': active[body['path']] = body
            elif kind == 'remove': active.pop(body['path'], None)
        history.append({'version': int(log.stem), 'sha256': sha(raw)})
    if metadata is None or metadata.get('id') != expected_uuid:
        raise ValueError(f'{table_path.name}: Delta table identity differs from vector')
    physical = sorted(path.name for path in table_path.glob('*.parquet'))
    active_paths = sorted(active)
    if active_paths != physical:
        raise ValueError(f'{table_path.name}: active Delta file set differs from local Parquet data bag')
    if active_paths:
        raise ValueError(f'{table_path.name}: expected empty data bag for the verified schema-only source page')
    return {'table': f'local.trussgraph.{table_path.name}', 'uuid': expected_uuid,
            'head_version': expected_version, 'head_json_sha256': history[-1]['sha256'],
            'log_history': history, 'action_counts': counts,
            'active_files': active_paths, 'physical_parquet_files': physical,
            'data_bag': 'empty'}


def main():
    evidence = ROOT / 'receipts'
    scope = read_json(HERE / 'r20-publication-scope.json')
    supervisor_path = SUPERVISOR_ROOT / 'supervisor-receipt.json'
    supervisor = read_json(supervisor_path)
    completed_path = evidence / 'completed.json'
    completed = read_json(completed_path)
    native_path = evidence / 'native-delta-publication-readback.json'
    native = read_json(native_path)
    ack_path = evidence / 'source-ack-deferred-proof.json'
    ack = read_json(ack_path)
    page_receipt = read_json(evidence / 'original-unacknowledged-page-reread.json')
    accepted = read_json(evidence / 'accepted-document-public-reread.json')
    catalog = read_json(evidence / 'registrar-catalog-public-reread.json')
    copied = read_json(evidence / 'source-copy-custody.json')
    prior_reconciliation = read_json(HERE / 'r20-readonly-reconciliation.json')
    source_public_state = prior_reconciliation.get('source_public_state')
    if source_public_state is None:
        source_public_state = prior_reconciliation.get('source', {}).get('fresh_stopped_clone_public_page_query')

    sidecar_path = ROOT / 'delta' / f"deferred-ack-{completed['request_digest']}.json"
    sidecar_raw = sidecar_path.read_bytes()
    sidecar = json.loads(sidecar_raw, object_pairs_hook=unique_pairs)
    request_raw = base64.b64decode(sidecar['request_bytes_base64'], validate=True)
    manifest_raw = base64.b64decode(sidecar['manifest_bytes_base64'], validate=True)
    request = json.loads(request_raw, object_pairs_hook=unique_pairs)
    manifest = json.loads(manifest_raw, object_pairs_hook=unique_pairs)
    from ashlar.attempt_store import verify_request_digest
    request_digest = verify_request_digest(request)
    if (sha(request_raw) != sidecar['request_sha256'] or sha(manifest_raw) != sidecar['manifest_sha256']
            or request_digest != sidecar['request_digest'] or completed['request_digest'] != request_digest
            or sidecar['publication_id'] != 'truss-graph-' + request_digest
            or manifest != sidecar['manifest'] or manifest['publication_id'] != sidecar['publication_id']
            or completed['publication_id'] != sidecar['publication_id']
            or completed.get('publication_committed') is not True or completed.get('acknowledged') is not False
            or completed.get('source_acknowledgement') != 'deferred_not_attempted'):
        raise ValueError('committed request, manifest, publication ID, or deferred-ACK receipt mismatch')
    result_path = ROOT / 'delta' / 'result.json'
    result_raw = result_path.read_bytes()
    result = json.loads(result_raw, object_pairs_hook=unique_pairs)
    if len(result['publications']) != 1 or result['publications'][0]['manifest'] != manifest:
        raise ValueError('Delta result manifest differs from the retained committed manifest')
    vector = sidecar['publication_vector']
    targets = vector['targets']
    versions = json.loads(manifest['table_versions_json'], object_pairs_hook=unique_pairs)
    if set(targets) != {item['table'] for item in native['tables']} or versions != {k: v[1] for k, v in targets.items()}:
        raise ValueError('native Delta inventory or manifest vector differs from committed vector')
    source_progress = json.loads(manifest['source_progress_json'], object_pairs_hook=unique_pairs)[FEED]
    position = json.loads(base64.b64decode(source_progress['position'], validate=True), object_pairs_hook=unique_pairs)
    if (source_progress['batch_id'] != '754' or source_progress['payload_digest'] != PAGE_SHA
            or position.get('nextXid') != '755' or position.get('consumerId') != CONSUMER
            or position.get('registrationId') != page_receipt['registration_id']):
        raise ValueError('manifest vector does not bind the verified R16b feed page')
    page_archive = Path(result['publications'][0]['source_page']['path'])
    page_archive_raw = page_archive.read_bytes()
    page_record = json.loads(page_archive_raw, object_pairs_hook=unique_pairs)
    page_bytes = base64.b64decode(page_record['content'], validate=True)
    if (sha(page_archive_raw) != result['publications'][0]['source_page']['sha256']
            or sidecar['source_page']['archive_sha256'] != sha(page_archive_raw)
            or sha(page_bytes) != PAGE_SHA or sidecar['source_page']['page_sha256'] != PAGE_SHA
            or page_record.get('consumer_id') != CONSUMER):
        raise ValueError('retained Truss source-page bytes differ from the publication vector')
    page_content = json.loads(page_bytes, object_pairs_hook=unique_pairs)
    if page_content.get('changes') != [] or page_content.get('sources') != []:
        raise ValueError('retained page is not the admitted schema-only page')

    accepted_expected = scope['source']['catalog_accepted']
    source_key = f"{accepted_expected['catalog_revision']}:{DOCUMENT}"
    if (accepted['document_id'] != DOCUMENT or accepted['document_revision'] != accepted_expected['document_revision']
            or accepted['registration_id'] != accepted_expected['registration_id']
            or accepted['catalog_revision'] != accepted_expected['catalog_revision']
            or accepted['document_sha256'] != DOCUMENT_SHA
            or catalog['catalog_authority_role'] != scope['publication']['catalog_authority_role']
            or catalog['truss_version'] != '0.0.51'
            or catalog['reader_page_sha256'] != PAGE_SHA or catalog['page_batch_sha256'] != PAGE_SHA
            or catalog['reader_registration_id'] != page_receipt['registration_id']
            or catalog['accepted_sources'] != {source_key: DOCUMENT_SHA}
            or len(catalog.get('reports', {})) != 1 or len(catalog.get('views', {})) != 1
            or catalog['transaction_access_mode'] != 'read_write_effect_free_rollback'
            or catalog.get('acknowledgement') is not False):
        raise ValueError('public registrar/catalog evidence does not bind the exact accepted source and feed page')
    if (page_receipt['page_sha256'] != PAGE_SHA or page_receipt['prior_sha256'] != CHECKPOINT_SHA
            or ack['page_sha256'] != PAGE_SHA or ack['checkpoint_sha256'] != CHECKPOINT_SHA
            or ack['ack_rows'] != 0 or ack.get('acknowledgement') is not False
            or ack.get('public_page_unchanged') is not True):
        raise ValueError('page receipt or fresh stopped-clone read-only ACK/checkpoint receipt mismatch')
    if (not isinstance(source_public_state, dict) or source_public_state.get('ack_rows') != 0
            or source_public_state.get('page_sha256') != PAGE_SHA
            or source_public_state.get('checkpoint_sha256') != CHECKPOINT_SHA
            or source_public_state.get('acknowledgement') is not False):
        raise ValueError('independent fresh-clone public/read-only source query did not bind zero ACK rows and the exact checkpoint')

    tables = []
    for table, (table_uuid, version) in sorted(targets.items()):
        readback = next(item for item in native['tables'] if item['table'] == table)
        if readback['uuid'] != table_uuid or readback['version'] != version or readback['rows'] != 0:
            raise ValueError(f'{table}: native Spark readback differs from the committed vector')
        tables.append(active_data_bag(ROOT / 'delta' / table.rsplit('.', 1)[-1], table_uuid, version))

    source_snapshot = tree_snapshot(R19_SOURCE)
    cold_snapshot = tree_snapshot(Path(scope['paths']['cold_backup_pgdata']))
    migration_clone_snapshot = tree_snapshot(Path(scope['paths']['migration_clone_pgdata']))
    audit_clone_snapshot = tree_snapshot(AUDIT_CLONE)
    expected_source = {'bytes': scope['r16b_source']['bytes'], 'tree_sha256': scope['r16b_source']['tree_sha256'],
                       'files': scope['r16b_source']['files']}
    if source_snapshot != expected_source or cold_snapshot != expected_source:
        raise ValueError('R16b source or R20 cold copy no longer matches exact source custody')
    process_rows_all = process_rows()
    needles = (str(Path(scope['paths']['migration_clone_pgdata'])), str(Path(scope['paths']['cold_backup_pgdata'])), str(AUDIT_CLONE))
    owned = [row for row in process_rows_all if any(needle in row['command'] for needle in needles)]
    if (owned or supervisor.get('process_custody_status') != 'no_owned_processes_remain'
            or supervisor.get('process_group_termination', {}).get('status') != 'terminated'
            or supervisor.get('process_group_termination', {}).get('remaining_pids') != []):
        raise ValueError(f'owned R20 PG/Spark process remains or supervisor cleanup incomplete: {owned}')

    receipt_hashes = {name: sha((evidence / name).read_bytes()) for name in (
        'accepted-document-public-reread.json', 'original-unacknowledged-page-reread.json',
        'registrar-catalog-acl-preflight.json', 'registrar-catalog-public-reread.json',
        'source-ack-deferred-proof.json', 'source-copy-custody.json',
        'spark-delta-runtime.json', 'native-delta-publication-readback.json', 'completed.json')}
    receipt = {
        'schema': 'ashlar.r20-readonly-publication-reconciliation/1',
        'status': 'passed_independent_readonly_reconciliation',
        'verifier_sha256': sha(Path(__file__).read_bytes()),
        'publication': {'publication_id': sidecar['publication_id'], 'request_digest': request_digest,
                        'request_sha256': sha(request_raw), 'manifest_sha256': sha(manifest_raw),
                        'result_sha256': sha(result_raw), 'deferred_ack_intent_sha256': sha(sidecar_raw),
                        'publication_committed': True, 'acknowledged': False,
                        'source_acknowledgement': 'deferred_not_attempted', 'publication_vector': vector},
        'source': {'document_id': DOCUMENT, 'document_sha256': DOCUMENT_SHA, 'accepted_public_api': True,
                   'registrar_catalog_public_api_receipt': True, 'feed_page_sha256': PAGE_SHA,
                   'prior_checkpoint_sha256': CHECKPOINT_SHA, 'next_checkpoint_sha256': page_receipt['next_sha256'],
                   'fresh_stopped_clone_public_page_query': source_public_state,
                   'fresh_stopped_clone_checkpoint_sha256': ack['checkpoint_sha256'],
                   'fresh_stopped_clone_ack_rows': ack['ack_rows'], 'page_unchanged': ack['public_page_unchanged']},
        'tables': tables,
        'source_custody': {'r19_retained_source': source_snapshot, 'r20_cold_backup': cold_snapshot,
                           'r20_migration_clone_after_worker_stop': migration_clone_snapshot,
                           'r20_audit_clone_after_readonly_query_and_shutdown': audit_clone_snapshot,
                           'audit_query_transaction_mode': 'READ ONLY', 'database_dml': False},
        'supervisor': {'status': supervisor['status'], 'failure': supervisor.get('failure'),
                       'receipt_sha256': sha((SUPERVISOR_ROOT / 'supervisor-receipt.json').read_bytes()),
                       'database_effects_may_remain': supervisor['database_effects_may_remain'],
                       'process_custody_status': supervisor['process_custody_status'],
                       'process_group_termination': supervisor['process_group_termination'],
                       'worker_completed_receipt_sha256': sha(completed_path.read_bytes())},
        'process_inventory_after_all_runs': {'r20_owned_processes': owned, 'sampled_process_count': len(process_rows_all)},
        'receipt_hashes': receipt_hashes,
    }
    output = HERE / 'r20-readonly-reconciliation.json'
    output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
    print(json.dumps({'status': receipt['status'], 'publication_id': sidecar['publication_id'],
                      'request_digest': request_digest, 'supervisor_status': supervisor['status'],
                      'supervisor_failure': supervisor.get('failure'), 'ack_rows': ack['ack_rows'],
                      'tables': [{'table': t['table'], 'head_version': t['head_version'],
                                  'head_json_sha256': t['head_json_sha256'], 'active_files': t['active_files']}
                                 for t in tables], 'receipt': str(output)}, sort_keys=True))


if __name__ == '__main__':
    main()
