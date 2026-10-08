"""Run the fixed, local UMF/string-record source example without external I/O.

Fixture mappings are explicit development authority, not accepted Truss IDs.
No publication, native singleton guarantee or source acknowledgement is issued.
"""
import json
import argparse
import hashlib
import subprocess
import tempfile
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from ashlar.apply import empty_state, plan_apply
from ashlar.catalog import Identity, MappingEntry
from ashlar.schema import SchemaIntake, _json
from ashlar.semantic_policy import StringRecordPolicy
from ashlar.source import jsonl_batches
from ashlar.staging import batch_row, batch_from_row
from ashlar.whole_entity import changes_from_batch

PIN = '16c35e8d943769ccfa7bb57d16785aa7159abe65'
RECORD_CHECK_PIN = 'c45c72a2a8a3c4fba61c40c5927dd9091acf8cc3'


def fixture_inputs():
    example = ROOT / 'examples/end-to-end'
    intake = SchemaIntake.read((example / 'schema-v3.intake.json').read_bytes(),
                               '3', trusted_validator_revision=PIN)
    owner = ('truss-weft-source-review-fixture', 'fixture', 'item')
    entries = [MappingEntry(Identity('type', owner), 17, True)]
    entries += [MappingEntry(Identity('property', owner + ('fixture', field)), ident, True)
                for field, ident in [('label', 23), ('caption', 24)]]
    policy = StringRecordPolicy.from_intake(
        intake, (example / 'schema-v3.interpretation.json').read_bytes(),
        entries, source_system='local-example')
    with (example / 'local-string-source.jsonl').open('rb') as source:
        batches = tuple(jsonl_batches(source, feed='local-jsonl', epoch='example-1'))
    return intake, policy, batches


def check_original_records(umf_source, intake, batches, output=None):
    records = []
    for batch in batches:
        changes_from_batch(batch)  # complete original transaction/event admission
        for record in batch.records:
            event = _json(record.raw)
            if event['operation'] == 'delete':
                continue  # deletion is a source operation, not a Record value
            if event['entity_kind'] != 'object' or event['type_id'] != '17':
                raise ValueError('Only the explicit example fixture type is mapped')
            props = _json(event['props_json'].encode('utf-8'))
            values = []
            for key, value in props.items():
                if key not in ('23', '24') or not isinstance(value, str):
                    raise ValueError('Explicit fixture string properties required')
                values.append({'field': {'module': 'fixture', 'element': 'label' if key == '23' else 'caption'},
                               'state': 'present', 'value': {'string': value}})
            records.append({'deliveryId': record.delivery_id, 'recordSha256': record.sha256, 'values': values})
    request = json.dumps({'identity': {'module': 'fixture', 'element': 'item'}, 'records': records},
                         ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    with tempfile.TemporaryDirectory(prefix='ashlar-umf-record-') as temporary:
        request_path = Path(temporary) / 'records.json'
        request_path.write_bytes(request)
        process = subprocess.run(['bun', str(ROOT / 'tools/check_umf_record_values.ts'),
                                  str(umf_source), RECORD_CHECK_PIN,
                                  str(ROOT / 'examples/end-to-end/schema-v3.umf.json'), str(request_path)],
                                 check=True, capture_output=True, timeout=30)
    if len(process.stdout) > 4 * 1024 * 1024:
        raise ValueError('Example producer result limit exceeded')
    result = _json(process.stdout)
    if result['producerRevision'] != RECORD_CHECK_PIN or result['sourceSha256'] != intake.source_sha256 or result['requestSha256'] != hashlib.sha256(request).hexdigest():
        raise ValueError('Original upstream result does not match example custody')
    if [(r['deliveryId'], r['recordSha256']) for r in result['records']] != [(r['deliveryId'], r['recordSha256']) for r in records]:
        raise ValueError('Complete original record result inventory required')
    if any(r['result']['validation']['valid'] is not True or r['result']['validation']['complete'] is not True for r in result['records']):
        raise ValueError('Actual complete logical Record results required')
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(process.stdout)
    return result


def run(umf_source=None, umf_check_output=None):
    intake, policy, batches = fixture_inputs()
    upstream = check_original_records(umf_source, intake, batches, umf_check_output) if umf_source is not None else None
    state = empty_state()
    for batch in batches:
        state = plan_apply(state, changes_from_batch(batch), schema_policy=policy)
    original = state
    # Cross the same JSON custody boundary used by native staging; reconstruct
    # from original bytes rather than treating cached Python objects as recovery.
    retained_rows = json.loads(json.dumps([batch_row(batch) for batch in batches]))
    for row in retained_rows:
        batch = batch_from_row(row)
        state = plan_apply(state, changes_from_batch(batch), schema_policy=policy)
    if state != original:
        raise ValueError('Exact replay changed complete graph state')
    live = sorted(state.current.values(), key=lambda value: value.key)
    return {
        'scope': 'local UMF-backed string-record example; fixture IDs only',
        'complete_interpretation': intake.complete_interpretation,
        'batches': len(batches), 'events': len(state.history),
        'objects': len(live), 'tombstones': len(state.tombstones),
        'replay_unchanged': True,
        'rows': [{'id': str(value.key.id), 'version': str(value.version),
                  'props_json': value.props_json, 'retained_json': value.retained_json}
                 for value in live],
        'upstream_record_checks': None if upstream is None else {
            'producer_revision': RECORD_CHECK_PIN, 'records': len(upstream['records']),
            'logical_checks_complete': True, 'original_document_complete': upstream['originalValidation']['complete'],
            'explicit_upgrade': upstream['upgrade']['operation'], 'native_acceptance': False},
        'published': False, 'acknowledged': False,
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--umf-source', type=Path, help='Clean pinned UMF c45c72a2 checkout for actual logical Record checks')
    parser.add_argument('--umf-check-output', type=Path, help='Retain complete original UMF producer result and upgrade receipt')
    args = parser.parse_args()
    if args.umf_check_output is not None and args.umf_source is None:
        parser.error('--umf-check-output requires --umf-source')
    print(json.dumps(run(args.umf_source, args.umf_check_output), indent=2, ensure_ascii=False))
