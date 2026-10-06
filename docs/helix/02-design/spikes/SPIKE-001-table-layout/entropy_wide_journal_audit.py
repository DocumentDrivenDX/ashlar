"""Audit exact final r94 query IDs, physical amplification and publication time."""
import json
from pathlib import Path
from persistent_sql import Client

B = Path(__file__).resolve().parent
O = B/'out/native/ashlar_entropy_wide_journal_20261006_r94'
summary = json.loads((O/'summary.json').read_text())
assert summary['state']=='passed wide-journal synthetic publication'
records = [json.loads(line) for line in (O/'statements.jsonl').read_text().splitlines()]
c = Client(O)
c.records = records
history = {q['query_id']:q for q in c.history()}
assert all(history.get(r['statement_id'],{}).get('is_final') for r in records), 'Refresh history; never repeat mutations'
assert not any(r['cancel_requested'] for r in records)
assert not any(history[r['statement_id']]['metrics'].get('result_from_cache') for r in records)

def detail(label):
    r = next(r for r in records if r['label']==label)
    return dict(zip([col['name'] for col in r['response']['manifest']['schema']['columns']],r['response']['result']['data_array'][0]))

prior = json.loads((B/'out/native/ashlar_entropy_publication_20261006_r89/audited-summary.json').read_text())
details = {role:detail('detail-'+role) for role in ('stage','current','raw','journal')}
merge = json.loads(detail('new-current-version')['operationMetrics'])
increments = {'stage':int(details['stage']['sizeInBytes']), 'current_new_files':int(merge['numTargetBytesAdded']),
              'raw_new_files':int(details['raw']['sizeInBytes'])-int(prior['durable_table_details']['source_record']['sizeInBytes']),
              'journal_new_files':int(details['journal']['sizeInBytes'])-int(prior['durable_table_details']['property_journal']['sizeInBytes'])}
assert all(value>=0 for value in increments.values())
result = dict(summary)
result.update(state='completed wide-journal publication; all exact statement metrics final and uncached',
              merge_metrics=merge, details=details, physical_increments=increments,
              physical_increment_bytes=sum(increments.values()),
              physical_scope='Stage/new current/raw/journal file bytes; logs, DV sidecars, manifest and older retained files excluded. Existing 2X-Small shared warehouse; no resized/new compute; attributable dollars unqualified.',
              phases=[{'label':r['label'],'caller_ms':r['wall_ms'],'metrics':history[r['statement_id']]['metrics']} for r in records])
qualification_path=O/'timestamp-qualification/summary.json'
oracle_path=B/'out/native/ashlar_wire_timestamp_20261006_r95/summary.json'
if qualification_path.exists() and oracle_path.exists():
    qualified=json.loads(qualification_path.read_text())['timestamp_instant_mismatches_min_max_microseconds'][0]
    oracle=json.loads(oracle_path.read_text())
    result['state']='completed wide-journal publication measurements; legacy wire timestamp loss qualified'
    result['wire_precision_qualification']={
        'r94_rows_with_published_at_loss':int(qualified[0]),'r94_microseconds_lost_per_row':int(qualified[1]),
        'r89_rows_with_published_at_loss':int(oracle['legacy_r89_mismatch_min_max_microseconds'][0][0]),
        'r89_microseconds_lost_per_row':int(oracle['legacy_r89_mismatch_min_max_microseconds'][0][1]),
        'cause':'Default to_json emits milliseconds; same-encoder payload comparison masked loss',
        'unchanged':'Native Delta carriers/journal timestamps and property/retained/cursor string values',
        'corrected_encoder':'wire_json.py synthetic-full-carrier/2 explicit UTC microseconds; independent 100k and timestamp literal roundtrips pass',
        'old_raw_payloads':'Retained without rewriting; no full all-field reconstruction claim'}
(O/'audited-summary.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'arrival_to_verified_manifest_seconds':summary['arrival_to_verified_manifest_seconds'],
                  'merge_metrics':merge,'increments':increments},indent=2))
