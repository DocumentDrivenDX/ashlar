"""Refresh exact final statement metrics and summarize r89 storage/timing."""
import json
from pathlib import Path
from persistent_sql import Client

B = Path(__file__).resolve().parent
O = B / 'out/native/ashlar_entropy_publication_20261006_r89'
report = json.loads((O / 'summary.json').read_text())
assert report['state'] == 'passed serialized synthetic publication slice'
records = [json.loads(line) for line in (O / 'statements.jsonl').read_text().splitlines()]
c = Client(O)
c.records = records
history = {q['query_id']: q for q in c.history()}
assert all(history.get(r['statement_id'], {}).get('is_final') for r in records), 'Refresh history, not SQL mutations'
assert not any(r['cancel_requested'] for r in records)

def detail(label):
    r = next(r for r in records if r['label'] == label)
    cols = [col['name'] for col in r['response']['manifest']['schema']['columns']]
    return dict(zip(cols, r['response']['result']['data_array'][0]))

result = {'state': 'completed; all exact statement metrics final',
          'changed_edges': report['changed_edges'], 'baseline_edges': report['baseline_edges'],
          'arrival_to_manifest_seconds': report['arrival_to_manifest_seconds'],
          'versions': report['versions'], 'validation': report['validation'],
          'phases': [{'label': r['label'], 'caller_ms': r['wall_ms'],
                      'metrics': history[r['statement_id']]['metrics']} for r in records],
          'edge_merge_metrics': json.loads(detail('output-edge-version')['operationMetrics']),
          'durable_table_details': {name: detail('detail-' + name) for name in ('source_record', 'property_journal', 'tombstone')},
          'edge_after': detail('edge-detail-after-publication'),
          'scope': report['scope'],
          'cost_scope': 'Existing 2X-Small single-cluster warehouse, no resize. Active table bytes exclude stage, retained old files/logs/DV sidecars and shared warehouse dollars.'}
inventory = Client(O / 'inventory')
inventory_rows = {}
for role, table in {'stage': report['stage'], 'manifest': report['tables']['publication_manifest']}.items():
    rows = inventory.sql('detail-' + role, 'DESCRIBE DETAIL ' + table)
    names = [col['name'] for col in inventory.records[-1]['response']['manifest']['schema']['columns']]
    inventory_rows[role] = dict(zip(names, rows[0]))
inventory.history()
result['supplemental_table_details'] = inventory_rows
result['active_incremental_bytes'] = int(result['edge_merge_metrics']['numTargetBytesAdded']) + sum(
    int(value['sizeInBytes']) for value in list(result['durable_table_details'].values()) + list(inventory_rows.values()))
result['active_incremental_bytes_scope'] = 'new current files + raw + journal + tombstone + immutable stage + manifest; excludes old retained files, logs, sidecars and later compaction'
(O / 'audited-summary.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({k: result[k] for k in ('state', 'arrival_to_manifest_seconds', 'edge_merge_metrics')}, indent=2))
