"""Final exact-history metrics for r91; no repeated maintenance submission."""
import json
from pathlib import Path
from persistent_sql import Client

O = Path(__file__).resolve().parent / 'out/native/ashlar_entropy_maintenance_20261006_r91'
summary = json.loads((O / 'summary.json').read_text())
assert summary['state'] == 'completed routine maintenance'
records = [json.loads(line) for line in (O / 'statements.jsonl').read_text().splitlines()]
c = Client(O)
c.records = records
history = {q['query_id']: q for q in c.history()}
assert all(history.get(r['statement_id'], {}).get('is_final') for r in records), 'Refresh history; do not repeat SQL'
assert not any(r['cancel_requested'] for r in records)

def detail(label):
    record = next(r for r in records if r['label'] == label)
    cols = [col['name'] for col in record['response']['manifest']['schema']['columns']]
    return dict(zip(cols, record['response']['result']['data_array'][0]))

result = dict(summary)
result.update(state='completed routine maintenance; final metrics audited',
              before=detail('before-detail'), after=detail('after-detail'),
              operation_metrics=json.loads(detail('after-history')['operationMetrics']),
              phases=[{'label': r['label'], 'caller_ms': r['wall_ms'], 'metrics': history[r['statement_id']]['metrics']} for r in records],
              storage_scope='Active files only; removed files retained for version1, Delta logs and sidecars excluded. Shared compute dollars unqualified.')
(O / 'audited-summary.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({'versions': [summary['old_version'], summary['new_version']], 'metrics': result['operation_metrics']}, indent=2))
