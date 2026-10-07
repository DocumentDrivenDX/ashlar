"""Final exact-query metrics for the read-only r98 batch predicate experiment."""
import json
from pathlib import Path
from persistent_sql import Client

B = Path(__file__).resolve().parent
O = B/'out/native/ashlar_raw_batch_filter_20261007_r98'
s = json.loads((O/'summary.json').read_text())
records = [json.loads(line) for line in (O/'statements.jsonl').read_text().splitlines()]
c = Client(O)
c.records = records
h = {q['query_id']:q for q in c.history()}
assert all(h.get(r['statement_id'],{}).get('is_final') for r in records), 'Refresh metrics, never re-execute'
assert all(r['response']['status']['state']=='SUCCEEDED' for r in records)
assert not any(h[r['statement_id']]['metrics'].get('result_from_cache') for r in records)
for i, pair in enumerate(s['pairs']):
    for name in ('unfiltered','filtered'):
        r = next(r for r in records if r['label']==f'{name}-{i}')
        pair[name+'_metrics'] = h[r['statement_id']]['metrics']
s['state'] = 'audited raw batch predicate; all exact query metrics final and uncached'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print(json.dumps([{k:v for k,v in pair.items() if not k.endswith('_metrics')} for pair in s['pairs']],indent=2))
