"""Refresh exact query IDs for r97; never repeat validation or mutations."""
import json
import argparse
from pathlib import Path
from persistent_sql import Client

B = Path(__file__).resolve().parent
O = B/'out/native/ashlar_raw_validation_20261007_r97'
args = argparse.ArgumentParser()
args.add_argument('--driver', action='store_true')
driver = args.parse_args().driver
if driver: O = O/'persistent-uncached'
s = json.loads((O/'summary.json').read_text())
records = [json.loads(line) for line in (O/'statements.jsonl').read_text().splitlines()]
c = Client(O)
c.records = records
h = {q['query_id']:q for q in c.history()}
assert all(h.get(r['statement_id'], {}).get('is_final') for r in records), 'Metrics lag; refresh history only'
assert all(r['response']['status']['state']=='SUCCEEDED' and not r.get('cancel_requested',False) for r in records)
cached = [r['label'] for r in records if h[r['statement_id']]['metrics'].get('result_from_cache')]
if driver: assert not cached, 'Cached result; no uncached performance claim'
for i, pair in enumerate(s['pairs']):
    def metric(label):
        r = next(r for r in records if r['label']==label)
        return h[r['statement_id']]['metrics']
    a, b, merged = metric(f'parity-{i}'), metric(f'membership-{i}'), metric(f'combined-{i}')
    pair.update(separate_execution_ms=a['execution_time_ms']+b['execution_time_ms'],
                combined_execution_ms=merged['execution_time_ms'],
                separate_read_bytes=a.get('read_bytes',0)+b.get('read_bytes',0),
                combined_read_bytes=merged.get('read_bytes'),
                separate_metrics=[a,b], combined_metrics=merged,
                uncached_pair=not any(m.get('result_from_cache') for m in (a,b,merged)))
s.update(state='audited paired combined raw validation; cached timings excluded' if cached else 'audited paired combined raw validation; exact queries final and uncached',
         cached_labels_excluded=cached,
         phases=[{'label':r['label'],'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in records])
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print(json.dumps(s['pairs'],indent=2))
