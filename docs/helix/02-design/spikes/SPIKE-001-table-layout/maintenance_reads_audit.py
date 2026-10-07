"""Audit exact final maintenance/read metrics without repeating operations."""
import json,math
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent
O=B/'out/native/ashlar_maintenance_reads_20261007_r102'
s=json.loads((O/'summary.json').read_text())
assert s['state']=='completed maintenance and exact paired reads'
rs=[json.loads(v) for v in (O/'statements.jsonl').read_text().splitlines()]
c=Client(O);c.records=rs
h={v['query_id']:v for v in c.history()}
assert all(h.get(r['statement_id'],{}).get('is_final') for r in rs), 'Refresh history only'
assert all(r['response']['status']['state']=='SUCCEEDED' for r in rs)
assert not any(h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs)
assert sum(r['label']=='routine-maintenance' for r in rs)==1
def p95(v): return sorted(v)[math.ceil(.95*len(v))-1]
groups={}
for prefix in ('before','pair-0-new','pair-0-published','pair-1-published','pair-1-new'):
    rows=[r for r in rs if r['label'].startswith(prefix+'-') and r['label'][len(prefix)+1:].isdigit()]
    assert len(rows)==30
    ms=[h[r['statement_id']]['metrics'] for r in rows]
    groups[prefix]={'n':30,'caller_p95_ms':p95([r['wall_ms'] for r in rows]),
        'engine_p95_ms':p95([v['execution_time_ms'] for v in ms]),
        'file_reads_p95':p95([v.get('read_files_count',0) for v in ms]),
        'remote_read_queries':sum(v.get('read_remote_bytes',0)>0 for v in ms)}
r=next(r for r in rs if r['label']=='maintenance-history')
names=[v['name'] for v in r['response']['manifest']['schema']['columns']]
history=[dict(zip(names,row)) for row in r['response']['result']['data_array']]
recent=[v for v in history if int(v['version'])>s['old_version']]
assert {int(v['version']) for v in recent}==set(range(s['old_version']+1,s['new_version']+1))
assert all(v['operation']=='OPTIMIZE' for v in recent)
maintenance=next(r for r in rs if r['label']=='routine-maintenance')
assert all(v['queryHistoryStatementId']==maintenance['statement_id'] for v in recent)
s['physical_metrics']=[json.loads(v['operationMetrics']) for v in recent]
s.update(state='audited maintenance and exact paired reads; all queries final and uncached',groups=groups,commits=recent,
    maintenance_caller_ms=next(r['wall_ms'] for r in rs if r['label']=='routine-maintenance'),
    maintenance_metrics=h[next(r['statement_id'] for r in rs if r['label']=='routine-maintenance')]['metrics'])
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print(json.dumps({'versions':[s['old_version'],s['new_version']],'maintenance_caller_ms':s['maintenance_caller_ms'],'groups':groups},indent=2))
