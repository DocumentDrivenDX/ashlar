"""Exact final metrics and commit audit for r100 statistics backfill."""
import json
from pathlib import Path
from persistent_sql import Client

B=Path(__file__).resolve().parent
O=B/'out/native/ashlar_journal_batch_statistics_20261007_r100'
s=json.loads((O/'summary.json').read_text())
assert s['state']=='completed journal batch statistics; exact full-row parity passed; final metrics pending'
records=[json.loads(line) for line in (O/'statements.jsonl').read_text().splitlines()]
c=Client(O);c.records=records
h={q['query_id']:q for q in c.history()}
assert all(h.get(r['statement_id'],{}).get('is_final') for r in records), 'Refresh history only'
assert all(r['response']['status']['state']=='SUCCEEDED' for r in records)
assert not any(h[r['statement_id']]['metrics'].get('result_from_cache') for r in records)
assert len([r for r in records if r['label']=='add-batch-statistic'])==1
assert len([r for r in records if r['label']=='recompute-delta-statistics'])==1
assert {v['operation'] for v in s['commits']}=={'COMPUTE STATS','SET TBLPROPERTIES'}
for key in ('numFiles','sizeInBytes','clusteringColumns','minReaderVersion','minWriterVersion','tableFeatures'):
    assert s['before_detail'][key]==s['after_detail'][key], 'Unexpected physical/protocol change'
s['phases']=[{'label':r['label'],'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in records]
s['state']='audited journal batch statistics; all exact metrics final and uncached; full-row parity passed'
s['pairs']=[{name:next(p for p in s['phases'] if p['label']==f'paired-{i}-{name}') for name in ('old','new')} for i in range(2)]
s['publication_scope']='No manifest installed: existing publications retain their pinned journal versions. Old/new comparison binds distinct metadata/statistics snapshots with equal full journal contents.'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print(json.dumps({'versions':[s['before_version'],s['after_version']], 'operations':[v['operation'] for v in s['commits']],
    'pairs':[{name:{'caller_ms':p[name]['caller_ms'],**{k:p[name]['metrics'].get(k) for k in ('execution_time_ms','read_bytes','read_files_count','pruned_files_count','read_remote_bytes')}} for name in ('old','new')} for p in s['pairs']]},indent=2))
