"""Refresh final native query metrics by saved IDs without replaying writes."""
import json,math
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_merge_pruning_r146'
s=json.loads((O/'summary.json').read_text());c=Client(O)
c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()]
h={q['query_id']:q for q in c.history()}
for mode in ('control','candidate'):
 lane=Client(O/(mode+'-read-lane'));lane.records=[json.loads(l) for l in (lane.out/'statements.jsonl').read_text().splitlines()]
 assert json.loads((lane.out/'process-bound.json').read_text())['exit_code']==0
 c.records+=lane.records;h.update({q['query_id']:q for q in lane.history()})
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] for r in c.records),'Refresh same IDs only'
measurements={}
for r in c.records:
 if r['label'].endswith(('-merge','-analyze','-intended','-output','-identities','-untouched')):
  m=h[r['statement_id']]['metrics'];assert not m.get('result_from_cache')
  measurements[r['label']]={'caller_ms':r['wall_ms'],'engine_ms':m['execution_time_ms'],'files':m.get('read_files_count'),'bytes':m.get('read_bytes'),'remote_bytes':m.get('read_remote_bytes'),'spill_bytes':m.get('spill_to_disk_bytes'),'metrics':m}
reads={}
for mode in ('control','candidate'):
 rs=[r for r in c.records if r['label'].startswith(mode+'-read-') and r['label']!=mode+'-read-oracle'];assert len(rs)==30
 assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs)
 def p95(values):return sorted(values)[math.ceil(.95*len(values))-1]
 reads[mode]={'caller_p95_ms':p95([r['wall_ms'] for r in rs]),'engine_p95_ms':p95([h[r['statement_id']]['metrics']['execution_time_ms'] for r in rs]),'files_p95':p95([h[r['statement_id']]['metrics']['read_files_count'] for r in rs]),'remote_queries':sum(h[r['statement_id']]['metrics']['read_remote_bytes']>0 for r in rs)}
s['reads']=reads
s.update(state='Both actual MERGEs and preservation checks audited; all measured queries final uncached',measurements=measurements)
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print(json.dumps({'reads':reads},indent=2));print(json.dumps({k:{a:v for a,v in m.items() if a!='metrics'} for k,m in measurements.items()},indent=2))
