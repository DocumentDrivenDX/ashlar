"""Qualify r183 telemetry: missing fields are unknown; inspect native overlap."""
import json
from pathlib import Path
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_lc_concurrent_r183'
s=json.loads((O/'summary.json').read_text());hist=[];records=[]
for i in range(2):
 D=O/('client'+str(i));hist.append({x['query_id']:x for x in json.loads((D/'query-history.json').read_text())});records.append({x['label']:x for x in map(json.loads,(D/'statements.jsonl').read_text().splitlines())})
for kind in ['point','floor']:
 values=[hist[i][records[i][kind+'-'+str(k)]['statement_id']] for i in range(2) for k in range(10)]
 assert all(x['is_final'] and x['status']=='FINISHED' for x in values)
 s['reads'][kind]['waiting_at_capacity_duration_ms_p95']=None
 s['reads'][kind]['queue_metric_qualification']='Capacity duration is absent from all native histories; original summary defaulted missing values to zero. Audit replaces that unsupported zero with unknown.'
 assert all('waiting_at_capacity_duration_ms' not in x['metrics'] for x in values)
 overlap=[]
 for k in range(10):
  pair=[hist[i][records[i][kind+'-'+str(k)]['statement_id']] for i in range(2)]
  overlap.append(max(0,min(x['execution_end_time_ms'] for x in pair)-max(x['query_start_time_ms'] for x in pair)))
 s['reads'][kind]['native_statement_overlap_ms']=overlap
 s['reads'][kind]['overlapping_pairs']=sum(x>0 for x in overlap)
 s['reads'][kind]['overlap_qualification']='Native query-start to execution-end windows overlap; this does not prove simultaneous task execution or warehouse saturation.'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps(s['reads'],indent=2))
