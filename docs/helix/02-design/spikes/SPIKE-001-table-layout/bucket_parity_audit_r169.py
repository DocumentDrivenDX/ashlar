"""Audit complete20M exact field proof and controls against the same native IDs."""
import json,time
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_parity_r169'
s=json.loads((O/'summary.json').read_text());assert s['state'].startswith('Full20M20-field exact copy parity passed')
assert len(s['ranges'])==4 and sum(int(x['rows']) for x in s['ranges'])==20000000 and all(x['mismatches']==0 for x in s['ranges'])
records=[];h={}
for out in [B/('out/native/ashlar_bucket_parity_'+run) for run in ('r167','r168','r169')]+[B/'out/native/ashlar_bucket_parity_controls_r168']:
 c=Client(out);c.records=[json.loads(x) for x in (out/'statements.jsonl').read_text().splitlines()];records.extend(c.records)
 for attempt in range(12):
  local={q['query_id']:q for q in c.history()}
  if all(r['statement_id'] in local and local[r['statement_id']]['is_final'] for r in c.records):break
  if attempt<11:time.sleep(5)
 assert all(local[r['statement_id']]['is_final'] and local[r['statement_id']]['status']=='FINISHED' and r['response']['status']['state']=='SUCCEEDED' for r in c.records),'Inspect same IDs'
 h.update(local)
exacts=[r for r in records if r['label'].startswith('exact-') and r['label']!='exact-controls'];assert len(exacts)==4
assert {r['statement_id'] for r in exacts}=={x['query_id'] for x in s['ranges']}
assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in exacts)
s['audited_costs_total']={k:sum(h[r['statement_id']]['metrics'].get(k,0) for r in records) for k in ('read_bytes','write_remote_bytes','spill_to_disk_bytes')};assert s['audited_costs_total']['read_bytes']<=100000000000 and s['audited_costs_total']['write_remote_bytes']==0
s['controls']=json.loads((B/'out/native/ashlar_bucket_parity_controls_r168/summary.json').read_text())
s['closed_cursor_observation']={'r167_final_result_fetch_ms':s['range_metrics']['exact-0']['metrics']['result_fetch_time_ms'],'r168_final_result_fetch_ms':s['range_metrics']['exact-1']['metrics']['result_fetch_time_ms'],'r169_final_result_fetch_ms':[s['range_metrics'][x]['metrics']['result_fetch_time_ms'] for x in ('exact-2','exact-3')],'qualification':'Retained-result-set lifecycle is supported by installed source and observed closure timing; sequential runs are not randomized causal isolation. These telemetry values are not caller result-fetch latency; exact driver caller timings are separate. No singleton SLO improvement inferred.'}
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps({'state':s['state'],'costs':s['audited_costs_total'],'controls':s['controls']['state']},indent=2))
