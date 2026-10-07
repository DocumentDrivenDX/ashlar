"""Final constant-query floor plus paired historic singleton timing breakdown."""
import json,math
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_client_floor_r143'
c=Client(O);c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()];h={q['query_id']:q for q in c.history()}
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] for r in c.records),'Refresh same IDs only'
def p95(values):return sorted(values)[math.ceil(.95*len(values))-1]
def summarize(records,history):
 assert len(records)==30 and all(not history[r['statement_id']]['metrics'].get('result_from_cache') for r in records)
 result={'n':30,'caller_p95_ms':p95([r['wall_ms'] for r in records])}
 for key in ('compilation_time_ms','execution_time_ms','total_time_ms','result_fetch_time_ms'):
  result[key+'_p95']=p95([history[r['statement_id']]['metrics'].get(key,0) for r in records])
 result['paired_caller_minus_server_p95_ms']=p95([r['wall_ms']-history[r['statement_id']]['metrics']['total_time_ms'] for r in records])
 result['paired_caller_minus_execution_p95_ms']=p95([r['wall_ms']-history[r['statement_id']]['metrics']['execution_time_ms'] for r in records])
 return result
previous=B/'out/native/ashlar_singleton_warm_r142'
records=[json.loads(l) for l in (previous/'statements.jsonl').read_text().splitlines()];history={h['query_id']:h for h in json.loads((previous/'query-history.json').read_text())}
s={'state':'Thirty exact SELECT1 controls audited final uncached; historic singleton timing breakdown derived','constant':summarize([r for r in c.records if r['label'].startswith('constant-')],h),'singleton_r142':summarize([r for r in records if r['label'].startswith('warm-read-')],history),'qualification':'Each residual is computed per query before its p95; do not add/subtract independently ranked p95s. Caller-minus-server includes transport/client and metric-boundary differences, not exclusively network. Caller-minus-execution includes compilation and other work; it is diagnostic, not a prediction of a zero-execution query. SELECT1 has a different plan/result size from exact20-field Delta reads. No statement preparation or scalar control proves singleton, concurrency or billion admission.'}
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps(s,indent=2))
