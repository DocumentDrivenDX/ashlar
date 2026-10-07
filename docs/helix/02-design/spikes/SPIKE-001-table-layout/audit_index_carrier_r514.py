"""Offline native receipt audit; no additional warehouse work."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent
D=B/'out/native/index_carrier_spike_r513'
s=json.loads((D/'summary.json').read_text())
h={q['query_id']:q for q in json.loads((D/'query-history.json').read_text())}
r=list(map(json.loads,(D/'statements.jsonl').read_text().splitlines()))
assert len(r)==len(h)
assert hashlib.sha256((B/'index_carrier_spike_r513.py').read_bytes()).hexdigest()==s['code_sha256']
cost={k:0 for k in s['costs']};failed=[]
for x in r:
 q=h[x['statement_id']]
 assert q['query_text']==x['sql'] and q['is_final']
 state=x['response']['status']['state']
 assert (state=='SUCCEEDED' and q['status']=='FINISHED') or (state=='FAILED' and q['status']=='FAILED')
 if state=='FAILED':failed.append(x['label'])
 for k in cost:cost[k]+=q['metrics'].get(k,0) or 0
assert len(failed)==2 and cost==s['costs']
expected={'carrier-cdf':[['5609','0']],'index-cdf':[['12454','0']],'full-logical-parity':[['2498028','0']],'index-counts':[['2498646','2498646','618']],'carrier-counts':[['2504255','2504255']]}
for label,value in expected.items():
 x=next(x for x in r if x['label']==label)
 assert x['response']['result']['data_array']==value
 assert not h[x['statement_id']]['metrics'].get('result_from_cache')
for role in ['index','append']:
 assert s[role+'_metrics']==h[s[role+'_statement_id']]['metrics']
result={'state':'Receipt SQL, terminal outcomes, final costs, code hash and complete parity/CDF results verified','statements':len(r),'negative_controls':failed,'costs':cost,'mutation_read_bytes':s['index_metrics']['read_bytes']+s['append_metrics']['read_bytes'],'mutation_write_bytes':s['index_metrics']['write_remote_bytes']+s['append_metrics']['write_remote_bytes'],'mutation_engine_ms':s['index_metrics']['execution_time_ms']+s['append_metrics']['execution_time_ms'],'mutation_caller_s':s['index_caller_s']+s['append_caller_s'],'qualification':s['qualification']}
(B/'out/index-carrier-audit-r514.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
