"""Offline exact native statements/results/plan/cost audit of range pilot."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent;O=B/'out/native/target_join_probe_r565';a=json.loads((O/'summary.json').read_text());assert a['state']=='Four complete range predecessor comparisons pass exact full20field equality'
assert hashlib.sha256((B/'target_join_probe_r565.py').read_bytes()).hexdigest()==a['code_sha256']
P=B/'out/native/ashlar_sixth_guard_publish_r537/audited-summary.json';pub=json.loads(P.read_text());assert hashlib.sha256(P.read_bytes()).hexdigest()==a['source_sha256'] and a['edge_pin']==pub['base']['edge_current'] and a['source_pin']==pub['inputs']['source_record']
r=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];h={x['query_id']:x for x in json.loads((O/'shared-history.json').read_text())['queries']};assert len(r)==len(h)==len({x['statement_id'] for x in r})==9
for x in r:
 q=h[x['statement_id']];assert q['status']=='FINISHED' and q['is_final'] and x['response']['status']['state']=='SUCCEEDED' and q['query_text']=='/* ashlar '+O.name+' '+x['label']+' */ '+x['sql']
assert a['source_bound']==next(x for x in r if x['label']=='source-bound')['response']['result']['data_array'][0]
assert a['queries']['broadcast'].replace('/*+ BROADCAST(s) */','')==a['queries']['default']
for label,v in a['plans'].items():
 rec=next(x for x in r if x['label']=='plan-'+label);assert rec['sql']=='EXPLAIN FORMATTED '+a['queries'][label] and rec['statement_id']==v['statement_id'];text='\n'.join(str(x[0]) for x in rec['response']['result']['data_array'])+'\n';assert text==(O/('plan-'+label+'.txt')).read_text() and hashlib.sha256(text.encode()).hexdigest()==v['sha256']
for i,x in enumerate(a['runs']):
 rec=next(y for y in r if y['statement_id']==x['statement_id']);assert rec['sql']==a['queries'][x['family']] and rec['label']=='read-'+str(i)+'-'+x['family'];assert rec['wall_ms']==x['caller_ms'] and rec['response']['result']['data_array']==x['result']==[[a['source_bound'][0],'0']];assert x['metrics']==h[x['statement_id']]['metrics']
cost={k:sum(x['metrics'].get(k,0) or 0 for x in h.values()) for k in a['costs']};assert cost==a['costs'] and all(v<=a['bounds'][k] for k,v in cost.items()) and a['wall_s']<=a['bounds']['wall_s']
result={'state':'Nine exact native statements and four complete target join results verified','source_bound':a['source_bound'],'runs':a['runs'],'costs':cost,'wall_s':a['wall_s'],'qualification':a['qualification']};(B/'out/target-join-audit-r566.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'state':result['state'],'source_bound':a['source_bound'],'costs':cost,'wall_s':a['wall_s']}))
