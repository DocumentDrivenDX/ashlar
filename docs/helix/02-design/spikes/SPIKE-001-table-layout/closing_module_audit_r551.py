"""Offline exact closing metadata/profile/schema and cohort-cost audit."""
import hashlib,json
from pathlib import Path
from publisher_custody_r462 import verify
B=Path(__file__).resolve().parent;p=B/'out/native/closing_metadata_native_r549';s=json.loads((p/'summary.json').read_text());assert s['state']=='Extracted ten-table closing module preserve exact UUID/profile/schema and actual commit IDs'
source=B/'out/native/ashlar_sixth_guard_publish_r537/audited-summary.json';pub=json.loads(source.read_text());assert hashlib.sha256(source.read_bytes()).hexdigest()==s['source_sha256'];elig=B/pub['sources']['eligibility'];assert hashlib.sha256(elig.read_bytes()).hexdigest()==s['eligibility_sha256']
for n,v in s['code_sha256'].items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==v
r=[dict(json.loads(line),worker=path.parent.name) for path in p.glob('closing_r549_worker*/statements.jsonl') for line in path.read_text().splitlines()];h={q['query_id']:q for q in json.loads((p/'shared-history.json').read_text())['queries']};assert len(r)==len(h)==len({x['statement_id'] for x in r})==38
for x in r:
 q=h[x['statement_id']];assert q['is_final'] and q['status']=='FINISHED' and x['response']['status']['state']=='SUCCEEDED' and q['query_text']=='/* ashlar '+x['worker']+' '+x['label']+' */ '+x['sql']
by={x['label']:x for x in r if x['label'] not in ['timeout','cache']};assert len(by)==30
results=[]
for c in s['cohorts']:
 phase=c['phase'];want={phase+'-'+e['key']:{**e,'key':phase+'-'+e['key']} for e in s['expected']};assert len(c['accepted'])==len(want)==10
 ids=[]
 for x in c['accepted']:
  e=want[x['key']];assert verify(x,e)==x and x['head']['queryHistoryStatementId']==e['head']['queryHistoryStatementId']
  for label,field in [('detail','detail'),('head','head'),('schema','schema')]:
   rec=by[label+'-'+x['key']];cols=[z['name'] for z in rec['response']['manifest']['schema']['columns']];value=[dict(zip(cols,row)) for row in rec['response']['result']['data_array']];assert value==(x[field] if field=='schema' else [x[field]]);ids.append(rec['statement_id'])
 assert set(ids)==set(c['statement_ids']) and len(ids)==30
 assert c['engine_sum_ms']==sum(h[sid]['metrics'].get('execution_time_ms',0) for sid in ids) and abs(c['caller_sum_ms']-sum(x['wall_ms'] for x in r if x['statement_id'] in ids))<.000001
 results.append({k:c[k] for k in ['phase','collection_s','engine_sum_ms','caller_sum_ms']})
cost={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in s['costs']};assert cost==s['costs'] and all(v<=s['bounds'][k] for k,v in cost.items()) and s['wall_s']<=180
result={'state':'All38 exact native statements/final metrics and complete extracted closing custody cohort verified','cohorts':results,'costs':cost,'wall_s':s['wall_s'],'qualification':s['qualification']};(B/'out/closing-module-audit-r551.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
