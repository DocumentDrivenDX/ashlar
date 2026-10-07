"""Exact native EXPLAIN audit; historical mutation metrics, no inferred plan."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent;O=B/'out/native/current_merge_plans_r559';a=json.loads((O/'summary.json').read_text())
assert a['state']=='Two nonexecuting current MERGE plans retained'
assert hashlib.sha256((B/'current_merge_plans_r559.py').read_bytes()).hexdigest()==a['code_sha256']
P=B/'out/native/ashlar_sixth_guard_publish_r537/audited-summary.json';pub=json.loads(P.read_text());assert hashlib.sha256(P.read_bytes()).hexdigest()==a['sources']['sixth_publication']
r=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];h={x['query_id']:x for x in json.loads((O/'shared-history.json').read_text())['queries']};assert len(r)==len(h)==4
for x in r:
 q=h[x['statement_id']];assert q['status']=='FINISHED' and q['is_final'] and x['response']['status']['state']=='SUCCEEDED'
 assert q['query_text']=='/* ashlar '+O.name+' '+x['label']+' */ '+x['sql']
for label,p in a['plans'].items():
 rec=next(x for x in r if x['label']==label);assert rec['sql']=='EXPLAIN FORMATTED '+p['query'] and rec['statement_id']==p['statement_id']
 text='\n'.join(str(x[0]) for x in rec['response']['result']['data_array'])+'\n'
 assert text==(O/(label+'.txt')).read_text() and hashlib.sha256(text.encode()).hexdigest()==p['sha256']
 assert 'MergeIntoCommandEdge' in text and 'Delta[version=9,' in text
cost={k:sum(x['metrics'].get(k,0) or 0 for x in h.values()) for k in a['costs']};assert cost==a['costs'] and all(v<=a['bounds'][k] for k,v in cost.items()) and a['wall_s']<=a['bounds']['wall_s']
H=B/'out/native/ashlar_sixth_guard_publish_r537/shared-history.json';old=json.loads(H.read_text());sid=next(x['statement_id'] for x in pub['commit_events'] if x['role']=='edge_current');q=next(x for x in old['queries'] if x['query_id']==sid);assert q['is_final'] and q['status']=='FINISHED'
result={'state':'Four exact native EXPLAIN statements verified; MERGE internals unavailable','costs':cost,'wall_s':a['wall_s'],'historical_merge':{'statement_id':sid,'metrics':q['metrics'],'history_sha256':hashlib.sha256(H.read_bytes()).hexdigest()},'qualification':a['qualification'],'interpretation':'Command-only plans do not expose executed adaptive scans/joins. Read files and pruning counters aggregate query work; cannot infer distinct target coverage or attribute metadata_time_ms exclusively to catalog overhead. Nested timings are not additive. Simple guard omission remains inadmissible.'}
(B/'out/current-merge-plan-audit-r560.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'state':result['state'],'costs':cost,'wall_s':a['wall_s']}))
