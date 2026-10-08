"""Audit all remaining covered block branches from saved exact native responses."""
import json,hashlib
from pathlib import Path
from append_edge_blocks_r681 import coverage_query,block_query
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_append_other_blocks_r682'
s=json.loads((O/'summary.json').read_text());records=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];history=json.loads((O/'shared-history.json').read_text());h={q['query_id']:q for q in history['queries']}
assert len(records)==len(h)==14 and len({r['statement_id'] for r in records})==14 and not history['missing_ids']
assert not (O/'live-statement.json').exists()
for r in records:
 q=h[r['statement_id']];assert q['query_text']==r['sql'] and q['is_final'] and q['status']=='FINISHED' and r['response']['status']['state']=='SUCCEEDED'
oracle_path=B/'out/append-edge-oracle-r656.json';oracle=json.loads(oracle_path.read_text());metrics={}
for role,t in s['roles'].items():
 pin=t['pin'];fields=oracle['chunks'][0]['roles'][role]['fields']
 r=next(r for r in records if r['label']=='coverage-'+role)
 assert r['sql']==coverage_query(pin['table'],pin['version'],role,48000000)
 assert r['response']['result']['data_array']==t['coverage']==[['8000000','0']]
 metrics[role]=[]
 for i,low in enumerate([40000000,44000000]):
  r=next(r for r in records if r['label']==role+'-'+str(low))
  assert r['sql']==block_query(pin['table'],pin['version'],role,fields,low,low+4000000,48000000)
  expected=[[str(j),str(oracle['chunks'][j]['roles'][role]['rows']),oracle['chunks'][j]['roles'][role]['digest']] for j in range(i*40,(i+1)*40)]
  assert r['response']['result']['data_array']==expected==t['blocks'][i]['groups']
  assert h[r['statement_id']]['metrics']['result_from_cache'] is False
  metrics[role].append({k:h[r['statement_id']]['metrics'].get(k) for k in ['execution_time_ms','read_bytes','read_files_count','pruned_files_count']})
 assert sum(int(r[1]) for block in t['blocks'] for r in block['groups'])==8000000
costs={k:sum(q['metrics'][k] for q in h.values()) for k in s['costs']};assert costs==s['costs']
a={'state':'All14 exact final native statements and240 complete independent digest groups audited','costs':costs,'wall_s':s['wall_s'],'block_metrics':metrics,'sources':{str(p.relative_to(B)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [oracle_path,O/'summary.json',O/'shared-history.json',O/'statements.jsonl']},'qualification':s['qualification']+' All branch fields and complete membership checked; SHA256 collision assumption. No full16M new-edge, wall-rate or sustained-service inference.'}
(O/'audited-summary-r683.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps({'state':a['state'],'block_metrics':metrics},indent=2))
