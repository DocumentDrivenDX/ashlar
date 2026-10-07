"""Offline complete private larger-file fixture native receipt audit."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent;O=B/'out/native/file256_recover_r571';OLD=B/'out/native/file256_pilot_r569';a=json.loads((O/'summary.json').read_text());assert a['state']=='Completed private256MiB maintenance recovered with full21field parity and exact schema/typed uniqueness'
assert hashlib.sha256((B/'file256_pilot_r569.py').read_bytes()).hexdigest()==a['code_sha256']
P=B/'out/native/scaled_fingerprint_r493/summary.json';p=json.loads(P.read_text());assert hashlib.sha256(P.read_bytes()).hexdigest()==a['source_sha256'] and a['source']=={'table':p['table'],'version':p['copy_version'],'id':p['table_id']}
r=[dict(json.loads(x),run=d.name) for d in [OLD,O] for x in (d/'statements.jsonl').read_text().splitlines()];h={x['query_id']:x for x in json.loads((O/'combined-history.json').read_text())['queries']};assert len(r)==len(h)==len({x['statement_id'] for x in r})
by={x['label']:x for x in r};assert len(by)==len(r)
for x in r:
 q=h[x['statement_id']];assert q['status']=='FINISHED' and q['is_final'] and x['response']['status']['state']=='SUCCEEDED' and q['query_text']==x['sql']
assert by['clone']['statement_id']==a['clone_statement_id'] and by['optimize']['statement_id']==a['optimize_statement_id']
assert by['clone']['sql']==f"CREATE TABLE {a['table']} SHALLOW CLONE {a['source']['table']} VERSION AS OF {a['source']['version']} TBLPROPERTIES ('delta.targetFileSize'='268435456')" and by['optimize']['sql']=='OPTIMIZE '+a['table']+' FULL'
assert hashlib.sha256((OLD/'summary.json').read_bytes()).hexdigest()==a['stopped_source_sha256']
assert hashlib.sha256((B/'file256_recover_r571.py').read_bytes()).hexdigest()==a['recovery_code_sha256']
for label,key in [('source-detail','source_current_detail'),('clone-history','clone_history'),('before-detail','before'),('before-schema','schema_before'),('optimize','optimize_result'),('after-history','history'),('after-detail','after'),('after-schema','schema_after'),('closing-history','closing_history')]:
 x=by[label];cols=[v['name'] for v in x['response']['manifest']['schema']['columns']];rows=[dict(zip(cols,v)) for v in x['response']['result']['data_array']];assert rows==([a[key]] if isinstance(a[key],dict) else a[key])
assert a['parity']==by['all21-multiset-parity']['response']['result']['data_array']==[['0']] and a['counts']==by['typed-unique-count']['response']['result']['data_array']==[p['fixture_integrity'][0][:2]]
assert a['schema_before']==a['schema_after'] and a['before']['id']==a['after']['id']==a['id'] and a['history']==a['closing_history']
new=[x for x in a['history'] if int(x['version'])>0];assert sorted(int(x['version']) for x in new)==list(range(1,a['after_version']+1)) and all(x['queryHistoryStatementId']==a['optimize_statement_id'] for x in new)
for d in [a['before'],a['after']]:assert json.loads(d['properties'])['delta.targetFileSize']=='268435456' and json.loads(d['clusteringColumns'])==['lookup_hash']
cost={k:sum(x['metrics'].get(k,0) or 0 for x in h.values()) for k in a['costs']};assert cost==a['costs'] and all(v<=a['bounds'][k] for k,v in cost.items()) and a['original_stopped_wall_s']<=a['bounds']['wall_s'] and a['recovery_s']<=180;assert a['optimize_metrics']==h[a['optimize_statement_id']]['metrics']
result={'state':'Complete larger-file fixture native history, full21field parity, schema and costs verified','statements':len(r),'table':a['table'],'id':a['id'],'source':a['source'],'after_version':a['after_version'],'geometry':{k:{f:a[k][f] for f in ['numFiles','sizeInBytes']} for k in ['before','after']},'counts':a['counts'],'optimize_metrics':a['optimize_metrics'],'costs':cost,'original_stopped_wall_s':a['original_stopped_wall_s'],'recovery_s':a['recovery_s'],'whole_wall_s':None,'qualification':a['qualification']};(B/'out/file256-recovery-audit-r572.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
