import json,sys,time
from pathlib import Path
from databricks.sdk.errors import NotFound
b=Path(__file__).resolve().parent;sys.path.insert(0,str(b));from persistent_sql import Client
p=b/'out/native/ashlar_range32_build_r309';a=json.loads((p/'summary.json').read_text());assert a['state']=='Stopped; inspect original native handles without replay';c=Client(p/'inspection');c.records=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];r=next(r for r in c.records if r['label']=='create');status=c.w.api_client.do('GET','/api/2.0/sql/statements/'+r['statement_id']);assert status['status']['state']=='CANCELED'
try:meta=c.w.api_client.do('GET','/api/2.1/unity-catalog/tables/'+a['table']);exists=True
except NotFound:meta=None;exists=False
inspection=[]
if exists:
 for label,sql in [('detail','DESCRIBE DETAIL '+a['table']),('history','DESCRIBE HISTORY '+a['table']+' LIMIT 100')]:
  try:inspection.append({'label':label,'rows':c.sql('inspect-'+label,sql)})
  except Exception as e:inspection.append({'label':label,'error':str(e)})
for n in range(15):
 h=c.history();q=next((q for q in h if q['query_id']==r['statement_id']),None)
 if q and q.get('is_final') and q['status']=='CANCELED':break
 if n==14:raise RuntimeError('Native final history pending; same handle only')
 time.sleep(2)
assert len(h)==len(c.records) and all(q.get('is_final') for q in h)
costs={k:sum(q['metrics'].get(k,0) for q in h) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};result={'state':'Same native CREATE terminal canceled; target state inspected','statement_id':r['statement_id'],'statement_state':status['status']['state'],'native_queries':h,'target_exists':exists,'target_metadata':meta,'inspection':inspection,'costs':costs,'qualification':'Canceled native output counters are work attempted, not committed table bytes or physical orphan inventory. No CREATE retry, OPTIMIZE, producerACK, pointer publication, compute resize or cleanup. Candidate unqualified.'};(p/'cancel-audit.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'state':result['state'],'target_exists':exists,'costs':costs},indent=2))
