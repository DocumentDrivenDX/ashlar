"""Bounded partition-local maintenance of only the verified owned full20M copy."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from bucket_apply_queries import BUCKET_SQL
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_zorder_r172'
assert not (O/'statements.jsonl').exists(),'Inspect prior write handles/commits; no restart'
s=json.loads((B/'out/native/ashlar_bucket_full_reads_r171/summary.json').read_text());T=s['table'];assert s['version']==4
c=BoundedReads(O);deadline=time.monotonic()+600
state={'table':T,'id':s['id'],'before_version':4,'version':4,'groups':[],'state':'Owned ZORDER in progress; unpublished'}
def save():(O/'checkpoint.json').write_text(json.dumps(state,indent=2)+'\n')
def sql(label,q):
 assert time.monotonic()<deadline,'Admission deadline; inspect existing owned state'
 return c.sql(label,q)
def detail(label):
 rows=sql(label,'DESCRIBE DETAIL '+T);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];d=dict(zip(names,rows[0]));assert d['id']==s['id'];return d
def costs():
 c.cursor.close();c.cursor=c.connection.cursor()
 for attempt in range(12):
  h={q['query_id']:q for q in c.history()}
  if all(r['statement_id'] in h and h[r['statement_id']]['is_final'] for r in c.records):break
  if attempt<11:time.sleep(5)
 assert all(h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in c.records),'Inspect same IDs'
 state['costs']={k:sum(h[r['statement_id']]['metrics'].get(k,0) for r in c.records) for k in ('read_bytes','write_remote_bytes','spill_to_disk_bytes')};save()
 assert state['costs']['read_bytes']<=45000000000 and state['costs']['write_remote_bytes']<=36000000000,'Stop admission: measured maintenance budget exceeded'
 return h
try:
 sql('timeout','SET STATEMENT_TIMEOUT=180');state['before_detail']=detail('before-detail');assert int(state['before_detail']['sizeInBytes'])<=36000000000
 assert json.loads(state['before_detail']['properties'])['delta.targetFileSize']=='67108864'
 assert int(sql('before-version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0])==4;save()
 for i in range(4):
  costs();before=state['version'];a,z=16*i,16*(i+1)
  output=sql('zorder-'+str(i),f'OPTIMIZE {T} WHERE lookup_bucket>={a} AND lookup_bucket<{z} ZORDER BY (lookup_hash)');qid=c.records[-1]['statement_id']
  v=int(sql('version-'+str(i),'DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]);assert v in (before,before+1)
  state['groups'].append({'range':[a,z],'before_version':before,'version':v,'query_id':qid,'output':output});state['version']=v;state['detail']=detail('detail-'+str(i));save();print('Completed group',i,'version',v,flush=True)
 v=state['version'];assert sql('identities',f'SELECT count(*),count(DISTINCT id) FROM {T} VERSION AS OF {v}')==[['20000000','20000000']]
 assert sql('bucket-integrity',f"SELECT count(*) FROM {T} VERSION AS OF {v} WHERE lookup_hash IS NULL OR NOT(lookup_hash RLIKE '^[0-9a-f]{{64}}$') OR lookup_bucket IS NULL OR lookup_bucket<>({BUCKET_SQL})")==[['0']]
 rows=sql('history','DESCRIBE HISTORY '+T);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];state['delta_history']=[dict(zip(names,r)) for r in rows]
 pins=sql('publication',"SELECT table_versions_json FROM client_dev.ashlar_entropy_20261006_r86.publication_manifest_r89 WHERE publication_id='r139-b1'");state['publication_vector']=json.loads(pins[0][0]);assert state['publication_vector']['client_dev.ashlar_entropy_20261006_r86.edge_current']==23
 h=costs();state['maintenance_metrics']={r['label']:{'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in c.records if r['label'].startswith('zorder-')}
 state['state']='Four owned partition-local ZORDER commands finished;20M global IDs/bucket invariant pass; inspect actual rewrites';state['qualification']='Only owned copy mutated. Input4 has full20M20-field exact parity; this phase does not automatically prove full-wide post-rewrite parity. No update, singleton, cold, producer authority, publication rate or billion admission.64 is an experiment count;64MiB is best-effort target, actual files recorded. Canonical r139 vector unchanged.';save();print(json.dumps(state['costs']),flush=True)
except Exception as e:state['state']='Stopped; inspect same native write IDs and owned commit lineage';state['error']=str(e);save();raise
finally:c.close()
