"""Owned four-range full20M copy; checkpoints before further write admission."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS
from bucket_apply_queries import BUCKET_SQL
columns=','.join(COLS)
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_build_r166'
assert not (O/'statements.jsonl').exists(),'Inspect existing handles and commits; no restart'
from persistent_sql import Client
prior=Client(B/'out/native/ashlar_bucket_build_r164');prior.records=[json.loads(x) for x in (prior.out/'statements.jsonl').read_text().splitlines()];prior.records += [json.loads(x) for x in (B/'out/native/ashlar_bucket_build_r165/statements.jsonl').read_text().splitlines()];ph={q['query_id']:q for q in prior.history()};assert all(ph[x['statement_id']]['is_final'] and ph[x['statement_id']]['status']=='FINISHED' for x in prior.records)
prior_costs={'read_bytes':sum(ph[x['statement_id']]['metrics'].get('read_bytes',0) for x in prior.records),'write_remote_bytes':sum(ph[x['statement_id']]['metrics'].get('write_remote_bytes',0) for x in prior.records)}
probe=json.loads((B/'out/native/ashlar_bucket_chunk_r163/audited-summary.json').read_text());E=probe['source'];T='client_dev.ashlar_entropy_20261006_r86.bucket_part_r164'
c=BoundedReads(O);deadline=time.monotonic()+600
state={'table':T,'source':E,'source_version':23,'chunks':[],'state':'building; excluded from publication'}
def save(): (O/'checkpoint.json').write_text(json.dumps(state,indent=2)+'\n')
def sql(label,s):
 assert time.monotonic()<deadline,'Admission deadline reached; inspect owned partial table'
 return c.sql(label,s)
def detail():
 rows=sql('detail-'+str(len(state['chunks'])),'DESCRIBE DETAIL '+T);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return dict(zip(names,rows[0]))
def final_history():
 for attempt in range(3):
  h={q['query_id']:q for q in c.history()}
  if all(r['statement_id'] in h and h[r['statement_id']]['is_final'] for r in c.records):break
  if attempt<2:time.sleep(2)
 assert all(h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in c.records),'Inspect same query IDs'
 state['costs']={'read_bytes':sum(h[r['statement_id']]['metrics'].get('read_bytes',0) for r in c.records),'write_remote_bytes':sum(h[r['statement_id']]['metrics'].get('write_remote_bytes',0) for r in c.records)};save()
 state['combined_costs']={k:state['costs'][k]+prior_costs[k] for k in prior_costs};save()
 assert state['combined_costs']['read_bytes']<=45000000000 and state['combined_costs']['write_remote_bytes']<=40000000000,'Stop admitting writes: cost bound crossed'
try:
 sql('timeout','SET STATEMENT_TIMEOUT=180')
 previous=json.loads((B/'out/native/ashlar_bucket_build_r164/checkpoint.json').read_text());state['id']=previous['id'];state['detail']=detail();assert state['detail']['id']==state['id']
 state['version']=int(sql('resume-version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]);assert state['version']==1
 assert sql('resume-count',f"SELECT count(*) FROM {T} VERSION AS OF 1 WHERE lookup_hash>='0' AND lookup_hash<'4'")==[probe['counts'][0]]
 assert sql('resume-total',f'SELECT count(*) FROM {T} VERSION AS OF 1')==[probe['counts'][0]]
 first=next(x for x in prior.records if x['label']=='append-0');assert first['response']['result']['data_array'][0]==[probe['counts'][0][0],probe['counts'][0][0]]
 state['chunks']=[{'range':probe['ranges'][0],'expected_count':probe['counts'][0][0],'version':1,'append_query_id':first['statement_id'],'recovery':'Native successful append and immutable count inspected; controller nested-row assertion failed after commit; no replay'}];save()
 for i,((a,z),expected) in enumerate(zip(probe['ranges'][1:],probe['counts'][1:]),start=1):
  final_history();before=state['version']
  sql('append-'+str(i),f"INSERT INTO {T} ({columns},lookup_bucket) SELECT {columns},{BUCKET_SQL} FROM {E} VERSION AS OF 23 WHERE lookup_hash>='{a}' AND lookup_hash<'{z}'")
  v=int(sql('version-'+str(i),'DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]);assert v==before+1
  rows=sql('count-'+str(i),f"SELECT count(*) FROM {T} VERSION AS OF {v} WHERE lookup_hash>='{a}' AND lookup_hash<'{z}'");assert rows==[expected]
  state['version']=v;state['chunks'].append({'range':[a,z],'expected_count':expected[0],'version':v,'append_query_id':next(r['statement_id'] for r in c.records if r['label']=='append-'+str(i))});state['detail']=detail();assert state['detail']['id']==state['id'];save();print('Committed chunk',i,'rows',expected[0],flush=True)
 assert sql('identities',f'SELECT count(*),count(DISTINCT id) FROM {T} VERSION AS OF {state["version"]}')==[['20000000','20000000']]
 assert sql('bucket-integrity',f"SELECT count(*) FROM {T} VERSION AS OF {state['version']} WHERE lookup_hash IS NULL OR NOT(lookup_hash RLIKE '^[0-9a-f]{{64}}$') OR lookup_bucket IS NULL OR lookup_bucket<>({BUCKET_SQL})")==[['0']]
 state['state']='Full20M disjoint copy committed; global IDs and bucket invariant pass; full-carrier parity pending';final_history();save();print(json.dumps(state['costs']),flush=True)
except Exception as e:
 state['state']='Stopped; inspect same native IDs and owned commit lineage before continuation';state['error']=str(e);save();raise
finally:c.close()
