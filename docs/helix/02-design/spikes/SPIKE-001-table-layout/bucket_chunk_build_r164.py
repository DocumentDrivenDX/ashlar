"""Owned four-range full20M copy; checkpoints before further write admission."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS
from bucket_apply_queries import BUCKET_SQL
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_build_r164'
assert not (O/'statements.jsonl').exists(),'Inspect existing handles and commits; no restart'
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
 assert state['costs']['read_bytes']<=45000000000 and state['costs']['write_remote_bytes']<=40000000000,'Stop admitting writes: cost bound crossed'
try:
 sql('timeout','SET STATEMENT_TIMEOUT=180');assert sql('absence',"SHOW TABLES IN client_dev.ashlar_entropy_20261006_r86 LIKE 'bucket_part_r164'")==[]
 props="'delta.enableRowTracking'='true','delta.enableDeletionVectors'='true','delta.parquet.compression.codec'='zstd','delta.targetFileSize'='67108864','delta.dataSkippingStatsColumns'='lookup_hash,source_system,rel_type_id,id,entity_version,apply_batch_id'"
 columns=','.join(COLS)
 sql('create',f'CREATE TABLE {T} USING DELTA PARTITIONED BY (lookup_bucket) TBLPROPERTIES ({props}) AS SELECT {columns},{BUCKET_SQL} lookup_bucket FROM {E} VERSION AS OF 23 WHERE false')
 state['detail']=detail();state['id']=state['detail']['id'];state['version']=int(sql('version-initial','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]);assert state['version']==0;save()
 for i,((a,z),expected) in enumerate(zip(probe['ranges'],probe['counts'])):
  final_history();before=state['version']
  sql('append-'+str(i),f"INSERT INTO {T} ({columns},lookup_bucket) SELECT {columns},{BUCKET_SQL} FROM {E} VERSION AS OF 23 WHERE lookup_hash>='{a}' AND lookup_hash<'{z}'")
  v=int(sql('version-'+str(i),'DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]);assert v==before+1
  rows=sql('count-'+str(i),f"SELECT count(*) FROM {T} VERSION AS OF {v} WHERE lookup_hash>='{a}' AND lookup_hash<'{z}'");assert rows==expected
  state['version']=v;state['chunks'].append({'range':[a,z],'expected_count':expected[0],'version':v,'append_query_id':next(r['statement_id'] for r in c.records if r['label']=='append-'+str(i))});state['detail']=detail();assert state['detail']['id']==state['id'];save();print('Committed chunk',i,'rows',expected[0],flush=True)
 assert sql('identities',f'SELECT count(*),count(DISTINCT id) FROM {T} VERSION AS OF {state["version"]}')==[['20000000','20000000']]
 assert sql('bucket-integrity',f"SELECT count(*) FROM {T} VERSION AS OF {state['version']} WHERE lookup_hash IS NULL OR NOT(lookup_hash RLIKE '^[0-9a-f]{{64}}$') OR lookup_bucket IS NULL OR lookup_bucket<>({BUCKET_SQL})")==[['0']]
 state['state']='Full20M disjoint copy committed; global IDs and bucket invariant pass; full-carrier parity pending';final_history();save();print(json.dumps(state['costs']),flush=True)
except Exception as e:
 state['state']='Stopped; inspect same native IDs and owned commit lineage before continuation';state['error']=str(e);save();raise
finally:c.close()
