"""Exhaustive final20M bucket values against E23 plus the immutable100k update."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS,TEXTS
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_final_parity_r178'
assert not (O/'statements.jsonl').exists(),'Inspect prior native IDs; no restart'
s=json.loads((B/'out/native/ashlar_bucket_update_r176/audited-summary.json').read_text());E=s['source'];U=s['stage'];x=s['owned']['part'];T=x['table'];assert x['version']==9
probe=json.loads((B/'out/native/ashlar_bucket_chunk_r163/audited-summary.json').read_text());c=BoundedReads(O);deadline=time.monotonic()+600
state={'table':T,'id':x['id'],'version':9,'source':E,'source_version':23,'stage':U,'stage_id':s['stage_id'],'stage_version':0,'ranges':[],'state':'Exhaustive final20M parity in progress'}
def save():(O/'summary.json').write_text(json.dumps(state,indent=2)+'\n')
def sql(label,q):
 assert time.monotonic()<deadline,'Controller admission deadline'
 return c.sql(label,q)
def costs():
 c.cursor.close();c.cursor=c.connection.cursor()
 for attempt in range(12):
  h={q['query_id']:q for q in c.history()}
  if all(r['statement_id'] in h and h[r['statement_id']]['is_final'] for r in c.records):break
  if attempt<11:time.sleep(5)
 assert all(h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in c.records),'Inspect same native IDs'
 state['costs']={k:sum(h[r['statement_id']]['metrics'].get(k,0) for r in c.records) for k in ('read_bytes','write_remote_bytes','spill_to_disk_bytes')};save()
 assert state['costs']['read_bytes']<=140000000000 and state['costs']['write_remote_bytes']==0,'Stop further admission: read-only proof budget exceeded'
 return h
try:
 sql('timeout','SET STATEMENT_TIMEOUT=180')
 for label,t,want_id,v in [('target',T,x['id'],9),('stage',U,s['stage_id'],0)]:
  rows=sql(label+'-detail','DESCRIBE DETAIL '+t);names=[p['name'] for p in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,rows[0]))['id']==want_id
  assert int(sql(label+'-version','DESCRIBE HISTORY '+t+' LIMIT 1')[0][0])==v
 for label,t,v,n in [('source',E,23,20000000),('target',T,9,20000000),('stage',U,0,100000)]:assert sql(label+'-identities',f'SELECT count(*),count(DISTINCT id) FROM {t} VERSION AS OF {v}')==[[str(n),str(n)]]
 keys=f'SELECT /*+ BROADCAST(k) */ b.id FROM {E} VERSION AS OF 23 b LEFT ANTI JOIN (SELECT id FROM {U} VERSION AS OF 0) k ON b.id=k.id'
 assert sql('expected-identities',f'SELECT count(*),count(DISTINCT id) FROM (({keys}) UNION ALL (SELECT id FROM {U} VERSION AS OF 0))')==[['20000000','20000000']]
 comparisons=[f"NOT(encode(e.{col},'UTF-8') <=> encode(a.{col},'UTF-8'))" if col in TEXTS else f'NOT(e.{col} <=> a.{col})' for col in COLS]
 for i,((a,z),expected) in enumerate(zip(probe['ranges'],probe['counts'])):
  costs()
  old=f"SELECT /*+ BROADCAST(k) */ {','.join('b.'+col for col in COLS)} FROM {E} VERSION AS OF 23 b LEFT ANTI JOIN (SELECT id FROM {U} VERSION AS OF 0) k ON b.id=k.id WHERE b.lookup_hash>='{a}' AND b.lookup_hash<'{z}'"
  new=f"SELECT {','.join(COLS)} FROM {U} VERSION AS OF 0 WHERE lookup_hash>='{a}' AND lookup_hash<'{z}'"
  actual=f"SELECT {','.join(COLS)} FROM {T} VERSION AS OF 9 WHERE lookup_hash>='{a}' AND lookup_hash<'{z}'"
  q=f"SELECT count(*),count_if({' OR '.join(comparisons)}) FROM (({old}) UNION ALL ({new})) e INNER JOIN ({actual}) a ON e.id=a.id"
  rows=sql('exact-'+str(i),q);assert rows==[[expected[0],'0']],rows
  state['ranges'].append({'range':[a,z],'rows':expected[0],'mismatches':0,'query_id':c.records[-1]['statement_id']});save();print('Exact final carriers passed range',i,expected[0],flush=True)
 assert sum(int(r['rows']) for r in state['ranges'])==20000000
 assert sql('final-version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]=='9'
 pins=sql('publication',"SELECT table_versions_json FROM client_dev.ashlar_entropy_20261006_r86.publication_manifest_r89 WHERE publication_id='r139-b1'");state['publication_vector']=json.loads(pins[0][0]);assert state['publication_vector'][E]==23
 h=costs();state['range_metrics']={r['label']:{'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in c.records if r['label'].startswith('exact-')};assert all(not v['metrics'].get('result_from_cache') for v in state['range_metrics'].values())
 state['state']='Full20M20-field final bucket9 equality to E23+stage0 passed';state['qualification']='Exact expected20M globally unique IDs and target20M globally unique IDs plus joined counts in four complete disjoint hash ranges establish exhaustive membership. All20 fields compared;11 text columns UTF8 binary/null-safe, numeric/timestamp columns native/null-safe. This covers prior ZORDER and mixed update copied rows, not only100k changed values. Fixture global IDs justify id join; all full native identity components separately compared. No source-authority/fencing, publisher freshness, singleton/cold or1B/5B admission; canonical r139 unchanged.';save();print(json.dumps(state['costs']),flush=True)
except Exception as e:state['state']='Stopped; inspect same native IDs; remaining ranges unproved';state['error']=str(e);save();raise
finally:c.close()
