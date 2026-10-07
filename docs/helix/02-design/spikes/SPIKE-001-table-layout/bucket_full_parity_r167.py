"""Full20M exact20-field copy parity in four immutable disjoint hash ranges."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS,TEXTS
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_parity_r167'
assert not (O/'statements.jsonl').exists(),'Inspect prior query IDs; no replay'
s=json.loads((B/'out/native/ashlar_bucket_build_r166/audited-summary.json').read_text());E=s['source'];T=s['table'];assert s['version']==4
probe=json.loads((B/'out/native/ashlar_bucket_chunk_r163/audited-summary.json').read_text())
c=BoundedReads(O);deadline=time.monotonic()+600
state={'source':E,'source_version':23,'table':T,'id':s['id'],'version':4,'ranges':[],'state':'Full20M exact parity in progress'}
def save():(O/'summary.json').write_text(json.dumps(state,indent=2)+'\n')
def sql(label,q):
 assert time.monotonic()<deadline,'Controller admission deadline reached'
 return c.sql(label,q)
def final_metrics():
 for attempt in range(3):
  h={q['query_id']:q for q in c.history()}
  if all(r['statement_id'] in h and h[r['statement_id']]['is_final'] for r in c.records):break
  if attempt<2:time.sleep(2)
 assert all(h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in c.records),'Inspect same query IDs'
 state['costs']={k:sum(h[r['statement_id']]['metrics'].get(k,0) for r in c.records) for k in ('read_bytes','write_remote_bytes','spill_to_disk_bytes')};save()
 assert state['costs']['read_bytes']<=100000000000,'Stop admission:100GB validation read budget exceeded'
 assert state['costs']['write_remote_bytes']==0
 return h
try:
 sql('timeout','SET STATEMENT_TIMEOUT=180')
 rows=sql('detail','DESCRIBE DETAIL '+T);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,rows[0]))['id']==s['id']
 assert sql('version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]=='4'
 for name,t,v in [('source',E,23),('target',T,4)]:assert sql(name+'-identities',f'SELECT count(*),count(DISTINCT id) FROM {t} VERSION AS OF {v}')==[['20000000','20000000']]
 # Fixture-wide nonnull unique IDs make the full join one-to-one; every logical
 # identity component is still compared. Binary text equality preserves bytes.
 comparisons=[f"NOT(encode(e.{x},'UTF-8') <=> encode(a.{x},'UTF-8'))" if x in TEXTS else f'NOT(e.{x} <=> a.{x})' for x in COLS]
 columns=','.join(COLS)
 for i,((a,z),expected) in enumerate(zip(probe['ranges'],probe['counts'])):
  final_metrics()
  left=f"SELECT {columns} FROM {E} VERSION AS OF 23 WHERE lookup_hash>='{a}' AND lookup_hash<'{z}'"
  right=f"SELECT {columns} FROM {T} VERSION AS OF 4 WHERE lookup_hash>='{a}' AND lookup_hash<'{z}'"
  q=f"SELECT count(*),count_if(e.id IS NULL OR a.id IS NULL OR {' OR '.join(comparisons)}) FROM ({left}) e FULL OUTER JOIN ({right}) a ON e.id=a.id"
  rows=sql('exact-'+str(i),q);assert rows==[[expected[0],'0']],rows
  state['ranges'].append({'range':[a,z],'rows':expected[0],'mismatches':0,'query_id':c.records[-1]['statement_id']});save();print('Exact full carriers passed range',i,expected[0],flush=True)
 assert sum(int(x['rows']) for x in state['ranges'])==20000000
 assert sql('final-version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]=='4'
 pins=sql('publication',"SELECT table_versions_json FROM client_dev.ashlar_entropy_20261006_r86.publication_manifest_r89 WHERE publication_id='r139-b1'");state['publication_vector']=json.loads(pins[0][0]);assert state['publication_vector'][E]==23
 h=final_metrics();state['range_metrics']={r['label']:{'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in c.records if r['label'].startswith('exact-')}
 state['state']='Full20M20-field exact copy parity passed at E23/owned4'
 state['qualification']='All20 fields compared null-safely; all11 text fields UTF8 binary equality; typed numerical/timestamp values native equality. Four disjoint hash ranges cover20M; both20M global nonnull unique IDs proved. Fixture-wide unique IDs justify the one-to-one join; not generic uniqueness enforcement. Derived bucket invariant independently passed r166. No ZORDER, update/read comparison, generic producer authority, publication-rate, cold or billion admission.';save();print(json.dumps(state['costs']),flush=True)
except Exception as e:state['state']='Stopped; inspect existing exact query IDs; remaining ranges unproved';state['error']=str(e);save();raise
finally:c.close()
