"""Read-only full-row explicit projection versus star; same pinned publication."""
import json,time,math,hashlib
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_read_shape_r212';assert not O.exists()
p=json.loads((B/'layout-package-candidate.json').read_text())
for name,digest in p['files'].items():assert hashlib.sha256((B/name).read_bytes()).hexdigest()==digest,name
s=json.loads((B/'out/native/ashlar_queue_serial_r197/audited-summary.json').read_text());T=s['owned_tables'][0];U=s['batches'][0]['source_stage'];c=BoundedReads(O)
a={'state':'running','table':T,'version':1,'oracle':U,'oracle_version':0}
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def history(reserve=0):
 c.cursor.close();c.cursor=c.connection.cursor()
 for n in range(10):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==9:raise
   time.sleep(2)
 a['costs']={k:sum(v['metrics'].get(k,0) for v in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=6000000000 and a['costs']['write_remote_bytes']==0
 return h
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=30');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
 for label,table,uid in [('target',T,s['owned_identity'][T]['id']),('source',U,'9ae80a48-cf24-425f-aeff-9472392207ca')]:
  rows=c.sql(label+'-detail','DESCRIBE DETAIL '+table);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,rows[0]))['id']==uid
 pins=c.sql('manifest',f"SELECT table_versions_json FROM {s['owned_tables'][-1]} WHERE publication_id='r189-b1'");assert json.loads(pins[0][0])==s['batches'][0]['versions']
 rows=c.sql('oracle',f"SELECT {','.join(COLS)} FROM {U} VERSION AS OF 0 ORDER BY sha2(cast(id AS STRING),256) LIMIT 10");assert len(rows)==10
 for i,row in enumerate(rows):
  if i%2==0:history(1000000000)
  for shape in (['explicit','star'] if i%2==0 else ['star','explicit']):
   projection=','.join(COLS) if shape=='explicit' else '*'
   query=f'SELECT {projection} FROM {T} VERSION AS OF 1 WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:rel AS BIGINT) AND id=CAST(:id AS BIGINT)'
   actual=c.sql(shape+'-'+str(i),query,parameters={'hash':row[16],'source':row[0],'rel':row[1],'id':row[2]})
   columns=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert columns==list(COLS) and actual==[row]
 h=history();p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1];a['reads']={}
 for shape in ['explicit','star']:
  rs=[r for r in c.records if r['label'].startswith(shape+'-')];assert len(rs)==10 and all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs)
  a['reads'][shape]={'caller_p95_ms':p95([r['wall_ms'] for r in rs]),**{k+'_p95':p95([h[r['statement_id']]['metrics'].get(k,0) for r in rs]) for k in ['execution_time_ms','compilation_time_ms','read_bytes','read_files_count']},'remote_queries':sum(h[r['statement_id']]['metrics'].get('read_remote_bytes',0)>0 for r in rs)}
 a['state']='Twenty full-field exact reads passed';a['qualification']='Ten same SHA-ranked updated keys, alternating shape, persistent driver, native cache false. Star column names/order asserted identical to all20 explicit columns. Physical publication1 and stage0 pinned, UUIDs and manifest verified. Not randomized causation, controlled cold, service p95, compiler plan reuse or billion admission; ten-sample nearest-rank p95 is maximum.';save();print(json.dumps(a,indent=2))
except Exception as e:a.update(state='Stopped; inspect existing handles before further admission',error=str(e));save();raise
finally:c.close()
