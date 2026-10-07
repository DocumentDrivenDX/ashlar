"""Ten identical full-field singleton keys: driver versus Statement API, read-only."""
import json,time,math
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from persistent_sql import Client
from property_apply_queries import COLS
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_read_transport_r211';assert not O.exists()
s=json.loads((B/'out/native/ashlar_queue_serial_r197/audited-summary.json').read_text());T=s['owned_tables'][0];U=s['batches'][0]['source_stage'];d=BoundedReads(O/'driver');r=Client(O/'rest',observation_timeout=60)
state={'table':T,'id':s['owned_identity'][T]['id'],'version':1,'oracle':U,'oracle_version':0,'state':'In progress'}
def save():(O/'summary.json').write_text(json.dumps(state,indent=2)+'\n')
def metrics(reserve=0):
 d.cursor.close();d.cursor=d.connection.cursor()
 for attempt in range(10):
  try:h=collect_history(d.w,d.records+r.records,O/'shared-history.json',require_final=True);break
  except HistoryPending:
   if attempt==9:raise
   time.sleep(2)
 state['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert state['costs']['read_bytes']+reserve<=6000000000 and state['costs']['write_remote_bytes']==0
 return h
try:
 d.sql('timeout','SET STATEMENT_TIMEOUT=30')
 a=d.sql('detail','DESCRIBE DETAIL '+T);names=[p['name'] for p in d.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,a[0]))['id']==state['id']
 pins=d.sql('manifest',f"SELECT table_versions_json FROM {s['owned_tables'][-1]} WHERE publication_id='r189-b1'");assert json.loads(pins[0][0])==s['batches'][0]['versions']
 cols=','.join('CAST(published_at AS STRING) AS published_at' if x=='published_at' else x for x in COLS)
 rows=d.sql('oracle',f'SELECT {cols} FROM {U} VERSION AS OF 0 ORDER BY sha2(cast(id AS STRING),256) LIMIT 10');assert len(rows)==10
 for i,row in enumerate(rows):
  if i%2==0:metrics(1000000000)
  q=f'SELECT {cols} FROM {T} VERSION AS OF 1 WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:rel AS BIGINT) AND id=CAST(:id AS BIGINT) AND rand()>=0'
  params={'hash':row[16],'source':row[0],'rel':row[1],'id':row[2]}
  for name in (['driver','rest'] if i%2==0 else ['rest','driver']):
   if name=='driver':actual=d.sql('point-'+str(i),q,parameters=params)
   else:
    request={'label':'point-'+str(i),'statement':q,'parameters':params,'started_epoch':time.time()};(O/'rest/inflight-request.json').write_text(json.dumps(request)+'\n')
    actual=r.sql('point-'+str(i),'/* ashlar ashlar_read_transport_r211 rest-'+str(i)+' */ '+q,parameters=[{'name':k,'value':v,'type':'STRING'} for k,v in params.items()])
   assert actual==[row]
 h=metrics();p95=lambda a:sorted(a)[math.ceil(.95*len(a))-1];state['reads']={}
 for name,c in [('driver',d),('rest',r)]:
  rs=[x for x in c.records if x['label'].startswith('point-')];assert len(rs)==10 and all(not h[x['statement_id']]['metrics'].get('result_from_cache') for x in rs)
  state['reads'][name]={'caller_p95_ms':p95([x['wall_ms'] for x in rs]),**{k+'_p95':p95([h[x['statement_id']]['metrics'].get(k,0) for x in rs]) for k in ['execution_time_ms','compilation_time_ms','read_bytes','read_files_count']},'caller_minus_native_p95_ms':p95([x['wall_ms']-h[x['statement_id']]['metrics']['total_time_ms'] for x in rs]),'remote_queries':sum(h[x['statement_id']]['metrics'].get('read_remote_bytes',0)>0 for x in rs)}
 state['state']='Twenty matched exact transport reads passed within budget';state['qualification']='Ten SHA-ranked same updated keys/full20fields; native timestamp cast to string on oracle and both readers for equal wire display, not a timestamp-support expansion. Both use same nondeterministic nonnegative rand predicate and metrics confirm result-uncached. Alternating order, prior validation/cache and telemetry idle gaps; not controlled cold, randomized causal result, service p95 or billion admission. REST parameters STRING plus explicit BIGINT casts match driver native decimal strings.';save();print(json.dumps({'reads':state['reads'],'costs':state['costs']},indent=2))
except Exception as e:state['state']='Stopped; inspect same native handles before any new query admission';state['error']=str(e);save();raise
finally:d.close()
