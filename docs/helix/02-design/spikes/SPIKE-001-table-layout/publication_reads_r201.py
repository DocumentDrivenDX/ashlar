"""Matched exact singleton reads on overlap/serial manifests, two small passes."""
import json,time,math
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_publication_reads_r201';assert not O.exists(),'Inspect previous IDs'
c=BoundedReads(O);deadline=time.monotonic()+600;state={'state':'In progress; read-only','targets':{}}
def sql(label,q,parameters=None):
 assert time.monotonic()<deadline,'Admission deadline'
 return c.sql(label,q,parameters=parameters)
def save():(O/'summary.json').write_text(json.dumps(state,indent=2)+'\n')
def telemetry(reserve=0):
 c.cursor.close();c.cursor=c.connection.cursor()
 for attempt in range(10):
  h={x['query_id']:x for x in c.history()}
  if all(r['statement_id'] in h and h[r['statement_id']]['is_final'] for r in c.records):break
  time.sleep(2)
 assert all(h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in c.records)
 state['costs']={k:sum(h[r['statement_id']]['metrics'].get(k,0) for r in c.records) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save()
 assert state['costs']['read_bytes']+reserve<=25000000000 and state['costs']['write_remote_bytes']==0,'Stop admission: read budget'
 return h
try:
 sql('timeout','SET STATEMENT_TIMEOUT=30')
 for name,run in [('overlap','ashlar_queue_resume_r192'),('serial','ashlar_queue_serial_r197')]:
  s=json.loads((B/'out/native'/run/'audited-summary.json').read_text());T=s['owned_tables'][0];x=s['owned_identity'][T];M=s['owned_tables'][-1];assert x['version']==1
  a=sql(name+'-detail','DESCRIBE DETAIL '+T);names=[p['name'] for p in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,a[0]))['id']==x['id'];assert sql(name+'-version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]=='1'
  pins=sql(name+'-manifest',f"SELECT table_versions_json FROM {M} WHERE publication_id='r189-b1'");assert json.loads(pins[0][0])==s['batches'][0]['versions']
  state['targets'][name]={'table':T,'id':x['id'],'version':1,'manifest':M,'publication_id':'r189-b1','vector':json.loads(pins[0][0])}
 U='client_dev.ashlar_entropy_20261006_r86.schedule_r189_1';state['oracle']=U;state['oracle_version']=0
 rows=sql('oracle',f"SELECT {','.join(COLS)} FROM {U} VERSION AS OF 0 ORDER BY sha2(cast(id AS STRING),256) LIMIT 20");assert len(rows)==20;telemetry(3000000000)
 for phase in [0,1]:
  for i,row in enumerate(rows):
   if i%4==0:telemetry(3000000000)
   for name in (['overlap','serial'] if (i+phase)%2==0 else ['serial','overlap']):
    x=state['targets'][name];q=f"SELECT {','.join(COLS)} FROM {x['table']} VERSION AS OF 1 WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:rel AS BIGINT) AND id=CAST(:id AS BIGINT)"
    assert sql(f'p{phase}-{name}-{i}',q,parameters={'hash':row[16],'source':row[0],'rel':row[1],'id':row[2]})==[row]
  print('Exact pass completed',phase,flush=True)
 for name,x in state['targets'].items():assert sql(name+'-final-version','DESCRIBE HISTORY '+x['table']+' LIMIT 1')[0][0]=='1'
 h=telemetry();p95=lambda a:sorted(a)[math.ceil(.95*len(a))-1];state['reads']={}
 for phase in [0,1]:
  state['reads'][str(phase)]={}
  for name in ['overlap','serial']:
   rs=[r for r in c.records if r['label'].startswith(f'p{phase}-{name}-')];assert len(rs)==20 and all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs)
   state['reads'][str(phase)][name]={'caller_p95_ms':p95([r['wall_ms'] for r in rs]),**{k+'_p95':p95([h[r['statement_id']]['metrics'].get(k,0) for r in rs]) for k in ['execution_time_ms','compilation_time_ms','read_bytes','read_files_count']},'remote_queries':sum(h[r['statement_id']]['metrics'].get('read_remote_bytes',0)>0 for r in rs),'caller_minus_server_p95_ms':p95([r['wall_ms']-h[r['statement_id']]['metrics']['total_time_ms'] for r in rs])}
 state['state']='Eighty matched exact publication reads across two passes passed within budget';state['qualification']='Same20 SHA-ranked updated identities and independent stage0 full20-field oracle; alternating order reversed in second pass; persistent client/result cache false. Prior validations may warm files and telemetry adds idle gaps. Not controlled cold, randomized causal comparison, production workload/service p95, concurrent ingest, sustained or billion admission. Different private manifests share modeled batch ID but pin separate verified owned tables.';save();print(json.dumps({'reads':state['reads'],'costs':state['costs']},indent=2))
except Exception as e:state['state']='Stopped; inspect same native IDs before more admission';state['error']=str(e);save();raise
finally:c.close()
