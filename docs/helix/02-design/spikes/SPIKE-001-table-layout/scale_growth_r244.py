"""Owned600k-to1M node growth with bounded complete-range verification."""
import json,time
from pathlib import Path
from range_verification_r243 import oracle_chunks,digest_query
from scale_mixed_r219 import Workload
from scale_mixed_roles_sql_r224 import role_sql,role_from_pinned_carrier
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_scale_growth_r244';assert not O.exists();O.mkdir();prior=json.loads((B/'out/native/ashlar_scale_growth_r242/audited-summary.json').read_text());a={'state':'Preparing bounded complete-prefix oracle','planned_graph':prior['planned_graph'],'slice':{'start':600000,'end':1000000},'tables':prior['tables'],'versions':{},'checks':{},'bounds':{'read_bytes':20000000000,'write_bytes':2000000000,'statement_s':180,'slice_wall_s':900}};started=time.monotonic()
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
save();a['oracle']=list(oracle_chunks(Workload(8000000,40000000),'node',1000000));a['oracle_s']=time.monotonic()-started;save();c=Client(O,observation_timeout=200,cancel_after=180)
def metrics(reserve=0):
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=20000000000 and a['costs']['write_remote_bytes']+reserve<=2000000000

def verify(role,end,phase):
 t=a['tables'][role];v=a['versions'][role];chunks=[chunk for chunk in a['oracle'] if chunk['end']<=end];assert chunks and chunks[-1]['end']==end
 expected=sum(chunk['roles'][role]['rows'] for chunk in chunks);assert c.sql(phase+'-count-'+role,f"SELECT count(*) FROM {t['table']} VERSION AS OF {v}")==[[str(expected)]]
 for i,chunk in enumerate(chunks):
  assert time.monotonic()-started<900
  if i%2==0:metrics(750000000)
  r=chunk['roles'][role];assert c.sql(f"{phase}-{role}-{chunk['start']}",digest_query(t['table'],v,role,r['fields'],'node',8000000,chunk['start'],chunk['end']))==[[str(r['rows']),r['digest']]]
 a['checks'][phase+'-'+role]={'rows':expected,'ranges':len(chunks),'version':v,'all_fields':'Each independent range digest matches; complete count proves coverage'};save();metrics()
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=180')
 for role,t in a['tables'].items():
  rows=c.sql('detail-before-'+role,'DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,rows[0]))['id']==t['id']
  rows=c.sql('head-before-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 1');a['versions'][role]=int(rows[0][0]);verify(role,600000,'prior')
 a['state']='Appending disjoint400k-node range';save()
 for role,t in a['tables'].items():
  assert time.monotonic()-started<900;metrics(750000000);cols=a['oracle'][0]['roles'][role]['fields']
  q=role_sql(role,'node',8000000,40000000,600000,1000000) if role=='object_current' else role_from_pinned_carrier(role,'node',8000000,40000000,600000,1000000,a['tables']['object_current']['table'],a['versions']['object_current'])
  c.sql('append-'+role,f"INSERT INTO {t['table']} ({','.join(cols)}) SELECT {','.join(cols)} FROM ({q})");sid=c.records[-1]['statement_id']
  rows=c.sql('commit-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 20');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];commits=[dict(zip(names,r)) for r in rows if dict(zip(names,r)).get('queryHistoryStatementId')==sid];assert len(commits)==1;a['versions'][role]=int(commits[0]['version']);a.setdefault('commits',{})[role]=commits[0];save();verify(role,1000000,'complete')
 t=a['tables']['object_current']['table'];v=a['versions']['object_current'];assert c.sql('identity',f'SELECT count(*),count(DISTINCT id),min(id),max(id) FROM {t} VERSION AS OF {v}')==[['1000000','1000000','1','1000000']]
 a['active_details']={}
 for role,t in a['tables'].items():
  rows=c.sql('detail-after-'+role,'DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];a['active_details'][role]=dict(zip(names,rows[0]))
 metrics();a['initial_table_details']=a['tables'];a['tables']={role:{'table':t['table'],'id':t['id'],'version':a['versions'][role],'rows':a['checks']['complete-'+role]['rows']} for role,t in a['tables'].items()};a['slice_wall_s']=time.monotonic()-started;a['state']='Complete1M-node prefix and all raw/history fields verified';a['qualification']='400k append with prior600k and final1M complete100k-range independent digests, whole-role count and current-ID uniqueness. No edges, graph manifest, real source authority, sustained ingestion, controlled cold/read target or billion-scale admission.';save();print(json.dumps({k:a[k] for k in ['state','versions','costs','slice_wall_s']},indent=2))
except Exception as e:a.update(state='Stopped; inspect same native handles and exact commits; never blindly replay writes',error=str(e));save();raise
