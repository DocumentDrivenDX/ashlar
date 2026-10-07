"""Owned600k-to1M node growth with bounded complete-range verification."""
import json,time
from pathlib import Path
from range_verification_r243 import oracle_chunks
from grouped_verification_r245 import grouped_query
from scale_mixed_r219 import Workload
from scale_mixed_roles_sql_r224 import role_sql,role_from_pinned_carrier
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_scale_growth_r247';assert not O.exists();O.mkdir();prior=json.loads((B/'out/native/ashlar_scale_growth_r246/audited-summary.json').read_text());a={'state':'Preparing bounded complete-prefix oracle','planned_graph':prior['planned_graph'],'slice':{'start':2000000,'end':4000000},'tables':prior['tables'],'versions':{},'checks':{},'bounds':{'read_bytes':30000000000,'write_bytes':8000000000,'statement_s':180,'slice_wall_s':900}};started=time.monotonic()
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
save();a['reused_oracle_source']={'file':'out/native/ashlar_scale_growth_r246/audited-summary.json','sha256':__import__('hashlib').sha256((B/'out/native/ashlar_scale_growth_r246/audited-summary.json').read_bytes()).hexdigest(),'entities':2000000};assert prior['oracle'][0]['start']==0 and prior['oracle'][-1]['end']==2000000 and all(q['start']==i*100000 and q['end']==(i+1)*100000 for i,q in enumerate(prior['oracle']));a['oracle']=prior['oracle']+list(oracle_chunks(Workload(8000000,40000000),'node',4000000,start=2000000));a['oracle_s']=time.monotonic()-started;save();c=Client(O,observation_timeout=200,cancel_after=180)
def metrics(reserve=0):
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=30000000000 and a['costs']['write_remote_bytes']+reserve<=8000000000

def verify(role,end,phase):
 t=a['tables'][role];v=a['versions'][role];chunks=[chunk for chunk in a['oracle'] if chunk['end']<=end];assert chunks and chunks[-1]['end']==end
 assert time.monotonic()-started<900;metrics(2500000000)
 expected=[[str(i),str(chunk['roles'][role]['rows']),chunk['roles'][role]['digest']] for i,chunk in enumerate(chunks)]
 actual=c.sql(phase+'-grouped-'+role,grouped_query(t['table'],v,role,chunks[0]['roles'][role]['fields'],'node',8000000,end));assert actual==expected,role
 a['checks'][phase+'-'+role]={'rows':sum(int(r[1]) for r in actual),'ranges':len(chunks),'version':v,'all_field_digests':actual,'invalid_bucket_absent':True};save();metrics()

try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=180')
 for role,t in a['tables'].items():
  rows=c.sql('detail-before-'+role,'DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,rows[0]))['id']==t['id']
  rows=c.sql('head-before-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 1');a['versions'][role]=int(rows[0][0]);verify(role,2000000,'prior')
 a['state']='Appending disjoint2M-node range';save()
 for role,t in a['tables'].items():
  assert time.monotonic()-started<900;metrics(2500000000);cols=a['oracle'][0]['roles'][role]['fields']
  q=role_sql(role,'node',8000000,40000000,2000000,4000000) if role=='object_current' else role_from_pinned_carrier(role,'node',8000000,40000000,2000000,4000000,a['tables']['object_current']['table'],a['versions']['object_current'])
  c.sql('append-'+role,f"INSERT INTO {t['table']} ({','.join(cols)}) SELECT {','.join(cols)} FROM ({q})");sid=c.records[-1]['statement_id']
  rows=c.sql('commit-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 20');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];commits=[dict(zip(names,r)) for r in rows if dict(zip(names,r)).get('queryHistoryStatementId')==sid];assert len(commits)==1;a['versions'][role]=int(commits[0]['version']);a.setdefault('commits',{})[role]=commits[0];save();verify(role,4000000,'complete')
 t=a['tables']['object_current']['table'];v=a['versions']['object_current'];assert c.sql('identity',f'SELECT count(*),count(DISTINCT id),min(id),max(id) FROM {t} VERSION AS OF {v}')==[['4000000','4000000','1','4000000']]
 a['active_details']={}
 for role,t in a['tables'].items():
  rows=c.sql('detail-after-'+role,'DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];a['active_details'][role]=dict(zip(names,rows[0]))
 metrics();a['initial_table_details']=a['tables'];a['tables']={role:{'table':t['table'],'id':t['id'],'version':a['versions'][role],'rows':a['checks']['complete-'+role]['rows']} for role,t in a['tables'].items()};a['slice_wall_s']=time.monotonic()-started;a['state']='Complete4M-node prefix and all raw/history fields verified';a['qualification']='2M append with prior2M and final4M complete grouped100k-range independent digests; all rows assigned including invalid membership and current-ID uniqueness. No edges, graph manifest, real source authority, sustained ingestion, controlled cold/read target or billion-scale admission.';save();print(json.dumps({k:a[k] for k in ['state','versions','costs','slice_wall_s']},indent=2))
except Exception as e:a.update(state='Stopped; inspect same native handles and exact commits; never blindly replay writes',error=str(e));save();raise
