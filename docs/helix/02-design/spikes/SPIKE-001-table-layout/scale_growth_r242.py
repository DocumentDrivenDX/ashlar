"""Append next owned complete-role node slice; exact complete prefix oracle."""
import json,time,hashlib,collections
from pathlib import Path
from scale_mixed_r219 import Workload
from scale_mixed_roles_sql_r224 import role_sql,role_from_pinned_carrier
from mixed_change_queries_r230 import row_hash,row_hash_sql
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_scale_growth_r242';assert not O.exists();O.mkdir();prior=json.loads((B/'out/native/ashlar_scale_growth_r239/summary.json').read_text());a={'state':'Preparing independent complete-prefix oracle','planned_graph':prior['planned_graph'],'slice':{'start':400000,'end':600000},'tables':prior['tables'],'versions':{},'checks':{},'bounds':{'read_bytes':10000000000,'write_bytes':2000000000,'statement_s':180,'slice_wall_s':900}};start=time.monotonic()
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
save();w=Workload(8000000,40000000);oracle=collections.defaultdict(list);columns={}
for i in range(600000):
 for role,row in w.roles('node',i):columns.setdefault(role,list(row));oracle[role].append(row_hash(row,columns[role]))
a['oracle_s']=time.monotonic()-start;save();c=Client(O,observation_timeout=200,cancel_after=180)
def metrics(reserve=0):
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=10000000000 and a['costs']['write_remote_bytes']+reserve<=2000000000
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=180')
 for role,m in a['tables'].items():
  rows=c.sql('detail-before-'+role,'DESCRIBE DETAIL '+m['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,rows[0]))['id']==m['id']
  rows=c.sql('head-before-'+role,'DESCRIBE HISTORY '+m['table']+' LIMIT 1');version=int(rows[0][0]);a['versions'][role]=version
  actual=c.sql('prior-prefix-'+role,f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(columns[role])}))),256) FROM {m['table']} VERSION AS OF {version}");expected=prior['checks'][role];assert actual==[[str(expected['rows']),expected['independent_all_field_digest']]],role
 a['state']='Appending verified disjoint next slice';save();metrics(750000000)
 for role,m in a['tables'].items():
  assert time.monotonic()-start<900;metrics(750000000)
  q=role_sql(role,'node',8000000,40000000,400000,600000) if role=='object_current' else role_from_pinned_carrier(role,'node',8000000,40000000,400000,600000,a['tables']['object_current']['table'],a['versions']['object_current'])
  c.sql('append-'+role,f"INSERT INTO {m['table']} ({','.join(columns[role])}) SELECT {','.join(columns[role])} FROM ({q})");sid=c.records[-1]['statement_id']
  history=c.sql('commit-'+role,'DESCRIBE HISTORY '+m['table']+' LIMIT 20');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];commits=[dict(zip(names,r)) for r in history if dict(zip(names,r)).get('queryHistoryStatementId')==sid];assert len(commits)==1;a['versions'][role]=int(commits[0]['version']);a.setdefault('commits',{})[role]=commits[0];save()
  actual=c.sql('complete-prefix-'+role,f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(columns[role])}))),256) FROM {m['table']} VERSION AS OF {a['versions'][role]}");expected=hashlib.sha256(''.join(sorted(oracle[role])).encode()).hexdigest();assert actual==[[str(len(oracle[role])),expected]],role;a['checks'][role]={'rows':len(oracle[role]),'independent_all_field_digest':expected};metrics()
 t=a['tables']['object_current']['table'];v=a['versions']['object_current'];assert c.sql('identity',f'SELECT count(*),count(DISTINCT id),min(id),max(id) FROM {t} VERSION AS OF {v}')==[['600000','600000','1','600000']]
 a['active_details']={}
 for role,m in a['tables'].items():
  rows=c.sql('detail-after-'+role,'DESCRIBE DETAIL '+m['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];a['active_details'][role]=dict(zip(names,rows[0]))
 metrics();a['slice_wall_s']=time.monotonic()-start;a['state']='Complete600k-node prefix and all raw/history rows verified after disjoint append';a['qualification']='Third200k-node range of proposed8M/40M graph. Prior complete logical prefix verified at observed physical versions; commitIDs bind new snapshots. Named columns and native carrier reuse, independent all-prefix SHA256 field digests. No complete graph/edges, manifest, performance causation, real source authority or full-scale admission.';save();print(json.dumps({'state':a['state'],'versions':a['versions'],'costs':a['costs'],'slice_wall_s':a['slice_wall_s']},indent=2))
except Exception as e:a.update(state='Stopped; inspect exact native handles and committed ranges; never replay unknown append',error=str(e));save();raise
