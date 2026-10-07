"""First2M complete-role edges over verified8M nodes; no graph publication."""
import json,time,hashlib
from pathlib import Path
from scale_mixed_r219 import Workload
from range_verification_r243 import oracle_chunks
from grouped_verification_r245 import grouped_query
from mixed_grouped_verification_r249 import mixed_grouped_query,expected_mixed
from scale_mixed_roles_sql_r224 import role_sql,role_from_pinned_carrier
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_scale_edges_r250';assert not O.exists();O.mkdir();prior=json.loads((B/'out/native/ashlar_scale_growth_r248/audited-summary.json').read_text());a={'state':'Preparing independent2M-edge oracles','planned_graph':{'nodes':8000000,'edges':40000000},'edge_slice':[0,2000000],'tables':prior['tables'],'versions':{k:t['version'] for k,t in prior['tables'].items()},'checks':{},'bounds':{'read_bytes':80000000000,'write_bytes':12000000000,'phase_reserve':3000000000,'statement_s':180,'wall_s':900},'reused_node_oracle_sha256':hashlib.sha256((B/'out/native/ashlar_scale_growth_r248/audited-summary.json').read_bytes()).hexdigest()};started=time.monotonic()
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
save();a['edge_oracle']=list(oracle_chunks(Workload(8000000,40000000),'edge',2000000));a['oracle_s']=time.monotonic()-started;save();c=Client(O,observation_timeout=200,cancel_after=180);F='client_dev.ashlar_entropy_20261006_r86'
def metrics(reserve=0):
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=80000000000 and a['costs']['write_remote_bytes']+reserve<=12000000000
 return h

def detail(role):
 t=a['tables'][role];rows=c.sql('detail-'+role,'DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];d=dict(zip(names,rows[0]));assert 'id' not in t or t['id']==d['id'];t['id']=d['id'];return d

def commit(role,sid):
 rows=c.sql('commit-'+role,'DESCRIBE HISTORY '+a['tables'][role]['table']+' LIMIT 20');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];found=[dict(zip(names,r)) for r in rows if dict(zip(names,r)).get('queryHistoryStatementId')==sid];assert len(found)==1,role;a['versions'][role]=int(found[0]['version']);a.setdefault('commits',{})[role]=found[0];save()

def verify(role,mixed=False,prior_only=False):
 t=a['tables'][role];v=a['versions'][role];chunks=prior['oracle'] if prior_only or role=='object_current' else a['edge_oracle'];fields=chunks[0]['roles'][role]['fields'];label=('prior-' if prior_only else 'complete-')+role
 if mixed:
  expected=expected_mixed(prior['oracle'],a['edge_oracle'],role);assert c.sql(label+'-count',f"SELECT count(*) FROM {t['table']} VERSION AS OF {v}")==[[str(sum(int(r[1]) for r in expected))]];actual=[]
  for first in range(0,len(expected),100):
   metrics(3000000000);actual+=c.sql(label+'-'+str(first),mixed_grouped_query(t['table'],v,role,fields,8000000,2000000,100000,first,100))
 else:
  kind='node' if prior_only or role=='object_current' else 'edge';end=8000000 if kind=='node' else 2000000;expected=[[str(i),str(q['roles'][role]['rows']),q['roles'][role]['digest']] for i,q in enumerate(chunks)];actual=c.sql(label,grouped_query(t['table'],v,role,fields,kind,8000000,end))
 assert actual==expected,role;a['checks'][label]={'version':v,'rows':sum(int(r[1]) for r in actual),'groups':actual,'all_fields':'Independent digests match; complete membership coverage'};save();metrics()
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=180')
 for role in ['object_current','source_record','property_journal']:
  detail(role)
  if role!='object_current':a['versions'][role]=int(c.sql('head-'+role,'DESCRIBE HISTORY '+a['tables'][role]['table']+' LIMIT 1')[0][0])
  verify(role,prior_only=True)
 a['state']='Materializing first2M complete-role edge slice';save()
 for role in ['edge_current','source_record','property_journal','adjacency_forward']:
  assert time.monotonic()-started<900;metrics(3000000000);cols=a['edge_oracle'][0]['roles'][role]['fields']
  if role in ['edge_current','adjacency_forward']:
   table=F+'.growth_'+role+'_r250';assert c.sql('absent-'+role,f"SHOW TABLES IN {F} LIKE 'growth_{role}_r250'")==[];a['tables'][role]={'table':table}
   q=role_sql(role,'edge',8000000,40000000,0,2000000) if role=='edge_current' else role_from_pinned_carrier(role,'edge',8000000,40000000,0,2000000,a['tables']['edge_current']['table'],a['versions']['edge_current']);cluster='lookup_hash' if role=='edge_current' else 'source_id';c.sql('create-'+role,f"CREATE TABLE {table} USING DELTA CLUSTER BY ({cluster}) TBLPROPERTIES ('delta.parquet.compression.codec'='zstd') AS {q}")
  else:
   q=role_from_pinned_carrier(role,'edge',8000000,40000000,0,2000000,a['tables']['edge_current']['table'],a['versions']['edge_current']);c.sql('append-'+role,f"INSERT INTO {a['tables'][role]['table']} ({','.join(cols)}) SELECT {','.join(cols)} FROM ({q})")
  sid=c.records[-1]['statement_id'];commit(role,sid);detail(role);verify(role,mixed=role in ['source_record','property_journal'])
 e=a['tables']['edge_current']['table'];ev=a['versions']['edge_current'];n=a['tables']['object_current']['table'];nv=a['versions']['object_current'];metrics(3000000000)
 assert c.sql('edge-identities',f'SELECT count(*),count(DISTINCT id),min(id),max(id) FROM {e} VERSION AS OF {ev}')==[['2000000','2000000','8000001','10000000']]
 assert c.sql('typed-endpoint-closure',f"SELECT count(*),count_if(s.id IS NULL OR t.id IS NULL) FROM {e} VERSION AS OF {ev} e LEFT JOIN {n} VERSION AS OF {nv} s ON e.source_system=s.source_system AND e.source_type=s.type_id AND e.source_id=s.id LEFT JOIN {n} VERSION AS OF {nv} t ON e.source_system=t.source_system AND e.target_type=t.type_id AND e.target_id=t.id")==[['2000000','0']]
 a['active_details']={role:detail(role) for role in a['tables']};h=metrics();a['cache_audit']=[{'label':r['label'],'statement_id':r['statement_id']} for r in c.records if h[r['statement_id']]['metrics'].get('result_from_cache')];a['tables']={role:{'table':t['table'],'id':t['id'],'version':a['versions'][role],'rows':a['checks']['complete-'+role]['rows'] if role!='object_current' else 8000000} for role,t in a['tables'].items()};a['state']='Complete8M-node/2M-edge staging passes every role field and typed endpoint closure';a['wall_s']=time.monotonic()-started;a['qualification']='First2M of planned40M edges, synthetic bootstrap. Prior node/raw/history verified, full mixed counts/all100 groups, complete edge/forward fields, unique allocatedIDs and both typed endpoints. No manifest/source ACK, ingestion latency/cold/concurrency/SLO, full DDL/producer authority or1B/5B admission.';save();print(json.dumps({k:a[k] for k in ['state','versions','costs','wall_s']},indent=2))
except Exception as e:a.update(state='Stopped; inspect same native handles and commits; never blindly replay writes',error=str(e));save();raise
