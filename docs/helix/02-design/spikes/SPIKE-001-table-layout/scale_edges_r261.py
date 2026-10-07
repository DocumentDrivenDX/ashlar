"""Grow verified16M edges to24M over8M nodes; no graph publication."""
import json,time,hashlib
from pathlib import Path
from scale_mixed_r219 import Workload
from range_verification_r243 import oracle_chunks
from grouped_verification_r245 import grouped_query
from pure_group_blocks_r254 import pure_grouped_block
from mixed_grouped_pruning_r253 import pruned_mixed_query
from mixed_grouped_verification_r249 import mixed_grouped_query,expected_mixed
from scale_mixed_roles_sql_r224 import role_sql,role_from_pinned_carrier
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_scale_edges_r261';assert not O.exists();O.mkdir();node_prior=json.loads((B/'out/native/ashlar_scale_growth_r248/audited-summary.json').read_text());prior=json.loads((B/'out/native/ashlar_scale_edges_r257/audited-summary.json').read_text());a={'state':'Preparing independent8M-edge oracles','planned_graph':{'nodes':8000000,'edges':40000000},'edge_slice':[16000000,24000000],'tables':prior['tables'],'versions':{k:t['version'] for k,t in prior['tables'].items()},'checks':{},'bounds':{'read_bytes':360000000000,'write_bytes':40000000000,'phase_reserve':10000000000,'statement_s':180,'wall_s':2400,'spill_bytes':30000000000},'reused_node_oracle_sha256':hashlib.sha256((B/'out/native/ashlar_scale_growth_r248/audited-summary.json').read_bytes()).hexdigest()};started=time.monotonic()
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
budget_path=B/'out/next-growth-budget-r260.json';budget=json.loads(budget_path.read_text());assert budget['new_edges']==8000000 and budget['next_prefix_edges']==24000000
a['budget_sha256']=hashlib.sha256(budget_path.read_bytes()).hexdigest();assert all(a['bounds'][k]==budget['bounds'][k] for k in ['read_bytes','write_bytes','spill_bytes','phase_reserve','wall_s']);assert a['bounds']['statement_s']==budget['bounds']['statement_cancel_s'];save();a['reused_edge_oracle_sha256']=hashlib.sha256((B/'out/native/ashlar_scale_edges_r257/audited-summary.json').read_bytes()).hexdigest();assert len(prior['edge_oracle'])==160 and all(q['start']==i*100000 and q['end']==(i+1)*100000 for i,q in enumerate(prior['edge_oracle']));a['edge_oracle']=list(prior['edge_oracle']);save()
for chunk in oracle_chunks(Workload(8000000,40000000),'edge',24000000,start=16000000):
 a['edge_oracle'].append(chunk);a['oracle_progress']={'end':chunk['end'],'elapsed_s':time.monotonic()-started}
 if len(a['edge_oracle'])%10==0:save()
a['oracle_s']=time.monotonic()-started;a['state']='Checking prior16M-edge prefix before any writes';save();c=Client(O,observation_timeout=200,cancel_after=180);F='client_dev.ashlar_entropy_20261006_r86'
def metrics(reserve=0):
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=360000000000 and a['costs']['write_remote_bytes']+reserve<=40000000000 and a['costs']['spill_to_disk_bytes']+reserve<=30000000000
 return h

def detail(role):
 t=a['tables'][role];rows=c.sql('detail-'+role,'DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];d=dict(zip(names,rows[0]));assert 'id' not in t or t['id']==d['id'];t['id']=d['id'];return d

def commit(role,sid):
 rows=c.sql('commit-'+role,'DESCRIBE HISTORY '+a['tables'][role]['table']+' LIMIT 20');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];found=[dict(zip(names,r)) for r in rows if dict(zip(names,r)).get('queryHistoryStatementId')==sid];assert len(found)==1,role;a['versions'][role]=int(found[0]['version']);a.setdefault('commits',{})[role]=found[0];save()

def verify(role,end,phase):
 assert time.monotonic()-started<2400;t=a['tables'][role];v=a['versions'][role];edges=[q for q in a['edge_oracle'] if q['end']<=end];nodes=node_prior['oracle'];label=phase+'-'+role
 if role in ['source_record','property_journal']:
  fields=nodes[0]['roles'][role]['fields'];expected=expected_mixed(nodes,edges,role);assert c.sql(label+'-count',f"SELECT count(*) FROM {t['table']} VERSION AS OF {v}")==[[str(sum(int(r[1]) for r in expected))]];actual=[]
  for first in range(0,len(expected),100):
   assert time.monotonic()-started<2400;metrics(10000000000);actual+=c.sql(label+'-'+str(first),pruned_mixed_query(t['table'],v,role,fields,8000000,end,100000,first,100))
 else:
  chunks=nodes if role=='object_current' else edges;fields=chunks[0]['roles'][role]['fields'];expected=[[str(i),str(q['roles'][role]['rows']),q['roles'][role]['digest']] for i,q in enumerate(chunks)]
  if role=='object_current':actual=c.sql(label,grouped_query(t['table'],v,role,fields,'node',8000000,8000000))
  else:
   assert c.sql(label+'-count',f"SELECT count(*) FROM {t['table']} VERSION AS OF {v}")==[[str(sum(int(r[1]) for r in expected))]];actual=[]
   for first in range(0,len(expected),100):
    assert time.monotonic()-started<2400;metrics(10000000000);actual+=c.sql(label+'-'+str(first),pure_grouped_block(t['table'],v,role,fields,8000000,end,100000,first,100))
 assert actual==expected,role;a['checks'][label]={'version':v,'rows':sum(int(r[1]) for r in actual),'groups':actual,'all_fields':'Independent complete digests match; full mixed-role membership coverage'};save();metrics()

try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=180')
 for role in ['object_current','source_record','property_journal','edge_current','adjacency_forward']:
  detail(role)
  settings=c.sql('maintenance-status-'+role,'DESCRIBE TABLE EXTENDED '+a['tables'][role]['table']);assert [r[1] for r in settings if r[0]=='Predictive Optimization']==['DISABLE']
  if role!='object_current':a['versions'][role]=int(c.sql('head-'+role,'DESCRIBE HISTORY '+a['tables'][role]['table']+' LIMIT 1')[0][0])
  verify(role,16000000,'prior')
 a['state']='Materializing next8M complete-role edge slice';save()
 for role in ['edge_current','source_record','property_journal','adjacency_forward']:
  assert time.monotonic()-started<2400;metrics(10000000000);cols=a['edge_oracle'][0]['roles'][role]['fields']
  q=role_sql(role,'edge',8000000,40000000,16000000,24000000) if role=='edge_current' else role_from_pinned_carrier(role,'edge',8000000,40000000,16000000,24000000,a['tables']['edge_current']['table'],a['versions']['edge_current'])
  c.sql('append-'+role,f"INSERT INTO {a['tables'][role]['table']} ({','.join(cols)}) SELECT {','.join(cols)} FROM ({q})")
  sid=c.records[-1]['statement_id'];commit(role,sid);detail(role);verify(role,24000000,'complete')
 e=a['tables']['edge_current']['table'];ev=a['versions']['edge_current'];n=a['tables']['object_current']['table'];nv=a['versions']['object_current'];metrics(10000000000)
 assert c.sql('edge-identities',f'SELECT count(*),count(DISTINCT id),min(id),max(id) FROM {e} VERSION AS OF {ev}')==[['24000000','24000000','8000001','32000000']]
 assert c.sql('typed-endpoint-closure',f"SELECT count(*),count_if(s.id IS NULL OR t.id IS NULL) FROM {e} VERSION AS OF {ev} e LEFT JOIN {n} VERSION AS OF {nv} s ON e.source_system=s.source_system AND e.source_type=s.type_id AND e.source_id=s.id LEFT JOIN {n} VERSION AS OF {nv} t ON e.source_system=t.source_system AND e.target_type=t.type_id AND e.target_id=t.id")==[['24000000','0']]
 a['active_details']={role:detail(role) for role in a['tables']};h=metrics();a['cache_audit']=[{'label':r['label'],'statement_id':r['statement_id']} for r in c.records if h[r['statement_id']]['metrics'].get('result_from_cache')];a['tables']={role:{'table':t['table'],'id':t['id'],'version':a['versions'][role],'rows':a['checks']['complete-'+role]['rows'] if role!='object_current' else 8000000} for role,t in a['tables'].items()};a['state']='Complete8M-node/24M-edge staging passes every role field and typed endpoint closure';a['wall_s']=time.monotonic()-started;a['qualification']='Accumulated24M of planned40M edges, synthetic bootstrap. Prior node/raw/history verified, full mixed counts/all320 groups, complete edge/forward fields, unique allocatedIDs and both typed endpoints. No manifest/source ACK, ingestion latency/cold/concurrency/SLO, full DDL/producer authority or1B/5B admission.';save();print(json.dumps({k:a[k] for k in ['state','versions','costs','wall_s']},indent=2))
except Exception as e:a.update(state='Stopped; inspect same native handles and commits; never blindly replay writes',error=str(e));save();raise
