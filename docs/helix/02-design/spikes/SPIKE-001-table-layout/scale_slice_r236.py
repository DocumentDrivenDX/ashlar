"""First owned complete-role200k-node slice of proposed8M/40M bootstrap."""
import json,time,hashlib,collections
from pathlib import Path
from scale_mixed_r219 import Workload
from scale_mixed_roles_sql_r224 import role_sql
from mixed_change_queries_r230 import row_hash,row_hash_sql
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_scale_slice_r236';assert not O.exists();O.mkdir();a={'state':'Preparing independent Python oracle','planned_graph':{'nodes':8000000,'edges':40000000},'slice':{'kind':'node','start':0,'end':200000},'tables':{},'checks':{},'bounds':{'write_bytes':2000000000,'read_bytes':10000000000,'statement_s':180,'slice_wall_s':900}};started=time.monotonic()
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
save();w=Workload(8000000,40000000);columns={};oracle=collections.defaultdict(list)
for i in range(200000):
 for role,row in w.roles('node',i):
  columns.setdefault(role,list(row));oracle[role].append(row_hash(row,columns[role]))
a['oracle_preparation_s']=time.monotonic()-started;a['state']='Running bounded native slice';save();c=Client(O,observation_timeout=200,cancel_after=180);F='client_dev.ashlar_entropy_20261006_r86'
def metrics(reserve=0):
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(v['metrics'].get(k,0) for v in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=10000000000 and a['costs']['write_remote_bytes']+reserve<=2000000000
try:
 a['runtime']=c.sql('runtime','SELECT current_version(),current_timezone()')
 warehouse=c.w.warehouses.get(c.warehouse_id);a['compute']={k:str(getattr(warehouse,k,None)) for k in ['id','name','state','cluster_size','min_num_clusters','max_num_clusters','enable_serverless_compute']};save();assert str(warehouse.state).split('.')[-1]=='RUNNING'
 c.sql('timeout','SET STATEMENT_TIMEOUT=180')
 for role in oracle:
  assert time.monotonic()-started<900;metrics(750000000)
  table=F+'.growth_'+role+'_r236';assert c.sql('absent-'+role,f"SHOW TABLES IN {F} LIKE 'growth_{role}_r236'")==[]
  cluster='lookup_hash' if role=='object_current' else 'delivery_id' if role=='source_record' else 'source_delivery_id';q=role_sql(role,'node',8000000,40000000,0,200000)
  c.sql('create-'+role,f"CREATE TABLE {table} USING DELTA CLUSTER BY ({cluster}) TBLPROPERTIES ('delta.parquet.compression.codec'='zstd') AS {q}")
  rows=c.sql('detail-'+role,'DESCRIBE DETAIL '+table);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];d=dict(zip(names,rows[0]));a['tables'][role]={'table':table,'id':d['id'],'version':0,'rows':len(oracle[role]),'bytes':int(d['sizeInBytes']),'files':int(d['numFiles']),'reader':d['minReaderVersion'],'writer':d['minWriterVersion']};save()
  expected=hashlib.sha256(''.join(sorted(oracle[role])).encode()).hexdigest();actual=c.sql('full-digest-'+role,f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(columns[role])}))),256) FROM {table} VERSION AS OF 0");assert actual==[[str(len(oracle[role])),expected]],role;a['checks'][role]={'rows':len(oracle[role]),'independent_all_field_digest':expected};metrics()
 t=a['tables']['object_current']['table'];assert c.sql('identity',f'SELECT count(*),count(DISTINCT id),min(id),max(id) FROM {t} VERSION AS OF 0')==[['200000','200000','1','200000']]
 metrics();a['active_bytes']=sum(x['bytes'] for x in a['tables'].values());a['slice_wall_s']=time.monotonic()-started;a['state']='First200k-node complete-role growth slice passes every independent row digest';a['qualification']='200k of8M planned nodes, source/raw and800k bootstrap events complete for slice only. No edges/full graph or manifest publication yet. All-row field digest under SHA256 collision assumptions. One bounded slice does not admit8M/40M,1B/5B or sustained/cold/caller gates. Existing warehouse unchanged; reported storage counters not full price/retained-storage bill.';save();print(json.dumps({'state':a['state'],'tables':a['tables'],'costs':a['costs'],'slice_wall_s':a['slice_wall_s']},indent=2))
except Exception as e:a.update(state='Stopped; inspect same native handles and slice ownership before further admission',error=str(e));save();raise
