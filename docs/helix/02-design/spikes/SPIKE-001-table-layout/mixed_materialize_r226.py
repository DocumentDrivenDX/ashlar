"""Native complete-role calibration with independent all-row field digest oracle."""
import json,time,hashlib,collections
from pathlib import Path
from scale_mixed_r219 import Workload
from scale_mixed_roles_sql_r224 import role_sql
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_mixed_materialize_r226';assert not O.exists();c=Client(O,observation_timeout=200,cancel_after=180);F='client_dev.ashlar_entropy_20261006_r86';a={'state':'running','tables':{},'checks':{},'bounds':{'read_bytes':10000000000,'write_bytes':1000000000,'native_statement_s':180}};N=4096;E=20480
w=Workload(N,E);oracle=collections.defaultdict(list);fields={}
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def scalar(k,v):
 if v is None:return None
 if k in ['published_at','received_at']:return v.replace('T',' ').removesuffix('Z')
 if type(v) is bool:return str(v).lower()
 return str(v)
def row_digest(row):
 encoded=''
 for key in fields[role]:
  value=scalar(key,row[key]);encoded+='N;' if value is None else 'V'+str(len(value.encode()))+':'+value.encode().hex().upper()+';'
 return hashlib.sha256(encoded.encode()).hexdigest()
for kind,count in [('node',N),('edge',E)]:
 for i in range(count):
  for role,row in w.roles(kind,i):
   fields.setdefault(role,list(row));assert fields[role]==list(row);oracle[role].append(row_digest(row))
def metrics(reserve=0):
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(v['metrics'].get(k,0) for v in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=10000000000 and a['costs']['write_remote_bytes']+reserve<=1000000000
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=180');a['runtime']=c.sql('runtime','SELECT current_version(),current_timezone()')
 for role in oracle:
  metrics(100000000)
  table=F+'.mixed_'+role+'_r226';assert c.sql('absent-'+role,f"SHOW TABLES IN {F} LIKE 'mixed_{role}_r226'")==[]
  kinds=['node'] if role=='object_current' else ['edge'] if role in ['edge_current','adjacency_forward'] else ['node','edge']
  q=' UNION ALL '.join('SELECT * FROM ('+role_sql(role,kind,N,E,0,N if kind=='node' else E)+')' for kind in kinds)
  cluster='lookup_hash' if role.endswith('current') else 'delivery_id' if role=='source_record' else 'source_id' if role=='adjacency_forward' else 'source_delivery_id'
  c.sql('create-'+role,f"CREATE TABLE {table} USING DELTA CLUSTER BY ({cluster}) TBLPROPERTIES ('delta.parquet.compression.codec'='zstd') AS {q}")
  detail=c.sql('detail-'+role,'DESCRIBE DETAIL '+table);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];d=dict(zip(names,detail[0]));a['tables'][role]={'table':table,'id':d['id'],'version':0,'rows':len(oracle[role]),'bytes':int(d['sizeInBytes']),'files':int(d['numFiles']),'reader':d['minReaderVersion'],'writer':d['minWriterVersion']};save()
  expressions=[]
  for field in fields[role]:
   value=f'CAST({field} AS STRING)';expressions.append(f"CASE WHEN {field} IS NULL THEN 'N;' ELSE concat('V',cast(length(encode({value},'UTF-8')) AS STRING),':',hex(encode({value},'UTF-8')),';') END")
  digest="sha2(concat("+','.join(expressions)+"),256)"
  actual=c.sql('all-row-digest-'+role,f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({digest}))),256) FROM {table} VERSION AS OF 0")
  expected=hashlib.sha256(''.join(sorted(oracle[role])).encode()).hexdigest();assert actual==[[str(len(oracle[role])),expected]],(role,actual,expected);a['checks'][role]={'rows':len(oracle[role]),'independent_all_field_digest':expected};metrics()
 node=a['tables']['object_current']['table']+' VERSION AS OF 0';edge=a['tables']['edge_current']['table']+' VERSION AS OF 0'
 for side in ['source','target']:
  assert c.sql('endpoint-'+side,f'SELECT count(*) FROM {edge} e LEFT ANTI JOIN {node} n ON e.source_system=n.source_system AND e.{side}_type=n.type_id AND e.{side}_id=n.id')==[['0']]
 for role,typ in [('object_current','type_id'),('edge_current','rel_type_id')]:
  t=a['tables'][role]['table'];assert c.sql('unique-'+role,f"SELECT count(*) FROM (SELECT source_system,{typ},id FROM {t} VERSION AS OF 0 GROUP BY ALL HAVING count(*)>1)")==[['0']]
 metrics();a['active_role_bytes']=sum(t['bytes'] for t in a['tables'].values());a['state']='All five native materialized roles pass independent full-row digests and typed identity/endpoint checks';a['qualification']='Full4096nodes20480edges bootstrap, all field UTF8 length/hex encoding and sorted SHA256 multiset against Python oracle. Cryptographic preservation evidence, not collision-free direct full-value comparisons or full native source replay. No declaration of NOT NULL/unique constraints, manifest publication, real producer fencing, runtime scale or SLO claim. Native storage telemetry not complete dollar price.';save();print(json.dumps(a,indent=2))
except Exception as e:a.update(state='Stopped; inspect exact persisted native handles before any retry',error=str(e));save();raise
