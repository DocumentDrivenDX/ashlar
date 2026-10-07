"""Owned small native role materialization, exact full-field round trips."""
import json,time,hashlib
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_mixed_r220';assert not O.exists()
from scale_mixed_r219 import Workload,SEED
from collections import defaultdict
w=Workload(128,640);roles=defaultdict(list)
for kind,count in [('node',128),('edge',640)]:
 for ordinal in range(count):
  for role,row in w.roles(kind,ordinal):roles[role].append(row)
F='client_dev.ashlar_entropy_20261006_r86';c=BoundedReads(O,socket_timeout=60);a={'state':'running','tables':{},'scope':'128 nodes640edges distinct mixed bootstrap; five roles, client-generated CTAS, not native range generator or manifest publication'}
ints={'type_id','rel_type_id','id','source_type','source_id','target_type','target_id','entity_version','root_id','source_position','property_id','event_ordinal'};bools={'old_present','new_present'};times={'published_at','received_at'}
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def literal(v):
 if v is None:return 'NULL'
 return "'"+str(v).encode().hex()+"'"
def metrics():
 c.cursor.close();c.cursor=c.connection.cursor()
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']<=1000000000 and a['costs']['write_remote_bytes']<=100000000
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=60')
 for name,rows in roles.items():
  table=F+'.mixed_'+name+'_r220';assert c.sql('absent-'+name,f"SHOW TABLES IN {F} LIKE 'mixed_{name}_r220'")==[]
  fields=list(rows[0]);assert all(list(r)==fields for r in rows)
  values=','.join('('+','.join(literal(r[f]) for f in fields)+')' for r in rows)
  types={f:'BIGINT' if f in ints else 'BOOLEAN' if f in bools else 'TIMESTAMP' if f in times else 'STRING' for f in fields}
  projection=','.join(f"CAST(decode(unhex({f}),'UTF-8') AS {types[f]}) AS {f}" for f in fields)
  cluster='lookup_hash' if name.endswith('current') else 'delivery_id' if name=='source_record' else 'source_id' if name=='adjacency_forward' else 'source_delivery_id'
  c.sql('create-'+name,f"CREATE TABLE {table} USING DELTA CLUSTER BY ({cluster}) TBLPROPERTIES ('delta.parquet.compression.codec'='zstd') AS SELECT {projection} FROM VALUES {values} AS v({','.join(fields)})")
  detail=c.sql('detail-'+name,'DESCRIBE DETAIL '+table);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];d=dict(zip(names,detail[0]));a['tables'][name]={'table':table,'id':d['id'],'version':0,'rows':len(rows),'files':int(d['numFiles']),'bytes':int(d['sizeInBytes']),'reader':d['minReaderVersion'],'writer':d['minWriterVersion']};save()
  # Compare multisets of every field, including timestamp display normalized only.
  q=','.join('CAST('+f+' AS STRING) AS '+f if f in times else f for f in fields)
  actual=c.sql('exact-'+name,f'SELECT {q} FROM {table} VERSION AS OF 0')
  def expected(r):return [None if r[f] is None else r[f].replace('T',' ').removesuffix('Z') if f in times else str(r[f]) for f in fields]
  from collections import Counter
  assert Counter(tuple(r) for r in actual)==Counter(tuple(expected(r)) for r in rows)
  metrics()
 a['state']='All five distinct-payload native roles match every expected field exactly';a['qualification']='Small CTAS synthetic evidence, no NOT NULL/unique constraints or full DDL package qualification. Bootstrap current/raw/journal/forward adjacency complete for128nodes640edges. Python-generated input, not a native SQL generation differential. Timestamp display normalized to native string; does not expand timestamp-token semantics. Lexical bags, old/new tokens, flags, signed BIGINT identities, retained/raw envelopes and cursor strings exact. Native byte/file widths include tiny-table overhead and cannot extrapolate to scale.';save();print(json.dumps(a,indent=2))
except Exception as e:a.update(state='Stopped; inspect existing native handles, no write retry',error=str(e));save();raise
finally:c.close()
