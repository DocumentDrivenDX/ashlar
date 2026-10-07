"""Bounded stored-carrier replay: clustering-on-write versus no clustering."""
import json,time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_append_layout_r137'
assert not (O/'statements.jsonl').exists(),'Inspect saved handles before repeating'
F='client_dev.ashlar_entropy_20261006_r86'
sources={'raw':(F+'.source_record_r89',16,['source_feed','source_epoch','delivery_id']),
         'journal':(F+'.property_journal_r89',18,['source_feed','source_epoch','source_position','id'])}
c=DriverClient(O);j=DriverClient(O/'journal-lane')
for client in (c,j):
 client.sql('timeout','SET STATEMENT_TIMEOUT=180')
 assert client.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
report={'state':'running stored-carrier replay','scope':'Existing warehouse, four100k-row outputs, approximately2.6GB expected writes. Two sequential parallel pairs; one pair each layout. Same already-serialized immutable inputs, so this isolates append layout from source serialization and does not prove live ingest throughput. Canonical tables/publication unchanged.','runs':[],'row_tracking':'Explicitly enabled on both layouts'}
def save():(O/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
save()
for mode in ('clustered','unclustered'):
 targets={}
 for role,(source,version,keys) in sources.items():
  target=F+f'.append_{role}_{mode}_r137';targets[role]=target
  assert c.sql(mode+'-'+role+'-absence',f"SHOW TABLES IN {F} LIKE 'append_{role}_{mode}_r137'")==[]
  c.sql(mode+'-'+role+'-create',f'CREATE TABLE {target} USING DELTA AS SELECT * FROM {source} VERSION AS OF {version} WHERE false')
  c.sql(mode+'-'+role+'-codec',f"ALTER TABLE {target} SET TBLPROPERTIES ('delta.parquet.compression.codec'='zstd','delta.dataSkippingStatsColumns'='{','.join(keys+(['apply_batch_id'] if role=='journal' else []))}')")
  c.sql(mode+'-'+role+'-tracking',f"ALTER TABLE {target} SET TBLPROPERTIES ('delta.enableRowTracking'='true')")
  if mode=='clustered':c.sql(mode+'-'+role+'-cluster',f"ALTER TABLE {target} CLUSTER BY ({','.join(keys)})")
  before=c.sql(mode+'-'+role+'-preflight',f'DESCRIBE DETAIL {target}')
  names=[col['name'] for col in c.records[-1]['response']['manifest']['schema']['columns']]
  metadata=dict(zip(names,before[0]));assert json.loads(metadata['properties'])['delta.enableRowTracking']=='true'
  assert 'rowTracking' in json.loads(metadata['tableFeatures'])
  assert json.loads(metadata['clusteringColumns'])==(keys if mode=='clustered' else [])
 start=time.monotonic()
 with ThreadPoolExecutor(max_workers=2) as pool:
  futures=[]
  for role,client in (('raw',c),('journal',j)):
   source,version,_=sources[role]
   futures.append(pool.submit(client.sql,mode+'-'+role+'-append',f"INSERT INTO {targets[role]} SELECT * FROM {source} VERSION AS OF {version} WHERE apply_batch_id='r133-b1'"))
  for future in futures:future.result()
 row={'mode':mode,'append_pair_wall_s':time.monotonic()-start,'targets':targets,'checks':{}}
 report['runs'].append(row);save();print(mode,'append pair',row['append_pair_wall_s'],flush=True)
 for role,(source,version,_) in sources.items():
  target=targets[role]
  assert c.sql(mode+'-'+role+'-count',f'SELECT count(*) FROM {target}')==[['100000']]
  # Normalize all strings to UTF8 bytes for exact retained lexical content.
  schema=c.sql(mode+'-'+role+'-schema',f'DESCRIBE TABLE {source}')
  cols=[]
  for name,typ,*_ in schema:
   if not name or name.startswith('#'):break
   cols.append(f"hex(encode({name},'UTF-8')) AS {name}" if typ.lower()=='string' else name)
  fields=','.join(cols)
  expected=f"SELECT {fields} FROM {source} VERSION AS OF {version} WHERE apply_batch_id='r133-b1'"
  actual=f'SELECT {fields} FROM {target}'
  assert c.sql(mode+'-'+role+'-exact',f'SELECT count(*) FROM (({expected} EXCEPT ALL {actual}) UNION ALL ({actual} EXCEPT ALL {expected}))')==[['0']]
  detail=c.sql(mode+'-'+role+'-detail',f'DESCRIBE DETAIL {target}')
  names=[col['name'] for col in c.records[-1]['response']['manifest']['schema']['columns']]
  row['checks'][role]={'count':100000,'exact_symmetric_difference':0,'detail':dict(zip(names,detail[0]))}
  save()
report['state']='Both stored-carrier append layouts and complete UTF8-exact raw/journal preservation passed; final metrics pending'
save();c.history();j.history();c.close();j.close();print(report['state'])
