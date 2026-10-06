"""Bounded multi-file identity-pruning comparison on existing varied payloads.
Small target files/partitions are diagnostic interventions, not production advice.
"""
import json,math
from pathlib import Path
from persistent_sql import Client
BASE=Path(__file__).resolve().parent;schema='ashlar_pruning_20261005_c1';F='client_dev.'+schema;out=BASE/'out/native'/schema;c=Client(out)
c.sql('schema',f"CREATE SCHEMA {F} COMMENT 'Ashlar synthetic file pruning diagnostic; no production data'")
c.sql('environment','SELECT current_version()')
source="SELECT 'pilot' source_system,cast(1 AS BIGINT) type_id,id,props_json,retained_json,pmod(id,64) routing_bucket FROM client_dev.ashlar_ingest_20261005_a2.serving VERSION AS OF 3"
variants=[('liquid_16m','CLUSTER BY (id)','16777216'),('liquid_128m','CLUSTER BY (id)','134217728'),('bucket64_z','PARTITIONED BY (routing_bucket)','134217728')]
for name,layout,size in variants:
 c.sql('create-'+name,f"CREATE TABLE {F}.{name} USING DELTA {layout} TBLPROPERTIES ('delta.dataSkippingStatsColumns'='source_system,type_id,id','delta.targetFileSize'='{size}') AS {source}")
 c.sql('optimize-'+name,f'OPTIMIZE {F}.{name}'+(' ZORDER BY (id)' if name.endswith('_z') else ' FULL'))
 assert c.sql('count-'+name,f'SELECT count(*),count(DISTINCT id) FROM {F}.{name}')==[['1000000','1000000']]
 # Exact whole-carrier parity across the entire seed; no hash-only proof.
 assert c.sql('parity-'+name,f"SELECT count(*) FROM {F}.{name} t JOIN client_dev.ashlar_ingest_20261005_a2.serving VERSION AS OF 3 s ON t.id=s.id WHERE t.props_json<>s.props_json OR t.retained_json<>s.retained_json")==[['0']]
 c.sql('detail-'+name,f'DESCRIBE DETAIL {F}.{name}')
 print('prepared',name,flush=True)
measures=[]
for rep in range(26):
 key=1+(rep*7919)%600000
 expected=None
 for name,_,_ in variants:
  extra=f' AND routing_bucket={key%64}' if name=='bucket64_z' else ''
  rows=c.sql(f'measure-{name}-{rep}',f"SELECT id,props_json,retained_json,uuid() FROM {F}.{name} WHERE source_system='pilot' AND type_id=1 AND id={key}{extra}")
  assert len(rows)==1 and rows[0][0]==str(key)
  clean=rows[0][:-1]
  if expected is None:expected=clean
  else:assert expected==clean
  if rep:measures.append(c.records[-1])
 print('round',rep,flush=True)
history=c.history()
print('Statements complete; refresh asynchronous metrics if needed and run summarize_pruning.py',flush=True)
