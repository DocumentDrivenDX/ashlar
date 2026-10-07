"""Higher-entropy narrow adjacency screen, not a production distribution claim."""
import json,math
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_hub_entropy_r116'
assert not (O/'statements.jsonl').exists(),'Inspect prior handles before repeating'
F='client_dev.ashlar_entropy_20261006_r86';A=F+'.adjacency_entropy_r116'
assert math.gcd(104729,4000000)==1
projection="""SELECT CASE WHEN pmod(target_id,10)=0 THEN 'other' ELSE 'pilot' END source_system,
cast(9000+32*cast(floor((ordinal-1)/4000000) AS BIGINT)+pmod(xxhash64(target_id),32) AS BIGINT) rel_type_id,
ordinal+4000000 edge_id,cast(1 AS BIGINT) source_type,
cast(CASE WHEN pmod(target_id,10)=0 THEN 160 ELSE 32 END AS BIGINT) source_id,
pmod(target_id,32)+1 target_type,target_id,cast(1 AS BIGINT) structural_version
FROM (SELECT id+1 ordinal,pmod(id*104729,4000000)+1 target_id FROM range(20000000))"""
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
assert c.sql('absence',f"SHOW TABLES IN {F} LIKE 'adjacency_entropy_r116'")==[]
c.sql('create',f"CREATE TABLE {A} USING DELTA CLUSTER BY (source_system,source_type,source_id,rel_type_id) TBLPROPERTIES ('delta.targetFileSize'='67108864','delta.parquet.compression.codec'='zstd','delta.dataSkippingStatsColumns'='source_system,source_type,source_id,rel_type_id,target_type,target_id,edge_id','delta.autoOptimize.autoCompact'='false','delta.autoOptimize.optimizeWrite'='false') AS {projection} WHERE false")
for batch in range(8):
 lower=9000+20*batch
 c.sql('append-'+str(batch),f'INSERT INTO {A} SELECT * FROM ({projection}) WHERE rel_type_id>={lower} AND rel_type_id<{lower+20} ORDER BY xxhash64(edge_id)')
versions={};details={}
def check(phase):
 v=int(c.sql(phase+'-version',f'DESCRIBE HISTORY {A} LIMIT 1')[0][0]);versions[phase]=v
 assert c.sql(phase+'-parity',f'SELECT count(*) FROM ((SELECT * FROM {A} VERSION AS OF {v} EXCEPT ALL {projection}) UNION ALL ({projection} EXCEPT ALL SELECT * FROM {A} VERSION AS OF {v}))')==[['0']]
 assert c.sql(phase+'-identity',f'SELECT count(*),count(DISTINCT edge_id),count(DISTINCT struct(source_system,rel_type_id,source_type,source_id,target_type,target_id)) FROM {A} VERSION AS OF {v}')==[['20000000','20000000','20000000']]
 for source,hub in [('pilot',32),('other',160)]:
  for rel,edge in [(9000,4000000),(9080,14000000),(9150,22000000)]:
   where=f"source_system='{source}' AND source_type=1 AND source_id={hub} AND (rel_type_id>{rel} OR (rel_type_id={rel} AND edge_id>{edge}))"
   expected=c.sql(f'oracle-{phase}-{source}-{rel}',f'SELECT * FROM ({projection}) WHERE {where} ORDER BY rel_type_id,edge_id LIMIT 100');assert len(expected)==100
   for i in range(3):assert c.sql(f'page-{phase}-{source}-{rel}-{i}',f'SELECT * FROM {A} VERSION AS OF {v} WHERE {where} ORDER BY rel_type_id,edge_id LIMIT 100')==expected
 rows=c.sql(phase+'-detail',f'DESCRIBE DETAIL {A}');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];details[phase]=dict(zip(names,rows[0]))
check('built')
v=versions['built']
assert c.sql('typed-closure',f'''SELECT count(*) FROM {A} VERSION AS OF {v} a LEFT JOIN {F}.object_current VERSION AS OF 0 s ON a.source_system=s.source_system AND a.source_type=s.type_id AND a.source_id=s.id LEFT JOIN {F}.object_current VERSION AS OF 0 t ON a.source_system=t.source_system AND a.target_type=t.type_id AND a.target_id=t.id WHERE s.id IS NULL OR t.id IS NULL''')==[['0']]
c.sql('optimize',f'OPTIMIZE {A}');check('optimized')
rows=c.sql('delta-history',f'DESCRIBE HISTORY {A}');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];(O/'delta-history.json').write_text(json.dumps([dict(zip(names,r)) for r in rows],indent=2)+'\n')
(O/'summary.json').write_text(json.dumps({'state':'20M parity, identities, typed closure and36 exact full-shape pages passed; final metrics pending','table':A,'versions':versions,'details':details,'fixture':'Affine bijection on4M destinations, hash-distributed32 relationship buckets per hop,90/10 source hubs, random hash write order within20-relationship append ranges;8 appends. Valid target types/source custody derived from destination identity.','qualification':'Higher entropy than periodic r113, not a production distribution. Full projection oracle uses native hash semantics; no independent Python hash oracle, real producer/publication, reverse access or billion-scale admission.'},indent=2)+'\n')
c.history();c.close();print('Completed higher-entropy20M adjacency screen')
