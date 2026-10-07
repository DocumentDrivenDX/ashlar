"""20M-edge synthetic hub adjacency layout; canonical E/N remain untouched."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent
O=B/'out/native/ashlar_hub_adjacency_r108'
assert not (O/'statements.jsonl').exists(),'Inspect handles; never blindly repeat CTAS'
F='client_dev.ashlar_entropy_20261006_r86';A=F+'.adjacency_hub_r108'
c=DriverClient(O)
c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
assert c.sql('absence',f"SHOW TABLES IN {F} LIKE 'adjacency_hub_r108'")==[]
projection=f'''SELECT source_system,id edge_id,
CASE WHEN id<=5000000 THEN cast(9000+pmod(id-4000000,32)+1 AS BIGINT) ELSE rel_type_id END rel_type_id,
CASE WHEN id<=5000000 THEN cast(1 AS BIGINT) ELSE source_type END source_type,
CASE WHEN id<=5000000 THEN cast(CASE WHEN source_system='pilot' THEN 32 ELSE 160 END AS BIGINT) ELSE source_id END source_id,
CASE WHEN id<=5000000 THEN cast(pmod(id-4000000,32)+1 AS BIGINT) ELSE target_type END target_type,
CASE WHEN id<=5000000 THEN id-4000000 ELSE target_id END target_id
FROM {F}.edge_current VERSION AS OF 0'''
c.sql('create',f"CREATE TABLE {A} USING DELTA CLUSTER BY (source_system,source_type,source_id) TBLPROPERTIES ('delta.targetFileSize'='67108864','delta.parquet.compression.codec'='zstd') AS {projection}")
version=int(c.sql('version',f'DESCRIBE HISTORY {A} LIMIT 1')[0][0])
assert version==0
assert c.sql('all-identities',f'SELECT count(*),count(DISTINCT edge_id) FROM {A} VERSION AS OF 0')==[['20000000','20000000']]
assert c.sql('full-projection-parity',f'SELECT count(*) FROM ((SELECT * FROM {A} VERSION AS OF 0 EXCEPT ALL {projection}) UNION ALL ({projection} EXCEPT ALL SELECT * FROM {A} VERSION AS OF 0))')==[['0']]
assert c.sql('typed-closure',f'''SELECT count(*) FROM {A} VERSION AS OF 0 a LEFT JOIN {F}.object_current VERSION AS OF 0 s
ON a.source_system=s.source_system AND a.source_type=s.type_id AND a.source_id=s.id
LEFT JOIN {F}.object_current VERSION AS OF 0 t ON a.source_system=t.source_system AND a.target_type=t.type_id AND a.target_id=t.id
WHERE s.id IS NULL OR t.id IS NULL''')==[['0']]
assert c.sql('unique-endpoint-pairs',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,source_type,source_id,target_type,target_id)) FROM {A} VERSION AS OF 0')==[['20000000','20000000']]
for source,hub,count in [('pilot',32,900004),('other',160,100004)]:
 for i in range(5):
  assert c.sql(f'count-{source}-{i}',f"SELECT count(*) FROM {A} VERSION AS OF 0 WHERE source_system='{source}' AND source_type=1 AND source_id={hub}")==[[str(count)]]
 for i in range(5):
  rows=c.sql(f'page-{source}-{i}',f"SELECT edge_id,rel_type_id,target_type,target_id FROM {A} VERSION AS OF 0 WHERE source_system='{source}' AND source_type=1 AND source_id={hub} AND edge_id>4000000 ORDER BY edge_id LIMIT 100")
  assert len(rows)==100 and len({r[0] for r in rows})==100
  expected=[n for n in range(1,1200) if (n%10!=0)==(source=='pilot')][:100]
  assert rows==[[str(4000000+n),str(9000+n%32+1),str(n%32+1),str(n)] for n in expected]
detail=c.sql('detail',f'DESCRIBE DETAIL {A}')
(O/'summary.json').write_text(json.dumps({'state':'20M synthetic hub adjacency preservation and bounded queries passed; final-history audit pending','table':A,'version':version,'hub_degrees':{'pilot32':900004,'other160':100004},'detail':detail,'qualification':'Synthetic altered endpoint fixture; not a projection of the canonical published graph. Canonical E/N unchanged. Typed closure and unique relation endpoint pairs checked; no source authority, general skew, full adjacency service or billion-scale admission.'},indent=2)+'\n')
c.history();c.close()
print('Completed hub adjacency experiment')
