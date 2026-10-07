"""Canonical publication structural parity and16MiB forward projection."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_canonical_adjacency_r120'
assert not (O/'statements.jsonl').exists(),'Inspect prior handles before repeating'
F='client_dev.ashlar_entropy_20261006_r86';E=F+'.edge_current';A=F+'.adjacency_canonical_r120'
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
manifest=c.sql('manifest',f"SELECT publication_id,table_versions_json FROM {F}.publication_manifest_r89 WHERE publication_id IN ('r101-b1','r103-b1') ORDER BY publication_id")
assert len(manifest)==2,manifest
vectors={r[0]:json.loads(r[1]) for r in manifest}
# Verify exact named table identity/version anchors from actual descriptors.
assert vectors['r103-b1'][E]==16 and vectors['r101-b1'][E]==13,vectors
assert vectors['r103-b1'][F+'.object_current']==0
cols='source_system,rel_type_id,id edge_id,source_type,source_id,target_type,target_id'
def projection(v):return f'SELECT {cols},cast(1 AS BIGINT) structural_version FROM {E} VERSION AS OF {v}'
assert c.sql('structural-reuse-proof',f'SELECT count(*) FROM (({projection(13)} EXCEPT ALL {projection(16)}) UNION ALL ({projection(16)} EXCEPT ALL {projection(13)}))')==[['0']]
assert c.sql('absence',f"SHOW TABLES IN {F} LIKE 'adjacency_canonical_r120'")==[]
c.sql('create',f"CREATE TABLE {A} (source_system STRING NOT NULL,rel_type_id BIGINT NOT NULL,edge_id BIGINT NOT NULL,source_type BIGINT NOT NULL,source_id BIGINT NOT NULL,target_type BIGINT NOT NULL,target_id BIGINT NOT NULL,structural_version BIGINT NOT NULL) USING DELTA CLUSTER BY (source_system,source_type,source_id,rel_type_id) TBLPROPERTIES ('delta.targetFileSize'='16777216','delta.parquet.compression.codec'='zstd','delta.dataSkippingStatsColumns'='source_system,source_type,source_id,rel_type_id,target_type,target_id,edge_id')")
c.sql('build',f'INSERT INTO {A} {projection(16)}')
v=int(c.sql('version',f'DESCRIBE HISTORY {A} LIMIT 1')[0][0])
assert c.sql('full-parity',f'SELECT count(*) FROM ((SELECT * FROM {A} VERSION AS OF {v} EXCEPT ALL {projection(16)}) UNION ALL ({projection(16)} EXCEPT ALL SELECT * FROM {A} VERSION AS OF {v}))')==[['0']]
assert c.sql('identities',f'SELECT count(*),count(DISTINCT edge_id),count(DISTINCT struct(source_system,rel_type_id,source_type,source_id,target_type,target_id)) FROM {A} VERSION AS OF {v}')==[['20000000','20000000','20000000']]
assert c.sql('closure',f'''SELECT count(*) FROM {A} VERSION AS OF {v} a LEFT JOIN {F}.object_current VERSION AS OF 0 s ON a.source_system=s.source_system AND a.source_type=s.type_id AND a.source_id=s.id LEFT JOIN {F}.object_current VERSION AS OF 0 t ON a.source_system=t.source_system AND a.target_type=t.type_id AND a.target_id=t.id WHERE s.id IS NULL OR t.id IS NULL''')==[['0']]
keys=c.sql('endpoint-keys',f'SELECT DISTINCT source_system,source_type,source_id FROM {A} VERSION AS OF {v} ORDER BY source_system,source_type,source_id LIMIT 10');assert len(keys)==10
for index,(source,typ,node) in enumerate(keys):
 where=f"source_system='{source}' AND source_type={typ} AND source_id={node}"
 expected=c.sql('oracle-'+str(index),f'SELECT * FROM ({projection(16)}) WHERE {where} ORDER BY rel_type_id,edge_id LIMIT 100')
 assert expected
 for i in range(3):assert c.sql(f'page-{index}-{i}',f'SELECT * FROM {A} VERSION AS OF {v} WHERE {where} ORDER BY rel_type_id,edge_id LIMIT 100')==expected
rows=c.sql('detail',f'DESCRIBE DETAIL {A}');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];detail=dict(zip(names,rows[0]))
(O/'summary.json').write_text(json.dumps({'state':'20M canonical projection, typed closure, identities, structural reuse proof and30 exact pages passed; final metrics pending','table':A,'version':v,'canonical_vectors':vectors,'physical_detail':detail,'coverage':'All canonical E16 independent edges, forward direction, all source and relationship IDs; structural revision1. Exact structural equivalence to E13 proven.','qualification':'Synthetic canonical native publication descriptors r101/r103. New projection is a candidate not activated in either immutable descriptor. No real producer authority, concurrent fencing, reverse/degree coverage, incremental structural publication or billion admission. NOT NULL shape explicitly created.'},indent=2)+'\n')
c.history();c.close();print('Completed canonical adjacency build and reuse proof')
