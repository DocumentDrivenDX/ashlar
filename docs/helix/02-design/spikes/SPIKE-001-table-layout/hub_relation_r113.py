"""Actual CONTRACT-003 (relationship,edge) ordering over pinned hub fixtures."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_hub_contract_pages_r113'
assert not (O/'statements.jsonl').exists(),'Inspect prior handles before repeating'
F='client_dev.ashlar_entropy_20261006_r86'
s=json.loads((B/'out/native/ashlar_hub_ordering_r111/audited-summary.json').read_text())
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
A=F+'.adjacency_relation_r113';baseline=F+'.adjacency_allhub_r110_endpoint'
assert c.sql('absence',f"SHOW TABLES IN {F} LIKE 'adjacency_relation_r113'")==[]
projection=f'SELECT *,cast(1 AS BIGINT) structural_version FROM {baseline} VERSION AS OF 0'
c.sql('create',f"CREATE TABLE {A} USING DELTA CLUSTER BY (source_system,source_type,source_id,rel_type_id) TBLPROPERTIES ('delta.dataSkippingStatsColumns'='source_system,source_type,source_id,rel_type_id,target_type,target_id,edge_id','delta.autoOptimize.autoCompact'='false','delta.autoOptimize.optimizeWrite'='false') AS {projection} WHERE false")
for batch in range(8):
 lower=9005+20*batch;upper=lower+20
 c.sql('append-'+str(batch),f'INSERT INTO {A} {projection} WHERE rel_type_id>={lower} AND rel_type_id<{upper}')
v=int(c.sql('version',f'DESCRIBE HISTORY {A} LIMIT 1')[0][0])
assert c.sql('parity',f'SELECT count(*) FROM ((SELECT * FROM {A} VERSION AS OF {v} EXCEPT ALL {projection}) UNION ALL ({projection} EXCEPT ALL SELECT * FROM {A} VERSION AS OF {v}))')==[['0']]
assert c.sql('identities',f'SELECT count(*),count(DISTINCT edge_id),min(structural_version),max(structural_version) FROM {A} VERSION AS OF {v}')==[['20000000','20000000','1','1']]
files=c.sql('hub-file-count',f'SELECT source_system,count(DISTINCT _metadata.file_path) FROM {A} VERSION AS OF {v} GROUP BY source_system ORDER BY source_system')
assert len(files)==2 and all(int(row[1])>=2 for row in files)
s['tables']={'edge_range_control':s['tables']['endpoint'],'relation_range':A}
s['versions']={'edge_range_control':s['versions']['endpoint'],'relation_range':v}

for source,hub in [('pilot',32),('other',160)]:
 for rel,edge in [(9005,4000000),(9085,14000000),(9155,22000000)]:
  expected=[]
  for relationship in range(rel,9165):
   typ,hop=divmod(relationship-9000,5)
   if not 1<=typ<=32:continue
   lower=hop*4000000+1;upper=(hop+1)*4000000
   ordinal=lower+((typ-1-lower)%32)
   while ordinal<=upper and len(expected)<100:
    identity=4000000+ordinal
    if (relationship,identity)>(rel,edge) and (ordinal%10!=0)==(source=='pilot'):
     expected.append([str(identity),str(relationship),str(typ),str((ordinal-1)%4000000+1)])
    ordinal+=32
   if len(expected)==100:break
  assert len(expected)==100
  for i in range(3):
   for mode in (('edge_range_control','relation_range') if i%2==0 else ('relation_range','edge_range_control')):
    table=s['tables'][mode];version=s['versions'][mode]
    query=f"SELECT edge_id,rel_type_id,target_type,target_id FROM {table} VERSION AS OF {version} WHERE source_system='{source}' AND source_type=1 AND source_id={hub} AND (rel_type_id>{rel} OR (rel_type_id={rel} AND edge_id>{edge})) ORDER BY rel_type_id,edge_id LIMIT 100"
    assert c.sql(f'page-{source}-{rel}-{mode}-{i}',query)==expected
detail=c.sql('detail',f'DESCRIBE DETAIL {A}');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];physical=dict(zip(names,detail[0]))
(O/'summary.json').write_text(json.dumps({'state':'36 contract-order page checks passed; final-history audit pending','tables':s['tables'],'versions':s['versions'],'physical_detail':physical,'qualification':'Full eight-column synthetic adjacency candidate with exact baseline parity and constant structural_version1; no canonical publication binding or production NOT NULL enforcement. Relation-range appends differ from edge-range control, whose schema has seven columns. Exact lexicographic page oracle unchanged; no production entropy, ingest, reverse traversal or billion-scale admission.'},indent=2)+'\n')
c.history();c.close();print('Passed 36 exact contract-order page checks')
