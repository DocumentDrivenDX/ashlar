"""Prepare pinned structural lifecycle fixture and exact degree baseline; no publication."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';N='client_dev.ashlar_composite_20261005_p1r1.object_current';out=B/'out/native/ashlar_structural_prepare_20261005_r1';c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
assert int(c.sql('edge-version',f'DESCRIBE HISTORY {F}.edge_current LIMIT 1')[0][0])==3
assert int(c.sql('adjacency-version',f'DESCRIBE HISTORY {F}.adjacency LIMIT 1')[0][0])==0
c.sql('degree-baseline',f"CREATE TABLE {F}.out_degree_r1 USING DELTA CLUSTER BY (source_system,rel_type_id,source_type,source_id) TBLPROPERTIES ('delta.feature.catalogManaged'='supported') AS SELECT source_system,rel_type_id,source_type,source_id,count(*) out_degree FROM {F}.adjacency VERSION AS OF 0 GROUP BY source_system,rel_type_id,source_type,source_id")
assert c.sql('degree-cardinality',f'SELECT count(*),sum(out_degree),min(out_degree),max(out_degree) FROM {F}.out_degree_r1')==[['2000000','10000000','5','5']]
c.sql('delete-stage',f"CREATE TABLE {F}.delete_r1 USING DELTA AS SELECT * FROM {F}.edge_current VERSION AS OF 3 WHERE source_system='pilot:0' AND rel_type_id=7 AND id<=20")
c.sql('insert-stage',f"CREATE TABLE {F}.insert_r1 USING DELTA AS SELECT 'pilot:0' source_system,cast(7 AS BIGINT) rel_type_id,1000000+id id,cast(1 AS BIGINT) source_type,cast(200001 AS BIGINT) source_id,cast(1 AS BIGINT) target_type,id target_id,'r1' schema_revision,cast(2 AS BIGINT) entity_version,'{{\"201\":\"hub-fixture\"}}' props_json,'{{\"future\":{{\"preserve\":true}}}}' retained_json,cast(NULL AS STRING) order_key,'fixture-edge-structure' source_feed,'e' source_epoch,cast(1 AS BIGINT) source_position,current_timestamp() published_at,sha2(to_json(named_struct('source_system','pilot:0','rel_type_id',cast(7 AS BIGINT),'id',1000000+id)),256) lookup_hash FROM range(1,20002)")
assert c.sql('insert-keys',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {F}.insert_r1')==[['20001','20001']]
assert c.sql('delete-keys',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {F}.delete_r1')==[['20','20']]
assert c.sql('new-identity-disjoint',f'SELECT count(*) FROM {F}.insert_r1 i JOIN {F}.edge_current VERSION AS OF 3 e ON i.source_system=e.source_system AND i.rel_type_id=e.rel_type_id AND i.id=e.id')==[['0']]
for side in ['source','target']:
 assert c.sql(side+'-endpoint-guard',f'SELECT count(*) FROM {F}.insert_r1 i LEFT ANTI JOIN {N} VERSION AS OF 3 n ON i.source_system=n.source_system AND i.{side}_type=n.type_id AND i.{side}_id=n.id')==[['0']]
# Independently account for deleted parallel identities, then compute projected hub work.
assert c.sql('removed-multiplicity',f"SELECT count(*) FROM {F}.delete_r1 a JOIN {F}.edge_current VERSION AS OF 3 b ON a.source_system=b.source_system AND a.rel_type_id=b.rel_type_id AND a.source_type=b.source_type AND a.source_id=b.source_id AND a.target_type=b.target_type AND a.target_id=b.target_id AND a.id<>b.id")==[['20']]
assert c.sql('hub-logical-work',f"SELECT count(*)+sum(d.out_degree) FROM {F}.insert_r1 i JOIN {F}.out_degree_r1 d ON i.source_system=d.source_system AND i.rel_type_id=d.rel_type_id AND i.target_type=d.source_type AND i.target_id=d.source_id")==[['120006']]
c.sql('degree-detail',f'DESCRIBE DETAIL {F}.out_degree_r1')
(out/'summary.json').write_text(json.dumps(dict(state='prepared',edge_version=3,adjacency_version=0,node_version=3,degree_version=0,insert_count=20001,delete_count=20,hub_logical_work_before_deletion=120006,scope='synthetic lifecycle stages and degree baseline only; no publication or rate admission'),indent=2))
c.history();c.close();print('Structural preparation passed',flush=True)
