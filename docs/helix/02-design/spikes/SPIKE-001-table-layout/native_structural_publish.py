"""Bounded synthetic edge lifecycle publication; no sustained rate or source adapter claim."""
import json,time
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';N='client_dev.ashlar_composite_20261005_p1r1.object_current';out=B/'out/native/ashlar_structural_publish_20261005_r3';c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
assert int(c.sql('edge-before',f'DESCRIBE HISTORY {F}.edge_current LIMIT 1')[0][0])==3
assert int(c.sql('adj-before',f'DESCRIBE HISTORY {F}.adjacency LIMIT 1')[0][0])==0
c.sql('adj-transaction-feature',f"ALTER TABLE {F}.adjacency SET TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
c.sql('tombstone-ddl',f"CREATE TABLE {F}.tombstone_r3 (source_system STRING,entity_kind STRING,type_id BIGINT,id BIGINT,entity_version BIGINT,source_feed STRING,source_epoch STRING,source_position BIGINT) USING DELTA CLUSTER BY (source_system,type_id,id) TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
c.sql('affected-groups',f'CREATE TABLE {F}.affected_r3 USING DELTA AS SELECT DISTINCT source_system,rel_type_id,source_type,source_id FROM (SELECT * FROM {F}.delete_r1 UNION ALL SELECT * FROM {F}.insert_r1)')
cols=['source_system','rel_type_id','id','source_type','source_id','target_type','target_id','schema_revision','entity_version','props_json','retained_json','order_key','source_feed','source_epoch','source_position','published_at','lookup_hash'];adj=cols[:7]
def key(a,b):return ' AND '.join(f'{a}.{x}={b}.{x}' for x in ['source_system','rel_type_id','id'])
def group(a,b):return ' AND '.join(f'{a}.{x}={b}.{x}' for x in ['source_system','rel_type_id','source_type','source_id'])
def carrier():return "to_json(named_struct("+','.join("'"+x+"',"+x for x in cols)+"),map('ignoreNullFields','false'))"
old=carrier();start=time.time()
stmt=f'''BEGIN ATOMIC
 IF (SELECT count(*) FROM {F}.delete_r1 d JOIN {F}.edge_current e ON {key('d','e')})<>20 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='delete cardinality'; END IF;
 IF (SELECT count(*) FROM {F}.insert_r1 i JOIN {F}.edge_current e ON {key('i','e')})<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='insert collision'; END IF;
 DELETE FROM {F}.edge_current e WHERE EXISTS (SELECT 1 FROM {F}.delete_r1 d WHERE {key('d','e')});
 INSERT INTO {F}.edge_current SELECT * FROM {F}.insert_r1;
 DELETE FROM {F}.adjacency e WHERE EXISTS (SELECT 1 FROM {F}.delete_r1 d WHERE {key('d','e')});
 INSERT INTO {F}.adjacency SELECT {','.join(adj)} FROM {F}.insert_r1;
 DELETE FROM {F}.out_degree_r1 d WHERE EXISTS (SELECT 1 FROM {F}.affected_r3 a WHERE {group('a','d')});
 INSERT INTO {F}.out_degree_r1 SELECT e.source_system,e.rel_type_id,e.source_type,e.source_id,count(*) FROM {F}.adjacency e JOIN {F}.affected_r3 a ON {group('a','e')} GROUP BY e.source_system,e.rel_type_id,e.source_type,e.source_id;
 INSERT INTO {F}.property_journal SELECT source_system,'edge',rel_type_id,id,cast(NULL AS BIGINT),2,'delete',true,{old},false,cast(NULL AS STRING),schema_revision,'fixture-edge-structure','e',1,id,'synthetic-structure:1',current_timestamp() FROM {F}.delete_r1;
 INSERT INTO {F}.property_journal SELECT source_system,'edge',rel_type_id,id,cast(NULL AS BIGINT),2,'insert',false,cast(NULL AS STRING),true,{old},schema_revision,'fixture-edge-structure','e',1,id,'synthetic-structure:1',current_timestamp() FROM {F}.insert_r1;
 INSERT INTO {F}.tombstone_r3 SELECT source_system,'edge',rel_type_id,id,2,'fixture-edge-structure','e',1 FROM {F}.delete_r1;
 IF (SELECT sum(out_degree) FROM {F}.out_degree_r1)<>10019981 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='degree sum'; END IF;
 IF (SELECT count(*) FROM {F}.property_journal WHERE source_feed='fixture-edge-structure')<>20021 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='lifecycle journal cardinality'; END IF;
 INSERT INTO {F}.receipt VALUES (3,current_timestamp()); END'''
c.sql('atomic-structure',stmt);apply_s=time.time()-start
versions={F+'.'+t:int(c.sql('version-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0]) for t in ['edge_current','adjacency','out_degree_r1','property_journal','tombstone_r3']};versions[N]=3
v=lambda t:versions[F+'.'+t]
assert c.sql('edge-keys',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {F}.edge_current VERSION AS OF {v("edge_current")}')==[['10019981','10019981']]
diff=' OR '.join(f'e.{x} IS DISTINCT FROM a.{x}' for x in adj)
assert c.sql('full-adjacency-parity',f'SELECT count(*),count_if({diff}) FROM {F}.edge_current VERSION AS OF {v("edge_current")} e FULL OUTER JOIN {F}.adjacency VERSION AS OF {v("adjacency")} a ON {key("e","a")}')==[['10019981','0']]
assert c.sql('full-degree-parity',f'WITH expected AS (SELECT source_system,rel_type_id,source_type,source_id,count(*) out_degree FROM {F}.adjacency VERSION AS OF {v("adjacency")} GROUP BY ALL) SELECT count_if(e.out_degree IS DISTINCT FROM d.out_degree) FROM expected e FULL OUTER JOIN {F}.out_degree_r1 VERSION AS OF {v("out_degree_r1")} d ON {group("e","d")}')==[['0']]
assert c.sql('insert-carrier-exact',f'SELECT count(*),count_if('+ ' OR '.join(f'i.{x} IS DISTINCT FROM e.{x}' for x in cols)+f') FROM {F}.insert_r1 i LEFT JOIN {F}.edge_current VERSION AS OF {v("edge_current")} e ON {key("i","e")}')==[['20001','0']]
assert c.sql('untouched-all-fields',f'SELECT count(*),count_if('+ ' OR '.join(f'o.{x} IS DISTINCT FROM e.{x}' for x in cols)+f') FROM {F}.edge_current VERSION AS OF 3 o JOIN {F}.edge_current VERSION AS OF {v("edge_current")} e ON {key("o","e")}')==[['9999980','0']]
assert c.sql('deleted-absent',f'SELECT count(*) FROM {F}.delete_r1 d JOIN {F}.edge_current VERSION AS OF {v("edge_current")} e ON {key("d","e")}')==[['0']]
for side in ['source','target']:
 assert c.sql(side+'-endpoints',f'SELECT count(*) FROM {F}.edge_current VERSION AS OF {v("edge_current")} e LEFT ANTI JOIN {N} VERSION AS OF 3 n ON e.source_system=n.source_system AND e.{side}_type=n.type_id AND e.{side}_id=n.id')==[['0']]
assert c.sql('hub-admission-work',f"SELECT count(*)+sum(d.out_degree) FROM {F}.adjacency VERSION AS OF {v('adjacency')} a JOIN {F}.out_degree_r1 VERSION AS OF {v('out_degree_r1')} d ON a.source_system=d.source_system AND a.rel_type_id=d.rel_type_id AND a.target_type=d.source_type AND a.target_id=d.source_id WHERE a.source_system='pilot:0' AND a.rel_type_id=7 AND a.source_type=1 AND a.source_id=200001")==[['120003']]
assert c.sql('journal-origins',f"SELECT count(*),count(DISTINCT struct(source_feed,source_epoch,source_position,event_ordinal)) FROM {F}.property_journal VERSION AS OF {v('property_journal')} WHERE source_feed='fixture-edge-structure'")==[['20021','20021']]
for table,op in [('delete_r1','delete'),('insert_r1','insert')]:
 value='old_json' if op=='delete' else 'new_json'
 assert c.sql('journal-carrier-'+op,f"SELECT count_if(j.{value} IS DISTINCT FROM s.carrier OR j.property_id IS NOT NULL OR j.operation<>'{op}') FROM (SELECT source_system,rel_type_id,id,{carrier()} carrier FROM {F}.{table}) s LEFT JOIN {F}.property_journal VERSION AS OF {v('property_journal')} j ON s.source_system=j.source_system AND s.rel_type_id=j.type_id AND s.id=j.id AND j.source_feed='fixture-edge-structure'")==[['0']]
progress={k:{'epoch':'e','position':1} for k in ['fixture-multi','fixture-multi-edge','fixture-edge-structure']}
c.sql('publish-descriptor',f"INSERT INTO {F}.publication_manifest VALUES ('r3-1','ashlar-delta/0.1-spike','{json.dumps(versions,separators=(',',':'))}','{json.dumps(progress,separators=(',',':'))}','{{\"pilot:0\":\"r1\",\"pilot:1\":\"r1\",\"pilot:2\":\"r1\",\"pilot:3\":\"r1\",\"pilot:4\":\"r1\"}}','{{\"structural_checks\":\"passed\"}}',current_timestamp())")
(out/'summary.json').write_text(json.dumps(dict(state='completed',versions=versions,atomic_apply_s=apply_s,through_descriptor_s=time.time()-start,edge_count=10019981,hub_logical_work=120003,scope='one synthetic structural batch; no throughput or physical scan admission'),indent=2));c.history();c.close();print('Structural publication verified',flush=True)
