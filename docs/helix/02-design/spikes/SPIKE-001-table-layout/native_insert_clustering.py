"""Experimental ID-only clustering plus atomic physical replacement; journal remains update.
Reuses n1 private canonical fixture; no source or production data.
"""
import json,time
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_scattered_20261005_n1'
out=B/'out/native/ashlar_insert_cluster_20261005_n8';c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180');c.sql('environment','SELECT current_version()')
before=int(c.sql('before-version',f'DESCRIBE HISTORY {F}.object_current LIMIT 1')[0][0])
c.sql('before-detail',f'DESCRIBE DETAIL {F}.object_current')
c.sql('configure-id-clustering',f'ALTER TABLE {F}.object_current CLUSTER BY (id)')
cols=['source_system','type_id','id','logical_key_json','schema_revision','entity_version','props_json','retained_json','root_id','source_feed','source_epoch','source_position','published_at']
payload='concat('+','.join(f"sha2(concat(cast(id AS STRING),':insertcluster:12:{i}'),256)" for i in range(32))+')'
c.sql('producer',f"CREATE TABLE {F}.producer_n8 USING DELTA AS SELECT *,to_json(named_struct('101',concat('g',cast(id%100 AS STRING)),'102',id%1000,'103',{payload})) new_props FROM {F}.object_current VERSION AS OF {before} WHERE pmod((id-1)*104729,10000000)>=2800000 AND pmod((id-1)*104729,10000000)<3000000")
assert c.sql('producer-identities',f'SELECT count(*),count(DISTINCT id),count(DISTINCT floor((id-1)/100000)) FROM {F}.producer_n8')==[['200000','200000','100']]
# One synthetic 20-second accumulation; producer creation is outside clock.
t0=time.time();ready=t0+20
(out/'schedule.json').write_text(json.dumps(dict(start_epoch=t0,ready_epoch=ready,entities=200000,accumulation_s=20,modeled_entities_s=10000,before_version=before,scope='one disjoint scattered batch on the already updated 10M n1 fixture; sequential comparison, not clean paired randomization'),indent=2)+'\n')
while time.time()<ready:time.sleep(min(.5,ready-time.time()))
started=time.time();c.sql('stage',f'CREATE TABLE {F}.stage_n8 USING DELTA AS SELECT * FROM {F}.producer_n8')
prior=' OR '.join(f'o.{x} IS DISTINCT FROM s.{x}' for x in cols)
identity="s.source_system=o.source_system AND s.type_id=o.type_id AND s.id=o.id"
unchanged=[x for x in cols if x not in ['props_json','entity_version','source_position','published_at']]
post=' OR '.join([f'o.{x} IS DISTINCT FROM s.{x}' for x in unchanged]+["o.props_json IS DISTINCT FROM s.new_props","o.entity_version IS DISTINCT FROM 12","o.source_position IS DISTINCT FROM 12","o.published_at IS NULL","j.id IS NULL","j.property_id IS DISTINCT FROM 103","j.entity_version IS DISTINCT FROM 12","j.operation IS DISTINCT FROM 'update'","j.old_present IS DISTINCT FROM true","j.new_present IS DISTINCT FROM true","j.schema_revision IS DISTINCT FROM 'r1'","j.event_ordinal IS DISTINCT FROM s.id","j.source_time_text IS DISTINCT FROM 'synthetic-batch:12'","from_json(concat('[',j.old_json,']'),'ARRAY<STRING>')[0] IS DISTINCT FROM get_json_object(s.props_json,'$.103')","from_json(concat('[',j.new_json,']'),'ARRAY<STRING>')[0] IS DISTINCT FROM get_json_object(s.new_props,'$.103')"])
old="to_json(array(get_json_object(props_json,'$.103')))";new="to_json(array(get_json_object(new_props,'$.103')))"
statement=f"""BEGIN ATOMIC
 IF (SELECT /*+ BROADCAST(s) */ count(*)<>200000 OR count(DISTINCT struct(s.source_system,s.type_id,s.id))<>200000 OR count_if({prior})<>0 FROM {F}.stage_n8 s JOIN {F}.object_current o ON {identity}) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ashlar fused prior mismatch/cardinality'; END IF;
 DELETE FROM {F}.object_current t WHERE EXISTS (SELECT 1 FROM {F}.stage_n8 s WHERE t.source_system=s.source_system AND t.type_id=s.type_id AND t.id=s.id);
 INSERT INTO {F}.object_current SELECT source_system,type_id,id,logical_key_json,schema_revision,cast(12 AS BIGINT),new_props,retained_json,root_id,source_feed,source_epoch,cast(12 AS BIGINT),current_timestamp() FROM {F}.stage_n8;
 INSERT INTO {F}.property_journal SELECT 'pilot','object',1,id,103,12,'update',true,substring({old},2,length({old})-2),true,substring({new},2,length({new})-2),'r1','S','e',12,id,'synthetic-batch:12',current_timestamp() FROM {F}.stage_n8;
 IF (SELECT /*+ BROADCAST(s,j) */ count(*)<>200000 OR count(DISTINCT struct(s.source_system,s.type_id,s.id))<>200000 OR count_if({post})<>0 FROM {F}.stage_n8 s JOIN {F}.object_current o ON {identity} LEFT JOIN {F}.property_journal j ON j.source_system=s.source_system AND j.entity_kind='object' AND j.type_id=s.type_id AND j.id=s.id AND j.source_feed='S' AND j.source_epoch='e' AND j.source_position=12) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ashlar fused canonical/journal mismatch/cardinality'; END IF;
 IF (SELECT count(*) FROM {F}.property_journal WHERE source_feed='S' AND source_epoch='e' AND source_position=12)<>200000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ashlar journal extra/missing event'; END IF;
 INSERT INTO {F}.receipt VALUES (12,current_timestamp());
 END"""
c.sql('atomic-apply',statement)
v={t:int(c.sql('version-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0]) for t in ['object_current','property_journal']}
c.sql('publish',f"INSERT INTO {F}.publication_manifest VALUES ('batch-12-insertcluster','ashlar-delta/0.1-spike','{json.dumps(v,separators=(',',':'))}','{{\"S\":{{\"epoch\":\"e\",\"position\":12}}}}','{{\"pilot\":\"r1\"}}','{{\"transaction_checks\":\"passed\",\"changed_objects\":200000,\"validation_variant\":\"insertcluster\"}}',current_timestamp())")
ended=time.time();result=dict(state='published',entities=200000,before_version=before,versions=v,processing_s=ended-started,oldest_freshness_s=ended-t0,newest_freshness_s=ended-ready)
(out/'summary.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)
# Independent after-publication checks, excluded from freshness.
assert c.sql('changed-exact',f'SELECT count(*),count_if({post}) FROM {F}.stage_n8 s LEFT JOIN {F}.object_current VERSION AS OF {v["object_current"]} o ON {identity} LEFT JOIN {F}.property_journal VERSION AS OF {v["property_journal"]} j ON j.source_system=s.source_system AND j.entity_kind=\'object\' AND j.type_id=s.type_id AND j.id=s.id AND j.source_feed=\'S\' AND j.source_epoch=\'e\' AND j.source_position=12')==[['200000','0']]
diff=' OR '.join(f'o.{x} IS DISTINCT FROM n.{x}' for x in cols)
assert c.sql('unchanged-all-carriers',f'SELECT count(*),count_if({diff}) FROM {F}.object_current VERSION AS OF {v["object_current"]} o JOIN {F}.object_current VERSION AS OF {before} n ON o.source_system=n.source_system AND o.type_id=n.type_id AND o.id=n.id WHERE pmod((o.id-1)*104729,10000000)<2800000 OR pmod((o.id-1)*104729,10000000)>=3000000')==[['9800000','0']]
assert c.sql('canonical-identities',f'SELECT count(*),count(DISTINCT struct(source_system,type_id,id)) FROM {F}.object_current VERSION AS OF {v["object_current"]}')==[['10000000','10000000']]
assert c.sql('journal-event-keys',f"SELECT count(*),count(DISTINCT struct(source_feed,source_epoch,source_position,event_ordinal)) FROM {F}.property_journal VERSION AS OF {v['property_journal']} WHERE source_position=12")==[['200000','200000']]
c.sql('mutation-history',f'DESCRIBE HISTORY {F}.object_current');c.sql('after-detail',f'DESCRIBE DETAIL {F}.object_current')
c.history();c.close();result['state']='completed';result['preservation']='200k changed exact; 9.8M unchanged all 13 fields; 10M canonical keys; 200k unique journal events'
(out/'summary.json').write_text(json.dumps(result,indent=2)+'\n');print('Fused validation probe completed',flush=True)
