"""Three disjoint 300k/30s multi-domain batches with both fence phases timed."""
import json,time
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import post as post_guard
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';out=B/'out/native/ashlar_fenced_scheduled_20261005_r17';c=DriverClient(out)
assert json.loads((B/'out/native/ashlar_edge_fused_guard_20261005_r17/summary.json').read_text())['state']=='passed'
base=json.loads((B/'out/native/ashlar_fenced_scheduled_20261005_r16/summary.json').read_text())['batches'][-1]['versions'];assert int(c.sql('before-edge',f'DESCRIBE HISTORY {F}.edge_current LIMIT 1')[0][0])==13
cols=['source_system','rel_type_id','id','source_type','source_id','target_type','target_id','schema_revision','entity_version','props_json','retained_json','order_key','source_feed','source_epoch','source_position','published_at','lookup_hash']
key='s.source_system=e.source_system AND s.rel_type_id=e.rel_type_id AND s.id=e.id';prior=' OR '.join(f's.{x} IS DISTINCT FROM e.{x}' for x in cols)
payload='concat('+','.join(f"sha2(concat(source_system,':',cast(rel_type_id AS STRING),':',cast(id AS STRING),':r17:{i}'),256)" for i in range(32))+')'
for batch in range(1,4):
 lo=90000+30000*batch+1;hi=lo+30000
 c.sql('prepare-'+str(batch),f"CREATE TABLE {F}.stage_r17_{batch} USING DELTA AS SELECT *,to_json(named_struct('201',{payload})) new_props,cast(substring(source_system,7) AS BIGINT)*3000000+(rel_type_id-7)*1000000+id ordinal FROM {F}.edge_current VERSION AS OF 13 WHERE id<=1000000 AND id>={lo} AND id<{hi}")
 assert c.sql('stage-count-'+str(batch),f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {F}.stage_r17_{batch}')==[['300000','300000']]
guard=f"IF (SELECT count(*) FROM {F}.fence_r11 WHERE stream='graph' AND epoch=2 AND owner='replacement')<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='stale scheduled publisher'; END IF; UPDATE {F}.fence_r11 SET sequence=sequence+1 WHERE stream='graph' AND epoch=2 AND owner='replacement';"
old="to_json(array(get_json_object(props_json,'$.201')))";new="to_json(array(get_json_object(new_props,'$.201')))";results=[];t0=time.time()
(out/'scope.json').write_text(json.dumps({'state':'running','batches':3,'entities_per_batch':300000,'arrival_interval_s':30,'stages':'prebuilt outside clock','readers':0,'start_epoch':t0},indent=2))
for batch in range(1,4):
 ready=t0+batch*30
 while time.time()<ready:time.sleep(min(.5,ready-time.time()))
 begun=time.time();stage=F+'.stage_r17_'+str(batch);vector=dict(base);vector[F+'.edge_current']=13+batch;vector[F+'.property_journal']=11+batch
 progress={k:{'epoch':'e','position':1} for k in ['fixture-multi','fixture-multi-edge','fixture-edge-structure','fixture-recovery','fixture-fenced']};progress['fixture-scheduled-fenced']={'epoch':'e','position':4};progress['fixture-broadcast300k']={'epoch':'e','position':3};progress['fixture-fused300k']={'epoch':'e','position':batch};revisions={'pilot:'+str(i):'r1' for i in range(5)}
 c.sql('atomic-batch-'+str(batch),f'''BEGIN ATOMIC {guard}
 IF (SELECT /*+ BROADCAST(s) */ count(*)<>300000 OR count(DISTINCT struct(s.source_system,s.rel_type_id,s.id))<>300000 OR count_if({prior})<>0 FROM {stage} s JOIN {F}.edge_current e ON {key}) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='scheduled prior mismatch'; END IF;
 DELETE FROM {F}.edge_current e WHERE EXISTS (SELECT 1 FROM {stage} s WHERE {key});
 INSERT INTO {F}.edge_current SELECT source_system,rel_type_id,id,source_type,source_id,target_type,target_id,schema_revision,cast(7 AS BIGINT),new_props,retained_json,order_key,'fixture-fused300k','e',{batch},current_timestamp(),lookup_hash FROM {stage};
 INSERT INTO {F}.property_journal SELECT source_system,'edge',rel_type_id,id,201,7,'update',true,substring({old},2,length({old})-2),true,substring({new},2,length({new})-2),schema_revision,'fixture-fused300k','e',{batch},ordinal,'synthetic-fused:{batch}',current_timestamp() FROM {stage};
 IF (SELECT /*+ BROADCAST(s,j) */ count(*)<>300000 OR count(DISTINCT struct(s.source_system,s.rel_type_id,s.id))<>300000 OR count_if({post_guard(batch)})<>0 FROM {stage} s JOIN {F}.edge_current e ON {key} LEFT JOIN {F}.property_journal j ON s.source_system=j.source_system AND s.rel_type_id=j.type_id AND s.id=j.id AND j.source_feed='fixture-fused300k' AND j.source_position={batch}) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='fused post state'; END IF;
 IF (SELECT count(*)<>300000 OR count(DISTINCT struct(source_feed,source_epoch,source_position,event_ordinal))<>300000 FROM {F}.property_journal WHERE source_feed='fixture-fused300k' AND source_position={batch}) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='fused journal cardinality'; END IF;
 INSERT INTO {F}.recovery_receipt_r8 VALUES ('r17-{batch}','{json.dumps(vector,separators=(',',':'))}','{json.dumps(progress,separators=(',',':'))}','{json.dumps(revisions,separators=(',',':'))}'); END''')
 for table in ['edge_current','property_journal']:assert int(c.sql('version-'+table+'-'+str(batch),f'DESCRIBE HISTORY {F}.{table} LIMIT 1')[0][0])==vector[F+'.'+table]
 c.sql('publish-'+str(batch),f'''BEGIN ATOMIC {guard} INSERT INTO {F}.manifest_r11 SELECT publication_id,'ashlar-delta/0.1-spike',versions_json,progress_json,revisions_json,'{{"scheduled_checks":"passed"}}',current_timestamp() FROM {F}.recovery_receipt_r8 WHERE publication_id='r17-{batch}'; END''')
 ended=time.time();r=dict(batch=batch,queue_s=begun-ready,processing_s=ended-begun,oldest_freshness_s=ended-(ready-30),versions=vector);results.append(r);(out/'progress.json').write_text(json.dumps(results,indent=2));print(json.dumps(r),flush=True)
(out/'summary.json').write_text(json.dumps(dict(state='completed',batches=results,scope='four prebuilt disjoint batches; no concurrent readers or stage/source capture clock; journal payload/full untouched verification remains separate'),indent=2));c.history();c.close();print('Scheduled fenced batches completed')
