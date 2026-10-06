"""Two remaining disjoint stages; new arrival clock, pending barrier, actual row-commit vectors."""
import json,time
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS,post
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';out=B/'out/native/ashlar_barrier_graph_20261005_r23';c=DriverClient(out)
base=json.loads((B/'out/native/ashlar_resolve_r17_20261005_r19/summary.json').read_text())['versions']
c.sql('barrier-ddl',f"CREATE TABLE {F}.barrier_r23 (stream STRING,epoch BIGINT,owner STRING,pending BIGINT,sequence BIGINT) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
c.sql('seed-barrier',f"INSERT INTO {F}.barrier_r23 VALUES ('graph',1,'publisher',NULL,0)")
c.sql('receipt-ddl',f"CREATE TABLE {F}.receipt_r23 (batch BIGINT,stage_name STRING,expected_count BIGINT,progress_json STRING,revisions_json STRING) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
key='s.source_system=e.source_system AND s.rel_type_id=e.rel_type_id AND s.id=e.id';prior=' OR '.join(f's.{x} IS DISTINCT FROM e.{x}' for x in COLS);old="to_json(array(get_json_object(props_json,'$.201')))";new="to_json(array(get_json_object(new_props,'$.201')))";results=[];t0=time.time()
(out/'scope.json').write_text(json.dumps({'state':'running','new_clock_epoch':t0,'entities_per_batch':300000,'arrival_interval_s':30,'batches':2,'readers':0,'stages':'previously prepared outside clock; failed old schedule not resumed'},indent=2))
for index,batch in enumerate([2,3],1):
 ready=t0+index*30
 while time.time()<ready:time.sleep(min(.5,ready-time.time()))
 begun=time.time();stage=F+'.stage_r17_'+str(batch);progress={k:{'epoch':'e','position':1} for k in ['fixture-multi','fixture-multi-edge','fixture-edge-structure','fixture-recovery','fixture-fenced']};progress['fixture-scheduled-fenced']={'epoch':'e','position':4};progress['fixture-broadcast300k']={'epoch':'e','position':3};progress['fixture-fused300k']={'epoch':'e','position':batch};revisions={'pilot:'+str(i):'r1' for i in range(5)}
 c.sql('atomic-data-'+str(batch),f'''BEGIN ATOMIC
 IF (SELECT count(*) FROM {F}.barrier_r23 WHERE stream='graph' AND epoch=1 AND owner='publisher' AND pending IS NULL)<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='pending graph batch'; END IF;
 UPDATE {F}.barrier_r23 SET pending={batch},sequence=sequence+1 WHERE stream='graph' AND epoch=1 AND owner='publisher' AND pending IS NULL;
 IF (SELECT /*+ BROADCAST(s) */ count(*)<>300000 OR count(DISTINCT struct(s.source_system,s.rel_type_id,s.id))<>300000 OR count_if({prior})<>0 FROM {stage} s JOIN {F}.edge_current e ON {key}) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='prior carrier mismatch'; END IF;
 DELETE FROM {F}.edge_current e WHERE EXISTS (SELECT 1 FROM {stage} s WHERE {key});
 INSERT INTO {F}.edge_current SELECT source_system,rel_type_id,id,source_type,source_id,target_type,target_id,schema_revision,cast(7 AS BIGINT),new_props,retained_json,order_key,'fixture-fused300k','e',{batch},current_timestamp(),lookup_hash FROM {stage};
 INSERT INTO {F}.property_journal SELECT source_system,'edge',rel_type_id,id,201,7,'update',true,substring({old},2,length({old})-2),true,substring({new},2,length({new})-2),schema_revision,'fixture-fused300k','e',{batch},ordinal,'synthetic-fused:{batch}',current_timestamp() FROM {stage};
 IF (SELECT /*+ BROADCAST(s,j) */ count(*)<>300000 OR count(DISTINCT struct(s.source_system,s.rel_type_id,s.id))<>300000 OR count_if({post(batch)})<>0 FROM {stage} s JOIN {F}.edge_current e ON {key} LEFT JOIN {F}.property_journal j ON s.source_system=j.source_system AND s.rel_type_id=j.type_id AND s.id=j.id AND j.source_feed='fixture-fused300k' AND j.source_position={batch}) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='post carrier journal mismatch'; END IF;
 IF (SELECT count(*)<>300000 OR count(DISTINCT struct(source_feed,source_epoch,source_position,event_ordinal))<>300000 FROM {F}.property_journal WHERE source_feed='fixture-fused300k' AND source_position={batch}) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='journal origin mismatch'; END IF;
 INSERT INTO {F}.receipt_r23 VALUES ({batch},'{stage}',300000,'{json.dumps(progress,separators=(',',':'))}','{json.dumps(revisions,separators=(',',':'))}'); END''')
 vector=dict(base)
 for table in ['edge_current','property_journal']:
  r=c.sql('resolve-'+table+'-'+str(batch),f"SELECT count(*),count(_metadata.row_commit_version),min(_metadata.row_commit_version),max(_metadata.row_commit_version),count(DISTINCT _metadata.row_commit_version) FROM {F}.{table} WHERE source_feed='fixture-fused300k' AND source_epoch='e' AND source_position={batch}");assert r[0][:2]==['300000','300000'] and r[0][2]==r[0][3] and r[0][4]=='1';vector[F+'.'+table]=int(r[0][2])
 c.sql('publish-and-clear-'+str(batch),f'''BEGIN ATOMIC
 IF (SELECT count(*) FROM {F}.barrier_r23 WHERE stream='graph' AND epoch=1 AND owner='publisher' AND pending={batch})<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='pending publication mismatch'; END IF;
 UPDATE {F}.barrier_r23 SET sequence=sequence+1 WHERE stream='graph' AND epoch=1 AND owner='publisher' AND pending={batch};
 IF (SELECT count(*) FROM {F}.receipt_r23 WHERE batch={batch})<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='receipt mismatch'; END IF;
 INSERT INTO {F}.manifest_r11 SELECT 'r23-{batch}','ashlar-delta/0.1-spike','{json.dumps(vector,separators=(',',':'))}',progress_json,revisions_json,'{{"pending_and_row_metadata":"passed"}}',current_timestamp() FROM {F}.receipt_r23 WHERE batch={batch};
 UPDATE {F}.barrier_r23 SET pending=NULL WHERE stream='graph' AND pending={batch}; END''')
 ended=time.time();r={'batch':batch,'queue_s':begun-ready,'processing_s':ended-begun,'oldest_freshness_s':ended-(ready-30),'versions':vector};results.append(r);(out/'progress.json').write_text(json.dumps(results,indent=2));print(json.dumps(r),flush=True)
(out/'summary.json').write_text(json.dumps({'state':'completed','batches':results,'scope':'new two-batch clock; prebuilt stages/no concurrent readers; cooperative full graph transaction guards; final exhaustive untouched/readback verifier separate'},indent=2));c.history();c.close();print('Barrier graph batches completed')
