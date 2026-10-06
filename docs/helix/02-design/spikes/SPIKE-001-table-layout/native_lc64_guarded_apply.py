"""Paired 64/16MiB guarded publication; no sustained-rate inference."""
import json,time
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS,post
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';N='client_dev.ashlar_lc64_20261005_r42';stage=N+'.stage_r44'
assert json.loads((B/'out/native/ashlar_lc64_ingest_prepare_20261005_r44/summary.json').read_text())['state']=='passed'
out=B/'out/native/ashlar_lc64_guarded_apply_20261005_r45';c=DriverClient(out);c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
for name,ns,ev,jv in [('lc64',N,0,0),('lc16',F,26,20)]:
 assert int(c.sql('initial-edge-'+name,f'DESCRIBE HISTORY {ns}.edge_current LIMIT 1')[0][0])==ev
 assert int(c.sql('initial-journal-'+name,f'DESCRIBE HISTORY {ns}.property_journal LIMIT 1')[0][0])==jv
c.sql('barrier-ddl',f"CREATE TABLE {N}.barrier_r45 (stream STRING,epoch BIGINT,owner STRING,pending BIGINT,sequence BIGINT) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
c.sql('barrier-seed',f"INSERT INTO {N}.barrier_r45 VALUES ('lc64',1,'publisher',NULL,0),('lc16',1,'publisher',NULL,0)")
c.sql('receipt-ddl',f"CREATE TABLE {N}.receipt_r45 (layout STRING,stage_name STRING,expected_count BIGINT,progress_json STRING,revisions_json STRING) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
c.sql('manifest-ddl',f"CREATE TABLE {N}.manifest_r45 LIKE {F}.manifest_r11")
c.sql('manifest-cm',f"ALTER TABLE {N}.manifest_r45 SET TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
base=json.loads((B/'out/native/ashlar_maintained_final_verify_20261005_r41_lc/summary.json').read_text())['versions']
prior=' OR '.join(f's.{x} IS DISTINCT FROM e.{x}' for x in COLS);key='s.source_system=e.source_system AND s.rel_type_id=e.rel_type_id AND s.id=e.id'
old="to_json(array(get_json_object(props_json,'$.201')))";new="to_json(array(get_json_object(new_props,'$.201')))"
guard=post(1).replace("'fixture-fused300k'","'fixture-file-target'").replace('IS DISTINCT FROM 7','IS DISTINCT FROM 10').replace("'synthetic-fused:1'","'synthetic-file-target:1'")
revisions={'pilot:'+str(i):'r1' for i in range(5)}
progress={k:{'epoch':'e','position':1} for k in ['fixture-multi','fixture-multi-edge','fixture-edge-structure','fixture-recovery','fixture-fenced','fixture-file-target','fixture-layout-compare']}
progress.update({'fixture-scheduled-fenced':{'epoch':'e','position':4},'fixture-broadcast300k':{'epoch':'e','position':3},'fixture-fused300k':{'epoch':'e','position':3},'fixture-maintained':{'epoch':'e','position':2}})
results=[]
for layout,ns in [('lc64',N),('lc16',F)]:
 extra=''
 started=time.time();(out/'progress.json').write_text(json.dumps({'state':'applying','layout':layout,'started_epoch':started,'completed':results},indent=2))
 c.sql('atomic-data-'+layout,f'''BEGIN ATOMIC
 IF (SELECT count(*) FROM {N}.barrier_r45 WHERE stream='{layout}' AND epoch=1 AND owner='publisher' AND pending IS NULL)<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='pending layout batch'; END IF;
 UPDATE {N}.barrier_r45 SET pending=1,sequence=sequence+1 WHERE stream='{layout}' AND epoch=1 AND owner='publisher' AND pending IS NULL;
 IF (SELECT /*+ BROADCAST(s) */ count(*)<>300000 OR count(DISTINCT struct(s.source_system,s.rel_type_id,s.id))<>300000 OR count_if({prior})<>0 FROM {stage} s JOIN {ns}.edge_current e ON {key}) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='prior mismatch'; END IF;
 DELETE FROM {ns}.edge_current e WHERE EXISTS (SELECT 1 FROM {stage} s WHERE {key});
 INSERT INTO {ns}.edge_current SELECT source_system,rel_type_id,id,source_type,source_id,target_type,target_id,schema_revision,cast(10 AS BIGINT),new_props,retained_json,order_key,'fixture-file-target','e',1,current_timestamp(),lookup_hash{extra} FROM {stage};
 INSERT INTO {ns}.property_journal SELECT source_system,'edge',rel_type_id,id,201,10,'update',true,substring({old},2,length({old})-2),true,substring({new},2,length({new})-2),schema_revision,'fixture-file-target','e',1,ordinal,'synthetic-file-target:1',current_timestamp() FROM {stage};
 IF (SELECT /*+ BROADCAST(s,j) */ count(*)<>300000 OR count(DISTINCT struct(s.source_system,s.rel_type_id,s.id))<>300000 OR count_if({guard})<>0 FROM {stage} s JOIN {ns}.edge_current e ON {key} LEFT JOIN {ns}.property_journal j ON s.source_system=j.source_system AND s.rel_type_id=j.type_id AND s.id=j.id AND j.source_feed='fixture-file-target' AND j.source_position=1) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='post mismatch'; END IF;
 IF (SELECT count(*)<>300000 OR count(DISTINCT struct(source_feed,source_epoch,source_position,event_ordinal))<>300000 FROM {ns}.property_journal WHERE source_feed='fixture-file-target' AND source_position=1) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='origin mismatch'; END IF;
 INSERT INTO {N}.receipt_r45 VALUES ('{layout}','{stage}',300000,'{json.dumps(progress,separators=(',',':'))}','{json.dumps(revisions,separators=(',',':'))}'); END''')
 vector={k:v for k,v in base.items() if k not in [F+'.edge_current',F+'.property_journal']}
 for table in ['edge_current','property_journal']:
  r=c.sql('resolve-'+layout+'-'+table,f"SELECT count(*),count(_metadata.row_commit_version),min(_metadata.row_commit_version),max(_metadata.row_commit_version),count(DISTINCT _metadata.row_commit_version) FROM {ns}.{table} WHERE source_feed='fixture-file-target' AND source_epoch='e' AND source_position=1")
  assert r[0][:2]==['300000','300000'] and r[0][2]==r[0][3] and r[0][4]=='1';vector[ns+'.'+table]=int(r[0][2])
 c.sql('publish-'+layout,f'''BEGIN ATOMIC
 IF (SELECT count(*) FROM {N}.barrier_r45 WHERE stream='{layout}' AND epoch=1 AND owner='publisher' AND pending=1)<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='pending publication mismatch'; END IF;
 UPDATE {N}.barrier_r45 SET sequence=sequence+1 WHERE stream='{layout}' AND epoch=1 AND owner='publisher' AND pending=1;
 IF (SELECT count(*) FROM {N}.receipt_r45 WHERE layout='{layout}' AND expected_count=300000)<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='receipt mismatch'; END IF;
 INSERT INTO {N}.manifest_r45 SELECT 'r45-{layout}','ashlar-delta/0.1-spike','{json.dumps(vector,separators=(',',':'))}',progress_json,revisions_json,'{{"pending_and_exact_carriers":"passed"}}',current_timestamp() FROM {N}.receipt_r45 WHERE layout='{layout}';
 UPDATE {N}.barrier_r45 SET pending=NULL WHERE stream='{layout}' AND pending=1; END''')
 ended=time.time();result={'layout':layout,'processing_s':ended-started,'oldest_freshness_single_30s_window_s':30+ended-started,'versions':vector};results.append(result)
 (out/'progress.json').write_text(json.dumps({'state':'published','completed':results},indent=2));print(json.dumps(result),flush=True)
c.sql('barrier-final',f'SELECT stream,pending,sequence FROM {N}.barrier_r45 ORDER BY stream')
c.history();c.close();(out/'summary.json').write_text(json.dumps({'state':'completed','batches':results,'scope':'one sequential common 300k batch per layout, source 30s window assumption, prebuilt stage/no readers; full prior/post/journal guards and resolved vectors; not sustained rate or exhaustive untouched verification'},indent=2));print('Paired LC64/16 guarded publications completed',flush=True)
