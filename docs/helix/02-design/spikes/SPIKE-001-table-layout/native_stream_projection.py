"""Pipelined publication with version-pinned scalar SELECT over canonical state.
Scheduled arrivals are synthetic, not a live feed; backlog never resets clocks.
"""
import json,time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from driver_sql import DriverClient as Client
BASE=Path(__file__).resolve().parent;schema='ashlar_stream_20261005_e7';F='client_dev.'+schema;out=BASE/'out/native'/schema;c=Client(out)
c.sql('schema',f"CREATE SCHEMA {F} COMMENT 'Ashlar synthetic scheduled publication stream'")
c.sql('environment','SELECT current_version()')
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
c.sql('seed-objects',f"CREATE TABLE {F}.objects USING DELTA CLUSTER BY (id) TBLPROPERTIES ('delta.dataSkippingStatsColumns'='id','delta.targetFileSize'='134217728','delta.feature.catalogManaged'='supported') AS SELECT *,cast(0 AS BIGINT) entity_version FROM client_dev.ashlar_ingest_20261005_a2.serving VERSION AS OF 3")
c.sql('journal',f"CREATE TABLE {F}.journal (batch BIGINT,id BIGINT,property_id BIGINT,old_present BOOLEAN,old_json STRING,new_present BOOLEAN,new_json STRING) USING DELTA CLUSTER BY (batch,id) TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
c.sql('manifest',f'CREATE TABLE {F}.manifest (batch BIGINT,versions STRING,entities BIGINT,recorded_at TIMESTAMP) USING DELTA')
c.sql('receipt',f"CREATE TABLE {F}.receipt (batch BIGINT,applied_at TIMESTAMP) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
# Producer payload construction is explicitly outside the arrival clock.
# Staging copies the ready producer payload inside the clock for every batch.
for batch in range(1,7):
 lower=(batch-1)*100000;upper=batch*100000
 payload='concat('+','.join(f"sha2(concat(cast(id AS STRING),':stream:{batch}:{i}'),256)" for i in range(32))+')'
 c.sql('producer-'+str(batch),f"CREATE TABLE {F}.producer_{batch} USING DELTA AS SELECT *,to_json(named_struct('101',concat('g',cast(id%100 AS STRING)),'102',id%1000,'103',{payload})) new_props FROM {F}.objects WHERE id>{lower} AND id<={upper}")
stage_client=Client(out/'staging')
stage_client.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
pool=ThreadPoolExecutor(max_workers=1)
t0=time.time();results=[]
def stage_batch(batch):
 ready=t0+batch*10
 while time.time()<ready:time.sleep(min(.5,ready-time.time()))
 stage_client.sql('stage-'+str(batch),f"CREATE TABLE {F}.stage_{batch} USING DELTA AS SELECT * FROM {F}.producer_{batch}")
 return time.time()
futures={batch:pool.submit(stage_batch,batch) for batch in range(1,7)}
(out/'schedule.json').write_text(json.dumps({'start_epoch':t0,'interval_s':10,'batch_entities':100000,'batches':6,'modeled_entities_s':10000,'oldest_arrival':'t0+(batch-1)*10','ready':'t0+batch*10','producer_generation':'prebuilt before schedule; staging remains timed','compute':'existing serverless Photon 2X-Small; no config changes'},indent=2)+'\n')
for batch in range(1,7):
 ready=t0+batch*10;oldest=t0+(batch-1)*10
 while time.time()<ready:time.sleep(min(.5,ready-time.time()))
 begin=time.time();lower=(batch-1)*100000;upper=batch*100000
 staged_at=futures[batch].result()
 old="to_json(array(get_json_object(props_json,'$.103')))";new="to_json(array(get_json_object(new_props,'$.103')))"
 statement=f"""BEGIN ATOMIC
 MERGE INTO {F}.objects t USING {F}.stage_{batch} s ON t.id=s.id AND t.id>{lower} AND t.id<={upper} WHEN MATCHED THEN UPDATE SET t.props_json=s.new_props,t.entity_version={batch};
 INSERT INTO {F}.journal SELECT {batch},id,103,true,substring({old},2,length({old})-2),true,substring({new},2,length({new})-2) FROM {F}.stage_{batch};
 IF (SELECT count(*) FROM {F}.stage_{batch} s JOIN {F}.objects o ON s.id=o.id WHERE o.id>{lower} AND o.id<={upper})<>100000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ashlar changed cardinality'; END IF;
 IF (SELECT count(*) FROM {F}.stage_{batch} s JOIN {F}.objects o ON s.id=o.id WHERE o.id>{lower} AND o.id<={upper} AND (o.props_json<>s.new_props OR o.retained_json<>s.retained_json OR o.entity_version<>{batch}))<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ashlar carrier mismatch'; END IF;
 IF (SELECT count(*) FROM {F}.journal WHERE batch={batch})<>100000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ashlar journal count'; END IF;
 IF (SELECT count(*) FROM {F}.journal j JOIN {F}.stage_{batch} s ON j.id=s.id WHERE j.batch={batch} AND (from_json(concat('[',j.old_json,']'),'ARRAY<STRING>')[0] IS DISTINCT FROM get_json_object(s.props_json,'$.103') OR from_json(concat('[',j.new_json,']'),'ARRAY<STRING>')[0] IS DISTINCT FROM get_json_object(s.new_props,'$.103')))<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ashlar journal value'; END IF;
 INSERT INTO {F}.receipt VALUES ({batch},current_timestamp()); END"""
 c.sql('atomic-apply-'+str(batch),statement)
 versions={}
 for table in ['objects','journal']:versions[table]=int(c.sql('version-'+table+'-'+str(batch),f'DESCRIBE HISTORY {F}.{table} LIMIT 1')[0][0])
 v=json.dumps(versions,separators=(',',':'))
 c.sql('publish-'+str(batch),f"INSERT INTO {F}.manifest VALUES ({batch},'{v}',100000,current_timestamp())")
 end=time.time();result={'batch':batch,'entities':100000,'ready_epoch':ready,'start_epoch':begin,'end_epoch':end,'queue_delay_s':begin-ready,'processing_s':end-begin,'staging_completed_epoch':staged_at,'oldest_freshness_s':end-oldest,'newest_freshness_s':end-ready,'versions':versions};results.append(result)
 (out/'stream-summary.json').write_text(json.dumps({'state':'running','batches':results},indent=2)+'\n');print(json.dumps(result),flush=True)
 if end-ready>120:raise RuntimeError('Backlog exceeded bounded 120s; stop further arrivals')
# Outside freshness timing: prove residual retrieval at EVERY published vector.
for result in results:
 batch=result['batch'];v=result['versions']
 rows=c.sql('hydrate-parity-'+str(batch),f"SELECT count(*),count_if(o.props_json IS DISTINCT FROM s.new_props OR o.retained_json IS DISTINCT FROM s.retained_json OR get_json_object(o.props_json,'$.101') IS DISTINCT FROM get_json_object(s.new_props,'$.101') OR cast(get_json_object(o.props_json,'$.102') AS BIGINT) IS DISTINCT FROM cast(get_json_object(s.new_props,'$.102') AS BIGINT) OR o.entity_version<>{batch}) FROM {F}.objects VERSION AS OF {v['objects']} o JOIN {F}.stage_{batch} s ON s.id=o.id")
 assert rows==[['100000','0']]
 key=(batch-1)*100000+1
 rows=c.sql('hydrate-singleton-'+str(batch),f"SELECT id,props_json,retained_json FROM {F}.objects VERSION AS OF {v['objects']} WHERE id={key}")
 # Execute the scalar graph projection at this same canonical version.
 projection=c.sql('scalar-projection-'+str(batch),f"SELECT concat('N5:pilot:1:',cast(id AS STRING)) node_key,'pilot' source_system,cast(1 AS BIGINT) type_id,id,get_json_object(props_json,'$.101') group_label,cast(get_json_object(props_json,'$.102') AS BIGINT) counter,entity_version FROM {F}.objects VERSION AS OF {v['objects']} WHERE id={key}")
 assert projection==[['N5:pilot:1:'+str(key),'pilot','1',str(key),'g'+str(key%100),str(key%1000),str(batch)]]
 expected=c.sql('hydrate-source-'+str(batch),f"SELECT id,new_props,retained_json FROM {F}.stage_{batch} WHERE id={key}")
 assert rows==expected and len(rows)==1
c.sql('objects-detail',f'DESCRIBE DETAIL {F}.objects')
pool.shutdown(wait=True);stage_client.close();c.records.extend(stage_client.records);(out/'all-statements.json').write_text(json.dumps(c.records,indent=2)+'\n');c.history();c.close();(out/'stream-summary.json').write_text(json.dumps({'state':'completed','batches':results,'scope':'six synthetic scheduled 100k object batches at 10s intervals; prebuilt producer generation excluded; staging overlaps serial publication via one independent driver session; staging/queue/inside-transaction validation included; duplicate outside validation removed; driver atomic apply includes journal/canonical/receipt (scalar serving is a version-pinned SELECT, no physical copy) and validation; full exact bags and unknown retained content retrieved from canonical pinned versions; post-run hydration checks excluded from publication clock; manifest follows version discovery; no live producer, edges, replay/deletes, reader load or recovery; not long-run p95 admission'},indent=2)+'\n')
print('Completed scheduled stream',flush=True)
