"""Six scheduled 100k-entity batches model a 10k/s source for one minute.
Scheduled arrivals are synthetic, not a live feed; backlog never resets clocks.
"""
import json,time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from persistent_sql import Client
BASE=Path(__file__).resolve().parent;schema='ashlar_stream_20261005_e2';F='client_dev.'+schema;out=BASE/'out/native'/schema;c=Client(out)
c.sql('schema',f"CREATE SCHEMA {F} COMMENT 'Ashlar synthetic scheduled publication stream'")
c.sql('environment','SELECT current_version()')
for table in ['objects','serving']:
 c.sql('seed-'+table,f"CREATE TABLE {F}.{table} USING DELTA CLUSTER BY (id) TBLPROPERTIES ('delta.dataSkippingStatsColumns'='id','delta.targetFileSize'='134217728') AS SELECT *,cast(0 AS BIGINT) entity_version FROM client_dev.ashlar_ingest_20261005_a2.serving VERSION AS OF 3")
c.sql('journal',f'CREATE TABLE {F}.journal (batch BIGINT,id BIGINT,property_id BIGINT,old_present BOOLEAN,old_json STRING,new_present BOOLEAN,new_json STRING) USING DELTA CLUSTER BY (batch,id)')
c.sql('manifest',f'CREATE TABLE {F}.manifest (batch BIGINT,versions STRING,entities BIGINT,recorded_at TIMESTAMP) USING DELTA')
workers={name:Client(out/'workers'/name) for name in ['objects','serving','journal']}
for name,w in workers.items():w.sql('warm-transport','SELECT 1,uuid()')
pool=ThreadPoolExecutor(max_workers=3)
t0=time.time();results=[]
(out/'schedule.json').write_text(json.dumps({'start_epoch':t0,'interval_s':10,'batch_entities':100000,'batches':6,'modeled_entities_s':10000,'oldest_arrival':'t0+(batch-1)*10','ready':'t0+batch*10','compute':'existing serverless Photon 2X-Small; no config changes'},indent=2)+'\n')
for batch in range(1,7):
 ready=t0+batch*10;oldest=t0+(batch-1)*10
 while time.time()<ready:time.sleep(min(.5,ready-time.time()))
 begin=time.time();lower=(batch-1)*100000;upper=batch*100000
 payload='concat('+','.join(f"sha2(concat(cast(id AS STRING),':stream:{batch}:{i}'),256)" for i in range(32))+')'
 c.sql('stage-'+str(batch),f"CREATE TABLE {F}.stage_{batch} USING DELTA AS SELECT *,to_json(named_struct('101',concat('g',cast(id%100 AS STRING)),'102',id%1000,'103',{payload})) new_props FROM {F}.objects WHERE id>{lower} AND id<={upper}")
 merge_sql=f'MERGE INTO {F}.objects t USING {F}.stage_{batch} s ON t.id=s.id AND t.id>{lower} AND t.id<={upper} WHEN MATCHED THEN UPDATE SET t.props_json=s.new_props,t.entity_version={batch}'
 old="to_json(array(get_json_object(props_json,'$.103')))";new="to_json(array(get_json_object(new_props,'$.103')))"
 journal_sql=f'INSERT INTO {F}.journal SELECT {batch},id,103,true,substring({old},2,length({old})-2),true,substring({new},2,length({new})-2) FROM {F}.stage_{batch}'
 serving_sql=f'MERGE INTO {F}.serving t USING {F}.stage_{batch} s ON t.id=s.id AND t.id>{lower} AND t.id<={upper} WHEN MATCHED THEN UPDATE SET t.props_json=s.new_props,t.entity_version={batch}'
 futures=[pool.submit(workers[name].sql,label+'-'+str(batch),statement) for name,label,statement in [('objects','merge',merge_sql),('journal','journal',journal_sql),('serving','serving',serving_sql)]]
 for future in futures:future.result()
 versions={}
 for table in ['objects','serving','journal']:versions[table]=int(c.sql('version-'+table+'-'+str(batch),f'DESCRIBE HISTORY {F}.{table} LIMIT 1')[0][0])
 assert c.sql('validate-'+str(batch),f"SELECT count(*) n,count_if(o.props_json<>s.new_props OR p.props_json<>s.new_props OR o.retained_json<>s.retained_json OR p.retained_json<>s.retained_json OR o.entity_version<>{batch} OR p.entity_version<>{batch}) bad FROM {F}.stage_{batch} s JOIN {F}.objects o ON s.id=o.id JOIN {F}.serving p ON s.id=p.id WHERE o.id>{lower} AND o.id<={upper} AND p.id>{lower} AND p.id<={upper}")==[['100000','0']]
 assert c.sql('journal-validate-'+str(batch),f"SELECT count(*) n,count_if(from_json(concat('[',j.old_json,']'),'ARRAY<STRING>')[0] IS DISTINCT FROM get_json_object(s.props_json,'$.103') OR from_json(concat('[',j.new_json,']'),'ARRAY<STRING>')[0] IS DISTINCT FROM get_json_object(s.new_props,'$.103')) bad FROM {F}.journal j JOIN {F}.stage_{batch} s ON j.id=s.id WHERE j.batch={batch}")==[['100000','0']]
 v=json.dumps(versions,separators=(',',':'))
 c.sql('publish-'+str(batch),f"INSERT INTO {F}.manifest VALUES ({batch},'{v}',100000,current_timestamp())")
 end=time.time();result={'batch':batch,'entities':100000,'ready_epoch':ready,'start_epoch':begin,'end_epoch':end,'queue_delay_s':begin-ready,'processing_s':end-begin,'oldest_freshness_s':end-oldest,'newest_freshness_s':end-ready,'versions':versions};results.append(result)
 (out/'stream-summary.json').write_text(json.dumps({'state':'running','batches':results},indent=2)+'\n');print(json.dumps(result),flush=True)
 if end-ready>120:raise RuntimeError('Backlog exceeded bounded 120s; stop further arrivals')
pool.shutdown()
for w in workers.values():c.records.extend(w.records)
(out/'all-statements.json').write_text(json.dumps(c.records,indent=2)+'\n')
c.history();(out/'stream-summary.json').write_text(json.dumps({'state':'completed','batches':results,'scope':'six synthetic scheduled 100k object batches at 10s intervals; staging/queue/validation included; parallel independent writes plus explicit target-range predicates; no live producer, edge changes, replay/delete or concurrent readers; not long-run p95 admission'},indent=2)+'\n')
print('Completed scheduled stream',flush=True)
