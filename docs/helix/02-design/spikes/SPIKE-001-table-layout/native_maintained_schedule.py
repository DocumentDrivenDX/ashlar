"""Two arrivals with maintenance inside publication clock and an old-vector reader."""
import argparse,hashlib,json,threading,time
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS,post
p=argparse.ArgumentParser();p.add_argument('layout',choices=['lc','partition']);layout=p.parse_args().layout
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';N='client_dev.ashlar_partition_zorder_20261005_r29';ns=F if layout=='lc' else N
prepared=json.loads((B/'out/native/ashlar_maintained_stage_prepare_20261005_r39/summary.json').read_text());assert prepared['state']=='passed'
published=json.loads((B/'out/native/ashlar_partition_maintenance_publish_20261005_r38/summary.json').read_text());base=next(r['versions'] for r in published['publications'] if r['layout']==layout)
out=B/('out/native/ashlar_maintained_schedule_20261005_r40_'+layout);c=DriverClient(out);c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
for table in ['edge_current','property_journal']:assert int(c.sql('initial-'+table,f'DESCRIBE HISTORY {ns}.{table} LIMIT 1')[0][0])==base[ns+'.'+table]
assert c.sql('initial-barrier',f"SELECT pending,sequence FROM {N}.barrier_r32 WHERE stream='{layout}' AND epoch=1 AND owner='publisher'")==[[None,'4']]
receipt=N+'.receipt_r40_'+layout
c.sql('receipt-ddl',f"CREATE TABLE {receipt} (batch BIGINT,stage_name STRING,expected_count BIGINT,progress_json STRING,revisions_json STRING) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
keys=[('pilot:'+str((i%10)//2),7+i%2,240001+(i*7919)%30000) for i in range(51)]
where=' OR '.join(f"(source_system='{s}' AND rel_type_id={r} AND id={i})" for s,r,i in keys)
priorrows=c.sql('reader-prior',f"SELECT {','.join(COLS)} FROM {N}.stage_r39_1 WHERE {where}");priorrows={(r[0],int(r[1]),int(r[2])):[r] for r in priorrows};assert len(priorrows)==51
stop=threading.Event();primed=threading.Event();reader_errors=[];reader_count=[]
def read_loop():
 rc=None
 try:
  rc=DriverClient(out/'reader');rc.sql('statement-cap','SET STATEMENT_TIMEOUT=180');n=0
  while not stop.is_set():
   source,rel,id=keys[n%51];h=hashlib.sha256(json.dumps(dict(source_system=source,rel_type_id=rel,id=id),separators=(',',':')).encode()).hexdigest();extra='' if layout=='lc' else f' AND lookup_bucket={int(h[0],16)//4}'
   phase='prime' if n<51 else 'read';rep=n if n<51 else n-51
   assert rc.sql(f'{phase}-{rep}',f"SELECT {','.join(COLS)} FROM {ns}.edge_current VERSION AS OF {base[ns+'.edge_current']} WHERE lookup_hash='{h}' AND source_system='{source}' AND rel_type_id={rel} AND id={id}{extra}")==priorrows[(source,rel,id)]
   n+=1;reader_count[:]=[n]
   if n==51:primed.set()
   if n>51:stop.wait(.5)
 except Exception as e:reader_errors.append(str(e));primed.set()
 finally:
  if rc is not None:rc.history();rc.close()
thread=threading.Thread(target=read_loop);thread.start();assert primed.wait(120) and not reader_errors
key='s.source_system=e.source_system AND s.rel_type_id=e.rel_type_id AND s.id=e.id';prior=' OR '.join(f's.{x} IS DISTINCT FROM e.{x}' for x in COLS)
old="to_json(array(get_json_object(props_json,'$.201')))";new="to_json(array(get_json_object(new_props,'$.201')))";revisions={'pilot:'+str(i):'r1' for i in range(5)}
results=[];t0=time.time()
try:
 for item in prepared['stages']:
  batch=item['batch'];stage=item['table'];ready=t0+batch*30
  while time.time()<ready:time.sleep(min(.5,ready-time.time()))
  assert not reader_errors
  begun=time.time();guard=post(batch).replace("'fixture-fused300k'","'fixture-maintained'").replace('IS DISTINCT FROM 7','IS DISTINCT FROM 9').replace(f"'synthetic-fused:{batch}'",f"'synthetic-maintained:{batch}'")
  progress={k:{'epoch':'e','position':1} for k in ['fixture-multi','fixture-multi-edge','fixture-edge-structure','fixture-recovery','fixture-fenced','fixture-layout-compare']};progress.update({'fixture-scheduled-fenced':{'epoch':'e','position':4},'fixture-broadcast300k':{'epoch':'e','position':3},'fixture-fused300k':{'epoch':'e','position':3},'fixture-maintained':{'epoch':'e','position':batch}})
  extra='' if layout=='lc' else ',CAST(conv(substr(lookup_hash,1,1),16,10) AS INT) DIV 4'
  (out/'progress.json').write_text(json.dumps({'state':'applying','batch':batch,'completed':results},indent=2))
  c.sql('atomic-data-'+str(batch),f'''BEGIN ATOMIC
 IF (SELECT count(*) FROM {N}.barrier_r32 WHERE stream='{layout}' AND epoch=1 AND owner='publisher' AND pending IS NULL)<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='pending maintained batch'; END IF;
 UPDATE {N}.barrier_r32 SET pending={batch},sequence=sequence+1 WHERE stream='{layout}' AND epoch=1 AND owner='publisher' AND pending IS NULL;
 IF (SELECT /*+ BROADCAST(s) */ count(*)<>300000 OR count(DISTINCT struct(s.source_system,s.rel_type_id,s.id))<>300000 OR count_if({prior})<>0 FROM {stage} s JOIN {ns}.edge_current e ON {key}) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='prior mismatch'; END IF;
 DELETE FROM {ns}.edge_current e WHERE EXISTS (SELECT 1 FROM {stage} s WHERE {key});
 INSERT INTO {ns}.edge_current SELECT source_system,rel_type_id,id,source_type,source_id,target_type,target_id,schema_revision,cast(9 AS BIGINT),new_props,retained_json,order_key,'fixture-maintained','e',{batch},current_timestamp(),lookup_hash{extra} FROM {stage};
 INSERT INTO {ns}.property_journal SELECT source_system,'edge',rel_type_id,id,201,9,'update',true,substring({old},2,length({old})-2),true,substring({new},2,length({new})-2),schema_revision,'fixture-maintained','e',{batch},ordinal,'synthetic-maintained:{batch}',current_timestamp() FROM {stage};
 IF (SELECT /*+ BROADCAST(s,j) */ count(*)<>300000 OR count(DISTINCT struct(s.source_system,s.rel_type_id,s.id))<>300000 OR count_if({guard})<>0 FROM {stage} s JOIN {ns}.edge_current e ON {key} LEFT JOIN {ns}.property_journal j ON s.source_system=j.source_system AND s.rel_type_id=j.type_id AND s.id=j.id AND j.source_feed='fixture-maintained' AND j.source_position={batch}) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='post mismatch'; END IF;
 IF (SELECT count(*)<>300000 OR count(DISTINCT struct(source_feed,source_epoch,source_position,event_ordinal))<>300000 FROM {ns}.property_journal WHERE source_feed='fixture-maintained' AND source_position={batch}) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='origin mismatch'; END IF;
 INSERT INTO {receipt} VALUES ({batch},'{stage}',300000,'{json.dumps(progress,separators=(',',':'))}','{json.dumps(revisions,separators=(',',':'))}'); END''')
  c.sql('maintenance-'+str(batch),f'OPTIMIZE {ns}.edge_current'+(' ZORDER BY (lookup_hash)' if layout=='partition' else ''))
  ev=int(c.sql('maintained-version-'+str(batch),f'DESCRIBE HISTORY {ns}.edge_current LIMIT 1')[0][0]);vector=dict(base);vector[ns+'.edge_current']=ev
  r=c.sql('resolve-journal-'+str(batch),f"SELECT count(*),count(_metadata.row_commit_version),min(_metadata.row_commit_version),max(_metadata.row_commit_version),count(DISTINCT _metadata.row_commit_version) FROM {ns}.property_journal WHERE source_feed='fixture-maintained' AND source_epoch='e' AND source_position={batch}");assert r[0][:2]==['300000','300000'] and r[0][2]==r[0][3] and r[0][4]=='1';jv=int(r[0][2]);vector[ns+'.property_journal']=jv
  assert c.sql('maintained-post-'+str(batch),f"SELECT /*+ BROADCAST(s,j) */ count(*),count(DISTINCT struct(s.source_system,s.rel_type_id,s.id)),count_if({guard}) FROM {stage} s JOIN {ns}.edge_current VERSION AS OF {ev} e ON {key} LEFT JOIN {ns}.property_journal VERSION AS OF {jv} j ON s.source_system=j.source_system AND s.rel_type_id=j.type_id AND s.id=j.id AND j.source_feed='fixture-maintained' AND j.source_position={batch}")==[['300000','300000','0']]
  c.sql('publish-'+str(batch),f'''BEGIN ATOMIC
 IF (SELECT count(*) FROM {N}.barrier_r32 WHERE stream='{layout}' AND epoch=1 AND owner='publisher' AND pending={batch})<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='pending publication mismatch'; END IF;
 UPDATE {N}.barrier_r32 SET sequence=sequence+1 WHERE stream='{layout}' AND epoch=1 AND owner='publisher' AND pending={batch};
 IF (SELECT count(*) FROM {receipt} WHERE batch={batch} AND expected_count=300000)<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='receipt mismatch'; END IF;
 INSERT INTO {N}.manifest_r32 SELECT 'r40-{layout}-{batch}','ashlar-delta/0.1-spike','{json.dumps(vector,separators=(',',':'))}',progress_json,revisions_json,'{{"maintenance_and_exact_changed_carriers":"passed"}}',current_timestamp() FROM {receipt} WHERE batch={batch};
 UPDATE {N}.barrier_r32 SET pending=NULL WHERE stream='{layout}' AND epoch=1 AND owner='publisher' AND pending={batch}; END''')
  ended=time.time();result={'batch':batch,'queue_s':begun-ready,'processing_s':ended-begun,'oldest_freshness_s':ended-(ready-30),'versions':vector};results.append(result);(out/'progress.json').write_text(json.dumps({'state':'published','completed':results},indent=2));print(json.dumps(result),flush=True)
finally:
 stop.set();thread.join();c.history();c.close()
assert not reader_errors
(out/'summary.json').write_text(json.dumps({'state':'completed','layout':layout,'batches':results,'reader_calls':reader_count[0],'scope':'two 30s arrivals, prebuilt stages, maintenance and changed-row validation inside freshness clock; old-vector full-carrier reader concurrently; exhaustive untouched/history/descriptor follow-up separate, no sustained admission'},indent=2));print('Maintenance-inclusive schedule completed',flush=True)
