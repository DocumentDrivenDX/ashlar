"""Guard private recovery descriptor publication with a transactionally written fence."""
import json,threading,time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';out=B/'out/native/ashlar_fenced_recovery_20261005_r11';c=DriverClient(out)
c.sql('fence-ddl',f"CREATE TABLE {F}.fence_r11 (stream STRING,epoch BIGINT,owner STRING,sequence BIGINT) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
c.sql('seed',f"INSERT INTO {F}.fence_r11 VALUES ('graph',1,'publisher',0)")
c.sql('manifest-ddl',f"CREATE TABLE {F}.manifest_r11 USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported') AS SELECT * FROM {F}.publication_manifest WHERE false")
receipt=c.sql('receipt',f"SELECT * FROM {F}.recovery_receipt_r8 WHERE publication_id='r8-1'");assert len(receipt)==1
# This test publishes a previously validated fixed receipt, not another graph mutation.
statement=f'''BEGIN ATOMIC
 IF (SELECT count(*) FROM {F}.fence_r11 WHERE stream='graph' AND epoch=1 AND owner='publisher')<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='stale publisher'; END IF;
 UPDATE {F}.fence_r11 SET sequence=sequence+1 WHERE stream='graph' AND epoch=1 AND owner='publisher';
 IF (SELECT count(*) FROM {F}.manifest_r11 WHERE publication_id='r8-1')=0 THEN
 INSERT INTO {F}.manifest_r11 SELECT publication_id,'ashlar-delta/0.1-spike',versions_json,progress_json,revisions_json,'{{"fenced_recovery":"passed"}}',current_timestamp() FROM {F}.recovery_receipt_r8 WHERE publication_id='r8-1';
 END IF;
 IF (SELECT count(*) FROM {F}.manifest_r11 WHERE publication_id='r8-1')<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='descriptor cardinality'; END IF; END'''
clients=[DriverClient(out/f'recovery-{i}') for i in range(2)];barrier=threading.Barrier(2)
def recover(i):
 r=clients[i];assert r.sql('empty-snapshot',f'SELECT count(*) FROM {F}.manifest_r11')==[['0']];barrier.wait();start=time.time()
 try:r.sql('recover-race',statement);return {'state':'success','wall_s':time.time()-start}
 except RuntimeError:
  status=r.records[-1]['response']['status'];assert status['state']=='FAILED' and 'DELTA_CONCURRENT' in status['error']['message'],status
  return {'state':'conflict','wall_s':time.time()-start}
with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(recover,range(2)))
assert sum(x['state']=='success' for x in results)>=1
# Conflict loser may revalidate/recover using a new authoritative transaction; no data replay.
before=int(c.sql('manifest-before-idempotent',f'DESCRIBE HISTORY {F}.manifest_r11 LIMIT 1')[0][0]);c.sql('recover-again',statement)
after=int(c.sql('manifest-after-idempotent',f'DESCRIBE HISTORY {F}.manifest_r11 LIMIT 1')[0][0]);assert after==before
rows=c.sql('descriptor-readback',f"SELECT publication_id,table_versions_json,source_progress_json,schema_revisions_json FROM {F}.manifest_r11");assert len(rows)==1 and rows[0]==receipt[0]
c.sql('revoke',f"UPDATE {F}.fence_r11 SET epoch=2,owner='replacement' WHERE stream='graph'")
versions={t:int(c.sql('before-stale-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0]) for t in ['fence_r11','manifest_r11']}
try:c.sql('stale-recovery',statement)
except RuntimeError:
 status=c.records[-1]['response']['status'];assert status['state']=='FAILED' and 'stale publisher' in status['error']['message']
else:raise AssertionError('stale publication accepted')
for t,v in versions.items():assert int(c.sql('after-stale-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0])==v
for r in clients:c.records.extend(r.records);r.close()
(out/'all-statements.json').write_text(json.dumps(c.records,indent=2));c.history();c.close();(out/'summary.json').write_text(json.dumps(dict(state='passed',race=results,descriptor_rows=1,sequential_repeat_manifest_version_unchanged=True,stale_rejected=True,scope='private CM manifest guarded with conditional fence write; one duplicate-recovery race; fixed validated receipt; graph transaction not integrated; no lease/network-loss/scale guarantee'),indent=2));print('Fenced recovery passed')
