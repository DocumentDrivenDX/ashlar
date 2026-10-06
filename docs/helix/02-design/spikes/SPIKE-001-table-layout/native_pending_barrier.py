"""Private cooperative publisher pending-batch barrier and atomic release controls."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';out=B/'out/native/ashlar_pending_barrier_20261005_r22';c=DriverClient(out)
for t,cols in [('barrier_r22','stream STRING,epoch BIGINT,owner STRING,pending STRING,sequence BIGINT'),('barrier_data_r22','id BIGINT,batch STRING,payload STRING'),('barrier_receipt_r22','batch STRING,expected_count BIGINT'),('barrier_manifest_r22','batch STRING,version BIGINT')]:
 c.sql('ddl-'+t,f"CREATE TABLE {F}.{t} ({cols}) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported','delta.enableRowTracking'='true')")
c.sql('seed-fence',f"INSERT INTO {F}.barrier_r22 VALUES ('graph',1,'publisher',NULL,0)")
c.sql('seed-data',f"INSERT INTO {F}.barrier_data_r22 VALUES (1,'seed','original')")
def data(batch):return f'''BEGIN ATOMIC
 IF (SELECT count(*) FROM {F}.barrier_r22 WHERE stream='graph' AND epoch=1 AND owner='publisher' AND pending IS NULL)<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='publication pending'; END IF;
 UPDATE {F}.barrier_r22 SET pending='{batch}',sequence=sequence+1 WHERE stream='graph' AND epoch=1 AND owner='publisher' AND pending IS NULL;
 UPDATE {F}.barrier_data_r22 SET batch='{batch}',payload='changed' WHERE id=1;
 INSERT INTO {F}.barrier_receipt_r22 VALUES ('{batch}',1); END'''
c.sql('commit-pending',data('r22-1'));c.close();r=DriverClient(out/'restart')
assert r.sql('durable-pending',f"SELECT pending FROM {F}.barrier_r22 WHERE stream='graph'")==[['r22-1']]
before={t:int(r.sql('before-next-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0]) for t in ['barrier_r22','barrier_data_r22','barrier_receipt_r22']}
try:r.sql('blocked-next-batch',data('r22-2'))
except RuntimeError:
 status=r.records[-1]['response']['status'];assert status['state']=='FAILED' and 'publication pending' in status['error']['message']
else:raise AssertionError('Next batch escaped barrier')
for t,v in before.items():assert int(r.sql('after-next-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0])==v
version=r.sql('resolve-pending',f"SELECT count(*),count(_metadata.row_commit_version),min(_metadata.row_commit_version),max(_metadata.row_commit_version) FROM {F}.barrier_data_r22 WHERE batch='r22-1'");assert version[0][:2]==['1','1'] and version[0][2]==version[0][3];v=int(version[0][2])
publish=f'''BEGIN ATOMIC
 IF (SELECT count(*) FROM {F}.barrier_r22 WHERE stream='graph' AND epoch=1 AND owner='publisher' AND pending='r22-1')<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='pending owner mismatch'; END IF;
 UPDATE {F}.barrier_r22 SET sequence=sequence+1 WHERE stream='graph' AND pending='r22-1';
 INSERT INTO {F}.barrier_manifest_r22 VALUES ('r22-1',{v});
 UPDATE {F}.barrier_r22 SET pending=NULL WHERE stream='graph' AND pending='r22-1'; END'''
# Deliberate failure after insert+clear must roll both back.
try:r.sql('publication-fault',publish.replace('; END',"; SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='barrier publication fault'; END"))
except RuntimeError:
 status=r.records[-1]['response']['status'];assert status['state']=='FAILED' and 'barrier publication fault' in status['error']['message']
else:raise AssertionError('Missing injected failure')
assert r.sql('pending-after-fault',f"SELECT pending FROM {F}.barrier_r22 WHERE stream='graph'")==[['r22-1']]
assert r.sql('no-descriptor-after-fault',f'SELECT count(*) FROM {F}.barrier_manifest_r22')==[['0']]
r.sql('publish-and-clear',publish)
assert r.sql('clear-after-publish',f"SELECT pending FROM {F}.barrier_r22 WHERE stream='graph'")==[[None]]
assert r.sql('descriptor-exact',f'SELECT * FROM {F}.barrier_manifest_r22')==[['r22-1',str(v)]]
r.sql('next-batch-after-release',data('r22-2'))
assert r.sql('next-pending',f"SELECT pending FROM {F}.barrier_r22 WHERE stream='graph'")==[['r22-2']]
assert r.sql('old-published-carrier',f'SELECT * FROM {F}.barrier_data_r22 VERSION AS OF {v}')==[['1','r22-1','changed']]
c.records.extend(r.records);c.history();r.close();(out/'summary.json').write_text(json.dumps({'state':'passed','first_published_version':v,'next_batch_blocked_before_publication':True,'fault_rolls_back_descriptor_and_clear':True,'next_allowed_after_release':True,'scope':'private cooperative publisher protocol; one sequential restart/fault/next-batch test; next synthetic batch intentionally pending; no permissions/fencing race/network-loss/full-graph integration proof'},indent=2));print('Pending publication barrier passed')
