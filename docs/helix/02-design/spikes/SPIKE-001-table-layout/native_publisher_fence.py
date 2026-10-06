"""Private single-row fence contention and stale epoch guard; no graph integration claim."""
import json,threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';out=B/'out/native/ashlar_publisher_fence_20261005_r10';c=DriverClient(out)
c.sql('fence-ddl',f"CREATE TABLE {F}.fence_r10 (stream STRING,epoch BIGINT,owner STRING) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
c.sql('fence-seed',f"INSERT INTO {F}.fence_r10 VALUES ('graph',0,'none')")
c.sql('receipt-ddl',f"CREATE TABLE {F}.fence_receipt_r10 (owner STRING,epoch BIGINT) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
clients=[DriverClient(out/f'contender-{i}') for i in range(2)];barrier=threading.Barrier(2)
def acquire(i):
 r=clients[i];assert r.sql('initial-snapshot',f"SELECT epoch FROM {F}.fence_r10 WHERE stream='graph'")==[['0']];barrier.wait()
 try:
  r.sql('acquire',f"""BEGIN ATOMIC
 UPDATE {F}.fence_r10 SET epoch=1,owner='p{i}' WHERE stream='graph' AND epoch=0;
 IF (SELECT count(*) FROM {F}.fence_r10 WHERE stream='graph' AND epoch=1 AND owner='p{i}')<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='fence acquisition lost'; END IF;
 INSERT INTO {F}.fence_receipt_r10 VALUES ('p{i}',1); END""")
  return 'success'
 except RuntimeError:
  status=r.records[-1]['response']['status'];assert status['state']=='FAILED',status
  return 'terminal-rejection'
with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(acquire,range(2)))
assert sorted(results)==['success','terminal-rejection'],results
winner=c.sql('winner',f"SELECT epoch,owner FROM {F}.fence_r10 WHERE stream='graph'");assert len(winner)==1 and winner[0][0]=='1'
assert c.sql('one-receipt',f'SELECT owner,epoch FROM {F}.fence_receipt_r10')==[[winner[0][1],'1']]
# Explicit epoch bump models revocation, without TTL or clock claims.
c.sql('revoke',f"UPDATE {F}.fence_r10 SET epoch=2,owner='replacement' WHERE stream='graph' AND epoch=1")
versions={t:int(c.sql('before-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0]) for t in ['fence_r10','fence_receipt_r10']}
try:
 c.sql('stale-holder',f"""BEGIN ATOMIC
 IF (SELECT count(*) FROM {F}.fence_r10 WHERE stream='graph' AND epoch=1 AND owner='{winner[0][1]}')<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='stale fence'; END IF;
 UPDATE {F}.fence_r10 SET owner='{winner[0][1]}' WHERE stream='graph';
 INSERT INTO {F}.fence_receipt_r10 VALUES ('stale',1); END""")
except RuntimeError:
 status=c.records[-1]['response']['status'];assert status['state']=='FAILED' and 'stale fence' in status['error']['message']
else:raise AssertionError('Stale holder accepted')
for t,v in versions.items():assert int(c.sql('after-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0])==v
for r in clients:c.records.extend(r.records);r.close()
(out/'all-statements.json').write_text(json.dumps(c.records,indent=2));c.history();c.close();(out/'summary.json').write_text(json.dumps(dict(state='passed',acquisition_results=results,winner=winner[0][1],stale_rejected=True,scope='one two-session race and explicit revocation; private fence tables only; no graph/manifest integration, TTL, network-loss or repeated contention admission'),indent=2));print('Fence controls passed')
