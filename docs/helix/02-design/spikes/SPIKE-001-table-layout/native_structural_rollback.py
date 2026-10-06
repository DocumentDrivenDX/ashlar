"""Explicit server-side fault after structural data/receipt writes; no disconnect recovery claim."""
import json,time
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';out=B/'out/native/ashlar_structural_rollback_20261005_r7';c=DriverClient(out)
s=json.loads((B/'out/native/ashlar_structural_descriptor_20261005_r4/summary.json').read_text());assert s['state']=='passed'
tables=['edge_current','adjacency','out_degree_r1','property_journal','tombstone_r3','receipt','publication_manifest']
before={t:int(c.sql('before-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0]) for t in tables}
assert all(before[t]==s['versions'][F+'.'+t] for t in tables[:5])
root="source_system='pilot:0' AND rel_type_id=7 AND source_type=1 AND source_id=200001"
expected=c.sql('hub-before',f'SELECT count(*) FROM {F}.edge_current WHERE {root}');assert expected==[['20001']]
manifest=c.sql('descriptor-before',f"SELECT * FROM {F}.publication_manifest WHERE publication_id='r3-2'");assert len(manifest)==1
statement=f'''BEGIN ATOMIC
 DELETE FROM {F}.edge_current WHERE {root};
 DELETE FROM {F}.adjacency WHERE {root};
 DELETE FROM {F}.out_degree_r1 WHERE {root};
 DELETE FROM {F}.property_journal WHERE source_feed='fixture-edge-structure';
 DELETE FROM {F}.tombstone_r3 WHERE source_feed='fixture-edge-structure';
 INSERT INTO {F}.receipt VALUES (7,current_timestamp());
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ashlar r7 injected structural rollback'; END'''
start=time.time()
try:c.sql('fault-after-all-writes',statement)
except RuntimeError:
 rec=c.records[-1];status=rec['response']['status'];assert status['state']=='FAILED' and 'ashlar r7 injected structural rollback' in status['error']['message'],status
else:raise AssertionError('Expected injected fault')
failed_s=time.time()-start
# Same terminal server result, no retries. Latest versions and contents must agree.
after={t:int(c.sql('after-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0]) for t in tables};assert after==before,(before,after)
assert c.sql('hub-after',f'SELECT count(*) FROM {F}.edge_current WHERE {root}')==expected
assert c.sql('hub-adjacency-after',f'SELECT count(*) FROM {F}.adjacency WHERE {root}')==expected
assert c.sql('hub-degree-after',f'SELECT out_degree FROM {F}.out_degree_r1 WHERE {root}')==expected
assert c.sql('journal-after',f"SELECT count(*) FROM {F}.property_journal WHERE source_feed='fixture-edge-structure'")==[['20021']]
assert c.sql('tombstones-after',f"SELECT count(*) FROM {F}.tombstone_r3 WHERE source_feed='fixture-edge-structure'")==[['20']]
assert c.sql('receipt-absent',f'SELECT count(*) FROM {F}.receipt WHERE batch=7')==[['0']]
assert c.sql('descriptor-after',f"SELECT * FROM {F}.publication_manifest WHERE publication_id='r3-2'")==manifest
(out/'summary.json').write_text(json.dumps(dict(state='passed',before=before,after=after,injected_failure_s=failed_s,scope='explicit SIGNAL after six participating table writes; manifest outside transaction; unchanged Delta versions and affected contents; not lost-response/cancellation recovery'),indent=2));c.history();c.close();print('Structural rollback verified',flush=True)
