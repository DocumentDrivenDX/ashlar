"""One property publication guarded at data and descriptor transactions."""
import json,time
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';out=B/'out/native/ashlar_fenced_graph_20261005_r12';c=DriverClient(out)
base=json.loads((B/'out/native/ashlar_receipt_recovery_20261005_r8r1/summary.json').read_text())['versions'];pred="source_system='pilot:0' AND rel_type_id=7 AND id=1000001"
assert c.sql('active-fence',f"SELECT epoch,owner FROM {F}.fence_r11 WHERE stream='graph'")==[['2','replacement']]
for t in ['edge_current','property_journal']:assert int(c.sql('before-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0])==base[F+'.'+t]
old=c.sql('old-carrier',f'SELECT * FROM {F}.edge_current WHERE {pred}');assert len(old)==1 and old[0][9]=='{"201":"recovered-fixture"}'
vector=dict(base);vector[F+'.edge_current']+=1;vector[F+'.property_journal']+=1
progress={k:{'epoch':'e','position':1} for k in ['fixture-multi','fixture-multi-edge','fixture-edge-structure','fixture-recovery','fixture-fenced']};revisions={'pilot:'+str(i):'r1' for i in range(5)}
def guard(epoch,owner):return f"IF (SELECT count(*) FROM {F}.fence_r11 WHERE stream='graph' AND epoch={epoch} AND owner='{owner}')<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='stale graph publisher'; END IF; UPDATE {F}.fence_r11 SET sequence=sequence+1 WHERE stream='graph' AND epoch={epoch} AND owner='{owner}';"
def data(epoch,owner):return f'''BEGIN ATOMIC
 {guard(epoch,owner)}
 IF (SELECT count(*) FROM {F}.edge_current WHERE {pred} AND props_json='{{"201":"recovered-fixture"}}' AND entity_version=3)<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='prior graph mismatch'; END IF;
 UPDATE {F}.edge_current SET props_json='{{"201":"fenced-fixture"}}',entity_version=4,source_feed='fixture-fenced',source_position=1,published_at=current_timestamp() WHERE {pred};
 INSERT INTO {F}.property_journal VALUES ('pilot:0','edge',7,1000001,201,4,'update',true,'"recovered-fixture"',true,'"fenced-fixture"','r1','fixture-fenced','e',1,1,'synthetic-fenced:1',current_timestamp());
 INSERT INTO {F}.recovery_receipt_r8 VALUES ('r12-1','{json.dumps(vector,separators=(',',':'))}','{json.dumps(progress,separators=(',',':'))}','{json.dumps(revisions,separators=(',',':'))}'); END'''
try:c.sql('stale-data',data(1,'publisher'))
except RuntimeError:
 status=c.records[-1]['response']['status'];assert status['state']=='FAILED' and 'stale graph publisher' in status['error']['message']
else:raise AssertionError('stale data writer accepted')
for t in ['edge_current','property_journal']:assert int(c.sql('after-stale-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0])==base[F+'.'+t]
assert c.sql('stale-receipt-absent',f"SELECT count(*) FROM {F}.recovery_receipt_r8 WHERE publication_id='r12-1'")==[['0']]
start=time.time();c.sql('fenced-data',data(2,'replacement'));data_s=time.time()-start
for t in ['edge_current','property_journal']:assert int(c.sql('committed-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0])==vector[F+'.'+t]
assert c.sql('old-pinned-during-gap',f'SELECT * FROM {F}.edge_current VERSION AS OF {base[F+".edge_current"]} WHERE {pred}')==old
assert c.sql('new-descriptor-absent',f"SELECT count(*) FROM {F}.manifest_r11 WHERE publication_id='r12-1'")==[['0']]
new=c.sql('new-carrier',f'SELECT * FROM {F}.edge_current VERSION AS OF {vector[F+".edge_current"]} WHERE {pred}');assert len(new)==1 and new[0][9]=='{"201":"fenced-fixture"}' and new[0][8]=='4'
for i in [0,1,2,3,4,5,6,7,10,11,13,16]:assert old[0][i]==new[0][i]
assert c.sql('exact-journal',f"SELECT count(*) FROM {F}.property_journal VERSION AS OF {vector[F+'.property_journal']} WHERE source_feed='fixture-fenced' AND property_id=201 AND entity_version=4 AND old_json='\"recovered-fixture\"' AND new_json='\"fenced-fixture\"'")==[['1']]
publication=f'''BEGIN ATOMIC {guard(2,'replacement')}
 IF (SELECT count(*) FROM {F}.manifest_r11 WHERE publication_id='r12-1')=0 THEN INSERT INTO {F}.manifest_r11 SELECT publication_id,'ashlar-delta/0.1-spike',versions_json,progress_json,revisions_json,'{{"fenced_graph":"passed"}}',current_timestamp() FROM {F}.recovery_receipt_r8 WHERE publication_id='r12-1'; END IF;
 IF (SELECT count(*) FROM {F}.manifest_r11 WHERE publication_id='r12-1')<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='descriptor count'; END IF; END'''
c.sql('fenced-descriptor',publication);elapsed=time.time()-start
r=c.sql('complete-readback',f"SELECT table_versions_json,source_progress_json,schema_revisions_json FROM {F}.manifest_r11 WHERE publication_id='r12-1'");assert len(r)==1 and [json.loads(x) for x in r[0]]==[vector,progress,revisions]
(out/'summary.json').write_text(json.dumps(dict(state='passed',versions=vector,data_s=data_s,through_descriptor_s=elapsed,stale_data_rejected=True,scope='single synthetic property event; same fence written at both phases; unchanged pinned old carrier during gap; no repeated rate, concurrent revocation, TTL or network-loss proof'),indent=2));c.history();c.close();print('Fenced graph publication passed')
