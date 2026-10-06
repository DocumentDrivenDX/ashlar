"""Synthetic single-writer commit/descriptor-gap recovery, not disconnect or fencing proof."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';out=B/'out/native/ashlar_receipt_recovery_20261005_r8r1';c=DriverClient(out)
base=json.loads((B/'out/native/ashlar_structural_descriptor_20261005_r4/summary.json').read_text())['versions'];pred="source_system='pilot:0' AND rel_type_id=7 AND id=1000001"
for t in ['edge_current','property_journal']:assert int(c.sql('before-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0])==base[F+'.'+t]
old=c.sql('old-carrier',f'SELECT * FROM {F}.edge_current WHERE {pred}');assert len(old)==1
c.sql('receipt-ddl',f"CREATE TABLE IF NOT EXISTS {F}.recovery_receipt_r8 (publication_id STRING,versions_json STRING,progress_json STRING,revisions_json STRING) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
vector=dict(base);vector[F+'.edge_current']+=1;vector[F+'.property_journal']+=1
progress={k:{'epoch':'e','position':1} for k in ['fixture-multi','fixture-multi-edge','fixture-edge-structure','fixture-recovery']};revisions={'pilot:'+str(i):'r1' for i in range(5)}
assert c.sql('empty-receipt',f'SELECT count(*) FROM {F}.recovery_receipt_r8')==[['0']]
# Planned versions rely on exclusive synthetic writer; recovery validates actual history, never latest.
c.sql('atomic-data-and-receipt',f'''BEGIN ATOMIC
 INSERT INTO {F}.property_journal SELECT source_system,'edge',rel_type_id,id,201,3,'update',true,substring(to_json(array('hub-fixture')),2,length(to_json(array('hub-fixture')))-2),true,substring(to_json(array('recovered-fixture')),2,length(to_json(array('recovered-fixture')))-2),schema_revision,'fixture-recovery','e',1,1,'synthetic-recovery:1',current_timestamp() FROM {F}.edge_current WHERE {pred};
 UPDATE {F}.edge_current SET props_json='{{"201":"recovered-fixture"}}',entity_version=3,source_feed='fixture-recovery',source_epoch='e',source_position=1,published_at=current_timestamp() WHERE {pred};
 INSERT INTO {F}.recovery_receipt_r8 VALUES ('r8-1','{json.dumps(vector,separators=(',',':'))}','{json.dumps(progress,separators=(',',':'))}','{json.dumps(revisions,separators=(',',':'))}'); END''')
# Stop before descriptor publication and reopen through independent session.
c.close();r=DriverClient(out/'recovery');receipt=r.sql('durable-receipt',f"SELECT * FROM {F}.recovery_receipt_r8 WHERE publication_id='r8-1'");assert len(receipt)==1
assert json.loads(receipt[0][1])==vector
for table,version in vector.items():assert int(r.sql('history-'+table.split('.')[-1],f'DESCRIBE HISTORY {table} LIMIT 1')[0][0])==version
new=r.sql('pinned-new-carrier',f'SELECT * FROM {F}.edge_current VERSION AS OF {vector[F+".edge_current"]} WHERE {pred}');assert len(new)==1
# All fields except the six intentional property/publication changes remain exact.
for i in [0,1,2,3,4,5,6,7,10,11,16]:assert new[0][i]==old[0][i],i
assert new[0][8]=='3' and new[0][9]=='{"201":"recovered-fixture"}' and new[0][12:15]==['fixture-recovery','e','1']
assert r.sql('old-pinned-carrier',f'SELECT * FROM {F}.edge_current VERSION AS OF {base[F+".edge_current"]} WHERE {pred}')==old
assert r.sql('journal-pinned',f"SELECT count(*) FROM {F}.property_journal VERSION AS OF {vector[F+'.property_journal']} WHERE source_feed='fixture-recovery' AND event_ordinal=1 AND property_id=201 AND entity_version=3 AND old_json='\"hub-fixture\"' AND new_json='\"recovered-fixture\"'")==[['1']]
for attempt in range(2):
 rows=r.sql('descriptor-presence-'+str(attempt),f"SELECT table_versions_json,source_progress_json,schema_revisions_json FROM {F}.publication_manifest WHERE publication_id='r8-1'")
 if not rows:
  r.sql('recover-publish',f"INSERT INTO {F}.publication_manifest SELECT publication_id,'ashlar-delta/0.1-spike',versions_json,progress_json,revisions_json,'{{\"receipt_recovery\":\"passed\"}}',current_timestamp() FROM {F}.recovery_receipt_r8 WHERE publication_id='r8-1'")
 rows=r.sql('descriptor-readback-'+str(attempt),f"SELECT table_versions_json,source_progress_json,schema_revisions_json FROM {F}.publication_manifest WHERE publication_id='r8-1'");assert len(rows)==1 and [json.loads(x) for x in rows[0]]==[vector,progress,revisions]
assert int(r.sql('edge-no-replay',f'DESCRIBE HISTORY {F}.edge_current LIMIT 1')[0][0])==vector[F+'.edge_current']
assert int(r.sql('journal-no-replay',f'DESCRIBE HISTORY {F}.property_journal LIMIT 1')[0][0])==vector[F+'.property_journal']
(out/'summary.json').write_text(json.dumps(dict(state='passed',versions=vector,recovery_attempts=2,data_replays=0,descriptor_rows=1,scope='one synthetic property update; deliberate post-commit session restart; exclusive writer only; no network loss/fencing/concurrent recovery guarantee'),indent=2));c.records.extend(r.records);c.history();r.close();print('Receipt descriptor recovery passed')
