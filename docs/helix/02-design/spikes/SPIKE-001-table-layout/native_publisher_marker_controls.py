"""Native rollback controls for exact conditional MERGE plus exclusive batch marker."""
import json
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS,post
B=Path(__file__).resolve().parent;N='client_dev.ashlar_publisher_marker_20261006_r50';source='client_dev.ashlar_lc64_20261005_r42';out=B/'out/native/ashlar_publisher_marker_20261006_r50';c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180');c.sql('schema',f'CREATE SCHEMA {N}')
cases=['clean','preexisting-marker','marker-outside-stage']
guard=post(1).replace("'fixture-fused300k'","'zz-spike-conditional'").replace('IS DISTINCT FROM 7','IS DISTINCT FROM 11').replace("'synthetic-fused:1'","'synthetic-conditional:1'")
condition=' AND '.join(f'e.{x} IS NOT DISTINCT FROM s.{x}' for x in COLS);key='s.source_system=e.source_system AND s.rel_type_id=e.rel_type_id AND s.id=e.id'
old="to_json(array(get_json_object(props_json,'$.201')))";new="to_json(array(get_json_object(new_props,'$.201')))";results=[]
for index,case in enumerate(cases):
 e=f'{N}.edge_{index}';s=f'{N}.stage_{index}';j=f'{N}.journal_{index}';f=f'{N}.fence_{index}';r=f'{N}.receipt_{index}'
 seed=f"SELECT {','.join(COLS)} FROM {source}.edge_current VERSION AS OF 1 WHERE source_system='pilot:0' AND rel_type_id=7 AND id BETWEEN 330001 AND 330003"
 if case=='duplicate-target':seed+=f" UNION ALL SELECT {','.join(COLS)} FROM {source}.edge_current VERSION AS OF 1 WHERE source_system='pilot:0' AND rel_type_id=7 AND id=330001"
 c.sql(case+'-edge',f"CREATE TABLE {e} USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported','delta.enableRowTracking'='true') AS {seed}")
 c.sql(case+'-physical-marker',f'ALTER TABLE {e} ADD COLUMNS (apply_batch_id STRING)')
 c.sql(case+'-stage',f"CREATE TABLE {s} USING DELTA AS SELECT *, '{{\"201\":\"conditional-control\"}}' new_props,id-330000 ordinal FROM {source}.edge_current VERSION AS OF 1 WHERE source_system='pilot:0' AND rel_type_id=7 AND id BETWEEN 330001 AND 330002")
 edits={'stale-props':"props_json='{}'",'stale-retained':"retained_json='{}'",'stale-endpoint':'target_id=target_id+1','stale-version':'entity_version=entity_version+1','stale-origin':'source_position=source_position+1','stale-null-order':"order_key='bad'",'wrong-hash':"lookup_hash='bad'",'duplicate-ordinal':'ordinal=1'}
 if case in edits:c.sql(case+'-fault',f'UPDATE {s} SET {edits[case]} WHERE id=330001')
 if case=='duplicate-ordinal':c.sql(case+'-fault2',f'UPDATE {s} SET ordinal=1 WHERE id=330002')
 if case=='duplicate-stage':c.sql(case+'-fault',f'INSERT INTO {s} SELECT * FROM {s} WHERE id=330001')
 if case=='missing-target':c.sql(case+'-fault',f'DELETE FROM {e} WHERE id=330001')
 if case in ['preexisting-marker','marker-outside-stage']:c.sql(case+'-fault',f"UPDATE {e} SET apply_batch_id='publisher:1:1' WHERE id={330001 if case=='preexisting-marker' else 330003}")
 c.sql(case+'-journal',f'CREATE TABLE {j} LIKE {source}.property_journal');c.sql(case+'-journal-cm',f"ALTER TABLE {j} SET TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
 c.sql(case+'-journal-marker',f'ALTER TABLE {j} ADD COLUMNS (apply_batch_id STRING)')
 c.sql(case+'-fence',f"CREATE TABLE {f} (pending BIGINT,sequence BIGINT) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')");c.sql(case+'-seed',f'INSERT INTO {f} VALUES (NULL,0)')
 c.sql(case+'-receipt',f"CREATE TABLE {r} (batch BIGINT,expected_count BIGINT) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')")
 if case=='clean':
  c.sql('same-native-origin',f"UPDATE {e} SET source_feed='zz-spike-conditional',source_epoch='e',source_position=1,apply_batch_id='old-publisher-batch' WHERE id=330003")
  c.sql('old-native-history',f"INSERT INTO {j} SELECT source_system,'edge',rel_type_id,id,201,0,'update',true,'null',true,'null',schema_revision,'zz-spike-conditional','e',1,999,'opaque-prior-event',current_timestamp(),'old-publisher-batch' FROM {e} WHERE id=330003")
  old_history=c.sql('old-history',f"SELECT * FROM {j} WHERE apply_batch_id='old-publisher-batch'")
 tables=[e,j,f,r];versions=[int(c.sql(case+'-before-v'+str(i),f'DESCRIBE HISTORY {t} LIMIT 1')[0][0]) for i,t in enumerate(tables)]
 before=c.sql(case+'-before-rows',f'SELECT x.*,x._metadata.row_id rid,x._metadata.row_commit_version rcv FROM {e} x ORDER BY id,rid')
 tamper=f"UPDATE {j} SET old_json='null' WHERE id=330001;" if case=='bad-journal-old' else f'DELETE FROM {j} WHERE id=330001;' if case=='missing-journal' else ''
 force="SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced after validated writes';" if case=='forced-failure' else ''
 statement=f'''BEGIN ATOMIC
 IF (SELECT count(*) FROM {f} WHERE pending IS NULL AND sequence=0)<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='fence mismatch'; END IF;
 UPDATE {f} SET pending=1,sequence=sequence+1 WHERE pending IS NULL AND sequence=0;
 IF (SELECT count(*) FROM {e} WHERE apply_batch_id='publisher:1:1')<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='marker already exists'; END IF;
 MERGE INTO {e} e USING {s} s ON {key} WHEN MATCHED AND ({condition}) THEN UPDATE SET e.props_json=s.new_props,e.entity_version=11,e.source_feed='zz-spike-conditional',e.source_epoch='e',e.source_position=1,e.published_at=current_timestamp(),e.apply_batch_id='publisher:1:1';
 INSERT INTO {j} SELECT source_system,'edge',rel_type_id,id,201,11,'update',true,substring({old},2,length({old})-2),true,substring({new},2,length({new})-2),schema_revision,'zz-spike-conditional','e',1,ordinal,'synthetic-conditional:1',current_timestamp(),'publisher:1:1' FROM {s};
 {tamper}
 IF (SELECT /*+ BROADCAST(s,j) */ count(*)<>2 OR count(DISTINCT struct(s.source_system,s.rel_type_id,s.id))<>2 OR count_if({guard} OR e.apply_batch_id IS DISTINCT FROM 'publisher:1:1')<>0 FROM {s} s JOIN {e} e ON {key} LEFT JOIN {j} j ON s.source_system=j.source_system AND s.rel_type_id=j.type_id AND s.id=j.id AND j.source_feed='zz-spike-conditional' AND j.source_epoch='e' AND j.source_position=1 AND j.apply_batch_id='publisher:1:1') THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='post mismatch'; END IF;
 IF (SELECT count(*)<>2 OR count(DISTINCT struct(source_feed,source_epoch,source_position,event_ordinal))<>2 FROM {j} WHERE apply_batch_id='publisher:1:1') THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='origin mismatch'; END IF;
 {force}
 INSERT INTO {r} VALUES (1,2); END'''
 if case=='clean':
  c.sql(case+'-apply',statement)
  after=c.sql(case+'-after-rows',f'SELECT x.*,x._metadata.row_id rid,x._metadata.row_commit_version rcv FROM {e} x ORDER BY id,rid')
  assert after[2]==before[2]
  for a,b in zip(after[:2],before[:2]):
   assert all(a[i]==b[i] for i in range(17) if i not in [8,9,12,13,14,15]) and a[8]=='11' and a[9]=='{"201":"conditional-control"}' and a[12:15]==['zz-spike-conditional','e','1'] and a[15] is not None and a[17]=='publisher:1:1' and a[18]==b[18] and a[19]!=b[19]
  assert c.sql(case+'-receipt-check',f'SELECT * FROM {r}')==[['1','2']]
  assert c.sql('preserved-old-history',f"SELECT * FROM {j} WHERE apply_batch_id='old-publisher-batch'")==old_history
 else:
  try:c.sql(case+'-apply',statement)
  except RuntimeError:
   assert c.records[-1]['response']['status']['state']=='FAILED'
   error=str(c.records[-1]['response']['status']['error']);assert any(x in error for x in ['post mismatch','origin mismatch','marker already exists','forced after validated writes','DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE']),error
  else:raise AssertionError('Fault unexpectedly committed: '+case)
  after_v=[int(c.sql(case+'-after-v'+str(i),f'DESCRIBE HISTORY {t} LIMIT 1')[0][0]) for i,t in enumerate(tables)];assert after_v==versions
  assert c.sql(case+'-after-rows',f'SELECT x.*,x._metadata.row_id rid,x._metadata.row_commit_version rcv FROM {e} x ORDER BY id,rid')==before
  assert c.sql(case+'-rollback-counts',f'SELECT (SELECT count(*) FROM {j}),(SELECT count(*) FROM {r}),(SELECT count(*) FROM {f} WHERE pending IS NULL AND sequence=0)')==[['0','0','1']]
 results.append(case);(out/'progress.json').write_text(json.dumps({'passed':results},indent=2));print(case,'passed',flush=True)
c.history();c.close();(out/'summary.json').write_text(json.dumps({'state':'passed','cases':results,'scope':'separate publisher apply_batch_id on canonical/journal; identical producer feed/epoch/position on unrelated old rows is preserved and does not block new batch; current marker reuse rejects staged/outside row with four-table rollback; tiny fixture, no producer guarantee or performance admission'},indent=2));print('Conditional MERGE native controls passed',flush=True)
