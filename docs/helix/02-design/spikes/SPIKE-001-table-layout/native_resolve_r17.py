"""Bounded recovery of r17 committed property batch; no data replay."""
import json
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS,post
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';out=B/'out/native/ashlar_resolve_r17_20261005_r19';c=DriverClient(out)
receipt=c.sql('invalid-receipt',f"SELECT versions_json,progress_json,revisions_json FROM {F}.recovery_receipt_r8 WHERE publication_id='r17-1'");assert len(receipt)==1;vector=json.loads(receipt[0][0]);assert vector[F+'.property_journal']==12
assert c.sql('fence',f"SELECT epoch,owner FROM {F}.fence_r11 WHERE stream='graph'")==[['2','replacement']]
resolved={};histories={}
for t,base in [('edge_current',13),('property_journal',11)]:
 rows=c.sql('history-'+t,f'DESCRIBE HISTORY {F}.{t}');versions=sorted(int(r[0]) for r in rows if int(r[0])>=base);assert versions and versions==list(range(base,max(versions)+1)),versions;histories[t]=max(versions)
 found=[]
 for v in versions:
  count=int(c.sql(f'marker-{t}-{v}',f"SELECT count(*) FROM {F}.{t} VERSION AS OF {v} WHERE source_feed='fixture-fused300k' AND source_epoch='e' AND source_position=1")[0][0]);assert count in [0,300000],(t,v,count)
  if count:found.append(v)
 assert found and found==versions[versions.index(found[0]):],found;resolved[t]=found[0];vector[F+'.'+t]=found[0]
assert resolved=={'edge_current':14,'property_journal':15}
# Snapshot uniqueness and exact changed rows plus all native journal semantics.
key='s.source_system=e.source_system AND s.rel_type_id=e.rel_type_id AND s.id=e.id'
assert c.sql('changed-journal-full',f"SELECT /*+ BROADCAST(s,j) */ count(*),count(DISTINCT struct(s.source_system,s.rel_type_id,s.id)),count_if({post(1)}) FROM {F}.stage_r17_1 s JOIN {F}.edge_current VERSION AS OF 14 e ON {key} LEFT JOIN {F}.property_journal VERSION AS OF 15 j ON s.source_system=j.source_system AND s.rel_type_id=j.type_id AND s.id=j.id AND j.source_feed='fixture-fused300k' AND j.source_position=1")==[['300000','300000','0']]
assert c.sql('all-keys',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {F}.edge_current VERSION AS OF 14')==[['10019981','10019981']]
diff=' OR '.join(f'o.{x} IS DISTINCT FROM e.{x}' for x in COLS)
assert c.sql('untouched-full17',f'SELECT count(*),count_if({diff}) FROM {F}.edge_current VERSION AS OF 13 o JOIN {F}.edge_current VERSION AS OF 14 e ON o.source_system=e.source_system AND o.rel_type_id=e.rel_type_id AND o.id=e.id LEFT ANTI JOIN {F}.stage_r17_1 s ON s.source_system=o.source_system AND s.rel_type_id=o.rel_type_id AND s.id=o.id')==[['9719981','0']]
assert c.sql('journal-origins',f"SELECT count(*),count(DISTINCT struct(source_feed,source_epoch,source_position,event_ordinal)) FROM {F}.property_journal VERSION AS OF 15 WHERE source_feed='fixture-fused300k'")==[['300000','300000']]
# Refuse changed physical history rather than infer later logical state is harmless.
for t,v in histories.items():assert int(c.sql('history-recheck-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0])==v
progress=json.loads(receipt[0][1]);revisions=json.loads(receipt[0][2]);assert progress['fixture-fused300k']=={'epoch':'e','position':1}
c.sql('corrected-publication',f'''BEGIN ATOMIC
 IF (SELECT count(*) FROM {F}.fence_r11 WHERE stream='graph' AND epoch=2 AND owner='replacement')<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='stale recovery'; END IF;
 UPDATE {F}.fence_r11 SET sequence=sequence+1 WHERE stream='graph' AND epoch=2 AND owner='replacement';
 IF (SELECT count(*) FROM {F}.manifest_r11 WHERE publication_id='r17-1-resolved')=0 THEN INSERT INTO {F}.manifest_r11 VALUES ('r17-1-resolved','ashlar-delta/0.1-spike','{json.dumps(vector,separators=(',',':'))}','{json.dumps(progress,separators=(',',':'))}','{json.dumps(revisions,separators=(',',':'))}','{{"marker_resolution":"passed","invalid_receipt":"r17-1"}}',current_timestamp()); END IF; END''')
r=c.sql('corrected-readback',f"SELECT table_versions_json,source_progress_json,schema_revisions_json FROM {F}.manifest_r11 WHERE publication_id='r17-1-resolved'");assert len(r)==1 and [json.loads(x) for x in r[0]]==[vector,progress,revisions]
for t,v in histories.items():assert int(c.sql('no-replay-'+t,f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0])==v
(out/'summary.json').write_text(json.dumps({'state':'passed','resolved':resolved,'versions':vector,'untouched_full17':9719981,'journal_exact':300000,'publication':'r17-1-resolved','data_replays':0,'scope':'bounded single-writer recovery with complete history and no subsequent logical writes; manual pending-publication barrier, not production enforcement or original rate admission'},indent=2));c.history();c.close();print('R17 corrected publication verified')
