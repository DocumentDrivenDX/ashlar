"""Synthetic publication-reference validation; no real Truss adapter claim."""
import json,hashlib
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent
N='client_dev.ashlar_layout_v03_20261006_r73';OLD='client_dev.ashlar_layout_v02_20261006_r65'
out=B/'out/native/ashlar_layout_v03_references_20261006_r74';c=Client(out)
assert json.loads((B/'out/native/ashlar_layout_v03_ddl_20261006_r73_resume/summary.json').read_text())['state']=='passed'
tables=['object_current','edge_current','property_journal','tombstone','source_record','adjacency_forward','adjacency_reverse','degree_summary','publication_manifest']
for t in tables:assert c.sql('empty-'+t,f'SELECT count(*) FROM {N}.{t}')==[['0']]
def lit(s):return "decode(unhex('"+s.encode('utf8').hex()+"'),'UTF-8')"
def cursor(seq):return f"to_json(named_struct('xid','1042','seq',cast({seq} AS STRING)))"
def ref(seq):return f"to_json(array('change','1042',cast({seq} AS STRING)))"
node_seq='row_number() OVER (ORDER BY type_id,id)'
c.sql('nodes',f"INSERT INTO {N}.object_current SELECT source_system,type_id,id,logical_key_json,schema_revision,entity_version,props_json,retained_json,root_id,'v03-fixture','e',NULL,published_at,lookup_hash,'r74:1',{cursor(node_seq)},{ref(node_seq)} FROM {OLD}.object_current VERSION AS OF 1")
c.sql('edges',f"INSERT INTO {N}.edge_current SELECT source_system,rel_type_id,id,source_type,source_id,target_type,target_id,schema_revision,entity_version,props_json,retained_json,order_key,'v03-fixture','e',NULL,published_at,lookup_hash,'r74:1',{cursor('3+id')},{ref('3+id')} FROM {OLD}.edge_current VERSION AS OF 1")
c.sql('journal',f"INSERT INTO {N}.property_journal SELECT source_system,entity_kind,type_id,id,property_id,entity_version,operation,old_present,old_json,new_present,new_json,schema_revision,'v03-fixture','e',NULL,event_ordinal,source_time_text,published_at,'r74:1',{cursor('event_ordinal')},{ref('event_ordinal')} FROM {OLD}.property_journal VERSION AS OF 2")
# These synthetic envelopes carry full supplied state, not the native Truss wire format.
for t,kind,typ in [('object_current','node','type_id'),('edge_current','edge','rel_type_id')]:
 fields=f"'fixture_kind','full-carrier','entity_kind','{kind}','type_id',cast({typ} AS STRING),'id',cast(id AS STRING),'props_json',props_json,'retained_json',retained_json"
 c.sql('raw-'+kind,f"INSERT INTO {N}.source_record SELECT source_feed,source_epoch,source_delivery_id,'fixture-carrier',source_cursor_json,payload,sha2(payload,256),schema_revision,current_timestamp(),'r74:1' FROM (SELECT *,to_json(named_struct({fields})) payload FROM {N}.{t})")
c.sql('tombstone',f"INSERT INTO {N}.tombstone VALUES ('pilot','node',1,99,2,'v03-fixture','e',NULL,{cursor('7')},{ref('7')})")
payload='{"fixture_kind":"delete","entity_kind":"node","type_id":"1","id":"99","entity_version":"2"}'
c.sql('raw-delete',f"INSERT INTO {N}.source_record SELECT 'v03-fixture','e',{ref('7')},'fixture-delete',{cursor('7')},{lit(payload)},sha2({lit(payload)},256),'r1',current_timestamp(),'r74:1'")
for t in ['adjacency_forward','adjacency_reverse']:c.sql('copy-'+t,f'INSERT INTO {N}.{t} SELECT * FROM {OLD}.{t} VERSION AS OF 1')
c.sql('copy-degree',f'INSERT INTO {N}.degree_summary SELECT * FROM {OLD}.degree_summary VERSION AS OF 1')
# Publication validator checks exact native cursor components, not JSON member order.
def bad_query(table):
 return f"SELECT count(*) FROM {N}.{table} p LEFT JOIN {N}.source_record r ON p.source_feed=r.source_feed AND p.source_epoch=r.source_epoch AND p.source_delivery_id=r.delivery_id WHERE p.source_position IS NOT NULL OR p.source_cursor_json IS NULL OR p.source_delivery_id IS NULL OR r.delivery_id IS NULL OR get_json_object(p.source_cursor_json,'$.xid') IS DISTINCT FROM get_json_object(r.source_cursor_json,'$.xid') OR get_json_object(p.source_cursor_json,'$.seq') IS DISTINCT FROM get_json_object(r.source_cursor_json,'$.seq')"
for t in tables[:4]:assert c.sql('valid-reference-'+t,bad_query(t))==[['0']]
assert c.sql('raw-key-and-digest',f'SELECT count(*),count(DISTINCT struct(source_feed,source_epoch,delivery_id)),count_if(payload_digest IS DISTINCT FROM sha2(payload_json,256)) FROM {N}.source_record')==[['7','7','0']]
# Actual row mutations exercise the validator. No manifest may exist while invalid.
original=c.sql('original-reference',f'SELECT source_epoch,source_cursor_json,source_delivery_id FROM {N}.object_current WHERE type_id=1 AND id=1')[0]
for label,assignment in [('missing',"source_delivery_id='absent'"),('cursor',"source_cursor_json='{}'"),('epoch',"source_epoch='other'")]:
 c.sql('break-'+label,f'UPDATE {N}.object_current SET {assignment} WHERE type_id=1 AND id=1')
 assert c.sql('detect-'+label,bad_query('object_current'))==[['1']]
 assert c.sql('no-publication-'+label,f'SELECT count(*) FROM {N}.publication_manifest')==[['0']]
 c.sql('restore-'+label,f"UPDATE {N}.object_current SET source_epoch={lit(original[0])},source_cursor_json={lit(original[1])},source_delivery_id={lit(original[2])} WHERE type_id=1 AND id=1")
 assert c.sql('restored-'+label,bad_query('object_current'))==[['0']]
vector={}
for t in tables[:-1]:vector[N+'.'+t]=int(c.sql('version-'+t,f'DESCRIBE HISTORY {N}.{t} LIMIT 1')[0][0])
progress={'v03-fixture':{'epoch':'e','cursor':{'xid':'1042','seq':'7'},'source_profile':'synthetic-full-carrier/1'}}
c.sql('publish',f"INSERT INTO {N}.publication_manifest SELECT 'r74:publication1','ashlar-delta/0.3',{lit(json.dumps(vector))},{lit(json.dumps(progress))},'{{\"pilot\":\"r1\"}}','{{\"reference_validation\":\"passed\",\"fixture\":true}}',current_timestamp()")
assert c.sql('manifest',f'SELECT table_versions_json,source_progress_json FROM {N}.publication_manifest')==[[json.dumps(vector),json.dumps(progress)]]
# Exact source tuple preserved in the pinned reference; raw table part of vector.
v=vector[N+'.source_record']
assert c.sql('pinned-raw',f'SELECT count(*) FROM {N}.source_record VERSION AS OF {v}')==[['7']]
(out/'summary.json').write_text(json.dumps({'state':'passed','versions':vector,'progress':progress,'raw_records':7,'reference_controls':['missing raw record','mismatched cursor','cross-epoch reference'],'scope':'Synthetic complete 0.3 publication reference coverage across objects,edges,journal,tombstones,raw source and structural projections. Actual invalid current-row mutations detected before manifest; no transaction rollback or real producer/fencing/recovery/performance/scale/engine claim.'},indent=2)+'\n')
print('0.3 reference validation and complete publication passed',flush=True)
