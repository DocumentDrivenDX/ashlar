"""Bounded exact raw-wire validation comparison; isolated precomputed witness only."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS
from wire_json import encode
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_wire_validation_r150'
assert not (O/'statements.jsonl').exists(),'Inspect prior native handles; no blind replay'
F='client_dev.ashlar_entropy_20261006_r86';S=F+'.schedule_r139_1';R=F+'.source_record_r89';W=F+'.wire_witness_r150'
c=BoundedReads(O);c.sql('timeout','SET STATEMENT_TIMEOUT=90')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
assert c.sql('absence',f"SHOW TABLES IN {F} LIKE 'wire_witness_r150'")==[]
assert c.sql('canonical-anchor',f'DESCRIBE HISTORY {F}.edge_current LIMIT 1')[0][0]=='23'
fields=list(COLS)+['old_json']
def payload(prefix):return encode('named_struct('+','.join("'"+col+"',"+prefix+col for col in fields)+')')
select=f"SELECT source_feed,source_epoch,source_delivery_id,schema_revision,apply_batch_id,source_cursor_json,published_at,{payload('')} payload_json FROM {S} VERSION AS OF 0"
start=time.monotonic()
c.sql('materialize',f"CREATE TABLE {W} USING DELTA TBLPROPERTIES ('delta.parquet.compression.codec'='zstd') AS SELECT *,sha2(payload_json,256) payload_digest FROM ({select})")
materialize_wall=time.monotonic()-start
assert c.sql('witness-version','DESCRIBE HISTORY '+W+' LIMIT 1')[0][0]=='0'
assert c.sql('membership',f'SELECT count(*),count(DISTINCT source_delivery_id) FROM {W} VERSION AS OF 0')==[['100000','100000']]
# Independent exact oracle deliberately re-serializes native source after creation.
expected=f'SELECT *,sha2(payload_json,256) payload_digest FROM ({select})'
assert c.sql('witness-exact',f'SELECT count(*) FROM (({expected} EXCEPT ALL SELECT * FROM {W} VERSION AS OF 0) UNION ALL (SELECT * FROM {W} VERSION AS OF 0 EXCEPT ALL {expected}))')==[['0']]
actual=f"(SELECT * FROM {R} VERSION AS OF 17 WHERE apply_batch_id='r139-b1')"
def validation(precomputed):
 stage=f'{W} VERSION AS OF 0' if precomputed else f'{S} VERSION AS OF 0'
 wire='s.payload_json' if precomputed else payload('s.')
 return f"""SELECT count(*) FROM {stage} s LEFT JOIN {actual} r
 ON s.source_feed=r.source_feed AND s.source_epoch=r.source_epoch AND s.source_delivery_id=r.delivery_id
 WHERE r.delivery_id IS NULL OR NOT(s.schema_revision <=> r.schema_revision) OR NOT(s.apply_batch_id <=> r.apply_batch_id)
 OR NOT(r.record_kind <=> 'synthetic-scheduled-wide-edge') OR r.received_at IS NULL
 OR NOT(s.source_cursor_json <=> r.source_cursor_json) OR NOT(r.payload_digest <=> sha2(r.payload_json,256))
 OR NOT(s.published_at <=> cast(get_json_object(r.payload_json,'$.published_at') AS TIMESTAMP))
 OR NOT(hex(encode(r.payload_json,'UTF-8')) <=> hex(encode({wire},'UTF-8')))"""
for i in range(3):
 for mode in (('control','stored') if i%2==0 else ('stored','control')):
  assert c.sql(mode+'-'+str(i),validation(mode=='stored'))==[['0']]
assert c.sql('raw-membership',f"SELECT count(*),count(DISTINCT delivery_id) FROM {R} VERSION AS OF 17 WHERE apply_batch_id='r139-b1'")==[['100000','100000']]
rows=c.sql('detail','DESCRIBE DETAIL '+W);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
detail=dict(zip(names,rows[0]))
report={'state':'Three balanced pairs and independent exact100k witness passed; native final metrics pending',
 'witness':W,'witness_id':detail['id'],'witness_version':0,'witness_detail':detail,'materialize_wall_s':materialize_wall,
 'source_stage':S,'stage_version':0,'raw':R,'raw_version':17,
 'qualification':'100k preexisting r139 exact full-wire witness. Materialization counted; independent exact witness oracle separately counted. No source/history append, MERGE, publication, actual producer or end-to-end admission. Stored witness is trusted only under owned immutable source and explicit provenance; digest does not replace exact raw UTF8 equality.'}
(O/'summary.json').write_text(json.dumps(report,indent=2)+'\n');c.history();c.close()
print('Balanced exact wire validation screen passed',flush=True)
