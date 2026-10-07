"""Bounded exact raw-wire validation comparison; isolated precomputed witness only."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS
from wire_json import encode
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_origin_validation_r151'
assert not (O/'statements.jsonl').exists(),'Inspect prior native handles; no blind replay'
F='client_dev.ashlar_entropy_20261006_r86';S=F+'.schedule_r139_1';R=F+'.source_record_r89';W=F+'.wire_witness_r150'
c=BoundedReads(O);c.sql('timeout','SET STATEMENT_TIMEOUT=90')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'

assert c.sql('canonical-anchor',f'DESCRIBE HISTORY {F}.edge_current LIMIT 1')[0][0]=='23'
fields=list(COLS)+['old_json']
def payload(prefix):return encode('named_struct('+','.join("'"+col+"',"+prefix+col for col in fields)+')')
origins=c.sql('stage-origin',f'SELECT source_feed,source_epoch,count(*) FROM {S} VERSION AS OF 0 GROUP BY source_feed,source_epoch')
assert len(origins)==1 and origins[0][2]=='100000'
feed,epoch=origins[0][:2]
assert feed is not None and epoch is not None
def lit(value):return "decode(unhex('"+value.encode().hex()+"'),'UTF-8')"
actual=f"(SELECT * FROM {R} VERSION AS OF 17 WHERE apply_batch_id='r139-b1')"
def validation(precomputed):
 stage=f'{S} VERSION AS OF 0'
 wire=payload('s.')
 raw=actual if not precomputed else f"(SELECT * FROM {R} VERSION AS OF 17 WHERE apply_batch_id='r139-b1' AND source_feed={lit(feed)} AND source_epoch={lit(epoch)})"
 return f"""SELECT count(*) FROM {stage} s LEFT JOIN {raw} r
 ON s.source_feed=r.source_feed AND s.source_epoch=r.source_epoch AND s.source_delivery_id=r.delivery_id
 WHERE r.delivery_id IS NULL OR NOT(s.schema_revision <=> r.schema_revision) OR NOT(s.apply_batch_id <=> r.apply_batch_id)
 OR NOT(r.record_kind <=> 'synthetic-scheduled-wide-edge') OR r.received_at IS NULL
 OR NOT(s.source_cursor_json <=> r.source_cursor_json) OR NOT(r.payload_digest <=> sha2(r.payload_json,256))
 OR NOT(s.published_at <=> cast(get_json_object(r.payload_json,'$.published_at') AS TIMESTAMP))
 OR NOT(hex(encode(r.payload_json,'UTF-8')) <=> hex(encode({wire},'UTF-8')))"""
for i in range(3):
 for mode in (('control','bounded') if i%2==0 else ('bounded','control')):
  assert c.sql(mode+'-'+str(i),validation(mode=='bounded'))==[['0']]
assert c.sql('raw-membership',f"SELECT count(*),count(DISTINCT delivery_id) FROM {R} VERSION AS OF 17 WHERE apply_batch_id='r139-b1'")==[['100000','100000']]
report={'state':'Three alternating exact origin-bounded validation pairs passed; native final metrics pending',
 'origins':origins,'source_stage':S,'stage_version':0,'raw':R,'raw_version':17,
 'qualification':'Read-only100k exact r139 full-wire checks. Stage proves one non-null feed/epoch with100k members; static raw filters are logically redundant with exact join. No mutation, materialization, MERGE, publication or sustained admission.'}
(O/'summary.json').write_text(json.dumps(report,indent=2)+'\n');c.history();c.close()
print('Balanced exact wire validation screen passed',flush=True)
