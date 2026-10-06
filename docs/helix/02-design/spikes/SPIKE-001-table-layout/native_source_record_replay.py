"""Single-table atomic replay/conflict controls; no concurrent publisher claim."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;N='client_dev.ashlar_layout_v02_20261006_r65'
T=N+'.source_record_r71';S=N+'.source_record_stage_r72'
out=B/'out/native/ashlar_source_record_replay_20261006_r72';c=Client(out)
prior=json.loads((B/'out/native/ashlar_source_record_20261006_r71_resume/summary.json').read_text());assert prior['state']=='passed'
assert c.sql('absent-stage',f"SHOW TABLES IN {N} LIKE 'source_record_stage_r72'")==[]
c.sql('stage',f'CREATE TABLE {S} USING DELTA AS SELECT * FROM {T} VERSION AS OF {prior["version"]}')
fields='source_feed,source_epoch,delivery_id,record_kind,source_cursor_json,payload_json,payload_digest,schema_revision,cast(received_at AS STRING),apply_batch_id'
before=c.sql('before',f'SELECT {fields} FROM {T} ORDER BY delivery_id');assert len(before)==5
merge=f'''MERGE INTO {T} t USING {S} s ON t.source_feed=s.source_feed AND t.source_epoch=s.source_epoch AND t.delivery_id=s.delivery_id
WHEN MATCHED AND (t.record_kind IS DISTINCT FROM s.record_kind OR t.source_cursor_json IS DISTINCT FROM s.source_cursor_json OR t.payload_json IS DISTINCT FROM s.payload_json OR t.payload_digest IS DISTINCT FROM s.payload_digest OR t.schema_revision IS DISTINCT FROM s.schema_revision)
THEN UPDATE SET payload_json=cast(raise_error('SOURCE_RECORD_CONFLICT') AS STRING)
WHEN NOT MATCHED THEN INSERT *'''
assert c.sql('stage-key-and-digest',f'SELECT count(*),count(DISTINCT struct(source_feed,source_epoch,delivery_id)),count_if(payload_digest IS DISTINCT FROM sha2(payload_json,256)) FROM {S}')==[['5','5','0']]
c.sql('identical-replay',merge)
assert c.sql('after-replay',f'SELECT {fields} FROM {T} ORDER BY delivery_id')==before
# Same key, byte-different payload with valid recomputed digest, plus a new key.
c.sql('stage-conflict',f"UPDATE {S} SET payload_json=concat(payload_json,' '),payload_digest=sha2(concat(payload_json,' '),256) WHERE delivery_id='fixture-raw/1'")
c.sql('stage-new-row',f"INSERT INTO {S} SELECT source_feed,source_epoch,'fixture-raw/6',record_kind,source_cursor_json,payload_json,payload_digest,schema_revision,received_at,apply_batch_id FROM {S} WHERE delivery_id='fixture-raw/5'")
assert c.sql('valid-conflict-stage',f'SELECT count(*),count(DISTINCT struct(source_feed,source_epoch,delivery_id)),count_if(payload_digest IS DISTINCT FROM sha2(payload_json,256)) FROM {S}')==[['6','6','0']]
try:
 c.sql('conflict-refusal',merge)
 raise AssertionError('Expected conflict refusal')
except RuntimeError:
 response=c.records[-1]['response']
 assert response['status']['state']=='FAILED'
 assert 'SOURCE_RECORD_CONFLICT' in json.dumps(response['status'])
assert c.sql('after-conflict',f'SELECT {fields} FROM {T} ORDER BY delivery_id')==before
version=int(c.sql('target-version',f'DESCRIBE HISTORY {T} LIMIT 1')[0][0])
(out/'summary.json').write_text(json.dumps({'state':'passed','target':T,'target_version':version,'rows':5,'identical_replay':'full field parity','conflicting_replay':'terminal failure; complete target parity; new row absent','scope':'One ordinary Delta MERGE atomic replay/refusal on valid unique staged keys. No concurrent key uniqueness, publisher fencing, cross-table atomicity, source-native delivery-ID derivation or full producer conformance.'},indent=2)+'\n')
print('Raw source-record replay and atomic conflict refusal passed',flush=True)
