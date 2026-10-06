"""Independent timestamp oracle; retain old wire and qualify its precision loss."""
import json
from pathlib import Path
from persistent_sql import Client
from wire_json import encode

B = Path(__file__).resolve().parent
O = B/'out/native/ashlar_wire_timestamp_20261006_r95'
F = 'client_dev.ashlar_entropy_20261006_r86'
c = Client(O,observation_timeout=960,cancel_after=900)
records_path=O/'statements.jsonl'
c.records=[json.loads(line) for line in records_path.read_text().splitlines()] if records_path.exists() else []
def read(label,statement):
    prior=next((r for r in c.records if r['label']==label and r['sql']==statement and r['response']['status']['state']=='SUCCEEDED'),None)
    if prior:
        return prior['response']['result']['data_array']
    return c.sql(label,statement)
cols = ['source_system','rel_type_id','id','source_type','source_id','target_type','target_id','schema_revision',
        'entity_version','props_json','retained_json','order_key','source_feed','source_epoch','source_position',
        'published_at','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id','old_json']
wire = encode('named_struct('+','.join("'"+col+"',"+col for col in cols)+')')
corrected = read('corrected-100k-timestamp-roundtrip',f'''SELECT count_if(NOT(published_at <=> cast(get_json_object(payload,'$.published_at') AS TIMESTAMP))),
 min(unix_micros(published_at)-unix_micros(cast(get_json_object(payload,'$.published_at') AS TIMESTAMP))),
 max(unix_micros(published_at)-unix_micros(cast(get_json_object(payload,'$.published_at') AS TIMESTAMP)))
 FROM (SELECT published_at,{wire} payload FROM {F}.publication_stage_r94 VERSION AS OF 0)''')
assert corrected==[['0','0','0']],corrected
# Independent sub-millisecond positive and pre-epoch controls; no data writes.
literal_struct="named_struct('time',ts,'bag','{\"102\":9007199254740993,\"103\":1.2300000000000000001}')"
probe = encode(literal_struct)
assert read('microsecond-and-pre-epoch-oracles',f'''SELECT count(*) FROM
 (SELECT ts,{probe} payload FROM (SELECT timestamp '2026-10-06 12:34:56.123456' ts
 UNION ALL SELECT timestamp '1969-12-31 23:59:59.987654' ts))
 WHERE NOT(ts <=> cast(get_json_object(payload,'$.time') AS TIMESTAMP))
 OR NOT(hex(encode(get_json_object(payload,'$.bag'),'UTF-8')) <=> hex(encode('{{"102":9007199254740993,"103":1.2300000000000000001}}','UTF-8')))''')==[['0']]
legacy = read('legacy-r89-published-at-loss',f'''SELECT count_if(NOT(s.published_at <=> cast(get_json_object(r.payload_json,'$.published_at') AS TIMESTAMP))),
 min(unix_micros(s.published_at)-unix_micros(cast(get_json_object(r.payload_json,'$.published_at') AS TIMESTAMP))),
 max(unix_micros(s.published_at)-unix_micros(cast(get_json_object(r.payload_json,'$.published_at') AS TIMESTAMP)))
 FROM {F}.publication_stage_r89 VERSION AS OF 0 s JOIN {F}.source_record_r89 VERSION AS OF 1 r
 ON s.source_feed=r.source_feed AND s.source_epoch=r.source_epoch AND s.source_delivery_id=r.delivery_id''')
history = {q['query_id']:q for q in c.history()}
assert all(history.get(r['statement_id'],{}).get('is_final') for r in c.records), 'Refresh existing history; no new wire submissions needed'
result = {'state':'passed explicit UTC microsecond encoder and independent oracles; legacy loss qualified',
          'corrected_r94_rows':100000,'corrected_mismatch_min_max_microseconds':corrected,
          'legacy_r89_mismatch_min_max_microseconds':legacy,
          'scope':'Read-only corrected encoding over retained stage; old raw payloads and descriptors unmodified. Default encoder loss concerns derived published_at, not property/retained/native cursor strings or Delta carrier/journal timestamps. Not native producer reconstruction.'}
(O/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
