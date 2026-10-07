"""Read-only paired r97 raw validation on the immutable completed r96 batch.

No new publication: timings qualify this validation query only.
"""
import json
import argparse
from pathlib import Path
from persistent_sql import Client
from wire_json import encode

B = Path(__file__).resolve().parent
args = argparse.ArgumentParser()
args.add_argument('--driver', action='store_true')
driver = args.parse_args().driver
O = B/'out/native/ashlar_raw_validation_20261007_r97'
if driver: O = O/'persistent-uncached'
assert not (O/'statements.jsonl').exists(), 'Inspect prior statement IDs; do not repeat an unknown execution'
if driver:
    from driver_sql import DriverClient
    c = DriverClient(O)
else:
    c = Client(O, observation_timeout=240, cancel_after=180)
prior = json.loads((B/'out/native/ashlar_scheduled_publication_20261006_r96/version-aware-preflight/summary.json').read_text())
batch = prior['batches'][-1]
assert batch['batch'] == 'r96-b3'
F = 'client_dev.ashlar_entropy_20261006_r86'
stage = f"SELECT * FROM {batch['source_stage']} VERSION AS OF 0"
raw = f"SELECT * FROM {F}.source_record_r89 VERSION AS OF {batch['versions'][F+'.source_record_r89']} WHERE apply_batch_id='r96-b3'"
cols = ['source_system','rel_type_id','id','source_type','source_id','target_type','target_id','schema_revision',
        'entity_version','props_json','retained_json','order_key','source_feed','source_epoch','source_position',
        'published_at','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id','old_json']
qualified = encode('named_struct('+','.join("'"+col+"',s."+col for col in cols)+')')
join = 's.source_feed=r.source_feed AND s.source_epoch=r.source_epoch AND s.source_delivery_id=r.delivery_id'
bad = f'''s.id IS NULL OR r.delivery_id IS NULL
 OR NOT(s.source_cursor_json <=> r.source_cursor_json)
 OR NOT(r.payload_digest <=> sha2(r.payload_json,256))
 OR NOT(s.published_at <=> cast(get_json_object(r.payload_json,'$.published_at') AS TIMESTAMP))
 OR NOT(hex(encode(r.payload_json,'UTF-8')) <=> hex(encode({qualified},'UTF-8')))'''
def combined(s, r):
    return f'''WITH s AS ({s}), r AS ({r}) SELECT count(*),count(DISTINCT s.id),
    count(DISTINCT r.delivery_id),count_if({bad}) FROM s FULL OUTER JOIN r ON {join}'''
def run(label, statement):
    result = c.sql(label, statement)
    assert not c.records[-1].get('cancel_requested',False), 'Cancelled; inspect same handle'
    print(label, round(c.records[-1]['wall_ms']), result, flush=True)
    return result

# Independent executable negative cases, using one native row and its exact
# corrected wire representation. Corruption recomputes the digest, so the
# timestamp/body gates must catch it without relying on digest mismatch.
one = stage+' ORDER BY id LIMIT 1'
fixture = f'''SELECT s.source_feed,s.source_epoch,s.source_delivery_id delivery_id,
 {qualified} payload_json,sha2({qualified},256) payload_digest,s.source_cursor_json FROM ({one}) s'''
assert run('control-valid',combined(one,fixture)) == [['1','1','1','0']]
assert run('control-missing',combined(one,fixture+' WHERE false')) == [['1','1','0','1']]
assert run('control-duplicate',combined(one,f'({fixture}) UNION ALL ({fixture})')) == [['2','1','1','0']]
extra = f'''SELECT source_feed,source_epoch,concat(delivery_id,':extra') delivery_id,payload_json,payload_digest,source_cursor_json FROM ({fixture})'''
assert run('control-extra',combined(one,f'({fixture}) UNION ALL ({extra})')) == [['2','1','2','1']]
corrupt = f'''SELECT source_feed,source_epoch,delivery_id,payload_json,sha2(payload_json,256) payload_digest,source_cursor_json
 FROM (SELECT source_feed,source_epoch,delivery_id,
 regexp_replace(payload_json,'"published_at":"[^"]+"','"published_at":"2000-01-01T00:00:00.000001Z"') payload_json,
 source_cursor_json FROM ({fixture}))'''
assert run('control-timestamp',combined(one,corrupt)) == [['1','1','1','1']]

separate_parity = f'''SELECT count(*) FROM ({stage}) s LEFT JOIN ({raw}) r ON {join} WHERE {bad}'''
separate_count = f'SELECT count(*),count(DISTINCT delivery_id) FROM ({raw})'
paired = []
for round_index, order in enumerate(('separate-first','combined-first')):
    timings = {}
    def separate():
        assert run(f'parity-{round_index}',separate_parity) == [['0']]
        parity = c.records[-1]['wall_ms']
        assert run(f'membership-{round_index}',separate_count) == [['100000','100000']]
        timings['separate_caller_ms'] = parity+c.records[-1]['wall_ms']
    def aggregate():
        assert run(f'combined-{round_index}',combined(stage,raw)) == [['100000','100000','100000','0']]
        timings['combined_caller_ms'] = c.records[-1]['wall_ms']
    if order == 'separate-first': separate(); aggregate()
    else: aggregate(); separate()
    paired.append(dict(order=order, **timings))
result = dict(state='completed read-only combined raw validation; paired metrics pending audit',
              baseline_batch=batch, pairs=paired,
              scope='Two alternating pairs over the same 100k hot-set batch, existing 2X-Small compute. No new publication, throughput distribution or sustained-rate admission.',
              invariant='Combined gate requires joined rows, distinct input IDs and distinct raw delivery IDs all equal declared 100k membership, and mismatch count zero. Duplicate control has zero mismatches but fails cardinality.')
(O/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
c.history()
if driver: c.close()
