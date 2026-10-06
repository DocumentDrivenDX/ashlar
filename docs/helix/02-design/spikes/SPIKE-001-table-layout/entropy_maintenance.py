"""One bounded routine OPTIMIZE after r89, preserving published version 1."""
import json
from pathlib import Path
from persistent_sql import Client

B = Path(__file__).resolve().parent
O = B / 'out/native/ashlar_entropy_maintenance_20261006_r91'
E = 'client_dev.ashlar_entropy_20261006_r86.edge_current'
assert not (O / 'summary.json').exists(), 'Inspect completed evidence; never repeat an unknown write'
c = Client(O, observation_timeout=960, cancel_after=900)

def sql(label, statement):
    rows = c.sql(label, statement)
    print(label, round(c.records[-1]['wall_ms']), flush=True)
    if c.records[-1]['cancel_requested']:
        raise RuntimeError('Cancellation bound reached; inspect committed history before further writes')
    return rows

assert int(sql('prior-version', f'DESCRIBE HISTORY {E} LIMIT 1')[0][0]) == 1
sql('before-detail', f'DESCRIBE DETAIL {E}')
sql('routine-maintenance', f'OPTIMIZE {E}')
v = int(sql('after-history', f'DESCRIBE HISTORY {E} LIMIT 1')[0][0])
assert v in (1, 2)
sql('after-detail', f'DESCRIBE DETAIL {E}')
if v != 1:
    assert sql('cardinality', f'SELECT count(*),count(DISTINCT id),count_if(entity_version=1) FROM {E} VERSION AS OF {v}') == [['20000000','20000000','200000']]
    cols = ['source_system','rel_type_id','id','source_type','source_id','target_type','target_id',
            'schema_revision','entity_version','props_json','retained_json','order_key','source_feed',
            'source_epoch','source_position','published_at','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id']
    text = {'source_system','schema_revision','props_json','retained_json','order_key','source_feed',
            'source_epoch','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id'}
    tests = [f"NOT (hex(encode(a.{col},'UTF-8')) <=> hex(encode(b.{col},'UTF-8')))" if col in text else f'NOT (a.{col} <=> b.{col})' for col in cols]
    assert sql('all-field-maintenance-parity', f'SELECT count(*) FROM {E} VERSION AS OF {v} a FULL OUTER JOIN {E} VERSION AS OF 1 b ON a.id=b.id WHERE a.id IS NULL OR b.id IS NULL OR ' + ' OR '.join(tests)) == [['0']]
c.history()
(O / 'summary.json').write_text(json.dumps({
    'state': 'completed routine maintenance', 'old_version': 1, 'new_version': v,
    'preservation': 'Same immutable version' if v == 1 else 'All 20M rows / all 20 fields compared; exact UTF-8 text; zero mismatches',
    'scope': 'Single routine OPTIMIZE on existing 2X-Small warehouse; no per-batch policy, production publication or sustained ingest claim. Manifest r89 remains pinned to version1.'
}, indent=2) + '\n')
