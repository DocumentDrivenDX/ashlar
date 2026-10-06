"""Exact affected-row checks on r89, with narrow keys before wide bag joins.

Stored-batch validator timing only, not a new ingest/freshness observation.
The exhaustive r89 audit remains separate; baseline raw origins unqualified.
"""
import json
from pathlib import Path
from persistent_sql import Client

B = Path(__file__).resolve().parent
O = B / 'out/native/ashlar_entropy_changed_validation_20261006_r93'
F = 'client_dev.ashlar_entropy_20261006_r86'
E, S = F + '.edge_current', F + '.publication_stage_r89'
assert not (O / 'summary.json').exists(), 'Audit existing evidence instead of repeating it'
c = Client(O, observation_timeout=960, cancel_after=900)
cols = ['source_system','rel_type_id','id','source_type','source_id','target_type','target_id',
        'schema_revision','entity_version','props_json','retained_json','order_key','source_feed',
        'source_epoch','source_position','published_at','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id']
text = {'source_system','schema_revision','props_json','retained_json','order_key','source_feed',
        'source_epoch','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id'}

def run(label, statement, expected):
    rows = c.sql(label, statement)
    assert rows == [[str(expected)]], (label, rows)
    assert not c.records[-1]['cancel_requested']
    print(label, round(c.records[-1]['wall_ms']), flush=True)

stage = f'SELECT * FROM {S} VERSION AS OF 0'
run('presence-negative-controls', '''SELECT count(*) FROM
 (VALUES ('{}',false),('{"107":null}',true),('{"107":true}',true),('malformed',true)) AS cases(input,expected)
 WHERE coalesce(array_contains(json_object_keys(input),'107'),true) <> expected''', 0)
changed = f"SELECT * FROM {E} VERSION AS OF 1 WHERE entity_version=1 AND apply_batch_id='r89'"
tests = [f"NOT (hex(encode(a.{col},'UTF-8')) <=> hex(encode(s.{col},'UTF-8')))" if col in text else f'NOT (a.{col} <=> s.{col})' for col in cols]
parity = lambda expected_stage: f'''SELECT count(*) FROM ({changed}) a FULL OUTER JOIN ({expected_stage}) s
 ON a.source_system=s.source_system AND a.rel_type_id=s.rel_type_id AND a.id=s.id
 WHERE a.id IS NULL OR s.id IS NULL OR ''' + ' OR '.join(tests)
run('affected-full-carrier-parity', parity(stage), 0)
keys = f'SELECT source_system,rel_type_id,id FROM {S} VERSION AS OF 0'
baseline = f'''SELECT /*+ BROADCAST(k) */ b.source_system,b.rel_type_id,b.id,b.props_json FROM
 {E} VERSION AS OF 0 b JOIN ({keys}) k ON b.source_system=k.source_system AND b.rel_type_id=k.rel_type_id AND b.id=k.id'''
run('narrow-key-prior-property-preservation', f'''SELECT count(*) FROM ({baseline}) b
 FULL OUTER JOIN ({stage}) s ON b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id
 WHERE b.id IS NULL OR s.id IS NULL OR
 NOT (hex(encode(s.props_json,'UTF-8')) <=> hex(encode(concat('{{"107":true',CASE WHEN b.props_json='{{}}' THEN '}}' ELSE concat(',',substring(b.props_json,2)) END),'UTF-8')))
 OR coalesce(array_contains(json_object_keys(b.props_json),'107'),true)''', 0)
# Alter only a valid JSON lexical token in one retained intended row; no writes.
negative = 'SELECT ' + ','.join("CASE WHEN id=4000001 THEN concat(props_json,' ') ELSE props_json END AS props_json" if col == 'props_json' else col for col in cols) + f' FROM {S} VERSION AS OF 0'
run('negative-lexical-stage-change', parity(negative), 1)
c.history()
(O / 'summary.json').write_text(json.dumps({
    'state': 'passed exact affected-row checks and lexical negative control',
    'rows': 200000, 'canonical_version': 1, 'baseline_version': 0, 'stage_version': 0,
    'checks': 'All20 affected carrier fields; exact old-property patch; narrow native keys before wide baseline join; byte-different valid JSON stage rejected',
    'scope': 'Stored synthetic batch validation experiment; does not replace exhaustive r89 evidence or qualify source completeness, concurrency authority, baseline origins, acknowledgement or arrival-to-publication latency'
}, indent=2) + '\n')
