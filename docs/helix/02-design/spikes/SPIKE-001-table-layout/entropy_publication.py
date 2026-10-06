"""Serialized synthetic 200k publication over the 20M-edge entropy baseline.

One-shot submission: inspect existing handles/history on failure, never rerun.
Baseline origin records are unavailable; qualification covers changed origins.
"""
import json
import re
import time
from pathlib import Path
from persistent_sql import Client
from scale_workload import select

B = Path(__file__).resolve().parent
O = B / 'out/native/ashlar_entropy_publication_20261006_r89'
F = 'client_dev.ashlar_entropy_20261006_r86'
E, N = F + '.edge_current', F + '.object_current'
S = F + '.publication_stage_r89'
tables = {name: F + '.' + name + '_r89' for name in
          ('source_record', 'property_journal', 'tombstone', 'publication_manifest')}
c = Client(O, observation_timeout=960, cancel_after=900)
report = {'state': 'running', 'changed_edges': 200000, 'baseline_edges': 20000000,
          'phases': [], 'tables': tables, 'stage': S,
          'scope': 'Serialized synthetic property-only publication; changed origins only. Baseline raw origins absent; no real source acknowledgement, fencing, sustained freshness or billion-scale admission.'}
start = time.time()

def save():
    report['elapsed_seconds'] = time.time() - start
    (O / 'summary.json').write_text(json.dumps(report, indent=2) + '\n')

def phase(label, sql):
    rows = c.sql(label, sql)
    report['phases'].append({'label': label, 'caller_ms': c.records[-1]['wall_ms'], 'result': rows})
    save()
    print(label, round(c.records[-1]['wall_ms']), flush=True)
    if c.records[-1]['cancel_requested']:
        raise RuntimeError('Cancellation bound reached: inspect history and committed state before further writes')
    return rows

def version(table, label):
    return int(phase(label, f'DESCRIBE HISTORY {table} LIMIT 1')[0][0])

def literal(value):
    return "decode(unhex('" + value.encode().hex() + "'),'UTF-8')"

assert version(E, 'baseline-edge-version') == 0
assert version(N, 'baseline-node-version') == 0
assert phase('private-tables-absent', f"SHOW TABLES IN {F} LIKE '*_r89'") == []
ddl = (B / 'sql/delta-layout-v03.sql').read_text()
for name, table in tables.items():
    statement = re.search(r'CREATE TABLE ' + name + r' \(.*?;', ddl, re.S).group(0)
    phase('create-' + name, statement.replace('CREATE TABLE ' + name, 'CREATE TABLE ' + table, 1))

# Preserve all generator fields except explicit new provenance and publish time.
cols = ['source_system', 'rel_type_id', 'id', 'source_type', 'source_id', 'target_type', 'target_id',
        'schema_revision', 'entity_version', 'props_json', 'retained_json', 'order_key',
        'source_feed', 'source_epoch', 'source_position', 'published_at', 'lookup_hash',
        'apply_batch_id', 'source_cursor_json', 'source_delivery_id']
overrides = {'source_epoch': "'entropy-publication-r89'", 'apply_batch_id': "'r89'",
             'published_at': 'current_timestamp()',
             'source_delivery_id': "concat('r89:edge:',cast(id AS STRING))",
             'source_cursor_json': "concat('{\"xid\":\"9007199254740995\",\"seq\":\"',cast(id AS STRING),'\"}')"}
projection = ','.join(overrides.get(col, col) + ' AS ' + col for col in cols)
arrival = time.time()
report['batch_arrival_epoch'] = arrival
phase('immutable-stage', f'CREATE TABLE {S} USING DELTA AS SELECT {projection} FROM ({select("edge", 200000, updated=True, entropy=True, truss_shape=True)})')
assert version(S, 'stage-version') == 0
assert phase('stage-cardinality', f'SELECT count(*),count(DISTINCT id),count(DISTINCT source_delivery_id) FROM {S} VERSION AS OF 0') == [['200000'] * 3]
# The synthetic wire uses strings for exact JSON carriers; it is not native Truss.
payload = 'to_json(named_struct(' + ','.join("'" + col + "'," + col for col in cols) + '),map(\'ignoreNullFields\',\'false\'))'
qualified_payload = 'to_json(named_struct(' + ','.join("'" + col + "',s." + col for col in cols) + '),map(\'ignoreNullFields\',\'false\'))'
phase('retain-raw-input', f'''INSERT INTO {tables['source_record']}
 SELECT source_feed,source_epoch,source_delivery_id,'synthetic-complete-edge',source_cursor_json,
 payload,sha2(payload,256),schema_revision,current_timestamp(),apply_batch_id
 FROM (SELECT *,{payload} payload FROM {S} VERSION AS OF 0)''')
phase('append-property-history', f'''INSERT INTO {tables['property_journal']}
 SELECT source_system,'edge',rel_type_id,id,cast(107 AS BIGINT),entity_version,'set',
 false,cast(NULL AS STRING),true,'true',schema_revision,source_feed,source_epoch,
 source_position,cast(0 AS BIGINT),cast(NULL AS STRING),published_at,apply_batch_id,
 source_cursor_json,source_delivery_id FROM {S} VERSION AS OF 0''')
assert phase('raw-cardinality', f"SELECT count(*),count(DISTINCT delivery_id) FROM {tables['source_record']}") == [['200000','200000']]
assert phase('raw-reference-integrity', f'''SELECT count(*) FROM {S} VERSION AS OF 0 s
 LEFT JOIN {tables['source_record']} r ON s.source_feed=r.source_feed AND s.source_epoch=r.source_epoch AND s.source_delivery_id=r.delivery_id
 WHERE r.delivery_id IS NULL OR NOT (s.source_cursor_json <=> r.source_cursor_json)
 OR NOT (r.payload_digest <=> sha2(r.payload_json,256))
 OR NOT (hex(encode(r.payload_json,'UTF-8')) <=> hex(encode({qualified_payload},'UTF-8')))''') == [['0']]
assert phase('journal-parity', f'''SELECT count(*) FROM (
 (SELECT source_system,'edge' entity_kind,rel_type_id type_id,id,cast(107 AS BIGINT) property_id,
 entity_version,'set' operation,false old_present,cast(NULL AS STRING) old_json,true new_present,'true' new_json,
 schema_revision,source_feed,source_epoch,source_position,cast(0 AS BIGINT) event_ordinal,cast(NULL AS STRING) source_time_text,
 published_at,apply_batch_id,source_cursor_json,source_delivery_id FROM {S} VERSION AS OF 0
 EXCEPT ALL SELECT * FROM {tables['property_journal']}) UNION ALL
 (SELECT * FROM {tables['property_journal']} EXCEPT ALL
 SELECT source_system,'edge',rel_type_id,id,cast(107 AS BIGINT),entity_version,'set',false,cast(NULL AS STRING),true,'true',
 schema_revision,source_feed,source_epoch,source_position,cast(0 AS BIGINT),cast(NULL AS STRING),published_at,apply_batch_id,
 source_cursor_json,source_delivery_id FROM {S} VERSION AS OF 0))''') == [['0']]
phase('apply-current', f'''MERGE INTO {E} t USING (SELECT * FROM {S} VERSION AS OF 0) s
 ON t.lookup_hash=s.lookup_hash AND t.source_system=s.source_system AND t.rel_type_id=s.rel_type_id AND t.id=s.id
 WHEN MATCHED AND t.entity_version=0 THEN UPDATE SET *''')
ev = version(E, 'output-edge-version')
assert ev == 1
assert phase('output-cardinality', f'SELECT count(*),count(DISTINCT id),count_if(entity_version=1) FROM {E} VERSION AS OF {ev}') == [['20000000','20000000','200000']]
# Full baseline/output comparison proves unchanged rows and fields: staged rows
# differ only in properties/version/provenance/time; every other column is exact.
changed = set(overrides) | {'entity_version', 'props_json'}
expected = {col: (f'CASE WHEN s.id IS NOT NULL THEN s.{col} ELSE b.{col} END' if col in changed else f'b.{col}') for col in cols}
text_cols = {'source_system','schema_revision','props_json','retained_json','order_key','source_feed','source_epoch','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id'}
tests = [f"NOT (hex(encode(a.{col},'UTF-8')) <=> hex(encode({expected[col]},'UTF-8')))" if col in text_cols else 'NOT (a.' + col + ' <=> ' + expected[col] + ')' for col in cols]
assert phase('full-output-parity', f'''SELECT count(*) FROM {E} VERSION AS OF {ev} a
 FULL OUTER JOIN {E} VERSION AS OF 0 b ON a.id=b.id
 LEFT JOIN {S} VERSION AS OF 0 s ON b.id=s.id
 WHERE a.id IS NULL OR b.id IS NULL OR ''' + ' OR '.join(tests)) == [['0']]
assert phase('updated-property-preservation', f'''SELECT count(*) FROM {S} VERSION AS OF 0 s JOIN {E} VERSION AS OF 0 b ON s.id=b.id
 WHERE NOT (hex(encode(s.props_json,'UTF-8')) <=> hex(encode(concat('{{"107":true',CASE WHEN b.props_json='{{}}' THEN '}}' ELSE concat(',',substring(b.props_json,2)) END),'UTF-8')))
 OR get_json_object(b.props_json,'$.107') IS NOT NULL''') == [['0']]
vector = {N: 0, E: ev}
for name in ('source_record', 'property_journal', 'tombstone'):
    vector[tables[name]] = version(tables[name], 'version-' + name)
    phase('detail-' + name, 'DESCRIBE DETAIL ' + tables[name])
assert phase('empty-tombstones', f"SELECT count(*) FROM {tables['tombstone']}") == [['0']]
progress = {'profile': 'synthetic-complete-stage/1', 'epoch': 'entropy-publication-r89',
            'stage_table': S, 'stage_version': 0, 'members': 200000,
            'cursor': 'scattered per-row tuple cursors; no producer checkpoint or acknowledgement'}
validation = {'changed_rows': 200000, 'full_current_rows_compared': 20000000,
              'changed_origin_references': 'passed', 'baseline_origin_references': 'unqualified; raw input unavailable',
              'journal': 'one absent-to-true property107 event per changed edge',
              'structure': 'all endpoint fields unchanged from validated baseline',
              'writer': 'single serialized synthetic writer; no crash/concurrency authority proof'}
values = ['r89', 'ashlar-delta/0.3-synthetic-changed-origin-slice',
          json.dumps(vector, sort_keys=True, separators=(',', ':')),
          json.dumps(progress, sort_keys=True, separators=(',', ':')),
          '{"synthetic":"synthetic-r1"}', json.dumps(validation, sort_keys=True, separators=(',', ':'))]
phase('publish-validated-vector', f"INSERT INTO {tables['publication_manifest']} SELECT " + ','.join(map(literal, values)) + ',current_timestamp()')
assert phase('manifest-readback', f"SELECT publication_id,profile_version,table_versions_json,source_progress_json,schema_revisions_json,validation_report_json FROM {tables['publication_manifest']}") == [values]
report.update(state='passed serialized synthetic publication slice', versions=vector,
              validation=validation, arrival_to_manifest_seconds=time.time()-arrival,
              publication_manifest_version=version(tables['publication_manifest'], 'manifest-version'))
phase('edge-detail-after-publication', f'DESCRIBE DETAIL {E}')
c.history()
save()
