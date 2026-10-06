"""Actual 100k hot-set publication with large old/new opaque property tokens.

One-shot synthetic profile; retain handles on failure, never blind-retry writes.
Does not qualify baseline origins, real source completeness or writer fencing.
"""
import json
import time
from pathlib import Path
from persistent_sql import Client
from wire_json import encode as wire_encode

B = Path(__file__).resolve().parent
O = B / 'out/native/ashlar_entropy_wide_journal_20261006_r94'
F = 'client_dev.ashlar_entropy_20261006_r86'
E, N, S = F+'.edge_current', F+'.object_current', F+'.publication_stage_r94'
R, J, T, M = [F+'.'+name+'_r89' for name in ('source_record','property_journal','tombstone','publication_manifest')]
assert not (O/'summary.json').exists(), 'Inspect existing handles/history before recovery'
c = Client(O, observation_timeout=960, cancel_after=900)
report = {'state':'running','changes':100000,'distribution':'Existing r89 opaque-property hot set; not new graph-wide scattered keys','phases':[],
          'scope':'Serialized synthetic changed-origin slice; baseline raw origins, real producer completeness/acknowledgement and fencing remain unqualified'}
start = time.time()

def phase(label, statement):
    rows = c.sql(label,statement)
    report['phases'].append({'label':label,'caller_ms':c.records[-1]['wall_ms'],'result':rows})
    (O/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
    print(label,round(c.records[-1]['wall_ms']),flush=True)
    assert not c.records[-1]['cancel_requested'], 'Cancellation bound reached; inspect commits'
    return rows

def version(table,label):
    return int(phase(label,'DESCRIBE HISTORY '+table+' LIMIT 1')[0][0])

def lit(value):
    return "decode(unhex('"+value.encode().hex()+"'),'UTF-8')"

assert version(E,'predecessor-current-version') == 2
assert version(R,'prior-raw-version') == version(J,'prior-journal-version') == 1
assert version(T,'prior-tombstone-version') == 0
prior = phase('predecessor-descriptor',f'SELECT publication_id,table_versions_json FROM {M}')
assert len(prior)==1 and prior[0][0]=='r89' and json.loads(prior[0][1])[E]==1
assert phase('stage-absent',f"SHOW TABLES IN {F} LIKE 'publication_stage_r94'")==[]
cols = ['source_system','rel_type_id','id','source_type','source_id','target_type','target_id','schema_revision',
        'entity_version','props_json','retained_json','order_key','source_feed','source_epoch','source_position',
        'published_at','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id']
text = {'source_system','schema_revision','props_json','retained_json','order_key','source_feed','source_epoch',
        'lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id'}
new_opaque = "concat_ws('',transform(sequence(0,cast(64+pmod(id,65) AS INT)), block -> sha2(concat('wide-r94:',cast(id AS STRING),':',cast(block AS STRING)),256)))"
new_props = "replace(props_json,concat('\"105\":\"',old_opaque,'\"'),concat('\"105\":\"',new_opaque,'\"'))"
overrides = {'entity_version':'entity_version+1','props_json':new_props,'source_epoch':"'entropy-wide-r94'",
             'published_at':'current_timestamp()','apply_batch_id':"'r94'",'source_delivery_id':"concat('r94:edge:',cast(id AS STRING))",
             'source_cursor_json':"concat('{\"xid\":\"9007199254740997\",\"seq\":\"',cast(id AS STRING),'\"}')"}
arrival = time.time()
report['batch_arrival_epoch'] = arrival
phase('immutable-wide-stage',f'''CREATE TABLE {S} USING DELTA AS SELECT
 {','.join(overrides.get(col,col)+' AS '+col for col in cols)},concat('"',old_opaque,'"') old_json
 FROM (SELECT *,{new_opaque} new_opaque,get_json_object(props_json,'$.105') old_opaque
 FROM {E} VERSION AS OF 2 WHERE entity_version=1 AND apply_batch_id='r89' AND pmod(id,8)>=4)''')
assert version(S,'stage-version')==0
assert phase('stage-cardinality',f'SELECT count(*),count(DISTINCT id),count(DISTINCT source_delivery_id) FROM {S} VERSION AS OF 0')==[['100000']*3]
assert phase('qualified-opaque-token-grammar',f'''SELECT count(*) FROM {S} VERSION AS OF 0
 WHERE old_json IS NULL OR NOT (old_json RLIKE '^"[0-9a-f]+"$') OR pmod(length(old_json)-2,64)<>0
 OR NOT (get_json_object(props_json,'$.105') RLIKE '^[0-9a-f]+$')''')==[['0']]
phase('journal-widths',f'SELECT min(length(old_json)),max(length(old_json)),avg(length(old_json)),avg(length(props_json)) FROM {S} VERSION AS OF 0')
keys = f'SELECT source_system,rel_type_id,id FROM {S} VERSION AS OF 0'
baseline = f'''SELECT /*+ BROADCAST(k) */ b.* FROM {E} VERSION AS OF 2 b JOIN ({keys}) k
 ON b.source_system=k.source_system AND b.rel_type_id=k.rel_type_id AND b.id=k.id'''
# Independent intended patch comparison against the actual previous snapshot.
expected = {col:overrides.get(col,'b.'+col) for col in cols}
expected['entity_version']='b.entity_version+1'
expected['props_json']="replace(b.props_json,concat('\"105\":',s.old_json),concat('\"105\":\"',get_json_object(s.props_json,'$.105'),'\"'))"
expected['published_at']='s.published_at'
for col in ('source_cursor_json','source_delivery_id'):
    expected[col]=overrides[col].replace('cast(id AS STRING)','cast(s.id AS STRING)')
tests = [f"NOT (hex(encode(s.{col},'UTF-8')) <=> hex(encode({expected[col]},'UTF-8')))" if col in text else f'NOT (s.{col} <=> {expected[col]})' for col in cols]
assert phase('intended-full-carrier-patch',f'''SELECT count(*) FROM ({baseline}) b FULL OUTER JOIN {S} VERSION AS OF 0 s
 ON b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id
 WHERE b.id IS NULL OR s.id IS NULL OR '''+' OR '.join(tests)+
 " OR instr(b.props_json,concat('\"105\":',s.old_json))=0 OR s.old_json=concat('\"',get_json_object(s.props_json,'$.105'),'\"') OR NOT (hex(encode(s.old_json,'UTF-8')) <=> hex(encode(concat('\"',get_json_object(b.props_json,'$.105'),'\"'),'UTF-8')))")==[['0']]
payload = wire_encode('named_struct('+','.join("'"+col+"',"+col for col in cols+['old_json'])+')')
phase('retain-wide-raw',f'''INSERT INTO {R} SELECT source_feed,source_epoch,source_delivery_id,'synthetic-wide-edge',source_cursor_json,
 payload,sha2(payload,256),schema_revision,current_timestamp(),apply_batch_id
 FROM (SELECT *,{payload} payload FROM {S} VERSION AS OF 0)''')
new_json = "concat('\"',get_json_object(props_json,'$.105'),'\"')"
journal = f'''SELECT source_system,'edge' entity_kind,rel_type_id type_id,id,cast(105 AS BIGINT) property_id,
 entity_version,'set' operation,true old_present,old_json,true new_present,{new_json} new_json,schema_revision,
 source_feed,source_epoch,source_position,cast(0 AS BIGINT) event_ordinal,cast(NULL AS STRING) source_time_text,
 published_at,apply_batch_id,source_cursor_json,source_delivery_id FROM {S} VERSION AS OF 0'''
phase('append-wide-history',f'INSERT INTO {J} {journal}')
rv,jv = version(R,'new-raw-version'),version(J,'new-journal-version')
assert rv==jv==2
assert phase('wide-raw-count',f"SELECT count(*),count(DISTINCT delivery_id) FROM {R} VERSION AS OF {rv} WHERE apply_batch_id='r94'")==[['100000','100000']]
qualified_payload = wire_encode('named_struct('+','.join("'"+col+"',s."+col for col in cols+['old_json'])+')')
assert phase('wide-raw-reference-parity',f'''SELECT count(*) FROM {S} VERSION AS OF 0 s LEFT JOIN {R} VERSION AS OF {rv} r
 ON s.source_feed=r.source_feed AND s.source_epoch=r.source_epoch AND s.source_delivery_id=r.delivery_id
 WHERE r.delivery_id IS NULL OR NOT (s.source_cursor_json <=> r.source_cursor_json) OR NOT (r.payload_digest <=> sha2(r.payload_json,256))
 OR NOT (s.published_at <=> cast(get_json_object(r.payload_json,'$.published_at') AS TIMESTAMP))
 OR NOT (hex(encode(r.payload_json,'UTF-8')) <=> hex(encode({qualified_payload},'UTF-8')))''')==[['0']]
actual_journal = f"SELECT * FROM {J} VERSION AS OF {jv} WHERE apply_batch_id='r94'"
assert phase('wide-journal-parity',f'SELECT count(*) FROM (({journal} EXCEPT ALL {actual_journal}) UNION ALL ({actual_journal} EXCEPT ALL {journal}))')==[['0']]
phase('apply-wide-current',f'''MERGE INTO {E} t USING (SELECT {','.join(cols)} FROM {S} VERSION AS OF 0) s
 ON t.lookup_hash=s.lookup_hash AND t.source_system=s.source_system AND t.rel_type_id=s.rel_type_id AND t.id=s.id
 WHEN MATCHED AND t.entity_version=1 AND t.apply_batch_id='r89' THEN UPDATE SET *''')
ev = version(E,'new-current-version')
assert ev==3
assert phase('output-cardinality',f'SELECT count(*),count(DISTINCT id),count_if(entity_version=2) FROM {E} VERSION AS OF {ev}')==[['20000000','20000000','100000']]
checks = [f"NOT (hex(encode(a.{col},'UTF-8')) <=> hex(encode(s.{col},'UTF-8')))" if col in text else f'NOT (a.{col} <=> s.{col})' for col in cols]
assert phase('affected-output-full-parity',f'''SELECT count(*) FROM
 (SELECT * FROM {E} VERSION AS OF {ev} WHERE entity_version=2 AND apply_batch_id='r94') a
 FULL OUTER JOIN {S} VERSION AS OF 0 s ON a.source_system=s.source_system AND a.rel_type_id=s.rel_type_id AND a.id=s.id
 WHERE a.id IS NULL OR s.id IS NULL OR '''+' OR '.join(checks))==[['0']]
vector = {N:0,E:ev,R:rv,J:jv,T:0}
validation = {'wire_encoder':'synthetic-full-carrier/2 UTC microseconds; independent timestamp check required','changed_rows':100000,'affected_carriers':'All20 fields compared exactly; previous fields/lexical patch checked before apply',
              'journal':'100k large present-to-present property105 tokens; exact parity',
              'baseline_origins':'Unqualified; baseline raw input unavailable','writer':'Serialized synthetic only; no fencing/receipt/real acknowledgement',
              'structural_projections':'Not deployed; endpoints/identity/retained fields unchanged',
              'predecessor':'r89 edge1; full r91 parity establishes semantic equivalence to maintenance edge2',
              'validation_scope':'Affected rows plus global identity counts, not an exhaustive new all-row field scan'}
values = ['r94','ashlar-delta/0.3-synthetic-changed-origin-slice',json.dumps(vector,sort_keys=True,separators=(',',':')),
          json.dumps({'epoch':'entropy-wide-r94','stage':S,'stage_version':0,'members':100000,'source_ack':'None; synthetic complete stage'},sort_keys=True,separators=(',',':')),
          '{"synthetic":"synthetic-r1"}',json.dumps(validation,sort_keys=True,separators=(',',':'))]
phase('publish-wide-vector',f'INSERT INTO {M} SELECT '+','.join(map(lit,values))+',current_timestamp()')
assert phase('wide-descriptor-readback',f"SELECT publication_id,profile_version,table_versions_json,source_progress_json,schema_revisions_json,validation_report_json FROM {M} WHERE publication_id='r94'")==[values]
report.update(state='passed wide-journal synthetic publication',versions=vector,validation=validation,
              arrival_to_verified_manifest_seconds=time.time()-arrival,elapsed_seconds=time.time()-start,stage=S)
for label,table in [('stage',S),('current',E),('raw',R),('journal',J)]:
    phase('detail-'+label,'DESCRIBE DETAIL '+table)
c.history()
(O/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
