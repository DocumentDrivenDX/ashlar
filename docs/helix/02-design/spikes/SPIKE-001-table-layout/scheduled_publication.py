"""Finite controlled admission schedule: two 10k/s windows plus a 100k/s burst.

Pre-staged synthetic inputs are gated by the controller; no network/extraction
rate claim. Three causal updates of the same 100k opaque hot-set members.
"""
import json
import time
from pathlib import Path
from persistent_sql import Client
from wire_json import encode

B=Path(__file__).resolve().parent
O=B/'out/native/ashlar_scheduled_publication_20261006_r96'
F='client_dev.ashlar_entropy_20261006_r86'
E,N=F+'.edge_current',F+'.object_current'
R,J,T,M=[F+'.'+name+'_r89' for name in ('source_record','property_journal','tombstone','publication_manifest')]
# Only the first preflight read was submitted by the earlier stopped attempt.
if (O/'summary.json').exists():
    previous=[json.loads(line) for line in (O/'statements.jsonl').read_text().splitlines()]
    assert len(previous)==1 and previous[0]['label']=='initial-current-version' and previous[0]['response']['status']['state']=='SUCCEEDED'
    O=O/'after-maintenance-preflight'
if (O/'summary.json').exists():
    previous=[json.loads(line) for line in (O/'statements.jsonl').read_text().splitlines()]
    assert len(previous)==4 and all(r['label'].startswith('initial-') and r['sql'].startswith('DESCRIBE HISTORY') and r['response']['status']['state']=='SUCCEEDED' for r in previous)
    O=O.parent/'version-aware-preflight'
assert not (O/'summary.json').exists(),'Inspect existing handles/commits; no blind re-run'
c=Client(O,observation_timeout=960,cancel_after=900)
cols=['source_system','rel_type_id','id','source_type','source_id','target_type','target_id','schema_revision',
      'entity_version','props_json','retained_json','order_key','source_feed','source_epoch','source_position',
      'published_at','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id']
texts={'source_system','schema_revision','props_json','retained_json','order_key','source_feed','source_epoch',
       'lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id'}
report={'state':'preparing controlled inputs','batches':[],'phases':[],
        'scope':'Finite synthetic hot-set batch admission, prepared typed input outside publisher clock; no real network producer, reader load, fencing/acknowledgement, baseline-origin, sustained-rate or billion-scale admission'}
def save(): (O/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
def sql(label,statement):
    rows=c.sql(label,statement)
    report['phases'].append({'label':label,'caller_ms':c.records[-1]['wall_ms'],'result':rows})
    save();print(label,round(c.records[-1]['wall_ms']),flush=True)
    assert not c.records[-1]['cancel_requested'],'Cancel bound reached; inspect committed state'
    return rows
def ver(table,label): return int(sql(label,f'DESCRIBE HISTORY {table} LIMIT 1')[0][0])
def lit(value): return "decode(unhex('"+value.encode().hex()+"'),'UTF-8')"
def eq(a,b,col):
    return f"hex(encode({a},'UTF-8')) <=> hex(encode({b},'UTF-8'))" if col in texts else f'{a} <=> {b}'
initial_version=ver(E,'initial-current-version')
assert initial_version>=3
history=sql('initial-current-lineage',f'DESCRIBE HISTORY {E} LIMIT 20')
names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
lineage=[dict(zip(names,row)) for row in history]
assert {int(row['version']) for row in lineage if int(row['version'])>=3}==set(range(3,initial_version+1))
assert all(row['operation']=='OPTIMIZE' for row in lineage if int(row['version'])>3)
report['initial_version']=initial_version
report['intervening_maintenance']=lineage
for role,table in [('raw',R),('journal',J)]:
    initial=ver(table,'initial-'+role+'-version')
    history=sql('initial-'+role+'-lineage',f'DESCRIBE HISTORY {table} LIMIT 20')
    names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
    lineage=[dict(zip(names,value)) for value in history]
    assert {int(value['version']) for value in lineage if int(value['version'])>=2}==set(range(2,initial+1))
    assert all(value['operation']=='OPTIMIZE' for value in lineage if int(value['version'])>2)
    report['initial_'+role+'_version']=initial
    report['intervening_'+role+'_maintenance']=lineage
assert sql('input-tables-absent',f"SHOW TABLES IN {F} LIKE 'schedule_r96_*'")==[]
inputs=[]
for i in range(3):
    batch=f'r96-b{i+1}';stage=F+f'.schedule_r96_{i+1}'
    base=f"SELECT {','.join(cols)} FROM {E} VERSION AS OF {initial_version} WHERE entity_version=2 AND apply_batch_id='r94'" if i==0 else f"SELECT {','.join(cols)} FROM {inputs[-1]} VERSION AS OF 0"
    opaque=f"concat_ws('',transform(sequence(0,cast(64+pmod(id,65) AS INT)), block -> sha2(concat('{batch}:',cast(id AS STRING),':',cast(block AS STRING)),256)))"
    overrides={'entity_version':'entity_version+1','props_json':"replace(props_json,concat('\"105\":\"',old_opaque,'\"'),concat('\"105\":\"',new_opaque,'\"'))",
               'source_epoch':"'schedule-r96'",'apply_batch_id':lit(batch),'published_at':'current_timestamp()',
               'source_delivery_id':f"concat('{batch}:edge:',cast(id AS STRING))",
               'source_cursor_json':f"concat('{{\"xid\":\"{9007199254741001+2*i}\",\"seq\":\"',cast(id AS STRING),'\"}}')"}
    sql('prepare-'+batch,f'''CREATE TABLE {stage} USING DELTA AS SELECT
    {','.join(overrides.get(col,col)+' AS '+col for col in cols)},concat('"',old_opaque,'"') old_json
    FROM (SELECT *,get_json_object(props_json,'$.105') old_opaque,{opaque} new_opaque FROM ({base}))''')
    assert ver(stage,'input-version-'+batch)==0
    assert sql('input-membership-'+batch,f'SELECT count(*),count(DISTINCT id) FROM {stage} VERSION AS OF 0')==[['100000','100000']]
    inputs.append(stage)
report['state']='running controlled admission'
clock_epoch=time.time();clock_mono=time.monotonic()
report['clock_epoch']=clock_epoch
windows=[(0,10,10000),(10,20,10000),(20,21,100000)]
save()
for i,(stage,(first,ready,rate)) in enumerate(zip(inputs,windows)):
    batch=f'r96-b{i+1}';old_v=ver(E,'before-current-'+batch)
    while time.monotonic()-clock_mono<ready:
        time.sleep(min(.2,ready-(time.monotonic()-clock_mono)))
    begin=time.monotonic()-clock_mono
    row={'batch':batch,'rows':100000,'nominal_arrival_rate':rate,'first_arrival_offset_s':first,
         'complete_input_ready_offset_s':ready,'publisher_start_offset_s':begin,'queue_wait_s':begin-ready,
         'source_clock':'uniform modeled per-record arrivals within controller window; complete pre-staged input released at window end',
         'source_stage':stage,'stage_version':0,'old_current_version':old_v}
    report['batches'].append(row);save()
    # Complete declared ASCII token checks against the actual predecessor.
    baseline=f'''SELECT /*+ BROADCAST(k) */ b.* FROM {E} VERSION AS OF {old_v} b JOIN
    (SELECT source_system,rel_type_id,id FROM {stage} VERSION AS OF 0) k
    ON b.source_system=k.source_system AND b.rel_type_id=k.rel_type_id AND b.id=k.id'''
    expected={col:'b.'+col for col in cols}
    expected.update(entity_version='b.entity_version+1',
        props_json="replace(b.props_json,concat('\"105\":',s.old_json),concat('\"105\":\"',get_json_object(s.props_json,'$.105'),'\"'))",
        source_epoch="'schedule-r96'",apply_batch_id=lit(batch),published_at='s.published_at',
        source_delivery_id=f"concat('{batch}:edge:',cast(s.id AS STRING))",
        source_cursor_json=f"concat('{{\"xid\":\"{9007199254741001+2*i}\",\"seq\":\"',cast(s.id AS STRING),'\"}}')")
    tests=['NOT('+eq('s.'+col,expected[col],col)+')' for col in cols]
    assert sql('intended-'+batch,f'''SELECT count(*) FROM ({baseline}) b FULL OUTER JOIN {stage} VERSION AS OF 0 s
    ON b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id
    WHERE b.id IS NULL OR s.id IS NULL OR instr(b.props_json,concat('"105":',s.old_json))=0
    OR NOT(s.old_json RLIKE '^"[0-9a-f]+"$') OR s.old_json=concat('"',get_json_object(s.props_json,'$.105'),'"')
    OR '''+' OR '.join(tests))==[['0']]
    payload=encode('named_struct('+','.join("'"+col+"',"+col for col in cols+['old_json'])+')')
    sql('capture-'+batch,f'''INSERT INTO {R} SELECT source_feed,source_epoch,source_delivery_id,'synthetic-scheduled-wide-edge',source_cursor_json,
    payload,sha2(payload,256),schema_revision,current_timestamp(),apply_batch_id
    FROM (SELECT *,{payload} payload FROM {stage} VERSION AS OF 0)''')
    journal=f'''SELECT source_system,'edge' entity_kind,rel_type_id type_id,id,cast(105 AS BIGINT) property_id,
    entity_version,'set' operation,true old_present,old_json,true new_present,concat('"',get_json_object(props_json,'$.105'),'"') new_json,
    schema_revision,source_feed,source_epoch,source_position,cast(0 AS BIGINT) event_ordinal,cast(NULL AS STRING) source_time_text,
    published_at,apply_batch_id,source_cursor_json,source_delivery_id FROM {stage} VERSION AS OF 0'''
    sql('journal-'+batch,f'INSERT INTO {J} {journal}')
    rv,jv=ver(R,'raw-version-'+batch),ver(J,'journal-version-'+batch)
    assert rv>=3+i and jv>=3+i
    qualified=encode('named_struct('+','.join("'"+col+"',s."+col for col in cols+['old_json'])+')')
    assert sql('raw-parity-'+batch,f'''SELECT count(*) FROM {stage} VERSION AS OF 0 s LEFT JOIN {R} VERSION AS OF {rv} r
    ON s.source_feed=r.source_feed AND s.source_epoch=r.source_epoch AND s.source_delivery_id=r.delivery_id
    WHERE r.delivery_id IS NULL OR NOT(s.source_cursor_json <=> r.source_cursor_json) OR NOT(r.payload_digest <=> sha2(r.payload_json,256))
    OR NOT(s.published_at <=> cast(get_json_object(r.payload_json,'$.published_at') AS TIMESTAMP))
    OR NOT(hex(encode(r.payload_json,'UTF-8')) <=> hex(encode({qualified},'UTF-8')))''')==[['0']]
    assert sql('raw-count-'+batch,f"SELECT count(*),count(DISTINCT delivery_id) FROM {R} VERSION AS OF {rv} WHERE apply_batch_id={lit(batch)}")==[['100000','100000']]
    actual=f"SELECT * FROM {J} VERSION AS OF {jv} WHERE apply_batch_id={lit(batch)}"
    assert sql('journal-parity-'+batch,f'SELECT count(*) FROM (({journal} EXCEPT ALL {actual}) UNION ALL ({actual} EXCEPT ALL {journal}))')==[['0']]
    previous='r94' if i==0 else f'r96-b{i}'
    sql('apply-'+batch,f'''MERGE INTO {E} t USING (SELECT {','.join(cols)} FROM {stage} VERSION AS OF 0) s
    ON t.lookup_hash=s.lookup_hash AND t.source_system=s.source_system AND t.rel_type_id=s.rel_type_id AND t.id=s.id
    WHEN MATCHED AND t.entity_version={2+i} AND t.apply_batch_id={lit(previous)} THEN UPDATE SET *''')
    new_v=ver(E,'current-version-'+batch)
    assert new_v>old_v
    commits=sql('batch-current-lineage-'+batch,f'DESCRIBE HISTORY {E} LIMIT 20')
    names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
    recent=[dict(zip(names,value)) for value in commits if int(value[0])>old_v]
    assert len([value for value in recent if value['operation']=='MERGE'])==1
    assert all(value['operation'] in ('MERGE','OPTIMIZE') for value in recent)
    row['commits']=recent
    assert sql('global-identities-'+batch,f'SELECT count(*),count(DISTINCT id),count_if(entity_version={3+i}) FROM {E} VERSION AS OF {new_v}')==[['20000000','20000000','100000']]
    checks=['NOT('+eq('a.'+col,'s.'+col,col)+')' for col in cols]
    assert sql('output-parity-'+batch,f'''SELECT count(*) FROM (SELECT * FROM {E} VERSION AS OF {new_v}
    WHERE entity_version={3+i} AND apply_batch_id={lit(batch)}) a FULL OUTER JOIN {stage} VERSION AS OF 0 s
    ON a.source_system=s.source_system AND a.rel_type_id=s.rel_type_id AND a.id=s.id
    WHERE a.id IS NULL OR s.id IS NULL OR '''+' OR '.join(checks))==[['0']]
    vector={E:new_v,N:0,R:rv,J:jv,T:0}
    validation={'rows':100000,'predecessor':'r94' if i==0 else previous,'wire':'synthetic-full-carrier/2 explicit UTC microseconds; independent instant equality passed',
                'checks':'All20 affected carrier fields and lexical patch; exact journal; raw bytes/digests/origins and global identity counts',
                'baseline_origins':'Unqualified; legacy raw timestamp loss retained','writer':'Serialized synthetic; no real completeness/fencing/acknowledgement','projection_coverage':'None; unchanged endpoints'}
    progress={'input_stage':stage,'stage_version':0,'members':100000,'admission_window_offsets_s':[first,ready],'clock_epoch':clock_epoch,'source_ack':'None'}
    values=[batch,'ashlar-delta/0.3-synthetic-changed-origin-slice',json.dumps(vector,sort_keys=True,separators=(',',':')),
            json.dumps(progress,sort_keys=True,separators=(',',':')),'{"synthetic":"synthetic-r1"}',json.dumps(validation,sort_keys=True,separators=(',',':'))]
    sql('publish-'+batch,f'INSERT INTO {M} SELECT '+','.join(map(lit,values))+',current_timestamp()')
    assert sql('descriptor-'+batch,f"SELECT publication_id,profile_version,table_versions_json,source_progress_json,schema_revisions_json,validation_report_json FROM {M} WHERE publication_id={lit(batch)}")==[values]
    finished=time.monotonic()-clock_mono
    row.update(verified_manifest_offset_s=finished,processing_s=finished-begin,
               complete_input_to_manifest_s=finished-ready,oldest_modeled_record_freshness_s=finished-first,
               versions=vector,complete_batches_waiting_at_finish=sum(end<=finished for _,end,_ in windows[i+1:]))
    save()
report['state']='completed finite controlled schedule; correctness checks passed'
report['elapsed_from_first_arrival_s']=time.monotonic()-clock_mono
report['source_preparation_excluded_from_publisher_clock']=True
for label,table in [('current',E),('raw',R),('journal',J)]:sql('final-detail-'+label,'DESCRIBE DETAIL '+table)
c.history();save()
