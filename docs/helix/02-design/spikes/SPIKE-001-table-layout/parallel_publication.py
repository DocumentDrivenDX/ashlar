"""One finite 100k batch with overlapping appends and bounded reader contention.

Pre-staged synthetic inputs are gated by the controller; no network/extraction
rate claim. One causal update of the existing 100k opaque hot-set members.
"""
import json
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from pathlib import Path
from driver_sql import DriverClient
from wire_json import encode

B=Path(__file__).resolve().parent
O=B/'out/native/ashlar_parallel_publication_20261007_r101'
F='client_dev.ashlar_entropy_20261006_r86'
E,N=F+'.edge_current',F+'.object_current'
R,J,T,M=[F+'.'+name+'_r89' for name in ('source_record','property_journal','tombstone','publication_manifest')]
assert not (O/'summary.json').exists(), 'Inspect handles and commits; no blind rerun'
c=DriverClient(O)
j=DriverClient(O/'journal-lane')
cols=['source_system','rel_type_id','id','source_type','source_id','target_type','target_id','schema_revision',
      'entity_version','props_json','retained_json','order_key','source_feed','source_epoch','source_position',
      'published_at','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id']
texts={'source_system','schema_revision','props_json','retained_json','order_key','source_feed','source_epoch',
       'lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id'}
report={'state':'preparing controlled inputs','batches':[],'phases':[],
        'scope':'Finite synthetic hot-set batch admission, prepared typed input outside publisher clock, one bounded reader; no real network producer or fencing/acknowledgement, baseline-origin, sustained-rate or billion-scale admission'}
def save(): (O/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
def sql(label,statement):
    rows=c.sql(label,statement)
    report['phases'].append({'label':label,'caller_ms':c.records[-1]['wall_ms'],'result':rows})
    save();print(label,round(c.records[-1]['wall_ms']),flush=True)
    assert not c.records[-1].get('cancel_requested',False),'Cancel bound reached; inspect committed state'
    return rows
def ver(table,label): return int(sql(label,f'DESCRIBE HISTORY {table} LIMIT 1')[0][0])
def lit(value): return "decode(unhex('"+value.encode().hex()+"'),'UTF-8')"
def eq(a,b,col):
    return f"hex(encode({a},'UTF-8')) <=> hex(encode({b},'UTF-8'))" if col in texts else f'{a} <=> {b}'
sql('session-timeout-set','SET STATEMENT_TIMEOUT=180')
assert sql('session-timeout-readback','SET STATEMENT_TIMEOUT')[0][-1]=='180'
assert sql('session-cache-readback','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
journal_details=sql('journal-statistics-preflight','DESCRIBE DETAIL '+J)
assert 'apply_batch_id' in json.loads(journal_details[0][[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']].index('properties')])['delta.dataSkippingStatsColumns'].split(',')
initial_version=ver(E,'initial-current-version')
assert initial_version>=12
history=sql('initial-current-lineage',f'DESCRIBE HISTORY {E} LIMIT 20')
names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
lineage=[dict(zip(names,row)) for row in history]
assert {int(row['version']) for row in lineage if int(row['version'])>=12}==set(range(12,initial_version+1))
assert all(row['operation']=='OPTIMIZE' for row in lineage if int(row['version'])>12)
report['initial_version']=initial_version
report['comparison']='r101: parallel independent raw/journal appends, batch statistics enabled and one concurrent reader. r99 had serial appends and no reader. Different workload/resource contention; no isolated speedup attribution.'
report['intervening_maintenance']=lineage
for role,table in [('raw',R),('journal',J)]:
    anchor=10 if role=='raw' else 12
    initial=ver(table,'initial-'+role+'-version')
    history=sql('initial-'+role+'-lineage',f'DESCRIBE HISTORY {table} LIMIT 20')
    names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
    lineage=[dict(zip(names,value)) for value in history]
    assert {int(value['version']) for value in lineage if int(value['version'])>=anchor}==set(range(anchor,initial+1))
    assert all(value['operation']=='OPTIMIZE' for value in lineage if int(value['version'])>anchor)
    report['initial_'+role+'_version']=initial
    report['intervening_'+role+'_maintenance']=lineage
assert sql('input-tables-absent',f"SHOW TABLES IN {F} LIKE 'schedule_r101_*'")==[]
inputs=[]
for i in range(1):
    batch=f'r101-b{i+1}';stage=F+f'.schedule_r101_{i+1}'
    base=f"SELECT {','.join(cols)} FROM {E} VERSION AS OF {initial_version} WHERE entity_version=8 AND apply_batch_id='r99-b3'" if i==0 else f"SELECT {','.join(cols)} FROM {inputs[-1]} VERSION AS OF 0"
    opaque=f"concat_ws('',transform(sequence(0,cast(64+pmod(id,65) AS INT)), block -> sha2(concat('{batch}:',cast(id AS STRING),':',cast(block AS STRING)),256)))"
    overrides={'entity_version':'entity_version+1','props_json':"replace(props_json,concat('\"105\":\"',old_opaque,'\"'),concat('\"105\":\"',new_opaque,'\"'))",
               'source_epoch':"'schedule-r101'",'apply_batch_id':lit(batch),'published_at':'current_timestamp()',
               'source_delivery_id':f"concat('{batch}:edge:',cast(id AS STRING))",
               'source_cursor_json':f"concat('{{\"xid\":\"{9007199254741021+2*i}\",\"seq\":\"',cast(id AS STRING),'\"}}')"}
    sql('prepare-'+batch,f'''CREATE TABLE {stage} USING DELTA AS SELECT
    {','.join(overrides.get(col,col)+' AS '+col for col in cols)},concat('"',old_opaque,'"') old_json
    FROM (SELECT *,get_json_object(props_json,'$.105') old_opaque,{opaque} new_opaque FROM ({base}))''')
    assert ver(stage,'input-version-'+batch)==0
    assert sql('input-membership-'+batch,f'SELECT count(*),count(DISTINCT id) FROM {stage} VERSION AS OF 0')==[['100000','100000']]
    inputs.append(stage)
r=DriverClient(O/'reader')
for client,name in ((j,'journal'),(r,'reader')):
    client.sql(name+'-timeout-set','SET STATEMENT_TIMEOUT=180')
    assert client.sql(name+'-timeout-readback','SET STATEMENT_TIMEOUT')[0][-1]=='180'
    assert client.sql(name+'-cache-readback','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
assert sql('published-predecessor',f"SELECT table_versions_json FROM {M} WHERE publication_id='r99-b3'")==[[json.dumps({E:12,N:0,R:10,J:10,T:0},sort_keys=True,separators=(',',':'))]]
expected_rows=sql('reader-expected-cohort',f"SELECT {','.join(cols)} FROM {F}.schedule_r99_3 VERSION AS OF 0 ORDER BY id LIMIT 30")
assert len(expected_rows)==30 and len({v[2] for v in expected_rows})==30
report['reader_profile']='One client, 30 existing large-token hot-set keys, full20 fields, exact immutable predecessor-stage expectations, pinned r99-b3 canonical version12; idle30/load<=250/post30. Not a representative graph-wide read workload or cold-data benchmark.'
def reader_query(phase,index):
    row=expected_rows[index%len(expected_rows)]
    assert len(row[16])==64 and all(ch in '0123456789abcdef' for ch in row[16])
    query=f"SELECT {','.join(cols)} FROM {E} VERSION AS OF 12 WHERE lookup_hash={lit(row[16])} AND source_system={lit(row[0])} AND rel_type_id={int(row[1])} AND id={int(row[2])}"
    assert r.sql(f'{phase}-{index}',query)==[row], 'Published predecessor full-carrier mismatch'
for index in range(30): reader_query('idle',index)
def reader_load(stop):
    for index in range(250):
        if stop.is_set(): break
        reader_query('load',index)
report['state']='running controlled admission'
clock_epoch=time.time();clock_mono=time.monotonic()
report['clock_epoch']=clock_epoch
report['clock_monotonic']=clock_mono
windows=[(0,10,10000)]
reader_stop=Event()
reader_pool=ThreadPoolExecutor(max_workers=1)
reader_future=reader_pool.submit(reader_load,reader_stop)
save()
for i,(stage,(first,ready,rate)) in enumerate(zip(inputs,windows)):
    batch=f'r101-b{i+1}';old_v=ver(E,'before-current-'+batch)
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
        source_epoch="'schedule-r101'",apply_batch_id=lit(batch),published_at='s.published_at',
        source_delivery_id=f"concat('{batch}:edge:',cast(s.id AS STRING))",
        source_cursor_json=f"concat('{{\"xid\":\"{9007199254741021+2*i}\",\"seq\":\"',cast(s.id AS STRING),'\"}}')")
    tests=['NOT('+eq('s.'+col,expected[col],col)+')' for col in cols]
    assert sql('intended-'+batch,f'''SELECT count(*) FROM ({baseline}) b FULL OUTER JOIN {stage} VERSION AS OF 0 s
    ON b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id
    WHERE b.id IS NULL OR s.id IS NULL OR instr(b.props_json,concat('"105":',s.old_json))=0
    OR NOT(s.old_json RLIKE '^"[0-9a-f]+"$') OR s.old_json=concat('"',get_json_object(s.props_json,'$.105'),'"')
    OR '''+' OR '.join(tests))==[['0']]
    payload=encode('named_struct('+','.join("'"+col+"',"+col for col in cols+['old_json'])+')')
    capture_sql=f'''INSERT INTO {R} SELECT source_feed,source_epoch,source_delivery_id,'synthetic-scheduled-wide-edge',source_cursor_json,
    payload,sha2(payload,256),schema_revision,current_timestamp(),apply_batch_id
    FROM (SELECT *,{payload} payload FROM {stage} VERSION AS OF 0)'''
    journal=f'''SELECT source_system,'edge' entity_kind,rel_type_id type_id,id,cast(105 AS BIGINT) property_id,
    entity_version,'set' operation,true old_present,old_json,true new_present,concat('"',get_json_object(props_json,'$.105'),'"') new_json,
    schema_revision,source_feed,source_epoch,source_position,cast(0 AS BIGINT) event_ordinal,cast(NULL AS STRING) source_time_text,
    published_at,apply_batch_id,source_cursor_json,source_delivery_id FROM {stage} VERSION AS OF 0'''
    append_start=time.monotonic()
    with ThreadPoolExecutor(max_workers=2) as append_pool:
        raw_future=append_pool.submit(c.sql,'capture-'+batch,capture_sql)
        journal_future=append_pool.submit(j.sql,'journal-'+batch,f'INSERT INTO {J} {journal}')
        raw_result=raw_future.result()
        journal_result=journal_future.result()
    row['append_pair_wall_s']=time.monotonic()-append_start
    for client,label,result in ((c,'capture-'+batch,raw_result),(j,'journal-'+batch,journal_result)):
        report['phases'].append({'label':label,'caller_ms':client.records[-1]['wall_ms'],'result':result})
        print(label,round(client.records[-1]['wall_ms']),flush=True)
    save()
    rv,jv=ver(R,'raw-version-'+batch),ver(J,'journal-version-'+batch)
    assert rv>=11+i and jv>=13+i
    qualified=encode('named_struct('+','.join("'"+col+"',s."+col for col in cols+['old_json'])+')')
    assert sql('raw-parity-'+batch,f'''SELECT count(*) FROM {stage} VERSION AS OF 0 s LEFT JOIN (SELECT * FROM {R} VERSION AS OF {rv} WHERE apply_batch_id={lit(batch)}) r
    ON s.source_feed=r.source_feed AND s.source_epoch=r.source_epoch AND s.source_delivery_id=r.delivery_id
    WHERE r.delivery_id IS NULL OR NOT(s.schema_revision <=> r.schema_revision) OR NOT(s.apply_batch_id <=> r.apply_batch_id)
    OR NOT(r.record_kind <=> 'synthetic-scheduled-wide-edge') OR r.received_at IS NULL
    OR NOT(s.source_cursor_json <=> r.source_cursor_json) OR NOT(r.payload_digest <=> sha2(r.payload_json,256))
    OR NOT(s.published_at <=> cast(get_json_object(r.payload_json,'$.published_at') AS TIMESTAMP))
    OR NOT(hex(encode(r.payload_json,'UTF-8')) <=> hex(encode({qualified},'UTF-8')))''')==[['0']]
    assert sql('raw-count-'+batch,f"SELECT count(*),count(DISTINCT delivery_id) FROM {R} VERSION AS OF {rv} WHERE apply_batch_id={lit(batch)}")==[['100000','100000']]
    actual=f"SELECT * FROM {J} VERSION AS OF {jv} WHERE apply_batch_id={lit(batch)}"
    assert sql('journal-parity-'+batch,f'SELECT count(*) FROM (({journal} EXCEPT ALL {actual}) UNION ALL ({actual} EXCEPT ALL {journal}))')==[['0']]
    previous='r99-b3' if i==0 else f'r101-b{i}'
    sql('apply-'+batch,f'''MERGE INTO {E} t USING (SELECT {','.join(cols)} FROM {stage} VERSION AS OF 0) s
    ON t.lookup_hash=s.lookup_hash AND t.source_system=s.source_system AND t.rel_type_id=s.rel_type_id AND t.id=s.id
    WHEN MATCHED AND t.entity_version={8+i} AND t.apply_batch_id={lit(previous)} THEN UPDATE SET *''')
    new_v=ver(E,'current-version-'+batch)
    assert new_v>old_v
    commits=sql('batch-current-lineage-'+batch,f'DESCRIBE HISTORY {E} LIMIT 20')
    names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
    recent=[dict(zip(names,value)) for value in commits if int(value[0])>old_v]
    assert len([value for value in recent if value['operation']=='MERGE'])==1
    assert all(value['operation'] in ('MERGE','OPTIMIZE') for value in recent)
    assert {int(value['version']) for value in recent}==set(range(old_v+1,new_v+1))
    row['commits']=recent
    assert sql('global-identities-'+batch,f'SELECT count(*),count(DISTINCT id),count_if(entity_version={9+i}) FROM {E} VERSION AS OF {new_v}')==[['20000000','20000000','100000']]
    checks=['NOT('+eq('a.'+col,'s.'+col,col)+')' for col in cols]
    assert sql('output-parity-'+batch,f'''SELECT count(*) FROM (SELECT * FROM {E} VERSION AS OF {new_v}
    WHERE entity_version={9+i} AND apply_batch_id={lit(batch)}) a FULL OUTER JOIN {stage} VERSION AS OF 0 s
    ON a.source_system=s.source_system AND a.rel_type_id=s.rel_type_id AND a.id=s.id
    WHERE a.id IS NULL OR s.id IS NULL OR '''+' OR '.join(checks))==[['0']]
    vector={E:new_v,N:0,R:rv,J:jv,T:0}
    validation={'rows':100000,'predecessor':'r99-b3' if i==0 else previous,'wire':'synthetic-full-carrier/2 explicit UTC microseconds; independent instant equality passed',
                'checks':'All20 affected carrier fields and lexical patch; exact journal; raw bytes/digests/origins and global identity counts',
                'baseline_origins':'Unqualified; legacy raw timestamp loss retained','writer':'One synthetic publisher with parallel independent raw/journal appends; no real completeness/fencing/acknowledgement','projection_coverage':'None; unchanged endpoints'}
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
reader_stop.set()
reader_future.result()
reader_pool.shutdown()
for index in range(30): reader_query('post',index)
report['reader_counts']={phase:sum(rec['label'].startswith(phase+'-') for rec in r.records) for phase in ('idle','load','post')}
report['state']='completed parallel publication and bounded singleton contention; correctness checks passed'
report['elapsed_from_first_arrival_s']=time.monotonic()-clock_mono
report['source_preparation_excluded_from_publisher_clock']=True
for label,table in [('current',E),('raw',R),('journal',J)]:sql('final-detail-'+label,'DESCRIBE DETAIL '+table)
c.history();j.history();r.history();save();c.close();j.close();r.close()
