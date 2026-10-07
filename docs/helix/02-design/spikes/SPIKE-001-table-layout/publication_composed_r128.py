"""One bounded synthetic property publication with explicit adjacency reuse.
Existing warehouse only; no write retry or resource provisioning.
"""
import argparse,json,time,threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from driver_sql import DriverClient
from wire_json import encode
from property_apply_queries import PropertyApply
from property_reuse_proof import prove
from isolation_reader import expected_r103,ExactReader
from isolation_controller import compare_readers

def main():
    args=argparse.Namespace(preflight_only=False)
    B=Path(__file__).resolve().parent
    O=B/('out/native/ashlar_isolation_r128_preflight' if args.preflight_only else 'out/native/ashlar_isolation_r128')
    F='client_dev.ashlar_entropy_20261006_r86'
    E,N=F+'.edge_current',F+'.object_current'
    R,J,T,M=[F+'.'+name+'_r89' for name in ('source_record','property_journal','tombstone','publication_manifest')]
    assert not (O/'summary.json').exists(), 'Inspect handles and commits; no blind rerun'
    c=DriverClient(O);active_stop=None
    j=DriverClient(O/'journal-lane')
    cols=['source_system','rel_type_id','id','source_type','source_id','target_type','target_id','schema_revision',
          'entity_version','props_json','retained_json','order_key','source_feed','source_epoch','source_position',
          'published_at','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id']
    texts={'source_system','schema_revision','props_json','retained_json','order_key','source_feed','source_epoch',
           'lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id'}
    report={'state':'preparing controlled inputs','batches':[],'phases':[],
            'scope':'Finite synthetic hot-set batch admission, prepared input outside publisher clock, one publisher with parallel raw/journal validation; no real network producer or fencing/acknowledgement, baseline-origin, sustained-rate or billion-scale admission'}
    def save(): (O/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
    def sql(label,statement):
        if active_stop is not None and active_stop.is_set():raise RuntimeError('Reader failure: inspect completed writes; no retry')
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
    assert initial_version==18
    history=sql('initial-current-lineage',f'DESCRIBE HISTORY {E} LIMIT 20')
    names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
    lineage=[dict(zip(names,row)) for row in history]
    assert {int(row['version']) for row in lineage if int(row['version'])>=18}==set(range(18,initial_version+1))
    assert all(row['operation']=='OPTIMIZE' for row in lineage if int(row['version'])>18)
    report['initial_version']=initial_version
    report['comparison']='One serialized property publisher; parallel raw/journal validation, qualified forward adjacency reuse, no simultaneous readers.'
    report['intervening_maintenance']=lineage
    for role,table in [('raw',R),('journal',J)]:
        anchor=14 if role=='raw' else 16
        initial=ver(table,'initial-'+role+'-version')
        history=sql('initial-'+role+'-lineage',f'DESCRIBE HISTORY {table} LIMIT 20')
        names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
        lineage=[dict(zip(names,value)) for value in history]
        assert {int(value['version']) for value in lineage if int(value['version'])>=anchor}==set(range(anchor,initial+1))
        assert all(value['operation']=='OPTIMIZE' for value in lineage if int(value['version'])>anchor)
        report['initial_'+role+'_version']=initial
        report['intervening_'+role+'_maintenance']=lineage
    versions=json.loads((B/'out/native/ashlar_isolation_r123/audited-summary.json').read_text())['batches'][0]['versions']
    assert sql('published-predecessor',f"SELECT table_versions_json FROM {M} WHERE publication_id='r123-b1'")==[[json.dumps(versions,sort_keys=True,separators=(',',':'))]]
    if args.preflight_only:
        report['state']='read-only lineage and descriptor preflight passed; no staged input or publication'
        save();c.history();j.history();c.close();j.close();return
    assert sql('input-tables-absent',f"SHOW TABLES IN {F} LIKE 'schedule_r128_*'")==[]
    inputs=[]
    for i in range(1):
        batch=f'r128-b{i+1}';stage=F+f'.schedule_r128_{i+1}'
        base=f"SELECT {','.join(cols)} FROM {E} VERSION AS OF {initial_version} WHERE entity_version=12 AND apply_batch_id='r123-b1'" if i==0 else f"SELECT {','.join(cols)} FROM {inputs[-1]} VERSION AS OF 0"
        opaque=f"concat_ws('',transform(sequence(0,cast(64+pmod(id,65) AS INT)), block -> sha2(concat('{batch}:',cast(id AS STRING),':',cast(block AS STRING)),256)))"
        overrides={'entity_version':'entity_version+1','props_json':"replace(props_json,concat('\"105\":\"',old_opaque,'\"'),concat('\"105\":\"',new_opaque,'\"'))",
                   'source_epoch':"'schedule-r128'",'apply_batch_id':lit(batch),'published_at':'current_timestamp()',
                   'source_delivery_id':f"concat('{batch}:edge:',cast(id AS STRING))",
                   'source_cursor_json':f"concat('{{\"xid\":\"{9007199254741035+2*i}\",\"seq\":\"',cast(id AS STRING),'\"}}')"}
        sql('prepare-'+batch,f'''CREATE TABLE {stage} USING DELTA AS SELECT
        {','.join(overrides.get(col,col)+' AS '+col for col in cols)},concat('"',old_opaque,'"') old_json
        FROM (SELECT *,get_json_object(props_json,'$.105') old_opaque,{opaque} new_opaque FROM ({base}))''')
        assert ver(stage,'input-version-'+batch)==0
        assert sql('input-membership-'+batch,f'SELECT count(*),count(DISTINCT id) FROM {stage} VERSION AS OF 0')==[['100000','100000']]
        inputs.append(stage)
    for client,name in ((j,'journal'),):
        client.sql(name+'-timeout-set','SET STATEMENT_TIMEOUT=180')
        assert client.sql(name+'-cache-readback','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
    def publish(stop):
        nonlocal active_stop
        active_stop=stop
        clock_epoch=time.time();clock_mono=time.monotonic()
        report['clock_epoch']=clock_epoch;report['clock_monotonic']=clock_mono
        windows=[(0,10,10000)]
        for i,(stage,(first,ready,rate)) in enumerate(zip(inputs,windows)):
            batch=f'r128-b{i+1}';old_v=ver(E,'before-current-'+batch)
            while time.monotonic()-clock_mono<ready:
                time.sleep(min(.2,ready-(time.monotonic()-clock_mono)))
            begin=time.monotonic()-clock_mono
            row={'batch':batch,'rows':100000,'nominal_arrival_rate':rate,'first_arrival_offset_s':first,
                 'complete_input_ready_offset_s':ready,'publisher_start_offset_s':begin,'queue_wait_s':begin-ready,
                 'source_clock':'uniform modeled per-record arrivals within controller window; complete pre-staged input released at window end',
                 'source_stage':stage,'stage_version':0,'old_current_version':old_v}
            report['batches'].append(row);save()
            q=PropertyApply(E,stage,old_v,12+i,batch,'r123-b1','schedule-r128',9007199254741035+2*i)
            assert sql('intended-'+batch,q.intended())==[['0']]
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
            assert rv>=15+i and jv>=17+i
            qualified=encode('named_struct('+','.join("'"+col+"',s."+col for col in cols+['old_json'])+')')
            raw_validation=f'''SELECT count(*) FROM {stage} VERSION AS OF 0 s LEFT JOIN (SELECT * FROM {R} VERSION AS OF {rv} WHERE apply_batch_id={lit(batch)}) r
            ON s.source_feed=r.source_feed AND s.source_epoch=r.source_epoch AND s.source_delivery_id=r.delivery_id
            WHERE r.delivery_id IS NULL OR NOT(s.schema_revision <=> r.schema_revision) OR NOT(s.apply_batch_id <=> r.apply_batch_id)
            OR NOT(r.record_kind <=> 'synthetic-scheduled-wide-edge') OR r.received_at IS NULL
            OR NOT(s.source_cursor_json <=> r.source_cursor_json) OR NOT(r.payload_digest <=> sha2(r.payload_json,256))
            OR NOT(s.published_at <=> cast(get_json_object(r.payload_json,'$.published_at') AS TIMESTAMP))
            OR NOT(hex(encode(r.payload_json,'UTF-8')) <=> hex(encode({qualified},'UTF-8')))'''
            actual=f"SELECT * FROM {J} VERSION AS OF {jv} WHERE apply_batch_id={lit(batch)}"
            journal_validation=f'SELECT count(*) FROM (({journal} EXCEPT ALL {actual}) UNION ALL ({actual} EXCEPT ALL {journal}))'
            validation_start=time.monotonic()
            with ThreadPoolExecutor(max_workers=2) as validation_pool:
                raw_check=validation_pool.submit(sql,'raw-parity-'+batch,raw_validation)
                journal_check=validation_pool.submit(j.sql,'journal-parity-'+batch,journal_validation)
                assert raw_check.result()==[['0']]
                assert journal_check.result()==[['0']]
            row['validation_pair_wall_s']=time.monotonic()-validation_start
            report['phases'].append({'label':'journal-parity-'+batch,'caller_ms':j.records[-1]['wall_ms'],'result':[['0']]})
            assert sql('raw-count-'+batch,f"SELECT count(*),count(DISTINCT delivery_id) FROM {R} VERSION AS OF {rv} WHERE apply_batch_id={lit(batch)}")==[['100000','100000']]
            save()
            previous='r123-b1' if i==0 else f'r128-b{i}'
            sql('apply-'+batch,q.apply())
            new_v=ver(E,'current-version-'+batch)
            assert new_v>old_v
            commits=sql('batch-current-lineage-'+batch,f'DESCRIBE HISTORY {E} LIMIT 20')
            names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
            recent=[dict(zip(names,value)) for value in commits if int(value[0])>old_v]
            assert len([value for value in recent if value['operation']=='MERGE'])==1
            assert all(value['operation'] in ('MERGE','OPTIMIZE') for value in recent)
            assert {int(value['version']) for value in recent}==set(range(old_v+1,new_v+1))
            row['commits']=recent
            assert sql('global-identities-'+batch,q.identities(new_v))==[['20000000','20000000','100000']]
            assert sql('output-parity-'+batch,q.output(new_v))==[['0']]
            adj=F+'.adjacency_canonical_r120'
            baseline_path=B/'out/native/ashlar_isolation_r123'
            baseline_records=[json.loads(l) for l in (baseline_path/'statements.jsonl').read_text().splitlines() if json.loads(l)['label']=='adjacency-reuse-r123-b1']
            baseline_history={h['query_id']:h for h in json.loads((baseline_path/'query-history.json').read_text()) if h['query_id']==baseline_records[0]['statement_id']}
            proof_start=time.monotonic()
            proof=prove(q,new_v,adj,1,1,100000,20000000,baseline_records+c.records,
                        dict(baseline_history,**{h['query_id']:h for h in c.history()}),recent,
                        mode='serialized-synthetic-property105')
            row['composed_proof_wall_s']=time.monotonic()-proof_start
            row['structural_reuse_proof']=proof
            save()
            vector={E:new_v,N:0,R:rv,J:jv,T:0,adj:1}
            validation={'rows':100000,'predecessor':'r123-b1' if i==0 else previous,'wire':'synthetic-full-carrier/2 explicit UTC microseconds; independent instant equality passed',
                        'checks':'All20 affected carrier fields and lexical patch; exact journal; raw bytes/digests/origins and global identity counts',
                        'baseline_origins':'Unqualified; legacy raw timestamp loss retained','writer':'One synthetic publisher with parallel independent raw/journal appends; no real completeness/fencing/acknowledgement','projection_coverage':'Full forward adjacency reused at adjacency_canonical_r120 v1, structural revision1. Inherited full baseline plus owned full-carrier checks and closed apply lineage prove structural reuse; independent full20M oracle runs after publication. Reverse/degree/typed property release unavailable.'}
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
        return report['batches']
    try:
        publish(threading.Event())
        active_stop=None
        latest=report['batches'][-1]
        structural=f'SELECT source_system,rel_type_id,id edge_id,source_type,source_id,target_type,target_id,cast(1 AS BIGINT) structural_version FROM {E} VERSION AS OF {latest["versions"][E]}'
        adj=F+'.adjacency_canonical_r120'
        assert sql('post-structural-oracle',f'SELECT count(*) FROM ((SELECT * FROM {adj} VERSION AS OF 1 EXCEPT ALL {structural}) UNION ALL ({structural} EXCEPT ALL SELECT * FROM {adj} VERSION AS OF 1))')==[['0']]
        report['post_structural_oracle']='Passed full20M parity outside recorded publication clock'

        fresh_rows=sql('fresh-reader-oracle',f"SELECT {','.join(cols)} FROM {latest['source_stage']} VERSION AS OF 0 ORDER BY id LIMIT 30")
        client=DriverClient(O/'fresh')
        try:
            reader=ExactReader(client,latest['versions'],fresh_rows,publication_id=latest['batch'])
            reader.preflight()
            for index in range(30):reader.read('fresh-post',index)
            client.history()
        finally:client.close()
        report['state']='completed one property publication with qualified full forward adjacency reuse; final audit pending'
        save()
    finally:
        c.history();j.history();c.close();j.close()

if __name__=='__main__':main()
