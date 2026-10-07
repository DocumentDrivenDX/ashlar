"""One bounded synthetic property publication with explicit adjacency reuse.
Existing warehouse only; no write retry or resource provisioning.
"""
import argparse,json,time,threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from bounded_reads_r145 import BoundedReads as DriverClient
from wire_json import encode
from raw_validation_queries import origin_sql,verified_origin,raw_validation as build_raw_validation
from property_apply_queries import PropertyApply
from property_reuse_proof import prove,baseline_sql
from isolation_reader import expected_r103,ExactReader
from isolation_controller import compare_readers

def main():
    args=argparse.Namespace(preflight_only=False)
    B=Path(__file__).resolve().parent
    O=B/('out/native/ashlar_queue_r189_preflight' if args.preflight_only else 'out/native/ashlar_queue_r189')
    F='client_dev.ashlar_entropy_20261006_r86'
    E,N=F+'.edge_current',F+'.object_current'
    R,J,T,M=[F+'.'+name+'_r89' for name in ('source_record','property_journal','tombstone','publication_manifest')]
    canonical_E=E
    original_R,original_J,original_M=R,J,M
    E,R,J,M=[F+'.'+name+'_queue_r189' for name in ('edge','raw','journal','manifest')]
    assert not O.exists(), 'Inspect handles and commits; no blind rerun'
    c=DriverClient(O);active_stop=None
    j=DriverClient(O/'journal-lane')
    a=DriverClient(O/'apply-lane')
    cols=['source_system','rel_type_id','id','source_type','source_id','target_type','target_id','schema_revision',
          'entity_version','props_json','retained_json','order_key','source_feed','source_epoch','source_position',
          'published_at','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id']
    texts={'source_system','schema_revision','props_json','retained_json','order_key','source_feed','source_epoch',
           'lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id'}
    report={'state':'preparing controlled inputs','batches':[],'phases':[],
            'scope':'Finite synthetic hot-set batch admission, prepared input outside publisher clock, one publisher with parallel raw/journal validation; no real network producer or fencing/acknowledgement, baseline-origin, sustained-rate or billion-scale admission'}
    def save(): (O/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
    deadline=time.monotonic()+600
    def sql(label,statement):
        assert time.monotonic()<deadline, "Controller admission deadline reached; inspect completed writes"
        if active_stop is not None and active_stop.is_set():raise RuntimeError('Reader failure: inspect completed writes; no retry')
        rows=c.sql(label,statement)
        report['phases'].append({'label':label,'caller_ms':c.records[-1]['wall_ms'],'result':rows})
        save();print(label,round(c.records[-1]['wall_ms']),flush=True)
        assert not c.records[-1].get('cancel_requested',False),'Cancel bound reached; inspect committed state'
        return rows
    def budget(reserve=0):
        histories={}
        for client in (c,j,a):
            client.cursor.close();client.cursor=client.connection.cursor()
        for attempt in range(12):
            histories={h['query_id']:h for client in (c,j,a) for h in client.history()}
            if all(r['statement_id'] in histories and histories[r['statement_id']]['is_final'] for client in (c,j,a) for r in client.records):break
            time.sleep(2)
        records=[r for client in (c,j,a) for r in client.records]
        assert all(histories[r['statement_id']]['is_final'] and histories[r['statement_id']]['status']=='FINISHED' for r in records)
        report['costs']={k:sum(histories[r['statement_id']]['metrics'].get(k,0) for r in records) for k in ('read_bytes','write_remote_bytes','spill_to_disk_bytes')};save()
        assert report['costs']['read_bytes']+reserve<=65000000000 and report['costs']['write_remote_bytes']<=3000000000,'Stop further admission: cost bounds exceeded'
        return histories
    def ver(table,label): return int(sql(label,f'DESCRIBE HISTORY {table} LIMIT 1')[0][0])
    def lit(value): return "decode(unhex('"+value.encode().hex()+"'),'UTF-8')"
    def eq(a,b,col):
        return f"hex(encode({a},'UTF-8')) <=> hex(encode({b},'UTF-8'))" if col in texts else f'{a} <=> {b}'
    sql('session-timeout-set','SET STATEMENT_TIMEOUT=180')
    assert sql('session-timeout-readback','SET STATEMENT_TIMEOUT')[0][-1]=='180'
    assert sql('session-cache-readback','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
    assert any(row[0]=='23' for row in sql('canonical-anchor',f'DESCRIBE HISTORY {canonical_E} LIMIT 10'))
    assert sql('owned-tables-absent',f"SHOW TABLES IN {F} LIKE '*queue_r189'")==[]
    for role,target,source,version in [('edge',E,canonical_E,23),('raw',R,original_R,17),('journal',J,original_J,19)]:
        sql('clone-'+role,f'CREATE TABLE {target} SHALLOW CLONE {source} VERSION AS OF {version}')
    sql('manifest-create',f'CREATE TABLE {M} USING DELTA AS SELECT * FROM {original_M} WHERE false')
    initial_version=ver(E,'initial-current-version')
    assert initial_version==0
    report['initial_version']=initial_version
    report['canonical_anchor']={'edge':23,'raw':17,'journal':19,'publication':'r139-b1'}
    report['owned_tables']=[E,R,J,M]
    report['comparison']='One synthetic origin-bounded batch with three independent write lanes on isolated clones; no readers or maintenance'
    assert sql('input-tables-absent',f"SHOW TABLES IN {F} LIKE 'schedule_r189_*'")==[]
    inputs=[]
    for i in range(1):
        batch=f'r189-b{i+1}';stage=F+f'.schedule_r189_{i+1}'
        base=f"SELECT {','.join(cols)} FROM {E} VERSION AS OF {initial_version} WHERE entity_version=15 AND apply_batch_id='r139-b1'" if i==0 else f"SELECT {','.join(cols)} FROM {inputs[-1]} VERSION AS OF 0"
        opaque=f"concat_ws('',transform(sequence(0,cast(length(get_json_object(props_json,'$.105'))/64-1 AS INT)), block -> sha2(concat('{batch}:',cast(id AS STRING),':',cast(block AS STRING)),256)))"
        overrides={'entity_version':'entity_version+1','props_json':"replace(props_json,concat('\"105\":\"',old_opaque,'\"'),concat('\"105\":\"',new_opaque,'\"'))",
                   'source_epoch':"'schedule-r189'",'apply_batch_id':lit(batch),'published_at':'current_timestamp()',
                   'source_delivery_id':f"concat('{batch}:edge:',cast(id AS STRING))",
                   'source_cursor_json':f"concat('{{\"xid\":\"{9007199254741101+2*i}\",\"seq\":\"',cast(id AS STRING),'\"}}')"}
        sql('prepare-'+batch,f'''CREATE TABLE {stage} USING DELTA AS SELECT
        {','.join(overrides.get(col,col)+' AS '+col for col in cols)},concat('"',old_opaque,'"') old_json
        FROM (SELECT *,get_json_object(props_json,'$.105') old_opaque,{opaque} new_opaque FROM ({base}))''')
        assert ver(stage,'input-version-'+batch)==0
        assert sql('input-membership-'+batch,f'SELECT count(*),count(DISTINCT id) FROM {stage} VERSION AS OF 0')==[['100000','100000']]
        inputs.append(stage)
    for client,name in ((j,'journal'),(a,'apply')):
        client.sql(name+'-timeout-set','SET STATEMENT_TIMEOUT=180')
        assert client.sql(name+'-cache-readback','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
    baseline_q=PropertyApply(E,inputs[0],initial_version,15,'r189-b1','r139-b1','schedule-r189',9007199254741101,eligibility_placement='on',input_ranges=6)
    assert sql('prepared-structural-baseline',baseline_sql(baseline_q,F+'.adjacency_canonical_r120',1,1))==[['0']]
    budget(50000000000)
    def publish(stop):
        nonlocal active_stop
        active_stop=stop
        clock_epoch=time.time();clock_mono=time.monotonic()
        report['clock_epoch']=clock_epoch;report['clock_monotonic']=clock_mono
        windows=[(0,10,10000)]
        for i,(stage,(first,ready,rate)) in enumerate(zip(inputs,windows)):
            batch=f'r189-b{i+1}';old_v=ver(E,'before-current-'+batch)
            while time.monotonic()-clock_mono<ready:
                time.sleep(min(.2,ready-(time.monotonic()-clock_mono)))
            begin=time.monotonic()-clock_mono
            row={'batch':batch,'rows':100000,'nominal_arrival_rate':rate,'first_arrival_offset_s':first,
                 'complete_input_ready_offset_s':ready,'publisher_start_offset_s':begin,'queue_wait_s':begin-ready,
                 'source_clock':'uniform modeled per-record arrivals within controller window; complete pre-staged input released at window end',
                 'source_stage':stage,'stage_version':0,'old_current_version':old_v}
            report['batches'].append(row);save()
            q=PropertyApply(E,stage,old_v,15+i,batch,('r139-b1' if i==0 else f'r189-b{i}'),'schedule-r189',9007199254741101+2*i,eligibility_placement='on',input_ranges=6)
            if i>0:
                assert sql('structural-baseline-'+batch,baseline_sql(q,F+'.adjacency_canonical_r120',1,1))==[['0']]
            assert sql('intended-'+batch,q.intended())==[['0']]
            budget(10000000000)
            payload=encode('named_struct('+','.join("'"+col+"',"+col for col in cols+['old_json'])+')')
            capture_sql=f'''INSERT INTO {R} SELECT source_feed,source_epoch,source_delivery_id,'synthetic-scheduled-wide-edge',source_cursor_json,
            payload,sha2(payload,256),schema_revision,current_timestamp(),apply_batch_id
            FROM (SELECT *,{payload} payload FROM {stage} VERSION AS OF 0)'''
            journal=f'''SELECT source_system,'edge' entity_kind,rel_type_id type_id,id,cast(105 AS BIGINT) property_id,
            entity_version,'set' operation,true old_present,old_json,true new_present,concat('"',get_json_object(props_json,'$.105'),'"') new_json,
            schema_revision,source_feed,source_epoch,source_position,cast(0 AS BIGINT) event_ordinal,cast(NULL AS STRING) source_time_text,
            published_at,apply_batch_id,source_cursor_json,source_delivery_id FROM {stage} VERSION AS OF 0'''
            append_start=time.monotonic()
            with ThreadPoolExecutor(max_workers=3) as append_pool:
                apply_future=append_pool.submit(a.sql,'apply-'+batch,q.apply())
                raw_future=append_pool.submit(c.sql,'capture-'+batch,capture_sql)
                journal_future=append_pool.submit(j.sql,'journal-'+batch,f'INSERT INTO {J} {journal}')
                raw_result=raw_future.result()
                journal_result=journal_future.result()
                apply_result=apply_future.result()
            row['append_pair_wall_s']=time.monotonic()-append_start
            for client,label,result in ((c,'capture-'+batch,raw_result),(j,'journal-'+batch,journal_result),(a,'apply-'+batch,apply_result)):
                report['phases'].append({'label':label,'caller_ms':client.records[-1]['wall_ms'],'result':result})
                print(label,round(client.records[-1]['wall_ms']),flush=True)
            save()
            rv,jv=ver(R,'raw-version-'+batch),ver(J,'journal-version-'+batch)
            assert rv>=1+i and jv>=1+i
            budget(10000000000)
            qualified=encode('named_struct('+','.join("'"+col+"',s."+col for col in cols+['old_json'])+')')
            origin_start=time.monotonic()
            sql('origin-proof-'+batch,origin_sql(stage))
            origin=verified_origin(stage,100000,c.records[-1])
            row['origin_proof_wall_s']=time.monotonic()-origin_start
            row['origin_proof']={'feed':origin.feed,'epoch':origin.epoch,'members':origin.members,'query_id':origin.query_id}
            raw_validation=build_raw_validation(stage,R,rv,batch,origin)
            actual=f"SELECT * FROM {J} VERSION AS OF {jv} WHERE apply_batch_id={lit(batch)}"
            journal_validation=f'SELECT count(*) FROM (({journal} EXCEPT ALL {actual}) UNION ALL ({actual} EXCEPT ALL {journal}))'
            new_v=ver(E,'current-version-'+batch)
            assert new_v>old_v
            commits=sql('batch-current-lineage-'+batch,f'DESCRIBE HISTORY {E} LIMIT 20')
            names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
            recent=[dict(zip(names,value)) for value in commits if int(value[0])>old_v]
            assert len([value for value in recent if value['operation']=='MERGE'])==1
            assert all(value['operation'] in ('MERGE','OPTIMIZE') for value in recent)
            assert {int(value['version']) for value in recent}==set(range(old_v+1,new_v+1))
            row['commits']=recent
            def current_checks():
                assert a.sql('global-identities-'+batch,q.identities(new_v))==[['20000000','20000000','100000']]
                assert a.sql('output-parity-'+batch,q.output(new_v))==[['0']]
                return True
            validation_start=time.monotonic()
            with ThreadPoolExecutor(max_workers=3) as validation_pool:
                current_check=validation_pool.submit(current_checks)
                raw_check=validation_pool.submit(sql,'raw-parity-'+batch,raw_validation)
                journal_check=validation_pool.submit(j.sql,'journal-parity-'+batch,journal_validation)
                assert raw_check.result()==[['0']]
                assert journal_check.result()==[['0']]
                assert current_check.result() is True
            row['validation_three_lane_wall_s']=time.monotonic()-validation_start
            report['phases'].append({'label':'journal-parity-'+batch,'caller_ms':j.records[-1]['wall_ms'],'result':[['0']]})
            assert sql('raw-count-'+batch,f"SELECT count(*),count(DISTINCT delivery_id) FROM {R} VERSION AS OF {rv} WHERE apply_batch_id={lit(batch)}")==[['100000','100000']]
            save()
            previous='r139-b1' if i==0 else f'r189-b{i}'
            # Current apply already completed in the independent append/write lane.
            adj=F+'.adjacency_canonical_r120'
            proof_start=time.monotonic()
            histories=budget(3000000000)
            proof=prove(q,new_v,adj,1,1,100000,20000000,c.records+a.records,
                        histories,recent,
                        mode='serialized-synthetic-property105')
            row['composed_proof_wall_s']=time.monotonic()-proof_start
            row['structural_reuse_proof']=proof
            save()
            vector={E:new_v,N:0,R:rv,J:jv,T:0,adj:1}
            validation={'rows':100000,'predecessor':'r139-b1' if i==0 else previous,'wire':'synthetic-full-carrier/2 explicit UTC microseconds; independent instant equality passed',
                        'checks':'All20 affected carrier fields and lexical patch; exact journal; raw bytes/digests/origins and global identity counts',
                        'baseline_origins':'Unqualified; legacy raw timestamp loss retained','writer':'One synthetic publisher with parallel independent raw/journal appends; no real completeness/fencing/acknowledgement','projection_coverage':'Full forward adjacency reused at adjacency_canonical_r120 v1, structural revision1. Inherited full baseline plus owned full-carrier checks and closed apply lineage prove structural reuse; independent full20M oracle runs after publication. Reverse/degree/typed property release unavailable.'}
            progress={'input_stage':stage,'stage_version':0,'members':100000,'admission_window_offsets_s':[first,ready],'clock_epoch':clock_epoch,'source_ack':'None'}
            values=[batch,'ashlar-delta/0.3-synthetic-changed-origin-slice',json.dumps(vector,sort_keys=True,separators=(',',':')),
                    json.dumps(progress,sort_keys=True,separators=(',',':')),'{"synthetic":"synthetic-r1"}',json.dumps(validation,sort_keys=True,separators=(',',':'))]
            sql('publish-'+batch,f'INSERT INTO {M} SELECT '+','.join(map(lit,values))+',current_timestamp()')
            assert sql('descriptor-'+batch,f"SELECT publication_id,profile_version,table_versions_json,source_progress_json,schema_revisions_json,validation_report_json FROM {M} WHERE publication_id={lit(batch)}")==[values]
            finished=time.monotonic()-clock_mono
            row.update(verified_manifest_offset_s=finished,processing_s=finished-begin,
                       complete_input_to_manifest_s=finished-ready,oldest_modeled_record_freshness_s=finished-first,uniform_record_age_p95_s=finished-first-.05*(ready-first),
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

        budget()
        report['state']='completed one isolated overlapped-write-and-validation property publication; native history audit pending, no sustained admission'
        save()
    finally:
        c.close();j.close();a.close();c.history();j.history();a.history()

if __name__=='__main__':main()
