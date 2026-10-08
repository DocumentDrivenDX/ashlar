"""Private bounded append-profile physical calibration, no publication or cleanup."""
import json
import time
from append_native_r637 import BASE, append_sql
from append_profile_r636 import OLD_NODES, OLD_EDGES, NEW_NODES, NEW_EDGES
from scale_mixed_roles_sql_r224 import role_sql
from persistent_sql import Client
from publication_history import collect_history, HistoryPending

OUT=BASE/'out/native/ashlar_append_write_r639'
F='client_dev.ashlar_entropy_20261006_r86'
BOUNDS={'read_bytes':5000000000,'write_remote_bytes':2000000000,'spill_to_disk_bytes':0,'wall_s':600,'statement_cancel_s':120}

def run():
    assert not OUT.exists(), 'Inspect prior handles instead of resubmitting'
    assert json.loads((BASE/'out/native/ashlar_append_native_r637/audited-summary-r638.json').read_text())['state'].startswith('All30')
    c=Client(OUT,observation_timeout=180,cancel_after=120)
    started=time.monotonic()
    result={'state':'Preparing private append calibration','bounds':BOUNDS,'tables':{},'checks':{},'qualification':'100k new nodes/500k new edges, all roles, full multiset comparison against SQL generator with independent boundary-sample Python parity. Not an independent full local oracle, published-vector growth, full endpoint closure, production ingest or scale admission. No cleanup, ACK, canonical mutation or compute resize.'}
    def save():
        result['wall_s']=time.monotonic()-started
        (OUT/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
    def metrics(reserve=0):
        for attempt in range(12):
            try:
                h=collect_history(c.w,c.records,OUT/'shared-history.json');break
            except HistoryPending:
                if attempt==11:raise
                time.sleep(2)
        costs={k:sum(q['metrics'][k] for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
        result['costs']=costs;save()
        assert costs['read_bytes']+reserve<=BOUNDS['read_bytes']
        assert costs['write_remote_bytes']+reserve<=BOUNDS['write_remote_bytes']
        assert costs['spill_to_disk_bytes']==0
        assert time.monotonic()-started<600
        return h
    try:
        c.sql('timeout','SET STATEMENT_TIMEOUT=120')
        c.sql('cache','SET use_cached_result=false')
        for kind,start,count,roles in [('node',OLD_NODES,100000,['object_current','source_record','property_journal']),('edge',OLD_EDGES,500000,['edge_current','source_record','property_journal','adjacency_forward'])]:
            for role in roles:
                metrics(250000000)
                key=kind+'_'+role
                table=F+'.append_'+key+'_r639'
                query=role_sql(role,kind,NEW_NODES,NEW_EDGES,start,start+count,_carrier_query=append_sql(kind,start,start+count))
                cluster={'source_record':'delivery_id','property_journal':'source_delivery_id','adjacency_forward':'source_id'}.get(role,'lookup_hash')
                statement=f"CREATE TABLE {table} USING DELTA CLUSTER BY ({cluster}) TBLPROPERTIES ('delta.targetFileSize'='67108864') AS {query}"
                c.sql('create-'+key,statement)
                sid=c.records[-1]['statement_id']
                history=c.sql('history-'+key,'DESCRIBE HISTORY '+table+' LIMIT 1')
                names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
                commit=dict(zip(names,history[0]))
                assert int(commit['version'])==0 and commit['queryHistoryStatementId']==sid
                d=c.sql('detail-'+key,'DESCRIBE DETAIL '+table)
                names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
                detail=dict(zip(names,d[0]))
                result['tables'][key]={'table':table,'id':detail['id'],'version':0,'detail':detail,'commit':commit};save()
                metrics(250000000)
                compare=f"WITH expected AS ({query}),actual AS (SELECT * FROM {table} VERSION AS OF 0),missing AS (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual),extra AS (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected) SELECT (SELECT count(*) FROM actual),(SELECT count(*) FROM missing),(SELECT count(*) FROM extra)"
                rows=c.sql('parity-'+key,compare)
                expected=count*4 if role=='property_journal' else count
                assert rows==[[str(expected),'0','0']],(key,rows)
                result['checks'][key]={'rows':expected,'missing':0,'extra':0,'full_field_multiset':True};save();metrics()
        result['state']='Complete private append write calibration with all-role multiset parity'
        metrics();save()
        (OUT/'live-statement.json').unlink(missing_ok=True)
        print(json.dumps({'state':result['state'],'costs':result['costs'],'wall_s':result['wall_s']},indent=2))
    except Exception as exc:
        result['state']='Stopped; inspect recorded same handles before any retry'
        result['error']=str(exc);save()
        raise
if __name__=='__main__':run()
