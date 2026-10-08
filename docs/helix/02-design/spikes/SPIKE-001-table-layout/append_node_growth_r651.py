"""Bounded owned node extent growth; same-handle recovery, never blind replay."""
import json
import hashlib
import time
from append_native_r637 import BASE, append_sql
from append_profile_r636 import NEW_NODES, NEW_EDGES
from append_node_digest_r647 import grouped_query
from scale_mixed_roles_sql_r224 import role_sql
from persistent_sql import Client
from publication_history import collect_history, HistoryPending

OUT=BASE/'out/native/ashlar_append_node_growth_r651'

def run():
    assert not OUT.exists(), 'Inspect recorded native handles/commits before any retry'
    budget=json.loads((BASE/'out/node-extent-budget-r646.json').read_text())
    prior=json.loads((BASE/'out/native/ashlar_append_node_preflight_r649/summary.json').read_text())
    audit=json.loads((BASE/'out/native/ashlar_append_node_preflight_r649/audited-summary-r650.json').read_text())
    oracle_path=BASE/'out/append-node-oracle-r645.json'
    oracle=json.loads(oracle_path.read_text())
    assert hashlib.sha256(oracle_path.read_bytes()).hexdigest()==audit['oracle_sha256']==prior['oracle_sha256']
    c=Client(OUT,observation_timeout=240,cancel_after=180)
    started=time.monotonic()
    result={'state':'Preflight before owned node growth','bounds':budget['bounds'],'oracle_sha256':audit['oracle_sha256'],'tables':prior['tables'],'versions':{r:0 for r in prior['tables']},'commits':[],'checks':{},'qualification':'Private8M new-node extent with associated raw records/property events; independent all-field local oracle. Existing published8M nodes and all edges untouched. No publication, ACK, graph-wide endpoint closure, complete16M/80M graph, billion-scale, ingest or singleton performance admission. Partial commits survive a stop and must be inspected; never replay automatically.'}
    def save():
        result['wall_s']=time.monotonic()-started
        (OUT/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
    def history_row(role,label):
        rows=c.sql(label,'DESCRIBE HISTORY '+result['tables'][role]['table']+' LIMIT 1')
        names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
        return dict(zip(names,rows[0]))
    def metrics(reserve=0):
        for attempt in range(12):
            try:
                h=collect_history(c.w,c.records,OUT/'shared-history.json');break
            except HistoryPending:
                if attempt==11:raise
                time.sleep(2)
        costs={k:sum(q['metrics'][k] for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
        result['costs']=costs;save()
        assert costs['read_bytes']+reserve<=budget['bounds']['read_bytes']
        assert costs['write_remote_bytes']+reserve<=budget['bounds']['write_remote_bytes']
        assert costs['spill_to_disk_bytes']==0
        assert time.monotonic()-started<budget['bounds']['wall_s']
        return h
    try:
        save()
        c.sql('timeout','SET STATEMENT_TIMEOUT=180')
        c.sql('cache','SET use_cached_result=false')
        for role,t in result['tables'].items():
            d=c.sql('identity-'+role,'DESCRIBE DETAIL '+t['table'])
            names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
            detail=dict(zip(names,d[0]));assert detail['id']==t['id']
            head=history_row(role,'preflight-'+role)
            assert int(head['version'])==0 and head['queryHistoryStatementId']==t['head']['queryHistoryStatementId']
            rows=c.sql('schema-'+role,'SELECT * FROM '+t['table']+' VERSION AS OF 0 LIMIT 0')
            assert not rows and c.records[-1]['response']['manifest']['schema']['columns']==t['schema']
        result['state']='Appending remaining7.5M node extent';save()
        for low in range(8500000,16000000,500000):
            high=low+500000
            for role,t in result['tables'].items():
                metrics(1000000000)
                head=history_row(role,f'before-{role}-{low}')
                assert int(head['version'])==result['versions'][role]
                fields=oracle['chunks'][0]['roles'][role]['fields']
                query=role_sql(role,'node',NEW_NODES,NEW_EDGES,low,high,_carrier_query=append_sql('node',low,high))
                c.sql(f'append-{role}-{low}',f"INSERT INTO {t['table']} ({','.join(fields)}) SELECT {','.join(fields)} FROM ({query})")
                sid=c.records[-1]['statement_id']
                commit=history_row(role,f'commit-{role}-{low}')
                assert commit['queryHistoryStatementId']==sid
                assert int(commit['version'])==result['versions'][role]+1
                assert commit['operation']=='WRITE' and commit['isBlindAppend'].lower()=='true'
                expected=2000000 if role=='property_journal' else 500000
                assert int(json.loads(commit['operationMetrics'])['numOutputRows'])==expected
                result['versions'][role]=int(commit['version'])
                result['commits'].append({'role':role,'start':low,'end':high,'statement_id':sid,'history':commit})
                save();metrics()
            print(json.dumps({'new_node_extent':high-8000000,'costs':result['costs'],'wall_s':result['wall_s']}),flush=True)
        result['state']='Verifying complete independent8M node extent';save()
        for role,t in result['tables'].items():
            metrics(1000000000)
            fields=oracle['chunks'][0]['roles'][role]['fields']
            actual=c.sql('complete-digest-'+role,grouped_query(t['table'],result['versions'][role],role,fields))
            expected=[[str(i),str(ch['roles'][role]['rows']),ch['roles'][role]['digest']] for i,ch in enumerate(oracle['chunks'])]
            assert actual==expected,role
            result['checks'][role]={'version':result['versions'][role],'groups':actual,'rows':sum(int(r[1]) for r in actual),'all_fields':True,'invalid_membership':0};save();metrics()
            head=history_row(role,'closing-'+role)
            assert int(head['version'])==result['versions'][role]
            d=c.sql('closing-detail-'+role,'DESCRIBE DETAIL '+t['table'])
            names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
            detail=dict(zip(names,d[0]));assert detail['id']==t['id']
            t['closing_detail']=detail;t['closing_head']=head
        result['state']='Complete independent8M native new-node extent parity';metrics();save()
        (OUT/'live-statement.json').unlink(missing_ok=True)
        print(json.dumps({'state':result['state'],'costs':result['costs'],'wall_s':result['wall_s']}),flush=True)
    except Exception as exc:
        result['state']='Stopped; inspect saved same handles and durable partial commits'
        result['error']=str(exc);save()
        raise
if __name__=='__main__':run()
