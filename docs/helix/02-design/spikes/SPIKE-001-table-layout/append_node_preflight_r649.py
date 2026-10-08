"""Read-only complete 500k-node native prefix comparison to full local oracle."""
import json
import hashlib
import time
from append_node_oracle_r645 import BASE
from append_node_digest_r647 import grouped_query
from persistent_sql import Client
from publication_history import collect_history, HistoryPending

OUT=BASE/'out/native/ashlar_append_node_preflight_r649'

def run():
    assert not OUT.exists(), 'Inspect saved handles rather than resubmitting'
    prior=json.loads((BASE/'out/native/ashlar_append_write_r643/summary.json').read_text())
    oracle_path=BASE/'out/append-node-oracle-r645.json'
    oracle=json.loads(oracle_path.read_text())
    receipt=json.loads((BASE/'out/append-node-oracle-audit-r648.json').read_text())
    assert receipt['oracle_sha256']==hashlib.sha256(oracle_path.read_bytes()).hexdigest()
    c=Client(OUT,observation_timeout=180,cancel_after=120)
    start=time.monotonic()
    result={'state':'Checking owned node prefix','oracle_sha256':receipt['oracle_sha256'],'tables':{},'checks':{},'bounds':{'read_bytes':5000000000,'write_remote_bytes':0,'spill_to_disk_bytes':0,'wall_s':300},'qualification':'Independent full500k native prefix parity against local8M oracle, including all fields and invalid-membership bucket. Read-only preflight; no remaining7.5M growth, graph-wide closure, publication or performance admission.'}
    c.sql('timeout','SET STATEMENT_TIMEOUT=120')
    c.sql('cache','SET use_cached_result=false')
    for role in ['object_current','source_record','property_journal']:
        old=prior['tables']['node_'+role]
        table=old['table']
        d=c.sql('detail-'+role,'DESCRIBE DETAIL '+table)
        names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
        detail=dict(zip(names,d[0]));assert detail['id']==old['id']
        h=c.sql('head-'+role,'DESCRIBE HISTORY '+table+' LIMIT 1')
        names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
        head=dict(zip(names,h[0]));assert int(head['version'])==0
        fields=oracle['chunks'][0]['roles'][role]['fields']
        schema=c.sql('schema-'+role,'SELECT * FROM '+table+' VERSION AS OF 0 LIMIT 0')
        assert not schema
        columns=c.records[-1]['response']['manifest']['schema']['columns']
        assert [x['name'] for x in columns]==fields
        result['tables'][role]={'table':table,'id':detail['id'],'head':head,'detail':detail,'schema':columns}
    for role,t in result['tables'].items():
        fields=oracle['chunks'][0]['roles'][role]['fields']
        expected=[[str(i),str(chunk['roles'][role]['rows']),chunk['roles'][role]['digest']] for i,chunk in enumerate(oracle['chunks'][:5])]
        actual=c.sql('digest-'+role,grouped_query(t['table'],0,role,fields))
        assert actual==expected,(role,actual)
        result['checks'][role]={'groups':actual,'all_fields':True,'no_invalid_bucket':True}
        assert time.monotonic()-start<300
    for attempt in range(12):
        try:
            history=collect_history(c.w,c.records,OUT/'shared-history.json');break
        except HistoryPending:
            if attempt==11:raise
            time.sleep(2)
    costs={k:sum(q['metrics'][k] for q in history.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
    assert all(costs[k]<=result['bounds'][k] for k in costs)
    result.update(state='Complete independent full500k native node-prefix parity',costs=costs,wall_s=time.monotonic()-start)
    (OUT/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
    (OUT/'live-statement.json').unlink(missing_ok=True)
    print(json.dumps({'state':result['state'],'costs':costs,'wall_s':result['wall_s']},indent=2))
if __name__=='__main__':run()
