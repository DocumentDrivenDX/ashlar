"""Audit saved native parity evidence without submitting SQL."""
import json
import hashlib
from append_native_r637 import BASE, OUT, append_sql, transport
from append_profile_r636 import AppendWorkload, OLD_NODES, OLD_EDGES, NEW_NODES, NEW_EDGES
from scale_mixed_roles_sql_r224 import role_sql
from scale_mixed_sql_r222 import literal

def run():
    records=[json.loads(x) for x in (OUT/'statements.jsonl').read_text().splitlines()]
    summary=json.loads((OUT/'summary.json').read_text())
    history=json.loads((OUT/'shared-history.json').read_text())
    queries={q['query_id']:q for q in history['queries']}
    assert len(records)==len(queries)==30 and not history['missing_ids']
    assert len({r['statement_id'] for r in records})==30
    for r in records:
        q=queries[r['statement_id']]
        assert q['is_final'] and q['status']=='FINISHED'
        assert q['query_text']==r['sql']
        assert r['response']['status']['state']=='SUCCEEDED'
        assert not r['response'].get('manifest',{}).get('truncated',False)
        assert q['metrics']['result_from_cache'] is False
    assert records[0]['sql']=='SET STATEMENT_TIMEOUT=60'
    assert records[1]['sql']=='SET use_cached_result=false'
    expected_labels=[]
    checked=0
    w=AppendWorkload()
    for kind,old,total in [('node',OLD_NODES,NEW_NODES),('edge',OLD_EDGES,NEW_EDGES)]:
        for start in [0,old-32,old,total-32]:
            for role in (['object_current','source_record','property_journal'] if kind=='node' else ['edge_current','source_record','property_journal','adjacency_forward']):
                label=f'{kind}-{start}-{role}'
                expected_labels.append(label)
                r=records[2+checked]
                assert r['label']==label
                expected=[row for i in range(start,start+32) for name,row in w.roles(kind,i) if name==role]
                fields=list(expected[0])
                nodes,edges=(OLD_NODES,OLD_EDGES) if start+32<=old else (NEW_NODES,NEW_EDGES)
                inner=role_sql(role,kind,nodes,edges,start,start+32,_carrier_query=append_sql(kind,start,start+32))
                projection=','.join((literal('2026-10-07T00:00:00Z') if f in ['published_at','received_at'] else 'cast('+f+' AS STRING)')+' AS '+f for f in fields)
                assert r['sql']=='SELECT '+projection+' FROM ('+inner+')'
                actual=r['response']['result']['data_array']
                assert sorted(json.dumps(x,ensure_ascii=False) for x in actual)==sorted(json.dumps([transport(row[f]) for f in fields],ensure_ascii=False) for row in expected)
                assert r['response']['manifest']['total_row_count']==len(expected)
                checked+=1
    assert checked==len(summary['checks'])==28
    costs={k:sum(q['metrics'][k] for q in queries.values()) for k in summary['costs']}
    assert costs==summary['costs']
    result={'state':'All30 exact final native statements and28 complete sampled role responses audited', 'costs':costs,'runtime_channels':list({json.dumps(q.get('channel_used'),sort_keys=True) for q in queries.values()}),
            'sources':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [OUT/'statements.jsonl',OUT/'shared-history.json',OUT/'summary.json']},
            'qualification':summary['qualification']+' Regenerated expected Python roles and submitted SQL; historical node/edge prefix remains bootstrap-profile evidence, not verification of current seven-change-batch published content.'}
    (OUT/'audited-summary-r638.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
if __name__=='__main__':run()
