"""Terminal independent receipt audit for full native append node extent."""
import json
import hashlib
from append_node_growth_r651 import BASE, OUT
from append_node_digest_r647 import grouped_query
from append_native_r637 import append_sql
from append_profile_r636 import NEW_NODES,NEW_EDGES
from scale_mixed_roles_sql_r224 import role_sql

def run():
    s=json.loads((OUT/'summary.json').read_text())
    assert s['state']=='Complete independent8M native new-node extent parity'
    assert not (OUT/'live-statement.json').exists()
    oracle_path=BASE/'out/append-node-oracle-r645.json';oracle=json.loads(oracle_path.read_text())
    assert hashlib.sha256(oracle_path.read_bytes()).hexdigest()==s['oracle_sha256']
    records=[json.loads(x) for x in (OUT/'statements.jsonl').read_text().splitlines()]
    history=json.loads((OUT/'shared-history.json').read_text());qs={q['query_id']:q for q in history['queries']}
    assert len(records)==len(qs)==155 and len({r['statement_id'] for r in records})==155
    assert not history['missing_ids'] and len(s['commits'])==45
    for r in records:
        q=qs[r['statement_id']]
        assert q['query_text']==r['sql'] and q['is_final'] and q['status']=='FINISHED'
        assert r['response']['status']['state']=='SUCCEEDED'
    for role,t in s['tables'].items():
        commits=[x for x in s['commits'] if x['role']==role]
        assert len(commits)==15
        fields=oracle['chunks'][0]['roles'][role]['fields']
        for i,event in enumerate(commits):
            low=8500000+500000*i;high=low+500000
            assert event['start']==low and event['end']==high
            assert int(event['history']['version'])==i+1 and event['history']['queryHistoryStatementId']==event['statement_id']
            assert event['history']['operation']=='WRITE' and event['history']['isBlindAppend'].lower()=='true'
            r=next(r for r in records if r['statement_id']==event['statement_id'])
            query=role_sql(role,'node',NEW_NODES,NEW_EDGES,low,high,_carrier_query=append_sql('node',low,high))
            assert r['sql']==f"INSERT INTO {t['table']} ({','.join(fields)}) SELECT {','.join(fields)} FROM ({query})"
            expected=2000000 if role=='property_journal' else 500000
            assert int(json.loads(event['history']['operationMetrics'])['numOutputRows'])==expected
        assert s['versions'][role]==15 and int(t['closing_head']['version'])==15
        assert t['closing_head']['queryHistoryStatementId']==commits[-1]['statement_id']
        assert t['closing_detail']['id']==t['id']
        expected=[[str(i),str(ch['roles'][role]['rows']),ch['roles'][role]['digest']] for i,ch in enumerate(oracle['chunks'])]
        r=next(r for r in records if r['label']=='complete-digest-'+role)
        assert r['sql']==grouped_query(t['table'],15,role,fields)
        assert r['response']['result']['data_array']==expected==s['checks'][role]['groups']
        assert qs[r['statement_id']]['metrics']['result_from_cache'] is False
    costs={k:sum(q['metrics'][k] for q in qs.values()) for k in s['costs']}
    assert costs==s['costs'] and all(costs[k]<=s['bounds'][k] for k in costs)
    assert s['wall_s']<=s['bounds']['wall_s']
    result={'state':'All155 exact final native statements,45 attributed appends and240 independent full-field digest groups audited','costs':costs,'wall_s':s['wall_s'],'oracle_sha256':s['oracle_sha256'],
            'profiles':{r:{'table':t['table'],'id':t['id'],'version':15,'rows':s['checks'][r]['rows'],'files':int(t['closing_detail']['numFiles']),'bytes':int(t['closing_detail']['sizeInBytes'])} for r,t in s['tables'].items()},
            'sources':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [OUT/'summary.json',OUT/'shared-history.json',OUT/'statements.jsonl']},'qualification':s['qualification']+' Every live row belongs to a verified100k-entity bucket; SHA256 collision assumption. Closing observations are not source authority, concurrency fence or retention proof.'}
    (OUT/'audited-summary-r652.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
if __name__=='__main__':run()
