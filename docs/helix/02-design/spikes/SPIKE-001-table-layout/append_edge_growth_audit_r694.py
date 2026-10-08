"""Terminal independent receipt audit for full native16M append edge extent."""
import json
import hashlib
from append_edge_growth_r693 import BASE, OUT
from append_edge_blocks_r681 import coverage_query, block_query
from append_native_r637 import append_sql
from append_profile_r636 import NEW_NODES,NEW_EDGES
from scale_mixed_roles_sql_r224 import role_sql

def run():
    s=json.loads((OUT/'summary.json').read_text())
    assert s['state']=='Complete independent16M native new-edge extent parity'
    assert not (OUT/'live-statement.json').exists()
    oracle_path=BASE/'out/combined-edge-oracle-r686.json';oracle=json.loads(oracle_path.read_text())
    assert hashlib.sha256(oracle_path.read_bytes()).hexdigest()==s['oracle_sha256']
    preflight=json.loads((BASE/'out/native/ashlar_append_next_preflight_r687/audited-summary-r689.json').read_text())
    assert s['charged_preflight_costs']==preflight['costs']
    assert oracle['start']==40000000 and oracle['end']==56000000 and len(oracle['chunks'])==160
    records=[json.loads(x) for x in (OUT/'statements.jsonl').read_text().splitlines()]
    history=json.loads((OUT/'shared-history.json').read_text());qs={q['query_id']:q for q in history['queries']}
    assert len(records)==len(qs)==237 and len({r['statement_id'] for r in records})==237
    assert not history['missing_ids'] and len(s['commits'])==64
    for r in records:
        q=qs[r['statement_id']]
        assert q['query_text']==r['sql'] and q['is_final'] and q['status']=='FINISHED'
        assert r['response']['status']['state']=='SUCCEEDED'
    for role,t in s['tables'].items():
        commits=[x for x in s['commits'] if x['role']==role]
        assert len(commits)==16
        fields=oracle['chunks'][0]['roles'][role]['fields']
        for i,event in enumerate(commits):
            low=48000000+500000*i;high=low+500000
            assert event['start']==low and event['end']==high
            assert int(event['history']['version'])==i+1+t['version'] and event['history']['queryHistoryStatementId']==event['statement_id']
            assert event['history']['operation']=='WRITE' and event['history']['isBlindAppend'].lower()=='true'
            r=next(r for r in records if r['statement_id']==event['statement_id'])
            query=role_sql(role,'edge',NEW_NODES,NEW_EDGES,low,high,_carrier_query=append_sql('edge',low,high))
            assert r['sql']==f"INSERT INTO {t['table']} ({','.join(fields)}) SELECT {','.join(fields)} FROM ({query})"
            expected=2000000 if role=='property_journal' else 500000
            assert int(json.loads(event['history']['operationMetrics'])['numOutputRows'])==expected
        assert s['versions'][role]==16+t['version'] and int(t['closing_head']['version'])==16+t['version']
        assert t['closing_head']['queryHistoryStatementId']==commits[-1]['statement_id']
        assert t['closing_detail']['id']==t['id']
        expected=[[str(i),str(ch['roles'][role]['rows']),ch['roles'][role]['digest']] for i,ch in enumerate(oracle['chunks'])]
        coverage=next(r for r in records if r['label']=='complete-coverage-'+role)
        total=64000000 if role=='property_journal' else 16000000
        assert coverage['sql']==coverage_query(t['table'],s['versions'][role],role,56000000)
        assert coverage['response']['result']['data_array']==[[str(total),'0']]
        actual=[]
        for low in range(40000000,56000000,4000000):
            r=next(r for r in records if r['label']=='complete-block-'+role+'-'+str(low))
            assert r['sql']==block_query(t['table'],s['versions'][role],role,fields,low,low+4000000,56000000)
            first=(low-40000000)//100000
            assert r['response']['result']['data_array']==expected[first:first+40]
            assert qs[r['statement_id']]['metrics']['result_from_cache'] is False
            actual+=r['response']['result']['data_array']
        assert actual==expected==s['checks'][role]['groups']
        assert sum(int(r[1]) for r in actual)==total
    assert s['endpoint_closure']['rows']==[['32000000','0','0']]
    r=next(r for r in records if r['label']=='complete-endpoint-closure')
    assert r['response']['result']['data_array']==s['endpoint_closure']['rows']
    assert qs[r['statement_id']]['metrics']['result_from_cache'] is False
    old='client_dev.ashlar_entropy_20261006_r86.growth_object_current_r236'
    new='client_dev.ashlar_entropy_20261006_r86.append_node_object_current_r643'
    e=s['tables']['edge_current']['table'];v=s['versions']['edge_current']
    nodes=f'SELECT source_system,type_id,id FROM {old} VERSION AS OF 6 UNION ALL SELECT source_system,type_id,id FROM {new} VERSION AS OF 15'
    endpoints=f'SELECT source_system,source_type type_id,source_id id FROM {e} VERSION AS OF {v} UNION ALL SELECT source_system,target_type type_id,target_id id FROM {e} VERSION AS OF {v}'
    expected_sql=f'WITH nodes AS ({nodes}),endpoints AS ({endpoints}),missing AS (SELECT e.* FROM endpoints e LEFT ANTI JOIN nodes n ON e.source_system=n.source_system AND e.type_id=n.type_id AND e.id=n.id) SELECT (SELECT count(*) FROM endpoints),(SELECT count(*) FROM missing),(SELECT count_if(source_system IS NULL OR type_id IS NULL OR id IS NULL) FROM endpoints)'
    assert r['sql']==expected_sql
    assert s['endpoint_closure']['pins']=={'old_nodes':6,'new_nodes':15,'edges':v}
    costs={k:sum(q['metrics'][k] for q in qs.values()) for k in s['phase_costs']}
    assert costs==s['phase_costs']
    combined={k:costs[k]+s['charged_preflight_costs'][k] for k in costs}
    assert combined==s['costs'] and all(combined[k]<=s['bounds'][k] for k in combined)
    assert s['wall_s']<=s['bounds']['wall_s']
    result={'state':'All237 exact final native statements,64 attributed appends and640 independent full-field digest groups audited','costs':combined,'phase_costs':costs,'charged_preflight_costs':s['charged_preflight_costs'],'wall_s':s['wall_s'],'oracle_sha256':s['oracle_sha256'],
            'profiles':{r:{'table':t['table'],'id':t['id'],'version':s['versions'][r],'rows':s['checks'][r]['rows'],'files':int(t['closing_detail']['numFiles']),'bytes':int(t['closing_detail']['sizeInBytes'])} for r,t in s['tables'].items()},
            'sources':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [OUT/'summary.json',OUT/'shared-history.json',OUT/'statements.jsonl']},'qualification':s['qualification']+' Every live row belongs to a verified100k-entity bucket; SHA256 collision assumption. Closing observations are not source authority, concurrency fence or retention proof.'}
    (OUT/'audited-summary-r694.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
if __name__=='__main__':run()
