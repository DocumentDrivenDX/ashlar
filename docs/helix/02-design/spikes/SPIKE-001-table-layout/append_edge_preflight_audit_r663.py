"""Audit saved independent node-prefix results and exact native finality."""
import json
import hashlib
from append_edge_preflight_r662 import BASE, OUT
from append_edge_digest_r658 import grouped_query
from mixed_change_queries_r230 import INTS, BOOLS, TIMES

def run():
    s=json.loads((OUT/'summary.json').read_text())
    oracle_path=BASE/'out/append-edge-oracle-r656.json'
    oracle=json.loads(oracle_path.read_text())
    assert hashlib.sha256(oracle_path.read_bytes()).hexdigest()==s['oracle_sha256']
    records=[json.loads(line) for line in (OUT/'statements.jsonl').read_text().splitlines()]
    history=json.loads((OUT/'shared-history.json').read_text())
    queries={q['query_id']:q for q in history['queries']}
    assert len(records)==len(queries)==22 and not history['missing_ids']
    assert len({r['statement_id'] for r in records})==22
    assert not (OUT/'live-statement.json').exists()
    for r in records:
        q=queries[r['statement_id']]
        assert q['query_text']==r['sql'] and q['is_final'] and q['status']=='FINISHED'
        assert r['response']['status']['state']=='SUCCEEDED'
    for role,t in s['tables'].items():
        assert t['head']['version']==t['closing_head']['version'] and t['head']['queryHistoryStatementId']==t['closing_head']['queryHistoryStatementId']
        assert int(t['head']['version'])==t['version']
        fields=oracle['chunks'][0]['roles'][role]['fields']
        expected=[[str(i),str(c['roles'][role]['rows']),c['roles'][role]['digest']] for i,c in enumerate(oracle['chunks'][:25])]
        rec=next(r for r in records if r['label']=='digest-'+role)
        assert rec['sql']==grouped_query(t['table'],t['version'],role,fields)
        assert rec['response']['result']['data_array']==expected==s['checks'][role]['groups']
        assert queries[rec['statement_id']]['metrics']['result_from_cache'] is False
        for col in t['schema']:
            f=col['name']
            typ='LONG' if f in INTS else 'BOOLEAN' if f in BOOLS else 'TIMESTAMP' if f in TIMES else 'STRING'
            assert col['type_name']==typ,(f,col['type_name'],typ)
    costs={k:sum(q['metrics'][k] for q in queries.values()) for k in s['costs']}
    assert costs==s['costs'] and all(costs[k]<=s['bounds'][k] for k in costs)
    result={'state':'All22 exact final native statements and100 independent complete-field digest groups audited','costs':costs,'wall_s':s['wall_s'],'oracle_sha256':s['oracle_sha256'],
            'sources':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [OUT/'summary.json',OUT/'statements.jsonl',OUT/'shared-history.json']},'qualification':s['qualification']+' Schema native transport types checked independently. Observed heads are preflight evidence, not a writer fence or guarantee of future head stability.'}
    (OUT/'audited-summary-r663.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
if __name__=='__main__':run()
