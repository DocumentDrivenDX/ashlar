"""Audit exact pinned union/closure native receipts and query scope."""
import json
import hashlib
from append_closure_r654 import BASE, OUT

def run():
    s=json.loads((OUT/'summary.json').read_text())
    records=[json.loads(x) for x in (OUT/'statements.jsonl').read_text().splitlines()]
    history=json.loads((OUT/'shared-history.json').read_text());qs={q['query_id']:q for q in history['queries']}
    assert len(records)==len(qs)==17 and len({r['statement_id'] for r in records})==17
    assert not history['missing_ids'] and not (OUT/'live-statement.json').exists()
    for r in records:
        q=qs[r['statement_id']]
        assert q['query_text']==r['sql'] and q['is_final'] and q['status']=='FINISHED'
        assert r['response']['status']['state']=='SUCCEEDED'
    t=s['tables'];node_old=t['old_nodes'];node_new=t['new_nodes'];e=t['new_edges'];a=t['new_adjacency']
    assert [node_old['version'],node_new['version'],e['version'],a['version']]==[6,15,0,0]
    for key,table in t.items():
        r=next(r for r in records if r['label']=='identity-'+key)
        names=[c['name'] for c in r['response']['manifest']['schema']['columns']]
        assert dict(zip(names,r['response']['result']['data_array'][0]))['id']==table['id']
        assert table['observed_head']['version']==table['closing_head']['version']
        assert table['observed_head']['queryHistoryStatementId']==table['closing_head']['queryHistoryStatementId']
    nodes=f"SELECT source_system,type_id,id FROM {node_old['table']} VERSION AS OF 6 UNION ALL SELECT source_system,type_id,id FROM {node_new['table']} VERSION AS OF 15"
    endpoints=f"SELECT source_system,source_type type_id,source_id id FROM {e['table']} VERSION AS OF 0 UNION ALL SELECT source_system,target_type type_id,target_id id FROM {e['table']} VERSION AS OF 0"
    sqls={
      'node-union':f"WITH nodes AS ({nodes}) SELECT count(*),count(DISTINCT named_struct('source_system',source_system,'type_id',type_id,'id',id)),count_if(source_system IS NULL OR type_id IS NULL OR id IS NULL) FROM nodes",
      'endpoint-closure':f"WITH nodes AS ({nodes}),endpoints AS ({endpoints}),missing AS (SELECT e.* FROM endpoints e LEFT ANTI JOIN nodes n ON e.source_system=n.source_system AND e.type_id=n.type_id AND e.id=n.id) SELECT (SELECT count(*) FROM endpoints),(SELECT count(*) FROM missing),(SELECT count_if(source_system IS NULL OR type_id IS NULL OR id IS NULL) FROM endpoints)"}
    fields='source_system,rel_type_id,id,source_type,source_id,target_type,target_id,entity_version'
    sqls['adjacency-parity']=f"WITH expected AS (SELECT {fields} FROM {e['table']} VERSION AS OF 0),actual AS (SELECT {fields} FROM {a['table']} VERSION AS OF 0),missing AS (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual),extra AS (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected) SELECT (SELECT count(*) FROM expected),(SELECT count(*) FROM actual),(SELECT count(*) FROM missing),(SELECT count(*) FROM extra)"
    expected={'node-union':[['16000000','16000000','0']],'endpoint-closure':[['5000000','0','0']],'adjacency-parity':[['2500000','2500000','0','0']]}
    for label,sql in sqls.items():
        r=next(r for r in records if r['label']==label)
        assert r['sql']==sql and r['response']['result']['data_array']==expected[label]
        assert qs[r['statement_id']]['metrics']['result_from_cache'] is False
    costs={k:sum(q['metrics'][k] for q in qs.values()) for k in s['costs']}
    assert costs==s['costs'] and all(costs[k]<=s['bounds'][k] for k in costs)
    result={'state':'All17 exact final native statements and complete pinned identity/endpoint/adjacency checks audited','costs':costs,'wall_s':s['wall_s'],'pins':{k:{'table':v['table'],'id':v['id'],'version':v['version']} for k,v in t.items()},
            'sources':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [OUT/'summary.json',OUT/'shared-history.json',OUT/'statements.jsonl']},'qualification':s['qualification']+' Null typed keys explicitly rejected, endpoint multiplicity retained, adjacency compared with bidirectional EXCEPT ALL. Stable observed heads do not establish a writer fence or atomic published vector.'}
    (OUT/'audited-summary-r655.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
if __name__=='__main__':run()
