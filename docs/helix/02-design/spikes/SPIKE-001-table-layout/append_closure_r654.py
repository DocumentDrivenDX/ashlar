"""Read-only complete typed-node union and append-edge endpoint closure."""
import json
import time
from append_native_r637 import BASE
from persistent_sql import Client
from publication_history import collect_history,HistoryPending

OUT=BASE/'out/native/ashlar_append_closure_r654'
F='client_dev.ashlar_entropy_20261006_r86'

def run():
    assert not OUT.exists(),'Inspect existing handles before retry'
    grown=json.loads((BASE/'out/native/ashlar_append_node_growth_r651/audited-summary-r652.json').read_text())
    pilot=json.loads((BASE/'out/native/ashlar_append_write_r643/summary.json').read_text())
    new=grown['profiles']['object_current'];edge=pilot['tables']['edge_edge_current'];adj=pilot['tables']['edge_adjacency_forward']
    tables={'old_nodes':{'table':F+'.growth_object_current_r236','id':'ea6b0cbe-28fc-426c-a52b-15844ef14e91','version':6},'new_nodes':new,'new_edges':dict(edge,version=0),'new_adjacency':dict(adj,version=0)}
    c=Client(OUT,observation_timeout=240,cancel_after=180)
    start=time.monotonic()
    result={'state':'Checking pinned typed union and endpoints','tables':tables,'checks':{},'bounds':{'read_bytes':40000000000,'write_remote_bytes':0,'spill_to_disk_bytes':5000000000,'wall_s':600},'qualification':'Complete typed identity/count and endpoint closure for pinned old8M/new8M node union and2.5M new edges, plus full adjacency multiset parity. Separate physical node extents, not single16M-node table or full80M-edge graph. No publication, canonical writes, ACK, source authority or performance admission.'}
    c.sql('timeout','SET STATEMENT_TIMEOUT=180');c.sql('cache','SET use_cached_result=false')
    def head(key,label):
        rows=c.sql(label,'DESCRIBE HISTORY '+tables[key]['table']+' LIMIT 1')
        names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
        return dict(zip(names,rows[0]))
    for key,t in tables.items():
        d=c.sql('identity-'+key,'DESCRIBE DETAIL '+t['table'])
        names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
        detail=dict(zip(names,d[0]));assert detail['id']==t['id']
        t['observed_head']=head(key,'head-'+key)
        assert int(t['observed_head']['version'])>=t['version']
    nodes=f"SELECT source_system,type_id,id FROM {tables['old_nodes']['table']} VERSION AS OF 6 UNION ALL SELECT source_system,type_id,id FROM {new['table']} VERSION AS OF 15"
    q=f"WITH nodes AS ({nodes}) SELECT count(*),count(DISTINCT named_struct('source_system',source_system,'type_id',type_id,'id',id)),count_if(source_system IS NULL OR type_id IS NULL OR id IS NULL) FROM nodes"
    rows=c.sql('node-union',q);assert rows==[['16000000','16000000','0']]
    result['checks']['node_union']=rows
    endpoints=f"SELECT source_system,source_type type_id,source_id id FROM {edge['table']} VERSION AS OF 0 UNION ALL SELECT source_system,target_type type_id,target_id id FROM {edge['table']} VERSION AS OF 0"
    q=f"WITH nodes AS ({nodes}),endpoints AS ({endpoints}),missing AS (SELECT e.* FROM endpoints e LEFT ANTI JOIN nodes n ON e.source_system=n.source_system AND e.type_id=n.type_id AND e.id=n.id) SELECT (SELECT count(*) FROM endpoints),(SELECT count(*) FROM missing),(SELECT count_if(source_system IS NULL OR type_id IS NULL OR id IS NULL) FROM endpoints)"
    rows=c.sql('endpoint-closure',q);assert rows==[['5000000','0','0']]
    result['checks']['endpoint_closure']=rows
    fields='source_system,rel_type_id,id,source_type,source_id,target_type,target_id,entity_version'
    q=f"WITH expected AS (SELECT {fields} FROM {edge['table']} VERSION AS OF 0),actual AS (SELECT {fields} FROM {adj['table']} VERSION AS OF 0),missing AS (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual),extra AS (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected) SELECT (SELECT count(*) FROM expected),(SELECT count(*) FROM actual),(SELECT count(*) FROM missing),(SELECT count(*) FROM extra)"
    rows=c.sql('adjacency-parity',q);assert rows==[['2500000','2500000','0','0']]
    result['checks']['adjacency_parity']=rows
    for key,t in tables.items():
        closing=head(key,'closing-'+key)
        assert closing['version']==t['observed_head']['version'] and closing['queryHistoryStatementId']==t['observed_head']['queryHistoryStatementId']
        t['closing_head']=closing
    for attempt in range(12):
        try:
            history=collect_history(c.w,c.records,OUT/'shared-history.json');break
        except HistoryPending:
            if attempt==11:raise
            time.sleep(2)
    costs={k:sum(q['metrics'][k] for q in history.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
    assert all(costs[k]<=result['bounds'][k] for k in costs)
    assert time.monotonic()-start<600
    result.update(state='Complete16M typed node union and5M endpoint-reference closure',costs=costs,wall_s=time.monotonic()-start)
    (OUT/'summary.json').write_text(json.dumps(result,indent=2)+'\n');(OUT/'live-statement.json').unlink(missing_ok=True)
    print(json.dumps({'state':result['state'],'costs':costs,'wall_s':result['wall_s']},indent=2))
if __name__=='__main__':run()
