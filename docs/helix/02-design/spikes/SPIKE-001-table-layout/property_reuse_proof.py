"""Owned-query proof for serialized synthetic property105 experiments only."""
from changed_structure_guard import guard
from property_apply_queries import PropertyApply,table,integer

def baseline_sql(q,adjacency,adj_version,structural_revision):
    table(adjacency);integer(adj_version);integer(structural_revision,1)
    structural=f'SELECT source_system,rel_type_id,id edge_id,source_type,source_id,target_type,target_id,cast({structural_revision} AS BIGINT) structural_version FROM {q.canonical} VERSION AS OF {q.old_version}'
    return f'SELECT count(*) FROM ((SELECT * FROM {adjacency} VERSION AS OF {adj_version} EXCEPT ALL {structural}) UNION ALL ({structural} EXCEPT ALL SELECT * FROM {adjacency} VERSION AS OF {adj_version}))'

def prove(q,new_version,adjacency,adj_version,structural_revision,members,edge_count,records,histories,commits,*,mode):
    if type(q) is not PropertyApply:raise ValueError('Owned profile object required')
    if mode!='serialized-synthetic-property105':raise ValueError('Source authority and fencing not qualified for other modes')
    integer(members,1);integer(edge_count,members)
    owned={
        'baseline':(baseline_sql(q,adjacency,adj_version,structural_revision),[['0']]),
        'membership':(q.membership(),[[str(members),str(members)]]),
        'intended':(q.intended(),[['0']]),
        'output':(q.output(new_version),[['0']]),
        'identities':(q.identities(new_version),[[str(edge_count),str(edge_count),str(members)]]),
        'apply':(q.apply(),None),
    }
    ids={}
    for role,(sql,result) in owned.items():
        matches=[r for r in records if r['sql']==sql];assert len(matches)==1,role
        r=matches[0];assert r['response']['status']['state']=='SUCCEEDED',role
        h=histories[r['statement_id']];assert h['status']=='FINISHED' and h['query_text'].endswith(sql),role
        if result is not None:assert r['response']['result']['data_array']==result,role
        ids[role]=r['statement_id']
    guard(commits,q.old_version,new_version,ids['apply'],members)
    return {'mode':mode,'canonical':q.canonical,'old_version':q.old_version,'new_version':new_version,'adjacency':adjacency,'adjacency_version':adj_version,'structural_revision':structural_revision,'proof_query_ids':ids,'qualification':'Owned ASCII property105 profile, trusted immutable stage0, inherited full structural baseline, closed owned apply lineage. Serialized synthetic experiment only; no real producer completeness/fencing or general SQL analysis.'}
