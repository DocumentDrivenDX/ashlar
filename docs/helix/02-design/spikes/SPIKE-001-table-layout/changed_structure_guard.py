"""Conservative finite synthetic apply lineage gate, not a general SQL verifier."""
import json

def guard(commits,old,new,query_id,members):
    assert {int(c['version']) for c in commits}==set(range(old+1,new+1))
    assert len(commits)==new-old
    assert all(c['operation'] in ('MERGE','OPTIMIZE') for c in commits)
    merges=[c for c in commits if c['operation']=='MERGE'];assert len(merges)==1
    c=merges[0];assert c['queryHistoryStatementId']==query_id
    assert int(c['readVersion'])==old
    m=json.loads(c['operationMetrics'])
    expected={'numSourceRows':members,'numTargetRowsUpdated':members,
              'numTargetRowsInserted':0,'numTargetRowsDeleted':0,
              'numTargetRowsCopied':0,'numTargetRowsNotMatchedBySourceUpdated':0,
              'numTargetRowsNotMatchedBySourceDeleted':0}
    for key,value in expected.items():assert int(m[key])==value
    # Connector-rendered operationParameters is not valid JSON in this evidence.
    # Do not guess its escaping or infer source membership from it.
    # This gate requires separately verified owned SQL and fixed stage custody.
    return True
