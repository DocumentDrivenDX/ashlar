"""Independent original payments population; no authored SQL execution claim."""
import hashlib
import json

SOURCE_SHA='f598a9ad230dcb930cd7c8856929e1e6d3f919dac6f736676257e467a8b119da'
GRAPH_SHA='92465e5d9a57c2eccc0b2a30e13ea00d7c5f79c56867eb43770d72a12b48feac'

def original_oracle(model_bytes,graph_bytes):
    if (type(model_bytes)is not bytes or type(graph_bytes)is not bytes
            or hashlib.sha256(model_bytes).hexdigest()!=SOURCE_SHA
            or hashlib.sha256(graph_bytes).hexdigest()!=GRAPH_SHA):
        raise ValueError('Exact original payments bytes required')
    model=json.loads(model_bytes);graph=json.loads(graph_bytes)
    if model['umf']!='0.8.0':raise ValueError('Original declared UMF0.8 required')
    elements={(m['id'],e['id']):e for m in model['modules'] for e in m['elements']}
    relationships={(m['id'],r['id']):r for m in model['modules'] for r in m['relationships']}
    objects={o['key']:o for o in graph['objects']}
    if len(objects)!=len(graph['objects']) or len({e['key']for e in graph['edges']})!=len(graph['edges']):
        raise ValueError('Distinct original occurrence identities required')
    keys=set()
    for row in graph['objects']:
        identity=row['type']
        if identity['document']!=model['id']:raise ValueError('Original document differs')
        record=elements[(identity['module'],identity['element'])]
        if record['kind']!='record':raise ValueError('Declared original Record required')
        declared={ref['element']for ref in record['members']}
        if set(row['values'])!=declared:raise ValueError('Complete declared original members required')
        if any(v is not None and type(v)is not str for v in row['values'].values()):
            raise ValueError('Original lexical/present-null values required')
        for key in record['keys']:
            value=tuple(row['values'][ref['element']]for ref in key['fields'])
            identifier=(identity['module'],identity['element'],key['id'],value)
            if identifier in keys:raise ValueError('Conflicting original authored key')
            keys.add(identifier)
    for edge in graph['edges']:
        source=objects[edge['source']];target=objects[edge['target']]
        identity=edge['relationship']
        if identity['document']!=model['id']:raise ValueError('Original relationship document differs')
        relationship=relationships[(identity['module'],identity['id'])]
        def matches(row,ref):return (row['type']['module'],row['type']['element'])==(ref['module'],ref['element'])
        if not any(matches(source,ref)for ref in relationship['source']):raise ValueError('Original source role differs')
        targets=[ref for ref in relationship['target']if matches(target,ref)]
        if len(targets)!=1:raise ValueError('Exact original target role required')
        record=elements[(target['type']['module'],target['type']['element'])]
        if sum(k['id']==targets[0]['key']for k in record['keys'])!=1:raise ValueError('Original target key missing')
    return {'format':'ashlar-original-payments-graph-oracle/0.1',
        'source_sha256':SOURCE_SHA,'graph_sha256':GRAPH_SHA,
        'objects':graph['objects'],'edges':graph['edges'],
        'source_metadata':graph['source_metadata'],'qualification':graph.get('qualification'),
        'scenarios':{},
        'oracle_scope':'Direct original declared members, authored key tuples, relationship roles and lexical population. Full original graph payload retained. Authored arithmetic/scenario results, UMF semantics, source authority and native execution are not admitted.'}
