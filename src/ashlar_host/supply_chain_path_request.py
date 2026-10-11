"""Exact original supply-chain relationship requests; no runtime authority."""
import hashlib,json,re
from .delta_custody import encoded
from .path_admission import BACKEND,PATHS_KEYS_BACKEND
from .supply_chain_request import supply_chain_count_star_request,source_snapshot


def supply_chain_path_request(sql,pack,model,graph,bindings,manifest,registry,aliases,*,profile='paths'):
    """Select an existing path compiler profile without changing source meaning.

    This pure builder pins the complete original source/carriers first. It neither
    validates compiler output nor grants publication, source or execution authority.
    """
    if type(sql)is not str or not 0<len(sql.encode())<=262144 or '\x00'in sql:
        raise ValueError('Explicit bounded supply-chain SQL required')
    if type(profile)is not str or profile not in ('paths','paths-keys'):
        raise ValueError('Explicit selected path profile required')
    bindings,manifest,registry,aliases=source_snapshot([bindings,manifest,registry,aliases])
    request=supply_chain_count_star_request('sensor',pack,model,graph,bindings,manifest,registry,aliases)
    binding=json.loads(request['target']['bindingJson']);document=json.loads(model);pin=binding['modelPins'][0]
    records={(r['logical']['module'],r['logical']['element']):r for r in binding['records']}
    edge_tables=[i for i,t in enumerate(binding['publication']['tables'])if t['name'][-1]=='edge_current']
    edges=[e for e in bindings['types']if e['identity'][0]=='edge'];types={tuple(e['identity']):e['type_id']for e in edges}
    definitions=[(m['id'],d)for m in document['modules']for d in m.get('relationships',[])]
    if (len(edge_tables)!=1 or len(edges)!=13 or len(types)!=13 or len(definitions)!=13
            or len(set(types.values()))!=13 or len(records)!=12
            or any(type(v)is not str or re.fullmatch('[1-9][0-9]{0,18}',v)is None or int(v)>9223372036854775807 for v in types.values())):
        raise ValueError('Complete original supply-chain relationship carriers required')
    relationships=[]
    for module,definition in definitions:
        identity=('edge',module,definition['id'])
        if identity not in types or definition.get('directed')is not True or len(definition['source'])!=1 or len(definition['target'])!=1:
            raise ValueError('Exact original directed relationship required')
        source,target=definition['source'][0],definition['target'][0]
        first=records[(source['module'],source['element'])];last=records[(target['module'],target['element'])]
        relationships.append({'logical':{'documentId':pin['documentId'],'module':module,'relationship':definition['id'],'revision':pin['revision']},
            'acceptedDefinition':definition,'table':edge_tables[0],'kind':'edge','sourceSystem':first['sourceSystem'],
            'typeId':types[identity],'schemaRevision':pin['revision'],'source':first['logical'],'target':last['logical']})
    binding['relationships']=relationships;raw=encoded(binding);backend=BACKEND if profile=='paths'else PATHS_KEYS_BACKEND
    request.update(sql=sql,interfaceVersion='weft-compile/0.4.0',dialect='weft-sql/0.4.0')
    request['target']={k:backend[k]for k in ('backendId','backendVersion','targetProfile')}
    request['target'].update(bindingJson=raw,bindingSha256=hashlib.sha256(raw.encode()).hexdigest())
    return request
