"""Exact original supply-chain five requests for separately selected 041.

Pure request construction, never source/publication authority or native execution.
"""
import hashlib,json,uuid
from ashlar.supply_chain_source import build_transaction,SOURCE_SHA,GRAPH_SHA
from .publication_reader import LAYOUT_SHA
from .delta_custody import encoded
from .count_star_admission import BACKEND
PACK_SHA='baf888ab976749e589df385c76900ebf146959b61eaa4b55ab840e142b54ae22'
SOURCE_SYSTEM='private-original-supply-chain-fixture'
CASE_IDS=('split-excursion','excursion','replay','lineage','sensor')

def source_snapshot(value):
    """Own exact JSON values, rejecting overloaded types/floats before effects."""
    if type(value)in (str,int,bool)or value is None:return value
    if type(value)is list:return [source_snapshot(v)for v in value]
    if type(value)is dict:
        if any(type(k)is not str for k in value):raise ValueError('Exact JSON object keys required')
        return {k:source_snapshot(v)for k,v in value.items()}
    raise ValueError('Exact JSON source metadata required')

def supply_chain_cases(pack_bytes,model_bytes,graph_bytes):
    for raw,digest in ((pack_bytes,PACK_SHA),(model_bytes,SOURCE_SHA),(graph_bytes,GRAPH_SHA)):
        if type(raw)is not bytes or hashlib.sha256(raw).hexdigest()!=digest:
            raise ValueError('Exact original supply-chain inputs required')
    pack=json.loads(pack_bytes);model=json.loads(model_bytes)
    if model['umf']!='0.8.0' or tuple(c['id']for c in pack['scenario_checks'])!=CASE_IDS:
        raise ValueError('Complete original0.8 five-query inventory required')
    return tuple((c['id'],c['sql'])for c in pack['scenario_checks'])

def _original_supply_chain_binding(sql,model,graph,bindings,manifest,registry,aliases):
    batch,expected=build_transaction(model,graph,source_system=SOURCE_SYSTEM)
    if bindings!=expected:raise ValueError('Exact original development catalog bindings required')
    versions=json.loads(manifest['table_versions_json']);rows={row['table']:row for row in registry}
    required={'object_current','edge_current','tombstone','whole_source_history','attempts','manifest'}
    if len(rows)!=6 or len(rows)!=len(registry) or {table.split('.')[-1] for table in rows}!=required or {table.split('.')[-1] for table in versions}!=required-{'attempts','manifest'} or not set(versions)<=set(rows):raise ValueError('Complete original six-role native publication required')
    if any(type(version)is not int or version<0 for version in versions.values()) or len({row['uuid'] for row in registry})!=6 or any(str(uuid.UUID(row['uuid']))!=row['uuid'] for row in registry):raise ValueError('Distinct canonical original UUIDs/nonnegative version pins required')
    if set(json.loads(manifest['source_progress_json']))!={SOURCE_SYSTEM} or set(aliases)!=set(versions) or len(set(aliases.values()))!=len(aliases):raise ValueError('Complete original source/vector/alias custody required')
    document=json.loads(model);pin={'documentId':document['id'],'revision':SOURCE_SHA,'sha256':SOURCE_SHA,'umfVersion':'0.8.0'}
    table_vector=[{'name':aliases[table].split('.'),'uuid':rows[table]['uuid'],'version':version} for table,version in sorted(versions.items())]
    object_table=next(table for table in versions if table.endswith('.object_current'));index=sorted(versions).index(object_table)
    elements={(module['id'],element['id']):element for module in document['modules'] for element in module['elements']}
    properties={tuple(item['identity']):item['property_id'] for item in bindings['properties']}
    records=[]
    for entry in bindings['types']:
        kind,module,element=entry['identity']
        if kind!='object':continue
        record=elements[(module,element)];homes=[]
        for member in record['members']:
            identity=(document['id'],member['module'],member['element'])
            homes.append({'logical':{'documentId':identity[0],'module':identity[1],'element':identity[2],'revision':SOURCE_SHA},'home':{'kind':'props','propertyId':properties[identity]}})
        records.append({'kind':'object','logical':{'documentId':document['id'],'module':module,'element':element,'revision':SOURCE_SHA},'sourceSystem':batch.feed,'typeId':entry['type_id'],'table':index,'schemaRevision':SOURCE_SHA,'properties':homes})
    binding={'profile':'ashlar-databricks-candidate/0.1.0','layoutRevision':'ashlar-delta/0.3','layoutSha256':LAYOUT_SHA,'modelPins':[pin],
        'publication':{'id':manifest['publication_id'],'manifestUuid':rows[next(table for table in rows if table.endswith('.manifest'))]['uuid'],'tables':table_vector},'records':records}
    raw=encoded(binding)
    return {'interfaceVersion':'weft-compile/0.3.0','dialect':'weft-sql/0.3.0','sql':sql,
        'modules':[{'documentJson':model.decode(),'pin':pin,'selectedModuleIds':sorted({record['logical']['module'] for record in records})}],
        'target':{'bindingJson':raw,'bindingSha256':hashlib.sha256(raw.encode()).hexdigest()},'options':{'allowCandidate':True}}


def supply_chain_count_star_request(case_id,pack_bytes,model_bytes,graph_bytes,
                                    bindings,manifest,registry,aliases):
    """Preserve original model/SQL/carriers; only select owning041 markers."""
    bindings,manifest,registry,aliases=source_snapshot([bindings,manifest,registry,aliases])
    if any(type(value)is not dict for value in (bindings,manifest,aliases))or type(registry)is not list:
        raise ValueError('Exact original metadata object/list carriers required')
    cases=dict(supply_chain_cases(pack_bytes,model_bytes,graph_bytes))
    if type(case_id)is not str or case_id not in cases:
        raise ValueError('Original supply-chain case required')
    if type(aliases)is not dict or any(type(v)is not str or len(v.split('.'))!=3 or
            any(not p or '\x00'in p for p in v.split('.'))for v in aliases.values()):
        raise ValueError('Complete three-component publication aliases required')
    request=_original_supply_chain_binding(cases[case_id],model_bytes,graph_bytes,bindings,manifest,registry,aliases)
    binding=json.loads(request['target']['bindingJson']);model=json.loads(model_bytes)
    elements={(m['id'],e['id']):e for m in model['modules']for e in m['elements']}
    props={tuple(p['identity']):p['property_id']for p in bindings['properties']}
    optional={(model['id'],module,element):props[(model['id'],module,element)]
              for (module,element),e in elements.items()if e['kind']=='field'and e['nullability']=='absent-allowed'}
    seen=set()
    for record in binding['records']:
        for prop in record['properties']:
            logical=prop['logical'];identity=(logical['documentId'],logical['module'],logical['element'])
            if identity in optional:
                if identity in seen or prop['home']!={'kind':'props','propertyId':optional[identity]}:
                    raise ValueError('Complete original optional property mapping required')
                prop['home']['encoding']='ashlar-weft-json-native-null/0.1-candidate';seen.add(identity)
    if seen!=set(optional):raise ValueError('Complete original optional property inventory required')
    raw=json.dumps(binding,ensure_ascii=False,separators=(',',':'))
    request.update(interfaceVersion='weft-compile/0.4.1',dialect='weft-sql/0.4.1')
    request['target']={name:BACKEND[name]for name in ('backendId','backendVersion','targetProfile')}
    request['target'].update(bindingJson=raw,bindingSha256=hashlib.sha256(raw.encode()).hexdigest())
    return request
