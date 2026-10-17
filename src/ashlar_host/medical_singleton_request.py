"""Original historical medical singleton correspondence, never source authority."""
import hashlib,json,uuid
from ashlar.medical_source import build_transaction,SOURCE_SHA
from .delta_custody import encoded
from .implicit_arithmetic_profile import PROFILE
from .medical_publication_plan import FEED
from .publication_reader import LAYOUT_SHA

SQL="SELECT e.resource_key FROM encounters e WHERE e.resource_key = 'Encounter/example'"
ROLES=frozenset(('object_current','edge_current','tombstone','whole_source_history'))


def medical_singleton_request(model,graph,baseline,bindings,manifest,registry,aliases):
    batch,expected=build_transaction(model,graph,baseline,source_system=FEED)
    if bindings!=expected:raise PermissionError('Original medical catalog correspondence differs')
    versions=json.loads(manifest['table_versions_json']);rows={r['table']:r for r in registry}
    if (len(rows)!=6 or len(registry)!=6 or {t.rsplit('.',1)[-1] for t in rows}!=ROLES|{'attempts','manifest'}
            or len(versions)!=4 or {t.rsplit('.',1)[-1] for t in versions}!=ROLES
            or not set(versions)<=set(rows) or set(aliases)!=set(versions)
            or len(set(aliases.values()))!=4 or set(json.loads(manifest['source_progress_json']))!={FEED}
            or json.loads(manifest['schema_revisions_json'])!={FEED:SOURCE_SHA}):
        raise PermissionError('Complete original medical source/publication vector required')
    if any(type(v)is not int or not 0<=v<2**63 for v in versions.values()):raise ValueError('Exact bounded native versions required')
    if any(type(a)is not str or len(a.split('.'))!=3 or any(not p.isidentifier()for p in a.split('.'))for a in aliases.values()):raise ValueError('Exact three-part native aliases required')
    ids=[r['uuid']for r in registry]
    if len(set(ids))!=6 or any(type(v)is not str or str(uuid.UUID(v))!=v for v in ids):raise ValueError('Distinct original native UUIDs required')
    document=json.loads(model);pin={'documentId':document['id'],'revision':SOURCE_SHA,'sha256':SOURCE_SHA,'umfVersion':document['umf']}
    tables=[{'name':aliases[t].split('.'),'uuid':rows[t]['uuid'],'version':v}for t,v in sorted(versions.items())]
    index=sorted(versions).index(next(t for t in versions if t.endswith('.object_current')))
    elements={(m['id'],e['id']):e for m in document['modules']for e in m['elements']}
    properties={tuple(p['identity']):p['property_id']for p in bindings['properties']};records=[]
    for entry in bindings['types']:
        kind,module,element=entry['identity']
        if kind!='object':continue
        homes=[]
        for member in elements[(module,element)]['members']:
            ident=(document['id'],member['module'],member['element'])
            homes.append({'logical':{'documentId':ident[0],'module':ident[1],'element':ident[2],'revision':SOURCE_SHA},'home':{'kind':'props','propertyId':properties[ident]}})
        records.append({'kind':'object','logical':{'documentId':document['id'],'module':module,'element':element,'revision':SOURCE_SHA},'sourceSystem':batch.feed,'typeId':entry['type_id'],'table':index,'schemaRevision':SOURCE_SHA,'properties':homes})
    binding={'profile':'ashlar-databricks-candidate/0.1.0','layoutRevision':'ashlar-delta/0.3','layoutSha256':LAYOUT_SHA,'modelPins':[pin],
        'publication':{'id':manifest['publication_id'],'manifestUuid':rows[next(t for t in rows if t.endswith('.manifest'))]['uuid'],'tables':tables},'records':records}
    raw=encoded(binding)
    target={k:dict(PROFILE.backend)[k]for k in ('backendId','backendVersion','targetProfile')}
    target.update(bindingJson=raw,bindingSha256=hashlib.sha256(raw.encode()).hexdigest())
    return {'interfaceVersion':PROFILE.compile_version,'dialect':PROFILE.dialect,'sql':SQL,
        'modules':[{'documentJson':model.decode(),'pin':pin,'selectedModuleIds':['domain']}],
        'target':target,'options':{'allowCandidate':True}}


def medical_singleton_oracle(model,graph,request):
    if hashlib.sha256(model).hexdigest()!=SOURCE_SHA or request['sql']!=SQL:raise PermissionError('Exact original medical singleton required')
    document=json.loads(model);original=json.loads(graph);rows=[];keys=[]
    for row in original['objects']:
        if row['type']=={'document':document['id'],'module':'domain','element':'encounters'}:
            value=row['values']['encounters.resource_key']
            if type(value)is not str:raise PermissionError('Original required key STRING required')
            if value=='Encounter/example':rows.append([value]);keys.append(row['key'])
    return {'rows':rows,'witnesses':{'original_object_keys':keys,'complete_objects':len(original['objects']),'complete_edges':len(original['edges'])},
        'scope':'Independent complete original medical graph required-STRING singleton; no clinical/action or broader query authority.'}
