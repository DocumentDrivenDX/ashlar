"""Independent original finite graph to complete native row bags.

No source transaction projection or generated SQL determines original values.
Binding carriers and delivery digests remain independently checked correspondence.
"""
import base64,datetime,hashlib,json
from .finite_pack import FinitePackDefinition
from .evolution_admission import original_equal
from .supply_chain_request import source_snapshot
def encoded_binding(value):return json.dumps(value,sort_keys=True,separators=(',',':'))
CLOCK='2026-10-09T12:00:00+00:00'

def original_finite_pack_oracle(definition,model_bytes,graph_bytes,bindings,batch,columns):
    """Independent original graph/model → full native row oracle.

    Reads original lexical values and declared members directly. Does not consume
    build_transaction event projections, graph_sql_plan results or native rows.
    Bindings are separately admitted development IDs, not semantic validation.
    """
    if type(definition)is not FinitePackDefinition:raise ValueError('Closed original finite definition required')
    facts=definition.inputs(model_bytes,graph_bytes);SOURCE_SHA=facts['model_sha256'];GRAPH_SHA=facts['graph_sha256']
    profile='ashlar-'+definition.name+'-development-bindings/0.2'
    bindings=source_snapshot(bindings)
    rebuilt,expected=definition.build(model_bytes,graph_bytes)
    if encoded_binding(bindings)!=encoded_binding(expected) or not original_equal(batch,rebuilt):raise ValueError('Exact original finite batch and development carriers required')
    if type(columns)is not dict or set(columns)!={'object_current','edge_current','tombstone','whole_source_history'}:
        raise ValueError('Complete four-role native projection schema required')
    model=json.loads(model_bytes);graph=json.loads(graph_bytes);elements={(m['id'],e['id']):e for m in model['modules'] for e in m['elements']}
    if bindings['profile']==profile:
        properties=bindings['properties'];expected_fields={(model['id'],m,e) for (m,e),field in elements.items() if field['kind']=='field'}
        if len(properties)!=len(expected_fields) or {tuple(p['identity']) for p in properties}!=expected_fields or len({p['property_id'] for p in properties})!=len(properties) or any(str(int(p['property_id']))!=p['property_id'] or not 0<int(p['property_id'])<2**63 for p in properties):raise ValueError('Injective complete canonical development Field binding required')
    else:raise ValueError('Unknown development binding profile')
    assigned={(b['kind'],b['originalKey']):b for b in bindings['entities']}
    if len(assigned)!=facts['objects']+facts['edges'] or {(k,key) for k,key in assigned}!={('object',o['key']) for o in graph['objects']}|{('edge',e['key']) for e in graph['edges']}:raise ValueError('Injective complete original binding inventory required')
    if len({(b['kind'],b['type_id'],b['id']) for b in bindings['entities']})!=facts['objects']+facts['edges']:raise ValueError('Distinct development carrier identities required')
    instant=datetime.datetime.fromisoformat(CLOCK);stamp=str(int((instant-datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc)).total_seconds())*1000000)
    result={role:[] for role in columns};cursor=json.dumps({'profile':batch.profile,'offset':batch.cursor_after},separators=(',',':'))
    original_records={r.delivery_id:r for r in batch.records}
    def text(value):return json.dumps(value,ensure_ascii=False,separators=(',',':'))
    for kind,rows in [('object',graph['objects']),('edge',graph['edges'])]:
        role='object_current' if kind=='object' else 'edge_current';typed='type_id' if kind=='object' else 'rel_type_id'
        for ordinal,original in enumerate(rows,1):
            binding=assigned[(kind,original['key'])];identity={'source_system':batch.feed,typed:int(binding['type_id']),'id':int(binding['id'])}
            props=[]
            if kind=='object':
                record=elements[(original['type']['module'],original['type']['element'])]
                for ref in record['members']:
                    field=elements[(ref['module'],ref['element'])];lexical=original['values'][ref['element']]
                    property_key=ref['element']
                    if bindings['profile']==profile:
                        candidates=[p for p in bindings['properties'] if p['identity']==[model['id'],ref['module'],ref['element']]]
                        if len(candidates)!=1:raise ValueError('Exact original qualified Field binding required')
                        property_key=candidates[0]['property_id']
                    props.append(text(property_key)+':'+('null' if lexical is None else text(lexical) if field['scalarType']=='string' else lexical))
            props_json='{'+','.join(props)+'}'
            retained=text({'profile':bindings['profile'],'sourceSha256':SOURCE_SHA,'graphSha256':GRAPH_SHA,'original':original})
            delivery=kind+':'+str(ordinal);raw=original_records[delivery]
            row={name:None for name,_ in columns[role]};row.update({k:str(v) for k,v in identity.items()})
            row.update(schema_revision=SOURCE_SHA,entity_version='1',props_json=props_json,retained_json=retained,source_feed=batch.feed,source_epoch=batch.epoch,source_cursor_json=cursor,source_delivery_id=delivery,published_at=stamp,lookup_hash=hashlib.sha256(text(identity).encode()).hexdigest(),apply_batch_id=batch.batch_id)
            endpoints=None
            if kind=='object':row['logical_key_json']='[]'
            else:
                a=assigned[('object',original['source'])];b=assigned[('object',original['target'])]
                row.update(source_type=a['type_id'],source_id=a['id'],target_type=b['type_id'],target_id=b['id'])
                endpoints=[{'source':batch.feed,'kind':'object','type_id':int(x['type_id']),'id':int(x['id'])} for x in (a,b)]
            result[role].append(row)
            state={'key':{'source':batch.feed,'kind':kind,'type_id':int(binding['type_id']),'id':int(binding['id'])},'version':1,'schema_revision':SOURCE_SHA,'props_json':props_json,'retained_json':retained,'endpoints':endpoints}
            event={'kind':'event','delivery_id':delivery,'source_profile':'ashlar-whole-entity/0.1','source_system':batch.feed,'entity_kind':kind,'type_id':binding['type_id'],'id':binding['id'],'entity_version':'1','schema_revision':SOURCE_SHA,'operation':'create','props_json':props_json,'retained_json':retained}
            if kind=='edge':event['endpoints']=[{'type_id':x['type_id'],'id':x['id']}for x in (a,b)]
            original_raw=(text(event)+'\n').encode();digest=hashlib.sha256(original_raw).hexdigest()
            if raw.raw!=original_raw or raw.sha256!=digest:raise ValueError('Original source record byte witness differs')
            change={'feed':batch.feed,'epoch':batch.epoch,'delivery_id':delivery,'raw_digest':digest,'operation':'create','state':state}
            result['whole_source_history'].append({'feed':batch.feed,'epoch':batch.epoch,'delivery_id':delivery,'digest':digest,'change_json':json.dumps(change,separators=(',',':')),'raw_base64':base64.b64encode(original_raw).decode()})
    return result
