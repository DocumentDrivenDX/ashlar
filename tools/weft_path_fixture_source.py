"""Finite authored source port; development IDs are not Truss/catalog authority."""
import copy
import hashlib
import json
from ashlar.source import jsonl_batches, records_digest
from ashlar.whole_entity import changes_from_batch
from weft_path_fixture_request import INPUT_HASHES, decode_public_receipt


def encode(value):
    return json.dumps(value,ensure_ascii=False,separators=(',',':'))


def build_path_transaction(model_bytes: bytes, graph_bytes: bytes, registry_bytes: bytes):
    """Build exactly one original finite source envelope, with no semantic admission."""
    for name,raw in zip(('model','graph','registry'),(model_bytes,graph_bytes,registry_bytes)):
        if type(raw) is not bytes or len(raw)>4_000_000 or hashlib.sha256(raw).hexdigest()!=INPUT_HASHES[name]:
            raise ValueError('Exact separately authored input required')
    model,graph,registry=map(json.loads,(model_bytes,graph_bytes,registry_bytes))
    fields={e['id']:e for e in model['modules'][0]['elements'] if e['kind']=='field'}
    records={e['id']:e for e in model['modules'][0]['elements'] if e['kind']=='record'}
    objects={o['key']:o for o in graph['objects']};events=[]
    for kind,rows in [('object',graph['objects']),('edge',graph['edges'])]:
        for ordinal,row in enumerate(rows,1):
            if kind=='object':
                rid=row['type']['element'];type_id=registry['types'][rid];entity_id=registry['objects'][row['key']];props=[]
                for ref in records[rid]['members']:
                    token=row['values'][ref['element']];family=fields[ref['element']]['scalarType']
                    value=encode(token) if family=='string' else token
                    props.append(encode(str(registry['properties'][ref['element']]))+':'+value)
            else:type_id=registry['relationships'][row['relationship']['id']];entity_id=int(row['physicalId']);props=[]
            event={'kind':'event','delivery_id':kind+':'+str(ordinal),'source_profile':'ashlar-whole-entity/0.1',
                   'source_system':registry['sourceSystem'],'entity_kind':kind,'type_id':str(type_id),'id':str(entity_id),
                   'entity_version':'1','schema_revision':INPUT_HASHES['model'],'operation':'create','props_json':'{'+','.join(props)+'}',
                   'retained_json':encode({'profile':registry['profile'],'sourceSha256':INPUT_HASHES['model'],'graphSha256':INPUT_HASHES['graph'],'original':row})}
            if kind=='edge':event['endpoints']=[{'type_id':str(registry['types'][objects[row[k]]['type']['element']]),'id':str(registry['objects'][row[k]])} for k in ('source','target')]
            events.append((encode(event)+'\n').encode())
    batch_id='authored-path-fixture-v1'
    begin=(encode({'kind':'begin','batch_id':batch_id})+'\n').encode()
    commit=(encode({'kind':'commit','batch_id':batch_id,'record_count':20,'records_sha256':records_digest(events)})+'\n').encode()
    batch,=jsonl_batches((begin,*events,commit),feed=registry['sourceSystem'],epoch=registry['epoch'])
    return batch


class AuthoredPathAdmission:
    """SchemaAdmission.admit port for this one verified finite original batch.

    Public model/dataset meaning comes from the injected genuine public verifier;
    private writer/source permissions and publication/ACK remain separate.
    """
    def __init__(self, batch, *, model_bytes: bytes, graph_bytes: bytes, registry_bytes: bytes,
                 public_receipt_bytes: bytes, verify_public_receipt):
        if type(public_receipt_bytes) is not bytes or len(public_receipt_bytes)>4_000_000 or not callable(verify_public_receipt):
            raise PermissionError('Bounded actual public verification required')
        rebuilt=build_path_transaction(model_bytes,graph_bytes,registry_bytes)
        if rebuilt!=batch:raise PermissionError('Exact original envelope correspondence required')
        model,graph,registry=map(json.loads,(model_bytes,graph_bytes,registry_bytes));public=decode_public_receipt(public_receipt_bytes);receipt=public.get('receipt',{})
        if (public.get('profile')!='ashlar-authored-path-public-dataset/0.1' or public.get('umfRevision')!='c7c95e1c4ea5b72541f47fa0350ca467ff02f395'
                or public.get('sourceSha256')!=INPUT_HASHES['model'] or public.get('graphSha256')!=INPUT_HASHES['graph'] or receipt.get('source')!=model
                or receipt.get('datasetValidation',{}).get('valid') is not True or receipt.get('datasetValidation',{}).get('complete') is not True):
            raise PermissionError('Complete public dataset correspondence required')
        expected={e['key']:(e['source'],e['target']) for e in graph['edges']}
        actual={e['instanceId']:(e['sourceInstanceId'],e['targetInstanceId']) for e in receipt.get('relationships',[])}
        if actual!=expected or len(receipt['relationships'])!=10 or len(receipt.get('keys',[]))!=10 or {r['instanceId'] for r in receipt.get('records',[])}!={o['key'] for o in graph['objects']} or len(receipt['records'])!=10:
            raise PermissionError('Complete public inventory required')
        if verify_public_receipt(model_bytes,graph_bytes,public_receipt_bytes) is not None:
            raise PermissionError('Public verification must complete without a result')
        self._changes=changes_from_batch(batch)
        self._facts={'profile':'ashlar.authored-path-source-admission/0.1','qualification':'Separately authored finite development source with genuine public UMF supplied-dataset admission; no Truss/catalog IDs, native writer, publication or ACK authority.','sourceSha256':INPUT_HASHES['model'],'graphSha256':INPUT_HASHES['graph'],
                     'registrySha256':INPUT_HASHES['registry'],'receiptSha256':hashlib.sha256(public_receipt_bytes).hexdigest(),
                     'feed':batch.feed,'epoch':batch.epoch,'changes':20,'publicDatasetValidation':copy.deepcopy(receipt['datasetValidation']),
                     'scope':'Exact supplied finite dataset/development carrier correspondence only; private native source/writer/ACK authority separately required.'}
    def metadata(self):
        return copy.deepcopy(self._facts)
    def admit(self, change):
        if change not in self._changes:raise PermissionError('No admitted original finite source change')


def independent_states(model_bytes: bytes, graph_bytes: bytes, registry_bytes: bytes) -> list:
    """Raw source -> all canonical logical states, independently of emitted events."""
    model,graph,registry=map(json.loads,(model_bytes,graph_bytes,registry_bytes))
    fields={e['id']:e for e in model['modules'][0]['elements'] if e['kind']=='field'}
    source_objects={o['key']:o for o in graph['objects']};result=[]
    for role in ('objects','edges'):
        for original in graph[role]:
            kind='object' if role=='objects' else 'edge'
            if kind=='object':
                rid=original['type']['element'];type_id=registry['types'][rid];eid=registry['objects'][original['key']]
                record=next(e for e in model['modules'][0]['elements'] if e['id']==rid)
                values=[]
                for member in record['members']:
                    field=fields[member['element']];text=original['values'][member['element']]
                    # No binary float conversion and no event-derived projection.
                    value=json.dumps(text,ensure_ascii=False) if field['scalarType']=='string' else text
                    values.append(json.dumps(str(registry['properties'][member['element']]))+':'+value)
                props='{'+','.join(values)+'}';endpoints=None
            else:
                type_id=registry['relationships'][original['relationship']['id']];eid=int(original['physicalId']);props='{}'
                endpoints=[{'source':registry['sourceSystem'],'kind':'object','type_id':registry['types'][source_objects[original[side]]['type']['element']],
                            'id':registry['objects'][original[side]]} for side in ('source','target')]
            result.append({'key':{'source':registry['sourceSystem'],'kind':kind,'type_id':type_id,'id':eid},'version':1,
                           'schema_revision':hashlib.sha256(model_bytes).hexdigest(),'props_json':props,
                           'retained_json':json.dumps({'profile':registry['profile'],'sourceSha256':hashlib.sha256(model_bytes).hexdigest(),'graphSha256':hashlib.sha256(graph_bytes).hexdigest(),'original':original},ensure_ascii=False,separators=(',',':')),
                           'endpoints':endpoints})
    return result


def independent_rows(model_bytes: bytes, graph_bytes: bytes, registry_bytes: bytes, *, batch, columns: dict, published_at_micros: str) -> dict:
    """Full current/history/tombstone oracle; raw journal contributes custody only."""
    if type(published_at_micros) is not str or not published_at_micros.isdigit():
        raise ValueError('Explicit publication timestamp required')
    states=independent_states(model_bytes,graph_bytes,registry_bytes)
    registry=json.loads(registry_bytes)
    if batch.feed!=registry['sourceSystem'] or batch.epoch!=registry['epoch'] or len(batch.records)!=20:
        raise ValueError('Original source envelope metadata differs')
    result={role:[] for role in columns}
    raw_records={r.delivery_id:r for r in batch.records}
    cursor=json.dumps({'profile':batch.profile,'offset':batch.cursor_after},separators=(',',':'))
    counters={'object':0,'edge':0}
    import base64
    for state in states:
        kind=state['key']['kind'];counters[kind]+=1;delivery=kind+':'+str(counters[kind]);raw=raw_records[delivery]
        role='object_current' if kind=='object' else 'edge_current';typed='type_id' if kind=='object' else 'rel_type_id'
        key=state['key'];lookup={'source_system':key['source'],typed:key['type_id'],'id':key['id']}
        row={name:None for name,_ in columns[role]};row.update({k:str(v) for k,v in lookup.items()})
        row.update(schema_revision=state['schema_revision'],entity_version='1',props_json=state['props_json'],retained_json=state['retained_json'],
                   source_feed=batch.feed,source_epoch=batch.epoch,source_cursor_json=cursor,source_delivery_id=delivery,published_at=published_at_micros,
                   lookup_hash=hashlib.sha256(json.dumps(lookup,separators=(',',':')).encode()).hexdigest(),apply_batch_id=batch.batch_id)
        if kind=='object':row['logical_key_json']='[]'
        else:
            a,b=state['endpoints'];row.update(source_type=str(a['type_id']),source_id=str(a['id']),target_type=str(b['type_id']),target_id=str(b['id']))
        result[role].append(row)
        change={'feed':batch.feed,'epoch':batch.epoch,'delivery_id':delivery,'raw_digest':raw.sha256,'operation':'create','state':state}
        result['whole_source_history'].append({'feed':batch.feed,'epoch':batch.epoch,'delivery_id':delivery,'digest':raw.sha256,
                                             'change_json':json.dumps(change,separators=(',',':')),'raw_base64':base64.b64encode(raw.raw).decode()})
    return result
