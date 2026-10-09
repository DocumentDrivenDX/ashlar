"""Original finite medical graph -> candidate whole-entity source transaction.

Bindings are development IDs, never Truss accepted IDs. The exact reviewed public receipt owns typed Boolean conversion and dataset
semantics. Hosts must actually recompute it before native acquisition and
establish separate source/writer authority. Original tokens remain retained.
"""
import hashlib
import json
import re
from pathlib import Path
from ashlar.source import records_digest, jsonl_batches

SOURCE_SHA='fc9938f152e129e041096219be4d7bef70aa35e16f56745411af232ee9cab1c8'
PUBLIC_SHA='1400eebbca3b082f19fd7699e74c708dfbd69e5ed21f7d3f5e77bd6f4e46b20e'
UMF_PIN='8e76c74d14203225d1ef159c9132bb9e9b0cdffe'
GRAPH_SHA='b22d788dcf23e2889f82bd407241d7af4bb5f8a8d4c3316768458a97448f18c4'


def encode(value):
    return json.dumps(value,ensure_ascii=False,separators=(',',':'))


def build_transaction(source_bytes,graph_bytes,public_bytes,*,source_system,binding_profile='ashlar-medical-development-bindings/0.2'):
    if binding_profile not in ('ashlar-medical-development-bindings/0.2',):
        raise ValueError('Explicit supported development binding profile required')
    if not isinstance(source_system,str) or not source_system or '\x00' in source_system:
        raise ValueError('Explicit development source namespace required')
    if hashlib.sha256(source_bytes).hexdigest()!=SOURCE_SHA or hashlib.sha256(graph_bytes).hexdigest()!=GRAPH_SHA:
        raise ValueError('Exact original medical bytes required')
    source=json.loads(source_bytes);graph=json.loads(graph_bytes)
    elements={(m['id'],e['id']):e for m in source['modules'] for e in m['elements']}
    if hashlib.sha256(public_bytes).hexdigest()!=PUBLIC_SHA:
        raise PermissionError('Exact reviewed original public medical receipt required')
    public=json.loads(public_bytes);receipt=public['receipt']
    if public['umfRevision']!=UMF_PIN or receipt['source']!=source:
        raise PermissionError('Exact owning original public source required')
    original_records={row['instanceId']:row for row in public['input']['records']}
    boolean_receipts={(row['request']['sourceContext']['objectKey'],row['request']['field']['element']):row for row in public['booleanReceipts']}
    if len(original_records)!=51 or len(boolean_receipts)!=3:
        raise PermissionError('Complete original finite public medical inventory required')
    field_ids={identity:str(i+1) for i,identity in enumerate(sorted(k for k,e in elements.items() if e['kind']=='field'))}
    identities=sorted({('object',o['type']['module'],o['type']['element']) for o in graph['objects']}|
                      {('edge',e['relationship']['module'],e['relationship']['id']) for e in graph['edges']})
    types={identity:str(i+1) for i,identity in enumerate(identities)}
    objects={o['key']:(types[('object',o['type']['module'],o['type']['element'])],str(i+1))
             for i,o in enumerate(graph['objects'])}
    events=[];bindings=[]
    for kind,rows in [('object',graph['objects']),('edge',graph['edges'])]:
        for i,row in enumerate(rows):
            identity=row['type'] if kind=='object' else row['relationship']
            type_id=types[(kind,identity['module'],identity['element' if kind=='object' else 'id'])]
            entity_id=objects[row['key']][1] if kind=='object' else str(i+1)
            fields=[]
            if kind=='object':
                record=elements[(identity['module'],identity['element'])]
                for ref in record['members']:
                    field=elements[(ref['module'],ref['element'])];token=row['values'][ref['element']] # Indexing deliberately refuses absent fields.
                    family=field['scalarType']
                    if token is None:value='null' # Present original null is retained, never inferred from missing.
                    elif not isinstance(token,str):raise ValueError('Lexical source carrier required')
                    elif family=='boolean':
                        proof=boolean_receipts[(row['key'],ref['element'])]
                        if proof['request']['token']!=token or proof['source']!=source or proof['request']['field']!=ref or proof['validation']['valid'] is not True or proof['validation']['complete'] is not True or type(proof['value']['boolean']) is not bool:
                            raise PermissionError('Original public Boolean token/Field receipt required')
                        value=encode(proof['value']['boolean'])
                    elif family=='string':value=encode(token)
                    elif family=='integer' and re.fullmatch(r'-?(?:0|[1-9][0-9]*)',token):value=token
                    elif family=='decimal' and re.fullmatch(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?',token):value=token
                    else:raise ValueError('Source token lacks exact JSON carrier')
                    admitted=[v for v in original_records[row['key']]['values'] if v['field']==ref]
                    expected_literal=None if token is None else {'boolean':proof['value']['boolean']} if family=='boolean' else {'string':token} if family=='string' else {'integerToken':token} if family=='integer' else {'decimalToken':token}
                    if len(admitted)!=1 or admitted[0]['state']!='present' or admitted[0]['value']!=expected_literal:
                        raise PermissionError('Original public member carrier differs')
                    property_id=field_ids[(ref['module'],ref['element'])] if binding_profile.endswith('/0.2') else ref['element']
                    fields.append(encode(property_id)+':'+value)
            event={'kind':'event','delivery_id':kind+':'+str(i+1),'source_profile':'ashlar-whole-entity/0.1',
                   'source_system':source_system,'entity_kind':kind,'type_id':type_id,'id':entity_id,
                   'entity_version':'1','schema_revision':SOURCE_SHA,'operation':'create',
                   'props_json':'{'+','.join(fields)+'}',
                   'retained_json':encode({'profile':binding_profile,
                                          'sourceSha256':SOURCE_SHA,'graphSha256':GRAPH_SHA,'publicReceiptSha256':PUBLIC_SHA,'original':row})}
            if kind=='edge':event['endpoints']=[dict(zip(('type_id','id'),objects[row[end]])) for end in ('source','target')]
            events.append((encode(event)+'\n').encode())
            bindings.append({'kind':kind,'originalKey':row['key'],'type_id':type_id,'id':entity_id})
    batch_id='original-medical-v1'
    begin=(encode({'kind':'begin','batch_id':batch_id})+'\n').encode()
    commit=(encode({'kind':'commit','batch_id':batch_id,'record_count':len(events),'records_sha256':records_digest(events)})+'\n').encode()
    lines=(begin,*events,commit)
    batch,=jsonl_batches(lines,feed=source_system,epoch='original-medical-v1')
    binding={'profile':binding_profile,'types':[{'identity':list(k),'type_id':v} for k,v in types.items()],
                  'entities':bindings,'qualification':'Candidate source transaction only; no accepted catalog IDs, semantic admission, publication or ACK authority.'}
    if binding_profile.endswith('/0.2'):binding['properties']=[{'identity':[source['id'],module,element],'property_id':identity} for (module,element),identity in field_ids.items()]
    return batch,binding


def write_candidate(ontology,graph,public_receipt,output,*,source_system,binding_profile='ashlar-medical-development-bindings/0.2'):
    batch,binding=build_transaction(Path(ontology).read_bytes(),Path(graph).read_bytes(),Path(public_receipt).read_bytes(),source_system=source_system,binding_profile=binding_profile)
    output=Path(output);output.mkdir(parents=False,exist_ok=False)
    (output/'source.jsonl').write_bytes(batch.begin+b''.join(r.raw for r in batch.records)+batch.commit)
    (output/'bindings.json').write_text(encode(binding)+'\n',encoding='utf-8')
    return batch,binding
