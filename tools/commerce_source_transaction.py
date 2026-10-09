"""Original finite commerce graph -> candidate whole-entity source transaction.

Bindings are development IDs, never Truss accepted IDs. Hosts must separately
recompute public UMF dataset admission and establish source/writer authority.
"""
import hashlib
import json
import re
from pathlib import Path
from ashlar.source import records_digest, jsonl_batches

SOURCE_SHA='51d87c554df36846e41378cfaafc81277fcab61f6c891a6c64f34dba8fd9ac2a'
GRAPH_SHA='8052dbcf2cf48a5e344c492d661872bc0a3f80a5c7be033ab5008dea9c86119e'
ROOT=Path(__file__).resolve().parents[1]/'examples/domain-packs/commerce/upstream'


def encode(value):
    return json.dumps(value,ensure_ascii=False,separators=(',',':'))


def build_transaction(source_bytes,graph_bytes,*,source_system,binding_profile='ashlar-commerce-development-bindings/0.2'):
    if binding_profile not in ('ashlar-commerce-development-bindings/0.1','ashlar-commerce-development-bindings/0.2'):
        raise ValueError('Explicit supported development binding profile required')
    if not isinstance(source_system,str) or not source_system or '\x00' in source_system:
        raise ValueError('Explicit development source namespace required')
    if hashlib.sha256(source_bytes).hexdigest()!=SOURCE_SHA or hashlib.sha256(graph_bytes).hexdigest()!=GRAPH_SHA:
        raise ValueError('Exact original commerce bytes required')
    source=json.loads(source_bytes);graph=json.loads(graph_bytes)
    elements={(m['id'],e['id']):e for m in source['modules'] for e in m['elements']}
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
                    field=elements[(ref['module'],ref['element'])];token=row['values'][ref['element']]
                    if not isinstance(token,str):raise ValueError('Lexical source carrier required')
                    family=field['scalarType']
                    if family=='string':value=encode(token)
                    elif family=='integer' and re.fullmatch(r'-?(?:0|[1-9][0-9]*)',token):value=token
                    elif family=='decimal' and re.fullmatch(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?',token):value=token
                    else:raise ValueError('Source token lacks exact JSON carrier')
                    property_id=field_ids[(ref['module'],ref['element'])] if binding_profile.endswith('/0.2') else ref['element']
                    fields.append(encode(property_id)+':'+value)
            event={'kind':'event','delivery_id':kind+':'+str(i+1),'source_profile':'ashlar-whole-entity/0.1',
                   'source_system':source_system,'entity_kind':kind,'type_id':type_id,'id':entity_id,
                   'entity_version':'1','schema_revision':SOURCE_SHA,'operation':'create',
                   'props_json':'{'+','.join(fields)+'}',
                   'retained_json':encode({'profile':binding_profile,
                                          'sourceSha256':SOURCE_SHA,'graphSha256':GRAPH_SHA,'original':row})}
            if kind=='edge':event['endpoints']=[dict(zip(('type_id','id'),objects[row[end]])) for end in ('source','target')]
            events.append((encode(event)+'\n').encode())
            bindings.append({'kind':kind,'originalKey':row['key'],'type_id':type_id,'id':entity_id})
    batch_id='original-commerce-v1'
    begin=(encode({'kind':'begin','batch_id':batch_id})+'\n').encode()
    commit=(encode({'kind':'commit','batch_id':batch_id,'record_count':len(events),'records_sha256':records_digest(events)})+'\n').encode()
    lines=(begin,*events,commit)
    batch,=jsonl_batches(lines,feed=source_system,epoch='original-commerce-v1')
    binding={'profile':binding_profile,'types':[{'identity':list(k),'type_id':v} for k,v in types.items()],
                  'entities':bindings,'qualification':'Candidate source transaction only; no accepted catalog IDs, semantic admission, publication or ACK authority.'}
    if binding_profile.endswith('/0.2'):binding['properties']=[{'identity':[source['id'],module,element],'property_id':identity} for (module,element),identity in field_ids.items()]
    return batch,binding


def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-system',required=True)
    parser.add_argument('--binding-profile',choices=['ashlar-commerce-development-bindings/0.1','ashlar-commerce-development-bindings/0.2'],default='ashlar-commerce-development-bindings/0.2')
    parser.add_argument('--output',type=Path,required=True,help='Fresh candidate fixture directory')
    args=parser.parse_args()
    batch,binding=build_transaction((ROOT/'ontology.json').read_bytes(),(ROOT/'graph/fixture.json').read_bytes(),source_system=args.source_system,binding_profile=args.binding_profile)
    args.output.mkdir(parents=False,exist_ok=False)
    (args.output/'source.jsonl').write_bytes(batch.begin+b''.join(r.raw for r in batch.records)+batch.commit)
    (args.output/'bindings.json').write_text(encode(binding)+'\n',encoding='utf-8')
    print('Created candidate commerce transaction: 11 objects, 10 edges; development bindings only')


if __name__=='__main__':main()
