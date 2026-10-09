"""Original archaeology/ecology query custody and independent graph expectations.

This preparation performs no query execution or publication admission. It retains
original authored SQL and original CSV expectations separately from graph values;
seeded graph identities are never decoded into CSV native IDs.
"""
import hashlib
import importlib
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
PACKS=('archaeology','ecology')
PACK_SHA={'archaeology':'401c0743e6aa8eddf541c49bb719c35e7fa67fb31113f494e94d09766d9b124f','ecology':'353f4fedbcd2f3d1a8ee13ad22b1bf496b430dcd766c5d0b1c6fb94d3e0e7481'}


def prepare(pack):
    if pack not in PACKS:
        raise ValueError('Explicit supported original pack required')
    oracle_module=importlib.import_module(pack+'_graph_oracle')
    converter=importlib.import_module(pack+'_source_transaction')
    original=oracle_module.ROOT
    paths={name:original/name for name in ('pack.json','ontology.json','graph/fixture.json')}
    raw={name:path.read_bytes() for name,path in paths.items()}
    if hashlib.sha256(raw['pack.json']).hexdigest()!=PACK_SHA[pack]:
        raise ValueError('Exact original authored scenario bytes required')
    source=json.loads(raw['ontology.json']);graph=json.loads(raw['graph/fixture.json'])
    pack_document=json.loads(raw['pack.json'])
    oracle=oracle_module.original_oracle(raw['ontology.json'],raw['graph/fixture.json'])
    _,bindings=converter.build_transaction(raw['ontology.json'],raw['graph/fixture.json'],source_system='private-original-'+pack+'-fixture')
    fields={tuple(item['identity']):item['property_id'] for item in bindings['properties']}
    types={tuple(item['identity']):item['type_id'] for item in bindings['types']}
    elements={(module['id'],element['id']):element for module in source['modules'] for element in module['elements']}
    tables=[]
    for identity,type_id in types.items():
        kind,module,element=identity
        if kind!='object':continue
        record=elements[(module,element)]
        tables.append({'identity':[source['id'],module,element],'development_type_id':type_id,
            'fields':[{'identity':[source['id'],ref['module'],ref['element']],
                'development_property_id':fields[(source['id'],ref['module'],ref['element'])],
                'original_field':elements[(ref['module'],ref['element'])]}
                for ref in record['members']]})
    scenarios=pack_document['scenario_checks']
    if len(scenarios)!=len(oracle['scenarios']) or {case['id'] for case in scenarios}!=set(oracle['scenarios']):
        raise ValueError('Complete original scenario/oracle inventory required')
    return {'format':'ashlar-original-pack-query-candidate/0.1','pack':pack,
        'original_inputs':[{'path':str(paths[name]),'sha256':hashlib.sha256(value).hexdigest(),'bytes':len(value)} for name,value in raw.items()],
        'source_metadata':graph['source_metadata'],'development_binding_profile':bindings['profile'],
        'tables':tables,'cases':[{'original_scenario':case,'original_graph_expected':oracle['scenarios'][case['id']]} for case in scenarios],
        'qualification':__doc__}


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('pack',choices=PACKS)
    args=parser.parse_args()
    print(json.dumps(prepare(args.pack),ensure_ascii=False,indent=2))
