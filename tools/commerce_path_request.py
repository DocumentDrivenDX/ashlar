"""Explicit original-commerce bindings for the private reviewed path compiler.

This constructs requests only. It grants no publication/source authority and
does not install or qualify a compiler or native execution profile.
"""
import hashlib
import json
import re

from local_delta_custody import encoded
from run_commerce_arithmetic_weft import compiler_request as scalar_request
from weft_path_plan import BACKEND


def commerce_path_request(sql, model, bindings, manifest, registry, aliases):
    request = scalar_request(sql, model, bindings, manifest, registry, aliases)
    binding = json.loads(request['target']['bindingJson'])
    document = json.loads(model)
    pin = binding['modelPins'][0]
    records = {(r['logical']['module'], r['logical']['element']): r
               for r in binding['records']}
    tables = [i for i, table in enumerate(binding['publication']['tables'])
              if table['name'][-1] == 'edge_current']
    edge_types = [entry for entry in bindings['types'] if entry['identity'][0] == 'edge']
    types = {tuple(entry['identity']): entry['type_id'] for entry in edge_types}
    if (len(tables) != 1 or len(edge_types) != 9 or len(types) != 9
            or len(set(types.values())) != 9
            or any(type(value) is not str or re.fullmatch(r'[1-9][0-9]{0,18}', value) is None
                   or int(value) > 9223372036854775807 for value in types.values())):
        raise ValueError('Complete original commerce edge carriers required')
    relationships = []
    for module in document['modules']:
        for definition in module.get('relationships', []):
            identity = ('edge', module['id'], definition['id'])
            if (identity not in types or definition.get('directed') is not True
                    or len(definition['source']) != 1 or len(definition['target']) != 1):
                raise ValueError('Exact original directed relationship required')
            source, target = definition['source'][0], definition['target'][0]
            source_record = records[(source['module'], source['element'])]
            target_record = records[(target['module'], target['element'])]
            relationships.append({
                'logical': {'documentId': pin['documentId'], 'module': module['id'],
                            'relationship': definition['id'], 'revision': pin['revision']},
                'acceptedDefinition': definition, 'table': tables[0], 'kind': 'edge',
                'sourceSystem': source_record['sourceSystem'], 'typeId': types[identity],
                'schemaRevision': pin['revision'], 'source': source_record['logical'],
                'target': target_record['logical'],
            })
    if len(relationships) != 9:
        raise ValueError('Complete original relationship inventory required')
    binding['relationships'] = relationships
    raw = encoded(binding)
    request.update(interfaceVersion='weft-compile/0.4.0', dialect='weft-sql/0.4.0')
    request['target'] = {key: BACKEND[key] for key in
                         ('backendId', 'backendVersion', 'targetProfile')}
    request['target'].update(bindingJson=raw, bindingSha256=hashlib.sha256(raw.encode()).hexdigest())
    return request
