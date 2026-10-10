"""Separate finite commerce evolution candidates; no semantic admission authority."""
import copy
import hashlib
import json
from pathlib import Path
from ashlar.commerce_source import build_transaction, SOURCE_SHA, GRAPH_SHA

PROFILE = 'ashlar-commerce-evolution-preparation/0.1'


def digest(value):
    return hashlib.sha256(value).hexdigest()


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, separators=(',', ':'))+'\n').encode()


def prepare(source_bytes, graph_bytes):
    # The original converter establishes exactly the original development IDs.
    _, original_binding = build_transaction(source_bytes, graph_bytes, source_system='commerce-evolution-preparation')
    source = json.loads(source_bytes); graph = json.loads(graph_bytes)
    registry = copy.deepcopy(original_binding)
    registry['profile'] = 'ashlar-commerce-evolution-development-registry/0.1'
    registry['originalBindingSha256'] = digest(encoded(original_binding))
    registry['originalSourceSha256'] = SOURCE_SHA
    registry['originalGraphSha256'] = GRAPH_SHA
    revisions = [{'id':'R1', 'source':source, 'graph':graph}]
    updated = copy.deepcopy(graph)
    product, = [o for o in updated['objects'] if o['type']['element']=='products']
    product['values']['products.unit_price'] = '12.75'
    revisions.append({'id':'R2-update', 'source':copy.deepcopy(source), 'graph':updated})
    deleted = copy.deepcopy(updated)
    fulfillments = [o for o in deleted['objects'] if o['type']['element']=='fulfillments']
    removed = fulfillments[1]['key']
    deleted['objects'] = [o for o in deleted['objects'] if o['key'] != removed]
    deleted['edges'] = [e for e in deleted['edges'] if e['source'] != removed and e['target'] != removed]
    revisions.append({'id':'R3-delete', 'source':copy.deepcopy(source), 'graph':deleted,
                      'deletedOriginalKey':removed})
    additive = copy.deepcopy(source)
    module, = [m for m in additive['modules'] if m['id']=='domain']
    field = {'id':'products.evolution_note','name':'evolution_note','kind':'field',
             'scalarType':'string','cardinality':'one','nullability':'absent-allowed','extensions':{}}
    module['elements'].append(field)
    record, = [e for e in module['elements'] if e['id']=='products']
    record['members'].append({'module':'domain','element':field['id']})
    identity=[source['id'],'domain',field['id']]
    next_id=str(max(int(p['property_id']) for p in registry['properties'])+1)
    registry['properties'].append({'identity':identity,'property_id':next_id})
    revisions.append({'id':'R4-additive-candidate','source':additive,'graph':copy.deepcopy(deleted),
                      'addedField':identity,'evolutionCompatibility':'not-admitted'})
    breaking=copy.deepcopy(additive)
    for m in breaking['modules']:
        for e in m['elements']:
            if e['id']=='order_lines.quantity':e['scalarType']='string'
    revisions.append({'id':'breaking-candidate','source':breaking,'graph':copy.deepcopy(deleted),
                      'evolutionCompatibility':'not-admitted'})
    return {'profile':PROFILE,'originalSourceSha256':digest(source_bytes),
            'originalGraphSha256':digest(graph_bytes),'registry':registry,'revisions':revisions,
            'qualification':'Separate finite candidates only; no public dataset receipt, evolution compatibility, catalog authority, publication or ACK admission.'}


def write(source, graph, output):
    source_bytes=Path(source).read_bytes(); graph_bytes=Path(graph).read_bytes()
    result=prepare(source_bytes,graph_bytes)
    output=Path(output);output.mkdir(exist_ok=False)
    (output/'original-model.json').write_bytes(source_bytes)
    (output/'original-graph.json').write_bytes(graph_bytes)
    (output/'candidate.json').write_bytes(encoded(result))
    return result


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('source');parser.add_argument('graph');parser.add_argument('output')
    args=parser.parse_args();write(args.source,args.graph,args.output)
