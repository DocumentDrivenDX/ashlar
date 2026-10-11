"""Complete original two-hop request preparation, without execution authority.

The twelve typed compilations jointly cover QUERY/supply-chain/two-hop only.
Other six common cases still require their own complete publication-held passes.
"""
import json

from .delta_custody import encoded
from .graphframes_pack_scenarios import common_oracle
from .supply_chain_oracle import original_graph_oracle
from .supply_chain_path_request import supply_chain_path_request
from .supply_chain_request import source_snapshot, supply_chain_cases


def prepare_supply_chain_two_hop(pack, model, graph, bindings, manifest, registry, aliases):
    """Prepare all compatible directed pairs and independent occurrence bags.

    No compiler, journal, SDK, source reader or native effect is opened. Full
    request admission remains the execution owner's responsibility. Native
    edge IDs are mapped only from the exact original transaction bindings.
    """
    supply_chain_cases(pack, model, graph)
    original = original_graph_oracle(model, graph)
    bindings, manifest, registry, aliases = source_snapshot([bindings, manifest, registry, aliases])
    document = json.loads(model)
    definitions = [r for module in document['modules'] for r in module.get('relationships', [])]
    pairs = [(a, b) for a in definitions for b in definitions
             if a['target'][0]['module'] == b['source'][0]['module']
             and a['target'][0]['element'] == b['source'][0]['element']]
    if len(pairs) != 12:
        raise ValueError('Complete original twelve directed pair inventory required')
    objects = {row['key']: row for row in original['objects']}
    edges = original['edges']
    assigned = {(row['kind'], row['originalKey']): row for row in bindings['entities']}
    # 24 exceeds the complete original edge count; nevertheless the real owner
    # must reject a true truncation marker, never treat the bound as proof.
    bound = len(edges) + 1
    prepared = []
    combined = []
    for first, second in pairs:
        record = first['source'][0]['element']
        sql = ('SELECT s.id, RELATED_PATHS(s.' + encoded(first['id']) + ', '
               + encoded(second['id']) + ', ' + str(bound) + ') AS paths FROM '
               + record + ' s ORDER BY s.id')
        request = supply_chain_path_request(sql, pack, model, graph, bindings,
                                            manifest, registry, aliases)
        expected = []
        for source in original['objects']:
            if source['type']['element'] != record:
                continue
            occurrences = []
            for a in edges:
                if a['source'] != source['key'] or a['relationship']['id'] != first['id']:
                    continue
                for b in edges:
                    if b['source'] != a['target'] or b['relationship']['id'] != second['id']:
                        continue
                    middle, target = objects[a['target']], objects[b['target']]
                    occurrences.append({
                        'intermediate': [middle['values'][middle['type']['element'] + '.id']],
                        'terminal': [target['values'][target['type']['element'] + '.id']],
                        'edges': [assigned['edge', a['key']]['id'], assigned['edge', b['key']]['id']]})
                    combined.append([source['key'], a['key'], middle['key'], b['key'], target['key']])
            occurrences.sort(key=lambda p: (tuple(v.encode() for v in p['intermediate']),
                                            tuple(v.encode() for v in p['terminal']),
                                            tuple(int(v) for v in p['edges'])))
            expected.append([source['values'][record + '.id'],
                             encoded({'items': occurrences, 'truncated': False})])
        expected.sort(key=lambda row: row[0].encode())
        prepared.append({'relationships': [first['id'], second['id']],
                         'request': request, 'expected_rows': expected})
    wanted = common_oracle({'objects': original['objects'], 'edges': edges})['two-hop']
    if sorted(encoded(row) for row in combined) != sorted(encoded(row) for row in wanted):
        raise ValueError('Complete shared original two-hop occurrence bag differs')
    return {'case': 'QUERY/supply-chain/two-hop', 'queries': prepared,
            'expected_occurrences': wanted,
            'qualification': 'Pure twelve-request preparation and shared original oracle correspondence only; no compiler, publication, ACK or native pass'}
