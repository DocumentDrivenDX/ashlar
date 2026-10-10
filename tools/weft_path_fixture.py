"""Independent finite path bags from separately authored raw source occurrences.

This module grants no publication authority and never consumes compiler output.
"""
import json
from pathlib import Path

FIXTURE = Path(__file__).resolve().parents[1] / 'examples/end-to-end/weft-paths'


def fixture_inputs():
    """Return the exact three authored input byte streams."""
    return tuple((FIXTURE / name).read_bytes() for name in
                 ('model.umf.json', 'graph.json', 'development-registry.json'))


def expected_cases(model_bytes: bytes, graph_bytes: bytes) -> dict:
    """Enumerate two hop occurrences; a self-loop may reuse one physical edge."""
    model, graph = json.loads(model_bytes), json.loads(graph_bytes)
    if model['umf'] != '0.8.0' or model['id'] != 'urn:ashlar:qualification:two-hop-paths':
        raise ValueError('Wrong authored fixture')
    objects = {o['key']: o for o in graph['objects']}
    if len(objects) != len(graph['objects']):
        raise ValueError('Duplicate source identity')
    edges = graph['edges']
    identities = {(e['relationship']['id'], e['physicalId']) for e in edges}
    if len(identities) != len(edges):
        raise ValueError('Duplicate complete physical edge identity')
    for e in edges:
        n = int(e['physicalId'])
        if str(n) != e['physicalId'] or not -(2**63) <= n < 2**63:
            raise ValueError('Noncanonical signed edge ID')
        if e['source'] not in objects or e['target'] not in objects:
            raise ValueError('Unknown endpoint')

    def key(k):
        o = objects[k]
        r = next(e for e in model['modules'][0]['elements'] if e['id'] == o['type']['element'])
        return [o['values'][f['element']] for f in r['keys'][0]['fields']]

    def occurrences(start, first, second, inverse=False):
        found = []
        for a in edges:
            if a['relationship']['id'] != first or a['source'] != start:
                continue
            for b in edges:
                if b['relationship']['id'] != second:
                    continue
                if (b['target'] if inverse else b['source']) != a['target']:
                    continue
                target = b['source'] if inverse else b['target']
                found.append({'intermediate': key(a['target']), 'terminal': key(target),
                              'edges': [a['physicalId'], b['physicalId']]})
        # Bridge's complete key uses Integer, String, Boolean order; retain lexemes.
        found.sort(key=lambda p: (int(p['intermediate'][0]), p['intermediate'][1],
                                 p['intermediate'][2] == 'true', tuple(p['terminal']),
                                 tuple(map(int, p['edges']))))
        return found

    queries = {
        'parallel-six': '''SELECT s.id, RELATED_PATHS(s.bridge, terminal, 10) AS paths FROM start s WHERE s.id = 'L1' ORDER BY s.id''',
        'truncated-two': '''SELECT s.id, RELATED_PATHS(s.bridge, terminal, 2) AS paths FROM start s WHERE s.id = 'L1' ORDER BY s.id''',
        'empty': '''SELECT s.id, RELATED_PATHS(s.bridge, terminal, 10) AS paths FROM start s WHERE s.id = 'L0' ORDER BY s.id''',
        'typed-order': '''SELECT s.id, RELATED_PATHS(s.bridge, terminal, 10) AS paths FROM start s WHERE s.id = 'L_order' ORDER BY s.id''',
        'inverse': '''SELECT s.id, RELATED_PATHS(s.bridge, starts, 10) AS paths FROM start s WHERE s.id = 'L1' ORDER BY s.id''',
        'self-loop': '''SELECT b.rank, RELATED_PATHS(b.loop, loop, 10) AS paths FROM bridge b WHERE b.rank = 1 ORDER BY b.rank''',
        'left-presence': '''SELECT a.id, RELATED_PATHS(s.bridge, terminal, 10) AS paths FROM anchor a LEFT JOIN start s ON a.start_ref = s.id ORDER BY a.id'''}

    six = occurrences('L1', 'start-bridge', 'bridge-terminal')
    ordered = occurrences('L_order', 'start-bridge', 'bridge-terminal')
    inverse = occurrences('L1', 'start-bridge', 'start-bridge', True)
    # Self-loop has the same complete Bridge key and may reuse one physical edge.
    loop = occurrences('P1', 'bridge-loop', 'bridge-loop')
    collection = lambda items, bound: {'items': items[:bound], 'truncated': len(items) > bound}
    result = {'profile': 'ashlar.authored-path-oracle/0.1', 'cases': [
        {'name': 'parallel-six', 'start': 'L1', 'paths': collection(six, 10), 'count': len(six), 'distinctTargets': len({tuple(p['terminal']) for p in six})},
        {'name': 'truncated-two', 'start': 'L1', 'paths': collection(six, 2), 'count': len(six)},
        {'name': 'empty', 'start': 'L0', 'paths': collection([], 10), 'count': 0, 'groupedRows': []},
        {'name': 'typed-order', 'start': 'L_order', 'paths': collection(ordered, 10)},
        {'name': 'inverse', 'start': 'L1', 'paths': collection(inverse, 10), 'count': len(inverse), 'distinctTargets': 1},
        {'name': 'self-loop', 'start': 'P1', 'paths': collection(loop, 10)},
        {'name': 'left-presence', 'rows': [
            ['A_empty', {'state': 'value', 'value': collection([], 10)}],
            ['A_match', {'state': 'value', 'value': collection(six, 10)}],
            ['A_missing', {'state': 'absent'}]]}],
        'qualification': 'Pure source occurrence oracle; no native execution, model authority, publication or ACK claim.'}

    for case in result['cases']:
        case['sql'] = queries[case['name']]
    result['expansionQueries'] = {
        'six': "SELECT COUNT(*) AS rows FROM start s CROSS JOIN EXPAND_PATHS(s.bridge, terminal) AS p WHERE s.id = 'L1'",
        'oneTarget': "SELECT COUNT_DISTINCT_PATH_TARGETS(p) AS targets FROM start s CROSS JOIN EXPAND_PATHS(s.bridge, terminal) AS p WHERE s.id = 'L1'",
        'zero': "SELECT COUNT(*) AS rows FROM start s CROSS JOIN EXPAND_PATHS(s.bridge, terminal) AS p WHERE s.id = 'L0'",
        'noGroups': "SELECT s.id, COUNT(*) AS rows FROM start s CROSS JOIN EXPAND_PATHS(s.bridge, terminal) AS p WHERE s.id = 'L0' GROUP BY s.id"}
    return result
