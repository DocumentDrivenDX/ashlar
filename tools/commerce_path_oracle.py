"""Independent finite original-commerce path witnesses, not a SQL interpreter.

Only named exact query cases are covered. This source oracle establishes expected
values, not publication, source authority, native schema or compiler authenticity.
The original source transaction assigns edge IDs by one-based fixture order.
"""
import hashlib
import json
from ashlar.commerce_source import SOURCE_SHA, GRAPH_SHA


COLLECTION = 'SELECT l.id, RELATED_PATHS(l."order_lines.product_id", "products.supplier_id", 2) AS paths FROM order_lines l ORDER BY l.id LIMIT 1'
COUNT = 'SELECT COUNT(*) AS rows FROM order_lines l CROSS JOIN EXPAND_PATHS(l."order_lines.product_id", "products.supplier_id") AS p'
DISTINCT = 'SELECT COUNT_DISTINCT_PATH_TARGETS(p) AS targets FROM order_lines l CROSS JOIN EXPAND_PATHS(l."order_lines.product_id", "products.supplier_id") AS p'


def original_commerce_path_oracle(model: bytes, graph: bytes, request: dict):
    if (type(model) is not bytes or type(graph) is not bytes
            or hashlib.sha256(model).hexdigest() != SOURCE_SHA
            or hashlib.sha256(graph).hexdigest() != GRAPH_SHA):
        raise ValueError('Exact original commerce oracle inputs required')
    if type(request) is not dict or request.get('sql') not in (COLLECTION, COUNT, DISTINCT):
        raise ValueError('Uncovered original commerce oracle case')
    document = json.loads(graph)
    objects = {obj['key']: obj for obj in document['objects']}
    lines = [obj for obj in document['objects'] if obj['type']['element'] == 'order_lines']
    edges = list(enumerate(document['edges'], 1))
    bags = []
    for line in lines:
        bag = []
        for first_id, first in edges:
            if first['relationship']['id'] != 'order_lines.product_id' or first['source'] != line['key']:
                continue
            product = objects[first['target']]
            for second_id, second in edges:
                if second['relationship']['id'] != 'products.supplier_id' or second['source'] != product['key']:
                    continue
                supplier = objects[second['target']]
                bag.append({'intermediate': [product['values']['products.id']],
                            'terminal': [supplier['values']['suppliers.id']],
                            'edges': [str(first_id), str(second_id)]})
        bag.sort(key=lambda item: (tuple(v.encode('utf-8') for v in item['intermediate']),
                                   tuple(v.encode('utf-8') for v in item['terminal']),
                                   tuple(int(v) for v in item['edges'])))
        bags.append((line['values']['order_lines.id'], bag))
    bags.sort(key=lambda pair: pair[0].encode('utf-8'))
    paths = [item for _, bag in bags for item in bag]
    targets = {tuple(item['terminal']) for item in paths}
    if request['sql'] == COLLECTION:
        rows = [[identity, json.dumps({'items': bag[:2], 'truncated': len(bag) > 2},
                                      ensure_ascii=False, separators=(',', ':'))]
                for identity, bag in bags[:1]]
    else:
        rows = [[str(len(paths) if request['sql'] == COUNT else len(targets))]]
    return {'rows': rows, 'witnesses': {'path_occurrences': len(paths),
                                       'distinct_terminal_keys': len(targets),
                                       'edge_pairs': [item['edges'] for item in paths]},
            'scope': 'Three exact original-commerce query cases; original fixture edge ordinal IDs only'}
