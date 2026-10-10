"""Independent finite original-commerce path witnesses, not a SQL interpreter.

Only ten named exact query cases are covered. This source oracle establishes expected
values, not publication, source authority, native schema or compiler authenticity.
The original source transaction assigns edge IDs by one-based fixture order.
"""
import hashlib
import json
from decimal import Decimal, localcontext
from ashlar.commerce_source import SOURCE_SHA, GRAPH_SHA


COLLECTION = 'SELECT l.id, RELATED_PATHS(l."order_lines.product_id", "products.supplier_id", 2) AS paths FROM order_lines l ORDER BY l.id LIMIT 1'
COUNT = 'SELECT COUNT(*) AS rows FROM order_lines l CROSS JOIN EXPAND_PATHS(l."order_lines.product_id", "products.supplier_id") AS p'
DISTINCT = 'SELECT COUNT_DISTINCT_PATH_TARGETS(p) AS targets FROM order_lines l CROSS JOIN EXPAND_PATHS(l."order_lines.product_id", "products.supplier_id") AS p'


ONE_HOP = 'SELECT p.id, RELATED_KEYS(p."products.supplier_id", 2) AS suppliers FROM products p ORDER BY p.id'
GROUPED = 'SELECT l.id, COUNT(*) AS rows FROM order_lines l CROSS JOIN EXPAND_PATHS(l."order_lines.product_id", "products.supplier_id") AS p GROUP BY l.id ORDER BY l.id'
PROPERTY_JOIN = 'SELECT l.id,p.id AS product FROM order_lines l JOIN products p ON l.product_id=p.id ORDER BY l.id'
FULFILLMENT = 'SELECT f.id FROM fulfillments f JOIN order_lines l ON f.line_id=l.id WHERE f.quantity>l.quantity ORDER BY f.id'
PARTIAL_RETURN = 'SELECT f.quantity AS fulfilled_quantity,r.quantity AS returned_quantity,l.quantity-f.quantity+r.quantity AS remaining_quantity FROM order_lines l JOIN fulfillments f ON f.line_id=l.id JOIN returns r ON r.line_id=l.id WHERE l.quantity>f.quantity'
SETTLEMENT = 'SELECT p.id FROM payments p JOIN invoices i ON p.invoice_id=i.id WHERE p.amount=i.amount AND p.currency=i.currency'
REFUND = 'SELECT r.id FROM refunds r JOIN returns x ON x.id=r.return_id JOIN order_lines l ON l.id=x.line_id JOIN products p ON p.id=l.product_id WHERE r.amount=x.quantity*p.unit_price'


def commerce_path_cases() -> tuple[tuple[str, str], ...]:
    """Return the ten fixed original-source intents; no compiler observation."""
    return (('collection', COLLECTION), ('path-count', COUNT), ('target-count', DISTINCT),
            ('one-hop', ONE_HOP), ('grouped-path-count', GROUPED), ('property-equality-join', PROPERTY_JOIN),
            ('fulfillment', FULFILLMENT), ('partial-return', PARTIAL_RETURN),
            ('settlement', SETTLEMENT), ('refund', REFUND))


def original_commerce_path_oracle(model: bytes, graph: bytes, request: dict) -> dict:
    if (type(model) is not bytes or type(graph) is not bytes
            or hashlib.sha256(model).hexdigest() != SOURCE_SHA
            or hashlib.sha256(graph).hexdigest() != GRAPH_SHA):
        raise ValueError('Exact original commerce oracle inputs required')
    if type(request) is not dict or request.get('sql') not in dict(commerce_path_cases()).values():
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
    elif request['sql'] in (COUNT, DISTINCT):
        rows = [[str(len(paths) if request['sql'] == COUNT else len(targets))]]
    elif request['sql'] == GROUPED:
        rows = [[identity, str(len(bag))] for identity, bag in bags if bag]
    else:
        def values(record):
            return [obj['values'] for obj in document['objects'] if obj['type']['element'] == record]
        def field(row, record, name): return row[record + '.' + name]
        def joined(left, right, left_field, right_field):
            return [(a, b) for a in values(left) for b in values(right)
                    if field(a, left, left_field) == field(b, right, right_field)]
        sql = request['sql']
        if sql == ONE_HOP:
            rows = []
            for product in document['objects']:
                if product['type']['element'] != 'products': continue
                keys = [[objects[edge['target']]['values']['suppliers.id']]
                        for edge in document['edges']
                        if edge['relationship']['id'] == 'products.supplier_id'
                        and edge['source'] == product['key']]
                keys.sort(key=lambda key: tuple(atom.encode('utf8') for atom in key))
                rows.append([product['values']['products.id'], json.dumps(
                    {'items': keys[:2], 'truncated': len(keys) > 2},
                    ensure_ascii=False,separators=(',', ':'))])
        elif sql == PROPERTY_JOIN:
            rows = [[field(l,'order_lines','id'),field(p,'products','id')]
                    for l,p in joined('order_lines','products','product_id','id')]
        elif sql == FULFILLMENT:
            rows = [[field(f,'fulfillments','id')]
                    for f,l in joined('fulfillments','order_lines','line_id','id')
                    if int(field(f,'fulfillments','quantity')) > int(field(l,'order_lines','quantity'))]
        elif sql == PARTIAL_RETURN:
            rows = []
            for l in values('order_lines'):
                for f in values('fulfillments'):
                    for r in values('returns'):
                        if (field(f,'fulfillments','line_id') == field(l,'order_lines','id')
                                == field(r,'returns','line_id')):
                            a,b,c = int(field(l,'order_lines','quantity')), int(field(f,'fulfillments','quantity')), int(field(r,'returns','quantity'))
                            if a > b: rows.append([str(b),str(c),str(a-b+c)])
        elif sql == SETTLEMENT:
            rows = [[field(p,'payments','id')] for p,i in joined('payments','invoices','invoice_id','id')
                    if Decimal(field(p,'payments','amount')) == Decimal(field(i,'invoices','amount'))
                    and field(p,'payments','currency') == field(i,'invoices','currency')]
        elif sql == REFUND:
            rows = []
            # Local precision covers the complete original finite integer/decimal
            # domains; no ambient decimal context or binary floating conversion.
            with localcontext() as context:
                context.prec = 128
                for r,x in joined('refunds','returns','return_id','id'):
                    for l in values('order_lines'):
                        for p in values('products'):
                            if (field(x,'returns','line_id') == field(l,'order_lines','id')
                                    and field(l,'order_lines','product_id') == field(p,'products','id')
                                    and Decimal(field(r,'refunds','amount')) == Decimal(field(x,'returns','quantity')) * Decimal(field(p,'products','unit_price'))):
                                rows.append([field(r,'refunds','id')])
        if sql in (ONE_HOP, PROPERTY_JOIN, FULFILLMENT):
            rows.sort(key=lambda row: tuple(cell.encode('utf8') for cell in row))
    return {'rows': rows, 'witnesses': {'path_occurrences': len(paths),
                                       'distinct_terminal_keys': len(targets),
                                       'edge_pairs': [item['edges'] for item in paths]},
            'scope': 'Ten exact original-commerce intents; independent original values/bags, original edge ordinal IDs; no native/source authority'}
