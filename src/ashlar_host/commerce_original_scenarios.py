"""Four unchanged commerce pack queries and independent original occurrence bags.

Request construction and source correspondence grant no publication, ACK,
compiler or native authority. Historical aliased path intents stay separate.
"""
import hashlib
import json
import re
from fractions import Fraction

from .source_identity import SOURCE_SHA, GRAPH_SHA
from .supply_chain_request import source_snapshot
from .count_star_query import commerce_count_star_request

PACK_SHA = '1a03b93174be64e83f8b9e79b115570fe5950e2277780041553f3ef0a782ef1a'
CASES = (
 ('partial-return', 'SELECT f.quantity,r.quantity,l.quantity-f.quantity+r.quantity FROM order_lines l JOIN fulfillments f ON f.line_id=l.id JOIN returns r ON r.line_id=l.id WHERE f.quantity<l.quantity'),
 ('fulfillment', 'SELECT f.id FROM fulfillments f JOIN order_lines l ON f.line_id=l.id WHERE f.quantity>l.quantity ORDER BY f.id'),
 ('settlement', 'SELECT p.id FROM payments p JOIN invoices i ON p.invoice_id=i.id WHERE p.amount=i.amount AND p.currency=i.currency'),
 ('refund', 'SELECT r.id FROM refunds r JOIN returns x ON x.id=r.return_id JOIN order_lines l ON l.id=x.line_id JOIN products p ON p.id=l.product_id WHERE r.amount=x.quantity*p.unit_price'),
)


def commerce_original_cases(pack_bytes):
    if type(pack_bytes) is not bytes or hashlib.sha256(pack_bytes).hexdigest() != PACK_SHA:
        raise ValueError('Exact original commerce pack required')
    pack = json.loads(pack_bytes)
    if tuple((c['id'], c['sql']) for c in pack['scenario_checks']) != CASES:
        raise ValueError('Complete unchanged original scenario inventory required')
    return CASES


def commerce_original_request(sql, model, bindings, manifest, registry, aliases, *, pack_bytes):
    if type(sql) is not str or sql not in dict(commerce_original_cases(pack_bytes)).values():
        raise ValueError('Exact original commerce scenario SQL required')
    # Borrowed metadata is copied before invoking any request/binding equality.
    bindings, manifest, registry, aliases = (source_snapshot(v) for v in (bindings, manifest, registry, aliases))
    return commerce_count_star_request(sql, model, bindings, manifest, registry, aliases)


def _number(token, *, integer=False):
    if type(token) is not str or len(token) > 128 or re.fullmatch(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?', token) is None:
        raise ValueError('Bounded exact original numeric lexical required')
    if integer and '.' in token:
        raise ValueError('Original integer lexical required')
    sign = -1 if token.startswith('-') else 1
    parts = token.lstrip('-').split('.')
    fraction = parts[1] if len(parts) == 2 else ''
    return Fraction(sign * int(parts[0] + fraction), 10 ** len(fraction))


def _scenario_bag(document, sql):
    """Relational field-equality occurrences, never relationship deduplication."""
    document = source_snapshot(document)
    if type(sql) is not str or sql not in dict(CASES).values():
        raise ValueError('Original scenario required')
    objects = document['objects']
    if type(objects) is not list or len(objects) > 10000:
        raise ValueError('Complete bounded source object bag required')
    keys = [o['key'] for o in objects]
    if any(type(k) is not str for k in keys) or len(set(keys)) != len(keys):
        raise ValueError('Independent unique source occurrence identities required')
    def records(name):
        return [o for o in objects if o['type'] == dict(document='urn:umf:domain:commerce', module='domain', element=name)]
    def field(obj, record, name):
        value = obj['values'][record + '.' + name]
        if type(value) is not str:
            raise ValueError('Original explicit lexical field required')
        return value
    def joins(left, right, left_field, right_field):
        return ((a, b) for a in records(left) for b in records(right)
                if field(a, left, left_field) == field(b, right, right_field))
    rows = []; witnesses = []
    def retain(row, occurrences):
        if len(rows) >= 10000:
            raise ValueError('Complete result occurrence capacity exceeded')
        rows.append(row); witnesses.append([o['key'] for o in occurrences])
    if sql == CASES[0][1]:
        for line, fulfillment in joins('order_lines', 'fulfillments', 'id', 'line_id'):
            for returned in records('returns'):
                if field(returned, 'returns', 'line_id') != field(line, 'order_lines', 'id'):
                    continue
                a, b, c = (_number(field(o, r, 'quantity'), integer=True) for o, r in
                           ((line, 'order_lines'), (fulfillment, 'fulfillments'), (returned, 'returns')))
                if b < a:
                    retain([field(fulfillment, 'fulfillments', 'quantity'), field(returned, 'returns', 'quantity'), str((a-b+c).numerator)], (line, fulfillment, returned))
    elif sql == CASES[1][1]:
        for fulfillment, line in joins('fulfillments', 'order_lines', 'line_id', 'id'):
            if _number(field(fulfillment, 'fulfillments', 'quantity'), integer=True) > _number(field(line, 'order_lines', 'quantity'), integer=True):
                retain([field(fulfillment, 'fulfillments', 'id')], (fulfillment, line))
        ordered = sorted(zip(rows, witnesses), key=lambda pair: pair[0][0].encode('utf8'))
        rows = [r for r, _ in ordered]; witnesses = [w for _, w in ordered]
    elif sql == CASES[2][1]:
        for payment, invoice in joins('payments', 'invoices', 'invoice_id', 'id'):
            if (_number(field(payment, 'payments', 'amount')) == _number(field(invoice, 'invoices', 'amount'))
                    and field(payment, 'payments', 'currency') == field(invoice, 'invoices', 'currency')):
                retain([field(payment, 'payments', 'id')], (payment, invoice))
    else:
        for refund, returned in joins('refunds', 'returns', 'return_id', 'id'):
            for line, product in joins('order_lines', 'products', 'product_id', 'id'):
                if (field(returned, 'returns', 'line_id') == field(line, 'order_lines', 'id')
                        and _number(field(refund, 'refunds', 'amount')) == _number(field(returned, 'returns', 'quantity'), integer=True) * _number(field(product, 'products', 'unit_price'))):
                    retain([field(refund, 'refunds', 'id')], (refund, returned, line, product))
    return {'rows': rows, 'witnesses': {'source_occurrences': witnesses, 'count': len(rows)},
            'scope': 'Original lexical identities and complete relational occurrence bags; no compiler, native or source authority'}


def original_commerce_scenario_oracle(model, graph, request):
    if (type(model) is not bytes or type(graph) is not bytes
            or hashlib.sha256(model).hexdigest() != SOURCE_SHA
            or hashlib.sha256(graph).hexdigest() != GRAPH_SHA):
        raise ValueError('Exact original commerce model and graph required')
    request = source_snapshot(request)
    return _scenario_bag(json.loads(graph), request['sql'])
