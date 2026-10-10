import copy
import json
import unittest
from unittest.mock import patch
from ashlar.weft_path_decode import PathDecodeConfig, PathDecodeError, decode_related_paths


def descriptor(family='string'):
    def identity(element):
        return dict(documentId='doc', revision='r1', module='m', element=element)
    logical = dict(family=family, nullable=False, facets={'integerWidth': {'bits': 64, 'signed': True}} if family == 'integer' else {})
    def key(record):
        return dict(id='key', fields=[identity(record + '.id')], types=[copy.deepcopy(logical)])
    def hop(a, b, name):
        return dict(identity=dict(documentId='doc', revision='r1', module='m', relationship=name), inverse=False,
                    **{'from': identity(a), 'to': identity(b)}, sourceKey=key(a), targetKey=key(b),
                    sourceMultiplicity={'min': 0, 'max': '*'}, targetMultiplicity={'min': 0, 'max': '*'}, targetLifecycle='independent')
    return dict(kind='relatedPaths', path=dict(startScan='s', hops=[hop('a', 'b', 'ab'), hop('b', 'c', 'bc')],
                span={'start': 0, 'end': 20}, hopSpans=[{'start': 2, 'end': 4}, {'start': 8, 'end': 10}]),
                startRecord=identity('a'), bound=1000, edgeEncoding='signed64-decimal/0.1')


PINS = [dict(documentId='doc', revision='r1', sha256='a' * 64, umfVersion='0.8.0')]


def row(a='b', b='c', x='1', y='2'):
    return dict(intermediate=[a], terminal=[b], edges=[x, y])


class PathDecodeTests(unittest.TestCase):
    def decode(self, cell, rep=None, limit=10000):
        raw = json.dumps(cell).encode() if type(cell) is dict else cell
        return decode_related_paths(rep or descriptor(), raw, model_pins=PINS, config=PathDecodeConfig(limit))

    def test_literal_parallel_bag_and_duplicate_occurrences(self):
        rows = [row(x=x, y=y) for x, y in [('1', '3'), ('1', '4'), ('1', '5'), ('2', '3'), ('2', '4'), ('2', '5')]]
        result = self.decode(dict(items=rows, truncated=False))
        self.assertEqual([r.edges for r in result.items], [('1', '3'), ('1', '4'), ('1', '5'), ('2', '3'), ('2', '4'), ('2', '5')])
        self.assertEqual(len({r.terminal for r in result.items}), 1)
        self.assertEqual(len(self.decode(dict(items=[row(), row()], truncated=False)).items), 2)
        self.assertEqual(json.loads(result.original)['items'], rows)

    def test_signed64_numeric_order_and_canonical_edges(self):
        edges = ['-9223372036854775808', '-2', '0', '2', '10', '9223372036854775807']
        self.assertEqual([i.edges[0] for i in self.decode(dict(items=[row(x=e) for e in edges], truncated=False)).items], edges)
        for edge in ['-9223372036854775809', '9223372036854775808', '-0', '01', '+1', '１', 1, True]:
            with self.subTest(edge=edge), self.assertRaises(PathDecodeError):
                self.decode(dict(items=[row(x=edge)], truncated=False))
        with self.assertRaises(PathDecodeError):
            self.decode(dict(items=[row(x='10'), row(x='2')], truncated=False))

    def test_typed_keys_preserve_lexemes_and_refuse_domains(self):
        rep = descriptor('integer')
        result = self.decode(dict(items=[row(a='02', b='-0'), row(a='10', b='0')], truncated=False), rep)
        self.assertEqual(result.items[0].intermediate, ('02',))
        for atom in ['9223372036854775808', '1e1', True]:
            with self.assertRaises(PathDecodeError):
                self.decode(dict(items=[row(a=atom)], truncated=False), rep)
        rep = descriptor('boolean')
        self.decode(dict(items=[row(a='false', b='true')], truncated=False), rep)
        for atom in ['False', '0', False]:
            with self.assertRaises(PathDecodeError):
                self.decode(dict(items=[row(a=atom)], truncated=False), rep)

    def test_presence_empty_and_bounds(self):
        rep = descriptor()
        rep['outerJoin'] = dict(scan='s', record=copy.deepcopy(rep['startRecord']))
        self.assertEqual(self.decode({'state': 'absent'}, rep).state, 'absent')
        result = self.decode({'state': 'value', 'value': {'items': [], 'truncated': False}}, rep)
        self.assertEqual((result.state, result.items, result.truncated), ('value', (), False))
        for cell in [None, {'state': 'null'}, {'items': [], 'truncated': False}, {'state': 'value', 'value': None}]:
            with self.assertRaises(PathDecodeError):
                self.decode(json.dumps(cell).encode(), rep)
        rep = descriptor(); rep['bound'] = 1
        self.decode(dict(items=[row()], truncated=True), rep)
        for cell in [dict(items=[], truncated=True), dict(items=[row(), row()], truncated=False)]:
            with self.assertRaises(PathDecodeError): self.decode(cell, rep)

    def test_json_utf8_arity_and_resource_controls(self):
        for raw in [b'{"items":[],"items":[],"truncated":false}', b'\xff', b'{"items":[],"truncated":NaN}', '\ud800']:
            with self.assertRaises(PathDecodeError): self.decode(raw)
        with self.assertRaises(PathDecodeError): self.decode(dict(items=[], truncated=False), limit=1)
        for limit in [0, True, 16777217]:
            with self.assertRaises(PathDecodeError): PathDecodeConfig(limit)
        bad = row(); bad['terminal'] = []
        with self.assertRaises(PathDecodeError): self.decode(dict(items=[bad], truncated=False))

    def test_empty_descriptor_validation_pins_and_continuity(self):
        for change in ['pin', 'continuity', 'facet', 'span', 'unknown']:
            rep = descriptor()
            if change == 'pin': rep['startRecord']['revision'] = 'r2'
            if change == 'continuity': rep['path']['hops'][1]['from']['element'] = 'other'
            if change == 'facet': rep['path']['hops'][0]['targetKey']['types'][0]['facets'] = {'unknown': 1}
            if change == 'span': rep['path']['hopSpans'].reverse()
            if change == 'unknown': rep['extra'] = True
            with self.subTest(change=change), self.assertRaises(PathDecodeError):
                self.decode(dict(items=[], truncated=False), rep)
        rep = descriptor()
        rep['path']['hops'][0]['targetKey']['fields'][0]['module'] = 'other-module'
        self.decode(dict(items=[], truncated=False), rep)

    def test_empty_original_metadata_counterexamples(self):
        empty = b'{"items":[],"truncated":false}'
        pins = copy.deepcopy(PINS)
        pins.append(dict(pins[0], documentId='other'))
        for role in ('identity', 'from', 'to'):
            rep = descriptor()
            rep['path']['hops'][0][role]['documentId'] = 'other'
            with self.subTest(role=role), self.assertRaises(PathDecodeError):
                decode_related_paths(rep, empty, model_pins=pins, config=PathDecodeConfig(1000))
        pins = copy.deepcopy(PINS)
        pins[0]['umfVersion'] = '9.9.9'
        with self.assertRaises(PathDecodeError):
            decode_related_paths(descriptor(), empty, model_pins=pins, config=PathDecodeConfig(1000))
        for key_role in ('sourceKey', 'targetKey'):
            rep = descriptor()
            key = rep['path']['hops'][0][key_role]
            key['fields'] *= 2
            key['types'] *= 2
            with self.subTest(key_role=key_role), self.assertRaises(PathDecodeError):
                self.decode(empty, rep)

    def test_numeric_atoms_rejected_before_number_conversion(self):
        # JSON scanner must call our rejecting callbacks, not built-in converters.
        import ashlar.weft_path_decode as decoder
        reject = decoder._number_atom
        with patch.object(decoder, '_number_atom', wraps=reject) as callback:
            for raw in (b'9' * 5000, b'1.25', b'1e9999', b'{"items":[],"truncated":1}', b'NaN'):
                before = callback.call_count
                with self.assertRaises(PathDecodeError):
                    self.decode(raw)
                self.assertEqual(callback.call_count, before + 1)
        # String atoms still take the exact scalar path and retain their lexemes.
        result = self.decode(dict(items=[row(a='0002', b='-0')], truncated=False), descriptor('integer'))
        self.assertEqual(result.items[0].intermediate, ('0002',))
        self.assertEqual(result.items[0].terminal, ('-0',))
