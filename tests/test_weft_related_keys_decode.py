"""Independent required-root String carrier controls; no native bag claim."""
import copy
from dataclasses import FrozenInstanceError
import json
import unittest

from ashlar.weft_related_keys_decode import (
    RelatedKeysDecodeConfig, RelatedKeysDecodeError, decode_related_keys,
)


PINS = [dict(documentId='original-doc', revision='r1', sha256='a' * 64, umfVersion='0.8.0')]


def descriptor(arity=1, bound=3):
    return dict(kind='relatedKeys',
                relationship=dict(documentId='original-doc', revision='r1', module='m', relationship='owns'),
                key=dict(id='primary',
                         fields=[dict(documentId='original-doc', revision='r1', module='m',
                                      element='target.key' + str(i)) for i in range(arity)],
                         types=[dict(family='string', facets={}, nullable=False) for _ in range(arity)]),
                bound=bound)


class RelatedKeysDecodeTests(unittest.TestCase):
    def decode(self, value, representation=None, pins=None, maximum=4096):
        raw = json.dumps(value).encode() if type(value) is dict else value
        return decode_related_keys(representation or descriptor(), raw,
                                   model_pins=PINS if pins is None else pins,
                                   config=RelatedKeysDecodeConfig(maximum))

    def test_exact_original_bytes_parallel_occurrences_and_immutable_result(self):
        original = b' {"items": [["a"], ["a"], ["b"]], "truncated": true}\n'
        result = self.decode(original)
        self.assertEqual(result.original, original)
        self.assertEqual(result.items, (('a',), ('a',), ('b',)))
        self.assertIs(result.truncated, True)
        with self.assertRaises(FrozenInstanceError):
            result.truncated = False
        with self.assertRaises(TypeError):
            result.items[0][0] = 'replacement'

    def test_composite_string_order_unicode_and_empty_string_preserve_lexemes(self):
        rep = descriptor(2, 5)
        value = {'items': [['', ''], ['a', '10'], ['a', '2'], ['é', '雪']], 'truncated': False}
        raw = json.dumps(value, ensure_ascii=False)
        result = self.decode(raw, rep)
        self.assertEqual(result.original, raw.encode('utf8'))
        self.assertEqual(result.items, tuple(tuple(item) for item in value['items']))
        with self.assertRaises(RelatedKeysDecodeError):
            self.decode({'items': [['a', '2'], ['a', '10']], 'truncated': False}, rep)

    def test_empty_bound_and_marker_constraints(self):
        for items, truncated in [([], False), ([['a']], False),
                                 ([['a'], ['b']], False), ([['a'], ['b']], True)]:
            self.decode(dict(items=items, truncated=truncated), descriptor(bound=2))
        for value in [dict(items=[], truncated=True), dict(items=[['a']], truncated=True),
                      dict(items=[['a'], ['b'], ['c']], truncated=False),
                      dict(items=[['a']], truncated=1)]:
            with self.subTest(value=value), self.assertRaises(RelatedKeysDecodeError):
                self.decode(value, descriptor(bound=2))

    def test_closed_carriers_duplicate_members_and_no_presence_fallback(self):
        cells = [b'{"items":[],"items":[],"truncated":false}',
                 b'{"items":[],"truncated":false,"truncated":true}',
                 b'{"items":[],"truncated":false,"extra":"secret-token"}',
                 b'{"state":"absent"}', b'{"state":"value","value":{"items":[],"truncated":false}}',
                 b'null', b'[]', b'{"items":[]}', b'{"items":[],"truncated":null}',
                 b'{"items":[["a"]],"truncated":false} trailing']
        for cell in cells:
            with self.subTest(cell=cell), self.assertRaises(RelatedKeysDecodeError) as raised:
                self.decode(cell)
            self.assertNotIn('secret-token', str(raised.exception))

    def test_non_string_atoms_unicode_nul_arity_and_order_refuse(self):
        for atom in [None, True, 1, 1.5, [], {}, '\0', '\ud800', '\udfff']:
            with self.subTest(atom=repr(atom)), self.assertRaises(RelatedKeysDecodeError):
                self.decode({'items': [[atom]], 'truncated': False})
        for cell in [b'{"items":[[NaN]],"truncated":false}', b'{"items":[[Infinity]],"truncated":false}',
                     b'{"items":[["\xff"]],"truncated":false}',
                     b'{"items":[[],["a"]],"truncated":false}',
                     b'{"items":[["a","b"]],"truncated":false}',
                     b'{"items":[["b"],["a"]],"truncated":false}']:
            with self.subTest(cell=cell), self.assertRaises(RelatedKeysDecodeError):
                self.decode(cell)

    def test_descriptor_and_original_pin_fences(self):
        bad = []
        for field, value in [('kind', 'relatedPaths'), ('bound', True), ('bound', 0),
                             ('bound', 1001), ('outerJoin', {'scan': 'left'})]:
            rep = descriptor(); rep[field] = value; bad.append(rep)
        rep = descriptor(); rep['relationship']['revision'] = 'replacement'; bad.append(rep)
        rep = descriptor(); rep['key']['fields'][0]['documentId'] = 'other-doc'; bad.append(rep)
        rep = descriptor(2); rep['key']['fields'][1] = copy.deepcopy(rep['key']['fields'][0]); bad.append(rep)
        rep = descriptor(); rep['key']['types'] = []; bad.append(rep)
        rep = descriptor(); rep['key']['fields'] = []; rep['key']['types'] = []; bad.append(rep)
        for family, facets, nullable in [('integer', {}, False), ('string', {'unknown': True}, False),
                                        ('string', {}, True), ('string', {}, 0)]:
            rep = descriptor(); rep['key']['types'][0] = dict(family=family, facets=facets, nullable=nullable)
            bad.append(rep)
        for rep in bad:
            with self.subTest(rep=rep), self.assertRaises(RelatedKeysDecodeError):
                self.decode({'items': [], 'truncated': False}, rep)
        for pins in [[], PINS + PINS, [dict(PINS[0], sha256='invalid')],
                     [dict(PINS[0], umfVersion='0.7.0-relabelled')], [dict(PINS[0], extra='unknown')]]:
            with self.subTest(pins=pins), self.assertRaises(RelatedKeysDecodeError):
                self.decode({'items': [], 'truncated': False}, pins=pins)

    def test_utf8_capacity_preflight_and_explicit_finite_configuration(self):
        raw = '{"items":[["雪"]],"truncated":false}'
        size = len(raw.encode('utf8'))
        self.decode(raw, maximum=size)
        self.decode(raw.encode('utf8'), maximum=size)
        for cell in [raw, raw.encode('utf8')]:
            with self.assertRaises(RelatedKeysDecodeError):
                self.decode(cell, maximum=size-1)
        for limit in [True, False, 0, -1, 16*1024*1024+1, 1.0, None]:
            with self.subTest(limit=limit), self.assertRaises(RelatedKeysDecodeError):
                RelatedKeysDecodeConfig(limit)
        with self.assertRaises(RelatedKeysDecodeError):
            self.decode(bytearray(raw.encode('utf8')))
        with self.assertRaises(RelatedKeysDecodeError):
            self.decode('\ud800')

    def test_no_mutable_metadata_or_parsed_cell_aliases_escape(self):
        rep, pins = descriptor(), copy.deepcopy(PINS)
        result = self.decode({'items': [['a'], ['a']], 'truncated': False}, rep, pins)
        rep['key']['fields'].clear(); pins[0]['revision'] = 'changed'
        self.assertEqual(result.items, (('a',), ('a',)))


if __name__ == '__main__':
    unittest.main()
