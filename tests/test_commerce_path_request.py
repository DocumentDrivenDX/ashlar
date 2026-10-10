"""Relationship composition controls; scalar source admission remains its owner."""
import copy
import hashlib
import json
import unittest
from unittest.mock import patch

from commerce_path_request import commerce_path_request
from test_weft_path_plan import FIXTURES


class CommercePathRequestTests(unittest.TestCase):
    def setUp(self):
        self.request = copy.deepcopy(next(f['request'] for f in FIXTURES
                                          if f['test'] == 'original_collection_retains_pins_keys_wide_order_and_closed_inventory'))
        self.model = self.request['modules'][0]['documentJson'].encode()
        binding = json.loads(self.request['target']['bindingJson'])
        self.bindings = {'types': [
            {'identity': ['edge', module['id'], r['id']], 'type_id': str(i)}
            for module in json.loads(self.model)['modules']
            for i, r in enumerate(module.get('relationships', []), 1)]}
        binding.pop('relationships')
        self.request['target']['bindingJson'] = json.dumps(binding)

    def call(self):
        with patch('commerce_path_request.scalar_request', return_value=copy.deepcopy(self.request)):
            return commerce_path_request('unchanged SQL', self.model, self.bindings, {}, [], {})

    def test_complete_original_relationship_composition_and_no_mutation(self):
        before = copy.deepcopy(self.bindings)
        result = self.call()
        raw = result['target']['bindingJson']
        binding = json.loads(raw)
        self.assertEqual(len(binding['relationships']), 9)
        self.assertEqual(result['target']['bindingSha256'], hashlib.sha256(raw.encode()).hexdigest())
        self.assertEqual(result['interfaceVersion'], 'weft-compile/0.4.0')
        self.assertEqual(result['dialect'], 'weft-sql/0.4.0')
        self.assertEqual(self.bindings, before)

    def test_missing_edge_carrier_refuses(self):
        self.bindings['types'].pop()
        with self.assertRaises(ValueError):
            self.call()

    def test_ambiguous_edge_carriers_refuse(self):
        original = copy.deepcopy(self.bindings)
        self.bindings['types'].append(copy.deepcopy(self.bindings['types'][0]))
        with self.assertRaises(ValueError):
            self.call()
        self.bindings = original
        self.bindings['types'][1]['type_id'] = self.bindings['types'][0]['type_id']
        with self.assertRaises(ValueError):
            self.call()

    def test_missing_edge_table_refuses(self):
        binding = json.loads(self.request['target']['bindingJson'])
        binding['publication']['tables'] = [t for t in binding['publication']['tables']
                                            if t['name'][-1] != 'edge_current']
        self.request['target']['bindingJson'] = json.dumps(binding)
        with self.assertRaises(ValueError):
            self.call()
