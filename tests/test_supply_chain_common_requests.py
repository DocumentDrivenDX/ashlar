# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure full-pair preparation controls; no installed/native query claims."""
import copy
import json
import unittest
from pathlib import Path

from ashlar.supply_chain_source import build_transaction
from ashlar_host.supply_chain_common_requests import prepare_supply_chain_two_hop

ROOT = Path(__file__).resolve().parents[1]


class SupplyChainCommonRequestsTests(unittest.TestCase):
    def setUp(self):
        source = ROOT / 'examples/domain-packs/supply-chain/upstream'
        self.raw = tuple((source / name).read_bytes() for name in
                         ('pack.json', 'ontology.json', 'graph/fixture.json'))
        _, self.bindings = build_transaction(*self.raw[1:], source_system='private-original-supply-chain-fixture')
        evidence = json.loads((ROOT / 'docs/helix/04-build/evidence/supply-chain-native-publication-20261009.json').read_bytes())
        self.manifest, self.registry = evidence['native_manifest'], evidence['table_registry']
        self.aliases = {name: name.replace('local.', 'spark_catalog.', 1)
                        for name in json.loads(self.manifest['table_versions_json'])}

    def prepare(self):
        return prepare_supply_chain_two_hop(*self.raw, self.bindings, self.manifest,
                                           self.registry, self.aliases)

    def test_complete_pair_inventory_includes_empty_and_self_pair(self):
        result = self.prepare()
        self.assertEqual(len(result['queries']), 12)
        self.assertEqual(result['case'], 'QUERY/supply-chain/two-hop')
        pairs = {tuple(q['relationships']) for q in result['queries']}
        self.assertIn(('containers.parent_id', 'containers.parent_id'), pairs)
        counts = []
        for query in result['queries']:
            binding = json.loads(query['request']['target']['bindingJson'])
            self.assertEqual(binding['modelPins'][0]['umfVersion'], '0.8.0')
            self.assertEqual(len(binding['relationships']), 13)
            rows = [json.loads(row[1]) for row in query['expected_rows']]
            self.assertTrue(all(row['truncated'] is False for row in rows))
            counts.extend(len(row['items']) for row in rows)
        self.assertIn(0, counts)
        self.assertEqual(sum(counts), len(result['expected_occurrences']))

    def test_original_edge_ids_and_whole_input_are_not_borrowed(self):
        original = copy.deepcopy(self.bindings)
        result = self.prepare()
        self.assertEqual(self.bindings, original)
        assigned = {row['originalKey']: row['id'] for row in self.bindings['entities'] if row['kind'] == 'edge'}
        expected = sorted((assigned[row[1]], assigned[row[3]]) for row in result['expected_occurrences'])
        actual = sorted(tuple(path['edges']) for query in result['queries']
                        for row in query['expected_rows'] for path in json.loads(row[1])['items'])
        self.assertEqual(actual, expected)
        self.bindings['entities'][0]['id'] = '999'
        with self.assertRaises(ValueError):
            self.prepare()

    def test_path_cells_preserve_native_struct_field_order_and_empty_wire(self):
        result = self.prepare()
        nonempty = empty = 0
        for query in result['queries']:
            roots = [row[0] for row in query['expected_rows']]
            self.assertEqual(roots, sorted(roots, key=lambda value: value.encode('utf-8')))
            for _, cell in query['expected_rows']:
                value = json.loads(cell)
                self.assertEqual(list(value), ['items', 'truncated'])
                self.assertEqual(cell, json.dumps(value, ensure_ascii=False, separators=(',', ':')))
                if not value['items']:
                    empty += 1
                    self.assertEqual(cell, '{"items":[],"truncated":false}')
                else:
                    nonempty += 1
                    for item in value['items']:
                        self.assertEqual(list(item), ['intermediate', 'terminal', 'edges'])
                    sorted_cell = json.dumps(value, ensure_ascii=False, separators=(',', ':'), sort_keys=True)
                    self.assertEqual(json.loads(sorted_cell), value)
                    self.assertNotEqual(sorted_cell, cell)
        self.assertGreater(nonempty, 0)
        self.assertGreater(empty, 0)

    def test_first_retained_paths530_struct_order_matches_independent_oracle(self):
        # Exact field declaration from retained Paths commit 530ae35 SQL.
        # This is a wire-format correspondence control, not a native result.
        declaration = "named_struct('intermediate',array(CAST(`p`.`m0` AS STRING)),'terminal',array(CAST(`p`.`t0` AS STRING)),'edges',array(CAST(p.__edge1 AS STRING),CAST(p.__edge2 AS STRING)))"
        order = [key for key in ('intermediate', 'terminal', 'edges')
                 if "'" + key + "'" in declaration]
        self.assertEqual([declaration.index("'" + key + "'") for key in order],
                         sorted(declaration.index("'" + key + "'") for key in order))
        first = self.prepare()['queries'][0]
        self.assertEqual(first['relationships'], ['items.lot_id', 'lots.product_id'])
        items = [item for _, cell in first['expected_rows'] for item in json.loads(cell)['items']]
        self.assertTrue(items)
        self.assertTrue(all(list(item) == order for item in items))
        self.assertEqual(items[0], {'intermediate': ['[42,0,"L1"]'],
                                    'terminal': ['[42,0,"P1"]'], 'edges': ['3', '2']})

    def test_changed_original_model_or_graph_refuses(self):
        original = self.raw
        for ordinal in (0, 1, 2):
            self.raw = tuple(raw + b' ' if index == ordinal else raw for index, raw in enumerate(original))
            with self.assertRaises(ValueError):
                self.prepare()
        self.raw = original


if __name__ == '__main__':
    unittest.main()
