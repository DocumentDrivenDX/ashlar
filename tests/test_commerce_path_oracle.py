import json
from pathlib import Path
import unittest
from commerce_path_oracle import COLLECTION, COUNT, DISTINCT, original_commerce_path_oracle


class Oracle(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[1] / 'examples/domain-packs/commerce/upstream'
        self.model = (root / 'ontology.json').read_bytes()
        self.graph = (root / 'graph/fixture.json').read_bytes()

    def test_original_opaque_keys_and_edge_occurrences(self):
        result = original_commerce_path_oracle(self.model, self.graph, {'sql': COLLECTION})
        self.assertEqual(result['witnesses'], {'path_occurrences': 1, 'distinct_terminal_keys': 1, 'edge_pairs': [['4', '1']]})
        self.assertEqual(result['rows'][0][0], '[42,0,"L1"]')
        cell = json.loads(result['rows'][0][1])
        self.assertEqual(cell, {'items': [{'intermediate': ['[42,0,"P1"]'], 'terminal': ['[42,0,"S1"]'], 'edges': ['4', '1']}], 'truncated': False})
        for sql in (COUNT, DISTINCT):
            self.assertEqual(original_commerce_path_oracle(self.model, self.graph, {'sql': sql})['rows'], [['1']])

    def test_changed_source_and_uncovered_query_refuse(self):
        for model, graph, sql in [(self.model+b' ', self.graph, COLLECTION),
                                  (self.model, self.graph+b' ', COLLECTION),
                                  (self.model, self.graph, COLLECTION+' ')]:
            with self.assertRaises(ValueError):
                original_commerce_path_oracle(model, graph, {'sql': sql})


if __name__ == '__main__': unittest.main()
