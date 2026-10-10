import unittest
from pathlib import Path
from prepare_supply_chain_query_cases import prepare_supply_chain_cases, CASE_IDS

ROOT = Path(__file__).resolve().parents[1] / 'examples/domain-packs/supply-chain/upstream'


class SupplyChainQueryCases(unittest.TestCase):
    def inputs(self):
        return tuple((ROOT / name).read_bytes() for name in ('pack.json', 'ontology.json', 'graph/fixture.json'))

    def test_original_queries_and_independent_lexical_bags(self):
        result = prepare_supply_chain_cases(*self.inputs())
        self.assertEqual(result['original_umf_version'], '0.8.0')
        cases = {case['id']: case for case in result['cases']}
        self.assertEqual(tuple(cases), CASE_IDS)
        self.assertEqual(cases['split-excursion']['original_graph_rows'], [['LOT1', '2']])
        self.assertEqual(cases['replay']['original_graph_rows'], [['[42,0,"SOURCE1"]', '2']])
        self.assertEqual(cases['sensor']['original_graph_rows'], [['Cel', 'probe']])
        self.assertEqual(cases['lineage']['original_graph_rows'],
                         [['SER1', 'LOT1', 'SKU1', '[42,0,"C1"]'], ['SER2', 'LOT1', 'SKU1', None]])
        self.assertEqual(cases['excursion']['original_graph_rows'], [['[42,0,"M2"]', '17', 'Cel']])
        self.assertIn('COUNT(DISTINCT s.id)', cases['split-excursion']['original_scenario']['sql'])
        self.assertIn('HAVING COUNT(*)>1', cases['replay']['original_scenario']['sql'])
        fields = [f['original_field'] for t in result['tables'] for f in t['fields']]
        self.assertEqual([f['facets'] for f in fields if f['scalarType'] == 'decimal'],
                         [{'precision': 18, 'scale': 2}] * 2)
        self.assertEqual(len(result['optional_props_requirements']), 1)
        requirement = result['optional_props_requirements'][0]
        self.assertEqual(requirement['identity'][-1], 'containers.parent_id')
        self.assertEqual(requirement['present_null'], 'state:null')
        self.assertEqual(requirement['missing_property'], 'refuse')

    def test_each_original_input_refuses_independent_tamper(self):
        for index in range(3):
            raw = list(self.inputs())
            raw[index] += b' '
            with self.subTest(index=index), self.assertRaises(ValueError):
                prepare_supply_chain_cases(*raw)

    def test_returned_mutation_does_not_change_next_preparation(self):
        first = prepare_supply_chain_cases(*self.inputs())
        first['cases'][0]['original_graph_rows'].clear()
        first['tables'].clear()
        self.assertEqual(prepare_supply_chain_cases(*self.inputs())['cases'][0]['original_graph_rows'], [['LOT1', '2']])


if __name__ == '__main__':
    unittest.main()
