import json
import unittest
from weft_path_fixture import fixture_inputs, expected_cases, FIXTURE

class PathFixtureTests(unittest.TestCase):
    def test_independent_occurrence_bags(self):
        m,g,_=fixture_inputs(); result=expected_cases(m,g)
        self.assertEqual(result,json.loads((FIXTURE/'expected.json').read_bytes()))
        cases={c['name']:c for c in result['cases']}
        six=cases['parallel-six'];self.assertEqual((six['count'],six['distinctTargets']),(6,1))
        self.assertEqual([p['edges'] for p in six['paths']['items']], [[a,b] for a in ['-9223372036854775808','0'] for b in ['-1','1','9223372036854775807']])
        self.assertTrue(cases['truncated-two']['paths']['truncated'])
        self.assertEqual([p['intermediate'][0] for p in cases['typed-order']['paths']['items']],['2','10'])
        self.assertEqual(cases['inverse']['count'],4)
        self.assertEqual(cases['self-loop']['paths']['items'][0]['edges'],['0','0'])
        self.assertEqual(cases['left-presence']['rows'][0][1]['state'],'value')
        self.assertEqual(cases['left-presence']['rows'][2][1],{'state':'absent'})
    def test_parallel_removal_changes_bag_without_target_change(self):
        m,g,_=fixture_inputs();v=json.loads(g);v['edges']=v['edges'][1:]
        c=expected_cases(m,json.dumps(v).encode())['cases'][0]
        self.assertEqual((c['count'],c['distinctTargets']),(3,1))
    def test_duplicate_complete_edge_identity_refuses(self):
        m,g,_=fixture_inputs();v=json.loads(g);v['edges'].append(v['edges'][0])
        with self.assertRaises(ValueError):expected_cases(m,json.dumps(v).encode())
