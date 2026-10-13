"""Pure original grouped path-occurrence COUNT; no native engine."""
import copy,json,unittest
from collections import Counter
import test_supply_chain_common_count_requests as fixtures
from ashlar_host.supply_chain_common_count_requests import prepare_supply_chain_common_count_requests

class Tests(unittest.TestCase):
    def fixture(self):
        fixture=fixtures.Controls();fixture.setUp();return fixture
    def test_original_two_hop_grouped_counts_and_edge_pair_witnesses(self):
        f=self.fixture();result=f.prepare('two-hop-count');query=result['queries'][0]
        self.assertEqual(query['relationships'],['items.lot_id','lots.product_id'])
        self.assertEqual(query['expected_rows'],[['[42,0,"I1"]','1'],['[42,0,"I2"]','1']])
        self.assertEqual(len(result['expected_occurrences']),2)
        self.assertEqual(query['original_path_occurrences'],result['expected_occurrences'])
        self.assertEqual(Counter(w['source']for w in query['original_edge_witnesses']),Counter(row[0]for row in result['expected_occurrences']))
        assigned={(b['kind'],b['originalKey']):b for b in f.bindings['entities']}
        for witness in query['original_edge_witnesses']:
            self.assertEqual(witness['edge_ids'],[assigned['edge',key]['id']for key in witness['original_edges']])
        self.assertEqual(query['request']['interfaceVersion'],'weft-compile/0.4.1')
        self.assertIn('CROSS JOIN EXPAND_PATHS',query['request']['sql']);self.assertIn('GROUP BY s.id',query['request']['sql'])
    def test_changed_original_graph_or_binding_refuses_without_substitution(self):
        f=self.fixture();graph=json.loads(f.raw[2]);graph['edges'].pop()
        with self.assertRaises(ValueError):
            prepare_supply_chain_common_count_requests(f.raw[0],f.raw[1],json.dumps(graph).encode(),f.bindings,f.manifest,f.registry,f.aliases,family='two-hop-count',part='complete')
        wrong=copy.deepcopy(f.bindings);wrong['entities'][0]['id']='999999'
        with self.assertRaises(ValueError):prepare_supply_chain_common_count_requests(*f.raw,wrong,f.manifest,f.registry,f.aliases,family='two-hop-count',part='complete')
