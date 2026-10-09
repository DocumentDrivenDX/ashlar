import copy,json,unittest
from pathlib import Path
from run_commerce_graphframes import carriers,topology
ROOT=Path(__file__).resolve().parents[1]
class Tests(unittest.TestCase):
    def setUp(self):
        self.receipt=json.loads((ROOT/'docs/helix/04-build/evidence/commerce-source-graph-oracle-20261009.json').read_bytes())
    def test_original_source_key_lexical_and_receipt_reference_custody(self):
        nodes,edges=carriers(self.receipt)
        self.assertEqual((len(nodes),len(edges)),(11,10))
        for n in nodes:
            self.assertEqual(json.loads(n['id'])[0],self.receipt['preparedRequests']['documentId'])
            self.assertTrue(json.loads(n['field_receipt_refs_json']))
            self.assertIn('id',json.loads(n['lexical_json']))
        self.assertEqual(topology(nodes,edges)['one_hop'],10)
        self.assertTrue(all(e['relationship_id'] for e in edges))
    def test_duplicate_dangling_and_literal_key_corruption_refuse(self):
        for mutation in ['duplicate-node','duplicate-edge','dangling','literal','key','edge-key','field-receipt']:
            r=copy.deepcopy(self.receipt)
            if mutation=='duplicate-node':r['graph']['nodes'].append(copy.deepcopy(r['graph']['nodes'][0]))
            if mutation=='duplicate-edge':r['graph']['edges'].append(copy.deepcopy(r['graph']['edges'][0]))
            if mutation=='dangling':r['graph']['edges'][0]['target']='absent'
            if mutation=='literal':r['graph']['nodes'][0]['values']={}
            if mutation=='key':r['graph']['nodes'][0]['key']='forged'
            if mutation=='edge-key':r['graph']['edges'][0]['key']='forged'
            if mutation=='field-receipt':r['publicUmfOperations']['fields'][0]['request']['value']={'string':'forged'}
            with self.subTest(mutation=mutation),self.assertRaises(ValueError):carriers(r)
    def test_parallel_loop_and_isolate_topology_control_separate_from_pack(self):
        nodes=[{'id':s} for s in ['a','b','isolate']]
        edges=[{'src':'a','dst':'b'},{'src':'a','dst':'b'},{'src':'a','dst':'a'}]
        self.assertEqual(topology(nodes,edges),{'nodes':3,'edges':3,'one_hop':3,'two_hop':3,'isolates':1})
