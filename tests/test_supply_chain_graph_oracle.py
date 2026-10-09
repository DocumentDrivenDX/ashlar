import unittest,json
from supply_chain_graph_oracle import ROOT,original_graph_oracle
class Tests(unittest.TestCase):
    def setUp(self):self.model=(ROOT/'ontology.json').read_bytes();self.graph=(ROOT/'graph/fixture.json').read_bytes()
    def test_original_nullable_parents_duplicate_events_and_source_ids_preserved(self):
        result=original_graph_oracle(self.model,self.graph);original=json.loads(self.graph)
        self.assertEqual(result['objects'],original['objects']);self.assertEqual(result['edges'],original['edges']);self.assertEqual((len(result['objects']),len(result['edges'])),(20,23))
        self.assertEqual(result['cases']['split-excursion'],[{'lot_code':'LOT1','shipment_count':'2'}])
        self.assertEqual(result['cases']['sensor'],[{'unit':'Cel','method':'probe'}])
        self.assertEqual(result['cases']['replay'],[{'upstream_event_id':'[42,0,"SOURCE1"]','count':'2'}])
        self.assertEqual(result['cases']['excursion'],[{'id':'[42,0,"M2"]','value':'17','unit':'Cel'}])
        self.assertEqual(result['cases']['lineage'],[{'serial':'SER1','lot_code':'LOT1','sku':'SKU1','parent_id':'[42,0,"C1"]'},{'serial':'SER2','lot_code':'LOT1','sku':'SKU1','parent_id':None}])
        self.assertEqual(sum(v is None for o in result['objects'] for v in o['values'].values()),2)
    def test_changed_original_bytes_refuse_instead_of_silently_rebinding(self):
        for model,graph in ((self.model+b' ',self.graph),(self.model,self.graph+b' ')):
            with self.assertRaises(ValueError):original_graph_oracle(model,graph)
