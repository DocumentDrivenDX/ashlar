import json,unittest
from ecology_graph_oracle import ROOT,original_oracle
class Tests(unittest.TestCase):
 def setUp(self):self.m=(ROOT/'ontology.json').read_bytes();self.g=(ROOT/'graph/fixture.json').read_bytes()
 def test_original_rows_and_nine_scenario_joins(self):
  x=original_oracle(self.m,self.g);g=json.loads(self.g)
  self.assertEqual(x['source_metadata'],g['source_metadata']);self.assertEqual(x['objects'],g['objects']);self.assertEqual(x['edges'],g['edges']);self.assertEqual(len(x['objects']),40);self.assertEqual(len(x['edges']),54)
  k=lambda s:'[42,0,"'+s+'"]'
  self.assertEqual(x['scenarios'],{'effort-event':[],'connected-measurements':[['dissolved-oxygen','2'],['temperature','2']],'censor':[[k('O2'),'0.1','below-detection']],'match':[[k('SM1')]],'effort':[[k('OC3')]],'zero':[[k('OC2')]],'network':[[k('R1'),k('R2')]],'comparability':[['benthos','whole','family']],'fishing':[['angler-hour']]})
  self.assertEqual(sum(v is None for o in x['objects'] for v in o['values'].values()),7)
 def test_changed_original_bytes_refuse(self):
  for m,g in [(self.m+b' ',self.g),(self.m,self.g+b' ')]:
   with self.assertRaises(ValueError):original_oracle(m,g)
