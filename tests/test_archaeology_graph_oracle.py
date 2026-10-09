import json,unittest
from archaeology_graph_oracle import ROOT,original_oracle
class Tests(unittest.TestCase):
 def setUp(self):self.m=(ROOT/'ontology.json').read_bytes();self.g=(ROOT/'graph/fixture.json').read_bytes()
 def test_original_all_rows_and_eight_scenario_joins(self):
  x=original_oracle(self.m,self.g);g=json.loads(self.g)
  self.assertEqual(x['source_metadata'],g['source_metadata']);self.assertEqual(x['objects'],g['objects']);self.assertEqual(x['edges'],g['edges']);self.assertEqual(len(x['objects']),39);self.assertEqual(len(x['edges']),46)
  k=lambda s:'[42,0,"'+s+'"]'
  self.assertEqual(x['scenarios'],{'cycle':[[k('ST1'),k('ST2')]],'dating':[['Author A','Author B']],'media':[[k('AS4'),'2']],'missing-media':[[k('AS6')]],'specialists':[['5','2','3','1']],'lineage':[[k('O1'),'1']],'evidence-links':[['Author A','bowl',None],['Author B',None,'synthetic-ungulate']],'sample':[[k('SA1'),'dry-sieve']]})
  self.assertEqual(sum(v is None for o in x['objects'] for v in o['values'].values()),13)
  decimals=[v for o in x['objects'] for k,v in o['values'].items() if k in ('pottery_results.weight','soil_results.value')]
  self.assertEqual(decimals,['2E+1','4E+1'])
 def test_changed_original_bytes_refuse(self):
  for m,g in [(self.m+b' ',self.g),(self.m,self.g+b' ')]:
   with self.assertRaises(ValueError):original_oracle(m,g)
