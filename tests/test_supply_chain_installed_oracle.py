import json
from pathlib import Path
from unittest import TestCase
from ashlar_host.supply_chain_oracle import supply_chain_result_oracle
ROOT=Path(__file__).resolve().parents[1]/'examples/domain-packs/supply-chain/upstream'
class Tests(TestCase):
 def setUp(self):self.model=(ROOT/'ontology.json').read_bytes();self.graph=(ROOT/'graph/fixture.json').read_bytes()
 def oracle(self,name):return supply_chain_result_oracle(name,self.model,self.graph)
 def test_full_two_row_lineage_and_present_null(self):
  self.assertEqual(self.oracle('lineage')['rows'],[['SER1','LOT1','SKU1','[42,0,"C1"]'],['SER2','LOT1','SKU1',None]])
  self.assertEqual(self.oracle('lineage')['witnesses']['complete_result_occurrences'],2)
 def test_complete_event_bag_before_original_having(self):
  graph=json.loads(self.graph);events=[o for o in graph['objects']if o['type']['element']=='events'];self.assertEqual(len(events),2)
  self.assertEqual(len({e['key']for e in events}),2);self.assertEqual(len({e['values']['events.upstream_event_id']for e in events}),1)
  self.assertEqual(self.oracle('replay')['rows'],[['[42,0,"SOURCE1"]','2']])
 def test_distinct_shipments_and_sensor_pairs_and_exact_excursion(self):
  self.assertEqual(self.oracle('split-excursion')['rows'],[['LOT1','2']]);self.assertEqual(self.oracle('sensor')['rows'],[['Cel','probe']]);self.assertEqual(self.oracle('excursion')['rows'],[['[42,0,"M2"]','17','Cel']])
 def test_changed_original_source_and_unknown_case_refuse(self):
  with self.assertRaises(ValueError):supply_chain_result_oracle('replay',self.model,self.graph+b' ')
  with self.assertRaises(ValueError):self.oracle('rewritten-query')
