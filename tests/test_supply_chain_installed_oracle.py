import json
from pathlib import Path
from unittest import TestCase
from ashlar_host.supply_chain_oracle import supply_chain_result_oracle,original_graph_oracle,_spark041_decimal_text
ROOT=Path(__file__).resolve().parents[1]/'examples/domain-packs/supply-chain/upstream'
class Tests(TestCase):
 def setUp(self):self.model=(ROOT/'ontology.json').read_bytes();self.graph=(ROOT/'graph/fixture.json').read_bytes()
 def oracle(self,name):return supply_chain_result_oracle(name,self.model,self.graph)
 def test_full_two_row_lineage_and_present_null(self):
  self.assertEqual(self.oracle('lineage')['rows'],[['SER1','LOT1','SKU1','{"state":"value","value":"[42,0,\\\"C1\\\"]"}'],['SER2','LOT1','SKU1','{"state":"null"}']])
  self.assertEqual(self.oracle('lineage')['witnesses']['complete_result_occurrences'],2)
 def test_complete_event_bag_before_original_having(self):
  graph=json.loads(self.graph);events=[o for o in graph['objects']if o['type']['element']=='events'];self.assertEqual(len(events),2)
  self.assertEqual(len({e['key']for e in events}),2);self.assertEqual(len({e['values']['events.upstream_event_id']for e in events}),1)
  self.assertEqual(self.oracle('replay')['rows'],[['[42,0,"SOURCE1"]','2']])
 def test_distinct_shipments_and_sensor_pairs_and_exact_excursion(self):
  self.assertEqual(self.oracle('split-excursion')['rows'],[['LOT1','2']]);self.assertEqual(self.oracle('sensor')['rows'],[['Cel','probe']]);self.assertEqual(self.oracle('excursion')['rows'],[['[42,0,"M2"]','17.00','Cel']])
 def test_changed_original_source_and_unknown_case_refuse(self):
  with self.assertRaises(ValueError):supply_chain_result_oracle('replay',self.model,self.graph+b' ')
  with self.assertRaises(ValueError):self.oracle('rewritten-query')

 def test_decimal_expected_carrier_derives_scale_without_mutating_original(self):
  original=original_graph_oracle(self.model,self.graph)
  self.assertEqual(original['cases']['excursion'][0]['value'],'17')
  model=json.loads(self.model);field=next(f for m in model['modules']for f in m['elements']if f['id']=='sensor_readings.value')
  self.assertEqual(field['facets'],{'precision':18,'scale':2})
  self.assertEqual(self.oracle('excursion')['rows'][0][1],_spark041_decimal_text(original['cases']['excursion'][0]['value'],18,2))
 def test_fixed_decimal_no_rounding_truncation_or_float(self):
  for value,p,s,expected in [('17',18,2,'17.00'),('-12.3',4,2,'-12.30'),('0.01',2,2,'0.01'),('123',3,0,'123')]:
   self.assertEqual(_spark041_decimal_text(value,p,s),expected)
  for value,p,s in [('1.234',4,2),('100',4,2),('1.230',4,2),('1e1',18,2),('NaN',18,2),('Infinity',18,2),(17.0,18,2),('-0',18,2),('-0.00',18,2),('01',18,2),('1',True,0),('1',18,True),('1',39,2),('1',18,19)]:
   with self.assertRaises(ValueError):_spark041_decimal_text(value,p,s)

 def test_lineage_presence_derived_from_original_present_values(self):
  original=original_graph_oracle(self.model,self.graph)['cases']['lineage']
  self.assertEqual([r['parent_id']for r in original],['[42,0,"C1"]',None])
  tagged=[json.loads(r[3])for r in self.oracle('lineage')['rows']]
  self.assertEqual(tagged,[{'state':'value','value':'[42,0,"C1"]'},{'state':'null'}])
  self.assertNotIn({'state':'absent'},tagged)
