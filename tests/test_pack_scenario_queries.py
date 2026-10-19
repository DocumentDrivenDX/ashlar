# SPDX-License-Identifier: MIT OR Apache-2.0
"""Public original17 pure helper parity; no source or engine authority."""
import base64,json,unittest
from ashlar_host import pack_scenario_queries as public
import test_pack_scenario_puppygraph as fixtures

class Tests(unittest.TestCase):
 def test_explicit_bindings_preserve_original_source_bag_without_profile_injection(self):
  from prepare_pack_scenario_puppygraph import entries as legacy_entries
  for profile in fixtures.ScenarioPuppyPreparationTests().profiles():
   bindings=json.loads(base64.b64decode(profile['source_admission']['original_bindings_base64']))
   expected=legacy_entries(profile)
   del profile['source_admission']['original_bindings_base64']
   self.assertEqual(public.entries(profile,bindings),expected)
   with self.assertRaises(ValueError):public.entries(profile,{})
   for case in profile['cases']:
    for language in ('Cypher','Gremlin'):
     query,params,names=public.query_plan(profile,case['original_scenario']['id'],language)
     self.assertTrue(query);self.assertTrue(names)
 def test_count_codec_preserves_exact_type_and_bounds(self):
  class Unknown(int):pass
  self.assertEqual(public.native_count(2**53+1),2**53+1)
  for value in (True,Unknown(1),-1,2**63,1.0):
   with self.subTest(value=value),self.assertRaises(ValueError):public.native_count(value)
  try:
   from gremlin_python.statics import long as graphson_long
  except ImportError:return
  self.assertEqual(public.native_count(graphson_long(2**53+1)),2**53+1)
 def test_native_map_codec_keeps_string_null_and_long_bag(self):
  expected=[{'graph_id':'n','text':'original','optional':None,'count':2}]
  row={'cells':{'carrier_key':'n','text':'original','count':2},'native_id':'Scenario[n]'}
  self.assertEqual(public.native_map_cells(row,'node',['graph_id','text','optional','count'],expected,public.MAP_PUPPY),expected[0])
  row['cells']['count']=True
  with self.assertRaises(ValueError):public.native_map_cells(row,'node',['graph_id','text','optional','count'],expected,public.MAP_PUPPY)
