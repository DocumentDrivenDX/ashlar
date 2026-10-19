# SPDX-License-Identifier: MIT OR Apache-2.0
"""Complete source footprint and logical integer comparison controls; no authority."""
import json,unittest
from ashlar_host.truss_indexed_commerce_guard import TrussIndexedCommerceGuard,SCHEMA

class Tests(unittest.TestCase):
 def guard(self):
  source={'publicSourceOnly':True,'field':{'element':'quantity'},'record':{'element':'line'}}
  representation={'representabilityOnly':True,'field':source['field'],'record':source['record']}
  row=['source','4','5','1','{"15":1E+2}','1E+2']
  return TrussIndexedCommerceGuard(json.dumps([{'source':source,'representability':representation,'rows':[row]}])),source,row
 def test_whole_native_footprint_and_distinct_interface_tokens(self):
  guard,check,row=self.guard();native=[*row[:-1],'100.0'];guard.require_source(check,{'schema':SCHEMA,'rows':[native]})
  self.assertEqual(json.loads(guard.entries_json)[0]['rows'][0][-1],'1E+2');self.assertEqual(native[-1],'100.0')
  for rows in ([],[native,native],[[*native[:3],'2',*native[4:]]],[[*native[:4],'{"15":100}',native[-1]]],[[*native[:-1],'101']]):
   with self.subTest(rows=rows),self.assertRaises(ValueError):guard.require_source(check,{'schema':SCHEMA,'rows':rows})
 def test_schema_and_unpaired_check_refuse(self):
  guard,check,row=self.guard()
  with self.assertRaises(ValueError):guard.require_source(check,{'schema':SCHEMA[:-1],'rows':[row]})
  with self.assertRaises(ValueError):guard.entry({'representabilityOnly':True})
