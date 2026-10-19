# SPDX-License-Identifier: MIT OR Apache-2.0
"""Exact original relationship binding controls; no compiler/native qualification."""
import copy,hashlib,json,unittest
from pathlib import Path
from ashlar.supply_chain_source import build_transaction
from ashlar_host.supply_chain_path_request import supply_chain_path_request
from ashlar_host.supply_chain_request import supply_chain_count_star_request
ROOT=Path(__file__).resolve().parents[1]
class SupplyChainPathRequestTests(unittest.TestCase):
 def setUp(self):
  p=ROOT/'examples/domain-packs/supply-chain/upstream';self.raw=tuple((p/n).read_bytes()for n in ('pack.json','ontology.json','graph/fixture.json'))
  _,self.bindings=build_transaction(*self.raw[1:],source_system='private-original-supply-chain-fixture')
  evidence=json.loads((ROOT/'docs/helix/04-build/evidence/supply-chain-native-publication-20261009.json').read_bytes());self.manifest=evidence['native_manifest'];self.registry=evidence['table_registry'];self.aliases={t:t.replace('local.','spark_catalog.',1)for t in json.loads(self.manifest['table_versions_json'])}
 def request(self,**kw):return supply_chain_path_request('SELECT id FROM items',*self.raw,self.bindings,self.manifest,self.registry,self.aliases,**kw)
 def test_complete_original_definitions_endpoints_and_carriers(self):
  request=self.request();binding=json.loads(request['target']['bindingJson']);base=json.loads(supply_chain_count_star_request('sensor',*self.raw,self.bindings,self.manifest,self.registry,self.aliases)['target']['bindingJson']);self.assertEqual(binding['records'],base['records']);self.assertEqual(binding['publication'],base['publication']);self.assertEqual(request['modules'][0]['documentJson'].encode(),self.raw[1]);self.assertEqual(binding['modelPins'][0]['umfVersion'],'0.8.0')
  definitions={(m['id'],d['id']):d for m in json.loads(self.raw[1])['modules']for d in m.get('relationships',[])};records={(r['logical']['module'],r['logical']['element']):r for r in binding['records']};types={tuple(e['identity']):e['type_id']for e in self.bindings['types']}
  self.assertEqual(len(binding['relationships']),13)
  for r in binding['relationships']:
   module=r['logical']['module'];name=r['logical']['relationship'];d=definitions[(module,name)];self.assertEqual(r['acceptedDefinition'],d);self.assertEqual(r['typeId'],types[('edge',module,name)]);self.assertEqual(binding['publication']['tables'][r['table']]['name'][-1],'edge_current')
   for side in ('source','target'):
    endpoint=d[side][0];self.assertEqual(r[side],records[(endpoint['module'],endpoint['element'])]['logical'])
  self.assertEqual(hashlib.sha256(request['target']['bindingJson'].encode()).hexdigest(),request['target']['bindingSha256'])
 def test_explicit_backend_preserves_source_and_nullable_home(self):
  a=self.request();b=self.request(profile='paths-keys');self.assertEqual(a['target']['bindingJson'],b['target']['bindingJson']);self.assertEqual(b['target']['backendId'],'ashlar.databricks.paths-keys');self.assertEqual(b['interfaceVersion'],'weft-compile/0.4.0');homes=[p['home']for r in json.loads(a['target']['bindingJson'])['records']for p in r['properties']if 'encoding'in p['home']];self.assertEqual(len(homes),1);self.assertEqual(homes[0]['encoding'],'ashlar-weft-json-native-null/0.1-candidate')
 def test_forged_missing_or_duplicate_carriers_refuse(self):
  for mode in ('missing','duplicate','changed'):
   original=self.bindings;self.bindings=copy.deepcopy(original);edges=[e for e in self.bindings['types']if e['identity'][0]=='edge']
   if mode=='missing':self.bindings['types'].remove(edges[0])
   elif mode=='duplicate':self.bindings['types'].append(copy.deepcopy(edges[0]))
   else:edges[0]['type_id']=edges[1]['type_id']
   with self.assertRaises(ValueError):self.request()
   self.bindings=original
 def test_borrowed_overloads_and_invalid_sql_profile_refuse(self):
  class Spoof(str):pass
  for sql in ('','x\x00',Spoof('SELECT id FROM items')):
   with self.assertRaises(ValueError):supply_chain_path_request(sql,*self.raw,self.bindings,self.manifest,self.registry,self.aliases)
  for profile in ('count-star',Spoof('paths')):
   with self.assertRaises(ValueError):self.request(profile=profile)
  original=self.bindings;self.bindings=copy.deepcopy(original);self.bindings['types'][0]['type_id']=Spoof(self.bindings['types'][0]['type_id'])
  with self.assertRaises(ValueError):self.request()
if __name__=='__main__':unittest.main()
