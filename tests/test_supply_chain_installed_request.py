import hashlib,json
from pathlib import Path
from unittest import TestCase,mock
from ashlar.supply_chain_source import build_transaction
from ashlar_host.delta_custody import encoded
from ashlar_host.supply_chain_request import supply_chain_cases,supply_chain_count_star_request
from ashlar_host.supply_chain_compile import SupplyChainCompileConfig,compile_supply_chain_cases,source_snapshot
from ashlar.weft_count_star_distribution import CountStarDistributionPaths
ROOT=Path(__file__).resolve().parents[1]
PINS={'split-excursion':'10fa9882b692eb91071c5b257d1bf36278e99d37248890cdeaa6ac20ad5a013f','excursion':'9e8be3bfc310dc4a75be2625382a166c3788f6dcd60444fe9c44082b50db7ee9','replay':'4c16096f86a309d109a6c1e6358c5ca9135fca5484bc0f662e8a3053deab628c','lineage':'41be129090c12b8b525e023f63c038d571b993161e4c276cc50553fdfec9ace8','sensor':'da2df5b56e77f47ba7ea84156da3aa9d36e22b958d1bc6ae9990dc2b55e8cd90'}
class Tests(TestCase):
 def setUp(self):
  p=ROOT/'examples/domain-packs/supply-chain/upstream';self.raw=tuple((p/n).read_bytes()for n in ('pack.json','ontology.json','graph/fixture.json'))
  r=json.loads((ROOT/'docs/helix/04-build/evidence/supply-chain-native-publication-20261009.json').read_bytes());self.manifest=r['native_manifest'];self.registry=r['table_registry'];self.aliases={t:t.replace('local.','spark_catalog.',1)for t in json.loads(self.manifest['table_versions_json'])}
  self.batch,self.bindings=build_transaction(*self.raw[1:],source_system='private-original-supply-chain-fixture')
 def request(self,name):return supply_chain_count_star_request(name,*self.raw,self.bindings,self.manifest,self.registry,self.aliases)
 def test_original_closed_transaction_carrier_and_binding_bytes(self):
  raw=self.batch.begin+b''.join(r.raw for r in self.batch.records)+self.batch.commit
  self.assertEqual(hashlib.sha256(raw).hexdigest(),'87f3721419b96da1dbfa0a4d425a4d721644768b135d41805c4b5c2662b42e94')
  self.assertEqual(hashlib.sha256((encoded(self.bindings)+'\n').encode()).hexdigest(),'9b156af621a6ee4d496adc52b638d5b2d9f46e115d96ca90e69fe57931d2509b')
 def test_original_five_retained_requests_only_explicit_marker_migration(self):
  p=ROOT/'docs/helix/04-build/evidence/supply-chain-compiler-20261010/originals/ashlar-supply-chain-compile-candidate-20261010-h'
  for name,_ in supply_chain_cases(*self.raw):
   raw=(p/(name+'.request.json')).read_bytes();self.assertEqual(hashlib.sha256(raw).hexdigest(),PINS[name]);expected=json.loads(raw)
   expected.update(interfaceVersion='weft-compile/0.4.1',dialect='weft-sql/0.4.1');expected['target']['backendVersion']='0.4.1-count-star-having-candidate'
   self.assertEqual(self.request(name),expected)
 def test_original_replay_sql_and_native_null_preserved(self):
  r=self.request('replay');self.assertEqual(r['sql'],'SELECT upstream_event_id,COUNT(*) FROM events GROUP BY upstream_event_id HAVING COUNT(*)>1')
  b=json.loads(r['target']['bindingJson']);homes=[p for t in b['records']for p in t['properties']if 'encoding'in p['home']]
  self.assertEqual(len(homes),1);self.assertEqual(homes[0]['logical']['element'],'containers.parent_id');self.assertEqual(homes[0]['home']['encoding'],'ashlar-weft-json-native-null/0.1-candidate')
 def test_bad_original_input_and_alias_refuse(self):
  with self.assertRaises(ValueError):supply_chain_cases(self.raw[0]+b' ',*self.raw[1:])
  self.aliases[next(iter(self.aliases))]='two.parts'
  with self.assertRaises(ValueError):self.request('sensor')
 def test_invalid_whole_corpus_metadata_refuses_before_effect(self):
  self.bindings['properties'][0]['property_id']='changed'
  cfg=SupplyChainCompileConfig(CountStarDistributionPaths(Path('/index'),Path('/installed')),1000000)
  with mock.patch('ashlar_host.supply_chain_compile.installed_count_star_schema_bundle')as effect:
   with self.assertRaises(ValueError):compile_supply_chain_cases(cfg,*self.raw,self.bindings,self.manifest,self.registry,self.aliases,observe=lambda *args:None)
   effect.assert_not_called()
 def test_overloaded_metadata_rejected(self):
  class Imposter(str):pass
  with self.assertRaises(ValueError):source_snapshot({'selected':Imposter('x')})
 def test_direct_builder_rejects_forged_overloaded_binding(self):
  import copy
  class Spoof(dict):
   def __ne__(self,other):return False
  forged=Spoof(copy.deepcopy(self.bindings));forged['properties'][0]['property_id']='forged-original-id'
  with self.assertRaises(ValueError):supply_chain_count_star_request('sensor',*self.raw,forged,self.manifest,self.registry,self.aliases)
  with self.assertRaises(ValueError):supply_chain_count_star_request('sensor',*self.raw,dict(forged),self.manifest,self.registry,self.aliases)
  self.assertNotEqual(self.bindings['properties'][0]['property_id'],'forged-original-id')
 def test_direct_builder_rejects_overloaded_borrowed_metadata(self):
  import copy
  class Imposter(str):pass
  variants=[]
  bindings=copy.deepcopy(self.bindings);bindings['properties'][0]['property_id']=Imposter(bindings['properties'][0]['property_id']);variants.append((bindings,self.manifest,self.registry,self.aliases))
  manifest=dict(self.manifest);manifest['publication_id']=Imposter(manifest['publication_id']);variants.append((self.bindings,manifest,self.registry,self.aliases))
  registry=copy.deepcopy(self.registry);registry[0]['uuid']=Imposter(registry[0]['uuid']);variants.append((self.bindings,self.manifest,registry,self.aliases))
  key=next(iter(self.aliases));aliases=dict(self.aliases);value=aliases.pop(key);aliases[Imposter(key)]=value;variants.append((self.bindings,self.manifest,self.registry,aliases))
  aliases=dict(self.aliases);aliases[key]=Imposter(aliases[key]);variants.append((self.bindings,self.manifest,self.registry,aliases))
  for metadata in variants:
   with self.subTest(metadata=metadata):
    with self.assertRaises(ValueError):supply_chain_count_star_request('sensor',*self.raw,*metadata)
 def mocked_pipeline(self,observer,*,snapshot_side_effect=None):
  from ashlar_host.count_star_admission import CountStarSchemaValidation
  import zlib
  fixture=json.loads(zlib.decompress((ROOT/'tests/fixtures/weft_count_star_admission.json.zlib').read_bytes()));schemas={k:v.encode()for k,v in fixture['schemas'].items()}
  cfg=SupplyChainCompileConfig(CountStarDistributionPaths(Path('/index'),Path('/installed')),1000000)
  with mock.patch('ashlar_host.supply_chain_compile.installed_count_star_schema_bundle',return_value=schemas),mock.patch('ashlar_host.supply_chain_compile.make_offline_count_star_schema_validation',return_value=CountStarSchemaValidation(schemas,lambda *a:None)),mock.patch('ashlar_host.supply_chain_compile.compile_count_star_distribution',return_value=b'{"status":"blocked"}'):
   if snapshot_side_effect is not None:
    with mock.patch('ashlar_host.supply_chain_compile.source_snapshot',side_effect=snapshot_side_effect):return compile_supply_chain_cases(cfg,*self.raw,self.bindings,self.manifest,self.registry,self.aliases,observe=observer)
   return compile_supply_chain_cases(cfg,*self.raw,self.bindings,self.manifest,self.registry,self.aliases,observe=observer)
 def test_blocked_bytes_observed_before_metadata_refusal(self):
  records=[]
  with self.assertRaises(ValueError):self.mocked_pipeline(lambda *row:records.append(row))
  self.assertEqual(len(records),2);self.assertEqual(records[0][0],'split-excursion');self.assertEqual(records[0][3],b'{"status":"blocked"}');self.assertEqual(records[0][2],records[1][2])
 def test_observer_cancellation_identity_survives_closing_failure(self):
  cancellation=KeyboardInterrupt()
  def observer(*args):
   self.bindings['properties'][0]['property_id']='changed'
   raise cancellation
  with self.assertRaises(KeyboardInterrupt)as error:self.mocked_pipeline(observer)
  self.assertIs(error.exception,cancellation)
 def test_closing_cancellation_outranks_ordinary_observer_failure(self):
  snapshot=source_snapshot([self.bindings,self.manifest,self.registry,self.aliases]);cancellation=KeyboardInterrupt()
  def observer(*args):raise ValueError('ordinary')
  with self.assertRaises(KeyboardInterrupt)as error:self.mocked_pipeline(observer,snapshot_side_effect=[snapshot,cancellation])
  self.assertIs(error.exception,cancellation)
 def test_request_bound_before_schema_files_and_relative_paths_refuse(self):
  with self.assertRaises(ValueError):SupplyChainCompileConfig(CountStarDistributionPaths(Path('relative'),Path('/installed')),1000000)
  cfg=SupplyChainCompileConfig(CountStarDistributionPaths(Path('/index'),Path('/installed')),1)
  with mock.patch('ashlar_host.supply_chain_compile.installed_count_star_schema_bundle')as effect:
   with self.assertRaises(ValueError):compile_supply_chain_cases(cfg,*self.raw,self.bindings,self.manifest,self.registry,self.aliases,observe=lambda *args:None)
   effect.assert_not_called()
