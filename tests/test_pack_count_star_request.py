"""Pure request/transport controls; retained metadata never grants authority."""
import copy,json
from pathlib import Path
from unittest import TestCase,mock
from ashlar_host.finite_pack import FinitePackDefinition
from ashlar_host.pack_count_star_request import pack_count_star_request
from ashlar_host.pack_count_star_compile import PackCountStarCompileConfig,compile_pack_count_star_cases
from ashlar.weft_count_star_distribution import CountStarDistributionPaths
ROOT=Path(__file__).resolve().parents[1]
class Tests(TestCase):
 def inputs(self,name):
  p=ROOT/'examples/domain-packs'/name/'upstream';raw=tuple((p/n).read_bytes()for n in ('pack.json','ontology.json','graph/fixture.json'));definition=FinitePackDefinition(name)
  report=json.loads((ROOT/'docs/helix/04-build/evidence'/(name+'-native-publication-20261009.json')).read_bytes())
  manifest=report['native_manifest'];registry=report['table_registry'];aliases={t:t.replace('local.','spark_catalog.',1)for t in json.loads(manifest['table_versions_json'])}
  _,bindings=definition.build(*raw[1:]);return definition,raw,bindings,manifest,registry,aliases
 def request(self,name,case):
  d,raw,*metadata=self.inputs(name);return pack_count_star_request(d,case,*raw,*metadata)
 def test_complete_original17_sql_models_carriers_and_optional_homes(self):
  for name,count,nulls in (('archaeology',8,9),('ecology',9,3)):
   d,raw,bindings,*metadata=self.inputs(name);document=json.loads(raw[1]);fields={(m['id'],f['id']):f for m in document['modules']for f in m['elements']}
   cases=d.cases(raw[1],raw[2],raw[0]);self.assertEqual(len(cases),count)
   for case,sql in cases:
    request=pack_count_star_request(d,case,*raw,bindings,*metadata);self.assertEqual(request['sql'],sql);self.assertEqual(request['modules'][0]['documentJson'].encode(),raw[1])
    self.assertEqual(request['interfaceVersion'],'weft-compile/0.4.1');binding=json.loads(request['target']['bindingJson']);properties=[p for r in binding['records']for p in r['properties']]
    self.assertEqual({p['home']['propertyId']for p in properties},{p['property_id']for p in bindings['properties']})
    optional=[p for p in properties if fields[(p['logical']['module'],p['logical']['element'])]['nullability']=='absent-allowed'];self.assertEqual(len(optional),nulls)
    for p in properties:
     if p in optional:self.assertEqual(p['home']['encoding'],'ashlar-weft-json-native-null/0.1-candidate')
     else:self.assertNotIn('encoding',p['home'])
 def test_no_rekeying_alias_drift_or_changed_original_bytes(self):
  d,raw,b,m,r,a=self.inputs('archaeology');bad=copy.deepcopy(b);bad['properties'][0]['property_id']='changed-original'
  for bindings,aliases,original in ((bad,a,raw),(b,{**a,next(iter(a)):'two.parts'},raw),(b,a,(raw[0]+b' ',*raw[1:]))):
   with self.assertRaises(ValueError):pack_count_star_request(d,'cycle',*original,bindings,m,r,aliases)
 def test_executable_equality_and_string_subclasses_rejected(self):
  d,raw,b,m,r,a=self.inputs('ecology')
  class Spoof(dict):
   def __ne__(self,other):return False
  forged=Spoof(copy.deepcopy(b));forged['properties'][0]['property_id']='forged'
  with self.assertRaises(ValueError):pack_count_star_request(d,'zero',*raw,forged,m,r,a)
  class Str(str):pass
  aliases=dict(a);key=next(iter(a));aliases[Str(key)]=aliases.pop(key)
  with self.assertRaises(ValueError):pack_count_star_request(d,'zero',*raw,b,m,r,aliases)
 def test_complete_requests_and_bound_before_compiler_or_schema_effects(self):
  d,raw,b,m,r,a=self.inputs('archaeology');cfg=PackCountStarCompileConfig(CountStarDistributionPaths(Path('/index'),Path('/installed')),1)
  with mock.patch('ashlar_host.pack_count_star_compile.installed_count_star_schema_bundle')as effect:
   with self.assertRaises(ValueError):compile_pack_count_star_cases(cfg,d,*raw,b,m,r,a,observe=lambda *a:None)
   effect.assert_not_called()
 def test_mutated_typed_configuration_refuses_before_effects(self):
  d,raw,*metadata=self.inputs('ecology');cfg=PackCountStarCompileConfig(CountStarDistributionPaths(Path('/index'),Path('/installed')),1000000);object.__setattr__(cfg,'maximum_artifact_bytes',True)
  with mock.patch('ashlar_host.pack_count_star_compile.installed_count_star_schema_bundle')as effect:
   with self.assertRaises(ValueError):compile_pack_count_star_cases(cfg,d,*raw,*metadata,observe=lambda *a:None)
   effect.assert_not_called()
 def mocked_pipeline(self,observe):
  import zlib
  from ashlar_host.count_star_admission import CountStarSchemaValidation
  d,raw,*metadata=self.inputs('ecology');self.pipeline_metadata=metadata;cfg=PackCountStarCompileConfig(CountStarDistributionPaths(Path('/index'),Path('/installed')),1000000)
  schemas={k:v.encode()for k,v in json.loads(zlib.decompress((ROOT/'tests/fixtures/weft_count_star_admission.json.zlib').read_bytes()))['schemas'].items()}
  with mock.patch('ashlar_host.pack_count_star_compile.installed_count_star_schema_bundle',return_value=schemas),mock.patch('ashlar_host.pack_count_star_compile.make_offline_count_star_schema_validation',return_value=CountStarSchemaValidation(schemas,lambda *a:None)),mock.patch('ashlar_host.pack_count_star_compile.compile_count_star_distribution',return_value=b'{"status":"blocked"}'):
   return compile_pack_count_star_cases(cfg,d,*raw,*metadata,observe=observe)
 def test_blocked_raw_bytes_observed_but_never_qualified(self):
  observations=[]
  with self.assertRaises(ValueError):self.mocked_pipeline(lambda *r:observations.append(r))
  self.assertEqual(len(observations),2);self.assertEqual(observations[0][0],'effort-event');self.assertEqual(observations[0][3],b'{"status":"blocked"}')
 def test_observer_original_cancellation_identity(self):
  cancellation=KeyboardInterrupt()
  def observe(*a):raise cancellation
  with self.assertRaises(KeyboardInterrupt)as caught:self.mocked_pipeline(observe)
  self.assertIs(caught.exception,cancellation)

 def test_mutation_after_snapshot_never_changes_requests_and_refuses_closing(self):
  observed=[]
  def observer(name,iteration,request,response):
   observed.append(json.loads(request))
   if len(observed)==1:self.pipeline_metadata[0]['properties'][0]['property_id']='changed-after-snapshot'
  with mock.patch('ashlar_host.pack_count_star_compile.admit_count_star_artifact',return_value=None):
   with self.assertRaisesRegex(ValueError,'closing input metadata'):self.mocked_pipeline(observer)
  self.assertEqual(len(observed),18)
  for request in observed:
   self.assertNotIn('changed-after-snapshot',request['target']['bindingJson'])
 def test_closing_cancellation_outranks_ordinary_observer_failure(self):
  from ashlar_host.supply_chain_request import source_snapshot
  d,raw,*metadata=self.inputs('ecology');snapshot=source_snapshot(metadata);cancellation=KeyboardInterrupt()
  def observer(*args):raise ValueError('ordinary observer failure')
  with mock.patch('ashlar_host.pack_count_star_compile.source_snapshot',side_effect=[snapshot,cancellation]):
   with self.assertRaises(KeyboardInterrupt)as caught:self.mocked_pipeline(observer)
  self.assertIs(caught.exception,cancellation)
