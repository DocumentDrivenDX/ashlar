# SPDX-License-Identifier: MIT OR Apache-2.0
"""0.2 owning orchestration controls with inert query/native lower ports only."""
import contextlib,json,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host import supply_chain_native as native,supply_chain_native_cli as cli
import test_supply_chain_native_cli as prior


def configured(root):
 path,value=prior.SupplyChainNativeCliTests().configuration(root)
 value['profile']=cli.PROFILE_TWO_HOP
 value['two_hop']={'distribution':{'index':str(root/'paths-index.json'),'installation':str(root/'paths-install')},'maximum_artifact_bytes':1048576,'capture':{'maximum_rows':128,'maximum_cell_bytes':1048576,'maximum_total_cell_bytes':16777216},'decoder':{'maximum_cell_bytes':1048576}}
 path.write_text(json.dumps(value));return path,value

class IntegrationTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name).resolve()
  self.path,self.value=configured(self.root);self.config=cli.parse_supply_chain_native_invocation(self.path.read_bytes()).native
  self.config.output.mkdir();self.driver=SimpleNamespace(source=None);self.pack=b'original';self.aliases={'original':'alias'};self.calls=[]
 def authored(self,driver,pack,aliases,*a,observe,observe_native):
  self.assertIs(driver,self.driver);self.assertIs(pack,self.pack);self.assertIs(aliases,self.aliases);self.calls.append('five')
  for i in range(5):
   for j in range(2):observe('case'+str(i),j,b'count request',b'count response')
   observe_native('case'+str(i),{'rows':[['original'],['original']]})
  return {'cases':[{'id':str(i)}for i in range(5)]}
 def two(self,driver,pack,aliases,config,*,observe):
  self.assertIs(driver,self.driver);self.assertIs(pack,self.pack);self.assertIs(aliases,self.aliases);self.assertIs(config,self.config.two_hop);self.calls.append('twelve')
  for i in range(12):
   for j in range(2):observe(i,j,b'paths request',b'paths response')
  return {'case':'QUERY/supply-chain/two-hop','queries':[{'ordinal':i,'evidence':{'rows':[['null',None],['null',None]]}}for i in range(12)]}
 def ports(self,two=None):
  import ashlar_host.supply_chain_two_hop_query as two_owner
  stack=contextlib.ExitStack()
  for owner,name,kw in [(native,'_schemas',{'return_value':None}),(native,'CountStarAdmissionConfig',{'return_value':None}),(native,'CountStarExecutionConfig',{'return_value':None}),(native,'finite_public_source',{'return_value':None}),(native,'query_supply_chain_cases',{'side_effect':self.authored}),(two_owner,'query_supply_chain_two_hop',{'side_effect':two or self.two})]:stack.enter_context(patch.object(owner,name,**kw))
  return stack
 def test_five_then_twelve_same_owner_and_complete_bounded_sinks(self):
  with self.ports():answer=native._queries(self.config,self.driver,self.pack,self.aliases)
  self.assertEqual(self.calls,['five','twelve']);self.assertEqual(len(answer['cases']),5);self.assertEqual(answer['two_hop_queries'],12)
  protocol=[json.loads(x)for x in (self.config.output/'compiler-observations.jsonl').read_text().splitlines()];rows=[json.loads(x)for x in (self.config.output/'native-observations.jsonl').read_text().splitlines()]
  self.assertEqual(len(protocol),34);self.assertEqual(len(rows),17);self.assertEqual(rows[-1]['captured']['rows'],[['null',None],['null',None]])
 def test_actual_two_hop_helper_and_orchestration_twenty_four_protocols(self):
  import test_supply_chain_two_hop_query as helpers
  import ashlar_host.supply_chain_two_hop_query as actual
  fixture=helpers.Controls();fixture.setUp();original,driver=fixture._inert_driver()
  # Real typed helper/original request+oracle preparation; isolated nominal
  # source/driver doubles and inert compiler/SDK executor confer no authority.
  self.driver=driver;self.pack=original.raw[0];self.aliases=original.aliases
  schemas={'compile-request-v0.4.schema.json':b'a','compile-response-v0.4.schema.json':b'b','logical-plan-v0.4.schema.json':b'c'}
  def execute(opened,request,*a,original_oracle,**k):
   self.assertIs(opened.context,driver.context)
   oracle=original_oracle(opened.model,opened.graph,request)
   return {'independent_rows':oracle['rows']}
  with self.ports(actual.query_supply_chain_two_hop),patch.object(actual,'installed_paths_schema_bundle',return_value=tuple(schemas.items())),patch.object(actual,'make_offline_path_schema_validation'),patch.object(actual,'PathAdmissionConfig'),patch.object(actual,'PathExecutionConfig'),patch.object(actual,'FiniteSupplyChainProvider'),patch.object(actual,'compile_paths_distribution',return_value=b'{}')as compiler,patch.object(actual,'execute_commerce_path',side_effect=execute):
   answer=native._queries(self.config,self.driver,self.pack,self.aliases)
  self.assertEqual(compiler.call_count,24);self.assertEqual(len(answer['two_hop']['queries']),12)
  self.assertEqual(len((self.config.output/'compiler-observations.jsonl').read_text().splitlines()),34)

 def test_paths_per_view_limit_before_retaining_record(self):
  def two(*a,observe,**k):observe(0,0,b'x'*1048577,b'{}')
  with self.ports(two),self.assertRaises(ValueError):native._queries(self.config,self.driver,self.pack,self.aliases)
  self.assertEqual(len((self.config.output/'compiler-observations.jsonl').read_text().splitlines()),10)
 def test_same_aggregate_protocol_budget_includes_both_groups(self):
  def five(driver,pack,aliases,*a,observe,observe_native):
   for i in range(5):
    for j in range(2):observe(str(i),j,b'x'*150000,b'y'*150000)
   return {'cases':[{}for _ in range(5)]}
  def two(*a,observe,**k):
   for i in range(12):
    for j in range(2):observe(i,j,b'x'*150000,b'y'*150000)
  with self.ports(two),patch.object(native,'query_supply_chain_cases',side_effect=five),self.assertRaises(ValueError):native._queries(self.config,self.driver,self.pack,self.aliases)
  raw=(self.config.output/'compiler-observations.jsonl').read_bytes();self.assertLessEqual(len(raw),8388608);self.assertGreater(len(raw),6000000)
 def test_duplicate_or_incomplete_protocol_withholds_combined_result(self):
  def duplicate(*a,observe,**k):observe(0,0,b'a',b'b');observe(0,0,b'a',b'b')
  with self.ports(duplicate),self.assertRaises(ValueError):native._queries(self.config,self.driver,self.pack,self.aliases)
 def test_complete_native_capture_budget_shared_and_primary_preserved(self):
  primary=KeyboardInterrupt('private');seen=[];original=native.report_body
  def body(v,maximum):
   seen.append(maximum)
   if len(seen)==6:raise primary
   return original(v,maximum)
  with self.ports(),patch.object(native,'report_body',side_effect=body),self.assertRaises(KeyboardInterrupt)as error:native._queries(self.config,self.driver,self.pack,self.aliases)
  self.assertIs(error.exception,primary);self.assertEqual(len(seen),6);self.assertTrue(all(b<a for a,b in zip(seen,seen[1:])))

class LifecycleTests(unittest.TestCase):
 def test_real_outer_composition_all_closing_and_no_report_on_second_group_cancel(self):
  # Actual composition, lifecycle and sinks; native transport/SDK and query ports
  # are inert. These substitutes confer no native or publication qualification.
  import ashlar.weft_paths_distribution as paths_owner
  for fail in (False,'querycancel','closing'):
   with tempfile.TemporaryDirectory()as tmp:
    root=Path(tmp).resolve();path,_=configured(root);cfg=cli.parse_supply_chain_native_invocation(path.read_bytes()).native;events=[];targets=[];primary=KeyboardInterrupt('private')
    def source_closing():
     events.append('source-close')
     if fail:raise OSError('source closing')
    source=SimpleNamespace(batch=SimpleNamespace(batch_id='original'),renew=source_closing,metadata=lambda:{})
    class Spark:
     def sql(self,text):
      if text.startswith('CREATE TABLE delta.'):Path(text.split('`')[1]).mkdir()
      return SimpleNamespace(first=lambda:{'id':'original'})
     def stop(self):events.append('spark-close')
    class Transport:
     def __init__(self,ts):self.targets={t.table:t for t in ts}
     def original_history(self,t):return []
     def close(self):events.append('transport-close')
    class Lease:
     def __init__(self,*a,**k):pass
     def renew(self):events.append('lease-current')
     def close(self):events.append('lease-close')
    class Driver:
     def __init__(self,*a,**k):self.source=a[5];self.publication_id=a[6]
     def prepare_original(self,r):pass
     def backend(self):return object()
     def restore_committed(self,r):events.append('restored');self.restored=True
    driver=[]
    def queries(config,selected,pack,aliases):
     self.assertTrue(selected.restored);driver.append(selected);events.append('five+twelve')
     if fail=='querycancel':raise primary
     return {'cases':[{}for _ in range(5)],'two_hop':{'queries':[{}for _ in range(12)]}}
    def init(*a,**k):targets.extend(a[3]);return Transport(a[3])
    def bundle(label):
     def call(*a):events.append(label);return {'schema':b'original'}
     return call
    descriptor=SimpleNamespace(versions={},raw={})
    patches=[patch.object(native,'_runtime',return_value=[]),patch.object(native,'_spark',return_value=Spark()),patch.object(native,'snapshot',return_value=b'original'),patch.object(native,'supply_chain_cases'),patch.object(native,'PrivateFiniteLease',Lease),patch.object(native,'HeldSupplyChainDataset',return_value=SimpleNamespace(metadata=lambda:{})),patch.object(native,'FiniteSupplyChainSource',return_value=source),patch.object(native,'fixture_columns',return_value={r:(('id','STRING'),)for r in native.GRAPH_ROLES}),patch.object(native,'_request',return_value={'stream':'original','predecessor':'origin','schema_revisions_json':'{}'}),patch.object(native.LocalDeltaTransport,'initialize',side_effect=init),patch.object(native.ReadOnlyTransport,'open',side_effect=lambda *a,**k:Transport(targets)),patch.object(native,'FiniteSupplyChainDriver',Driver),patch.object(native,'publish_batch',return_value=descriptor),patch.object(native,'_queries',side_effect=queries),patch.object(native,'installed_count_star_schema_bundle',side_effect=bundle('count-installation')),patch.object(paths_owner,'installed_paths_schema_bundle',side_effect=bundle('paths-installation'))]
    with contextlib.ExitStack()as stack:
     for p in patches:stack.enter_context(p)
     if fail:
      with self.assertRaises(KeyboardInterrupt if fail=='querycancel' else OSError)as caught:native.publish_query_supply_chain_native(cfg)
      if fail=='querycancel':self.assertIs(caught.exception,primary)
      self.assertFalse((cfg.output/'report.json').exists())
     else:
      answer=native.publish_query_supply_chain_native(cfg);self.assertEqual(answer['two_hop_queries'],12);self.assertEqual(len(json.loads((cfg.output/'report.json').read_bytes())['cases']),5)
    self.assertEqual(len(driver),1);self.assertIn('source-close',events);self.assertIn('spark-close',events);self.assertIn('lease-close',events)
    self.assertEqual(events.count('paths-installation'),2);self.assertEqual(events.count('count-installation'),2)
