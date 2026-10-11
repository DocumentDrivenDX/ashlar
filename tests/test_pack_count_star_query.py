import json,unittest
from unittest.mock import patch
from ashlar_host.pack_count_star_query import finite_pack_public_source,query_pack_count_star_cases
from ashlar_host.finite_dataset import FinitePackSource
from ashlar_host.pack_count_star_oracle import pack_count_star_result_oracle
from test_pack_count_star_reader import fixture,ROOT
class QueryTests(unittest.TestCase):
 def source(self,name):
  source=fixture(name).source;source.batch,source.bindings=source.definition.build(source.model,source.graph);source.raw=b'{"original":true}';return source
 def projection(self,source):
  model=json.loads(source.model);graph=json.loads(source.graph);module=model['modules'][0];record=next(e for e in module['elements']if e['kind']=='record');field=next(e for e in module['elements']if e['kind']=='field'and any(m=={'module':module['id'],'element':e['id']}for m in record['members']))
  fields={(m['id'],e['id']):e for m in model['modules']for e in m['elements']};properties={tuple(p['identity']):p['property_id']for p in source.bindings['properties']};entities={(e['kind'],e['originalKey']):e for e in source.bindings['entities']};facts=source.definition.facts()
  rows=[]
  for obj in graph['objects']:
   if obj['type']!={'document':model['id'],'module':module['id'],'element':record['id']}:continue
   props=[]
   for ref in record['members']:
    token=obj['values'][ref['element']];kind=fields[(ref['module'],ref['element'])]['scalarType'];props.append(json.dumps(properties[(model['id'],ref['module'],ref['element'])])+':'+('null'if token is None else json.dumps(token,ensure_ascii=False,separators=(',',':'))if kind=='string'else token))
   entity=entities[('object',obj['key'])];rows.append({'source_system':facts['source_system'],'type_id':entity['type_id'],'id':entity['id'],'schema_revision':facts['model_sha256'],'props_json':'{'+','.join(props)+'}','extracted_token':obj['values'][field['id']]})
  identity=lambda element:{'documentId':model['id'],'revision':facts['model_sha256'],'module':module['id'],'element':element}
  check={'record':identity(record['id']),'field':identity(field['id']),'publicSourceOnly':True,'propertyId':properties[(model['id'],module['id'],field['id'])],'logicalType':{'family':field['scalarType'],'facets':field.get('facets',{}),'nullable':field['nullability']!='required'}}
  return {'check':check,'rows':rows}
 def test_complete_original17_oracles_and_raw_graph_unchanged(self):
  count=0
  for name in ('archaeology','ecology'):
   s=self.source(name);graph=s.graph;model=s.model;pack=(ROOT/'examples/domain-packs'/name/'upstream/pack.json').read_bytes()
   for case,_ in s.definition.cases(model,graph,pack):self.assertIsInstance(pack_count_star_result_oracle(s.definition,case,model,graph)['rows'],list);count+=1
   self.assertEqual(s.graph,graph);self.assertEqual(s.model,model)
  self.assertEqual(count,17)
 def test_full_field_bag_and_duplicate_or_missing_refusal(self):
  for name in ('archaeology','ecology'):
   s=self.source(name);projection=self.projection(s);request={'modules':[{'documentJson':s.model.decode()}]};artifact={'modelPins':[],'bindingSha256':'test'}
   with patch.object(FinitePackSource,'metadata',return_value={}):
    validate=finite_pack_public_source(s,1048576);self.assertIn('receiptSha256',validate(request,artifact,[projection]))
    self.assertTrue(projection['rows']);projection['rows'].append(projection['rows'][0])
    with self.assertRaises(ValueError):validate(request,artifact,[projection])
 def test_closing_source_failure_withholds_receipt(self):
  s=self.source('ecology');projection=self.projection(s)
  with patch.object(FinitePackSource,'metadata',side_effect=[{}, {}, ValueError('closing source')]):
   validate=finite_pack_public_source(s,1048576)
   with self.assertRaises(ValueError):validate({'modules':[{'documentJson':s.model.decode()}]},{'modelPins':[],'bindingSha256':'x'},[projection])
 def test_borrowed_binding_mutation_after_snapshot_refused(self):
  s=self.source('archaeology');projection=self.projection(s)
  with patch.object(FinitePackSource,'metadata',return_value={}):
   validate=finite_pack_public_source(s,1048576)
   s.bindings['properties'][0]['property_id']='forged'
   with self.assertRaises(ValueError):validate({'modules':[{'documentJson':s.model.decode()}]},{'modelPins':[],'bindingSha256':'x'},[projection])
 def test_executable_projection_carrier_refused(self):
  class Spoof(dict):
   def __eq__(self,other):return True
  s=self.source('ecology');projection=self.projection(s);projection['rows'][0]=Spoof(projection['rows'][0])
  with patch.object(FinitePackSource,'metadata',return_value={}):
   validate=finite_pack_public_source(s,1048576)
   with self.assertRaises(ValueError):validate({'modules':[{'documentJson':s.model.decode()}]},{'modelPins':[],'bindingSha256':'x'},[projection])
 def test_public_bounds_and_untyped_query_refused(self):
  for maximum in (True,-1,16777217):
   with self.assertRaises(ValueError):finite_pack_public_source(self.source('ecology'),maximum)
  with self.assertRaises(ValueError):query_pack_count_star_cases(object(),b'',{},object(),object(),observe=lambda *args:None)

class OrchestrationTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  import tarfile
  cls.compiled={}
  with tarfile.open(ROOT/'docs/helix/04-build/evidence/pack-installed-compiler-20261010/actual.tar.gz','r:gz')as archive:
   for pack in ('archaeology','ecology'):
    definition=fixture(pack).source.definition;rows=[]
    for name in definition.facts()['case_ids']:
     request=archive.extractfile('actual/compile-output/'+pack+'-'+name+'-0.request').read();response=archive.extractfile('actual/compile-output/'+pack+'-'+name+'-0.response').read()
     rows.append({'id':name,'request':json.loads(request),'response':json.loads(response),'requestBytes':request,'responseBytes':response})
    cls.compiled[pack]={'cases':rows}
 def setup(self,pack):
  from pathlib import Path
  from types import SimpleNamespace
  from ashlar.weft_count_star_distribution import CountStarDistributionPaths
  from ashlar.weft_path_decode import PathDecodeConfig
  from ashlar_host.pack_count_star_compile import PackCountStarCompileConfig
  from ashlar_host.count_star_execution import CountStarExecutionConfig
  from ashlar_host.count_star_admission import CountStarAdmissionConfig,CountStarSchemaValidation,SCHEMA_SHA256
  from ashlar_host.path_capture import PathCaptureConfig
  from ashlar_host.finite_pack import UMF_REVISION
  driver=fixture(pack);source=driver.source;source.batch,source.bindings=source.definition.build(source.model,source.graph);source.raw=b'original receipt'
  driver.transport=SimpleNamespace(targets={t:SimpleNamespace(table=t,uuid='fixture-uuid')for t in driver.tables.values()})
  compile_config=PackCountStarCompileConfig(CountStarDistributionPaths(Path('/fixture/index.json'),Path('/fixture/installation')),1048576)
  execution=CountStarExecutionConfig(CountStarAdmissionConfig(1048576,CountStarSchemaValidation(__import__('test_count_star_admission').SCHEMAS,lambda *args:None)),PathCaptureConfig(100,10000,100000),PathDecodeConfig(10000),lambda *args:None,UMF_REVISION)
  packbytes=(ROOT/'examples/domain-packs'/pack/'upstream/pack.json').read_bytes()
  return driver,packbytes,{t:t for t in driver.tables.values()},compile_config,execution
 def test_all_original17_dispatch_exact_artifacts_oracles_and_observations(self):
  from ashlar_host import pack_count_star_query as selected
  seen=[];native=[]
  for pack in ('archaeology','ecology'):
   args=self.setup(pack);driver=args[0];cases=self.compiled[pack]['cases'];seen_case=[]
   def execute(opened,request,response,recompiled,*,config,original_oracle):
    case=cases[len(seen_case)];self.assertEqual(request,case['request']);self.assertEqual(response,case['response']);self.assertEqual(recompiled,case['response'])
    self.assertEqual(original_oracle(opened.model,opened.graph,request),pack_count_star_result_oracle(driver.source.definition,case['id'],opened.model,opened.graph))
    self.assertEqual(opened.native_files()['source_transaction'],'actual-owning-transaction');self.assertNotEqual(opened.native_files()['source_transaction'],opened.native_files()['finite_source_receipt'])
    config.native_observer({'rows':[]});seen_case.append(case['id']);return {'inert':case['id']}
   with patch.object(FinitePackSource,'metadata',return_value={'source_transaction_sha256':'actual-owning-transaction'}),patch.object(selected,'compile_pack_count_star_cases',return_value=self.compiled[pack]),patch.object(selected,'FinitePackProvider'),patch.object(selected,'execute_commerce_count_star',side_effect=execute):
    result=selected.query_pack_count_star_cases(*args,observe=lambda *args:None,observe_native=lambda name,raw:native.append(name))
   self.assertEqual([r['id']for r in result['cases']],driver.source.definition.facts()['case_ids']);seen.extend(seen_case)
  self.assertEqual(len(seen),17);self.assertEqual(native,seen)
 def test_execution_failure_stops_later_cases_and_withholds_result(self):
  from ashlar_host import pack_count_star_query as selected
  args=self.setup('ecology');failure=KeyboardInterrupt();calls=[]
  def execute(*args,**kwargs):calls.append(1);raise failure
  with patch.object(FinitePackSource,'metadata',return_value={'source_transaction_sha256':'transaction'}),patch.object(selected,'compile_pack_count_star_cases',return_value=self.compiled['ecology']),patch.object(selected,'FinitePackProvider'),patch.object(selected,'execute_commerce_count_star',side_effect=execute):
   with self.assertRaises(KeyboardInterrupt)as caught:selected.query_pack_count_star_cases(*args,observe=lambda *args:None)
  self.assertIs(caught.exception,failure);self.assertEqual(len(calls),1)
 def test_closing_source_renewal_refuses_complete_result(self):
  from ashlar_host import pack_count_star_query as selected
  args=self.setup('ecology');calls=[]
  def renew(source):
   calls.append(1)
   if len(calls)==2:raise ValueError('closing original source changed')
  with patch.object(FinitePackSource,'metadata',return_value={'source_transaction_sha256':'transaction'}),patch.object(FinitePackSource,'renew',renew),patch.object(selected,'compile_pack_count_star_cases',return_value=self.compiled['ecology']),patch.object(selected,'FinitePackProvider'),patch.object(selected,'execute_commerce_count_star',return_value={'inert':True}):
   with self.assertRaises(ValueError):selected.query_pack_count_star_cases(*args,observe=lambda *args:None)
  self.assertEqual(len(calls),2)
 def test_definition_substitution_before_operation_refused(self):
  from ashlar_host import pack_count_star_query as selected
  args=self.setup('ecology');called=[]
  class Forged:
   def inputs(self,*args):called.append(1);raise AssertionError('must not run')
  args[0].source.definition=Forged()
  with self.assertRaises(ValueError):selected.query_pack_count_star_cases(*args,observe=lambda *args:None)
  with self.assertRaises(ValueError):finite_pack_public_source(args[0].source,1048576)
  self.assertEqual(called,[])
 def test_unknown_compiled_capability_refuses_before_native_interval(self):
  import copy
  from ashlar_host import pack_count_star_query as selected
  from ashlar_host.count_star_admission import CountStarPlanError
  args=self.setup('ecology');compiled=copy.deepcopy(self.compiled['ecology']);compiled['cases'][0]['response']['logicalPlan']['requiredCapabilities'].append('unknown.original-capability')
  with patch.object(FinitePackSource,'metadata',return_value={'source_transaction_sha256':'transaction'}),patch.object(selected,'compile_pack_count_star_cases',return_value=compiled),patch.object(selected,'FinitePackProvider')as provider:
   with self.assertRaises(CountStarPlanError):selected.query_pack_count_star_cases(*args,observe=lambda *args:None)
  provider.return_value.interval.assert_not_called();provider.return_value.runtime.assert_not_called()
