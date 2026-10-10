"""Original pack correspondence and inert producer ports; no native qualification."""
import hashlib,json,tempfile,unittest
from pathlib import Path
from unittest import mock
from ashlar.staging import batch_row
from ashlar_host import finite_dataset as dataset
from ashlar_host.finite_pack import FinitePackDefinition,UMF_REVISION
from ashlar_host.delta_custody import encoded

ROOT=Path(__file__).resolve().parents[1]
class Policy:
 def __init__(self):self.calls=0;self.failure=None
 def admit_source(self,facts,context):
  self.calls+=1
  if self.failure is not None:raise self.failure

class Tests(unittest.TestCase):
 def setUp(self):self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name).resolve()
 def inputs(self,name):
  p=ROOT/'examples/domain-packs'/name/'upstream';return (p/'ontology.json').read_bytes(),(p/'graph/fixture.json').read_bytes(),(p/'pack.json').read_bytes()
 def source(self,name):
  definition=FinitePackDefinition(name);model,graph,pack=self.inputs(name);m=self.root/(name+'.model');g=self.root/(name+'.graph');m.write_bytes(model);g.write_bytes(graph);output=self.root/(name+'.receipt')
  authored=json.loads(graph);facts=definition.facts()
  receipt={'profile':'ashlar-finite-pack-public-dataset/0.1','pack':name,'umfRevision':UMF_REVISION,'sourceSha256':facts['model_sha256'],'graphSha256':facts['graph_sha256'],'receipt':{'scope':'supplied-dataset-only','input':{'scope':{'id':'ashlar-original-'+name+'-fixture','closure':'supplied-dataset-only'}},'datasetValidation':{'valid':True,'complete':True,'diagnostics':[]},'records':[{'instanceId':o['key'],'result':{'validation':{'valid':True}}}for o in authored['objects']],'keys':[{}for o in authored['objects']],'relationships':[{'instanceId':e['key'],'sourceInstanceId':e['source'],'targetInstanceId':e['target']}for e in authored['edges']]}}
  def capture(argv,**kwargs):
   if argv[-2:]==['rev-parse','HEAD']:return (UMF_REVISION+'\n').encode(),b''
   if argv[-2:]==['status','--porcelain']:return b'',b''
   output.write_text(json.dumps(receipt));return b'Inert producer port only',b''
  patch=mock.patch.object(dataset,'capture',side_effect=capture);patch.start();self.addCleanup(patch.stop)
  config=dataset.FiniteDatasetConfig(definition,self.root,Path('/usr/bin/true'),Path('/usr/bin/git'),5,1048576,33554432)
  producer=dataset.HeldFiniteDataset(config,m,g,output);policy=Policy();source=dataset.FinitePackSource(definition,m,g,producer,policy=policy,context=object());return source,policy,pack
 def test_all_original_inventory_versions_cases_and_edge_occurrences(self):
  for name,nodes,edges in (('supply-chain',20,23),('archaeology',39,46),('ecology',40,54)):
   definition=FinitePackDefinition(name);model,graph,pack=self.inputs(name);batch,bindings=definition.build(model,graph)
   self.assertEqual(len(batch.records),nodes+edges);self.assertEqual(len({(e['kind'],e['originalKey'])for e in bindings['entities']}),nodes+edges)
   self.assertEqual(len(definition.cases(model,graph,pack)),len(definition.facts()['case_ids']));self.assertEqual(definition.facts()['umf_version'],'0.8.0')
   oracle=definition.oracle(model,graph);self.assertEqual(len(oracle['edges']),edges)
 def test_definition_rejects_subclasses_unknown_and_changed_bytes(self):
  class Forged(str):
   def __eq__(self,other):raise AssertionError('Executable equality')
  for value in (Forged('ecology'),'unknown',True):
   with self.assertRaises(ValueError):FinitePackDefinition(value)
  definition=FinitePackDefinition('ecology');m,g,p=self.inputs('ecology')
  with self.assertRaises(ValueError):definition.inputs(m+b' ',g)
  facts=definition.facts();facts['case_ids'].clear();self.assertEqual(len(definition.facts()['case_ids']),9)
 def test_lexical_numbers_nulls_endpoint_identity_and_history_inputs(self):
  source,policy,_=self.source('ecology');graph=json.loads(source.graph)
  nulls=sum(v is None for o in graph['objects']for v in o['values'].values());self.assertEqual(nulls,7)
  props='\n'.join(c.state.props_json for c in source.changes if c.state.key.kind=='object');self.assertIn('1E+1',props);self.assertIn(':0.1',props);self.assertIn(':null',props);self.assertIn(':0',props)
  for change in source.changes:source.admit(change)
  edges=[c for c in source.changes if c.state.key.kind=='edge'];self.assertEqual(len(edges),54);self.assertTrue(all(len(c.state.endpoints)==2 for c in edges));self.assertGreater(policy.calls,2)
  by_carrier={(c.state.key.kind,str(c.state.key.type_id),str(c.state.key.id)):c.state for c in source.changes}
  original={('object',o['key']):o for o in graph['objects']};original.update({('edge',e['key']):e for e in graph['edges']})
  property_ids={identity['identity'][-1]:identity['property_id']for identity in source.bindings['properties']}
  objects={b['originalKey']:(int(b['type_id']),int(b['id']))for b in source.bindings['entities']if b['kind']=='object'}
  for binding in source.bindings['entities']:
   authored=original[(binding['kind'],binding['originalKey'])];state=by_carrier[(binding['kind'],binding['type_id'],binding['id'])]
   self.assertEqual(json.loads(state.retained_json)['original'],authored)
   if binding['kind']=='object':self.assertEqual(json.loads(state.props_json,parse_int=str,parse_float=str),{property_ids[k]:v for k,v in authored['values'].items()})
   else:self.assertEqual([(e.type_id,e.id)for e in state.endpoints],[objects[authored[end]]for end in ('source','target')])
 def test_owned_producer_request_and_forged_carrier_refusal_before_authority(self):
  source,policy,_=self.source('archaeology');row=batch_row(source.batch);facts=source.definition.facts();r={'stream':'finite','batch_id':source.batch.batch_id,'predecessor':'origin','schema_revisions_json':encoded({facts['source_system']:facts['model_sha256']}),'source_batch_json':row['batch_json'],'source_batch_digest':row['batch_digest']};r['request_digest']=hashlib.sha256(encoded(r).encode()).hexdigest();source.admit_request(r)
  class Forged(str):
   def __ne__(self,other):return False
  r['source_batch_json']=Forged('{}');r['request_digest']=hashlib.sha256(encoded({k:v for k,v in r.items()if k!='request_digest'}).encode()).hexdigest();before=policy.calls
  with self.assertRaises(ValueError):source.admit_request(r)
  self.assertEqual(policy.calls,before)
  class Fake:
   def __eq__(self,other):return True
  with self.assertRaises(ValueError):source.admit(Fake())
 def test_current_authority_required_and_cancellation_identity(self):
  source,policy,_=self.source('supply-chain');failure=KeyboardInterrupt('current authority');policy.failure=failure
  with self.assertRaises(KeyboardInterrupt)as caught:source.metadata()
  self.assertIs(caught.exception,failure)
  with self.assertRaises(ValueError):dataset.FinitePackSource(source.definition,*source.paths,source.producer,policy=object(),context=object())
 def test_replaced_original_file_and_changed_receipt_refuse(self):
  source,_,_=self.source('archaeology');p=source.paths[0];replacement=self.root/'replacement';replacement.write_bytes(p.read_bytes());replacement.replace(p)
  with self.assertRaises(ValueError):source.metadata()
  source,_,_=self.source('ecology');source.producer.output.write_bytes(b'{}')
  with self.assertRaises(ValueError):source.metadata()
 def test_mismatched_producer_pack_and_unknown_bounds_refuse(self):
  source,_,_=self.source('supply-chain')
  with self.assertRaises(ValueError):dataset.FinitePackSource(FinitePackDefinition('ecology'),*source.paths,source.producer,policy=Policy(),context=object())
  with self.assertRaises(ValueError):dataset.FiniteDatasetConfig(source.definition,self.root,Path('/usr/bin/true'),Path('/usr/bin/git'),True,1048576,33554432)
 def test_consistently_replaced_owned_producer_receipt_cannot_replace_admission(self):
  source,_,_=self.source('ecology');changed=json.loads(source.producer.raw);changed['changed']='separate receipt';raw=json.dumps(changed).encode();source.producer.output.write_bytes(raw);source.producer.raw=raw
  source.producer.renew()
  with self.assertRaises(ValueError):source.metadata()
 def test_inconsistent_record_validation_refuses_source_construction(self):
  source,_,_=self.source('supply-chain');changed=json.loads(source.producer.raw);changed['receipt']['records'][0]['result']['validation']['valid']=False;raw=json.dumps(changed).encode();source.producer.output.write_bytes(raw);source.producer.raw=raw
  with self.assertRaises(ValueError):dataset.FinitePackSource(source.definition,*source.paths,source.producer,policy=Policy(),context=object())
 def test_installed_oracle_compatibility_and_complete_scenario_bags(self):
  import archaeology_graph_oracle,ecology_graph_oracle
  for name,shim in (('archaeology',archaeology_graph_oracle),('ecology',ecology_graph_oracle)):
   m,g,p=self.inputs(name);expected=FinitePackDefinition(name).oracle(m,g);self.assertEqual(expected,shim.original_oracle(m,g));self.assertEqual(set(expected['scenarios']),set(c[0]for c in FinitePackDefinition(name).cases(m,g,p)))

if __name__=='__main__':unittest.main()
