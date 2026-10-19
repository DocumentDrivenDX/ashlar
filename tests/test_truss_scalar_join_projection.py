# SPDX-License-Identifier: MIT OR Apache-2.0
import copy,hashlib,json,unittest
from unittest.mock import patch
import test_truss_scalar_projection as scalars
import test_truss_compatible_binding as compatibility
from ashlar_host.truss_scalar_join_projection import admit_truss_scalar_join_projection
from ashlar_host.truss_compatible_binding import admit_truss_compatible_join_revisions,admit_truss_compatible_revisions
from ashlar_host.delta_custody import encoded

class Tests(unittest.TestCase):
 def fixture(self):
  _,artifact,admission,request,manifest,expected=scalars.Tests().fixture()
  source=artifact['logicalPlan']['source'];identity=artifact['columns'][0]['sourceIdentities'][0];logical=artifact['columns'][0]['representation']['logicalType']
  field=lambda scan:{'identity':identity,'scan':scan,'span':{'start':1,'end':2},'type':logical}
  artifact['logicalPlan']['joins']=[{'right':{**source,'occurrence':'s1'},'on':[{'op':'equal','left':field('s0'),'right':{'kind':'field','field':field('s1')}}]}]
  artifact['logicalPlan']['requiredCapabilities']=['equal','innerJoin','project','scan','type.integer'];artifact['logicalPlan']['outputs'].append({'name':'other','expression':{'op':'field','scan':'s1','identity':identity}})
  artifact['columns'].append({**copy.deepcopy(artifact['columns'][0]),'outputName':'other','position':2})
  admission.revisions['2']['types']['2']['required']={'3'}
  binding=json.loads(request['target']['bindingJson'])
  for record in binding['records']:
   for prop in record['properties']:prop['home']['kind']='props'
  request['target']['bindingJson']=encoded(binding)
  expected['object_current'].append(copy.deepcopy(expected['object_current'][0]))
  return artifact,admission,request,manifest,expected
 def admit(self,artifact,admission,request,manifest,expected):
  with patch('ashlar_host.truss_scalar_join_projection.truss_compatible_compile_request',return_value=request),patch('ashlar_host.truss_scalar_join_projection.admit_truss_compatible_join_revisions',return_value={'physical_revisions':['1','2']}):return admit_truss_scalar_join_projection(admission,artifact,request,manifest,expected,revision='2',document_id='cohort',physical_revisions=('1','2'))
 def test_large_exact_integer_selfjoin_retains_duplicate_pairs(self):
  args=self.fixture();projection=self.admit(*args);large={'integer_token':'9007199254740993'};small={'integer_token':'42'}
  rows=[{'label':large,'other':large}for _ in range(4)]+[{'label':small,'other':small}];projection.require_complete_bag(rows)
  with self.assertRaisesRegex(ValueError,'complete original'):projection.require_complete_bag(rows[:-1])
 def test_changed_domain_and_extra_predicate_refuse(self):
  for mutation in ('domain','predicate'):
   artifact,admission,request,manifest,expected=self.fixture()
   if mutation=='domain':artifact['logicalPlan']['joins'][0]['on'][0]['right']['field']['type']={'family':'decimal','facets':{'precision':18,'scale':4},'nullable':False}
   else:artifact['logicalPlan']['joins'][0]['on'].append(copy.deepcopy(artifact['logicalPlan']['joins'][0]['on'][0]))
   with self.assertRaises(ValueError):self.admit(artifact,admission,request,manifest,expected)
 def test_schema_obligation_selfjoin_deduplicates_record_legacy_stays_closed(self):
  cases=compatibility.Tests();admission,manifest,uuids=cases.fixture();request=cases.request(admission,manifest,uuids);binding=json.loads(request['target']['bindingJson']);artifact=cases.artifact(binding);source=artifact['logicalPlan']['source'];artifact['logicalPlan']['joins']=[{'right':{**source,'occurrence':'s1'},'on':[]}]
  self.assertEqual(admit_truss_compatible_join_revisions(admission,artifact,binding,revision='2',document_id='cohort')['physical_revisions'],['1','2'])
  with self.assertRaises(ValueError):admit_truss_compatible_revisions(admission,artifact,binding,revision='2',document_id='cohort')

 def test_schema_obligation_two_distinct_records_and_missing_proof_refuses(self):
  cases=compatibility.Tests();admission,manifest,uuids=cases.fixture();request=cases.request(admission,manifest,uuids);binding=json.loads(request['target']['bindingJson']);artifact=cases.artifact(binding)
  artifact['logicalPlan']['joins']=[{'right':{'occurrence':'s1','pin':binding['modelPins'][0],'record':binding['records'][1]['logical']},'on':[]}]
  artifact['obligations'][0]['parameters']['records'].append({'logical':binding['records'][1]['logical'],'primarySchemaRevision':'2','compatibleSchemaRevisions':['1','2']})
  self.assertEqual(admit_truss_compatible_join_revisions(admission,artifact,binding,revision='2',document_id='cohort')['physical_revisions'],['1','2'])
  artifact['obligations'][0]['parameters']['records'].pop()
  with self.assertRaisesRegex(ValueError,'obligation'):admit_truss_compatible_join_revisions(admission,artifact,binding,revision='2',document_id='cohort')

 def test_decimal_equality_equivalent_spellings_keeps_pair_multiplicity(self):
  artifact,admission,request,manifest,expected=self.fixture()
  model=copy.deepcopy(admission.revisions['2']['models']['cohort'][0])
  for module in model['modules']:
   for field in module['elements']:
    if field['id']=='B-label':field.update(scalarType='decimal',facets={'precision':18,'scale':4})
  raw=encoded(model).encode();admission.revisions['2']['models']['cohort']=(model,raw);digest=hashlib.sha256(raw).hexdigest()
  def repin(value):
   if type(value)is dict:
    if 'revision'in value:value['revision']=digest
    if 'sha256'in value:value['sha256']=digest
    for v in value.values():repin(v)
   elif type(value)is list:
    for v in value:repin(v)
  binding=json.loads(request['target']['bindingJson']);repin(binding);request['target']['bindingJson']=encoded(binding);request['target']['bindingSha256']=hashlib.sha256(encoded(binding).encode()).hexdigest();repin(artifact);artifact['bindingSha256']=request['target']['bindingSha256']
  logical={'family':'decimal','facets':{'precision':18,'scale':4},'nullable':False}
  for column in artifact['columns']:column['representation'].update(logicalType=logical,decoder='exact-decimal')
  artifact['logicalPlan']['typeGraph'][0]['type']=logical
  predicate=artifact['logicalPlan']['joins'][0]['on'][0];predicate['left']['type']=logical;predicate['right']['field']['type']=logical
  artifact['logicalPlan']['requiredCapabilities']=['equal','innerJoin','project','scan','type.decimal']
  expected['object_current']=expected['object_current'][:2]
  expected['object_current'][0]['props_json']='{"3":1.23000}'
  expected['object_current'][1]['props_json']='{"3":1.23}'
  projection=self.admit(artifact,admission,request,manifest,expected)
  row={'label':{'decimal_token':'1.2300'},'other':{'decimal_token':'1.23'}}
  projection.require_complete_bag([row for _ in range(4)])
  self.assertEqual(projection.decode(artifact['columns'][0],'1.2300'),{'decimal_token':'1.2300'})
  with self.assertRaisesRegex(ValueError,'complete original'):projection.require_complete_bag([row for _ in range(3)])
