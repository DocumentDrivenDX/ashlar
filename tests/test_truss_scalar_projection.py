# SPDX-License-Identifier: MIT OR Apache-2.0
import copy,hashlib,json,unittest
from unittest.mock import patch
import test_truss_value_projection as original
from ashlar_host.delta_custody import encoded
from ashlar_host.truss_scalar_projection import admit_truss_scalar_projection,TrussScalarProjection

class Tests(unittest.TestCase):
 def fixture(self):
  _,_,admission,artifact,request,manifest,expected=original.Tests().fixture()
  model=copy.deepcopy(admission.revisions['2']['models']['cohort'][0])
  for module in model['modules']:
   for field in module['elements']:
    if field['id']=='B-label':field.update(scalarType='integer',facets={'integerWidth':{'bits':64,'signed':True}})
  raw=encoded(model).encode();admission.revisions['2']['models']['cohort']=(model,raw);digest=hashlib.sha256(raw).hexdigest()
  def repin(value):
   if type(value)is dict:
    if 'revision'in value:value['revision']=digest
    if 'sha256'in value:value['sha256']=digest
    for v in value.values():repin(v)
   elif type(value)is list:
    for v in value:repin(v)
  binding=json.loads(request['target']['bindingJson']);repin(binding);request['target']['bindingJson']=encoded(binding);request['target']['bindingSha256']=hashlib.sha256(encoded(binding).encode()).hexdigest()
  repin(artifact);artifact['bindingSha256']=request['target']['bindingSha256'];artifact['columns']=artifact['columns'][:1];plan=artifact['logicalPlan'];plan['outputs']=plan['outputs'][:1];plan['typeGraph']=plan['typeGraph'][:1];plan['requiredCapabilities']=['project','scan','type.integer']
  logical={'family':'integer','facets':{'integerWidth':{'bits':64,'signed':True}},'nullable':False};artifact['columns'][0]['representation']={'kind':'scalar','logicalType':logical,'carrier':'text','decoder':'exact-integer'};plan['typeGraph'][0]['type']=logical
  expected['object_current'][0]['props_json']='{"3":9007199254740993,"5":"tagged"}';expected['object_current'][1]['props_json']='{"3":42}'
  with patch('ashlar_host.truss_compatible_binding.truss_compatible_compile_request',return_value=request):projection=admit_truss_scalar_projection(admission,artifact,request,manifest,expected,revision='2',document_id='cohort',physical_revisions=('1','2'))
  return projection,artifact,admission,request,manifest,expected
 def test_exact_large_integer_and_complete_bag(self):
  projection,artifact,*_=self.fixture();row=projection.decode(artifact['columns'][0],'9007199254740993');self.assertEqual(row,{'integer_token':'9007199254740993'});projection.require_complete_bag([{'label':row},{'label':{'integer_token':'42'}}])
  with self.assertRaisesRegex(ValueError,'complete original'):projection.require_complete_bag([{'label':row}])
 def test_unknown_facets_and_filtered_plan_refuse(self):
  for change in ('facets','filter'):
   _,artifact,admission,request,manifest,expected=self.fixture()
   if change=='filter':artifact['logicalPlan']['filters']=[{'op':'eq'}]
   else:artifact['columns'][0]['representation']['logicalType']['facets']={'integerWidth':{'bits':32,'signed':True}}
   with patch('ashlar_host.truss_compatible_binding.truss_compatible_compile_request',return_value=request),self.assertRaises(ValueError):admit_truss_scalar_projection(admission,artifact,request,manifest,expected,revision='2',document_id='cohort',physical_revisions=('1','2'))

 def test_decimal_bag_compares_exact_value_retaining_carrier(self):
  column={'outputName':'amount','representation':{'kind':'scalar','logicalType':{'family':'decimal','facets':{'precision':18,'scale':4},'nullable':False},'carrier':'text','decoder':'exact-decimal'}}
  projection=TrussScalarProjection(encoded([column]),encoded([{'amount':{'decimal_token':'1.2300'}}]))
  value=projection.decode(column,'1.2300');self.assertEqual(value,{'decimal_token':'1.2300'})
  projection.require_complete_bag([{'amount':{'decimal_token':'1.23'}}])
  with self.assertRaises(ValueError):projection.require_complete_bag([{'amount':{'decimal_token':'1.2301'}}])

 def test_source_decimal_exponents_zeros_and_surplus_fraction_zeros(self):
  for scale in (2,4):
   with self.subTest(scale=scale):self.check_decimal_domain(scale)

 def test_undeclared_or_nonintegral_decimal_scale_refuses(self):
  for scale in (3,2.0,False):
   with self.subTest(scale=scale),self.assertRaises(ValueError):self.check_decimal_domain(scale)

 def check_decimal_domain(self,scale):
  _,artifact,admission,request,manifest,expected=self.fixture()
  model=admission.revisions['2']['models']['cohort'][0]
  for module in model['modules']:
   for field in module['elements']:
    if field['id']=='B-label':field.update(scalarType='decimal',facets={'precision':18,'scale':scale})
  raw=encoded(model).encode();admission.revisions['2']['models']['cohort']=(model,raw);digest=hashlib.sha256(raw).hexdigest()
  def repin(value):
   if type(value)is dict:
    if 'revision'in value:value['revision']=digest
    if 'sha256'in value:value['sha256']=digest
    for v in value.values():repin(v)
   elif type(value)is list:
    for v in value:repin(v)
  binding=json.loads(request['target']['bindingJson']);repin(binding);request['target']['bindingJson']=encoded(binding);request['target']['bindingSha256']=hashlib.sha256(encoded(binding).encode()).hexdigest();repin(artifact);artifact['bindingSha256']=request['target']['bindingSha256']
  logical={'family':'decimal','facets':{'precision':18,'scale':scale},'nullable':False}
  artifact['columns'][0]['representation'].update(logicalType=logical,decoder='exact-decimal');artifact['logicalPlan']['typeGraph'][0]['type']=logical;artifact['logicalPlan']['requiredCapabilities']=['project','scan','type.decimal']
  rows=expected['object_current'];rows[0]['props_json']='{"3":1e2}';rows[1]['props_json']='{"3":1.2300000}';rows.append({**rows[1],'props_json':'{"3":0e999999999999}'})
  with patch('ashlar_host.truss_compatible_binding.truss_compatible_compile_request',return_value=request):projection=admit_truss_scalar_projection(admission,artifact,request,manifest,expected,revision='2',document_id='cohort',physical_revisions=('1','2'))
  projection.require_complete_bag([{'label':{'decimal_token':token}}for token in ('100.0000','1.2300','0.0000')])
  self.assertEqual(projection.decode(artifact['columns'][0],'100.0000'),{'decimal_token':'100.0000'})
