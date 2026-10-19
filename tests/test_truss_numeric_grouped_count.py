# SPDX-License-Identifier: MIT OR Apache-2.0
"""Small exact grouping source/IR ports; no engine execution."""
import copy,hashlib,json,unittest
from unittest.mock import patch
import test_truss_scalar_projection as scalars
from ashlar_host.delta_custody import encoded
from ashlar_host.truss_numeric_grouped_count import admit_truss_numeric_grouped_count

class Tests(unittest.TestCase):
 def fixture(self,decimal=False):
  _,artifact,admission,request,manifest,expected=scalars.Tests().fixture()
  if decimal:
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
   binding=json.loads(request['target']['bindingJson']);repin(binding);request['target']['bindingJson']=encoded(binding);repin(artifact)
   logical={'family':'decimal','facets':{'precision':18,'scale':4},'nullable':False};artifact['columns'][0]['representation'].update(logicalType=logical,decoder='exact-decimal');artifact['logicalPlan']['typeGraph'][0]['type']=logical
   expected['object_current'][0]['props_json']='{"3":1.23000}';expected['object_current'][1]['props_json']='{"3":1.23}'
  binding=json.loads(request['target']['bindingJson'])
  for record in binding['records']:
   for prop in record['properties']:prop['home']['kind']='props'
  request['target']['bindingJson']=encoded(binding);request['target']['bindingSha256']=hashlib.sha256(encoded(binding).encode()).hexdigest();artifact['bindingSha256']=request['target']['bindingSha256']
  admission.revisions['2']['types']['2']['required']={'3'}
  plan=artifact['logicalPlan'];identity=artifact['columns'][0]['sourceIdentities'][0];logical=artifact['columns'][0]['representation']['logicalType'];integer={'family':'integer','facets':{},'nullable':False}
  plan.update(aggregate=True,groups=[{'identity':identity,'scan':'s0','span':{'start':1,'end':2},'type':logical}],requiredCapabilities=['aggregate','aggregate.count','group','project','scan','type.'+logical['family']]);plan['outputs'].append({'name':'total','expression':{'op':'count','type':integer}})
  artifact['columns'].append({'nullable':False,'outputName':'total','position':2,'representation':{'kind':'scalar','logicalType':integer,'carrier':'text','decoder':'exact-integer'},'sourceIdentities':[plan['source']['record']]})
  return artifact,admission,request,manifest,expected
 def admit(self,args):
  artifact,admission,request,manifest,expected=args
  with patch('ashlar_host.truss_numeric_grouped_count.truss_compatible_compile_request',return_value=request),patch('ashlar_host.truss_numeric_grouped_count.admit_truss_compatible_revisions',return_value={'physical_revisions':['1','2']}):return admit_truss_numeric_grouped_count(admission,artifact,request,manifest,expected,revision='2',document_id='cohort',physical_revisions=('1','2'))
 def test_exact_large_integer_groups_and_empty_bag(self):
  args=self.fixture();args[-1]['object_current'].append(copy.deepcopy(args[-1]['object_current'][0]));projection=self.admit(args);projection.require_complete_bag([{'label':{'integer_token':'9007199254740993'},'total':2},{'label':{'integer_token':'42'},'total':1}]);self.assertEqual(projection.decode(args[0]['columns'][1],'2'),2)
  with self.assertRaisesRegex(ValueError,'complete original'):projection.require_complete_bag([{'label':{'integer_token':'42'},'total':1}])
  args[-1]['object_current']=[];self.admit(args).require_complete_bag([])
 def test_decimal_equivalent_spellings_share_one_exact_group(self):
  args=self.fixture(decimal=True);projection=self.admit(args);projection.require_complete_bag([{'label':{'decimal_token':'1.2300'},'total':2}]);self.assertEqual(projection.decode(args[0]['columns'][0],'1.2300'),{'decimal_token':'1.2300'})
  with self.assertRaisesRegex(ValueError,'complete original'):projection.require_complete_bag([{'label':{'decimal_token':'1.2301'},'total':2}])
 def test_unknown_aggregate_or_group_descriptor_refuses(self):
  for mutation in ('aggregate','descriptor'):
   args=self.fixture()
   if mutation=='aggregate':args[0]['logicalPlan']['outputs'][1]['expression']['op']='sum'
   else:args[0]['logicalPlan']['groups'][0]['type']['facets']={'integerWidth':{'bits':32,'signed':True}}
   with self.assertRaises(ValueError):self.admit(args)
 def test_original_guard_then_nominal_mixed_numeric_count_decode(self):
  from types import SimpleNamespace
  from ashlar_host.truss_weft_query import _execute_string_artifact
  args=self.fixture();projection=self.admit(args);artifact=args[0]
  artifact.update(parameters=[],sql='SELECT original_grouped_count')
  artifact['obligations']=[{'id':'ashlar.candidate.scalarIntegrity','parameters':{'checks':[{'sql':'SELECT original_guard','failureCode':'WFT-NUMERIC-DOMAIN','field':artifact['columns'][0]['sourceIdentities'][0],'record':artifact['logicalPlan']['source']['record']}]}}]
  calls=[]
  def capture(frame,*,config):
   calls.append(frame)
   return {'schema':[['violations','STRING']],'rows':[['0']]}if frame=='SELECT original_guard'else{'schema':[['label','STRING'],['total','STRING']],'rows':[['9007199254740993','1'],['42','1']]}
  with patch('ashlar_host.path_capture.capture_string_frame',side_effect=capture):
   rows,checks=_execute_string_artifact(SimpleNamespace(sql=lambda sql,args:sql),artifact,2,grouped=True,value_projection=projection)
  projection.require_complete_bag(rows);self.assertEqual(calls,['SELECT original_guard','SELECT original_grouped_count']);self.assertEqual(len(checks),1);self.assertIs(type(rows[0]['total']),int)
