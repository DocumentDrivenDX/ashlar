# SPDX-License-Identifier: MIT OR Apache-2.0
import copy,unittest
from unittest.mock import patch
from ashlar_host.truss_scalar_projection import admit_truss_scalar_projection
import test_truss_scalar_projection as original

class Tests(unittest.TestCase):
 def fixture(self,token='9007199254740993',ordered=False):
  _,artifact,admission,request,manifest,expected=original.Tests().fixture()
  request['sql']='SELECT r.label FROM B r WHERE r.label = '+token+(' ORDER BY r.label LIMIT 1'if ordered else '')
  identity=artifact['columns'][0]['sourceIdentities'][0];logical=artifact['columns'][0]['representation']['logicalType'];sql=request['sql']
  left={'identity':identity,'scan':'s0','type':logical,'span':{'start':sql.index('r.label',25),'end':sql.index('r.label',25)+7}}
  right={'kind':'literal','value':token,'type':logical,'span':{'start':sql.index(token),'end':sql.index(token)+len(token)}}
  plan=artifact['logicalPlan'];plan['filters']=[{'op':'equal','left':left,'right':right}];plan['requiredCapabilities']=sorted(['project','scan','type.integer','equal','filter'])
  artifact['parameters']=[{'position':1,'value':token,'logicalType':logical,'origin':{'kind':'literal','sourceSpan':right['span']}}]
  if ordered:
   pos=sql.rindex('r.label');plan['order']=[{**left,'span':{'start':pos,'end':pos+7}}];plan['limit']=1;plan['requiredCapabilities']=sorted(plan['requiredCapabilities']+['order.asc','limit'])
  return artifact,admission,request,manifest,expected
 def admit(self,args,maximum_rows=10000):
  artifact,admission,request,manifest,expected=args
  with patch('ashlar_host.truss_compatible_binding.truss_compatible_compile_request',return_value=request):return admit_truss_scalar_projection(admission,artifact,request,manifest,expected,revision='2',document_id='cohort',physical_revisions=('1','2'),maximum_rows=maximum_rows)
 def test_exact_filter_limit_and_full_bag(self):
  for token,ordered,rows in [('9007199254740993',False,[{'label':{'integer_token':'9007199254740993'}}]),('42',False,[{'label':{'integer_token':'42'}}]),('3',False,[]),('9007199254740993',True,[{'label':{'integer_token':'9007199254740993'}}])]:
   with self.subTest(token=token,ordered=ordered):
    args=self.fixture(token,ordered);projection=self.admit(args);projection.require_complete_bag(rows)
    with self.assertRaises(ValueError):projection.require_complete_bag(rows+[{'label':{'integer_token':'999'}}])
 def test_original_correspondence_and_invalid_full_population_refuse(self):
  for kind in ('literal-origin','literal-span','capability','order-type','boolean-source','unmatched-invalid-source','limit-budget'):
   args=self.fixture(ordered=True);artifact,admission,request,manifest,expected=args
   if kind=='literal-origin':artifact['parameters'][0]['origin']['sourceSpan']={'start':0,'end':1}
   elif kind=='literal-span':artifact['logicalPlan']['filters'][0]['right']['span']={'start':0,'end':1}
   elif kind=='capability':artifact['logicalPlan']['requiredCapabilities'].append('order.desc')
   elif kind=='order-type':artifact['logicalPlan']['order'][0]['type']={'family':'string'}
   elif kind=='boolean-source':expected['object_current'][0]['props_json']='{"3":true}'
   elif kind=='limit-budget':
    request['sql']=request['sql'][:-1]+'2';artifact['logicalPlan']['limit']=2
   else:expected['object_current'][1]['props_json']='{"3":9223372036854775808}'
   with self.subTest(kind=kind),self.assertRaises(ValueError):self.admit(args,maximum_rows=1 if kind=='limit-budget'else 10000)
