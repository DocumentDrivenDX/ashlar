# SPDX-License-Identifier: MIT OR Apache-2.0
import copy,hashlib,json,unittest
from unittest.mock import patch
from ashlar_host.delta_custody import encoded
from ashlar_host.truss_integer_source_guard import admit_truss_integer_source_guard,SCHEMA
from ashlar_host.truss_scalar_projection import admit_truss_scalar_projection
import test_truss_scalar_projection as original

class Tests(unittest.TestCase):
 def fixture(self):
  _,artifact,admission,request,manifest,expected=original.Tests().fixture()
  model=admission.revisions['2']['models']['cohort'][0];model['umf']='0.8.0'
  for m in model['modules']:
   for e in m['elements']:
    if e['id']=='B-label':e.pop('facets',None)
  raw=encoded(model).encode();digest=hashlib.sha256(raw).hexdigest();admission.revisions['2']['models']['cohort']=(model,raw)
  def repin(v):
   if type(v)is dict:
    for k in ('revision','sha256'):
     if k in v:v[k]=digest
    if 'umfVersion'in v:v['umfVersion']='0.8.0'
    for x in v.values():repin(x)
   elif type(v)is list:
    for x in v:repin(x)
  binding=json.loads(request['target']['bindingJson']);repin(binding);
  for record in binding['records']:
   for prop in record['properties']:prop['home']['kind']='props'
  request['target']['bindingJson']=encoded(binding);request['target']['bindingSha256']=hashlib.sha256(encoded(binding).encode()).hexdigest();repin(artifact);artifact['bindingSha256']=request['target']['bindingSha256']
  for revision in admission.revisions.values():
   for type_info in revision['types'].values():
    if '3'in type_info['fields']:type_info.setdefault('domains',{})['3']=('integer','required');type_info.setdefault('facets',{})['3']={}
  for identity,row in enumerate(expected['object_current'],1):row['id']=str(identity)
  logical={'family':'integer','facets':{},'nullable':False};artifact['columns'][0]['representation']['logicalType']=logical;artifact['logicalPlan']['typeGraph'][0]['type']=logical
  field=artifact['columns'][0]['sourceIdentities'][0];record=artifact['logicalPlan']['source']['record']
  source={'publicSourceOnly':True,'field':field,'record':record,'propertyId':'3','logicalType':logical,'sql':'SELECT original_source','failureCode':'WFT-NUMERIC-DOMAIN'}
  represent={'representabilityOnly':True,'field':field,'record':record,'sql':'SELECT original_capacity','failureCode':'WFT-CAPABILITY'}
  artifact['obligations']=[{'id':'ashlar.candidate.scalarIntegrity','parameters':{'checks':[source,represent]}}]
  return admission,artifact,request,manifest,expected,source,represent
 def test_exact_nominal_complete_source_and_original_integer_descriptor(self):
  admission,artifact,request,manifest,expected,source,_=self.fixture()
  guard=admit_truss_integer_source_guard(admission,artifact,request,expected,revision='2',document_id='cohort',physical_revisions=('1','2'))
  rows=json.loads(guard.entries_json)[0]['rows'];guard.require_source(source,{'schema':SCHEMA,'rows':rows});self.assertEqual(guard.source_budget(source),2)
  self.assertEqual(rows[0][-1],'9007199254740993')
  with patch('ashlar_host.truss_compatible_binding.truss_compatible_compile_request',return_value=request):projection=admit_truss_scalar_projection(admission,artifact,request,manifest,expected,revision='2',document_id='cohort',physical_revisions=('1','2'))
  projection.require_complete_bag([{'label':{'integer_token':'9007199254740993'}},{'label':{'integer_token':'42'}}])
  for changed in (rows[:1],rows+[rows[0]],[rows[0][:-1]+['9007199254740992'],rows[1]]):
   with self.assertRaises(ValueError):guard.require_source(source,{'schema':SCHEMA,'rows':changed})
 def test_inventory_and_original_atom_refuse(self):
  for kind in ('missing-pair','missing-both','missing-output','wrong-home','unknown-member','boolean','exponent','duplicate','nonfinite'):
   admission,artifact,request,manifest,expected,source,_=self.fixture()
   if kind=='missing-pair':artifact['obligations'][0]['parameters']['checks'].pop()
   elif kind=='missing-both':artifact['obligations'][0]['parameters']['checks']=[]
   elif kind=='missing-output':
    extra=copy.deepcopy(artifact['columns'][0]);extra['sourceIdentities'][0]['element']='B-tag';artifact['columns'].append(extra)
   elif kind=='duplicate':expected['object_current'][0]['props_json']='{"3":1,"3":2}'
   elif kind=='nonfinite':expected['object_current'][0]['props_json']='{"3":NaN}'
   elif kind=='wrong-home':source['propertyId']='5'
   elif kind=='unknown-member':source['unknown']=True
   elif kind=='boolean':expected['object_current'][0]['props_json']='{"3":true}'
   else:expected['object_current'][0]['props_json']='{"3":1e2}'
   with self.subTest(kind=kind),self.assertRaises(ValueError):admit_truss_integer_source_guard(admission,artifact,request,expected,revision='2',document_id='cohort',physical_revisions=('1','2'))
   if kind in ('duplicate','nonfinite'):
    with patch('ashlar_host.truss_compatible_binding.truss_compatible_compile_request',return_value=request),self.assertRaises(ValueError):admit_truss_scalar_projection(admission,artifact,request,manifest,expected,revision='2',document_id='cohort',physical_revisions=('1','2'))

 def test_original_representability_failure_stops_user_sql_without_integer_conversion(self):
  from types import SimpleNamespace
  from ashlar_host.truss_weft_query import _execute_string_artifact
  admission,artifact,request,manifest,expected,source,represent=self.fixture()
  expected['object_current'][0]['props_json']='{"3":'+('9'*5000)+'}'
  guard=admit_truss_integer_source_guard(admission,artifact,request,expected,revision='2',document_id='cohort',physical_revisions=('1','2'))
  with patch('ashlar_host.truss_compatible_binding.truss_compatible_compile_request',return_value=request):projection=admit_truss_scalar_projection(admission,artifact,request,manifest,expected,revision='2',document_id='cohort',physical_revisions=('1','2'))
  artifact['parameters']=[];artifact['sql']='SELECT original_user';calls=[]
  def sql(text,args):calls.append(text);return text
  def capture(frame,config):
   if frame==source['sql']:return {'schema':SCHEMA,'rows':json.loads(guard.entries_json)[0]['rows']}
   if frame==represent['sql']:return {'schema':[['violations','STRING']],'rows':[['1']]}
   raise AssertionError('Result SQL must not run')
  with patch('ashlar_host.path_capture.capture_string_frame',side_effect=capture),self.assertRaisesRegex(ValueError,'representability'):
   _execute_string_artifact(SimpleNamespace(sql=sql),artifact,1,value_projection=projection,integer_source_guard=guard)
  self.assertEqual(calls,[source['sql'],represent['sql']])

 def test_missing_all_guards_prevents_result_sql(self):
  from types import SimpleNamespace
  from ashlar_host.truss_weft_query import _execute_string_artifact
  admission,artifact,request,manifest,expected,*_=self.fixture()
  with patch('ashlar_host.truss_compatible_binding.truss_compatible_compile_request',return_value=request):projection=admit_truss_scalar_projection(admission,artifact,request,manifest,expected,revision='2',document_id='cohort',physical_revisions=('1','2'))
  artifact['parameters']=[];artifact['obligations'][0]['parameters']['checks']=[]
  with patch('ashlar_host.path_capture.capture_string_frame')as capture,self.assertRaisesRegex(ValueError,'nominal source guards'):
   _execute_string_artifact(SimpleNamespace(sql=lambda *a,**k:(_ for _ in ()).throw(AssertionError('SQL must not run'))),artifact,1,value_projection=projection)
  capture.assert_not_called()

 def test_existing_numeric_grouped_count_needs_no_mathematical_field_guard(self):
  from types import SimpleNamespace
  from ashlar_host.truss_weft_query import _execute_string_artifact
  from ashlar_host.truss_numeric_grouped_count import TrussNumericGroupedCount
  import test_truss_numeric_grouped_count as grouped
  artifact,*_=grouped.Tests().fixture()
  artifact['parameters']=[];artifact['sql']='SELECT original_grouped'
  artifact['obligations']=[{'id':'ashlar.candidate.scalarIntegrity','parameters':{'checks':[]}}]
  projection=TrussNumericGroupedCount(encoded(artifact['columns']),'[]','total')
  captured={'schema':[['label','STRING'],['total','STRING']],'rows':[['42','1']]}
  with patch('ashlar_host.path_capture.capture_string_frame',return_value=captured)as capture:
   rows,checks=_execute_string_artifact(SimpleNamespace(sql=lambda text,args:text),artifact,2,grouped=True,value_projection=projection)
  self.assertEqual(rows,[{'label':{'integer_token':'42'},'total':1}]);self.assertEqual(checks,[]);capture.assert_called_once()
