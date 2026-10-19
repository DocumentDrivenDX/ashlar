# SPDX-License-Identifier: MIT OR Apache-2.0
"""Inert accepted-model/binding ports; real shared value decoder and bag checks."""
import hashlib,json,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar.publication import ResolutionError
from ashlar_host.delta_custody import encoded
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.truss_value_projection import admit_truss_value_projection

class Tests(unittest.TestCase):
 def fixture(self):
  raw=Path('examples/truss-native/cohort-r2.umf.json').read_bytes();model=json.loads(raw);digest=hashlib.sha256(raw).hexdigest()
  logical=lambda element:{'documentId':'cohort','revision':digest,'module':'m','element':element}
  pin={'documentId':'cohort','revision':digest,'sha256':digest,'umfVersion':'0.7.0'}
  selected={'owner':('cohort','m','B'),'fields':{'3':'label-pin','5':'tag-pin'},'keys':{'primary':('3',)}}
  admission=object.__new__(TrussGraphAdmission)
  admission.revisions={'2':{'models':{'cohort':(model,raw)},'types':{'2':selected}},'1':{'types':{'2':{**selected,'fields':{'3':'label-pin'}}}}}
  names=['local.truss.'+role for role in sorted(('object_current','edge_current','tombstone','whole_source_history'))]
  tables=[{'name':name.split('.'),'uuid':'original-'+str(index),'version':3}for index,name in enumerate(names)]
  binding={'modelPins':[pin],'publication':{'manifestUuid':'original','tables':tables},'records':[{'logical':logical('B'),'kind':'object','typeId':'2',
   'sourceSystem':'installation','properties':[{'logical':logical(field),'home':{'propertyId':pid}}for field,pid in [('B-label','3'),('B-tag','5')]]}]}
  request={'sql':'SELECT b."B-label" AS label,b."B-tag" AS tag FROM B b','target':{'bindingJson':encoded(binding),'bindingSha256':hashlib.sha256(encoded(binding).encode()).hexdigest()}}
  string={'family':'string','facets':{},'nullable':False};columns=[];outputs=[];graph=[]
  for position,(field,name,availability)in enumerate([('B-label','label','required'),('B-tag','tag','absent-allowed')],1):
   identity=logical(field);rep={'kind':'value','descriptor':identity,'nativeNull':False}if availability=='absent-allowed'else{'kind':'scalar','logicalType':string,'carrier':'text','decoder':'text'}
   columns.append({'position':position,'outputName':name,'nullable':False,'representation':rep,'sourceIdentities':[identity]})
   outputs.append({'name':name,'expression':{'op':'field','scan':'s0','identity':identity}})
   graph.append({'availability':availability,'identity':identity,'kind':'scalar','type':string})
  plan={'aggregate':False,'filters':[],'groups':[],'joins':[],'order':[],'limit':None,'pageKey':None,'readProfile':None,
   'irVersion':'weft-ir/0.2.0','modulePins':[pin],'requiredCapabilities':['project','scan','type.string','value.presence'],
   'source':{'occurrence':'s0','pin':pin,'record':logical('B')},'outputs':outputs,'typeGraph':graph}
  artifact={'modelPins':[pin],'bindingSha256':request['target']['bindingSha256'],'logicalPlan':plan,'columns':columns}
  expected={'object_current':[{'source_system':'installation','type_id':'2','schema_revision':'2','props_json':'{"3":"Beta","5":"tagged"}'},
   {'source_system':'installation','type_id':'2','schema_revision':'1','props_json':'{"3":"Delta"}'}]}
  manifest={'table_versions_json':encoded({name:3 for name in names})}
  with patch('ashlar_host.truss_weft_query.truss_string_compile_request',return_value=request):
   projection=admit_truss_value_projection(admission,artifact,request,manifest,expected,revision='2',document_id='cohort')
  return projection,columns,admission,artifact,request,manifest,expected
 def test_optional_present_and_absent_complete_bag_keeps_required_fields(self):
  projection,columns,_,artifact,*_=self.fixture()
  rows=[{'label':projection.decode(columns[0],'Beta'),'tag':projection.decode(columns[1],'{"state":"value","value":"tagged"}')},
   {'label':projection.decode(columns[0],'Delta'),'tag':projection.decode(columns[1],'{"state":"absent"}')}]
  projection.require_complete_bag(rows)
  self.assertEqual(rows[1]['tag'],{'state':'absent'});self.assertEqual(rows[0]['tag'],{'state':'value','value':'tagged'})
  from types import SimpleNamespace
  from ashlar_host.truss_weft_query import _execute_string_artifact
  artifact['parameters']=[];artifact['sql']='SELECT values'
  artifact['obligations']=[{'id':'ashlar.candidate.scalarIntegrity','parameters':{'checks':[{'sql':'SELECT guard','failureCode':'WFT-NUMERIC-DOMAIN','field':columns[1]['sourceIdentities'][0],'record':artifact['logicalPlan']['source']['record']} ]}}]
  calls=[]
  def capture(frame,*,config):
   calls.append(frame)
   return {'schema':[['violations','STRING']],'rows':[['0']]}if frame=='SELECT guard'else{'schema':[['label','STRING'],['tag','STRING']],'rows':[['Beta','{"state":"value","value":"tagged"}'],['Delta','{"state":"absent"}']]}
  with patch('ashlar_host.path_capture.capture_string_frame',side_effect=capture):
   native,checks=_execute_string_artifact(SimpleNamespace(sql=lambda sql,args:sql),artifact,2,value_projection=projection)
  projection.require_complete_bag(native);self.assertEqual(native,rows);self.assertEqual(calls,['SELECT guard','SELECT values']);self.assertEqual(len(checks),1)
 def test_wrong_descriptor_explicit_null_and_native_null_refuse(self):
  projection,columns,admission,artifact,request,manifest,expected=self.fixture()
  for value in (None,'{"state":"null"}','{"state":"value","value":null}','{"state":"absent","value":"extra"}'):
   with self.assertRaises(ResolutionError):projection.decode(columns[1],value)
  columns[1]['representation']['descriptor']={**columns[1]['representation']['descriptor'],'element':'B-value'}
  with self.assertRaises(ValueError):projection.decode(columns[1],'{"state":"absent"}')
  with patch('ashlar_host.truss_weft_query.truss_string_compile_request',return_value=request):
   with self.assertRaises(ValueError):admit_truss_value_projection(admission,artifact,request,manifest,expected,revision='2',document_id='cohort')
 def test_missing_or_extra_native_row_cannot_publish_partial_bag(self):
  projection,*_=self.fixture();row={'label':'Beta','tag':{'state':'value','value':'tagged'}}
  with self.assertRaises(ValueError):projection.require_complete_bag([row])
  with self.assertRaises(ValueError):projection.require_complete_bag([row,row,{'label':'Delta','tag':{'state':'absent'}}])

 def test_prior_field_pin_change_with_same_keys_refuses(self):
  _,_,admission,artifact,request,manifest,expected=self.fixture()
  admission.revisions['1']['types']['2']['fields']['3']='changed-old-property-pin'
  with patch('ashlar_host.truss_weft_query.truss_string_compile_request',return_value=request):
   with self.assertRaisesRegex(ValueError,'schema/key correspondence'):
    admit_truss_value_projection(admission,artifact,request,manifest,expected,revision='2',document_id='cohort')
