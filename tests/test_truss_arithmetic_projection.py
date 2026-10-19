# SPDX-License-Identifier: MIT OR Apache-2.0
"""Small original-shaped arithmetic IR/source oracle ports."""
import copy,hashlib,json,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar_host.delta_custody import encoded
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.truss_arithmetic_projection import admit_truss_arithmetic_projection

class Tests(unittest.TestCase):
 def fixture(self):
  raw=Path('examples/truss-native/typed-query-record.umf.json').read_bytes();model=json.loads(raw);digest=hashlib.sha256(raw).hexdigest();pin={'documentId':model['id'],'revision':digest,'sha256':digest,'umfVersion':model['umf']};logical=lambda field:{'documentId':model['id'],'revision':digest,'module':'m','element':field}
  admission=object.__new__(TrussGraphAdmission);admission.revisions={'1':{'models':{model['id']:(model,raw)},'types':{'1':{'owner':(model['id'],'m','TypedRecord'),'required':{'4'},'fields':{'4':'quantitypin'},'keys':{}}}}}
  record={'logical':logical('TypedRecord'),'kind':'object','typeId':'1','sourceSystem':'original','schemaRevision':'1','properties':[{'logical':logical('quantity'),'home':{'kind':'props','propertyId':'4'}}]}
  binding={'modelPins':[pin],'publication':{'manifestUuid':'uuid','tables':[{'name':['local','truss','object_current'],'uuid':'uuid','version':1}]},'records':[record]};request={'sql':'SELECT r.quantity+1 AS next_quantity FROM TypedRecord r','target':{'bindingJson':encoded(binding),'bindingSha256':hashlib.sha256(encoded(binding).encode()).hexdigest()}}
  integer={'family':'integer'};span={'start':7,'end':17};field={'identity':logical('quantity'),'scan':'s0','span':span,'type':{'family':'integer','facets':{'integerWidth':{'bits':64,'signed':True}},'nullable':False}}
  left={'domain':integer,'span':span,'kind':{'op':'field','field':field}};right={'domain':integer,'span':{'start':18,'end':19},'kind':{'op':'literal','value':'1'}};expr={'domain':integer,'span':{'start':7,'end':19},'kind':{'op':'binary','operator':'+','left':left,'right':right}}
  column={'nullable':False,'outputName':'next_quantity','position':1,'representation':{'kind':'scalar','logicalType':{'family':'integer','facets':{},'nullable':False},'carrier':'text','decoder':'exact-integer'},'sourceIdentities':[logical('quantity')]}
  plan={'aggregate':False,'filters':[],'groups':[],'irVersion':'weft-ir/0.4.2','joins':[],'limit':None,'modulePins':[pin],'order':[],'outputs':[{'name':'next_quantity','expression':{'op':'arithmetic','expression':expr}}],'pageKey':None,'readProfile':None,'requiredCapabilities':['arithmetic.+','arithmetic.exact.integer','project','scan','type.integer'],'source':{'occurrence':'s0','pin':pin,'record':logical('TypedRecord')},'typeGraph':[]}
  artifact={'logicalPlan':plan,'modelPins':[pin],'bindingSha256':request['target']['bindingSha256'],'columns':[column]};manifest={'table_versions_json':'{"local.truss.object_current":1}'};expected={'object_current':[{'source_system':'original','type_id':'1','schema_revision':'1','props_json':'{"4":9007199254740993}'}]}
  return admission,artifact,request,manifest,expected,model['id']
 def admit(self,args):
  admission,artifact,request,manifest,expected,doc=args
  with patch('ashlar_host.truss_arithmetic_projection.truss_implicit_arithmetic_request',return_value=request):return admit_truss_arithmetic_projection(admission,artifact,request,manifest,expected,revision='1',document_id=doc)
 def test_exact_large_integer_and_complete_bag(self):
  args=self.fixture();projection=self.admit(args);value=projection.decode(args[1]['columns'][0],'9007199254740994');self.assertEqual(value,{'integer_token':'9007199254740994'});projection.require_complete_bag([{'next_quantity':value}])
  with self.assertRaisesRegex(ValueError,'complete original'):projection.require_complete_bag([])
 def test_domain_and_operation_drift_refuse(self):
  for mutation in ('domain','operation'):
   args=self.fixture();expr=args[1]['logicalPlan']['outputs'][0]['expression']['expression']
   if mutation=='domain':expr['domain']={'family':'decimal','scale':4}
   else:expr['kind']['operator']='/'
   with self.assertRaises(ValueError):self.admit(args)

 def test_decimal_multiplication_ignores_ambient_rounding_context(self):
  args=self.fixture();admission,artifact,request,manifest,expected,doc=args
  model=copy.deepcopy(admission.revisions['1']['models'][doc][0])
  for module in model['modules']:
   for field in module['elements']:
    if field['id']=='quantity':field.update(scalarType='decimal',facets={'precision':18,'scale':4})
  raw=encoded(model).encode();admission.revisions['1']['models'][doc]=(model,raw);digest=hashlib.sha256(raw).hexdigest()
  def repin(value):
   if type(value)is dict:
    if 'revision'in value:value['revision']=digest
    if 'sha256'in value:value['sha256']=digest
    for v in value.values():repin(v)
   elif type(value)is list:
    for v in value:repin(v)
  binding=json.loads(request['target']['bindingJson']);repin(binding);request['target']['bindingJson']=encoded(binding);request['target']['bindingSha256']=hashlib.sha256(encoded(binding).encode()).hexdigest();repin(artifact);artifact['bindingSha256']=request['target']['bindingSha256']
  domain={'family':'decimal','scale':4};expr=artifact['logicalPlan']['outputs'][0]['expression']['expression'];expr['domain']=domain;expr['kind']['operator']='*';expr['kind']['left']['domain']=domain;expr['kind']['left']['kind']['field']['type']={'family':'decimal','facets':{'precision':18,'scale':4},'nullable':False};expr['kind']['right']['kind']['value']='2'
  artifact['columns'][0]['representation'].update(logicalType={'family':'decimal','facets':{'scale':4},'nullable':False},decoder='exact-decimal');artifact['logicalPlan']['requiredCapabilities']=['arithmetic.*','arithmetic.exact.decimal','project','scan','type.decimal','type.integer'];expected['object_current'][0]['props_json']='{"4":12345678901234.5678}'
  from decimal import localcontext
  with localcontext()as ctx:
   ctx.prec=3
   projection=self.admit(args);row={'next_quantity':{'decimal_token':'24691357802469.1356'}};projection.require_complete_bag([row]);self.assertEqual(projection.decode(artifact['columns'][0],'24691357802469.1356'),row['next_quantity'])

 def test_decimal_coefficient_precision_includes_declared_scale(self):
  from ashlar_host.truss_arithmetic_projection import TrussArithmeticProjection
  def projection(scale):
   column={'outputName':'amount','representation':{'kind':'scalar','logicalType':{'family':'decimal','facets':{'scale':scale},'nullable':False},'carrier':'text','decoder':'exact-decimal'}}
   return TrussArithmeticProjection(encoded([column]),encoded([])),column
  admitted,column=projection(38)
  token='0.'+'0'*38
  self.assertEqual(admitted.decode(column,token),{'decimal_token':token})
  nonzero='0.'+'1'*38
  self.assertEqual(admitted.decode(column,nonzero),{'decimal_token':nonzero})
  admitted,column=projection(4)
  with self.assertRaisesRegex(ValueError,'coefficient/scale'):admitted.decode(column,'1'*35)
