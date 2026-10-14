"""Consumed STRING join scope; inert original publication/source fixtures."""
import copy,hashlib,json,unittest
from ashlar_host.delta_custody import encoded
from ashlar_host.truss_scalar_join_projection import admit_truss_scalar_join_count
import test_truss_scalar_join_projection as original

class Tests(unittest.TestCase):
 def fixture(self):
  artifact,admission,request,manifest,expected=original.Tests().fixture()
  model=copy.deepcopy(admission.revisions['2']['models']['cohort'][0]);model['umf']='0.8.0';model['extensions']['urn:unknown:consumer']={'exact':'retain me'}
  for module in model['modules']:
   for field in module['elements']:
    if field['id']=='B-label':field['scalarType']='string';field.pop('facets',None)
    if field['id']=='B-tag':field['scalarType']='integer';field.pop('facets',None)
  raw=encoded(model).encode();admission.revisions['2']['models']['cohort']=(model,raw);digest=hashlib.sha256(raw).hexdigest()
  def repin(v):
   if type(v)is dict:
    for key in ('revision','sha256'):
     if key in v:v[key]=digest
    if 'umfVersion'in v:v['umfVersion']='0.8.0'
    for x in v.values():repin(x)
   elif type(v)is list:
    for x in v:repin(x)
  binding=json.loads(request['target']['bindingJson']);repin(binding);request['target']['bindingJson']=encoded(binding);repin(artifact)
  logical={'family':'string','facets':{},'nullable':False}
  for column in artifact['columns']:column['representation']={'kind':'scalar','logicalType':logical,'carrier':'text','decoder':'text'}
  plan=artifact['logicalPlan'];plan['joins'][0]['on'][0]['left']['type']=logical;plan['joins'][0]['on'][0]['right']['field']['type']=logical
  plan['typeGraph'][0]['type']=logical;plan['requiredCapabilities']=['equal','innerJoin','project','scan','type.string']
  values=['雪','雪\u0301','雪']
  for row,value in zip(expected['object_current'],values):row['props_json']='{"3":'+json.dumps(value,ensure_ascii=False)+',"5":'+('9'*5000)+'}'
  return artifact,admission,request,manifest,expected
 def test_complete_string_equality_multiplicity_and_unused_math_preserved(self):
  args=self.fixture();artifact,admission,*_=args;raw=admission.revisions['2']['models']['cohort'][1]
  projection=original.Tests().admit(*args)
  expected=[{'label':'雪','other':'雪'}for _ in range(4)]+[{'label':'雪\u0301','other':'雪\u0301'}]
  projection.require_complete_bag(expected);self.assertEqual(admission.revisions['2']['models']['cohort'][1],raw)
  self.assertIn(b'urn:unknown:consumer',raw)
  with self.assertRaises(ValueError):projection.require_complete_bag(expected[:-1])
 def test_consumed_type_extra_predicate_and_duplicate_json_refuse(self):
  for kind in ('family','predicate','duplicate'):
   args=self.fixture();artifact,_,_,_,expected=args
   if kind=='family':artifact['logicalPlan']['joins'][0]['on'][0]['right']['field']['type']={'family':'integer','facets':{},'nullable':False}
   elif kind=='predicate':artifact['logicalPlan']['joins'][0]['on'].append(copy.deepcopy(artifact['logicalPlan']['joins'][0]['on'][0]))
   else:expected['object_current'][0]['props_json']='{"3":"a","3":"b"}'
   with self.subTest(kind=kind),self.assertRaises(ValueError):original.Tests().admit(*args)
