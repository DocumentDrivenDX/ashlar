"""Original-shaped authored root projection; injected captures, no native claim."""
import copy,json,unittest
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.delta_custody import encoded
from ashlar_host.truss_two_hop_count import admit_truss_two_hop_projection
from ashlar_host.truss_weft_query import _execute_truss_two_hop_projection_artifact
import test_truss_two_hop_count as count_fixture

class Tests(unittest.TestCase):
 def fixture(self):
  args=count_fixture.Tests().fixture();admission,artifact,request,manifest,expected,doc=args
  request['sql']='SELECT r."record-id" AS root FROM TypedRecord r CROSS JOIN EXPAND_PATHS(r.related,related) AS p'
  plan=artifact['logicalPlan'];field=plan['pathExpansion']['path']['hops'][0]['sourceKey']['fields'][0];typ={'family':'string','facets':{},'nullable':False}
  plan.update(aggregate=False,outputs=[{'name':'root','expression':{'op':'field','scan':'s0','identity':field}}],requiredCapabilities=['project','relationship.pathExpansion','relationship.twoHopPaths','scan','type.string'],typeGraph=[{'availability':'required','identity':field,'kind':'scalar','type':typ}])
  path=plan['pathExpansion']['path'];path['span']={'start':0,'end':len(request['sql'])};path['hopSpans']=[dict(path['span']),dict(path['span'])]
  artifact['columns']=[{'nullable':False,'outputName':'root','position':1,'representation':{'kind':'scalar','logicalType':typ,'carrier':'text','decoder':'text'},'sourceIdentities':[field]}]
  expected['edge_current'][0].update(target_id='1');expected['edge_current'][1].update(source_id='1',target_id='1')
  return args
 def admit(self,args,maximum_rows=10000):
  a,f,r,m,e,d=args
  with patch('ashlar_host.truss_two_hop_count.truss_implicit_arithmetic_request',return_value=r):return admit_truss_two_hop_projection(a,f,r,m,e,revision='1',document_id=d,maximum_rows=maximum_rows)
 def test_parallel_selfloop_pairs_preserve_root_bag_and_empty(self):
  args=self.fixture();p=self.admit(args);p.require_complete_bag([{'root':'R1'}]*4)
  self.assertEqual(p.decode(args[1]['columns'][0],'R1'),'R1')
  with self.assertRaises(ValueError):p.require_complete_bag([{'root':'R1'}]*3)
  with self.assertRaises(ValueError):self.admit(args,maximum_rows=3)
  args[4]['edge_current']=[];self.admit(args).require_complete_bag([])
 def test_field_hop_or_duplicate_occurrence_refuses(self):
  for kind in ('field','hop','duplicate'):
   args=self.fixture()
   if kind=='field':args[1]['logicalPlan']['outputs'][0]['expression']['identity']=dict(args[1]['logicalPlan']['source']['record'])
   elif kind=='hop':args[1]['logicalPlan']['pathExpansion']['path']['hops'][1]['targetLifecycle']='owned'
   else:args[4]['edge_current'].append(copy.deepcopy(args[4]['edge_current'][0]))
   with self.subTest(kind=kind),self.assertRaises(ValueError):self.admit(args)
 def test_original_guards_precede_query_and_refusal_withholds_it(self):
  args=self.fixture();p=self.admit(args);artifact=args[1];artifact.update(sql='SELECT original_result',parameters=[])
  phases=['ashlar.candidate.scalarIntegrity','ashlar.candidate.relationshipIntegrity','ashlar.path.occurrenceIntegrity']
  artifact['obligations']=[{'id':'ashlar.candidate.publication','parameters':{}}]+[{'id':name,'parameters':{'checks':[{'sql':'SELECT guard_'+str(i)}]}}for i,name in enumerate(phases)]
  for refusal in (False,True):
   seen=[]
   def capture(sql,*,config):
    seen.append(sql)
    return {'schema':[['root','STRING']],'rows':[['R1']]*4 if sql=='SELECT original_result'else[['1'if refusal else'0']]}
   with patch('ashlar_host.path_capture.capture_string_frame',side_effect=capture):
    if refusal:
     with self.assertRaises(ValueError):_execute_truss_two_hop_projection_artifact(SimpleNamespace(sql=lambda sql,args:sql),artifact,p,4)
     self.assertNotIn('SELECT original_result',seen)
    else:
     rows,checks=_execute_truss_two_hop_projection_artifact(SimpleNamespace(sql=lambda sql,args:sql),artifact,p,4);p.require_complete_bag(rows)
     self.assertEqual(len(checks),3);self.assertEqual(seen,['SELECT guard_0','SELECT guard_1','SELECT guard_2','SELECT original_result'])
