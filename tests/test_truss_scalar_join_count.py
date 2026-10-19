# SPDX-License-Identifier: MIT OR Apache-2.0
"""Basic exact numeric join-count source ports; no native engine."""
import copy,unittest
from unittest.mock import patch
import test_truss_scalar_join_projection as projections
from ashlar_host.truss_scalar_join_projection import admit_truss_scalar_join_count

class Tests(unittest.TestCase):
 def fixture(self):
  artifact,admission,request,manifest,expected=projections.Tests().fixture();plan=artifact['logicalPlan'];integer={'family':'integer','facets':{},'nullable':False}
  plan.update(aggregate=True,typeGraph=[],outputs=[{'name':'matches','expression':{'op':'count','type':integer}}],requiredCapabilities=['aggregate','aggregate.count','equal','innerJoin','project','scan','type.integer'])
  artifact['columns']=[{'nullable':False,'outputName':'matches','position':1,'representation':{'kind':'scalar','logicalType':integer,'carrier':'text','decoder':'exact-integer'},'sourceIdentities':[plan['source']['record'],plan['joins'][0]['right']['record']]}]
  return artifact,admission,request,manifest,expected
 def admit(self,args):
  artifact,admission,request,manifest,expected=args
  with patch('ashlar_host.truss_scalar_join_projection.truss_compatible_compile_request',return_value=request),patch('ashlar_host.truss_scalar_join_projection.admit_truss_compatible_join_revisions',return_value={'physical_revisions':['1','2']}):return admit_truss_scalar_join_count(admission,artifact,request,manifest,expected,revision='2',document_id='cohort',physical_revisions=('1','2'))
 def test_large_integer_duplicate_pairs_and_empty_zero(self):
  args=self.fixture();self.assertEqual(self.admit(args),[{'matches':5}]);args[-1]['object_current']=[];self.assertEqual(self.admit(args),[{'matches':0}])
 def test_repeated_scan_identity_and_exact_count_expression_required(self):
  for mutation in ('identity','aggregate'):
   args=self.fixture()
   if mutation=='identity':args[0]['columns'][0]['sourceIdentities'].pop()
   else:args[0]['logicalPlan']['outputs'][0]['expression']['op']='sum'
   with self.assertRaises(ValueError):self.admit(args)
