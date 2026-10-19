# SPDX-License-Identifier: MIT OR Apache-2.0
"""Small distinct terminal key set versus independent path multiplicity tests."""
import unittest
from types import SimpleNamespace
from unittest.mock import patch
import test_truss_two_hop_count as paths
from ashlar_host.truss_two_hop_count import admit_truss_two_hop_count,admit_truss_two_hop_target_count
from ashlar_host.truss_weft_query import _execute_truss_two_hop_count_artifact

class Tests(unittest.TestCase):
 def fixture(self):
  args=paths.Tests().fixture();artifact=args[1];plan=artifact['logicalPlan'];terminal=plan['pathExpansion']['path']['hops'][-1];integer={'family':'integer','facets':{},'nullable':False}
  plan['outputs']=[{'name':'targets','expression':{'op':'countDistinctPathTargets','pathOccurrence':'p0','type':integer}}];plan['requiredCapabilities']=['aggregate','aggregate.pathTargetDistinctCount','project','relationship.pathExpansion','relationship.twoHopPaths','scan']
  column=artifact['columns'][0];column['outputName']='targets';column['representation']['pathTarget']={'record':terminal['to'],'key':terminal['targetKey'],'pathOccurrence':'p0'};column['sourceIdentities']=[terminal['to'],*terminal['targetKey']['fields']]
  return args
 def admit(self,args):
  admission,artifact,request,manifest,expected,doc=args
  with patch('ashlar_host.truss_two_hop_count.truss_implicit_arithmetic_request',return_value=request):return admit_truss_two_hop_target_count(admission,artifact,request,manifest,expected,revision='1',document_id=doc)
 def test_two_path_occurrences_one_target_and_empty_zero(self):
  self.assertEqual(paths.Tests().admit(paths.Tests().fixture()).expected_count,2)
  args=self.fixture();projection=self.admit(args);self.assertEqual(projection.expected_count,1);projection.require_complete_bag([{'targets':1}]);self.assertEqual(projection.decode(args[1]['columns'][0],'1'),1)
  with self.assertRaisesRegex(ValueError,'complete original'):projection.require_complete_bag([{'targets':2}])
  with self.assertRaisesRegex(ValueError,'complete original'):projection.require_complete_bag([])
  args[4]['edge_current']=[];self.admit(args).require_complete_bag([{'targets':0}])
 def test_wrong_terminal_descriptor_refuses_and_original_remains_intact(self):
  args=self.fixture();original=args[1]['columns'][0]['representation']['pathTarget'];projection=self.admit(args);projection.decode(args[1]['columns'][0],'1');self.assertIs(args[1]['columns'][0]['representation']['pathTarget'],original)
  original['pathOccurrence']='foreign'
  with self.assertRaisesRegex(ValueError,'descriptor'):self.admit(args)
 def test_guard_refusal_prevents_distinct_result_sql(self):
  args=self.fixture();projection=self.admit(args);artifact=args[1];artifact.update(sql='SELECT original_distinct_result',parameters=[])
  phases=['ashlar.candidate.scalarIntegrity','ashlar.candidate.relationshipIntegrity','ashlar.path.occurrenceIntegrity','ashlar.path.countCapacity'];artifact['obligations']=[{'id':'ashlar.candidate.publication','parameters':{}}]+[{'id':name,'parameters':{'checks':[{'sql':'SELECT guard_'+str(i)}]}}for i,name in enumerate(phases)];seen=[]
  def capture(sql,*,config):seen.append(sql);return {'schema':[['targets','STRING']],'rows':[['1'if sql=='SELECT guard_3'else'0']]}
  with patch('ashlar_host.path_capture.capture_string_frame',side_effect=capture),self.assertRaisesRegex(ValueError,'guard refused'):_execute_truss_two_hop_count_artifact(SimpleNamespace(sql=lambda sql,args:sql),artifact,projection,1)
  self.assertNotIn('SELECT original_distinct_result',seen)
