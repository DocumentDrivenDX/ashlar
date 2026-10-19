# SPDX-License-Identifier: MIT OR Apache-2.0
"""A refused emitted guard must stop the unchanged user query."""
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.delta_custody import encoded
from ashlar_host.truss_arithmetic_projection import TrussArithmeticProjection
from ashlar_host.truss_weft_query import _execute_truss_arithmetic_artifact

class Tests(unittest.TestCase):
 def test_source_then_arithmetic_guards_and_no_query_after_refusal(self):
  columns=[{'outputName':'n','representation':{'logicalType':{'family':'integer','facets':{}}}}]
  projection=TrussArithmeticProjection(encoded(columns),encoded([{'n':{'integer_token':'2'}}]))
  artifact={'columns':columns,'parameters':[], 'sql':'SELECT result', 'obligations':[
   {'id':'ashlar.arithmetic.exact','parameters':{'checks':[{'sql':'SELECT arithmetic','phase':'projection-survivors'}]}},
   {'id':'ashlar.candidate.scalarIntegrity','parameters':{'checks':[{'sql':'SELECT source'}]}}]}
  for refused in (None,'SELECT source','SELECT arithmetic'):
   with self.subTest(refused=refused):
    observed=[]
    def query(sql,**kwargs):observed.append(sql);return sql
    def capture(sql,**kwargs):return {'schema':[['n','STRING']],'rows':[['2'if sql=='SELECT result'else'1'if sql==refused else'0']]}
    with patch('ashlar_host.path_capture.capture_string_frame',side_effect=capture):
     if refused:
      with self.assertRaisesRegex(ValueError,'guard refused'):_execute_truss_arithmetic_artifact(SimpleNamespace(sql=query),artifact,projection,1)
      self.assertNotIn('SELECT result',observed)
     else:
      rows,checks=_execute_truss_arithmetic_artifact(SimpleNamespace(sql=query),artifact,projection,1)
      self.assertEqual(rows,[{'n':{'integer_token':'2'}}]);self.assertEqual(observed,['SELECT source','SELECT arithmetic','SELECT result'])
