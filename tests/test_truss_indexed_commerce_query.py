# SPDX-License-Identifier: MIT OR Apache-2.0
"""Small phase-order/refusal controls; no compiler or engine qualification."""
import json,unittest
from unittest.mock import patch
from ashlar_host.truss_indexed_commerce_query import execute_indexed_commerce_artifact
from ashlar_host.truss_indexed_commerce_guard import TrussIndexedCommerceGuard,SCHEMA
from ashlar_host.truss_indexed_commerce_oracle import IndexedCommercePopulation,AcceptedCommerceHome
from ashlar_host.commerce_original_scenarios import CASES

class Tests(unittest.TestCase):
 def fixture(self):
  source={'publicSourceOnly':True,'sql':'source','field':{},'record':{}};representation={'representabilityOnly':True,'sql':'represent','field':{},'record':{}}
  row=['s','4','5','1','{"q":10}','10'];guard=TrussIndexedCommerceGuard(json.dumps([{'source':source,'representability':representation,'rows':[row]}]))
  population=IndexedCommercePopulation((),(AcceptedCommerceHome('s','4','1','order_lines','quantity','q','integer'),),1024,10)
  column={'outputName':'id','representation':{'kind':'scalar','carrier':'text','decoder':'text','logicalType':{'family':'string','facets':{},'nullable':False}}}
  artifact={'parameters':[],'sql':'result','columns':[column],'logicalPlan':{'order':[],'typeGraph':[],'outputs':[{'expression':{'op':'field'}}]},'obligations':[{'id':'ashlar.candidate.scalarIntegrity','parameters':{'checks':[source,representation,{'sql':'scalar'}]}},{'id':'ashlar.arithmetic.exact','parameters':{'checks':[{'sql':'arithmetic'}]}}]}
  return guard,population,artifact,row
 def execute(self,*,bad_source=False,bad_capacity=False):
  guard,population,artifact,row=self.fixture();calls=[]
  class Spark:
   def sql(self,sql,args):calls.append(sql);return sql
  def capture(sql,config):
   if sql=='source':return {'schema':SCHEMA,'rows':[]if bad_source else[row]}
   if sql=='result':return {'schema':[['id','STRING']],'rows':[['expected']]}
   return {'schema':[['violations','STRING']],'rows':[['1'if sql=='represent'and bad_capacity else'0']]}
  def oracle(*args,**kwargs):calls.append('oracle');return {'rows':[['expected']],'witnesses':{},'scope':'inert'}
  with patch('ashlar_host.truss_indexed_commerce_query.capture_string_frame',side_effect=capture),patch('ashlar_host.truss_indexed_commerce_query.current_truss_commerce_oracle',side_effect=oracle):
   try:answer=execute_indexed_commerce_artifact(Spark(),artifact,guard,population,authored_sql=CASES[1][1],maximum_rows=1)
   except ValueError:return None,calls
  return answer,calls
 def test_source_precedes_all_capacity_and_positional_result(self):
  answer,calls=self.execute();self.assertEqual(calls,['source','scalar','represent','arithmetic','oracle','result']);self.assertEqual(answer['rows'],[['expected']])
 def test_bad_full_source_prevents_capacity_or_user_sql(self):
  answer,calls=self.execute(bad_source=True);self.assertIsNone(answer);self.assertEqual(calls,['source'])
 def test_capacity_prevents_oracle_and_user_sql(self):
  answer,calls=self.execute(bad_capacity=True);self.assertIsNone(answer);self.assertEqual(calls,['source','scalar','represent'])
