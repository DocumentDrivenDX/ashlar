# SPDX-License-Identifier: MIT OR Apache-2.0
"""Current published-row expectations, not original seed-fixture expectations."""
import unittest
from unittest.mock import patch
from ashlar_host.truss_indexed_commerce_oracle import (AcceptedCommerceHome as Home,IndexedCommercePopulation as Population,current_truss_commerce_oracle,IndexedOracleCapacityError,IndexedOracleDomainError,integer_source_key)
from ashlar_host.commerce_original_scenarios import CASES

class Tests(unittest.TestCase):
 def population(self,records):
  rows=[];homes=[]
  for type_id,(record,fields,values)in enumerate(records,1):
   for pid,(name,family)in enumerate(fields,1):homes.append(Home('current-source',str(type_id),'2'if record=='products'else'1',record,name,str(pid),family,(('precision',18),('scale',2))if family=='decimal'else()))
   for native_id,props in values:rows.append({'source_system':'current-source','type_id':str(type_id),'id':str(native_id),'schema_revision':'2'if record=='products'else'1','props_json':props})
  return Population.from_rows(rows,homes,maximum_source_bytes=65536,maximum_objects=100),rows,homes
 def run_case(self,population,index):return current_truss_commerce_oracle(population,CASES[index][1],maximum_rows=128,maximum_numeric_digits=38)
 def test_signed_partial_return_preserves_duplicate_occurrences_and_positions(self):
  p,rows,homes=self.population([('order_lines',[('id','string'),('quantity','integer')],[(1,'{"1":"L","2":-2}')]),('fulfillments',[('line_id','string'),('quantity','integer')],[(2,'{"1":"L","2":-5}'),(3,'{"1":"L","2":-5}')]),('returns',[('line_id','string'),('quantity','integer')],[(4,'{"1":"L","2":-7}'),(5,'{"1":"L","2":-7}')])])
  rows.clear();homes.clear();result=self.run_case(p,0);self.assertEqual(result['rows'],[['-5','-7','-4']]*4);self.assertEqual(len({tuple(tuple(i)for i in w)for w in result['witnesses']['current_native_occurrences']}),4)
 def test_fulfillment_utf8_order_settlement_currency_and_refund_exact_equality(self):
  p,_,_=self.population([('order_lines',[('id','string'),('quantity','integer'),('product_id','string')],[(1,'{"1":"L","2":2,"3":"P"}')]),('fulfillments',[('id','string'),('line_id','string'),('quantity','integer')],[(2,'{"1":"雪","2":"L","3":3}'),(3,'{"1":"z","2":"L","3":4}')]),('payments',[('id','string'),('invoice_id','string'),('amount','decimal'),('currency','string')],[(4,'{"1":"paid","2":"I","3":25.00,"4":"USD"}'),(5,'{"1":"wrong-currency","2":"I","3":25.0,"4":"EUR"}')]),('invoices',[('id','string'),('amount','decimal'),('currency','string')],[(6,'{"1":"I","2":25.00,"3":"USD"}')]),('returns',[('id','string'),('line_id','string'),('quantity','integer')],[(7,'{"1":"R","2":"L","3":2}')]),('products',[('id','string'),('unit_price','decimal')],[(8,'{"1":"P","2":13.75}')]),('refunds',[('id','string'),('return_id','string'),('amount','decimal')],[(9,'{"1":"equal","2":"R","3":27.50}'),(10,'{"1":"greater-not-equal","2":"R","3":28.00}')])])
  self.assertEqual(self.run_case(p,1)['rows'],[['z'],['雪']]);self.assertEqual(self.run_case(p,2)['rows'],[['paid']]);self.assertEqual(self.run_case(p,3)['rows'],[['equal']])
 def test_mathematical_integer_no_inferred_width_and_giant_token_capacity_before_conversion(self):
  p,_,_=self.population([('order_lines',[('id','string'),('quantity','integer')],[(1,'{"1":"L","2":9223372036854775808}')]),('fulfillments',[('line_id','string'),('quantity','integer')],[(2,'{"1":"L","2":9223372036854775807}')]),('returns',[('line_id','string'),('quantity','integer')],[(3,'{"1":"L","2":1}')])])
  self.assertEqual(self.run_case(p,0)['rows'],[['9223372036854775807','1','2']])
  p,_,_=self.population([('order_lines',[('id','string'),('quantity','integer')],[(1,'{"1":"L","2":'+('9'*5000)+'}')]),('fulfillments',[('line_id','string'),('quantity','integer')],[(2,'{"1":"L","2":1}')]),('returns',[('line_id','string'),('quantity','integer')],[(3,'{"1":"L","2":1}')])])
  with self.assertRaises(IndexedOracleCapacityError):self.run_case(p,0)
 def test_wrong_native_atom_duplicate_json_identity_and_missing_home_refuse(self):
  for props in ('{"1":"L","2":"10"}','{"1":"L","2":1,"2":2}'):
   p,_,_=self.population([('order_lines',[('id','string'),('quantity','integer')],[(1,props)]),('fulfillments',[('line_id','string'),('quantity','integer')],[(2,'{"1":"L","2":1}')]),('returns',[('line_id','string'),('quantity','integer')],[(3,'{"1":"L","2":1}')])])
   with self.assertRaises(IndexedOracleDomainError):self.run_case(p,0)
  p,rows,homes=self.population([('products',[('id','string'),('unit_price','decimal')],[(1,'{"1":"P","2":13.75}')])])
  with self.assertRaises(IndexedOracleDomainError):Population.from_rows(rows+rows,homes,maximum_source_bytes=65536,maximum_objects=100)
  with self.assertRaises(IndexedOracleCapacityError):Population(p.objects,(),65536,100)
  foreign=dict(rows[0]);foreign['source_system']='other-source'
  with self.assertRaises(IndexedOracleDomainError):Population.from_rows([foreign],homes,maximum_source_bytes=65536,maximum_objects=100)
 def test_decimal_accepted_fixed_scale_domain_refusal_is_not_capacity(self):
  p,_,_=self.population([('payments',[('id','string'),('invoice_id','string'),('amount','decimal'),('currency','string')],[(1,'{"1":"pay","2":"I","3":12345678901234567.0,"4":"USD"}')]),('invoices',[('id','string'),('amount','decimal'),('currency','string')],[(2,'{"1":"I","2":12345678901234567.0,"3":"USD"}')])])
  with self.assertRaises(IndexedOracleDomainError):self.run_case(p,2)
 def test_symbolic_scientific_integer_membership_and_canonical_native_cells(self):
  with patch('ashlar_host.truss_indexed_commerce_oracle.int',create=True,side_effect=AssertionError('No integer conversion')):
   self.assertEqual(integer_source_key('1E+2'),(1,'1',2));self.assertEqual(len(integer_source_key('9'*5000)[1]),5000);self.assertEqual(integer_source_key('0E'+('9'*5000)),(0,'0',0))
  self.assertEqual(integer_source_key('1E+2'),integer_source_key('100.0'))
  self.assertEqual(integer_source_key('10.0'),(1,'1',1));self.assertEqual(integer_source_key('-10.0'),(-1,'1',1))
  self.assertEqual(integer_source_key('-0.000E'+('9'*5000)),(0,'0',0))
  for token in ('1.5','1E-2'):
   with self.assertRaises(IndexedOracleDomainError):integer_source_key(token)
  for token in ('1E'+('9'*5000),'1E-'+('9'*5000),'1'*16385):
   with self.assertRaises(IndexedOracleCapacityError):integer_source_key(token)
  with self.assertRaises(IndexedOracleDomainError):integer_source_key('not-a-number')
  p,_,_=self.population([('order_lines',[('id','string'),('quantity','integer')],[(1,'{"1":"L","2":1E+2}')]),('fulfillments',[('line_id','string'),('quantity','integer')],[(2,'{"1":"L","2":10.0}')]),('returns',[('line_id','string'),('quantity','integer')],[(3,'{"1":"L","2":-0.00E+100}')])])
  self.assertEqual(self.run_case(p,0)['rows'],[['10','0','90']]);self.assertIn('1E+2',p.objects[0].props_json);self.assertIn('10.0',p.objects[1].props_json)
 def test_decimal_facets_by_value_before_execution_capacity_and_nul_string(self):
  p,_,_=self.population([('payments',[('id','string'),('invoice_id','string'),('amount','decimal'),('currency','string')],[(1,'{"1":"pay","2":"I","3":1E+2,"4":"USD\\u0000"}')]),('invoices',[('id','string'),('amount','decimal'),('currency','string')],[(2,'{"1":"I","2":100.0000,"3":"USD\\u0000"}')])])
  self.assertEqual(self.run_case(p,2)['rows'],[['pay']])
  p,_,_=self.population([('payments',[('id','string'),('invoice_id','string'),('amount','decimal'),('currency','string')],[(1,'{"1":"pay","2":"I","3":1E+100,"4":"USD"}')]),('invoices',[('id','string'),('amount','decimal'),('currency','string')],[(2,'{"1":"I","2":1E+100,"3":"USD"}')])])
  with self.assertRaises(IndexedOracleDomainError):self.run_case(p,2)
 def test_result_bag_capacity_is_not_truncation(self):
  p,_,_=self.population([('fulfillments',[('id','string'),('line_id','string'),('quantity','integer')],[(1,'{"1":"F","2":"L","3":2}'),(2,'{"1":"G","2":"L","3":2}')]),('order_lines',[('id','string'),('quantity','integer')],[(3,'{"1":"L","2":1}')])])
  with self.assertRaises(IndexedOracleCapacityError):current_truss_commerce_oracle(p,CASES[1][1],maximum_rows=1,maximum_numeric_digits=38)
