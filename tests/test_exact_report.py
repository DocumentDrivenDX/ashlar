# SPDX-License-Identifier: MIT OR Apache-2.0
"""Exact report carrier controls, independent of compiler/native execution."""
import json,unittest
from decimal import Decimal
from ashlar.weft_decode import ExactScalar
from ashlar.weft_path_decode import PathItem
from ashlar_host.exact_report import report_body
from ashlar_host.pack_native_cli import _body

class ExactReportTests(unittest.TestCase):
 def test_large_key_decimal_original_bytes_and_compatibility(self):
  original=b'\x00\xff';value={'decoded':[ExactScalar(2**63-1,'9223372036854775807'),ExactScalar(Decimal('-0.00'),'-0.00'),PathItem((2**53+1,),{'key':Decimal('1.20')},())],'raw':original}
  raw=report_body(value,10000);self.assertEqual(_body(value,10000),raw);decoded=json.loads(raw)
  self.assertEqual(decoded['decoded'][0]['value'],{'$type':'integer','text':'9223372036854775807'})
  self.assertEqual(decoded['decoded'][1]['value'],{'$type':'Decimal','text':'-0.00'})
  self.assertEqual(decoded['decoded'][2]['intermediate'][0],{'$type':'integer','text':'9007199254740993'})
  self.assertEqual(decoded['raw'],{'$type':'bytes','hex':'00ff'});self.assertEqual(value['raw'],original)
 def test_unknown_executable_and_unbounded_carriers_refuse(self):
  class Unknown:
   def __str__(self):raise AssertionError('coercion')
  class Text(str):pass
  for value in (Unknown(),Text('text'),float('nan'),Decimal('NaN'),2**600,{'x':'large'}):
   with self.assertRaises(ValueError):report_body(value,4 if type(value)is dict else 10000)
  for bound in (True,-1,0,1.5,64*1024*1024+1):
   with self.assertRaises(ValueError):report_body({},bound)

if __name__=='__main__':unittest.main()
