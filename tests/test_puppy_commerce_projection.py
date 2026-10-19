# SPDX-License-Identifier: MIT OR Apache-2.0
import json
import unittest
from ashlar_host.puppy_commerce_projection import quantity, coefficient, signed64, commerce_snapshot

class ExactCommerceProjectionTests(unittest.TestCase):
    def test_signed64_exact_boundaries(self):
        for value in (-(2**63), 0, 2**63-1):
            self.assertEqual(quantity(str(value)), value)
        for value in (True, 1.0, 2**63, -(2**63)-1):
            with self.assertRaises(ValueError): signed64(value)

    def test_original_decimal_tokens_are_exact_coefficients(self):
        facets = {'precision':18, 'scale':2}
        self.assertEqual(coefficient('17.00', facets), 1700)
        self.assertEqual(coefficient('-0.00', facets), 0)
        for value in ('1.001', '1e999999', 'NaN', 'Infinity', '1'*65):
            with self.assertRaises(ValueError): coefficient(value, facets)
        with self.assertRaises(ValueError): coefficient('92233720368547758.08', facets)

    def test_carriers_and_facets_cannot_execute_equality(self):
        class Text(str):
            def __eq__(self, other): raise AssertionError('executable text')
        with self.assertRaises(ValueError): quantity(Text('1'))
        with self.assertRaises(ValueError): coefficient(Text('1'), {'precision':18,'scale':2})
        with self.assertRaises(ValueError): coefficient('1', {'precision':18,'scale':True})

    def test_serialized_budget_precedes_copy_and_encoding(self):
        for value,limit in [('\0'*8,8),([None]*100,1)]:
            with self.assertRaises(ValueError):commerce_snapshot(value,limit)
        values=['\0'*8, [None]*100, {'é\n':['😀',False,None,-(2**63),2**63-1]},
                '\"\\\b\f\n\r\t\x1f', [], {}]
        for value in values:
            encoded=json.dumps(value,ensure_ascii=False,separators=(',',':')).encode('utf8')
            self.assertEqual(commerce_snapshot(value,len(encoded)),value)
            with self.assertRaises(ValueError):commerce_snapshot(value,len(encoded)-1)
        with self.assertRaises(ValueError):commerce_snapshot('\ud800')
        cycle=[];cycle.append(cycle)
        with self.assertRaises(ValueError):commerce_snapshot(cycle)

if __name__ == '__main__': unittest.main()
