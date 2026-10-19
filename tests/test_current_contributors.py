# SPDX-License-Identifier: MIT OR Apache-2.0
import json
import unittest
from ashlar.current_contributors import VERSION,decode_current_contributors,require_matching_contributors


def raw(rows):
    return json.dumps({'version':VERSION,'tuples':rows},ensure_ascii=False).encode()
def cell(scan,key):return {'scan':scan,'key':key}


class ContributorTests(unittest.TestCase):
    def test_original_bytes_occurrences_and_independent_multiset(self):
        rows=[[cell('a','x'),cell('b','y')],[cell('a','z'),cell('b','q')],[cell('a','x'),cell('b','y')]]
        original=b'  '+raw(rows)+b'\n'
        actual=decode_current_contributors(original,expected_scans=('a','b'))
        self.assertIs(actual.original_bytes,original)
        self.assertEqual(actual.tuples[0],actual.tuples[2])
        expected=decode_current_contributors(raw(list(reversed(rows))),expected_scans=('a','b'))
        self.assertIsNone(require_matching_contributors(actual,expected))
        substituted = [[cell('a','wrong-key'),cell('b','y')]] + rows[1:]
        for changed in [rows[:-1],rows+[rows[0]],substituted]:
            with self.assertRaises(ValueError):require_matching_contributors(actual,decode_current_contributors(raw(changed),expected_scans=('a','b')))
        wrong=decode_current_contributors(raw([[cell('b','y'),cell('a','x')]]),expected_scans=('b','a'))
        with self.assertRaises(ValueError):require_matching_contributors(actual,wrong)
    def test_utf8_boundary_empty_and_opaque_nul(self):
        for key in ['é'*128,'opaque\0key']:
            self.assertEqual(decode_current_contributors(raw([[cell('s',key)]]),expected_scans=('s',)).tuples,((('s',key),),))
        with self.assertRaises(ValueError):decode_current_contributors(raw([[cell('s','é'*129)]]),expected_scans=('s',))
        empty=decode_current_contributors(raw([]),expected_scans=('s',))
        self.assertEqual(empty.tuples,());require_matching_contributors(empty,empty)
    def test_closed_json_order_and_bounds(self):
        valid=raw([[cell('a','x'),cell('b','y')]])
        invalid=[b'{"version":"x","version":"x","tuples":[]}',raw([[cell('b','x'),cell('a','y')]]),raw([[cell('a',None),cell('b','y')]]),raw([[cell('a',1),cell('b','y')]]),valid.replace(b'"key": "x"',b'"key": "\\ud800"'),b'\xff',valid.replace(b'"tuples":',b'"extra":null,"tuples":')]
        for data in invalid:
            with self.subTest(data=data),self.assertRaises(ValueError):decode_current_contributors(data,expected_scans=('a','b'))
        for kw in [{'maximum_bytes':True},{'maximum_tuples':False},{'maximum_bytes':len(valid)-1}]:
            with self.assertRaises(ValueError):decode_current_contributors(valid,expected_scans=('a','b'),**kw)
        self.assertEqual(len(decode_current_contributors(valid,expected_scans=('a','b'),maximum_bytes=len(valid)).tuples),1)
