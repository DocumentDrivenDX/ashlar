import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from prepare_commerce_exact_puppygraph import quantity,coefficient,signed64
from check_commerce_exact_puppygraph import scalar_integer,verify_intermediates
class ExactFiniteRepresentationTests(unittest.TestCase):
    def test_integer_exact_bounds_and_capability_refusal(self):
        for value in [-(2**63),-9007199254740993,0,9007199254740993,2**63-1]:self.assertEqual(quantity(str(value)),value)
        for token in [str(-(2**63)-1),str(2**63),'1.5','1e3']:
            with self.assertRaisesRegex(ValueError,'Backend capability'):quantity(token)
    def test_fixed_decimal_coefficients_keep_no_rounding(self):
        for token,expected in [('-0.00',0),('12.5',1250),('125',12500),('9999999999999999.99',999999999999999999)]:self.assertEqual(coefficient(token,{'precision':18,'scale':2}),expected)
        with self.assertRaises(ValueError):coefficient('0.001',{'precision':18,'scale':2})
        with self.assertRaises(ValueError):coefficient('1.20',{})
    def test_intermediate_overflow_and_native_float_bool_refuse(self):
        with self.assertRaisesRegex(ValueError,'Backend capability'):signed64((2**63-1)+1)
        for value in [1.0,True,'1',2**63]:
            with self.assertRaises(ValueError):scalar_integer(value)
    def test_unfiltered_intermediates_cannot_hide_rounding_or_missing_paths(self):
        prepared={'all_unfiltered_intermediate_guards':[{'scenario':'partial-return','source_keys':['F','L','R'],'subtract':'4','add':'6'}]}
        verify_intermediates([{'source_keys':['F','L','R'],'subtract':4,'add':6}],prepared,'partial-return')
        for rows in [[],[{'source_keys':['F','L','R'],'subtract':4.0,'add':6}],[{'source_keys':['F','L','R'],'subtract':4,'add':5}]]:
            with self.assertRaises(ValueError):verify_intermediates(rows,prepared,'partial-return')

class NativeClosingCustodyTests(unittest.TestCase):
    def test_failure_and_changed_close_withhold_results(self):
        from unittest.mock import patch
        from check_commerce_exact_puppygraph import execute_checked
        for closing in [ValueError('sealed file changed'),{'database':'changed'}]:
            with patch('check_commerce_exact_puppygraph.native_custody',side_effect=[{'database':'original'},closing]),patch('check_commerce_exact_puppygraph.cypher',return_value={'actual':'provisional'}):
                with self.assertRaises(ValueError):execute_checked({},'Cypher','endpoint','user','password')
    def test_closing_success_releases_exact_result(self):
        from unittest.mock import patch
        from check_commerce_exact_puppygraph import execute_checked
        with patch('check_commerce_exact_puppygraph.native_custody',return_value={'database':'sealed'}),patch('check_commerce_exact_puppygraph.cypher',return_value={'actual':'provisional'}):
            self.assertEqual(execute_checked({},'Cypher','endpoint','user','password'),{'actual':'provisional','opening_closing_native_custody':{'database':'sealed'}})

class RetainedNativeScenarioTests(unittest.TestCase):
    def test_both_native_languages_preserve_exact_scenario_and_intermediate_bags(self):
        import json
        base=Path(__file__).resolve().parents[1]/'docs/helix/04-build/evidence/commerce-exact-puppygraph-20261009'
        prepared=json.loads((base/'receipt.json').read_text())
        expected={'partial-return':[[6,2,6]],'fulfillment':[['F2']],'settlement':[['PAY1']],'refund':[['RF1']]}
        for name in ['cypher-report.json','gremlin-report.json']:
            report=json.loads((base/name).read_text())
            self.assertEqual(report['actual'],expected)
            self.assertEqual(report['expected'],expected)
            self.assertEqual(report['opening_closing_native_custody']['sealed_databases']['/tmp/commerce-exact.duckdb'],prepared['database_sha256'])
            for scenario in ['partial-return','refund']:
                record=next(r for r in report['records'] if r['case']==('partial-intermediates' if scenario=='partial-return' else 'refund-intermediates'))
                verify_intermediates(record['native_rows'],prepared,scenario)

if __name__=='__main__':unittest.main()
