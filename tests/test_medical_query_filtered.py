"""Pinned finite encounter order/limit case; no native execution claim."""
import json,unittest
from pathlib import Path
from ashlar.weft_implicit_arithmetic_distribution import ImplicitArithmeticDistributionPaths
from ashlar_host.medical_query_requests import prepare_medical_queries,require_medical_case_bag,FILTERED_LIMIT
from ashlar_host.medical_query_batch import MedicalQueryBatch,MedicalQueryBatchBuffer
from ashlar_host.medical_singleton_query import MedicalSingletonBuffer
import test_medical_singleton_query as fixtures

class Tests(unittest.TestCase):
    def test_original_full_row_population_order_and_limit(self):
        bindings,manifest,registry,aliases=fixtures.MedicalSingletonTests().fixture()
        cases=prepare_medical_queries(fixtures.MODEL,fixtures.GRAPH,fixtures.BASELINE,bindings,manifest,registry,aliases,('filtered-limit',))
        self.assertEqual(len(cases),1);case=cases[0];self.assertEqual(case['request']['sql'],FILTERED_LIMIT)
        self.assertEqual(case['oracle']['witnesses']['complete_population'],1)
        self.assertEqual(case['oracle']['witnesses']['limit'],2)
        self.assertEqual(len(case['oracle']['rows'][0]),5)
        evidence={'native_result':{'rows':case['oracle']['rows']}}
        actual=require_medical_case_bag(fixtures.MODEL,fixtures.GRAPH,bindings,case,{},evidence)
        self.assertEqual(actual,case['oracle']['witnesses']['original_rows'])
        for rows in ([],case['oracle']['rows']*2,[case['oracle']['rows'][0][:-1]]):
            with self.subTest(rows=rows),self.assertRaises(ValueError):
                require_medical_case_bag(fixtures.MODEL,fixtures.GRAPH,bindings,case,{}, {'native_result':{'rows':rows}})
    def test_explicit_case_preserves_default_and_complete_twohop_requirement(self):
        paths=ImplicitArithmeticDistributionPaths(Path('/private/tmp/index'),Path('/private/tmp/install'))
        self.assertEqual(MedicalQueryBatch(paths).cases,('singleton','two-hop'))
        MedicalQueryBatch(paths,('filtered-limit',));MedicalQueryBatch(paths,('singleton','two-hop','filtered-limit'))
        buffer=MedicalSingletonBuffer(b'{}',b'{}',b'{}','{}')
        cases=tuple((name,buffer,'[]')for name in ('singleton','filtered-limit',*(f'two-hop-{i}'for i in range(1,6))))
        self.assertEqual(len(MedicalQueryBatchBuffer(cases,'{}').report()['cases']),7)
        with self.assertRaises(ValueError):MedicalQueryBatchBuffer(cases[:-1],'{}')

if __name__=='__main__':unittest.main()
