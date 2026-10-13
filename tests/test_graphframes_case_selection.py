"""Pure authored-case selection; no GraphFrames/JVM execution."""
import unittest
from ashlar_host.graphframes_case_selection import select_graphframes_profile,validate_graphframes_result_selection


class Tests(unittest.TestCase):
    def test_authored_order_exact_queries_and_subset_evidence(self):
        original=('partial-return','fulfillment','settlement','refund')
        cases=[{'original_scenario':{'id':name,'sql':'original '+name},'original_graph_expected':[[name]]}for name in original]
        profile={'pack':'commerce','cases':cases,'nodes':['entire graph'],'source_admission':{'original':'bytes'}}
        self.assertIs(select_graphframes_profile(profile),profile)
        selected=select_graphframes_profile(profile,('refund','fulfillment'))
        self.assertEqual(selected['selected_case_ids'],['fulfillment','refund'])
        self.assertEqual(selected['original_case_ids'],list(original))
        self.assertIs(selected['cases'][0],cases[1]);self.assertIs(selected['nodes'],profile['nodes'])
        for invalid in (('unknown',),('refund','refund')):
            with self.assertRaises(ValueError):select_graphframes_profile(profile,invalid)
        result={'format':'ashlar-selected-pack-graphframes/0.1','pack':'commerce','cases':selected['cases'],
            'original_case_ids':list(original),'selected_case_ids':selected['selected_case_ids'],'selected_cases_passed':2}
        validate_graphframes_result_selection(result)
        result['selected_cases_passed']=4
        with self.assertRaises(ValueError):validate_graphframes_result_selection(result)
