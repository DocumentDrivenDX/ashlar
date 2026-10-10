"""Independent retained graph and exact public request correspondence; no native claims."""
import copy,json,unittest
from pathlib import Path
from run_pack_left_weft import opt_in,tagged_oracle,admit_ordered_rows,admit_matched_source_strings,prepare_original_case
from prepare_pack_query_cases import prepare
ROOT=Path(__file__).resolve().parents[1]
class PackLeftTests(unittest.TestCase):
    def test_actual_runner_preparation_selects_exact_original_left_query_and_graph(self):
        case=prepare_original_case()
        self.assertEqual(case['original_scenario']['id'],'evidence-links')
        self.assertEqual(case['original_scenario']['sql'],'SELECT i.author,p.form,f.taxon FROM interpretations i JOIN interpretation_evidence e ON e.interpretation_id=i.id LEFT JOIN pottery_results p ON p.id=e.pottery_result_id LEFT JOIN fauna_results f ON f.id=e.fauna_result_id ORDER BY i.id')
        self.assertEqual(case['original_graph_expected'],[['Author A','bowl',None],['Author B',None,'synthetic-ungulate']])
    def test_original_request_opt_in_preserves_exact_query_modules(self):
        request=json.loads((ROOT/'examples/end-to-end/weft-left-compiler-fixtures/original-request.json').read_bytes())
        self.assertEqual(opt_in(request),request)
        broken=copy.deepcopy(request);binding=json.loads(broken['target']['bindingJson'])
        for r in binding['records']:
            r['properties']=[p for p in r['properties']if p['logical']['element']!='interpretation_evidence.fauna_result_id']
        broken['target']['bindingJson']=json.dumps(binding)
        with self.assertRaises(ValueError):opt_in(broken)
    def test_original_graph_oracle_order_multiplicity_and_unmatched_tags(self):
        case=next(c for c in prepare('archaeology')['cases']if c['original_scenario']['id']=='evidence-links')
        rows=tagged_oracle(case['original_graph_expected'])
        self.assertEqual(rows,[['Author A',{'state':'value','value':'bowl'},{'state':'absent'}],['Author B',{'state':'absent'},{'state':'value','value':'synthetic-ungulate'}]])
        admit_ordered_rows(rows,rows)
        for bad in (rows[::-1],rows+rows[:1],[[False,*rows[0][1:]],rows[1]]):
            with self.assertRaises(ValueError):admit_ordered_rows(bad,rows)
    def test_all_matched_original_payloads_proved_string_before_runtime(self):
        raw=(ROOT/'examples/domain-packs/archaeology/upstream/graph/fixture.json').read_bytes();proof=admit_matched_source_strings(raw)
        self.assertEqual(len(proof),2)
        graph=json.loads(raw);obj=next(o for o in graph['objects']if o['type']['element']=='pottery_results')
        for bad in (None,False,0):
            obj['values']['pottery_results.form']=bad
            with self.assertRaises(ValueError):admit_matched_source_strings(json.dumps(graph).encode())
        del obj['values']['pottery_results.form']
        with self.assertRaises(ValueError):admit_matched_source_strings(json.dumps(graph).encode())
if __name__=='__main__':unittest.main()
