# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure original SQL, independent identities/arithmetic and occurrence controls."""
import copy
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from ashlar_host import commerce_original_scenarios as subject
from ashlar_host.commerce_path_oracle import PARTIAL_RETURN

ROOT = Path(__file__).resolve().parents[1]
PACK = (ROOT/'examples/domain-packs/commerce/upstream/pack.json').read_bytes()
MODEL = (ROOT/'src/ashlar_host/resources/ontology.json').read_bytes()
GRAPH = (ROOT/'src/ashlar_host/resources/graph.json').read_bytes()

class Tests(unittest.TestCase):
    def test_all_four_sql_bytes_and_original_graph_bags(self):
        original = json.loads(PACK)['scenario_checks']
        self.assertEqual(subject.commerce_original_cases(PACK), tuple((c['id'],c['sql'])for c in original))
        expected={'partial-return':[['6','2','6']], 'fulfillment':[['[42,0,"F2"]']], 'settlement':[['[42,0,"PAY1"]']], 'refund':[['[42,0,"RF1"]']]}
        for name,sql in subject.commerce_original_cases(PACK):
            answer=subject.original_commerce_scenario_oracle(MODEL,GRAPH,{'sql':sql})
            self.assertEqual(answer['rows'],expected[name]);self.assertEqual(answer['witnesses']['count'],1)
            self.assertTrue(answer['witnesses']['source_occurrences'][0])
        self.assertNotEqual(subject.CASES[0][1],PARTIAL_RETURN)
        self.assertEqual(subject.CASES[0][1],original[0]['sql'])

    def test_request_reuses_public_profile_without_sql_repair_or_borrowed_mutation(self):
        from test_host_paths_query import metadata,BINDINGS
        manifest,registry,aliases=metadata();before=copy.deepcopy((BINDINGS,manifest,registry,aliases))
        request=subject.commerce_original_request(subject.CASES[0][1],MODEL,BINDINGS,manifest,registry,aliases,pack_bytes=PACK)
        self.assertEqual(request['sql'],subject.CASES[0][1]);self.assertEqual(request['interfaceVersion'],'weft-compile/0.4.1')
        self.assertEqual(request['modules'][0]['documentJson'],MODEL.decode())
        binding=json.loads(request['target']['bindingJson']);self.assertEqual(len(binding['records']),10);self.assertEqual(len(binding['relationships']),9)
        self.assertEqual((BINDINGS,manifest,registry,aliases),before)

    def test_unknown_rewritten_sql_and_original_source_mutation_refuse(self):
        for sql in (PARTIAL_RETURN,subject.CASES[0][1]+' ',False):
            with self.assertRaises(ValueError):subject.commerce_original_request(sql,MODEL,{}, {}, [], {},pack_bytes=PACK)
        for model,graph in ((MODEL+b' ',GRAPH),(MODEL,GRAPH+b' ')):
            with self.assertRaises(ValueError):subject.original_commerce_scenario_oracle(model,graph,{'sql':subject.CASES[0][1]})
        with self.assertRaises(ValueError):subject.commerce_original_cases(PACK+b' ')

    def test_executable_json_carriers_refuse_before_public_request(self):
        class Forged(dict):
            def __eq__(self,other):return True
            def __ne__(self,other):return False
        with patch.object(subject,'commerce_count_star_request',side_effect=AssertionError('Borrowed carrier reached request builder')):
            with self.assertRaises(ValueError):subject.commerce_original_request(subject.CASES[0][1],MODEL,Forged(),{},[],{},pack_bytes=PACK)
        with self.assertRaises(ValueError):subject.original_commerce_scenario_oracle(MODEL,GRAPH,Forged(sql=subject.CASES[0][1]))

    def test_source_occurrences_are_not_deduplicated_or_trimmed(self):
        graph=json.loads(GRAPH);returned=next(o for o in graph['objects']if o['type']['element']=='returns');second=copy.deepcopy(returned);second['key']='separate-return-occurrence';graph['objects'].append(second)
        answer=subject._scenario_bag(graph,subject.CASES[0][1])
        self.assertEqual(answer['rows'],[['6','2','6'],['6','2','6']]);self.assertEqual(answer['witnesses']['count'],2)
        self.assertNotEqual(answer['witnesses']['source_occurrences'][0],answer['witnesses']['source_occurrences'][1])
        graph['objects'][-1]['key']=returned['key']
        with self.assertRaises(ValueError):subject._scenario_bag(graph,subject.CASES[0][1])

    def test_decimal_compare_uses_exact_coefficients_not_float_or_ambient_context(self):
        from decimal import localcontext
        graph=json.loads(GRAPH);payment=next(o for o in graph['objects']if o['type']['element']=='payments');invoice=next(o for o in graph['objects']if o['type']['element']=='invoices')
        payment['values']['payments.amount']='9007199254740993.01';invoice['values']['invoices.amount']='9007199254740993.010'
        with localcontext()as context:
            context.prec=1;self.assertEqual(subject._scenario_bag(graph,subject.CASES[2][1])['rows'],[['[42,0,"PAY1"]']])
        invoice['values']['invoices.amount']='9007199254740993.02'
        self.assertEqual(subject._scenario_bag(graph,subject.CASES[2][1])['rows'],[])
        payment['values']['payments.amount']='NaN'
        with self.assertRaises(ValueError):subject._scenario_bag(graph,subject.CASES[2][1])

    def test_exact_refund_and_partial_return_do_not_use_pack_expected_ids(self):
        graph=json.loads(GRAPH);refund=next(o for o in graph['objects']if o['type']['element']=='refunds');refund['values']['refunds.id']='opaque-original-id-without-trimming'
        self.assertEqual(subject._scenario_bag(graph,subject.CASES[3][1])['rows'],[['opaque-original-id-without-trimming']])
        refund['values']['refunds.amount']='25.0000000000000000000000000001'
        self.assertEqual(subject._scenario_bag(graph,subject.CASES[3][1])['rows'],[])
        returned=next(o for o in graph['objects']if o['type']['element']=='returns');returned['values']['returns.quantity']='3'
        self.assertEqual(subject._scenario_bag(graph,subject.CASES[0][1])['rows'],[['6','3','7']])

if __name__=='__main__':unittest.main()
