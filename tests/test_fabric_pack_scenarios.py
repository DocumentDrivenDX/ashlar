"""Finite original17 GQL construction, not native Fabric qualification."""
import copy
import json
import unittest
from ashlar_host.fabric_pack_scenarios import gql_plan
from ashlar_host.graphframes_pack_scenarios import ARITY, WITNESSES
import test_graphframes_pack_scenarios as fixtures


class Tests(unittest.TestCase):
    def test_all17_preserve_outputs_and_scan_occurrences(self):
        from ashlar_host.pack_scenario_queries import plans
        total = 0
        for pack in ('archaeology', 'ecology'):
            profile = fixtures.Tests().profile(pack)
            for name, (scans, joins, predicates, outputs, group) in plans(pack).items():
                query, names = gql_plan(profile, name)
                self.assertEqual(names, ['result_' + str(i) for i in range(ARITY[pack][name])] + (WITNESSES if name == 'evidence-links' else []))
                for scan in scans:
                    self.assertIn('(' + scan.split(':')[0] + ':Scenario' + pack.title() + 'Node)', query)
                self.assertEqual('RETURN DISTINCT' in query, name == 'comparability')
                self.assertNotIn('LIMIT', query)
                self.assertNotIn('COALESCE', query.upper())
                total += 1
        self.assertEqual(total, 17)

    def test_group_having_order_and_optional_witnesses(self):
        archaeology = fixtures.Tests().profile('archaeology')
        media, _ = gql_plan(archaeology, 'media')
        self.assertIn('count(DISTINCT ', media)
        self.assertIn(' GROUP BY ashlarScenarioGroup NEXT FILTER result_1 > 1 RETURN result_0,result_1', media)
        optional, names = gql_plan(archaeology, 'evidence-links')
        self.assertEqual(optional.count(' OPTIONAL MATCH '), 2)
        self.assertIn('OPTIONAL MATCH (p:ScenarioArchaeologyNode) WHERE p.original_type = ', optional)
        self.assertIn('OPTIONAL MATCH (f:ScenarioArchaeologyNode) WHERE f.original_type = ', optional)
        self.assertTrue(optional.endswith(' ORDER BY interpretation_order_token'))
        self.assertEqual(names[3:], WITNESSES)
        for witness in WITNESSES:
            self.assertIn(' AS ' + witness, optional)
        ecology = fixtures.Tests().profile('ecology')
        connected, _ = gql_plan(ecology, 'connected-measurements')
        self.assertTrue(connected.endswith(' GROUP BY ashlarScenarioGroup ORDER BY result_0'))
        dating, _ = gql_plan(archaeology, 'dating')
        self.assertIn('.integer_', dating)
        zero, _ = gql_plan(ecology, 'zero')
        self.assertIn('.integer_', zero)
        self.assertIn(' = 0', zero)
        self.assertIn('"not-detected"', zero)

    def test_closed_identifiers_and_literal_escaping(self):
        profile = fixtures.Tests().profile('archaeology')
        for member in ('lexical_column', 'presence_column', 'signed64_column'):
            broken = copy.deepcopy(profile)
            broken['fields'][0][member] = 'field_1) RETURN 1 //'
            with self.assertRaises(ValueError):
                gql_plan(broken, 'cycle')
        broken = copy.deepcopy(profile)
        broken['fields'].append(broken['fields'][0])
        with self.assertRaises(ValueError):
            gql_plan(broken, 'cycle')
        for pack, name in [('unknown', 'cycle'), ('archaeology', 'unknown')]:
            broken = copy.deepcopy(profile)
            broken['pack'] = pack
            with self.assertRaises(ValueError):
                gql_plan(broken, name)
        changed = copy.deepcopy(profile)
        original = 'urn:quoted:"\\\n☃'
        for field in changed['fields']:
            field['record_identity'][0] = original
        query, _ = gql_plan(changed, 'cycle')
        value = json.dumps({'document': original, 'module': 'domain', 'element': 'stratigraphic_assertions'}, sort_keys=True, separators=(',', ':'))
        self.assertIn(json.dumps(value, ensure_ascii=False), query)
        broken = copy.deepcopy(profile)
        for f in broken['fields']:
            f['signed64_column'] = None
        with self.assertRaises(ValueError):
            gql_plan(broken, 'dating')

    def test_complete_native_result_comparison_keeps_optional_witnesses_and_bags(self):
        import hashlib
        from ashlar_host.fabric_gql import FabricGqlResult
        from ashlar_host.fabric_pack_scenarios import validate_gql_case
        for pack in ('archaeology','ecology'):
            helper=fixtures.Tests();profile=helper.profile(pack)
            for case in profile['cases']:
                name=case['original_scenario']['id'];query,names=gql_plan(profile,name)
                schema=helper.schema(profile,case);rows=helper.rows(profile,case)
                kinds=tuple((x['name'],'INT64'if x['type']=='bigint'else'STRING')for x in schema)
                digest=hashlib.sha256(json.dumps({'query':query},ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
                result=FabricGqlResult(digest,(),kinds,tuple(map(tuple,rows)))
                validate_gql_case(profile,name,result)
                if rows:
                    broken=FabricGqlResult(digest,(),kinds,result.rows+result.rows[:1])
                    with self.assertRaises(ValueError):validate_gql_case(profile,name,broken)
                with self.assertRaises(ValueError):validate_gql_case(profile,name,FabricGqlResult('0'*64,(),kinds,result.rows))

    def test_native_no_data_omits_schema_only_for_independently_empty_case(self):
        import hashlib
        from ashlar_host.fabric_gql import FabricGqlResult
        from ashlar_host.fabric_pack_scenarios import validate_gql_case
        profile=fixtures.Tests().profile('ecology');name='effort-event'
        query,_=gql_plan(profile,name)
        digest=hashlib.sha256(json.dumps({'query':query},ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
        native={'status':{'code':'02000'},'result':{'kind':'TABLE','columns':[],'data':[]}}
        def result(value):return FabricGqlResult(digest,(json.dumps(value).encode(),),(),())
        self.assertEqual(validate_gql_case(profile,name,result(native)),[])
        for change in ('status','warning','continuation','unexpected'):
            bad=copy.deepcopy(native);candidate=copy.deepcopy(profile)
            if change=='status':bad['status']['code']='00000'
            elif change=='warning':bad['additionalStatuses']=[{'code':'01000'}]
            elif change=='continuation':bad['result']['nextPage']='pending'
            else:
                next(c for c in candidate['cases']if c['original_scenario']['id']==name)['original_graph_expected']=[['missing']]
            with self.assertRaises(ValueError):validate_gql_case(candidate,name,result(bad))
