import json,unittest
from pathlib import Path
from check_pack_scenario_puppygraph import query_plan,execute,held,admit
class NativeScenarioQueryTests(unittest.TestCase):
    def profiles(self):
        from test_pack_scenario_puppygraph import ScenarioPuppyPreparationTests
        return ScenarioPuppyPreparationTests().profiles()
    def test_all_original_native_translations_and_semantics(self):
        for profile in self.profiles():
            for case in profile['cases']:
                name=case['original_scenario']['id']
                for language in ('Cypher','Gremlin'):
                    script,b,n=query_plan(profile,name,language)
                    self.assertTrue(script);self.assertTrue(all(k.startswith('ashlar')for k in b));self.assertEqual(len(n),10 if name=='evidence-links'else len(case['original_graph_expected'][0])if case['original_graph_expected']else 1)
                    if name in ('media','connected-measurements'):self.assertIn('DISTINCT'if language=='Cypher'else'.dedup().count()',script)
                    if name=='evidence-links':self.assertIn('OPTIONAL MATCH'if language=='Cypher'else'.coalesce(',script);self.assertIn('interpretation_order_token',script)
                    if name=='connected-measurements':self.assertIn('ORDER BY'if language=='Cypher'else'.order()',script)
    def test_complete_closing_source_change_refuses(self):
        from unittest.mock import patch
        prepared={'reports':[]}
        with self.assertRaises(ValueError):held(prepared,'Cypher',None,lambda:{},iter([{'x':'a'},{'x':'b'}]).__next__)
        with self.assertRaises(ValueError):held(prepared,'Cypher',None,iter([{'db':'a'},{'db':'b'}]).__next__,lambda:{})
    def test_unknown_protocol_refuses(self):
        p=self.profiles()[0]
        with self.assertRaises(ValueError):query_plan(p,'cycle','SQL')
    def test_client_close_failure_withholds_result(self):
        import sys,types
        from unittest.mock import patch,Mock
        remote=Mock();remote.close.side_effect=RuntimeError('close failed')
        client=types.SimpleNamespace(Client=Mock(return_value=remote));serializer=types.SimpleNamespace(GraphSONSerializersV3d0=lambda:None)
        driver=types.ModuleType('gremlin_python.driver');driver.client=client;driver.serializer=serializer
        with patch.dict(sys.modules,{'gremlin_python':types.ModuleType('gremlin_python'),'gremlin_python.driver':driver}),patch('check_pack_scenario_puppygraph.observe_six',return_value={}),patch('check_pack_scenario_puppygraph.held',return_value={'original_files':{},'closing':{}}):
            from check_pack_scenario_puppygraph import check
            with self.assertRaisesRegex(RuntimeError,'close failed'):check({},'Gremlin','x',{}, {},'u','p',lambda:{})
        remote.close.assert_called_once()
    def test_wrong_native_edge_endpoint_refuses(self):
        from check_pack_scenario_puppygraph import carrier_check,raw_projection
        p=self.profiles()[0];_,rows,names,label=raw_projection(p,'edge','Cypher');raw=[]
        for r in rows:raw.append(dict(r,native_id=label+'['+r['graph_id']+']',native_src='wrong',native_dst='wrong'))
        with self.assertRaisesRegex(ValueError,'incidence'):carrier_check(p,'edge','Cypher',lambda s,b:raw)
    def test_admission_source_mutation_refuses_before_client(self):
        from unittest.mock import patch
        from check_pack_scenario_puppygraph import admit_guarded
        with patch('check_pack_scenario_puppygraph.admit',return_value={}),patch('check_pack_scenario_puppygraph.check')as client:
            with self.assertRaisesRegex(ValueError,'during admission'):admit_guarded('x',{},[],'x',iter([{'source':'original'},{'source':'changed'}]).__next__)
            client.assert_not_called()
    def test_wrong_publication_profile_refuses_before_client(self):
        import tempfile,hashlib
        from unittest.mock import patch
        from prepare_pack_scenario_puppygraph import build
        profiles=self.profiles()
        with tempfile.TemporaryDirectory()as t,patch('prepare_pack_scenario_puppygraph.prepare',side_effect=profiles):
            directory=Path(t)/'fresh';build([{'pack':'archaeology'},{'pack':'ecology'}],'x',directory)
            trusted={n:hashlib.sha256((directory/n).read_bytes()).hexdigest()for n in ('receipt.json','model.json','scenarios.duckdb')}
            wrong=dict(profiles[0],publication={'unrelated':'publication'})
            with patch('prepare_pack_graph_scenarios.prepare',return_value=wrong),patch('check_pack_scenario_puppygraph.check')as client:
                with self.assertRaisesRegex(ValueError,'publication/profile'):admit(directory,trusted,[{'pack':'archaeology'},{'pack':'ecology'}],'x')
                client.assert_not_called()
