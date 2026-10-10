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
    def test_gremlin_carrier_chunks_cover_all_cells_and_correlate(self):
        from check_pack_scenario_puppygraph import carrier_chunks,carrier_check,raw_projection
        for p in self.profiles():
            for kind in ('node','edge'):
                chunks=carrier_chunks(p,kind,'Gremlin');_,expected,all_names,label=raw_projection(p,kind,'Gremlin')
                self.assertEqual(set().union(*map(set,chunks)),set(all_names));self.assertTrue(all(len(c)<=18 for c in chunks))
                self.assertTrue(all({'id','graph_id','original_key','original_type'}<=set(c)for c in chunks))
                scripts={}
                for cols in chunks:
                    script,rows,_,_=raw_projection(p,kind,'Gremlin',cols);scripts[script]=[dict(cells={{'id':'carrier_id','graph_id':'carrier_key'}.get(n,n):([r[n]]if kind=='node'else r[n])for n in cols if r[n]is not None},native_id=label+'['+r['graph_id']+']',**({'native_src':'Scenario'+p['pack'].title()+'Node['+r['src']+']','native_dst':'Scenario'+p['pack'].title()+'Node['+r['dst']+']'}if kind=='edge'else {}))for r in rows]
                actual=carrier_check(p,kind,'Gremlin',lambda s,b:scripts[s]);self.assertEqual(len(actual['chunks']),len(chunks))
                scripts[next(iter(scripts))][0]['cells']['unexpected']='changed'
                with self.assertRaises(ValueError):carrier_check(p,kind,'Gremlin',lambda s,b:scripts[s])
    def test_join_predicates_apply_before_next_scan(self):
        p=self.profiles()[0];script,_,_=query_plan(p,'specialists','Gremlin')
        self.assertLess(script.index('.where('),script.index(".as('s')"))
        self.assertLess(script.index(".where(",script.index(".as('s')")),script.index(".as('l')"))
    def test_attempt_journal_precedes_native_and_retains_failure(self):
        import tempfile
        from check_pack_scenario_puppygraph import journal_query,staged_query
        with tempfile.TemporaryDirectory()as t:
            path=Path(t)/'attempts.jsonl'
            def fail(s,b):
                self.assertEqual(json.loads(path.read_text().splitlines()[0])['outcome'],'submitted-before-native-query')
                raise RuntimeError('native refusal')
            q=journal_query(fail,lambda:{'six':'unchanged'},path)
            with self.assertRaisesRegex(RuntimeError,'native refusal'):staged_query(q,{'stage':'carrier','columns':['a']},'original-script',{'ashlarValue':'original'})
            rows=[json.loads(x)for x in path.read_text().splitlines()];self.assertEqual([r['outcome']for r in rows],['submitted-before-native-query','failed']);self.assertEqual(rows[1]['script'],'original-script');self.assertEqual(rows[1]['stage']['columns'],['a'])
    def test_role_specific_map_null_scalar_and_loss_refusals(self):
        from check_pack_scenario_puppygraph import native_map_cells
        import copy
        expected=[{'graph_id':'g','original_key':'key','empty':'','zero':0,'null':None}];names=list(expected[0])
        node={'cells':{'carrier_key':['g'],'original_key':['key'],'empty':[''],'zero':[0]},'native_id':'native'}
        edge={'cells':{'carrier_key':'g','original_key':'key','empty':'','zero':0},'native_id':'native','native_src':'s','native_dst':'t'}
        for kind,raw in [('node',node),('edge',edge)]:
            self.assertEqual(native_map_cells(raw,kind,names,expected),expected[0])
            for key,value in [('zero',True),('zero',0.0),('empty',False),('unknown','x')]:
                bad=copy.deepcopy(raw);bad['cells'][key]=[value]if kind=='node'else value
                with self.assertRaises(ValueError):native_map_cells(bad,kind,names,expected)
            bad=copy.deepcopy(raw);del bad['cells']['empty']
            with self.assertRaises(ValueError):native_map_cells(bad,kind,names,expected)
        for value in ([],['key','key'],'key'):
            bad=copy.deepcopy(node);bad['cells']['original_key']=value
            with self.assertRaises(ValueError):native_map_cells(bad,'node',names,expected)
        bad=copy.deepcopy(node);bad['cells']['null']=[];self.assertEqual(native_map_cells(bad,'node',names,expected)['null'],None)
        bad=copy.deepcopy(edge);bad['cells']['zero']=[0]
        with self.assertRaises(ValueError):native_map_cells(bad,'edge',names,expected)
    def test_value_map_native_edge_endpoint_refusal(self):
        from check_pack_scenario_puppygraph import carrier_chunk_check,raw_projection
        p=self.profiles()[0];_,rows,names,label=raw_projection(p,'edge','Gremlin');raw=[{'cells':{{'id':'carrier_id','graph_id':'carrier_key'}.get(n,n):r[n]for n in names if r[n]is not None},'native_id':label+'['+r['graph_id']+']','native_src':'wrong','native_dst':'wrong'}for r in rows]
        with self.assertRaisesRegex(ValueError,'incidence'):carrier_chunk_check(p,'edge','Gremlin',lambda s,b:raw,names)
    def test_actual_graphson_int64_wrapper_and_other_subclasses(self):
        try:
            from gremlin_python.structure.io.graphsonV3d0 import GraphSONReader
        except ImportError:self.skipTest('Qualified optional Gremlin3.7.3 runtime required')
        from check_pack_scenario_puppygraph import native_map_cells
        expected=[{'graph_id':'g','count':0}];value=GraphSONReader().to_object({'@type':'g:Int64','@value':0})
        raw={'cells':{'carrier_key':'g','count':value},'native_id':'native','native_src':'s','native_dst':'t'}
        cells=native_map_cells(raw,'edge',['graph_id','count'],expected);self.assertEqual(cells['count'],0);self.assertIs(type(cells['count']),int);self.assertIs(raw['cells']['count'],value)
        class FakeInt(int):pass
        for bad in (FakeInt(0),True,0.0,GraphSONReader().to_object({'@type':'g:Int64','@value':2**63})):
            raw['cells']['count']=bad
            with self.assertRaises(ValueError):native_map_cells(raw,'edge',['graph_id','count'],expected)
