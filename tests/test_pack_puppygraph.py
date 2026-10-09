import unittest
from check_pack_puppygraph import integer,projection,queries,verify_query

class PackPuppyTests(unittest.TestCase):
    def test_counts_are_exact(self):
        for bad in (True,1.0,-1,2**63,'1'):
            with self.assertRaises(ValueError):integer(bad)
        self.assertEqual(integer(2**53+1),2**53+1)
    def test_native_membership_and_isolation(self):
        c=queries('Cypher','ecology');g=queries('Gremlin','ecology')
        self.assertIn('n.source_system=$source AND n.type_id=$type',c['filtered-limit'][0])
        self.assertIn(".has('source_system',source).has('type_id',type)",g['filtered-limit'][0])
        self.assertIn('WHERE NOT (n)-[:EcologyEdge]-()',c['isolates'][0])
        self.assertIn(".not(__.bothE('EcologyEdge'))",g['isolates'][0])
        self.assertIn(' MATCH (b)-[f:',c['two-hop'][0])
        self.assertIn('.is(ashlarSingletonKey)',g['singleton'][0])
        self.assertNotIn('.is(key)',g['singleton'][0])
        self.assertIn('$ashlarSingletonKey',c['singleton'][0])
    def test_complete_projection_and_nulls(self):
        for lang in ('Cypher','Gremlin'):
            p=projection(lang,'medical','edge')
            for n in ('props_json','retained_json','native_src','native_dst','original_type'):self.assertIn(n,p)
            self.assertNotIn('properties(',p)
        self.assertIn('constant(null)',projection('Gremlin','medical','node'))
        with self.assertRaises(ValueError):projection('Cypher','unknown','node')
    def test_closed_results_and_duplicate_occurrences(self):
        exp={'one-hop':[['a','e','b'],['a','e','b']]}
        rows=[dict(source='a',edge='e',target='b')]*2
        self.assertEqual(len(verify_query('one-hop',rows,['source','edge','target'],exp)),2)
        for bad in (rows[:1],[dict(rows[0],extra='x')], [dict(rows[0],source=1)]):
            with self.assertRaises(ValueError):verify_query('one-hop',bad,['source','edge','target'],exp)
        with self.assertRaises(ValueError):verify_query('filtered-total',[{'total':True}],['total'],{'filtered-total':1})

class CarrierTests(unittest.TestCase):
    def test_raw_cells_and_identity_are_not_inferred(self):
        from check_pack_puppygraph import carriers
        from run_graph_release_graphframes import columns
        row={n:None for n in columns('node')};row.update(id='7',graph_id='qualified',props_json='{"1":null}')
        typ={'module':'m','element':'t'}
        import json,copy
        raw=dict(row,original_key='k',original_type=json.dumps(typ,sort_keys=True,separators=(',',':')),native_id='MedicalNode[qualified]')
        graph={'objects':[{'key':'k','type':typ}]}
        self.assertEqual(carriers([raw],'medical','node',[row],{'qualified':'k'},graph),[row])
        for name,value in [('id',7),('props_json','{"1":"null"}'),('native_id','MedicalNode[7]'),('original_key','wrong'),('retained_json','')]:
            changed=copy.deepcopy(raw);changed[name]=value
            with self.assertRaises(ValueError):carriers([changed],'medical','node',[row],{'qualified':'k'},graph)
        missing=dict(raw);missing.pop('retained_json')
        with self.assertRaises(ValueError):carriers([missing],'medical','node',[row],{'qualified':'k'},graph)
    def test_closing_change_withholds_report(self):
        from check_pack_puppygraph import run_held
        from unittest.mock import patch
        prepared=[dict(pack=p,value={},graph={},nodes={},edges={},admission={},expected={})for p in ('archaeology','ecology','medical')]
        db={n:'a'*64 for n in ['/tmp/releases.duckdb','/tmp/commerce.duckdb','/tmp/augmentations.duckdb','/tmp/commerce-exact.duckdb','/tmp/packs.duckdb']}
        opening={'container':{'id':'x'},'requested_model_sha256':'a'*64,'observed_model_sha256':'a'*64,'catalog_observed':False,'databases':db}
        with patch('check_pack_puppygraph.execute_pack',return_value={}):
            with self.assertRaises(ValueError):run_held(prepared,'Cypher',None,iter([opening,dict(opening,observed_model_sha256='b'*64)]).__next__)

class LifecycleTests(unittest.TestCase):
    def test_wrong_edge_endpoint_refuses(self):
        from check_pack_puppygraph import carriers
        from run_graph_release_graphframes import columns
        import json
        row={n:None for n in columns('edge')};row.update(id='9',graph_id='edge',src='a',dst='b')
        typ={'module':'m','element':'rel'}
        raw=dict(row,original_key='e',original_type=json.dumps(typ,sort_keys=True,separators=(',',':')),native_id='EcologyEdge[edge]',native_src='EcologyNode[a]',native_dst='EcologyNode[wrong]')
        with self.assertRaisesRegex(ValueError,'incidence'):
            carriers([raw],'ecology','edge',[row],{'edge':'e'},{'edges':[{'key':'e','relationship':typ}]})
    def test_client_close_failure_withholds_return(self):
        from check_pack_puppygraph import check
        from unittest.mock import patch
        import sys,types
        class Scope:
            def __enter__(self):return self
            def __exit__(self,*args):raise RuntimeError('close failed')
            def session(self):return self
        neo=types.SimpleNamespace(GraphDatabase=types.SimpleNamespace(driver=lambda *a,**k:Scope()))
        with patch.dict(sys.modules,{'neo4j':neo}),patch('check_pack_puppygraph.observe_native',return_value={}),patch('check_pack_puppygraph.run_held',return_value={'success':True}):
            with self.assertRaisesRegex(RuntimeError,'close failed'):
                check([], 'Cypher','bolt://127.0.0.1:17887',{}, {},'u','p')
        class Remote:
            def close(self):raise RuntimeError('remote close failed')
        package=types.ModuleType('gremlin_python');driver=types.ModuleType('gremlin_python.driver')
        driver.client=types.SimpleNamespace(Client=lambda *a,**k:Remote())
        driver.serializer=types.SimpleNamespace(GraphSONSerializersV3d0=lambda:None)
        with patch.dict(sys.modules,{'gremlin_python':package,'gremlin_python.driver':driver}),patch('check_pack_puppygraph.observe_native',return_value={}),patch('check_pack_puppygraph.run_held',return_value={'success':True}):
            with self.assertRaisesRegex(RuntimeError,'remote close failed'):
                check([], 'Gremlin','ws://127.0.0.1:18182/gremlin',{}, {},'u','p')
