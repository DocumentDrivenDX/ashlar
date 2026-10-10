import base64,copy,json,unittest
from pathlib import Path
from prepare_pack_scenario_puppygraph import entries,build
from prepare_pack_graph_scenarios import prepare

class ScenarioPuppyPreparationTests(unittest.TestCase):
    def profiles(self):
        # Real source-only artifacts are generated in-test from immutable original
        # lexical bytes and explicit development bindings, with no API admission
        # claim. Production build() always recomputes actual public receipts.
        import importlib
        from test_pack_graph_scenarios import ScenarioProfileTests
        from prepare_pack_graph_scenarios import project
        from run_graph_release_graphframes import columns
        result=[]
        for pack in ('archaeology','ecology'):
            v,g,n,a=ScenarioProfileTests().fixture(pack)
            converter=importlib.import_module(pack+'_source_transaction')
            model=base64.b64decode(a['original_model_base64']);graph=base64.b64decode(a['original_graph_base64'])
            batch,binding=converter.build_transaction(model,graph,source_system='private-original-'+pack+'-fixture')
            a['original_bindings_base64']=base64.b64encode(json.dumps(binding).encode()).decode()
            assigned={e['originalKey']:e for e in binding['entities']if e['kind']=='edge'}
            for i,e in enumerate(g['edges']):
                row={name:None for name in columns('edge')};b=assigned[e['key']];row.update(id=b['id'],rel_type_id=b['type_id'],graph_id='edge-'+str(i),src='source',dst='target');v['edges'].append(row)
            result.append(project(pack,v,g,n,a))
        return result
    def test_complete_original_carriers_and_finite_integer_cells(self):
        for p in self.profiles():
            nodes,edges,longs=entries(p)
            self.assertEqual(len(nodes),len(p['nodes']));self.assertEqual(len(edges),len(p['edges']))
            self.assertEqual([r['props_json']for r in nodes],[r['props_json']for r in p['nodes']])
            self.assertEqual(len(longs),2 if p['pack']=='archaeology' else 1)
            self.assertTrue(all(e['original_key']for e in edges))
            for r in nodes:
                self.assertEqual(r['carrier_id'],r['id']);self.assertEqual(r['carrier_key'],r['graph_id'])
    def test_long_bool_float_and_capacity_refuse(self):
        p=self.profiles()[0];column=next(f['signed64_column']for f in p['fields']if f['signed64_column'])
        for value in (True,1.0,2**63):
            mutated=copy.deepcopy(p);mutated['nodes'][0][column]=value
            with self.assertRaises(ValueError):entries(mutated)
        mutated=copy.deepcopy(p);mutated['edges'][0]['rel_type_id']='unknown'
        with self.assertRaises(ValueError):entries(mutated)
    def test_native_duckdb_complete_parity_and_model_types(self):
        try:import duckdb
        except ImportError:self.skipTest('Existing optional DuckDB dependency required')
        from unittest.mock import patch
        import tempfile
        profiles=self.profiles()
        with tempfile.TemporaryDirectory()as temporary,patch('prepare_pack_scenario_puppygraph.prepare',side_effect=profiles):
            out=Path(temporary)/'fresh';receipt=build([{'pack':'archaeology'},{'pack':'ecology'}],'not-invoked',out)
            self.assertFalse(receipt['engine_executed']);model=json.loads((out/'model.json').read_bytes())
            self.assertEqual(len(model['node']),2);self.assertEqual(len(model['edge']),2)
            self.assertEqual(sum(a['type']=='LONG'for n in model['node']for a in n['attribute']),3)
            with duckdb.connect(str(out/'scenarios.duckdb'),read_only=True)as c:
                self.assertEqual(c.execute('SELECT count(*) FROM carrier.archaeology_nodes').fetchone()[0],39)
                self.assertEqual(c.execute('SELECT count(*) FROM carrier.ecology_edges').fetchone()[0],54)
            self.assertEqual((out/'scenarios.duckdb').stat().st_mode&0o777,0o444)
