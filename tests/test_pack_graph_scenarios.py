import base64,copy,json,unittest
from pathlib import Path
from prepare_pack_graph_scenarios import project,signed64_token
from run_graph_release_graphframes import columns

class ScenarioProfileTests(unittest.TestCase):
    def fixture(self,pack):
        root=Path(__file__).resolve().parents[1]/'examples/domain-packs'/pack/'upstream'
        model=(root/'ontology.json').read_bytes();raw=(root/'graph/fixture.json').read_bytes();graph=json.loads(raw)
        nodes={str(i):o['key']for i,o in enumerate(graph['objects'])}
        rows=[]
        for key in nodes:
            row={n:None for n in columns('node')};row.update(graph_id=key,id=key,props_json='{"opaque":null}');rows.append(row)
        return {'nodes':rows,'edges':[]},graph,nodes,{'original_model_base64':base64.b64encode(model).decode(),'original_graph_base64':base64.b64encode(raw).decode()}
    def test_all_original_cases_and_raw_cells_remain(self):
        for pack,count in [('archaeology',8),('ecology',9)]:
            value,graph,nodes,a=self.fixture(pack);x=project(pack,value,graph,nodes,a)
            self.assertEqual(len(x['cases']),count);self.assertFalse(x['engine_executed'])
            self.assertTrue(all(r['props_json']=='{"opaque":null}'for r in x['nodes']))
            self.assertTrue(all(f['original_field']['kind']=='field'for f in x['fields']))
            self.assertEqual(len(x['fields']),len({f['lexical_column']for f in x['fields']}))
            self.assertTrue(x['integer_capacity_guards'])
    def test_presence_not_member_null_distinction(self):
        value,graph,nodes,a=self.fixture('ecology');x=project('ecology',value,graph,nodes,a)
        states={r[f['presence_column']]for r in x['nodes']for f in x['fields']}
        self.assertEqual(states,{'present','present-null','nonmember'})
        for r in x['nodes']:
            for f in x['fields']:
                if r[f['presence_column']]=='present':self.assertIsInstance(r[f['lexical_column']],str)
                else:self.assertIsNone(r[f['lexical_column']])
    def test_signed64_is_finite_representation(self):
        field={'scalarType':'integer'}
        self.assertEqual(signed64_token(str(2**53+1),field),2**53+1)
        for token in ('1.0','+1','01',str(2**63),str(-(2**63)-1),True,None):
            with self.assertRaises(ValueError):signed64_token(token,field)
        with self.assertRaises(ValueError):signed64_token('1',{'scalarType':'decimal'})
        self.assertEqual(signed64_token('-0',field),0)
    def test_changed_original_source_refuses(self):
        value,graph,nodes,a=self.fixture('archaeology');graph=copy.deepcopy(graph);graph['objects'][0]['values'].clear()
        with self.assertRaises(ValueError):project('archaeology',value,graph,nodes,a)
    def test_changed_public_receipt_refuses_before_release(self):
        from prepare_pack_graph_scenarios import prepare
        from unittest.mock import patch
        import tempfile,types
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);(root/'public-dataset.json').write_bytes(b'original')
            candidate=dict(pack='archaeology',publication=str(root),release='missing',release_sha256='a'*64,custody='missing',custody_sha256='b'*64)
            def changed(args,**kwargs):Path(args[-1]).write_bytes(b'changed');return types.SimpleNamespace(returncode=0)
            with patch('prepare_pack_graph_scenarios.subprocess.run',side_effect=changed),patch('prepare_pack_graph_scenarios.load_release')as load:
                with self.assertRaises(PermissionError):prepare(candidate,Path('original-public-api'))
                load.assert_not_called()
