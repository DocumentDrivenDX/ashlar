import hashlib,json,sys,tempfile,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from prepare_commerce_puppygraph import prepare
ROOT=Path(__file__).resolve().parents[1]
class CommercePuppyPreparationTests(unittest.TestCase):
    def inputs(self):
        p=ROOT/'docs/helix/04-build/evidence/commerce-graph-export-20261009';r=(p/'release.graph.json').read_bytes();c=(p/'custody.json').read_bytes()
        return r,c,hashlib.sha256(r).hexdigest(),hashlib.sha256(c).hexdigest()
    def test_exact_carriers_original_metadata_and_single_handle(self):
        import duckdb
        with tempfile.TemporaryDirectory() as d:
            out=Path(d)/'prepared';receipt=prepare(*self.inputs(),out)
            self.assertTrue(receipt['full_carrier_parity'])
            c=duckdb.connect(str(out/'commerce.duckdb'),read_only=True)
            try:
                self.assertEqual(c.execute('SELECT count(*) FROM carrier.nodes').fetchone()[0],11)
                self.assertEqual(c.execute('SELECT count(*) FROM carrier.edges').fetchone()[0],10)
                self.assertEqual(c.execute('SELECT count(DISTINCT original_key) FROM carrier.nodes').fetchone()[0],11)
            finally:c.close()
            model=json.loads((out/'model.json').read_text())
            self.assertEqual([n['label'] for n in model['node']],['CommerceNode'])
            queries=json.loads((out/'queries.json').read_text())
            self.assertEqual(queries['expected']['filtered-total'],2)
            self.assertEqual(len(queries['expected']['filtered-limit']),1)
            self.assertIn('LIMIT 1',queries['queries']['filtered-limit'])
    def test_wrong_pair_refuses_before_output(self):
        r,c,rs,cs=self.inputs()
        with tempfile.TemporaryDirectory() as d:
            out=Path(d)/'no-output'
            with self.assertRaises(ValueError):prepare(r,c,rs,'0'*64,out)
            self.assertFalse(out.exists())
if __name__=='__main__':unittest.main()

class CommerceSchemaAdditionTests(unittest.TestCase):
    def test_preserves_prior_labels_and_refuses_collision(self):
        from activate_commerce_puppygraph import merged_model
        prior={'catalog':[{'name':'old'}],'node':[{'label':'R1Node'}],'edge':[]}
        addition={'catalog':[{'name':'new'}],'node':[{'label':'CommerceNode'}],'edge':[]}
        result=merged_model(prior,addition)
        self.assertEqual(result['node'],prior['node']+addition['node'])
        self.assertEqual(prior['node'],[{'label':'R1Node'}])
        addition['node']=[{'label':'R1Node'}]
        with self.assertRaises(ValueError):merged_model(prior,addition)
