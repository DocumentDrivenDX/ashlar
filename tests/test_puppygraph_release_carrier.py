import hashlib,json,tempfile,unittest
from pathlib import Path
from prepare_puppygraph_releases import prepare
ROOT=Path(__file__).resolve().parents[1]
BASE=ROOT/'docs/helix/04-build/evidence/puppygraph-releases-20261009'
class Tests(unittest.TestCase):
    def inputs(self):
        return {name:(BASE/(digest+'.graph.json'),digest) for name,digest in json.loads((BASE/'receipt.json').read_bytes())['releases'].items()}
    def test_all_raw_carriers_and_nulls_survive_export(self):
        import duckdb
        with tempfile.TemporaryDirectory() as temp:
            out=Path(temp)/'new';inputs=self.inputs();receipt=prepare(inputs,out)
            with duckdb.connect(str(out/'releases.duckdb'),read_only=True) as db:
                for release,(path,digest) in inputs.items():
                    self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),digest)
                    original=json.loads(path.read_bytes())
                    for role in ['nodes','edges']:
                        cursor=db.execute('SELECT * FROM carrier.'+release.lower()+'_'+role)
                        names=[c[0] for c in cursor.description];rows=[dict(zip(names,row)) for row in cursor.fetchall()]
                        for row in rows:
                            self.assertEqual(row.pop('carrier_key'),row['graph_id']);self.assertEqual(row.pop('carrier_id'),row['id'])
                        bag=lambda xs:sorted(json.dumps(x,sort_keys=True) for x in xs)
                        self.assertEqual(bag(rows),bag(original[role]))
            self.assertEqual(hashlib.sha256((out/'releases.duckdb').read_bytes()).hexdigest(),receipt['databaseSha256'])
    def test_untrusted_release_digest_refuses_before_output_creation(self):
        with tempfile.TemporaryDirectory() as temp:
            out=Path(temp)/'new';inputs=self.inputs();path,_=inputs['R1'];inputs['R1']=(path,'0'*64)
            with self.assertRaises(ValueError):prepare(inputs,out)
            self.assertFalse(out.exists())

    def test_wrong_incident_vertex_refuses_with_unchanged_carrier_properties(self):
        from check_puppygraph_releases import assert_endpoints
        row={'src':'source-key','dst':'target-key'}
        assert_endpoints('R1',row,'R1Node[source-key]',{'@type':'g:String','@value':'R1Node[target-key]'})
        with self.assertRaises(ValueError):assert_endpoints('R1',row,'R1Node[source-key]','R1Node[other-target]')
        with self.assertRaises(ValueError):assert_endpoints('R1',row,'R2Node[source-key]','R1Node[target-key]')
