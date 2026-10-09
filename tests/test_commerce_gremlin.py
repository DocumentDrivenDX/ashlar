import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from check_commerce_gremlin import projection
from run_graph_release_graphframes import columns
class GremlinProjectionTests(unittest.TestCase):
    def test_every_exact_carrier_and_native_incidence(self):
        for kind in ('node','edge'):
            script=projection(kind)
            for name in columns(kind):self.assertIn(repr(name),script)
            self.assertIn("__.values('carrier_id')",script);self.assertIn("__.values('carrier_key')",script)
            self.assertIn('__.constant(null)',script)
            if kind=='edge':
                self.assertIn('__.outV().id()',script);self.assertIn('__.inV().id()',script)
    def test_no_scalar_casting(self):
        for kind in ('node','edge'):
            script=projection(kind)
            self.assertNotIn('parse',script);self.assertNotIn('toDouble',script)
if __name__=='__main__':unittest.main()

class RetainedNativeGremlinTests(unittest.TestCase):
    def test_complete_native_original_walk_bags_and_full_key(self):
        import json,hashlib
        from run_graph_release_graphframes import load_release
        from run_commerce_release_graphframes import original,query_oracle,bag
        from private_graph_custody import PROFILE
        root=Path(__file__).resolve().parents[1];p=root/'docs/helix/04-build/evidence/commerce-graph-export-20261009'
        r=(p/'release.graph.json').read_bytes();c=(p/'custody.json').read_bytes()
        value=load_release(r,hashlib.sha256(r).hexdigest(),custody_profile=PROFILE,custody_payload=c,trusted_custody_sha256=hashlib.sha256(c).hexdigest())
        graph,*_=original(value);expected=query_oracle(graph)
        report=json.loads((root/'docs/helix/04-build/evidence/commerce-gremlin-common-20261009/query-report.json').read_text())
        for name,fields in [('one-hop',['a','e','b']),('two-hop',['a','e','b','f','c'])]:
            record=next(r for r in report['records'] if r['case']==name)
            self.assertEqual(bag([[r[f] for f in fields] for r in record['native_rows']]),bag(expected[name]))
        singleton=next(r for r in report['records'] if r['case']=='singleton')
        self.assertEqual(singleton['bindings']['originalKey'],expected['singleton'][0])
        self.assertIn(".filter(__.values('original_key').is(originalKey))",singleton['script'])
        self.assertNotIn('.order()',singleton['script']);self.assertEqual(len(singleton['native_rows']),1)
        limit=next(r for r in report['records'] if r['case']=='filtered-limit')
        self.assertIn('.limit(1)',limit['script']);self.assertEqual(len(limit['native_rows']),1)
        total=next(r for r in report['records'] if r['case']=='filtered-total')
        self.assertEqual(total['native_rows'],[2])
