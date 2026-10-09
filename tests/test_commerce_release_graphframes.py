import copy,hashlib,json,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from run_commerce_release_graphframes import original,query_oracle,bag,load_release,PROFILE
ROOT=Path(__file__).resolve().parents[1]
class CommerceGraphFramesTests(unittest.TestCase):
    def admitted(self):
        p=ROOT/'docs/helix/04-build/evidence/commerce-graph-export-20261009';r=(p/'release.graph.json').read_bytes();c=(p/'custody.json').read_bytes()
        return load_release(r,hashlib.sha256(r).hexdigest(),custody_profile=PROFILE,custody_payload=c,trusted_custody_sha256=hashlib.sha256(c).hexdigest())
    def test_original_complete_carriers(self):
        graph,nodes,edges,admission=original(self.admitted())
        self.assertEqual((len(nodes),len(edges)),(11,10));self.assertEqual(admission['declared_umf'],'0.8.0')
        self.assertEqual(set(nodes.values()),{o['key'] for o in graph['objects']})
    def test_carrier_value_and_missing_row_refuse(self):
        for mutation in ['props','missing']:
            v=self.admitted()
            if mutation=='props':v['nodes'][0]['props_json']='{}'
            else:v['edges'].pop()
            with self.assertRaises(ValueError):original(v)
    def test_walk_bags_preserve_parallel_occurrences_and_cycles(self):
        graph={'objects':[{'key':'A','type':{'element':'T'}},{'key':'B','type':{'element':'T'}}], 'edges':[
          {'key':'x','source':'A','target':'B','relationship':{'element':'R'}},
          {'key':'y','source':'A','target':'B','relationship':{'element':'R'}},
          {'key':'z','source':'B','target':'A','relationship':{'element':'R'}}]}
        result=query_oracle(graph)
        self.assertEqual(len(result['one-hop']),3);self.assertEqual(len(result['two-hop']),4)
        self.assertIn(['A','x','B','z','A'],result['two-hop'])
        self.assertIn(['A','{"element":"R"}',2,1],result['grouped-count'])
        self.assertEqual(result['filtered-limit'],['A','B']);self.assertEqual(result['filtered-total'],2)
    def test_observable_limit(self):
        g={'objects':[{'key':k,'type':{'element':'T'}} for k in ['C','B','A']], 'edges':[]}
        r=query_oracle(g);self.assertEqual(r['singleton'],['A']);self.assertEqual(r['filtered-limit'],['A','B']);self.assertEqual(r['filtered-total'],3)
if __name__=='__main__':unittest.main()

class NativeLimitSelectionTests(unittest.TestCase):
    def test_deterministic_largest_original_type(self):
        from check_commerce_graphframes_limit import selection
        graph={'objects':[{'key':'A','type':{'element':'Single'}},{'key':'C','type':{'element':'Many'}},{'key':'B','type':{'element':'Many'}}]}
        _,typ,keys=selection(graph)
        self.assertEqual(typ,'{"element":"Many"}');self.assertEqual(keys,['B','C'])
    def test_no_observable_truncation_refuses(self):
        from check_commerce_graphframes_limit import selection
        with self.assertRaises(ValueError):selection({'objects':[{'key':'A','type':{'element':'Single'}}]})
