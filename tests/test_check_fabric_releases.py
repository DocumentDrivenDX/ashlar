# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure Fabric query/oracle controls; no live engine qualification."""
import copy,json,tempfile,unittest
from pathlib import Path
from check_fabric_releases import cases,verify,identifier,check

class Tests(unittest.TestCase):
    def test_parallel_self_loop_distinct_edge_oracle(self):
        value={'nodes':[{'graph_id':'a'},{'graph_id':'b'},{'graph_id':'isolated'}],'edges':[{'graph_id':'x','src':'a','dst':'b'},{'graph_id':'y','src':'a','dst':'b'},{'graph_id':'z','src':'a','dst':'a'}]}
        corpus=cases(value);self.assertEqual(len(corpus),7)
        self.assertEqual([r['second_edge'] for r in corpus[3][2]],['x','y'])
        self.assertEqual([r['second_edge'] for r in corpus[4][2]],['x','y','z'])
        self.assertTrue(corpus[4][1].startswith('MATCH WALK '))
        self.assertEqual([r for r in corpus[4][2] if r['first_edge']==r['second_edge']],[dict(source='a',first_edge='z',middle='a',second_edge='z',destination='a')])
        self.assertEqual(corpus[5][2],[{'row_count':3}]);self.assertEqual(corpus[6][2],[{'row_count':3}])
        self.assertNotIn(' AS edge,',corpus[2][1]);self.assertIn('relationship_id',corpus[2][1])
    def test_exact_null_text_and_multiplicity(self):
        expected=[{'x':None},{'x':' {"n":9007199254740993} 雪 '},{'x':None}]
        body={'status':{'code':'00000'},'result':{'kind':'TABLE','columns':[{'name':'x','gqlType':'STRING'}],'data':list(reversed(expected))}}
        verify(body,expected,('x',))
        for mutate in [lambda b:b['result']['data'].pop(),lambda b:b['result']['data'][1].update(x=9007199254740993),lambda b:b.update(nextPage='opaque'),lambda b:b['result'].update(nextPage='opaque'),lambda b:b['status'].update(cause={'code':'02000'}),lambda b:b.update(additionalStatuses=[{'code':'01000'}]),lambda b:b['status'].update(code='42000')]:
            bad=copy.deepcopy(body);mutate(bad)
            with self.assertRaises(ValueError):verify(bad,expected,('x',))
    def test_native_failure_retained_without_retry(self):
        root=Path(__file__).resolve().parents[1]/'docs/helix/04-build/evidence/puppygraph-releases-20261009'
        digests=['ca4b40f78b1a27e3d6df50eb17816ec49e30f0f70d6ff7dabc195307c07642e7','a6a4cb268f6bb3f8d193b35289d5b593ad08e33d8fc561e29487919fd1502e3a']
        inputs={n:(root/(d+'.graph.json'),d) for n,d in zip(('R1','R2'),digests)}
        graphs={'R1':'8947b7a0-130b-4a9a-9597-1d8a326a6f23','R2':'fe883e8f-7f7d-42d0-a58d-299eafbb4e36'};calls=[]
        def transport(*args):calls.append(args);return 200,{'status':{'code':'42000'},'nativeFailure':'preserved'}
        with tempfile.TemporaryDirectory() as temp:
            out=Path(temp)/'report.json'
            with self.assertRaises(ValueError):check(inputs,'0a4dbd9c-e1a7-4ec8-8ce9-aa51991ff793',graphs,out,transport=transport)
            report=json.loads(out.read_text());self.assertFalse(report['allPassed']);self.assertEqual(report['records'][0]['response']['nativeFailure'],'preserved');self.assertEqual(len(calls),1)
            with self.assertRaises(ValueError):check(inputs,'0a4dbd9c-e1a7-4ec8-8ce9-aa51991ff793',graphs,out,transport=transport)
            self.assertEqual(len(calls),1)
    def test_canonical_identifier(self):
        good='0a4dbd9c-e1a7-4ec8-8ce9-aa51991ff793';self.assertEqual(identifier(good),good)
        for bad in [good.upper(),good+'/',None]:
            with self.assertRaises(ValueError):identifier(bad)

if __name__=='__main__':unittest.main()
