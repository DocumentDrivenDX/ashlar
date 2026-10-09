import json,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from prepare_graph_augmentations_puppygraph import source_carriers
ROOT=Path(__file__).resolve().parents[1]/'examples/graph-augmentations'
class AuthoredPuppyCarrierTests(unittest.TestCase):
    def carriers(self):return source_carriers(json.loads((ROOT/'source.json').read_bytes()),(ROOT/'model.umf.json').read_bytes())
    def test_null_absence_empty_zero_false_remain_distinct(self):
        rows,_,_=self.carriers();by_key={r['carrier_key']:r for r in rows}
        self.assertEqual((by_key['presence-absent']['text_state'],by_key['presence-absent']['text_value_json']),('absent',None))
        self.assertEqual((by_key['presence-null']['text_state'],by_key['presence-null']['text_value_json']),('present','null'))
        self.assertEqual(by_key['presence-present-empty-string']['text_token'],'')
        self.assertEqual(by_key['presence-present-zero']['integer_token'],'0')
        self.assertEqual(by_key['presence-present-false']['boolean_token'],'false')
    def test_large_integer_signedzero_unicode_and_parallel_edges_exact(self):
        rows,edges,_=self.carriers()
        tokens={r['integer_token']for r in rows if r['integer_state']=='present'}
        self.assertIn('-9007199254740993',tokens);self.assertIn('9007199254740993',tokens)
        self.assertIn('-0.00',{r['decimal_token']for r in rows})
        self.assertIn('é',{r['text_token']for r in rows});self.assertIn('é',{r['text_token']for r in rows})
        self.assertEqual([e['carrier_key']for e in edges if e['src']=='A1'and e['dst']=='B1'],['E1','E2'])
    def test_missing_ordered_declaration_members_refuse(self):
        fixture=json.loads((ROOT/'source.json').read_bytes());fixture['nodes'][0]['values'].reverse()
        with self.assertRaises(ValueError):source_carriers(fixture,(ROOT/'model.umf.json').read_bytes())
if __name__=='__main__':unittest.main()

class RetainedSharedNativePuppyTests(unittest.TestCase):
    def test_both_native_languages_preserve_all_twenty_one_scalar_state_queries(self):
        base=ROOT.parents[1]/'docs/helix/04-build/evidence/graph-augmentations-puppygraph-20261009'
        fixture=json.loads((ROOT/'source.json').read_bytes());nodes,_,_=source_carriers(fixture,(ROOT/'model.umf.json').read_bytes());by_key={r['carrier_key']:r for r in nodes}
        for filename in ['cypher-report.json','gremlin-report.json']:
            report=json.loads((base/filename).read_bytes());records=[r for r in report['records']if r['case'].startswith(('A-SCALAR:','A-PRESENCE:'))]
            self.assertEqual(len(records),21)
            for r in records:
                key=r['case'].split(':',1)[1];source=next(n for n in fixture['nodes']if n['key']==key)
                member=next(m for m in source['values']if m['field']['element']!='id'and m['state']=='present')if source['case']=='A-SCALAR'else next(m for m in source['values']if m['field']['element']=={'presence-absent':'text','presence-null':'text','presence-present-empty-string':'text','presence-present-zero':'integer','presence-present-false':'boolean'}[key])
                field=member['field']['element'];carrier=by_key[key]
                self.assertEqual(r['native_rows'],[{'state':carrier[field+'_state'],'value_json':carrier[field+'_value_json'],'token':carrier[field+'_token']}])
