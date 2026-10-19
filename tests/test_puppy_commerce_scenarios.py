# SPDX-License-Identifier: MIT OR Apache-2.0
import json, unittest, hashlib
from pathlib import Path
from contextlib import contextmanager
from puppy_commerce_fixture import admission
from ashlar_host.puppy_commerce_scenarios import execute_commerce_scenarios, PuppyCommerceScenarioConfig
ROOT=Path(__file__).resolve().parents[1]/'docs/helix/04-build/evidence/commerce-exact-puppygraph-20261009'
def config(language):
    root=ROOT.parent/'commerce-graph-export-20261009'
    release=(root/'release.graph.json').read_bytes();custody=(root/'custody.json').read_bytes()
    pack=(ROOT.parents[4]/'examples/domain-packs/commerce/upstream/pack.json').read_bytes()
    return PuppyCommerceScenarioConfig(language,release,custody,hashlib.sha256(release).hexdigest(),hashlib.sha256(custody).hexdigest(),pack)
class Policy:
    def __init__(self):self.active=False;self.closed=False
    @contextmanager
    def hold(self,context):
        self.active=True
        try:yield
        finally:self.active=False;self.closed=True
    def admit_exact_commerce(self,p,c):assert self.active
    def renew_exact_commerce(self,p,c):assert self.active
class ScenarioTests(unittest.TestCase):
    def test_qualified_original_queries_and_all_records_unchanged(self):
        prepared=json.loads((ROOT/'receipt.json').read_bytes())
        for language in ('Cypher','Gremlin'):
            report=json.loads((ROOT/(language.lower()+'-report.json')).read_bytes());records=iter(report['records']);policy=Policy();seen=[]
            class Transport:
                def query(self,lang,query,context):
                    original=next(records);self.assert_language=lang
                    self.original=original
                    if query!=original.get('query',original.get('script')):raise AssertionError('Original qualified query changed')
                    return original['native_rows']
            result=execute_commerce_scenarios(prepared,config(language),Transport(),policy,None,observe=seen.append,source_admission=admission(self))
            self.assertEqual(result['actual'],prepared['expected']);self.assertEqual(result['records'],report['records']);self.assertTrue(policy.closed)
    def test_original_cancel_and_closing_attempted(self):
        prepared=json.loads((ROOT/'receipt.json').read_bytes());policy=Policy();cancel=KeyboardInterrupt()
        class Transport:
            def query(self,*args):raise cancel
        with self.assertRaises(KeyboardInterrupt)as caught:execute_commerce_scenarios(prepared,config('Cypher'),Transport(),policy,None,observe=lambda v:None,source_admission=admission(self))
        self.assertIs(caught.exception,cancel);self.assertTrue(policy.closed)

    def test_changed_complete_projection_refuses_before_transport(self):
        prepared=json.loads((ROOT/'receipt.json').read_bytes());prepared['nodes'][0]['retained_json']='{}'
        class Transport:
            def query(self,*a):raise AssertionError('transport acquired')
        with self.assertRaises(ValueError):execute_commerce_scenarios(prepared,config('Cypher'),Transport(),Policy(),None,observe=lambda v:None,source_admission=admission(self))

    def test_unfiltered_intermediate_failure_is_retained_and_stops_filter_query(self):
        prepared=json.loads((ROOT/'receipt.json').read_bytes());report=json.loads((ROOT/'cypher-report.json').read_bytes());records=iter(report['records']);seen=[];policy=Policy()
        class Transport:
            def query(self,*a):
                item=next(records)
                return []if item['case']=='partial-intermediates'else item['native_rows']
        with self.assertRaises(ValueError):execute_commerce_scenarios(prepared,config('Cypher'),Transport(),policy,None,observe=seen.append,source_admission=admission(self))
        self.assertEqual([v['case']for v in seen],['full-nodes','full-edges','partial-intermediates']);self.assertTrue(policy.closed)

if __name__=='__main__':unittest.main()
