# SPDX-License-Identifier: MIT OR Apache-2.0
"""Inert gate tests; never start Spark, Weft, native operations or SQL."""
import json,os,tempfile,unittest
from pathlib import Path
from r02_live_alias_gate import EXPECTED_JARS,R02LiveAliasReviewGate

class LiveAliasGateTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.root.chmod(0o700)
        self.attempt='a'*32;self.app='local-test-app';self.source='b'*40
        self.vector={'publicationId':'publication-test','manifestUuid':'11111111-1111-4111-8111-111111111111','tables':{
          'edge_current':{'uuid':'22222222-2222-4222-8222-222222222222','version':5},
          'object_current':{'uuid':'33333333-3333-4333-8333-333333333333','version':3},
          'tombstone':{'uuid':'44444444-4444-4444-8444-444444444444','version':3},
          'whole_source_history':{'uuid':'55555555-5555-4555-8555-555555555555','version':4}}}
        namespace='ashlar_configured_query_'+'c'*32
        self.aliases={r:'spark_catalog.'+namespace+'.'+r for r in self.vector['tables']}
        self.runtime={'weftVersion':'0.4.53','weftWheelSha256':'4c8281945b852af02cae8602747fb7b1b652ab3a1369d7650efce62ae6d35c6c',
          'weftSourceCommit':'726a840e956e25217d6aeea2674fdfcf3f342a24','weftIndexSha256':'f'*64,
          'cohortReceiptSha256':'fe48796cead974bc06821bd296428f47b389e1016a7b43ff186ec15cb03f72e5',
          'pythonVersion':'3.12.14','jdkVersion':'21','sparkVersion':'4.2.0','deltaVersion':'4.4.1','jarSha256':EXPECTED_JARS}
        self.live=[]
    def tearDown(self):self.tmp.cleanup()
    def gate(self,stage='pre-sql'):
        return R02LiveAliasReviewGate(self.root,attempt_id=self.attempt,stage=stage,source_revision=self.source,
          spark_application_id=self.app,vector=self.vector,aliases=self.aliases,runtime=self.runtime,
          live_check=lambda:self.live.append('renew'),deadline_seconds=5)
    def compiled(self,name,wrapped=False,after=None):
        tables=[{'name':self.aliases[r].split('.'),'uuid':v['uuid'],'version':v['version']}for r,v in sorted(self.vector['tables'].items())]
        binding={'publication':{'id':self.vector['publicationId'],'manifestUuid':self.vector['manifestUuid'],'tables':tables}}
        raw=json.dumps(binding,sort_keys=True,separators=(',',':'))
        target={'bindingJson':raw,'bindingSha256':__import__('hashlib').sha256(raw.encode()).hexdigest()}
        request={'target':target,'sql':'SELECT 1'}
        if wrapped:request={'version':'weft.engine-page/0.1','compiler':'ashlar-spark4-pages','request':request,'pageSize':2}
        req=json.dumps(request,separators=(',',':')).encode()
        resp={'status':'compiled','artifact':{'sql':'SELECT 1','bindingSha256':target['bindingSha256']}}
        if not wrapped:resp['bindingSha256']=target['bindingSha256'];resp['sql']='SELECT 1'
        return {'request':req,'response':json.dumps(resp,separators=(',',':')).encode(),'expected':[['expected']]}
    def approve(self,gate):
        receipt={'profile':'ashlar-r02-live-alias-review-receipt/0.1','decision':'approve','reviewer':'Sol',
          'attemptId':gate.attempt_id,'stage':gate.stage,'manifestSha256':gate.manifest_sha,'pid':gate.pid,
          'sparkApplicationId':gate.app,'publicationVectorSha256':gate.vector_sha,'aliasMapSha256':gate.alias_sha,'runtimeSha256':gate.runtime_sha}
        (self.root/(gate.stage+'-review.json')).write_text(json.dumps(receipt),encoding='utf-8')
    def stage_one(self):
        return {'singleton':{'request':b'{"selector":"person-1"}','expected':{'id':'person-1'}},
          'backend03-original':self.compiled('backend03-original'),
          'backend03-positive':self.compiled('backend03-positive'),
          'backend02-page1':{**self.compiled('backend02-page1',wrapped=True),
            'programResponse':self.compiled('backend02-page1',wrapped=True)['response'],
            'expected':{'rows':[['person-1'],['person-2']],'continuationAfter':'["person-2"]'}}}
    def run_all(self, gate, names, *, outcome=None):
        def execute(execute_one):
            values=[]
            for name in names:
                values.append(execute_one(name,lambda n=name:n))
            return outcome if outcome is not None else values
        return gate.execute_after_review(self.root/(gate.stage+'-review.json'),execute)
    def test_stage_one_requires_exact_artifact_set_and_live_binding(self):
        g=self.gate();bad=self.stage_one();bad.pop('backend02-page1')
        with self.assertRaises(PermissionError):g.freeze(bad)
        g=self.gate();path,digest=g.freeze(self.stage_one())
        self.assertTrue(path.exists());self.assertEqual(len(digest),64);self.assertTrue(self.live)
        manifest=json.loads(path.read_text());page1=manifest['artifacts'][3]
        self.assertEqual(page1['programResponseSha256'],__import__('hashlib').sha256(self.stage_one()['backend02-page1']['programResponse']).hexdigest())
        self.assertTrue((self.root/'pre-sql-backend02-page1-program-response.json').exists())
        self.approve(g);hit=[]
        result=self.run_all(g,('singleton','backend03-original','backend03-positive','backend02-page1'),
            outcome={'continuationAfter':'["person-2"]'})
        self.assertEqual(result,{'continuationAfter':'["person-2"]'})
        with self.assertRaises(PermissionError):
            self.run_all(g,('singleton','backend03-original','backend03-positive','backend02-page1'))
    def test_page2_gate_binds_reviewed_page1_token_and_prior_gate(self):
        g=self.gate();g.freeze(self.stage_one());self.approve(g)
        self.run_all(g,('singleton','backend03-original','backend03-positive','backend02-page1'),
            outcome={'continuationAfter':'["person-2"]'})
        g.next_stage_after_page1(deadline_seconds=60)
        item=self.compiled('backend02-continuation',wrapped=True);item['after']='["person-2"]'
        path,_=g.freeze({'backend02-continuation':item})
        manifest=json.loads(path.read_text());self.assertEqual(manifest['page1AfterSha256'],__import__('hashlib').sha256(b'["person-2"]').hexdigest())
        self.assertEqual(manifest['priorStageManifestSha256'],g.prior_stage_sha)
        self.approve(g);hit=[]
        def execute_page2(execute_one):
            hit.append(execute_one('backend02-continuation',lambda:'page2'))
        g.execute_after_review(self.root/'post-page1-review.json',execute_page2)
        self.assertEqual(hit,['page2'])
        with self.assertRaises(PermissionError):
            g.execute_after_review(self.root/'post-page1-review.json',execute_page2)
    def test_missing_or_mismatched_review_never_calls_executor(self):
        g=self.gate();g.freeze(self.stage_one());hit=[]
        with self.assertRaises(PermissionError):g.execute_after_review(self.root/'pre-sql-review.json',lambda run:hit.append('SQL'))
        self.assertEqual(hit,[])
    def test_stage_one_cannot_include_continuation_and_callback_failure_consumes_review(self):
        g=self.gate();g.freeze(self.stage_one());self.approve(g)
        def bad(execute_one):
            execute_one('backend02-continuation',lambda:'must not run')
        with self.assertRaises(PermissionError):g.execute_after_review(self.root/'pre-sql-review.json',bad)
        self.assertTrue(g.consumed)
        with self.assertRaises(PermissionError):g.execute_after_review(self.root/'pre-sql-review.json',bad)
        self.assertFalse(g.stage_succeeded)
    def test_page2_transition_requires_successful_page1_result(self):
        g=self.gate();g.freeze(self.stage_one())
        with self.assertRaises(PermissionError):g.next_stage_after_page1(deadline_seconds=60)

    def test_page2_transition_resets_gate_to_fresh_bounded_review_window(self):
        from unittest.mock import patch
        g=self.gate();g.freeze(self.stage_one());self.approve(g)
        self.run_all(g,('singleton','backend03-original','backend03-positive','backend02-page1'),
            outcome={'continuationAfter':'["person-2"]'})
        with patch('r02_live_alias_gate.time.monotonic',return_value=200.0):
            g.next_stage_after_page1(deadline_seconds=90)
        self.assertEqual(g.stage,'post-page1')
        self.assertEqual(g.deadline,290.0)
        with self.assertRaises(PermissionError):g.next_stage_after_page1(deadline_seconds=91)
    def test_absolute_stage_deadlines_do_not_drift_after_review_manifest(self):
        from unittest.mock import patch
        import time
        start=time.monotonic()
        with patch('r02_live_alias_gate.time.monotonic',return_value=start):
            g=R02LiveAliasReviewGate(self.root,attempt_id=self.attempt,stage='pre-sql',
                source_revision=self.source,spark_application_id=self.app,vector=self.vector,
                aliases=self.aliases,runtime=self.runtime,live_check=lambda:None,
                deadline_monotonic=start+155.0)
        self.assertEqual(g.deadline,start+155.0)
        g.freeze(self.stage_one());self.approve(g)
        self.run_all(g,('singleton','backend03-original','backend03-positive','backend02-page1'),
            outcome={'continuationAfter':'["person-2"]'})
        stage2_start=time.monotonic();stage2_deadline=stage2_start+90.0
        with patch('r02_live_alias_gate.time.monotonic',return_value=stage2_start):
            g.next_stage_after_page1(deadline_monotonic=stage2_deadline)
        self.assertEqual(g.deadline,stage2_deadline)
    def test_wrong_runtime_or_alias_namespace_refuses(self):
        with self.assertRaises(PermissionError):R02LiveAliasReviewGate(self.root,attempt_id=self.attempt,stage='pre-sql',source_revision=self.source,spark_application_id=self.app,vector=self.vector,aliases=self.aliases,runtime={**self.runtime,'weftVersion':'0.4.48'},live_check=lambda:None)
        wrong=dict(self.aliases);wrong['edge_current']='spark_catalog.other.edge_current'
        with self.assertRaises(PermissionError):R02LiveAliasReviewGate(self.root,attempt_id=self.attempt,stage='pre-sql',source_revision=self.source,spark_application_id=self.app,vector=self.vector,aliases=wrong,runtime=self.runtime,live_check=lambda:None)

if __name__=='__main__':unittest.main()
