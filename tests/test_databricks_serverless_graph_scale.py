# SPDX-License-Identifier: MIT OR Apache-2.0
"""Generator/lifecycle controls only; no Spark or Databricks calls."""
import hashlib,json
from pathlib import Path
from types import SimpleNamespace
import unittest
from prepare_databricks_serverless_graph_scale import PROFILE,prepare_serverless,run_probe

LAYOUT=(Path(__file__).resolve().parents[1]/'sql/ashlar-delta-v03/runtime-carriers.generated.json').read_bytes()
class ServerlessTests(unittest.TestCase):
    def config(self):return dict(profile=PROFILE,catalog='client_dev',schema='ashlar_scale_tiny_initial',stage='expanded-explicit',seed=17,deadline_seconds=180,whole_seconds=360,performance_target='PERFORMANCE_OPTIMIZED',notebook_path='/Users/operator/ashlar-scale-probe',job_name='ashlar-scale-initial',maximum_cost_usd='25.00',layout_sha256=hashlib.sha256(LAYOUT).hexdigest())
    def prepare(self,config=None):return prepare_serverless(json.dumps(config or self.config()).encode(),LAYOUT)
    def test_single_serverless_task_reuses_original_plan_oracles(self):
        v=self.prepare();task=v['job_settings']['tasks'][0]
        self.assertEqual(v['plan']['config']['nodes'],1000000);self.assertEqual(v['plan']['config']['edges'],10000000)
        self.assertEqual(v['job_settings']['timeout_seconds'],360);self.assertEqual(task['timeout_seconds'],360)
        self.assertIn('deadline_seconds=180',v['notebook'])
        self.assertEqual(v['job_settings']['performance_target'],'PERFORMANCE_OPTIMIZED')
        original=self.config();original.pop('performance_target');original['whole_seconds']=480
        self.assertEqual(self.prepare(original)['job_settings']['performance_target'],'STANDARD')
        self.assertEqual(task['max_retries'],0);self.assertTrue(task['disable_auto_optimization'])
        self.assertNotIn('existing_cluster_id',task);self.assertNotIn('warehouse_id',json.dumps(v['job_settings']))
        self.assertEqual(len(v['plan']['queries']),7);self.assertEqual(v['plan']['measurement']['warm_runs'],1)
        self.assertEqual(next(q['expected'] for q in v['plan']['queries'] if q['name']=='isolate-two-hop'),[])
        compile(v['notebook'],'generated-notebook.py','exec')
        expanded=self.config();expanded['stage']='initial'
        large=self.prepare(expanded)
        self.assertEqual((large['plan']['config']['nodes'],large['plan']['config']['edges']),(100000,1000000))
    def test_expansion_deadlines_unknown_compute_refused_before_effects(self):
        for change in ({'stage':'implicit-expansion'},{'deadline_seconds':181},{'catalog':'other'},{'compute':'shared'},{'maximum_cost_usd':'100.00'},{'nodes':1000001},{'whole_seconds':361},{'performance_target':'FAST'}):
            c=self.config();c.update(change)
            with self.subTest(change=change),self.assertRaises(ValueError):self.prepare(c)
    def test_setup_failure_does_not_continue_ingest_or_retry(self):
        calls=[];original=KeyboardInterrupt('selected cancellation')
        def sql(statement):calls.append(statement);raise original
        plan=self.prepare()['plan']
        conf=SimpleNamespace(set=lambda *args:None,get=lambda key:'UTC' if key=='spark.sql.session.timeZone' else '90')
        with self.assertRaises(KeyboardInterrupt) as got:run_probe(SimpleNamespace(sql=sql,conf=conf),plan,json.loads(LAYOUT),deadline_seconds=150,progress=lambda _:None)
        self.assertIs(got.exception,original);self.assertEqual(calls,[plan['setup'][0]])

    def test_non_utc_readback_refuses_before_native_effects(self):
        calls=[]
        spark=SimpleNamespace(conf=SimpleNamespace(set=lambda *args:None,get=lambda key:'not-selected'),sql=lambda s:calls.append(s))
        with self.assertRaises(ValueError):run_probe(spark,self.prepare()['plan'],json.loads(LAYOUT),deadline_seconds=180,progress=lambda _:None)
        self.assertEqual(calls,[])
