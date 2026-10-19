# SPDX-License-Identifier: MIT OR Apache-2.0
"""Preparation/oracle controls only; no SDK, Spark or paid compute execution."""
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from prepare_databricks_graph_scale import (PROFILE,SHARED_WAREHOUSE,parse_configuration,
    prepare_plan,outgoing,measure_query,SparkSQLExecutor)

ROOT=Path(__file__).resolve().parents[1]
LAYOUT=(ROOT/'sql/ashlar-delta-v03/runtime-carriers.generated.json').read_bytes()


class ScaleTests(unittest.TestCase):
    def document(self):
        return dict(profile=PROFILE,catalog='client_dev',schema='fresh_layout_probe',
            compute=dict(kind='spark-cluster',id='explicit-dedicated-id',dedicated=True),
            scale=dict(stage='initial',nodes=100_000,edges=1_000_000),seed=17,
            limits=dict(statement_seconds=120,whole_seconds=900,maximum_rows=1024,
                maximum_bytes=1_048_576,maximum_cost_usd='10.00'),
            cache_mode='observe-without-changing-settings',layout_sha256=hashlib.sha256(LAYOUT).hexdigest())
    def config(self,doc=None):return parse_configuration(json.dumps(doc or self.document()).encode())
    def test_canonical_layout_and_independent_small_graph_oracles(self):
        doc=self.document();doc['scale'].update(nodes=100,edges=1000)
        config=self.config(doc);plan=prepare_plan(config,LAYOUT)
        layout=json.loads(LAYOUT)
        for i,role in enumerate(('object_current','edge_current')):
            original=layout['carriers'][role]['sql']
            self.assertEqual(plan['setup'][i+1],original.replace('CREATE TABLE `'+role+'`',
                'CREATE TABLE `client_dev`.`fresh_layout_probe`.`'+role+'`',1))
        # Independently enumerate only this tiny unit fixture, not scale payloads.
        edges=[(i,(i//10)%99,((i//10)%99+(i%10)//2)%99)for i in range(1000)]
        first=[(e,t)for e,s,t in edges if s==0]
        self.assertEqual(list(outgoing(config,0)),first)
        second=[dict(edge1=str(e),edge2=str(e2),target=str(t2))
                for e,t in first for e2,s2,t2 in edges if s2==t]
        actual=next(q['expected']for q in plan['queries']if q['name']=='connected-two-hop')
        self.assertEqual(actual,second)
        self.assertEqual(outgoing(config,99),())
        self.assertTrue(any(e==t==0 for e,s,t in edges))
        self.assertGreater(len(first),len({t for _,t in first}))
        self.assertTrue(all('range('in sql for sql in plan['ingest']))
        self.assertEqual(plan['state'],'prepared-not-executed')
    def test_config_and_layout_refusals_before_effects(self):
        self.assertEqual(self.config().nodes,100_000)
        for mutate in [lambda d:d['compute'].update(id=SHARED_WAREHOUSE),
                       lambda d:d['compute'].update(dedicated=False),
                       lambda d:d['scale'].update(nodes=1_000_000,edges=10_000_000),
                       lambda d:d['limits'].update(maximum_cost_usd='0'),
                       lambda d:d['limits'].update(maximum_rows=True),
                       lambda d:d.update(extra='unknown')]:
            d=self.document();mutate(d)
            with self.subTest(document=d),self.assertRaises(ValueError):self.config(d)
        d=self.document();d['scale'].update(stage='expanded-explicit',nodes=1_000_000,edges=10_000_000)
        self.assertEqual(self.config(d).nodes,1_000_000)
        with self.assertRaises(ValueError):prepare_plan(self.config(),LAYOUT+b' ')
        d=self.document();d['limits']['maximum_rows']=1
        with self.assertRaises(ValueError):prepare_plan(self.config(d),LAYOUT)
    def test_complete_measurement_oracle_and_no_invented_timing_or_cost(self):
        q=dict(sql='SELECT fixed',expected=[{'x':'a'},{'x':'a'}])
        executor=SimpleNamespace(query=lambda *args:SimpleNamespace(rows=[{'x':'a'},{'x':'a'}]))
        sample=measure_query(executor,q,maximum_rows=2,maximum_bytes=100)
        self.assertTrue(sample['passed']);self.assertIsNone(sample['execution_ns'])
        self.assertIsNone(sample['dbu']);self.assertEqual(sample['rows'],2)
        executor.query=lambda *args:SimpleNamespace(rows=[{'x':'a'}])
        with self.assertRaises(ValueError):measure_query(executor,q,maximum_rows=2,maximum_bytes=100)
        closed=[]
        class Iterator:
            def __iter__(self):return iter([['a'],['a'],['a']])
            def close(self):closed.append(True)
        frame=SimpleNamespace(columns=['x'],limit=lambda n:SimpleNamespace(toLocalIterator=lambda **kw:Iterator()))
        spark=SimpleNamespace(sql=lambda sql:frame)
        with self.assertRaises(ValueError):SparkSQLExecutor(spark,maximum_rows=2,maximum_bytes=100).query('SELECT fixed',{})
        self.assertEqual(closed,[True])


if __name__=='__main__':unittest.main()
