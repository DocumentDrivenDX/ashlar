import json
from pathlib import Path
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from whole_graph_sql import graph_sql_plan
from ashlar.apply import empty_state
from ashlar.source import jsonl_batches
ROOT=Path(__file__).resolve().parents[1]
TABLES={k:'c.s.'+k for k in ['object_current','edge_current','tombstone','whole_source_history']}
class WholeGraphSQLTests(unittest.TestCase):
    def batches(self):return list(jsonl_batches((ROOT/'examples/end-to-end/graph-source.jsonl').read_bytes().splitlines(keepends=True),feed='whole-entity-fixture',epoch='epoch-1'))
    def plan(self,prior,batch):return graph_sql_plan(prior,batch,TABLES,materialized_at='2026-10-08T17:00:00+00:00',schema_policy=lambda x:None)
    def test_plan_preserves_original_carriers_and_replay_is_empty(self):
        first,second=self.batches();state,steps=self.plan(empty_state(),first)
        self.assertEqual(len(steps),5)
        self.assertEqual(self.plan(state,first)[1],[])
        final,steps=self.plan(state,second)
        self.assertEqual(len(final.current),2);self.assertEqual(len(final.tombstones),3)
        rows=json.loads(next(s['parameters']['rows'] for s in steps if 'INSERT INTO `c`.`s`.`object_current`' in s['statement']))
        self.assertEqual(rows[0]['retained_json'],'{"future":18446744073709551615}')
        self.assertEqual(rows[0]['props_json'],'{"23":"updated","24":null}')
        history=json.loads(next(s['parameters']['rows'] for s in steps if 'whole_source_history' in s['statement']))
        self.assertEqual(len(history),4)
    def test_unsafe_target_or_clock_refuses(self):
        batch=self.batches()[0]
        for tables,clock in [(dict(TABLES,object_current='bad;drop'),'2026-10-08T17:00:00+00:00'),(TABLES,'2026-10-08T17:00:00')]:
            with self.assertRaises(ValueError):graph_sql_plan(empty_state(),batch,tables,materialized_at=clock,schema_policy=lambda x:None)
if __name__=='__main__':unittest.main()
