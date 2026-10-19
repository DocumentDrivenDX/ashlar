# SPDX-License-Identifier: MIT OR Apache-2.0
"""Original source/retained clock correspondence; inert local records, no native calls."""
import json,unittest
from pathlib import Path
from ashlar.source_config import load_jsonl_configuration
from ashlar.source import jsonl_batches
from ashlar_host.managed_carriers import load_managed_carriers
from ashlar_host.managed_source_oracle import configured_prefix_carrier_rows,retained_materialization_clock,CLOCK_OPERATION,CLOCK_SQL
ROOT=Path(__file__).resolve().parents[1]

class Tests(unittest.TestCase):
    def test_generic_prefix_matches_original_independent_carriers(self):
        from fixture_oracle import fixture_inventory
        c=load_jsonl_configuration(ROOT/'examples/end-to-end/configured-source.json');batches=tuple(jsonl_batches(c.source.read_bytes().splitlines(keepends=True),feed=c.feed,epoch=c.epoch));carriers=load_managed_carriers();columns={r:carriers.ordered_columns(r)for r in ('object_current','edge_current','tombstone','whole_source_history')}
        expected,_=fixture_inventory(batches,columns,materialized_at='2023-11-14T22:13:20.123456+00:00')
        result=configured_prefix_carrier_rows(c,batches,materialization_clock_us='1700000000123456',carriers=carriers)
        self.assertEqual(result.rows(),expected);self.assertEqual(len(result.rows()['whole_source_history']),4)
        self.assertEqual(result.rows()['object_current'][0]['logical_key_json'],'[]')
        detached=result.rows();detached['object_current'].clear();self.assertTrue(result.rows()['object_current'])
        with self.assertRaises(ValueError):configured_prefix_carrier_rows(c,batches[::-1],materialization_clock_us='1700000000123456',carriers=carriers)
        with self.assertRaises(ValueError):configured_prefix_carrier_rows(c,batches,materialization_clock_us='1700000000123456',carriers=carriers,maximum_rows=1)
    def test_clock_requires_exact_workload_statement_and_complete_native_shape(self):
        import tempfile
        from test_managed_publication_selection import Tests as Fixtures
        with tempfile.TemporaryDirectory()as temp:
            spec,db=Fixtures().fixture(Path(temp))
            try:
                rows=db.execute('SELECT operation,request,digest,handle,response FROM submission').fetchall();workload=db.execute('SELECT workload FROM stream_scope').fetchone()[0]
                self.assertEqual(retained_materialization_clock(rows,workload).microseconds,'1700000000123456')
                for kind in ('missing','wrong-workload','truncated','fraction','handle'):
                    changed=list(rows)
                    if kind=='missing':changed=[]
                    elif kind=='wrong-workload':
                        with self.assertRaises(ValueError):retained_materialization_clock(changed,'0'*64)
                        continue
                    else:
                        values=list(changed[0]);response=json.loads(values[4])
                        if kind=='truncated':response['manifest']['truncated']=True
                        elif kind=='handle':response['statement_id']='foreign'
                        else:response['result']['data_array'][0][0]='1.5'
                        values[4]=json.dumps(response);changed=[tuple(values)]
                    with self.subTest(kind=kind),self.assertRaises(ValueError):retained_materialization_clock(changed,workload)
            finally:db.close()

if __name__=='__main__':unittest.main()
