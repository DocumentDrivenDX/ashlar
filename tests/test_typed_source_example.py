import json,shutil,tempfile,unittest
from pathlib import Path
from run_typed_source_example import run

class TypedSourceExampleTests(unittest.TestCase):
    def test_public_umf_and_actual_duckdb_small_lifecycle(self):
        source=Path('/private/tmp/ashlar-umf-records-c45c72a2')
        if not source.exists() or not shutil.which('duckdb'):self.skipTest('Actual public UMF source and DuckDB required')
        with tempfile.TemporaryDirectory() as temp:
            out=Path(temp)/'run';summary=run(source,out)
            self.assertEqual((summary['transactions'],summary['events'],summary['history_events'],summary['tombstones']),(3,4,4,1))
            self.assertEqual(summary['native_rows'][0]['quantity'],'9007199254740994')
            self.assertEqual(summary['native_rows'][0]['amount'],'40.00')
            self.assertTrue(summary['replay_unchanged']);self.assertEqual(summary['replay_native_writes'],0)
            self.assertEqual(len(json.loads((out/'history-readback.json').read_text())),4)
            with self.assertRaises(ValueError):run(source,out)
