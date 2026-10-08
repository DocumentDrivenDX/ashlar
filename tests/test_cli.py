import base64
import json
from pathlib import Path
import subprocess
import sys
import unittest
from ashlar.source import jsonl_batches

ROOT = Path(__file__).resolve().parents[1]

class CLITests(unittest.TestCase):
    def invoke(self, raw):
        return subprocess.run([sys.executable, '-m', 'ashlar', 'inspect-source',
                               '--feed', 'fixture', '--epoch', 'epoch',
                               '--cursor-before', '18446744073709551616'],
                              input=raw, capture_output=True, cwd=ROOT)

    def test_original_batch_bytes_and_large_cursor_from_module_command(self):
        raw = (ROOT / 'examples/end-to-end/local-string-source.jsonl').read_bytes()
        result = self.invoke(raw)
        self.assertEqual(result.returncode, 0, result.stderr)
        values = [json.loads(line) for line in result.stdout.splitlines()]
        originals = tuple(jsonl_batches(raw.splitlines(keepends=True), feed='fixture', epoch='epoch',
                                       cursor_before='18446744073709551616'))
        self.assertEqual(len(values), len(originals))
        for value, original in zip(values, originals):
            self.assertEqual(value['cursor_after'], original.cursor_after)
            self.assertEqual(base64.b64decode(value['begin_base64']), original.begin)
            self.assertEqual(base64.b64decode(value['commit_base64']), original.commit)
            self.assertEqual([base64.b64decode(row['raw_base64']) for row in value['records']],
                             [row.raw for row in original.records])

    def test_incomplete_first_transaction_emits_no_batch(self):
        result = self.invoke(b'{"kind":"begin","batch_id":"incomplete"}\n')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b'')

class CSVCLITests(unittest.TestCase):
    def invoke(self,raw,mapping='{"label":"23","caption":"24"}'):
        return subprocess.run([sys.executable,'-m','ashlar','inspect-csv','--feed','csv-example','--epoch','immutable-example-1','--source-system','local-example','--schema-revision','3','--type-id','17','--properties-json',mapping],input=raw,capture_output=True,cwd=ROOT)
    def test_user_input_custody_is_directly_recoverable_with_outer_checkpoint(self):
        from ashlar.csv_source import csv_batches
        from ashlar.staging import batch_from_row
        from ashlar.source_checkpoint import csv_checkpoint
        raw=(ROOT/'examples/end-to-end/string-source.csv').read_bytes()
        result=self.invoke(raw);self.assertEqual(result.returncode,0,result.stderr)
        values=[json.loads(line) for line in result.stdout.splitlines()]
        originals=tuple(csv_batches(raw.splitlines(keepends=True),feed='csv-example',epoch='immutable-example-1',source_system='local-example',schema_revision='3',type_id='17',properties={'label':'23','caption':'24'}))
        self.assertEqual(len(values),4)
        for value,batch in zip(values,originals):
            self.assertEqual(value['format'],'ashlar-csv-source-custody/0.1')
            self.assertEqual(batch_from_row(value['batch_row']),batch)
            self.assertEqual(value['source_checkpoint_json'],csv_checkpoint(batch))
    def test_invalid_or_duplicate_mapping_cannot_emit_custody(self):
        raw=(ROOT/'examples/end-to-end/string-source.csv').read_bytes()
        for mapping in ['{"label":23}','{"label":"23","label":"24"}','[]']:
            result=self.invoke(raw,mapping);self.assertNotEqual(result.returncode,0)
            self.assertEqual(result.stdout,b'')
