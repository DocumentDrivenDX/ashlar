"""Check the user-facing command and shared fixture loader remain executable."""
import json
from pathlib import Path
import subprocess
import sys
import unittest

class LocalExampleTests(unittest.TestCase):
    def test_cli_composes_schema_source_apply_and_retained_replay(self):
        root = Path(__file__).resolve().parents[1]
        result = subprocess.run([sys.executable, str(root / 'tools/run_local_example.py')],
                                cwd=root, capture_output=True, text=True, check=True)
        value = json.loads(result.stdout)
        self.assertEqual((value['batches'], value['events'], value['objects'], value['tombstones']), (3, 4, 1, 1))
        self.assertEqual(value['rows'], [{'id': '1', 'version': '2',
                          'props_json': '{"23":"updated","24":"雪"}',
                          'retained_json': '{"future":18446744073709551615}'}])
        self.assertTrue(value['replay_unchanged'])
        self.assertFalse(value['complete_interpretation'])
        self.assertFalse(value['published'])
        self.assertFalse(value['acknowledged'])
