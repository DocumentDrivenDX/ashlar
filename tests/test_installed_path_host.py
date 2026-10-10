"""Installed path adapters preserve the reviewed public guard behavior.

These tests use retained synthetic ports; they establish no native qualification.
"""
import ast
from pathlib import Path
import json
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


class InstalledPathHostTests(unittest.TestCase):
    def test_capture_migration_preserves_unchanged_definitions(self):
        before = ast.parse((ROOT / 'tools/weft_path_capture.py').read_text())
        after = ast.parse((ROOT / 'src/ashlar_host/path_capture.py').read_text())
        # Capture retains its original surface plus the reviewed cleanup fix.
        for tree in (before, after):
            tree.body = [node for node in tree.body if not (
                isinstance(node, ast.FunctionDef) and node.name == '_capture_string_frame')]
        self.assertEqual(ast.dump(before, include_attributes=False),
                         ast.dump(after, include_attributes=False))

    def test_original_profile_remains_the_default_admission(self):
        from ashlar_host import path_admission
        from ashlar_host.path_admission import (PathAdmissionConfig,
            PathSchemaValidation, BACKEND)
        with patch.dict(sys.modules, {'weft_path_plan': path_admission}):
            from test_weft_path_plan import SCHEMAS
        config = PathAdmissionConfig(16777216,
            PathSchemaValidation(SCHEMAS, lambda *args: None))
        self.assertEqual(config.profile, 'paths')
        self.assertEqual(dict(BACKEND), {
            'backendId': 'ashlar.databricks.paths',
            'backendVersion': '0.4.0-paths-candidate',
            'interfaceVersion': 'weft-backend/0.3.0',
            'targetProfile': 'spark4-delta4-paths-candidate'})
        # Actual legacy artifact, guard and cleanup behavior is exercised below.

    def test_installed_public_guards_and_cleanup(self):
        script = '''
import json, sys, unittest
from ashlar_host import path_admission, path_capture, path_execution
assert not any(name.startswith(('pyspark', 'psycopg', 'delta', 'tools')) for name in sys.modules)
sys.modules['weft_path_plan'] = path_admission
sys.modules['weft_path_capture'] = path_capture
sys.modules['run_commerce_path_weft'] = path_execution
suite = unittest.TestSuite()
for name in ('test_weft_path_plan', 'test_weft_path_capture', 'test_run_commerce_path_weft', 'test_weft_path_interval_cleanup', 'test_weft_path_predicate_admission'):
    suite.addTests(unittest.defaultTestLoader.loadTestsFromName(name))
from contextlib import contextmanager
import test_run_commerce_path_weft as fixtures
class CleanupIdentityTests(unittest.TestCase):
    def test_readonly_interval_marker_preserves_primary(self):
        class Primary(KeyboardInterrupt):
            def __setattr__(self, name, value):
                if name == 'interval_cleanup_failed': raise AttributeError('marker refused')
                super().__setattr__(name, value)
        helper = fixtures.PathExecutionTests(); case = helper.fixture()
        opened, config, oracle = helper.setup(case, [])
        primary = Primary('synthetic original cancellation')
        @contextmanager
        def interval(context):
            opened.provider.active = True
            try: yield
            finally:
                opened.provider.active = False
                raise OSError('synthetic interval cleanup')
        def sql(*args, **kwargs): raise primary
        opened.provider.interval = interval
        opened.provider.driver.transport.spark.sql = sql
        with self.assertRaises(KeyboardInterrupt) as caught:
            helper.execute(case, opened, config, oracle)
        self.assertIs(caught.exception, primary)
        self.assertFalse(opened.provider.active)

    def test_iterator_cleanup_cancellation_preserves_primary(self):
        primary = KeyboardInterrupt('synthetic capture cancellation')
        class Iterator:
            def __iter__(self): return self
            def __next__(self): raise primary
            def close(self): raise KeyboardInterrupt('synthetic cleanup cancellation')
        frame = fixtures.frame(['value'], [])
        frame.toLocalIterator = lambda **kwargs: Iterator()
        with self.assertRaises(KeyboardInterrupt) as caught:
            path_capture.capture_string_frame(frame, config=path_capture.PathCaptureConfig(2, 128, 256))
        self.assertIs(caught.exception, primary)

    def test_cleanup_only_cancellation_propagates(self):
        cleanup = KeyboardInterrupt('synthetic cleanup cancellation')
        class Iterator:
            def __iter__(self): return self
            def __next__(self): raise StopIteration
            def close(self): raise cleanup
        frame = fixtures.frame(['value'], [])
        frame.toLocalIterator = lambda **kwargs: Iterator()
        with self.assertRaises(KeyboardInterrupt) as caught:
            path_capture.capture_string_frame(frame, config=path_capture.PathCaptureConfig(2, 128, 256))
        self.assertIs(caught.exception, cleanup)

    def test_readonly_capture_marker_preserves_primary(self):
        class Primary(path_capture.PathCaptureError):
            def __setattr__(self, name, value):
                if name == 'cleanup_failed' and hasattr(self, name): raise AttributeError('marker refused')
                super().__setattr__(name, value)
        primary = Primary('synthetic capture refusal')
        class Iterator:
            def __iter__(self): return self
            def __next__(self): raise primary
            def close(self): raise OSError('synthetic cleanup refusal')
        frame = fixtures.frame(['value'], [])
        frame.toLocalIterator = lambda **kwargs: Iterator()
        with self.assertRaises(path_capture.PathCaptureError) as caught:
            path_capture.capture_string_frame(frame, config=path_capture.PathCaptureConfig(2, 128, 256))
        self.assertIs(caught.exception, primary)
    def test_cleanup_accessor_preserves_primary(self):
        primary = KeyboardInterrupt('synthetic capture cancellation')
        class Iterator:
            def __iter__(self): return self
            def __next__(self): raise primary
            @property
            def close(self): raise SystemExit('synthetic cleanup accessor cancellation')
        frame = fixtures.frame(['value'], [])
        frame.toLocalIterator = lambda **kwargs: Iterator()
        with self.assertRaises(KeyboardInterrupt) as caught:
            path_capture.capture_string_frame(frame, config=path_capture.PathCaptureConfig(2, 128, 256))
        self.assertIs(caught.exception, primary)

suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(CleanupIdentityTests))
result = unittest.TextTestRunner().run(suite)
print(json.dumps({'tests': result.testsRun, 'passed': result.wasSuccessful()}))
sys.exit(0 if result.wasSuccessful() else 1)
'''
        environment = dict(os.environ)
        environment['PYTHONPATH'] = os.pathsep.join(str(ROOT / path) for path in ('src', 'tests', 'tools'))
        environment['PYTHONDONTWRITEBYTECODE'] = '1'
        outcome = subprocess.run([sys.executable, '-B', '-c', script], cwd=ROOT,
                                 env=environment, capture_output=True, timeout=30)
        self.assertEqual(outcome.returncode, 0, outcome.stderr.decode('utf8', errors='replace'))
        summary = json.loads(outcome.stdout)
        self.assertTrue(summary['passed'])
        self.assertGreaterEqual(summary['tests'], 40)


if __name__ == '__main__':
    unittest.main()
