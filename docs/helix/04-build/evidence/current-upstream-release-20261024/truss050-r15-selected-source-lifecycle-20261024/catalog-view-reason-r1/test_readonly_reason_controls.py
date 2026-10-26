from __future__ import annotations

import ast
import subprocess
import unittest
from pathlib import Path

import run_catalog_view_reason_r1 as supervisor


PACKET = Path(__file__).resolve().parent
WORKER = PACKET / 'read_catalog_view_reason.py'
RUNNER = PACKET / 'run_catalog_view_reason_r1.py'


class ReadOnlyReasonControls(unittest.TestCase):
    def test_payload_is_one_shot_bounded_and_local(self):
        payload = supervisor.build_approval_payload()
        self.assertEqual(payload['limits'], {
            'attempts': 1,
            'automatic_retries': 0,
            'child_deadline_seconds': 120,
            'combined_retained_and_attempt_bytes': 1073741824,
            'log_bytes': 16777216,
            'parallelism': 'sequential local only',
            'cloud_resources': False,
        })
        self.assertEqual(payload['retained_state']['post_attempt_tree_sha256'], supervisor.DATA_TREE_SHA256)
        self.assertEqual(payload['read_boundary']['transaction'], 'READ COMMITTED READ ONLY')
        self.assertEqual(
            payload['read_boundary']['forbidden_stages'],
            ['migration', 'source mutation', 'feed mutation', 'publication', 'ACK', 'seed submission', 'continuation'],
        )

    def test_worker_has_only_the_catalog_view_read_path(self):
        tree = ast.parse(WORKER.read_text())
        calls = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertIn('catalog_view_in_transaction', calls)
        self.assertIn('catalog_view_request', calls)
        forbidden = {'run_migration', 'publish', 'ack', 'read_feed', 'write_feed', 'submit_seed', 'continue_feed'}
        self.assertTrue(forbidden.isdisjoint(calls))
        self.assertTrue({'open', 'write_bytes_new', 'write_new', 'subprocess'}.isdisjoint(calls))
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split('.')[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split('.')[0])
        self.assertTrue(imported <= {'__future__', 'hashlib', 'importlib', 'json', 'os', 'sys', 'pathlib', 'catalog_view_diagnostic', 'psycopg', 'truss'})
        strings = [node.value.upper() for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str)]
        sql = [value for value in strings if 'TRANSACTION' in value or 'TIMEOUT' in value]
        self.assertTrue(any('READ ONLY' in value for value in sql))
        self.assertFalse(any(token in value for value in sql for token in (' INSERT ', ' UPDATE ', ' DELETE ', ' CREATE ', ' ALTER ', ' DROP ')))

    def test_runner_launches_only_the_fixed_reason_helper(self):
        source = RUNNER.read_text()
        tree = ast.parse(source)
        imports = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name.split('.')[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module.split('.')[0])
        self.assertNotIn('campaign_worker_r15', source)
        self.assertNotIn('run_migration', source)
        self.assertNotIn('spark', imports)
        self.assertNotIn('pyspark', imports)
        self.assertIn("str(WORKER)", source)
        self.assertEqual(supervisor.MAX_CHILD_SECONDS, 120)

    def test_worker_imports_in_the_exact_isolated_launch_shape_without_capture(self):
        code = (
            "import importlib.util; "
            f"p={str(WORKER)!r}; "
            "s=importlib.util.spec_from_file_location('candidate_worker', p); "
            "m=importlib.util.module_from_spec(s); s.loader.exec_module(m); "
            "assert callable(m.load_classifier()); "
            "assert callable(m.capture)"
        )
        result = subprocess.run(
            [str(supervisor.OLD48_VENV / 'bin/python'), '-I', '-B', '-c', code],
            cwd=supervisor.ROOT,
            env=supervisor.clean_environment(),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr.decode(errors='replace'))
        self.assertEqual(result.stdout, b'')

    def test_exact_approval_precedes_marker_and_postgres_start(self):
        source = RUNNER.read_text()
        self.assertLess(source.index('if approval != expected:'), source.index('ATTEMPT.mkdir('))
        self.assertLess(source.index('ATTEMPT.mkdir('), source.index('write_new(STARTED,'))
        self.assertLess(source.index('write_new(STARTED,'), source.index('from truss import LocalPostgres'))
        self.assertFalse(supervisor.ATTEMPT.exists())
        self.assertFalse(supervisor.STARTED.exists())
        supervisor.verify_runtime_auxiliaries()


if __name__ == '__main__':
    unittest.main()
