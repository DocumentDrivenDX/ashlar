from __future__ import annotations

import ast
import os
import subprocess
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import truss
from truss import CatalogViewUnavailable

import read_catalog_view_reason_r2 as helper


PACKET = Path(__file__).resolve().parent
WORKER = PACKET / 'read_catalog_view_reason_r2.py'
INTERPRETER = Path(sys.prefix) / 'bin/python'


class ReadCatalogViewReasonR2Tests(unittest.TestCase):
    def test_exact_isolated_runtime_bootstrap_reaches_mock_connection_without_network(self):
        code = f'''import importlib.util
p={str(WORKER)!r}
s=importlib.util.spec_from_file_location('r2_worker', p)
m=importlib.util.module_from_spec(s)
s.loader.exec_module(m)
calls=[]
def boundary(**kwargs):
    calls.append(kwargs)
    raise RuntimeError("MOCK_CONNECTION_BOUNDARY")
try:
    m.capture(connection_factory=boundary)
except RuntimeError as error:
    assert str(error) == "MOCK_CONNECTION_BOUNDARY"
    assert len(calls) == 1
    assert calls[0]["user"] == "ashlar048_accept_consumer_r1"
    assert calls[0]["connect_timeout"] == 5
    assert calls[0]["options"] == "-c statement_timeout=2000"
    print("BOOTSTRAP_AND_VERSION_OK; MOCK_BOUNDARY_REACHED; NO_NETWORK")
else:
    raise AssertionError("mock connection boundary not reached")
'''
        env = {
            'PATH': '/usr/bin:/bin',
            'HOME': '/Users/erik',
            'PYTHONNOUSERSITE': '1',
            'PYTHONDONTWRITEBYTECODE': '1',
            'ASHLAR_LOCAL_PG_CONNINFO': 'host=/private/tmp/nonexistent-ashlar-socket dbname=discard user=discard',
        }
        result = subprocess.run(
            [str(INTERPRETER), '-I', '-B', '-c', code],
            cwd=Path('/private/tmp'), env=env, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr.decode(errors='replace'))
        self.assertEqual(result.stdout.decode().strip(), 'BOOTSTRAP_AND_VERSION_OK; MOCK_BOUNDARY_REACHED; NO_NETWORK')

    def test_available_result_contains_only_length_and_digest(self):
        class Available:
            def __init__(self, view):
                self.view = view

        with patch.object(truss, 'CatalogViewAvailable', Available):
            result = helper.safe_result(Available(SimpleNamespace(original_bytes=b'{"private":"value"}')))
        self.assertEqual(result['outcome'], 'available')
        self.assertEqual(result['bytes'], len(b'{"private":"value"}'))
        self.assertNotIn('private', json_repr(result))
        self.assertNotIn('original_bytes', result)

    def test_unavailable_result_preserves_only_known_reason(self):
        result = helper.safe_result(CatalogViewUnavailable(reason='resource'))
        self.assertEqual(result, {'outcome': 'unavailable', 'reason': 'resource'})

    def test_error_output_redacts_message_and_connection_values(self):
        result = helper.safe_error(RuntimeError('password=secret host=/private/tmp/private-socket'))
        self.assertEqual(result, {'outcome': 'error', 'error_type': 'RuntimeError'})
        self.assertNotIn('secret', json_repr(result))
        self.assertNotIn('private-socket', json_repr(result))

    def test_helper_retains_read_only_and_no_stage_access_guards(self):
        tree = ast.parse(WORKER.read_text())
        calls = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertIn('catalog_view_in_transaction', calls)
        self.assertIn('rollback', calls)
        self.assertIn('close', calls)
        forbidden = {'run_migration', 'publish', 'ack', 'write_feed', 'submit_seed', 'continue_feed'}
        self.assertTrue(forbidden.isdisjoint(calls))
        strings = [node.value.upper() for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str)]
        self.assertTrue(any('READ ONLY' in value for value in strings))
        self.assertFalse(any(any(token in value for token in (' INSERT ', ' UPDATE ', ' DELETE ', ' CREATE ', ' ALTER ', ' DROP ')) for value in strings))


def json_repr(value):
    import json
    return json.dumps(value, sort_keys=True)


if __name__ == '__main__':
    unittest.main()
