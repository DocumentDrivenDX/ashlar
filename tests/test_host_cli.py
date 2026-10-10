"""Installed commands keep help and invalid configuration outside native execution."""
import contextlib
import io
import subprocess
import sys
import unittest
from unittest.mock import patch

from ashlar.cli import main


class HostCliTests(unittest.TestCase):
    def test_help_needs_neither_host_package_nor_native_libraries(self):
        script = '''import sys
from ashlar.cli import main
try:
 main()
except SystemExit as error:
 assert error.code == 0
assert not any(name.split('.')[0] in {'ashlar_host','pyspark','delta','psycopg'} for name in sys.modules)
'''
        for command in ('publish-commerce', 'query-commerce'):
            result = subprocess.run([sys.executable, '-B', '-S', '-c', script,
                                     command, '--help'], capture_output=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(b'--producer-maximum-receipt-bytes', result.stdout)

    def test_invalid_configuration_refuses_before_composition(self):
        args = ['ashlar', 'publish-commerce', '--output', 'relative-output',
                '--jars', '/explicit/jars', '--model', '/explicit/model',
                '--graph', '/explicit/graph', '--umf-source', '/explicit/umf',
                '--bun', '/explicit/bun', '--git', '/explicit/git',
                '--postgres-container', 'ashlar-e2e-truss-pg17',
                '--postgres-host', '127.0.0.1', '--postgres-database', 'truss_e2e',
                '--postgres-port', '15432', '--producer-timeout-seconds', '20',
                '--producer-maximum-output-bytes', '1048576',
                '--producer-maximum-receipt-bytes', '4194304',
                '--source-system', 'private-original-commerce-fixture',
                '--binding-profile', 'ashlar-commerce-development-bindings/0.2']
        out, err = io.StringIO(), io.StringIO()
        with patch.object(sys, 'argv', args), contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            with self.assertRaises(SystemExit) as raised:
                main()
        self.assertEqual(raised.exception.code, 2)
        self.assertEqual(out.getvalue(), '')
        self.assertEqual(err.getvalue(), 'ashlar-host: refused\n')
        self.assertNotIn('ashlar_host.commerce', sys.modules)
        self.assertFalse(any(name.split('.')[0] in {'pyspark','delta','psycopg'} for name in sys.modules))
