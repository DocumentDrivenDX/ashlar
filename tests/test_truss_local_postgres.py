import hashlib
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ashlar_host.truss_local_postgres import inspect_external_postgres_18_6_pg_ctl


class ExternalPostgresToolTests(unittest.TestCase):
    def test_external_pg_ctl_is_hash_and_version_bound_without_starting_server(self):
        with tempfile.TemporaryDirectory() as directory:
            executable = Path(directory) / 'pg_ctl'
            executable.write_bytes(b'packet-pinned external pg_ctl')
            executable.chmod(0o700)
            executable = executable.resolve()
            digest = hashlib.sha256(executable.read_bytes()).hexdigest()
            process = SimpleNamespace(returncode=0,
                                      stdout='pg_ctl (PostgreSQL) 18.6 (Homebrew)\n')
            with patch('ashlar_host.truss_local_postgres.subprocess.run',
                       return_value=process) as run:
                receipt = inspect_external_postgres_18_6_pg_ctl(str(executable), digest)
            run.assert_called_once_with([str(executable), '--version'],
                stdin=unittest.mock.ANY, capture_output=True, text=True,
                timeout=5, check=False)
            self.assertEqual(receipt['sha256'], digest)
            self.assertEqual(receipt['version'], 'pg_ctl (PostgreSQL) 18.6 (Homebrew)')
            self.assertFalse(receipt['server_started'])

    def test_external_pg_ctl_rejects_wrong_hash_and_wrong_major_minor(self):
        with tempfile.TemporaryDirectory() as directory:
            executable = Path(directory) / 'pg_ctl'
            executable.write_bytes(b'pg_ctl')
            executable.chmod(0o700)
            executable = executable.resolve()
            with self.assertRaisesRegex(ValueError, 'digest differs'):
                inspect_external_postgres_18_6_pg_ctl(str(executable), '0' * 64)
            digest = hashlib.sha256(executable.read_bytes()).hexdigest()
            with patch('ashlar_host.truss_local_postgres.subprocess.run',
                       return_value=SimpleNamespace(returncode=0,
                           stdout='pg_ctl (PostgreSQL) 17.6 (Homebrew)\n')):
                with self.assertRaisesRegex(ValueError, 'PostgreSQL 18.6'):
                    inspect_external_postgres_18_6_pg_ctl(str(executable), digest)
