"""Finite host limits and SDK-free command admission for the separate profile."""
import contextlib
import dataclasses
import io
from pathlib import Path
import subprocess
import sys
import unittest
from types import ModuleType
from unittest.mock import Mock, patch

from ashlar.cli import main
from ashlar.weft_path_decode import PathDecodeConfig
from ashlar_host.config import (HostError, ProducerConfig, PrivatePostgresConfig,
                               QueryCommerceConfig, QueryCommercePathsConfig)
from ashlar_host.path_capture import PathCaptureConfig


class PathsHostConfigurationTests(unittest.TestCase):
    def config(self, **changes):
        p = Path('/explicit')
        values = dict(index=p/'index', installation=p/'installation',
            publication=p/'publication', output=p/'output', jars=p/'jars',
            model=p/'model', graph=p/'graph',
            producer=ProducerConfig(p/'umf', p/'bun', p/'git', 20, 1048576, 4194304),
            postgres=PrivatePostgresConfig('ashlar-e2e-truss-pg17','127.0.0.1',15432,'truss_e2e'),
            maximum_artifact_bytes=16777216,
            capture=PathCaptureConfig(1000,16777216,67108864),
            decoder=PathDecodeConfig(16777216))
        values.update(changes)
        return QueryCommercePathsConfig(**values)

    def test_separate_immutable_profile_and_finite_limits(self):
        config = self.config()
        self.assertNotIsInstance(config, QueryCommerceConfig)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            config.maximum_artifact_bytes = 1
        for changes in (
                {'maximum_artifact_bytes': True}, {'maximum_artifact_bytes': 16777217},
                {'index':Path('relative')}, {'capture':PathCaptureConfig(1001,1,1)},
                {'capture':PathCaptureConfig(1,1,67108865)},
                {'capture':PathCaptureConfig(1,1,1), 'decoder':PathDecodeConfig(2)},
                {'producer':object()}, {'decoder':object()}):
            with self.subTest(changes=changes), self.assertRaises(HostError):
                self.config(**changes)

    def test_help_without_host_or_sdk_imports(self):
        script = '''import sys
from ashlar.cli import main
try: main()
except SystemExit as e: assert e.code == 0
assert not any(n.split('.')[0] in {'ashlar_host','pyspark','delta','psycopg','jsonschema'} for n in sys.modules)
'''
        result = subprocess.run([sys.executable,'-B','-S','-c',script,
            'query-commerce-paths','--help'], capture_output=True, timeout=5)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn(b'--maximum-total-cell-bytes',result.stdout)

    def test_invalid_bounds_refuse_before_paths_composition(self):
        arguments = ['ashlar','query-commerce-paths']
        for name in ('output','jars','model','graph','umf-source','bun','git',
                     'index','installation','publication'):
            arguments.extend(['--'+name,'/explicit/'+name])
        for name,value in dict(postgres_container='ashlar-e2e-truss-pg17',
            postgres_host='127.0.0.1',postgres_database='truss_e2e',postgres_port=15432,
            producer_timeout_seconds=20,producer_maximum_output_bytes=1048576,
            producer_maximum_receipt_bytes=4194304,maximum_artifact_bytes=16777216,
            maximum_rows=0,maximum_cell_bytes=1024,maximum_total_cell_bytes=4096).items():
            arguments.extend(['--'+name.replace('_','-'),str(value)])
        out,err = io.StringIO(),io.StringIO()
        composition = ModuleType('ashlar_host.paths_query')
        composition.query_commerce_paths = Mock(side_effect=AssertionError('Composition reached'))
        with patch.dict(sys.modules, {'ashlar_host.paths_query': composition}), patch.object(sys,'argv',arguments), contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            with self.assertRaises(SystemExit) as raised: main()
        self.assertEqual(raised.exception.code,2)
        self.assertEqual(out.getvalue(),'')
        self.assertEqual(err.getvalue(),'ashlar-host: refused\n')
        composition.query_commerce_paths.assert_not_called()
