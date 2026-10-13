import hashlib
import contextlib
import io
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from ashlar_host.config import HostError
from ashlar_host.implicit_arithmetic_command import run_implicit_arithmetic_command
from ashlar_host.ordinary_query_connection import OrdinaryQueryConnectionFactory
import test_implicit_arithmetic_configuration as configuration_fixture


class Tests(unittest.TestCase):
    def setUp(self):
        fixture = configuration_fixture.Tests()
        fixture.setUp()
        self.config = fixture.config

    def test_invalid_pin_refuses_before_file_read(self):
        with patch('ashlar_host.commerce_query_command.InvocationFile.read') as read:
            with self.assertRaises(HostError):
                run_implicit_arithmetic_command(self.config, provider_path='/missing',
                    provider_sha256='wrong', pack_path='/missing')
            read.assert_not_called()

    def test_captured_provider_supplies_exact_port_and_original_pack(self):
        original = (Path(__file__).resolve().parents[1]/
                    'examples/domain-packs/commerce/upstream/pack.json').read_bytes()
        code = b'from test_commerce_query_sources import factory\ndef commerce_query_source_factory(config):\n return factory()\nfrom ashlar_host.ordinary_query_connection import OrdinaryQueryConnectionFactory\ndef ordinary_query_connection_factory(config):\n return OrdinaryQueryConnectionFactory(lambda role: None)\n'
        with tempfile.TemporaryDirectory() as name:
            root = Path(name).resolve()
            provider, pack = root/'provider.py', root/'pack.json'
            provider.write_bytes(code); pack.write_bytes(original)
            with patch('ashlar_host.implicit_arithmetic_query.query_commerce_implicit_arithmetic', return_value={'complete': True}) as query:
                answer = run_implicit_arithmetic_command(self.config,
                    provider_path=provider, provider_sha256=hashlib.sha256(code).hexdigest(),
                    pack_path=pack)
                self.assertEqual(answer, {'complete': True})
                self.assertIs(type(query.call_args.kwargs['ordinary_session']), OrdinaryQueryConnectionFactory)
                self.assertEqual(query.call_args.kwargs['pack_bytes'], original)
                custody = query.call_args.kwargs['invocation_custody']
                custody.renew()
                pack.write_bytes(original+b' ')
                with self.assertRaises(HostError):
                    custody.renew()
                pack.write_bytes(original)
            with patch('ashlar_host.implicit_arithmetic_query.query_commerce_implicit_arithmetic') as query:
                with self.assertRaises(HostError):
                    run_implicit_arithmetic_command(self.config, provider_path=provider,
                        provider_sha256='0'*64, pack_path=pack)
                query.assert_not_called()

    def test_help_does_not_import_native_or_host(self):
        code = "import sys; from ashlar.cli import main\ntry: main()\nexcept SystemExit as e: assert e.code == 0\nassert not any(n.split('.')[0] in {'ashlar_host','pyspark','psycopg'} for n in sys.modules)"
        result = subprocess.run([sys.executable, '-B', '-S', '-c', code,
            'query-commerce-implicit-arithmetic', '--help'], capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(b'--session-provider-sha256', result.stdout)

    def test_cli_dispatch_constructs_typed_settings_and_forwards_explicit_provider(self):
        from ashlar.cli import main
        values = {name: '/explicit/'+name for name in (
            'index', 'installation', 'publication', 'output', 'jars', 'model',
            'graph', 'umf-source', 'bun', 'git', 'session-provider', 'pack')}
        values.update({'postgres-container':'ashlar-e2e-truss-pg17',
            'postgres-host':'127.0.0.1', 'postgres-database':'truss_e2e',
            'postgres-port':'15432', 'producer-timeout-seconds':'30',
            'producer-maximum-output-bytes':'1024', 'producer-maximum-receipt-bytes':'4096',
            'maximum-artifact-bytes':'4096', 'maximum-rows':'100',
            'maximum-cell-bytes':'1024', 'maximum-total-cell-bytes':'4096',
            'session-provider-sha256':'a'*64})
        argv = ['ashlar', 'query-commerce-implicit-arithmetic']
        for key, value in values.items():
            argv.extend(['--'+key, value])
        output = io.StringIO()
        with patch.object(sys, 'argv', argv), contextlib.redirect_stdout(output), \
             patch('ashlar_host.implicit_arithmetic_command.run_implicit_arithmetic_command') as command:
            main()
        command.assert_called_once()
        config = command.call_args.args[0]
        self.assertIs(type(config), type(self.config))
        self.assertEqual(config.profile, 'implicit-arithmetic')
        self.assertEqual(command.call_args.kwargs['provider_path'], Path('/explicit/session-provider'))
        self.assertEqual(command.call_args.kwargs['provider_sha256'], 'a'*64)
        self.assertEqual(command.call_args.kwargs['pack_path'], Path('/explicit/pack'))
        self.assertEqual(output.getvalue(), 'ashlar-host: report /explicit/output/report.json\n')
        from ashlar_host.implicit_arithmetic_configuration import ExplicitCommercePostgresEndpoint
        explicit=argv+['--postgres-profile','explicit-loopback']
        explicit[explicit.index('--postgres-container')+1]='chosen-original-fixture'
        explicit[explicit.index('--postgres-port')+1]='25433'
        with patch.object(sys,'argv',explicit),contextlib.redirect_stdout(io.StringIO()),patch('ashlar_host.implicit_arithmetic_command.run_implicit_arithmetic_command') as command:
            main()
        endpoint=command.call_args.args[0].postgres
        self.assertIs(type(endpoint),ExplicitCommercePostgresEndpoint)
        self.assertEqual(endpoint.port,25433)
