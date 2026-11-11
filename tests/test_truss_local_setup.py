# SPDX-License-Identifier: MIT OR Apache-2.0
import io,json,tempfile,unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch
from ashlar_host.truss_local_setup import run_setup_truss_local_command

class Tests(unittest.TestCase):
    def test_055_setup_forwards_bounded_install_deadline_only_to_install(self):
        from ashlar_host.truss_local_setup import (
            _adopt_setup_host, _install_catalog_in_transaction,
            _install_statement_timeout_ms)
        from unittest.mock import Mock
        assembly=type('Assembly',(),{'install_in_transaction':lambda self,tx,**kwargs:kwargs})()
        self.assertEqual(_install_statement_timeout_ms('0.0.55'),60000)
        self.assertEqual(_install_catalog_in_transaction(assembly,object(),'0.0.55'),
                         {'operation_timeout_ms':180000})
        self.assertEqual(_install_statement_timeout_ms('0.0.54'),15000)
        self.assertEqual(_install_catalog_in_transaction(assembly,object(),'0.0.54'),{})
        connection=object()
        with patch('truss.adopt_psycopg_host',return_value=Mock())as adopt:
            _adopt_setup_host(connection,install_profile=True)
            adopt.assert_called_once_with(connection,
                statement_timeout_ms=60000,operation_timeout_ms=60000)
            adopt.reset_mock()
            _adopt_setup_host(connection)
            adopt.assert_called_once_with(connection)

    def test_preflight_refuses_invalid_configuration_without_native_sdk(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);selected=root/'configuration.json';selected.write_text('{}')
            original_import=__import__
            def guarded(name,*args,**kwargs):
                if name in ('psycopg','truss'):raise AssertionError('Preflight must precede native SDK loading')
                return original_import(name,*args,**kwargs)
            with patch('builtins.__import__',side_effect=guarded):
                with self.assertRaises(ValueError):run_setup_truss_local_command(selected,root/'report.json')
            self.assertFalse((root/'report.json').exists())
    def test_installed_dispatch_uses_public_host_and_prints_only_summary(self):
        from ashlar.cli import main
        output=io.StringIO()
        with patch('sys.argv',['ashlar','setup-truss-local','--configuration','/private/config.json','--output','/private/setup.json']),patch('ashlar_host.truss_local_setup.run_setup_truss_local_command',return_value={'data_directory':'/private/data','revision':1,'private_native_carrier':'not for stdout'})as run,redirect_stdout(output):
            main()
        run.assert_called_once_with('/private/config.json','/private/setup.json')
        self.assertNotIn('private_native_carrier',output.getvalue())
        self.assertEqual(json.loads(output.getvalue())['output'],'/private/setup.json')

    def test_original_cohort_modules_preflight_preserves_bytes_and_refuses_duplicates(self):
        from ashlar_host.truss_local_setup import selections
        source=Path('tests/fixtures/golden/truss-schema-acceptance-20261012/source.umf.json').read_bytes()
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();bun=root/'bun';bun.write_text('selected installed executable');document=root/'cohort.umf.json';document.write_bytes(source);origin=root/'origin.json';origin.write_text('{}');configuration=root/'configuration.json'
            configuration.write_text(json.dumps({'data_directory':str(root/'fresh-data'),'bun':str(bun),'document_path':str(document),'document_id':'cohort','document_revision':'r1','origin_path':str(origin),'request_key':'cohort-setup'}))
            selected=selections(configuration,root/'report.json')
            self.assertEqual(selected[3],source);self.assertEqual(selected[5],('m',));self.assertEqual(json.loads(selected[3])['umf'],'0.7.0')
            duplicated=json.loads(source);duplicated['modules'].append(duplicated['modules'][0]);document.write_text(json.dumps(duplicated))
            with self.assertRaises(ValueError):selections(configuration,root/'report.json')
            self.assertFalse((root/'fresh-data').exists());self.assertFalse((root/'report.json').exists())
