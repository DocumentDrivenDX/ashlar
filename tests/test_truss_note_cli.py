# SPDX-License-Identifier: MIT OR Apache-2.0
import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar_host.truss_note_cli import run_publish_truss_note_command

class Tests(unittest.TestCase):
    def test_invalid_page_limit_refuses_before_native_sdk_or_source_effects(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);selected=root/'configuration.json'
            selected.write_text(json.dumps({'connection_uri':'private consumer','administrator_uri':'private administrator','reader_role':'reader','consumer_id':'consumer','feed':'feed','jars':['unselected-a','unselected-b'],'maximum_pages':True}))
            original_import=__import__
            def guarded(name,*args,**kwargs):
                if name in ('psycopg','pyspark'):raise AssertionError('Invalid configuration must not load native SDK')
                return original_import(name,*args,**kwargs)
            with patch('builtins.__import__',side_effect=guarded):
                with self.assertRaises(ValueError):run_publish_truss_note_command(selected,root/'output')
            self.assertFalse((root/'output').exists())

    def test_installed_dispatch_prints_only_location_and_count(self):
        import io
        from contextlib import redirect_stdout
        from ashlar.cli import main
        output=io.StringIO()
        with patch('sys.argv',['ashlar','publish-truss-note','--configuration','/private/config.json','--output','/private/destination']),patch('ashlar_host.truss_note_cli.run_publish_truss_note_command',return_value={'publications':[{'manifest':'private business data'}]})as run,redirect_stdout(output):
            main()
        run.assert_called_once_with('/private/config.json','/private/destination')
        self.assertEqual(json.loads(output.getvalue()),{'output':'/private/destination/result.json','publications':1})
        self.assertNotIn('private business data',output.getvalue())

    def test_graph_command_dispatches_concrete_graph_publisher(self):
        import io
        from contextlib import redirect_stdout
        from ashlar.cli import main
        with patch('sys.argv',['ashlar','publish-truss-graph','--configuration','/private/config.json','--output','/private/destination']),patch('ashlar_host.truss_note_cli.run_publish_truss_graph_command',return_value={'publications':[]})as run,redirect_stdout(io.StringIO())as output:
            main()
        run.assert_called_once_with('/private/config.json','/private/destination')
        self.assertEqual(json.loads(output.getvalue())['publications'],0)

    def test_wait_budget_refuses_before_source_effects(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);selected=root/'configuration.json'
            selected.write_text(json.dumps({'connection_uri':'private consumer','administrator_uri':'private administrator','reader_role':'reader','consumer_id':'consumer','feed':'feed','jars':[],'maximum_pages':1,'wait_seconds':61}))
            with self.assertRaisesRegex(ValueError,'Wait budget'):
                run_publish_truss_note_command(selected,root/'output')
            self.assertFalse((root/'output').exists())

    def test_graph_export_refuses_note_mode_before_native_effects(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);selected=root/'configuration.json'
            selected.write_text(json.dumps({'connection_uri':'private','administrator_uri':'private','reader_role':'reader','consumer_id':'consumer','feed':'feed','jars':[],'maximum_pages':1,'export_graph':True}))
            with self.assertRaisesRegex(ValueError,'Graph export requires'):
                run_publish_truss_note_command(selected,root/'output')
            self.assertFalse((root/'output').exists())

    def test_graph_export_is_forwarded_without_archives(self):
        import sys,types
        from contextlib import nullcontext
        from ashlar_host.truss_note_cli import run_publish_truss_graph_command
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();selected=root/'configuration.json'
            jars=[root/'spark.jar',root/'storage.jar']
            for path in jars:path.write_bytes(b'installed')
            selected.write_text(json.dumps({'connection_uri':'private','administrator_uri':'private','reader_role':'reader','consumer_id':'consumer','feed':'feed','jars':list(map(str,jars)),'maximum_pages':1,'export_graph':True}))
            sdk=types.ModuleType('psycopg');sdk.connect=lambda *args,**kwargs:None
            with patch.dict(sys.modules,{'psycopg':sdk}),patch('ashlar_host.truss_runtime.require_truss_psycopg_runtime'),patch('ashlar_host.commerce_export_cli.open_commerce_export_spark',return_value=nullcontext('spark')),patch('ashlar_host.truss_note_publication.register_note_consumer'),patch('ashlar_host.truss_note_publication.publish_graph_pages',return_value={})as publish:
                run_publish_truss_graph_command(selected,root/'output')
            self.assertIs(publish.call_args.kwargs['export_graph'],True)
            self.assertEqual(publish.call_args.kwargs['archived_acceptance_files'],())
