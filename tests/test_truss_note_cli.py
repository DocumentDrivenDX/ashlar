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
