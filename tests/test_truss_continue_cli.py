# SPDX-License-Identifier: MIT OR Apache-2.0
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.truss_continue_cli import run_continue_truss_graph_command


class Tests(unittest.TestCase):
    def test_invalid_limits_refuse_before_opening_original_custody(self):
        with patch('ashlar_host.truss_continue_cli.run_selected_truss_command')as opening:
            with self.assertRaises(ValueError):
                run_continue_truss_graph_command('/config','/output',maximum_pages=0)
            opening.assert_not_called()
            with self.assertRaises(ValueError):
                run_continue_truss_graph_command('/config','/output',maximum_page_bytes=2097153)
            opening.assert_not_called()

    def test_exact_selector_restored_before_continuation(self):
        events=[]
        @contextmanager
        def writer(*args):
            events.append('open');yield;events.append('close')
        driver=SimpleNamespace(writer=writer,context=object(),source_sessions=SimpleNamespace(batch=lambda:SimpleNamespace(feed='feed')))
        def owner(config,output,operation):
            return operation(driver,{'request_digest':'a'*64,'connection_uri':'postgresql://ordinary'})
        def restore(driver,digest):events.append(digest)
        def advance(driver,connection,**options):
            self.assertEqual(events,['open','a'*64,'close'])
            self.assertEqual(options,{'maximum_pages':1,'wait_seconds':0,'maximum_page_bytes':2097152,'archives':()})
            return {'publications':[]}
        with patch('ashlar_host.truss_continue_cli.run_selected_truss_command',side_effect=owner),patch('ashlar_host.truss_continue_cli.restore_applied_truss_publication',side_effect=restore),patch('ashlar_host.truss_continue.continue_truss_graph',side_effect=advance),patch.dict('sys.modules',{'psycopg':SimpleNamespace()}):
            self.assertEqual(run_continue_truss_graph_command('/config','/output',maximum_page_bytes=2097152),{'publications':[]})

    def test_private_interruption_report_preserves_failure_and_refuses_overwrite(self):
        import json
        from pathlib import Path
        from tempfile import TemporaryDirectory
        with TemporaryDirectory()as directory:
            output=Path(directory)/'failure.json'
            failure=OSError('private URI must not be reported')
            failure.cleanup_failed=True
            failure.ashlar_truss_continuation_interruption={'format':'ashlar-truss-continuation-interruption/0.1',
                'outcome':'unconfirmed','publication_intent':{'path':'/original/intent.json','sha256':'a'*64}}
            with patch('ashlar_host.truss_continue_cli.run_selected_truss_command',side_effect=failure):
                with self.assertRaises(OSError)as raised:run_continue_truss_graph_command('/configuration',output)
                self.assertIs(raised.exception,failure)
                report=json.loads(output.read_bytes())
                self.assertEqual(report['publication_intent']['path'],'/original/intent.json')
                self.assertTrue(report['owners_cleanup_failed'])
                self.assertNotIn(str(failure),output.read_text());self.assertEqual(output.stat().st_mode&0o777,0o600)
                saved=output.read_bytes()
                with self.assertRaises(OSError)as raised:run_continue_truss_graph_command('/configuration',output)
                self.assertIs(raised.exception,failure);self.assertEqual(output.read_bytes(),saved)
