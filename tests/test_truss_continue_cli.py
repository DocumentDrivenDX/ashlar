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
            self.assertEqual(options,{'maximum_pages':1,'wait_seconds':0,'archives':()})
            return {'publications':[]}
        with patch('ashlar_host.truss_continue_cli.run_selected_truss_command',side_effect=owner),patch('ashlar_host.truss_continue_cli.restore_applied_truss_publication',side_effect=restore),patch('ashlar_host.truss_continue.continue_truss_graph',side_effect=advance),patch.dict('sys.modules',{'psycopg':SimpleNamespace()}):
            self.assertEqual(run_continue_truss_graph_command('/config','/output'),{'publications':[]})
