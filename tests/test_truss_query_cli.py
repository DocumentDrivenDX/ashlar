# SPDX-License-Identifier: MIT OR Apache-2.0
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.truss_query_cli import run_query_truss_graph_command


class Tests(unittest.TestCase):
    def test_restore_then_query_preserves_original_protocol(self):
        events=[]
        @contextmanager
        def writer(stream,context):
            events.append('open');yield;events.append('close')
        driver=SimpleNamespace(writer=writer,context=object(),source_sessions=SimpleNamespace(batch=lambda:SimpleNamespace(feed='feed')))
        def owned(config,output,operation):return operation(driver,{'request_digest':'a'*64})
        def restore(driver,digest):events.append('restore')
        def execute(*args,**kwargs):
            self.assertEqual(events,['open','restore','close'])
            return {'rows':[{'id':'original'}],'request_bytes':b'{"request":1}\n','response_bytes':b'{"response":1}\n','publication_id':'original'}
        with patch('ashlar_host.truss_query_cli.run_selected_truss_command',side_effect=owned),patch('ashlar_host.truss_query_cli.restore_applied_truss_publication',side_effect=restore),patch('ashlar_host.truss_query_cli.execute_truss_string_query',side_effect=execute):
            answer=run_query_truss_graph_command('/config','/output',index='/index',installation='/compiler',sql='SELECT n.id FROM Note n',revision='1',document_id='original')
        self.assertEqual(answer['request_original_json'],'{"request":1}\n')
        self.assertEqual(answer['rows'],[{'id':'original'}])

    def test_invalid_capacity_refuses_before_owner(self):
        with patch('ashlar_host.truss_query_cli.run_selected_truss_command')as opening:
            with self.assertRaises(ValueError):
                run_query_truss_graph_command('/config','/output',index='/index',installation='/compiler',sql='SELECT x FROM X',revision='1',document_id='original',maximum_rows=0)
            opening.assert_not_called()
