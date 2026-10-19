# SPDX-License-Identifier: MIT OR Apache-2.0
import json,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
from unittest.mock import patch
import ashlar_host.truss_query_batch as command

class Tests(unittest.TestCase):
    def query(self,name):return {'name':name,'sql':'SELECT original','revision':'1','document_id':'cohort','maximum_rows':2}
    def test_invalid_closed_batch_before_owner_effects(self):
        for queries in ([],[self.query('same'),self.query('same')],[dict(self.query('a'),maximum_rows=True)],[dict(self.query('a'),unknown='no')]):
            with self.assertRaises(ValueError):command.parse_truss_query_batch(json.dumps({'profile':'ashlar-truss-query-batch/0.1','queries':queries}).encode())
        with self.assertRaises(ValueError):command.parse_truss_query_batch(b'{"profile":"ashlar-truss-query-batch/0.1","profile":"duplicate","queries":[]}')
        with tempfile.TemporaryDirectory()as directory:
            path=Path(directory).resolve()/'batch.json';path.write_bytes(b'{}')
            with patch.object(command,'run_selected_truss_command')as open_owner:
                with self.assertRaises(ValueError):command.run_query_truss_graph_batch_command('/original',str(path.parent/'result.json'),index='/index',installation='/compiler',batch=path)
                open_owner.assert_not_called()
    def test_shared_owner_restore_once_buffers_until_close_and_refuses_drift(self):
        for outcome in ('success','mixed','closing'):
            with self.subTest(outcome=outcome),tempfile.TemporaryDirectory()as directory:
                root=Path(directory).resolve();batch=root/'batch.json';batch.write_text(json.dumps({'profile':'ashlar-truss-query-batch/0.1','queries':[self.query('one'),self.query('two')]}))
                index=root/'index.json';index.write_text('selected');installation=root/'compiler';installation.mkdir();output=root/'result.json';events=[]
                manifest={'publication_id':'original'}
                driver=SimpleNamespace(context=object(),source_sessions=SimpleNamespace(batch=lambda:SimpleNamespace(feed='original-feed')),policy=SimpleNamespace(active={'manifest':manifest}))
                @contextmanager
                def writer(*args):events.append('writer-open');yield;events.append('writer-close')
                driver.writer=writer
                def owner(configuration,destination,operation,*,report_writer):
                    events.append('owner-open');result=operation(driver,{'request_digest':'original-digest'})
                    self.assertFalse(output.exists());events.append('owner-close')
                    if outcome=='closing':raise RuntimeError('owned close failed')
                    return report_writer(result,output)
                calls=[]
                def execute(*args,**kwargs):
                    calls.append(kwargs);self.assertEqual(events[-1],'writer-close')
                    return {'publication_id':'foreign'if outcome=='mixed'else'original','rows':[{'text':'original'}],'request_bytes':b'{"request":"original"}\n','response_bytes':b'{"response":"original"}\n'}
                with patch.object(command,'run_selected_truss_command',side_effect=owner),patch.object(command,'restore_applied_truss_publication')as restore,patch.object(command,'execute_truss_string_query',side_effect=execute):
                    if outcome=='success':
                        result=command.run_query_truss_graph_batch_command('/original',output,index=index,installation=installation,batch=batch)
                        self.assertEqual([q['name']for q in result['queries']],['one','two']);self.assertEqual(result['publication_id'],'original');self.assertEqual(output.stat().st_mode&0o777,0o600)
                        self.assertEqual(result['queries'][0]['request_original_json'],'{"request":"original"}\n');self.assertEqual(len(calls),2)
                    else:
                        with self.assertRaises((ValueError,RuntimeError)):command.run_query_truss_graph_batch_command('/original',output,index=index,installation=installation,batch=batch)
                        self.assertFalse(output.exists())
                    restore.assert_called_once_with(driver,'original-digest')
