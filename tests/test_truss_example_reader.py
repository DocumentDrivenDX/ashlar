# SPDX-License-Identifier: MIT OR Apache-2.0
import hashlib,json,sys,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.truss_example_reader import run_configure_truss_example_reader_command

class Tests(unittest.TestCase):
    def fixture(self,path):
        root=path/'example';root.mkdir();(root/'data').mkdir();publication=root/'publication';publication.mkdir()
        page=publication/'source.json';page.write_text('original-page')
        jars=[]
        for name in ('delta.jar','storage.jar'):
            p=path/name;p.write_text('selected');jars.append(str(p))
        query={'connection_uri':'original-runtime','publication_directory':str(publication),'jars':jars,'feed':'original-feed','source_page_path':str(page),'source_page_sha256':hashlib.sha256(page.read_bytes()).hexdigest(),'request_digest':'a'*64}
        (root/'query-configuration.json').write_text(json.dumps(query))
        (root/'setup.json').write_text(json.dumps({'data_directory':str(root/'data')}))
        (root/'accepted.json').write_text(json.dumps({'data_directory':str(root/'data'),'roles':{'consumer':'actual-ordinary'},'report_original_base64':'x'*350000}))
        connection=path/'live.json';connection.write_text(json.dumps({'profile':'ashlar-local-truss-connection/0.1','data_directory':str(root/'data'),'connection_uri':'actual-live-admin'}))
        return root,connection,query
    def test_original_selection_and_large_accepted_report_private_output(self):
        with tempfile.TemporaryDirectory()as directory:
            path=Path(directory).resolve();root,connection,query=self.fixture(path);output=path/'reader.json'
            fake=SimpleNamespace(conninfo=SimpleNamespace(conninfo_to_dict=lambda value:{'user':'admin','host':'actual'},make_conninfo=lambda **kwargs:'fresh-'+kwargs['user']))
            with patch.dict(sys.modules,{'psycopg':fake}):result=run_configure_truss_example_reader_command(root,connection,output)
            selected=json.loads(output.read_text());self.assertEqual(selected,{**query,'connection_uri':'fresh-actual-ordinary'})
            self.assertEqual(output.stat().st_mode&0o777,0o600);self.assertEqual(result['output'],str(output))
            self.assertNotIn('fresh-actual-ordinary',json.dumps(result))
            query['destination_id']='original-delta-installation'
            (root/'query-configuration.json').write_text(json.dumps(query))
            receipt_output=path/'receipt-reader.json'
            with patch.dict(sys.modules,{'psycopg':fake}),patch('ashlar_host.truss_feed_sessions.require_truss_receipt_sdk'):
                run_configure_truss_example_reader_command(root,connection,receipt_output)
            self.assertEqual(json.loads(receipt_output.read_text()),{**query,'connection_uri':'fresh-actual-ordinary'})
    def test_foreign_data_and_existing_output_refuse(self):
        with tempfile.TemporaryDirectory()as directory:
            path=Path(directory).resolve();root,connection,query=self.fixture(path);output=path/'reader.json'
            original=connection.read_text();value=json.loads(original);value['data_directory']=str(path/'foreign');connection.write_text(json.dumps(value))
            with self.assertRaises(ValueError):run_configure_truss_example_reader_command(root,connection,output)
            self.assertFalse(output.exists());connection.write_text(original);output.write_text('preserve')
            with self.assertRaises(ValueError):run_configure_truss_example_reader_command(root,connection,output)
            self.assertEqual(output.read_text(),'preserve')
            query['destination_id']=None;(root/'query-configuration.json').write_text(json.dumps(query))
            invalid=path/'invalid-reader.json'
            with self.assertRaises(ValueError):run_configure_truss_example_reader_command(root,connection,invalid)
            self.assertFalse(invalid.exists())
    def test_explicit_intent_selection_preserves_shape_and_refuses_foreign_or_changed_intent(self):
        with tempfile.TemporaryDirectory()as directory:
            path=Path(directory).resolve();root,connection,query=self.fixture(path)
            intent_path=root/'publication/publication-intent-2.json'
            intent={k:query[k]for k in ('feed','source_page_path','source_page_sha256','request_digest')};intent['publication_id']='actual-selected'
            intent_path.write_text(json.dumps(intent))
            selected={k:query[k]for k in ('connection_uri','publication_directory','jars')};selected['publication_intent']={'path':str(intent_path),'sha256':hashlib.sha256(intent_path.read_bytes()).hexdigest()}
            reader=root/'reader-configuration.json';reader.write_text(json.dumps(selected));output=path/'typed-reader.json'
            fake=SimpleNamespace(conninfo=SimpleNamespace(conninfo_to_dict=lambda value:{'user':'admin'},make_conninfo=lambda **kwargs:'fresh-'+kwargs['user']))
            with patch.dict(sys.modules,{'psycopg':fake}):run_configure_truss_example_reader_command(root,connection,output,reader_configuration=reader)
            self.assertEqual(json.loads(output.read_text()),{**selected,'connection_uri':'fresh-actual-ordinary'})
            foreign={**selected,'publication_directory':str(path)};reader.write_text(json.dumps(foreign))
            with self.assertRaisesRegex(ValueError,'publication directory'):run_configure_truss_example_reader_command(root,connection,path/'foreign-reader',reader_configuration=reader)
            reader.write_text(json.dumps(selected));intent_path.write_text(json.dumps({**intent,'publication_id':'changed'}))
            with self.assertRaisesRegex(ValueError,'intent digest'):run_configure_truss_example_reader_command(root,connection,path/'changed-reader',reader_configuration=reader)
            self.assertFalse((path/'changed-reader').exists())
            intent_path.write_text(json.dumps(intent));renew=__import__('ashlar_host.evolution_cli',fromlist=['InvocationFile']).InvocationFile.renew
            def changed_before_write(owner,maximum):
                if owner.path==reader:intent_path.write_text(json.dumps({**intent,'publication_id':'changed-after-parse'}))
                return renew(owner,maximum)
            with patch.dict(sys.modules,{'psycopg':fake}),patch('ashlar_host.truss_example_reader.InvocationFile.renew',new=changed_before_write):
                with self.assertRaises(ValueError):run_configure_truss_example_reader_command(root,connection,path/'raced-reader',reader_configuration=reader)
            self.assertFalse((path/'raced-reader').exists())
