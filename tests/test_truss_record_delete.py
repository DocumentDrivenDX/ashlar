# SPDX-License-Identifier: MIT OR Apache-2.0
import json,tempfile,unittest,sys
from pathlib import Path
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.truss_record_delete import run_delete_truss_record_command

class Tests(unittest.TestCase):
    def inputs(self,root):
        (root/'bun').write_text('selected');(root/'model').write_text('{"id":"doc","umf":"0.7.0"}');(root/'origin').write_text('{ "operator": "selected" }')
        config={'connection_uri':'private','bun':str(root/'bun'),'document_path':str(root/'model'),'document_revision':'r1','document_id':'doc','module_id':'m','record_id':'Record','origin_path':str(root/'origin'),'request_key':'original-key','object_id':7,'expected_version':1}
        path=root/'configuration';path.write_text(json.dumps(config));return path,config
    def test_malformed_refuses_before_connection(self):
        with tempfile.TemporaryDirectory()as d:
            root=Path(d).resolve();path,config=self.inputs(root)
            for invalid in ({**config,'object_id':True},{**config,'expected_version':0},{**config,'set':{}},{**config,'request_key':''}):
                path.write_text(json.dumps(invalid))
                with patch.dict(sys.modules,{'psycopg':SimpleNamespace(connect=lambda *a,**k:self.fail('No connection permitted'))}):
                    with self.assertRaises(ValueError):run_delete_truss_record_command(path,root/'result')
                self.assertFalse((root/'result').exists())
    def test_original_deletion_absence_receipt_and_stale_version(self):
        @dataclass
        class Identity:id:str='7'
        @dataclass
        class Object:
            id:int=7;version:int=1;values:tuple=(('keep','original'),)
            @property
            def identity(self):return Identity()
        @dataclass
        class Found:object:Object
        @dataclass
        class Absent:pass
        @dataclass
        class DeletedIdentity:identity:Identity;deletion_version:str='2'
        @dataclass
        class Deleted:
            identity:Identity;deleted:tuple;deletion_version:str='2';operation:str='delete_object'
        @dataclass
        class Semantic:results:tuple
        @dataclass
        class Response:semantic:Semantic
        @dataclass
        class Replay(Response):pass
        @dataclass
        class Success:response:Response
        class Ok:
            def __init__(self,value):self.value=value
        calls=[];events=[];semantic=Semantic((Deleted(Identity(),(DeletedIdentity(Identity()),)),))
        applied=Success(Response(semantic));replay=Success(Replay(semantic))
        class Assembly:
            def get_object_in_transaction(self,tx,**kwargs):return Ok(Absent()if calls else Found(Object()))
            def group_context_in_transaction(self,tx):return Ok(SimpleNamespace(input=lambda ops,origin:('original',ops,origin),request=lambda key:key))
            def apply_in_transaction(self,tx,original,request,**kwargs):calls.append((original,request,kwargs));return Ok(applied if len(calls)==1 else replay)
            def dispose(self):events.append('dispose')
        class Host:
            def adopt_transaction(self):return Ok('tx')
            def detach(self):events.append('detach');return True
        class Connection:
            def __enter__(self):return self
            def __exit__(self,*args):events.append('closed');self_test.assertFalse(destination.exists())
            def execute(self,*args):pass
            def commit(self):events.append('commit')
        with tempfile.TemporaryDirectory()as d:
            root=Path(d).resolve();path,config=self.inputs(root);destination=root/'result';self_test=self
            sdk=SimpleNamespace(Ok=Ok,CatalogDocument=lambda *args:args,CatalogRuntimeConfiguration=lambda:None,create_reference_assembly=lambda *args:Ok(Assembly()),adopt_psycopg_host=lambda connection:Host(),ObjectFound=Found,ObjectNotFound=Absent)
            contracts=SimpleNamespace(TextValue=lambda **k:k,MapValue=lambda **k:k,MapEntry=lambda **k:k)
            groups=SimpleNamespace(Delete=lambda **kwargs:kwargs,StoredObjectReference=lambda **kwargs:kwargs,Deleted=Deleted,GroupSuccess=Success,CommittedReceiptReplay=Replay)
            modules={'psycopg':SimpleNamespace(connect=lambda *a,**k:Connection()),'truss':sdk,'truss.contracts':contracts,'truss.groups':groups}
            with patch.dict(sys.modules,modules):result=run_delete_truss_record_command(path,destination)
            operation=calls[0][0][1][0];self.assertEqual(operation['operation'],'delete_object');self.assertEqual(operation['expected_version'],'1');self.assertEqual(operation['target']['identity'],Identity())
            self.assertEqual(calls[0],calls[1]);self.assertEqual(calls[0][2]['documents'][0][2],(root/'model').read_bytes());self.assertEqual(result['before']['values'],(('keep','original'),));self.assertEqual(events[-1],'closed');self.assertEqual(destination.stat().st_mode&0o777,0o600)
            calls.clear();destination=root/'stale';config['expected_version']=2;path.write_text(json.dumps(config))
            with patch.dict(sys.modules,modules):
                with self.assertRaisesRegex(RuntimeError,'caller identity/version'):run_delete_truss_record_command(path,destination)
            self.assertEqual(calls,[]);self.assertFalse(destination.exists())
