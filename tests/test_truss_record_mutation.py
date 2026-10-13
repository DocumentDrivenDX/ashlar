import json,tempfile,unittest,sys
from pathlib import Path
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.truss_record_mutation import scalar_tag,run_create_truss_record_command

class Tests(unittest.TestCase):
    def inputs(self,root):
        (root/'bun').write_text('selected');(root/'model').write_text('{"id":"doc","umf":"0.7.0"}');(root/'origin').write_text('{ "operator": "selected" }')
        config={'connection_uri':'private','bun':str(root/'bun'),'document_path':str(root/'model'),'document_revision':'r1','document_id':'doc','module_id':'m','record_id':'Record','origin_path':str(root/'origin'),'request_key':'original-key','values':{'name':{'kind':'string','text':'雪'},'number':{'kind':'integer','text':'9007199254740993'}}}
        path=root/'configuration';path.write_text(json.dumps(config));return path,config
    def test_tags_and_invalid_configuration_refuse_before_connection(self):
        for tag,expected in (({'kind':'decimal','text':'1.2300'},('decimal','1.2300')),({'kind':'boolean','value':True},('boolean',True)),({'kind':'null'},('null',None))):self.assertEqual(scalar_tag(tag),expected)
        for tag in ({'kind':'integer','text':'01'},{'kind':'decimal','text':'NaN'},{'kind':'boolean','value':1},{'kind':'null','extra':1}):
            with self.assertRaises(ValueError):scalar_tag(tag)
        with tempfile.TemporaryDirectory()as d:
            root=Path(d).resolve();path,config=self.inputs(root);config['values']['number']={'kind':'integer','text':'1.0'};path.write_text(json.dumps(config))
            with patch.dict(sys.modules,{'psycopg':SimpleNamespace(connect=lambda *args,**kwargs:self.fail('No connection permitted'))}):
                with self.assertRaises(ValueError):run_create_truss_record_command(path,root/'result')
            self.assertFalse((root/'result').exists())
    def test_original_group_readback_replay_and_closure_before_private_report(self):
        @dataclass
        class Text:
            kind:str;text:str
        @dataclass
        class Carrier:
            value:object=None
        @dataclass
        class Semantic:
            results:tuple
        @dataclass
        class Identity:
            id:str='7'
        @dataclass
        class Created:
            identity:Identity;version:str='1'
        @dataclass
        class Response:
            semantic:Semantic
        @dataclass
        class Replay(Response):pass
        @dataclass
        class Success:
            response:Response
        @dataclass
        class Object:
            id:int;version:int;values:tuple
            @property
            def identity(self):return Identity(str(self.id))
        @dataclass
        class Found:
            object:Object
        class Ok:
            def __init__(self,value):self.value=value
        events=[];semantic=Semantic((Created(Identity()),));applied=Success(Response(semantic));replay=Success(Replay(semantic));calls=[]
        class Assembly:
            def get_type_reference_in_transaction(self,tx,**kwargs):return Ok(SimpleNamespace(definition_pin='actual-pin'))
            def group_context_in_transaction(self,tx):return Ok(SimpleNamespace(input=lambda ops,origin:('original',ops,origin),request=lambda key:key))
            def apply_in_transaction(self,tx,original,request,**kwargs):calls.append((original,request,kwargs));return Ok(applied if len(calls)==1 else replay)
            def get_object_in_transaction(self,tx,**kwargs):return Ok(Found(Object(7,1,(('name','雪'),('number',Text('integer','9007199254740993'))))))
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
            sdk=SimpleNamespace(Ok=Ok,CatalogDocument=lambda *args:args,CatalogRuntimeConfiguration=lambda:None,create_reference_assembly=lambda *args:Ok(Assembly()),adopt_psycopg_host=lambda connection:Host(),ObjectFound=Found)
            contracts=SimpleNamespace(TextValue=Text,BooleanValue=Carrier,NullValue=Carrier,MapValue=lambda **kwargs:kwargs,MapEntry=lambda **kwargs:kwargs)
            groups=SimpleNamespace(CreateObject=lambda **kwargs:kwargs,RootlessOwnership=lambda:None,AuthoredValue=lambda **kwargs:kwargs,GroupSuccess=Success,CommittedReceiptReplay=Replay)
            with patch.dict(sys.modules,{'psycopg':SimpleNamespace(connect=lambda *args,**kwargs:Connection()),'truss':sdk,'truss.contracts':contracts,'truss.groups':groups}):result=run_create_truss_record_command(path,destination)
            self.assertEqual(events[-1],'closed');self.assertEqual(calls[0],calls[1]);self.assertEqual(calls[0][1],'original-key');self.assertEqual(calls[0][2]['documents'][0][2],(root/'model').read_bytes());self.assertEqual(result['object']['id'],7);self.assertEqual(destination.stat().st_mode&0o777,0o600)
