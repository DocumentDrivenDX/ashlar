"""Focused public edge mutation ports, without native source execution."""
import json,tempfile,unittest,sys
from pathlib import Path
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.truss_typed_edge_command import run_create_truss_edge_command

class Tests(unittest.TestCase):
 def inputs(self,root):
  (root/'bun').write_text('selected');(root/'model').write_text('{"id":"doc","umf":"0.7.0"}');(root/'origin').write_text('{"operator":"selected"}')
  endpoint=lambda id:{'document_id':'doc','module_id':'m','record_id':'Typed','entity_id':id}
  config={'connection_uri':'private','bun':str(root/'bun'),'document_path':str(root/'model'),'document_revision':'r1','document_id':'doc','module_id':'m','relationship_id':'related','origin_path':str(root/'origin'),'request_key':'edge-key','values':{'number':{'kind':'integer','text':'9007199254740993'}},'source':endpoint(1),'target':endpoint(2),'order_key':{'state':'text','text':'雪'}}
  path=root/'configuration';path.write_text(json.dumps(config));return path,config
 def test_unknown_order_or_endpoint_refuses_before_connection(self):
  for mutation in ('order','endpoint'):
   with tempfile.TemporaryDirectory()as d:
    root=Path(d).resolve();path,config=self.inputs(root)
    if mutation=='order':config['order_key']={'state':'unknown','text':'preserved'}
    else:config['source']['entity_id']=True
    path.write_text(json.dumps(config))
    with patch.dict(sys.modules,{'psycopg':SimpleNamespace(connect=lambda *a,**kw:self.fail('No native effects permitted'))}),self.assertRaises(ValueError):run_create_truss_edge_command(path,root/'result')
 def test_actual_references_exact_retained_readback_replay_and_refusal(self):
  @dataclass
  class Text:kind:str;text:str
  @dataclass
  class Identity:id:str
  @dataclass
  class Created:identity:Identity;version:str='1'
  @dataclass
  class Semantic:results:tuple
  @dataclass
  class Response:semantic:Semantic
  @dataclass
  class Replay(Response):pass
  @dataclass
  class Success:response:Response
  @dataclass
  class Object:id:int;identity:Identity;document_id:str;module_id:str;record_id:str
  @dataclass
  class ObjectFound:object:Object
  @dataclass
  class Edge:
   identity:Identity;version:int;source:Identity;target:Identity;order_key:str;retained:tuple
  @dataclass
  class EdgeFound:edge:Edge
  class Ok:
   def __init__(self,value):self.value=value
  semantic=Semantic((Created(Identity('7')),));applied=Success(Response(semantic));replay=Success(Replay(semantic))
  for missing in (False,True):
   with self.subTest(missing=missing),tempfile.TemporaryDirectory()as d:
    root=Path(d).resolve();path,config=self.inputs(root);destination=root/'result';events=[];calls=[];self_test=self
    class Assembly:
     def get_relationship_reference_in_transaction(self,tx,**kwargs):return Ok(None if missing else SimpleNamespace(definition_pin='actual-relationship-pin'))
     def get_object_in_transaction(self,tx,**kwargs):id=kwargs['object_id'];return Ok(ObjectFound(Object(id,Identity(str(id)),'doc','m','Typed')))
     def group_context_in_transaction(self,tx):return Ok(SimpleNamespace(input=lambda ops,origin:('original',ops,origin),request=lambda key:key))
     def apply_in_transaction(self,tx,original,request,**kwargs):calls.append((original,request,kwargs));return Ok(applied if len(calls)==1 else replay)
     def get_edge_in_transaction(self,tx,**kwargs):return Ok(EdgeFound(Edge(Identity('7'),1,Identity('1'),Identity('2'),'雪',(('number',Text('integer','9007199254740993')),))))
     def dispose(self):events.append('dispose')
    class Host:
     def adopt_transaction(self):return Ok('tx')
     def detach(self):events.append('detach');return True
    class Connection:
     def __enter__(self):return self
     def __exit__(self,*args):events.append('closed');self_test.assertFalse(destination.exists())
     def execute(self,*args):pass
     def commit(self):events.append('commit')
    sdk=SimpleNamespace(Ok=Ok,CatalogDocument=lambda *args:args,CatalogRuntimeConfiguration=lambda:None,create_reference_assembly=lambda *args:Ok(Assembly()),adopt_psycopg_host=lambda c:Host(),ObjectFound=ObjectFound,EdgeFound=EdgeFound)
    contracts=SimpleNamespace(TextValue=Text,BooleanValue=lambda **kw:kw,NullValue=lambda:None,MapValue=lambda **kw:kw,MapEntry=lambda **kw:kw)
    groups=SimpleNamespace(CreateEdge=lambda **kw:kw,StoredObjectReference=lambda **kw:kw,NullOrderKey=lambda:None,TextOrderKey=lambda **kw:kw,AuthoredValue=lambda **kw:kw,GroupSuccess=Success,CommittedReceiptReplay=Replay)
    with patch.dict(sys.modules,{'psycopg':SimpleNamespace(connect=lambda *a,**kw:Connection()),'truss':sdk,'truss.contracts':contracts,'truss.groups':groups}):
     if missing:
      with self.assertRaisesRegex(RuntimeError,'relationship reference'):run_create_truss_edge_command(path,destination)
      self.assertFalse(destination.exists());self.assertEqual(calls,[])
     else:
      result=run_create_truss_edge_command(path,destination);self.assertEqual(result['edge']['identity']['id'],'7');self.assertEqual(calls[0],calls[1]);operation=calls[0][0][1][0];self.assertEqual(operation['relationship_definition_pin'],'actual-relationship-pin');self.assertEqual(operation['source']['identity'],Identity('1'));self.assertEqual(operation['order_key'],{'text':'雪'});self.assertEqual(calls[0][2]['documents'][0][2],(root/'model').read_bytes());self.assertEqual(destination.stat().st_mode&0o777,0o600)
    self.assertEqual(events[-1],'closed')
 def test_installed_command_dispatch_selects_public_edge_wrapper(self):
  import io
  from contextlib import redirect_stdout
  from ashlar.cli import main
  with patch('sys.argv',['ashlar','create-truss-edge','--configuration','/configuration','--output','/output']),patch('ashlar_host.truss_typed_edge_command.run_create_truss_edge_command')as command,redirect_stdout(io.StringIO()):main()
  command.assert_called_once_with('/configuration','/output')
