"""Focused public edge update/delete ports; no native source qualification."""
import json,tempfile,unittest,sys
from pathlib import Path
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.truss_typed_edge_update import run_update_truss_edge_command
from ashlar_host.truss_typed_edge_delete import run_delete_truss_edge_command

class Tests(unittest.TestCase):
 def inputs(self,root,operation):
  (root/'bun').write_text('selected');(root/'model').write_text('{"id":"doc","umf":"0.7.0"}');(root/'origin').write_text('{ "operator": "selected" }')
  config={'connection_uri':'private','bun':str(root/'bun'),'document_path':str(root/'model'),'document_revision':'r1','document_id':'doc','module_id':'m','relationship_id':'related','origin_path':str(root/'origin'),'request_key':'original','edge_id':7,'expected_version':1}
  if operation=='update':config.update(set={'number':{'kind':'integer','text':'9007199254740993'}},unset=['remove'],endpoint_change={'state':'replace','source':{'document_id':'doc','module_id':'m','record_id':'Typed','entity_id':1},'target':{'document_id':'doc','module_id':'m','record_id':'Typed','entity_id':3}},order_key_change={'state':'null'})
  path=root/'configuration';path.write_text(json.dumps(config));return path,config
 def test_original_declared_versions_preserve_update_delete_inputs(self):
  from ashlar_host.truss_typed_edge_update import preflight as update_preflight
  from ashlar_host.truss_typed_edge_delete import preflight as delete_preflight
  for operation,preflight in (('update',update_preflight),('delete',delete_preflight)):
   for version,document_id,accepted in (('0.7.0','doc',True),('0.8.0','doc',True),('0.9.0','doc',False),('0.8.0','other',False)):
    with self.subTest(operation=operation,version=version,document_id=document_id),tempfile.TemporaryDirectory()as d:
     root=Path(d).resolve();path,_=self.inputs(root,operation)
     original=(' { "id": "'+document_id+'", "umf": "'+version+'", "x-original": {"opaque": true} }\n').encode()
     (root/'model').write_bytes(original)
     with patch.dict(sys.modules,{'psycopg':None,'truss':None}):
      if accepted:
       result=preflight(path,root/'result');self.assertEqual(result[3],original)
      else:
       with self.assertRaisesRegex(ValueError,'Original declared UMF'):preflight(path,root/'result')
     self.assertFalse((root/'result').exists());self.assertEqual((root/'model').read_bytes(),original)
 def exercise(self,operation,*,noop=False,stale=False):
  @dataclass
  class Identity:id:str;definition_pin:str='actual-pin'
  @dataclass
  class Text:kind:str;text:str
  @dataclass
  class Edge:identity:Identity;version:int;source:Identity;target:Identity;order_key:object;retained:tuple
  @dataclass
  class Found:edge:Edge
  @dataclass
  class Absent:pass
  @dataclass
  class Object:document_id:str;module_id:str;record_id:str;id:int;identity:Identity
  @dataclass
  class ObjectFound:object:Object
  @dataclass
  class Changed:identity:Identity;version:str='2';operation:str='update_edge';events:tuple=('original-event',)
  @dataclass
  class Unchanged:identity:Identity;version:str='1';operation:str='update_edge';events:tuple=()
  @dataclass
  class DeletedIdentity:identity:Identity;deletion_version:str='2'
  @dataclass
  class Deleted:identity:Identity;deleted:tuple;deletion_version:str='2';operation:str='delete_edge';events:tuple=('original-event',)
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
  identity=Identity('7');before=Edge(identity,1,Identity('1'),Identity('2'),'old',(('keep',True),('remove','gone')));after=before if noop else Edge(identity,2,Identity('1'),Identity('3'),None,(('keep',True),('number',Text('integer','9007199254740993'))));result=Deleted(identity,(DeletedIdentity(identity),))if operation=='delete'else Unchanged(identity)if noop else Changed(identity);semantic=Semantic((result,));applied=Success(Response(semantic));replay=Success(Replay(semantic));calls=[];events=[];self_test=self
  class Assembly:
   def get_relationship_reference_in_transaction(self,tx,**kwargs):return Ok(SimpleNamespace(definition_pin='actual-pin'))
   def get_edge_in_transaction(self,tx,**kwargs):return Ok(Absent()if operation=='delete'and calls else Found(after if calls else before))
   def get_object_in_transaction(self,tx,**kwargs):id=kwargs['object_id'];return Ok(ObjectFound(Object('doc','m','Typed',id,Identity(str(id)))))
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
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory).resolve();path,config=self.inputs(root,operation);destination=root/'result'
   if noop:config.update(set={},unset=['absent'],endpoint_change={'state':'unchanged'},order_key_change={'state':'unchanged'})
   if stale:config['expected_version']=2
   path.write_text(json.dumps(config));sdk=SimpleNamespace(Ok=Ok,CatalogDocument=lambda *a:a,CatalogRuntimeConfiguration=lambda:None,create_reference_assembly=lambda *a:Ok(Assembly()),adopt_psycopg_host=lambda c:Host(),EdgeFound=Found,EdgeNotFound=Absent,ObjectFound=ObjectFound);contracts=SimpleNamespace(TextValue=Text,BooleanValue=lambda **k:k,NullValue=lambda:None,MapValue=lambda **k:k,MapEntry=lambda **k:k);groups=SimpleNamespace(UpdateEdge=lambda **k:k,EndpointChange=lambda **k:k,NullOrderKey=lambda:{'state':'null'},TextOrderKey=lambda **k:k,Delete=lambda **k:k,StoredObjectReference=lambda **k:k,AuthoredValue=lambda **k:k,Changed=Changed,Unchanged=Unchanged,Deleted=Deleted,GroupSuccess=Success,CommittedReceiptReplay=Replay);modules={'psycopg':SimpleNamespace(connect=lambda *a,**k:Connection()),'truss':sdk,'truss.contracts':contracts,'truss.groups':groups};command=run_update_truss_edge_command if operation=='update'else run_delete_truss_edge_command
   with patch.dict(sys.modules,modules):
    if stale:
     with self.assertRaisesRegex(RuntimeError,'identity.*version'):command(path,destination)
     self.assertEqual(calls,[]);self.assertFalse(destination.exists())
    else:
     report=command(path,destination);self.assertEqual(calls[0],calls[1]);self.assertEqual(calls[0][2]['documents'][0][2],(root/'model').read_bytes());self.assertEqual(destination.stat().st_mode&0o777,0o600);selection=calls[0][0][1][0];self.assertEqual(selection['target']['identity'],identity);self.assertEqual(selection['expected_version'],'1')
     if operation=='update':
      self.assertEqual(report['edge']['version'],1 if noop else 2);self.assertEqual(dict(report['edge']['retained'])['keep'],True)
      if noop:self.assertNotIn('order_key_change',selection);self.assertNotIn('endpoint_change',selection)
      else:self.assertEqual(selection['endpoint_change']['target']['identity'],Identity('3'));self.assertEqual(selection['order_key_change'],{'state':'null'})
     else:self.assertEqual(selection['operation'],'delete_edge');self.assertEqual(report['before']['identity']['id'],'7')
   self.assertEqual(events[-1],'closed')
 def test_update_exact_retained_endpoints_order_and_absent_unset_noop(self):
  self.exercise('update');self.exercise('update',noop=True);self.exercise('update',stale=True)
 def test_delete_exact_identity_absence_receipt_and_stale_version(self):
  self.exercise('delete');self.exercise('delete',stale=True)
 def test_bad_closed_changes_refuse_before_source_connection(self):
  for operation in ('update','delete'):
   with tempfile.TemporaryDirectory()as directory:
    root=Path(directory).resolve();path,config=self.inputs(root,operation);config['expected_version']=True;path.write_text(json.dumps(config))
    with patch.dict(sys.modules,{'psycopg':SimpleNamespace(connect=lambda *a,**k:self.fail('No connection permitted'))}),self.assertRaises(ValueError):(run_update_truss_edge_command if operation=='update'else run_delete_truss_edge_command)(path,root/'result')
