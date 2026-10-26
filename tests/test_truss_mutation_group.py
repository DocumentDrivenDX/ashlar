# SPDX-License-Identifier: MIT OR Apache-2.0
"""Atomic mutation public-port controls; no native owner qualification."""
from dataclasses import make_dataclass
from pathlib import Path
from types import ModuleType,SimpleNamespace
from unittest.mock import patch
import json,tempfile,unittest
from ashlar_host.truss_mutation_group import preflight,run_mutate_truss_record_group_command

class Tests(unittest.TestCase):
 def fixture(self,root,*,replacement=False):
  (root/'bun').write_bytes(b'inert');(root/'model').write_bytes(b' {"id":"doc","umf":"0.8.0","opaque":true}\n');(root/'origin').write_bytes(b'{"source":"original"}')
  operations=[{'operation':'delete_edge','module_id':'m','relationship_id':'link','entity_id':7,'expected_version':1},{'operation':'delete_object','module_id':'m','record_id':'product','entity_id':1,'expected_version':1}]
  if replacement:
   endpoint=lambda n:{'state':'stored','document_id':'doc','module_id':'m','record_id':'product'if n==1 else'supplier','entity_id':n}
   operations=[{'operation':'create_edge','module_id':'m','relationship_id':'link','alias':'replacement','source':endpoint(1),'target':endpoint(3),'values':{},'order_key':{'state':'text','text':'雪'}},operations[0]]
  config={'profile':'ashlar-truss-mutation-group/0.1','connection_uri':'private','bun':str(root/'bun'),'document_path':str(root/'model'),'document_revision':'r1','document_id':'doc','source_version':'0.8.0','origin_path':str(root/'origin'),'request_key':'original','operations':operations}
  (root/'config').write_text(json.dumps(config));return config
 def test_closed_preflight_freezes_original_selections_and_refuses_before_sdk(self):
  with tempfile.TemporaryDirectory()as d:
   root=Path(d).resolve();original=self.fixture(root,replacement=True);config,_,_,model,_,_=preflight(root/'config',root/'result')
   self.assertEqual(model,(root/'model').read_bytes());original['operations'][0]['source']['entity_id']=99;self.assertEqual(config.operations[0].source.entity_id,1)
   original=self.fixture(root,replacement=True)
   for change in ('version','alias','duplicate','deleted-endpoint','inexact','extra'):
    raw=json.loads(json.dumps(original))
    if change=='version':raw['source_version']='0.7.0'
    if change=='alias':raw['operations'][0]['target']={'state':'alias','alias':'invented'}
    if change=='duplicate':raw['operations'].append(raw['operations'][1])
    if change=='deleted-endpoint':raw['operations'].append({'operation':'delete_object','module_id':'m','record_id':'product','entity_id':1,'expected_version':1})
    if change=='inexact':raw['operations'][1]['expected_version']=True
    if change=='extra':raw['trusted']=True
    (root/'config').write_text(json.dumps(raw))
    with self.subTest(change=change),patch.dict('sys.modules',{'truss':None,'psycopg':None}),self.assertRaises(ValueError):preflight(root/'config',root/'result')
    self.assertFalse((root/'result').exists())
 def runtime(self,*,wrong=False,stale=False,primary=None,cleanup=None):
  truss=ModuleType('truss');contracts=ModuleType('truss.contracts');groups=ModuleType('truss.groups');psycopg=ModuleType('psycopg');calls={'apply':0,'commit':0,'close':0}
  def dto(module,name,fields):
   cls=make_dataclass(name,fields,frozen=True);setattr(module,name,cls);return cls
  Text=dto(contracts,'TextValue',['kind','text']);dto(contracts,'BooleanValue',['value']);dto(contracts,'NullValue',[]);dto(contracts,'MapValue',['entries']);dto(contracts,'MapEntry',['key','value'])
  Create=dto(groups,'CreateEdge',['relationship_definition_pin','source','target','values','order_key','alias']);dto(groups,'UpdateObject',['target','set','unset','expected_version']);dto(groups,'UpdateEdge',['target','set','unset','expected_version','order_key_change']);Delete=dto(groups,'Delete',['operation','target','expected_version']);dto(groups,'StoredObjectReference',['identity']);dto(groups,'AuthoredValue',['name','value']);Null=dto(groups,'NullOrderKey',[]);dto(groups,'TextOrderKey',['text'])
  Created=dto(groups,'Created',['operation','identity','version','alias']);dto(groups,'Changed',['operation','identity','version']);Deleted=dto(groups,'Deleted',['operation','identity','deletion_version','deleted']);Success=dto(groups,'GroupSuccess',['response']);Replay=dto(groups,'CommittedReceiptReplay',['semantic']);Ok=dto(truss,'Ok',['value']);FoundObject=dto(truss,'ObjectFound',['object']);AbsentObject=dto(truss,'ObjectNotFound',[]);FoundEdge=dto(truss,'EdgeFound',['edge']);AbsentEdge=dto(truss,'EdgeNotFound',[]);dto(truss,'CatalogDocument',['document_id','document_revision','content']);dto(truss,'CatalogRuntimeConfiguration',[])
  Identity=make_dataclass('Identity',['id','definition_pin']);Object=make_dataclass('Object',['document_id','module_id','record_id','id','identity','version']);Edge=make_dataclass('Edge',['identity','version','source','target','order_key','retained']);DeletedId=make_dataclass('DeletedId',['identity','deletion_version']);Alias=make_dataclass('Alias',['alias','identity']);Semantic=make_dataclass('Semantic',['results','aliases']);Fresh=make_dataclass('Fresh',['semantic'])
  identities={n:Identity(str(n),'product'if n==1 else'supplier')for n in (1,2,3)};objects={n:Object('doc','m','product'if n==1 else'supplier',n,i,1)for n,i in identities.items()};edges={7:Edge(Identity('7','link'),1,identities[1],identities[2],None,())};semantic=None
  class Assembly:
   def dispose(self):pass
   def get_type_reference_in_transaction(self,tx,**kw):return Ok(SimpleNamespace(definition_pin=kw['record_id']))
   def get_relationship_reference_in_transaction(self,tx,**kw):return Ok(SimpleNamespace(definition_pin=kw['relationship_id']))
   def get_object_in_transaction(self,tx,**kw):return Ok(FoundObject(objects[kw['object_id']])if kw['object_id']in objects else AbsentObject())
   def get_edge_in_transaction(self,tx,**kw):
    edge=edges.get(kw['edge_id'])
    if edge is not None and stale:edge=Edge(edge.identity,2,edge.source,edge.target,edge.order_key,edge.retained)
    if edge is not None and wrong and calls['apply']:edge=Edge(edge.identity,edge.version,edge.source,identities[2],edge.order_key,edge.retained)
    return Ok(FoundEdge(edge)if edge is not None else AbsentEdge())
   def group_context_in_transaction(self,tx):return Ok(SimpleNamespace(input=lambda ops,origin:ops,request=lambda key:key))
   def apply_in_transaction(self,tx,ops,request,**kw):
    nonlocal semantic
    calls['apply']+=1;calls['operations']=ops;calls['documents']=kw['documents']
    if primary:raise primary
    if semantic is not None:return Ok(Success(Replay(semantic)))
    results=[];aliases=[]
    for op in ops:
     if isinstance(op,Create):
      identity=Identity('8','link');edges[8]=Edge(identity,1,op.source.identity,op.target.identity,None if isinstance(op.order_key,Null)else op.order_key.text,());results.append(Created('create_edge',identity,'1',op.alias));aliases.append(Alias(op.alias,identity))
     else:
      identity=op.target.identity;index=int(identity.id)
      if op.operation=='delete_edge':edges.pop(index)
      else:
       if any(e.source==identity or e.target==identity for e in edges.values()):raise RuntimeError('Owner dependencies remain')
       objects.pop(index)
      results.append(Deleted(op.operation,identity,'2',(DeletedId(identity,'2'),)))
    semantic=Semantic(tuple(results),tuple(aliases));return Ok(Success(Fresh(semantic)))
  class Connection:
   def execute(self,*args):pass
   def commit(self):calls['commit']+=1
   def close(self):
    calls['close']+=1
    if cleanup:raise cleanup
  truss.create_reference_assembly=lambda *args:Ok(Assembly());truss.adopt_psycopg_host=lambda c:SimpleNamespace(adopt_transaction=lambda:Ok(object()),detach=lambda:True);psycopg.connect=lambda *a,**kw:Connection()
  return {'truss':truss,'truss.contracts':contracts,'truss.groups':groups,'psycopg':psycopg},calls
 def test_required_link_replacement_and_incident_deletion_complete_replay(self):
  for replacement in (True,False):
   with self.subTest(replacement=replacement),tempfile.TemporaryDirectory()as d:
    root=Path(d).resolve();self.fixture(root,replacement=replacement);modules,calls=self.runtime()
    with patch.dict('sys.modules',modules):result=run_mutate_truss_record_group_command(root/'config',root/'result')
    self.assertEqual((calls['apply'],calls['commit'],calls['close']),(2,2,1));self.assertEqual(result['source_version'],'0.8.0');self.assertEqual(calls['documents'][0].content,(root/'model').read_bytes());self.assertTrue((root/'result').is_file())
    self.assertEqual([op.operation if hasattr(op,'operation')else'create_edge'for op in calls['operations']],['create_edge','delete_edge']if replacement else['delete_edge','delete_object'])
 def test_stale_and_wrong_readback_withhold_success_and_close(self):
  for stale in (True,False):
   with tempfile.TemporaryDirectory()as d:
    root=Path(d).resolve();self.fixture(root,replacement=True);modules,calls=self.runtime(stale=stale,wrong=not stale)
    with patch.dict('sys.modules',modules),self.assertRaises(RuntimeError):run_mutate_truss_record_group_command(root/'config',root/'result')
    self.assertFalse((root/'result').exists());self.assertEqual(calls['close'],1)
    if stale:self.assertEqual((calls['apply'],calls['commit']),(0,0))
 def test_owner_cancellation_survives_cleanup_and_no_report(self):
  with tempfile.TemporaryDirectory()as d:
   root=Path(d).resolve();self.fixture(root);primary=KeyboardInterrupt();modules,calls=self.runtime(primary=primary,cleanup=OSError('private'))
   with patch.dict('sys.modules',modules),self.assertRaises(KeyboardInterrupt)as caught:run_mutate_truss_record_group_command(root/'config',root/'result')
   self.assertIs(caught.exception,primary);self.assertTrue(primary.cleanup_failed);self.assertEqual((calls['commit'],calls['close']),(0,1));self.assertFalse((root/'result').exists())
 def test_cli_dispatch_once_no_effects(self):
  from ashlar.cli import main
  import contextlib,io
  with patch('sys.argv',['ashlar','mutate-truss-record-group','--configuration','/config','--output','/result']),patch('ashlar_host.truss_mutation_group.run_mutate_truss_record_group_command')as command,contextlib.redirect_stdout(io.StringIO()):main()
  command.assert_called_once_with('/config','/result')
