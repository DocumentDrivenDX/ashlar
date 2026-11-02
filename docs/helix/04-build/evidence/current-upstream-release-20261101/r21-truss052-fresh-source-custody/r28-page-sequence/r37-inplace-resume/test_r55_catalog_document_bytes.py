"""Focused contract test for the R55 model bytes passed to CatalogDocument."""
from __future__ import annotations
import ast, importlib.util, sys, types
from pathlib import Path
HERE=Path(__file__).resolve().parent
WORKER=HERE/"r55-continuation-worker.py"
tree=ast.parse(WORKER.read_text())
calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=="apply_group"]
assert len(calls)==1 and any(k.arg=="model" and isinstance(k.value,ast.Name) and k.value.id=="model_raw" for k in calls[0].keywords), "native group call must pass exact model bytes"
helper_spec=importlib.util.spec_from_file_location("r52_group_helper",HERE/"r52-group-helper.py")
helper=importlib.util.module_from_spec(helper_spec); helper_spec.loader.exec_module(helper)
received=[]
class CatalogDocument:
 def __init__(self, document_id, revision, content):
  if type(content) is not bytes: raise TypeError("CatalogDocument.content must be bytes")
  self.document_id=document_id; self.revision=revision; self.content=content; received.append(content)
class PreparedGroup: pass
class Ok:
 def __init__(self,value): self.value=value
class GroupSuccess:
 def __init__(self,response): self.response=response
class AppliedPending:
 semantic=types.SimpleNamespace(results=[object(),object()])
class Connection:
 def __enter__(self): return self
 def __exit__(self,*args): return False
 def commit(self): pass
class Assembly:
 group_selection=types.SimpleNamespace(layout_profile="l",capability_profile="c",value_profile="v")
 def prepare_group_in_transaction(self,*args,**kwargs):
  item=types.SimpleNamespace(authored_identity="Person",definition_pin="pin")
  context=types.SimpleNamespace(authorized_scope_identity="scope",request=lambda key:types.SimpleNamespace(identity=types.SimpleNamespace(authorized_scope_identity="scope")))
  prepared=PreparedGroup(); prepared.types=(item,); prepared.relationships=(); prepared.context=context
  return Ok(prepared)
 def apply_in_transaction(self,*args,**kwargs): return Ok(GroupSuccess(AppliedPending()))
class StopRun(RuntimeError): pass
def value(result,label):
 if not isinstance(result,Ok): raise StopRun(label)
 return result.value
def transaction(conn,fn,**kwargs): return fn(Assembly(),object())
base=types.SimpleNamespace(_value=value,StopRun=StopRun,_transaction=transaction)
old={n:sys.modules.get(n) for n in ("truss","truss.contracts","truss.groups","psycopg")}
truss=types.ModuleType("truss"); truss.CatalogDocument=CatalogDocument; truss.PreparedGroup=PreparedGroup
contracts=types.ModuleType("truss.contracts")
class Value:
 def __init__(self,**kw): self.__dict__.update(kw)
for n in ("MapEntry","MapValue","NullValue","TextValue"): setattr(contracts,n,Value)
groups=types.ModuleType("truss.groups")
for n in ("AliasObjectReference","AuthoredValue","CreateEdge","CreateObject","CurrentCatalog","GroupSemanticInput","RootlessOwnership","TextOrderKey"): setattr(groups,n,Value)
groups.GroupSuccess=GroupSuccess
psycopg=types.ModuleType("psycopg"); psycopg.connect=lambda **kw:Connection()
sys.modules.update({"truss":truss,"truss.contracts":contracts,"truss.groups":groups,"psycopg":psycopg})
try:
 import tempfile
 with tempfile.TemporaryDirectory() as temp:
  raw=b'{"id":"frozen-model"}'
  result=helper.apply_group(base,lambda role:{"user":role},model=raw,document_id="doc",document_revision="1",request_key="r55-test",origin={"x":"y"},operations=({"op":"create_object","type":"Person","alias":"p"},),bun=Path("/bin/true"),receipt_path=Path(temp)/"receipt.json",expected_count=2)
  assert isinstance(result,GroupSuccess) and received==[raw] and received[0] is raw
finally:
 for n,v in old.items():
  if v is None: sys.modules.pop(n,None)
  else: sys.modules[n]=v
print("R55 exact model bytes reach CatalogDocument.content")
