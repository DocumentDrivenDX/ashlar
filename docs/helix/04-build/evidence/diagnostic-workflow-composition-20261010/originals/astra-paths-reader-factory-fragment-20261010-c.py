"""Exact source try/finally fragment with inert ports; not whole-factory/native proof."""
import ast,hashlib,json
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
from ashlar_host import publication_reader as module
path=Path('/Users/erik/Projects/ashlar/src/ashlar_host/publication_reader.py');raw=path.read_bytes()
assert hashlib.sha256(raw).hexdigest()=='f023b4a0ba0da047b1c404b8ab095938e3afea4db7b9430ab2f1155e0c33d0a6'
tree=ast.parse(raw);factory=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name=='open_commerce_reader');owned=next(n for n in factory.body if isinstance(n,ast.Try))
observations=[]
for failure in (OSError('synthetic-close'),KeyboardInterrupt(),SystemExit(),GeneratorExit()):
 guard=ValueError('synthetic-closing-drift');state=module.ReaderCleanupState()
 class Database:
  def execute(self,sql,*args):
   if 'local_publication_artifact'in sql:return SimpleNamespace(fetchone=lambda:('{}',json.dumps({'manifest':manifest})))
   return SimpleNamespace(fetchone=lambda:(json.dumps({'selected_steps':[],'complete_prior_oracle':{},'generated_steps':[],'zero_match_elisions':[]}),))
 class Transport:
  db=Database()
  def close(self):raise failure
 manifest={'table_versions_json':'{}','source_progress_json':'{}','publication_id':'synthetic'}
 calls=[]
 def native_files():raise guard
 namespace=dict(vars(module));namespace.update(transport=None,primary=None,spark=None,publication=Path('/synthetic'),targets=[],
  PrivatePolicy=lambda *args:SimpleNamespace(initializing=True),ReadOnlyTransport=SimpleNamespace(open=lambda *args:Transport()),
  original_report={'native_manifest':manifest,'protected_ack_scope':{},'source_schema':'synthetic','source_signature_sha256':'synthetic'},
  AckScope=lambda **kwargs:SimpleNamespace(service_schema='ashlar_ack_pipeline_synthetic'),NativeDriver=lambda *args,**kwargs:object(),
  batch=SimpleNamespace(feed='synthetic'),admission=SimpleNamespace(changes=[]),fixture_columns=lambda *args:[],
  expected={},model=b'{}',graph=b'{}',bindings={},compiler_request=lambda *args:{'target':{'bindingJson':'{}'}},
  PublicationProvider=lambda *args,**kwargs:SimpleNamespace(_reader_closed=False),native_files=native_files,
  original_native={},cleanup_state=state)
 function=ast.FunctionDef(name='exercise',args=ast.arguments(posonlyargs=[],args=[],kwonlyargs=[],kw_defaults=[],defaults=[]),body=[ast.Assign(targets=[ast.Name(id='transport',ctx=ast.Store())],value=ast.Constant(None)),ast.Assign(targets=[ast.Name(id='primary',ctx=ast.Store())],value=ast.Constant(None)),owned],decorator_list=[])
 fragment=ast.fix_missing_locations(ast.Module(body=[function],type_ignores=[]));exec(compile(fragment,str(path),'exec'),namespace)
 try:
  with contextmanager(namespace['exercise'])():pass
 except BaseException as actual:
  assert actual is failure
  assert state.failed and not state.cleanup_only
  observations.append({'cleanupClass':type(failure).__name__,'guardIdentityPreserved':False,'actualIdentityIsCleanup':True,'cleanupFailedFact':True,'cleanupOnlyFact':False})
 else:raise AssertionError('combined-fault-lost')
assert hashlib.sha256(path.read_bytes()).hexdigest()=='f023b4a0ba0da047b1c404b8ab095938e3afea4db7b9430ab2f1155e0c33d0a6'
value={'scope':'Exact open_commerce_reader acquired-transport try/finally AST fragment, inert ports; no whole-factory or native execution claim','sourceSha256':hashlib.sha256(raw).hexdigest(),'finding':'Inherited direct finally-close replaces post-yield guard primary; added cleanup fact correctly marks failed true, cleanup_only false. Not a new fact-port identity regression.','observations':observations}
Path('/private/tmp/astra-paths-reader-factory-fragment-20261010-c.json').write_text(json.dumps(value,indent=2)+'\n');print(json.dumps(value))
