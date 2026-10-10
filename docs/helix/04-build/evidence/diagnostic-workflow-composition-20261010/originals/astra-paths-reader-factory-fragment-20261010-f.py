"""Exact source try/finally fragment with inert ports; not whole-factory/native proof."""
import ast,hashlib,json
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
from ashlar_host import publication_reader as module
path=Path('/Users/erik/Projects/ashlar/src/ashlar_host/publication_reader.py');raw=path.read_bytes()
assert hashlib.sha256(raw).hexdigest()=='d0693a36262871f5e4fe6335dadb7b67e5330ab1abe2b420d98724ade6b3ba5f'
tree=ast.parse(raw);factory=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name=='open_commerce_reader');owned=next(n for n in factory.body if isinstance(n,ast.Try))
observations=[]
pairs=[(ValueError('synthetic-closing-drift'),failure)for failure in (OSError('synthetic-close'),KeyboardInterrupt(),SystemExit(),GeneratorExit())]
pairs.extend((guard,OSError('synthetic-close'))for guard in (KeyboardInterrupt(),SystemExit(),GeneratorExit()))
cases=[(guard,failure,True,use_state)for guard,failure in pairs for use_state in (False,True)]
cases.extend((None,failure,True,use_state)for failure in (OSError('synthetic-close'),KeyboardInterrupt(),SystemExit(),GeneratorExit())for use_state in (False,True))
cases.extend((guard,None,False,use_state)for guard in (OSError('synthetic-open'),KeyboardInterrupt(),SystemExit(),GeneratorExit())for use_state in (False,True))
for guard,failure,owned_transport,use_state in cases:
 state=module.ReaderCleanupState()if use_state else None
 closed=[];guard_observed=[];entered=[]
 class Database:
  def execute(self,sql,*args):
   if 'local_publication_artifact'in sql:return SimpleNamespace(fetchone=lambda:(json.dumps({'request_digest':'synthetic'}),json.dumps({'manifest':manifest})))
   return SimpleNamespace(fetchone=lambda:(json.dumps({'selected_steps':[],'complete_prior_oracle':{},'generated_steps':[],'zero_match_elisions':[]}),))
 class Transport:
  db=Database()
  def close(self):
   closed.append(True);raise failure
 manifest={'table_versions_json':'{}','source_progress_json':'{}','publication_id':'synthetic'}
 calls=[]
 def native_files():
  if guard is not None:
   guard_observed.append(True);raise guard
  return {}
 def opening(*args):
  if not owned_transport:raise guard
  return Transport()
 namespace=dict(vars(module));namespace.update(transport=None,primary=None,spark=None,publication=Path('/synthetic'),targets=[],
  PrivatePolicy=lambda *args:SimpleNamespace(initializing=True),ReadOnlyTransport=SimpleNamespace(open=opening),
  original_report={'native_manifest':manifest,'protected_ack_scope':{},'source_schema':'synthetic','source_signature_sha256':'synthetic'},
  AckScope=lambda **kwargs:SimpleNamespace(service_schema='ashlar_ack_pipeline_synthetic'),NativeDriver=lambda *args,**kwargs:object(),
  batch=SimpleNamespace(feed='synthetic'),admission=SimpleNamespace(changes=[]),fixture_columns=lambda *args:[],
  ROOT=Path('/synthetic'),expected={},model=b'{}',graph=b'{}',bindings={},compiler_request=lambda *args:{'target':{'bindingJson':'{}'}},
  PublicationProvider=lambda *args,**kwargs:SimpleNamespace(_reader_closed=False),native_files=native_files,
  original_native={},cleanup_state=state)
 function=ast.FunctionDef(name='exercise',args=ast.arguments(posonlyargs=[],args=[],kwonlyargs=[],kw_defaults=[],defaults=[]),body=[ast.Assign(targets=[ast.Name(id='transport',ctx=ast.Store())],value=ast.Constant(None)),ast.Assign(targets=[ast.Name(id='primary',ctx=ast.Store())],value=ast.Constant(None)),owned],decorator_list=[])
 fragment=ast.fix_missing_locations(ast.Module(body=[function],type_ignores=[]));exec(compile(fragment,str(path),'exec'),namespace)
 try:
  with contextmanager(namespace['exercise'])():entered.append(True)
 except BaseException as actual:
  expected=guard if guard is not None else failure
  assert actual is expected
  assert len(closed)==int(owned_transport)
  assert bool(entered)is owned_transport
  assert bool(guard_observed)is (owned_transport and guard is not None)
  if state is not None:
   assert state.failed is owned_transport
   assert state.cleanup_only is (owned_transport and guard is None)
  if guard is not None and owned_transport:assert guard.cleanup_failed is True
  observations.append({'guardClass':type(guard).__name__,'cleanupClass':type(failure).__name__,'cleanupStatePresent':use_state,'ownedTransport':owned_transport,'primaryIdentityPreserved':True,'closeCount':len(closed),'cleanupFailedFact':state.failed if state is not None else None,'cleanupOnlyFact':state.cleanup_only if state is not None else None})
 else:raise AssertionError('combined-fault-lost')
assert hashlib.sha256(path.read_bytes()).hexdigest()=='d0693a36262871f5e4fe6335dadb7b67e5330ab1abe2b420d98724ade6b3ba5f'
value={'scope':'Exact open_commerce_reader acquired-transport try/finally AST fragment, inert ports; no whole-factory or native execution claim','sourceSha256':hashlib.sha256(raw).hexdigest(),'resolution':'Factory finally now uses owner finish(primary, actual observed close), preserving primary identity, with and without cleanup-state observation. No transport means no close; cleanup-only errors remain original cleanup failures.','observations':observations}
Path('/private/tmp/astra-paths-reader-factory-fragment-20261010-f.json').write_text(json.dumps(value,indent=2)+'\n');print(json.dumps(value))
