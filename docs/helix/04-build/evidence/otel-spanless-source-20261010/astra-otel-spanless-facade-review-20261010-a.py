from pathlib import Path
from contextvars import ContextVar
from importlib import metadata
import ast,asyncio,hashlib,json,sys,threading
ROOT=Path('/Users/erik/Projects/ashlar');sys.path.insert(0,str(ROOT/'src'))
from ashlar_host.otel import OtelRun
from ashlar_host.diagnostics import DiagnosticsError
from opentelemetry.trace import SpanContext,TraceFlags,TraceState
def d(p):
 p=Path(p);r=p.read_bytes();return {'path':str(p),'bytes':len(r),'sha256':hashlib.sha256(r).hexdigest()}
freeze=Path('/private/tmp/ashlar-spanless-source-controls-20261010-b.json');assert d(freeze)['sha256']=='99007a9fc1d84eef003f01e54c5d82e12b3ef3af9933132ff76629b879a009c5'
owned=json.loads(freeze.read_bytes())['files'];pins=[]
for x in owned:
 x={**x,'path':str(ROOT/x['path'])};assert d(x['path'])==x;pins.append(x)
run=OtelRun.__new__(OtelRun);run._operation_binding=ContextVar('independent_spanless_binding',default=(None,None,True));calls=[]
run._invoke=lambda request:calls.append(request)
def start(label='1'):return run.trace_context(label*32,'held-read','ashlar.operation.started')
def callmode():return calls[-1]['create_span']
checks=[]
for invalid in (None,0,1,0.0,1.0,'false',[],{},object()):
 before=len(calls)
 try:
  with run.operation_context(create_span=invalid):raise AssertionError('invalidmodeentered')
 except DiagnosticsError as error:assert str(error)=='diagnostics-configuration'
 assert len(calls)==before and run._operation_binding.get()==(None,None,True)
checks.append('9nonboolvaluesrefusebeforebinding/RPC')
parent=SpanContext(0x1234567890abcdef1234567890abcdef,0x1234567890abcdef,True,TraceFlags(3),TraceState())
for port in ('parent_context','retry_link'):
 for value in (parent,object()):
  before=len(calls)
  try:
   with run.operation_context(create_span=False,**{port:value}):raise AssertionError('conflictingcontextentered')
  except DiagnosticsError:pass
  assert len(calls)==before and run._operation_binding.get()==(None,None,True)
checks.append('4spanlesscontextconflictsrefusebeforeRPC')
start();assert callmode() is True
with run.operation_context(create_span=False):
 start();assert callmode() is False
 with run.operation_context():start();assert callmode() is True
 start();assert callmode() is False
 for name in ('phase','finished'):
  run.trace_context('1'*32,'held-read','ashlar.operation.'+name)
  assert 'create_span' not in calls[-1] and calls[-1]['parent'] is calls[-1]['retry_link'] is None
start();assert callmode() is True
checks.append('default/nested/restorationandstartonlywireselection')
for exception in (KeyboardInterrupt(),SystemExit(),GeneratorExit()):
 try:
  with run.operation_context(create_span=False):raise exception
 except BaseException as error:assert error is exception
 assert run._operation_binding.get()==(None,None,True)
checks.append('3nonExceptionidentitiesandbindingrestoration')
with run.operation_context(parent_context=parent):
 start();assert calls[-1]['parent']=={'trace_id':'%032x'%parent.trace_id,'span_id':'%016x'%parent.span_id,'trace_flags':3,'is_remote':True}
 with run.operation_context(create_span=False):start();assert calls[-1]['parent'] is None and callmode() is False
 start();assert calls[-1]['parent']['trace_id']=='%032x'%parent.trace_id and callmode() is True
checks.append('explicitparentrestoresaroundspanlessbinding')
thread_results=[]
with run.operation_context(create_span=False):
 def other():
  thread_results.append(run._operation_binding.get())
  with run.operation_context():thread_results.append(run._operation_binding.get())
 thread=threading.Thread(target=other);thread.start();thread.join(1);assert not thread.is_alive()
 assert run._operation_binding.get()==(None,None,False)
assert thread_results==[(None,None,True),(None,None,True)]
checks.append('threadindependence')
async def async_controls():
 entered=asyncio.Event();release=asyncio.Event();seen=[]
 async def left():
  with run.operation_context(create_span=False):
   entered.set();await release.wait();seen.append(('left',run._operation_binding.get()[2]))
  seen.append(('left-restored',run._operation_binding.get()[2]))
 async def right():
  await entered.wait();seen.append(('right',run._operation_binding.get()[2]));release.set();await asyncio.sleep(0)
  seen.append(('right-after',run._operation_binding.get()[2]))
 await asyncio.gather(left(),right())
 assert dict(seen)=={'left':False,'left-restored':True,'right':True,'right-after':True}
 entered=asyncio.Event()
 async def cancelled():
  try:
   with run.operation_context(create_span=False):entered.set();await asyncio.Event().wait()
  finally:seen.append(('cancel-restored',run._operation_binding.get()[2]))
 task=asyncio.create_task(cancelled());await entered.wait();task.cancel()
 try:await task
 except asyncio.CancelledError:pass
 assert dict(seen)['cancel-restored'] is True and run._operation_binding.get()==(None,None,True)
asyncio.run(async_controls());checks.append('interleavedtasksandtaskcancellationrestoreisolatedbindings')
other=OtelRun.__new__(OtelRun);other._operation_binding=ContextVar('independent_other_owner',default=(None,None,True))
with run.operation_context(create_span=False):assert other._operation_binding.get()==(None,None,True)
checks.append('separateownerbindingsdonotleak')
prior=Path('/private/tmp/ashlar-otel-installed-preparation-20261010-a/source/src/ashlar_host/otel.py')
old=ast.parse(prior.read_bytes());new=ast.parse((ROOT/'src/ashlar_host/otel.py').read_bytes())
def methods(tree):
 cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='OtelRun')
 return {n.name:ast.dump(n,include_attributes=False) for n in cls.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))}
a=methods(old);b=methods(new);changed={'__init__','operation_context','trace_context'}
assert a.keys()==b.keys() and all(a[n]==b[n] for n in a if n not in changed)
checks.append('allunchangedlifecycle/supervisionmethodASTsidentical')
for row in pins:assert d(row['path'])==row
receipt={'verdict':'approved-spanless-facade-source-only','exactFiles':pins,'ownerReceipt':d(freeze),'independentControls':checks,'controlGroups':len(checks),'priorApprovedFacadeSnapshot':d(prior),'runtime':{'python':sys.version,'opentelemetryApi':metadata.version('opentelemetry-api')},'scope':'Pure actual owning facade methods through __new__ with explicit fake RPC sink and genuine API SpanContext fixture; no facade constructor, worker, receiver, network or native execution. Actualprocessownerregressions run separately.','findings':[],'limitations':['SpanlessSDKprojection/queue/lossreview delegatedseparately.','No actual installedreceiveruntracedqualification or fullC006claim.'],'script':d(__file__)}
out=Path('/private/tmp/astra-otel-spanless-facade-review-20261010-a.json');out.write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(d(out)))
