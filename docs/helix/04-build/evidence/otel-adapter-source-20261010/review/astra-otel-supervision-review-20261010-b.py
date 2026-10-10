"""Finite fake subprocess/selector controls; no process or network execution."""
import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import struct
import sys
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

BASE=Path('/private/tmp/astra-otel-supervision-review-20261010-b')
ROOT=Path('/Users/erik/Projects/ashlar')
source=BASE.with_suffix('.source.py')
def digest(path):
    data=path.read_bytes(); assert len(data)<200000
    return {'path':str(path),'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
inputs={name:digest(ROOT/path) for name,path in {
 'owner_source':'src/ashlar_host/otel.py','author_tests':'tests/test_otel_supervision.py',
 'worker':'src/ashlar_host/_otel_worker.py','worker_tests':'tests/test_otel_worker.py',
 'config':'src/ashlar_host/config.py','diagnostics':'src/ashlar_host/diagnostics.py',
 'contract':'docs/helix/02-design/contracts/CONTRACT-006-diagnostics.md',
 'adr':'docs/helix/02-design/adr/ADR-001-delta-canonical-and-serving-layout.md',
 'fixture_dependency':'tests/test_diagnostics_configuration.py'}.items()}
inputs['executed_snapshot']=digest(source)
assert inputs['executed_snapshot']['sha256']=='58c4af496201627a63a2c6cfc7dc2f412380d6adad3c723d6bb5b9fd3a9281b5'
sys.path.insert(0,str(ROOT/'src'))
spec=importlib.util.spec_from_file_location('ashlar_host.otel',source)
mod=importlib.util.module_from_spec(spec);sys.modules[spec.name]=mod;spec.loader.exec_module(mod)
checks=[]; observations=[]
def check(name,value):
    assert value,name
    checks.append(name)
def caught(action):
    try: action()
    except BaseException as exc: return exc
    return None
def prohibited(*args,**kwargs): raise AssertionError('real subprocess port prohibited')
class Clock:
    now=10.
    def monotonic(self): return self.now
class Stream:
    def __init__(self,fd,failure=None): self.fd=fd;self.failure=failure;self.closes=0
    def fileno(self):return self.fd
    def close(self):
        self.closes+=1
        if self.failure:raise self.failure
class Process:
    pid=99999999
    def __init__(self,exitcode=None,first=None,second=None):
        self.exitcode=exitcode;self.returncode=exitcode;self.stdin=Stream(80,first);self.stdout=Stream(81,second)
        self.waits=[];self.group_alive=True;self.kills=[]
    def poll(self):return self.exitcode
    def wait(self,timeout):self.waits.append(timeout);self.exitcode=0;self.returncode=0;return 0
    def kill(self,pid,signal):
        assert pid==self.pid
        self.kills.append(signal);self.group_alive=False
def run(process=None):
    r=mod.OtelRun.__new__(mod.OtelRun)
    r._process=process or Process();r._closed=False;r._lock=threading.Lock();r._local=threading.local()
    r._group_termination_attempted=False
    r._config=SimpleNamespace(limits=SimpleNamespace(export_timeout_ms=100,shutdown_timeout_ms=1000,max_queue_records=1))
    return r
def good_loss():return {name:{'submitted':0,'handed_off':0,'dropped':0,'unknown':False,'flush':'complete'} for name in ('logs','spans','metrics')}
def frame(value):
    raw=json.dumps({'ok':True,'value':value},separators=(',',':')).encode()
    return struct.pack('>I',len(raw))+raw
class Pipe:
    def __init__(self,data,clock,selector_error=None,close_error=None,partial=False):
        self.data=bytearray(data);self.clock=clock;self.selector_error=selector_error;self.close_error=close_error
        self.partial=partial;self.written=bytearray();self.timeouts=[];self.reads=0
    def read(self,fd,count):
        assert fd==81;self.reads+=1
        if self.partial:count=min(3,count)
        result=self.data[:count];del self.data[:count];return bytes(result)
    def write(self,fd,data):
        assert fd==80
        if self.partial:data=data[:3]
        self.written.extend(data);return len(data)
    def selector(self):
        owner=self
        class Selector:
            def __enter__(self):return self
            def __exit__(self,*args):
                if owner.close_error:raise owner.close_error
            def close(self):
                if owner.close_error:raise owner.close_error
            def register(self,*args):pass
            def select(self,timeout):
                owner.timeouts.append(timeout)
                if owner.selector_error:raise owner.selector_error
                owner.clock.now+=.0001
                return [True]
        return Selector()
    def patches(self,stack):
        stack.enter_context(patch.object(mod.selectors,'DefaultSelector',side_effect=self.selector))
        stack.enter_context(patch.object(mod.os,'read',side_effect=self.read))
        stack.enter_context(patch.object(mod.os,'write',side_effect=self.write))

with patch.object(mod.subprocess,'Popen',side_effect=prohibited):
    clock=Clock();p=Process(first=OSError('ordinary'),second=KeyboardInterrupt('cancel'));r=run(p)
    with patch.object(mod.time,'monotonic',clock.monotonic),patch.object(mod.os,'killpg',side_effect=p.kill):
        error=caught(lambda:r._dispose(11.))
    check('SUP001 corrected cleanup first cancellation exact',error is p.stdout.failure)
    check('cleanup still attempts streams and wait',p.stdin.closes==1 and p.stdout.closes==1 and len(p.waits)==1)
    observations.append({'case':'cleanup_late_cancellation','expected':'original KeyboardInterrupt','observed':type(error).__name__,'identity_preserved':error is p.stdout.failure})

    primary=KeyboardInterrupt('original');p=Process(first=OSError('ordinary'));r=run(p)
    with patch.object(mod.os,'killpg',side_effect=p.kill):
        check('preexisting cancellation preserved by disposal',caught(lambda:r._dispose(primary=primary)) is None and primary.cleanup_failed)

    primary=KeyboardInterrupt('select');clock=Clock();p=Process();r=run(p)
    pipe=Pipe(b'',clock,selector_error=primary,close_error=OSError('selector close'))
    with contextlib.ExitStack() as stack:
        pipe.patches(stack);stack.enter_context(patch.object(mod.time,'monotonic',clock.monotonic))
        stack.enter_context(patch.object(mod.os,'killpg',side_effect=p.kill))
        error=caught(lambda:r._invoke({'op':'context'}))
    check('SUP002 corrected selector exit original cancellation identity',error is primary and primary.cleanup_failed)
    check('selector cancellation replacement still disposes child',not p.group_alive and len(p.waits)==1 and not r._lock.locked())
    observations.append({'case':'selector_cleanup_preserves_primary','expected':'original KeyboardInterrupt','observed':type(error).__name__,'identity_preserved':error is primary})

    p=Process(exitcode=0);r=run(p)
    with patch.object(mod.os,'killpg',side_effect=p.kill),patch.object(r,'_exchange',return_value=good_loss()):
        loss=r.shutdown(11.)
    check('SUP003 corrected leader-exited group killed',not p.group_alive and p.kills==[9] and loss==good_loss())
    observations.append({'case':'leader_exited_group_alive','killpg_calls':len(p.kills),'shutdown_loss_complete':True,'fake_group_survives':p.group_alive,
        'qualification':'Fake child group model; no real descendant process spawned and no claim current SDK actually creates descendants.'})

    clock=Clock();p=Process();r=run(p)
    def timeout_exchange(request,deadline):clock.now=deadline;raise mod.DiagnosticsError('diagnostics-configuration')
    with patch.object(mod.time,'monotonic',clock.monotonic),patch.object(mod.os,'killpg',side_effect=p.kill),patch.object(r,'_exchange',side_effect=timeout_exchange):
        error=caught(lambda:r._invoke({'op':'context'}))
    check('control failure allocates fresh cleanup window',p.waits==[2.0])
    observations.append({'case':'control_timeout_cleanup_window','request_deadline':10.1,'reap_timeout_after_request_deadline':p.waits[0],
       'qualification':'Shutdown explicit deadline does not refresh; startup/control disposal has an additional hard-coded two-second ceiling.'})

    clock=Clock();p=Process();r=run(p)
    with patch.object(mod.time,'monotonic',clock.monotonic),patch.object(mod.os,'killpg',side_effect=p.kill):r._dispose(10.)
    check('expired explicit disposal deadline does not refresh',p.waits==[0])

    r=run();r._lock.acquire()
    with patch.object(r,'_exchange',side_effect=AssertionError('interleaved exchange')):
        check('busy invocation refuses without touching process',isinstance(caught(lambda:r._invoke({'op':'context'})),mod.DiagnosticsError) and not r._closed)
    r._lock.release()

    clock=Clock();r=run();wire=frame({'transport_deadline':10.05})+frame({'transport_complete':True})+frame(good_loss())
    pipe=Pipe(wire,clock)
    with contextlib.ExitStack() as stack:
        pipe.patches(stack);stack.enter_context(patch.object(mod.time,'monotonic',clock.monotonic))
        loss=r._exchange({'op':'close','deadline':11.},11.)
    check('transport completion restores only original whole deadline',loss==good_loss() and any(0<t<.05 for t in pipe.timeouts) and pipe.timeouts[-1]>.9)

    for name,wire in (
        ('oversize length',struct.pack('>I',65537)),
        ('duplicate response member',struct.pack('>I',34)+b'{"ok":true,"ok":true,"value":null}'),
        ('active transport final without completion',frame({'transport_deadline':10.05})+frame(good_loss())),
        ('nested transport begin',frame({'transport_deadline':10.05})*2),
        ('completion without begin',frame({'transport_complete':True})),
        ('per-request deadline exceeds whole',frame({'transport_deadline':12.})),
        ('finite progress cap',(frame({'transport_deadline':10.05})+frame({'transport_complete':True}))*23),
    ):
        clock=Clock();r=run();pipe=Pipe(wire,clock)
        with contextlib.ExitStack() as stack:
            pipe.patches(stack);stack.enter_context(patch.object(mod.time,'monotonic',clock.monotonic))
            error=caught(lambda:r._exchange({'op':'close','deadline':11.},11.))
        check('reject '+name,isinstance(error,mod.DiagnosticsError))

    clock=Clock();r=run();pipe=Pipe(frame(None),clock,partial=True)
    with contextlib.ExitStack() as stack:
        pipe.patches(stack);stack.enter_context(patch.object(mod.time,'monotonic',clock.monotonic))
        value=r._exchange({'op':'context'},11.)
    check('partial pipe reads and writes round trip',value is None and json.loads(pipe.written[4:])=={'op':'context'})

    r=run();r._local.context=({'trace_id':'1'*32,'span_id':'2'*16,'trace_flags':1,'is_remote':True},None)
    requests=[]
    with patch.object(r,'_invoke',side_effect=lambda request:requests.append(request)):
        for event in ('started','phase','finished'):r.trace_context('3'*32,'publication','ashlar.operation.'+event)
    check('updated parent context forwarded only at start',requests[0]['parent'] is not None and all(q['parent'] is None and q['retry_link'] is None for q in requests[1:]))

    # A bounded fake syscall pause exposes a concurrent caller reaping before
    # the first caller has completed its supposedly protecting group action.
    p=Process(exitcode=0);r=run(p);entered=threading.Event();release=threading.Event();order=[];errors=[]
    def pending_kill(pid,signal):
        order.append('kill_entered');entered.set()
        assert release.wait(1)
        p.kill(pid,signal);order.append('kill_completed')
    def reap(timeout):order.append('reap');p.waits.append(timeout);return 0
    def dispose_thread():
        error=caught(lambda:r._dispose())
        if error is not None:errors.append(type(error).__name__)
    p.wait=reap
    with patch.object(mod.os,'killpg',side_effect=pending_kill):
        first=threading.Thread(target=dispose_thread);first.start()
        try:
            assert entered.wait(1)
            r._dispose()
        finally:
            release.set();first.join(1)
    check('concurrent disposal threads completed',not first.is_alive() and not errors)
    check('reproduced concurrent reap precedes effective kill',order==['kill_entered','reap','kill_completed','reap'])
    observations.append({'case':'concurrent_disposal_order','order':order,'qualification':'Fake delayed killpg models scheduling before the kernel group action; no numeric PID reuse or actual unrelated process was tested.'})

# Execute the eleven inspected owner tests: actual finite helper processes,
# never the actual exporter worker. SDK API span construction in one owner
# test is inert and does not construct an exporter or receiver.
sys.path.insert(0,str(ROOT/'tests'))
test_spec=importlib.util.spec_from_file_location('review_supervision_tests',ROOT/'tests/test_otel_supervision.py')
tests=importlib.util.module_from_spec(test_spec);test_spec.loader.exec_module(tests)
owner_stream=io.StringIO()
owner_result=unittest.TextTestRunner(stream=owner_stream,verbosity=2).run(unittest.defaultTestLoader.loadTestsFromModule(tests))
BASE.with_suffix('.author.log').write_text(owner_stream.getvalue())
check('all11 inspected owner tests pass',owner_result.testsRun==11 and owner_result.wasSuccessful() and not owner_result.skipped)
closing={name:digest(Path(value['path'])) for name,value in inputs.items()}
check('all reviewed source dependency test docs and snapshot hashes unchanged',closing==inputs)

log=json.dumps({'checks':checks,'observations':observations},indent=2,sort_keys=True)+'\n'
BASE.with_suffix('.log').write_text(log)
receipt={'status':'SUP001-003 resolved; concurrent cleanup ordering finding remains; not frozen approval','inputs':inputs,
 'script':digest(Path(__file__)),'log':digest(BASE.with_suffix('.log')),'checks':len(checks),
 'findings':[{'id':'SUP-004','issue':'Concurrent _dispose callers can reap before the first caller completes killpg: _group_termination_attempted becomes true before the group action and no disposal coordination prevents another caller immediately reaching wait. This breaks the new unreaped-PID ownership argument.','smallest_correction':'Coordinate one owner of group termination and reaping; concurrent callers must not reap or report completed cleanup while its group action is pending. Preserve the supplied total deadline and cancellation semantics.'}],
 'resolved':['SUP-001 first cancellation precedence','SUP-002 primary-preserving selector closure','SUP-003 leader liveness no longer skips owned group action'],
 'author_tests':{'count':owner_result.testsRun,'passed':owner_result.wasSuccessful(),'skipped':len(owner_result.skipped),'log':digest(BASE.with_suffix('.author.log'))},
 'observations':[{'id':'SUP-O01','issue':'Startup/control failure cleanup creates a fresh two-second wait budget; explicit shutdown deadline is retained.','disposition':'Do not describe all startup/control lifecycle work as covered by its request deadline. Either include cleanup in the original total or explicitly document this separate allowance; no claimed shutdown refresh defect.'}],
 'limitations':['Independent controls use finite fake process/clock/pipe/selector ports; eleven owner tests additionally run inspected finite helper subprocesses and one inert SDK API span; no exporter/receiver/network/native/Git','Source snapshot and six implementation/test files plus direct fixtures/docs pinned before and after','Concurrent ownership counterexample uses fake scheduling, not demonstrated PID reuse'],
 'closing_owner_source':digest(ROOT/'src/ashlar_host/otel.py')}
BASE.with_suffix('.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')
print(json.dumps({'receipt':str(BASE.with_suffix('.json')),'sha256':digest(BASE.with_suffix('.json'))['sha256'],'checks':len(checks),'source':inputs['executed_snapshot']['sha256']}))
