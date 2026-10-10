"""Finite fake subprocess/selector controls; no process or network execution."""
import contextlib
import hashlib
import importlib.util
import json
from pathlib import Path
import struct
import sys
import threading
from types import SimpleNamespace
from unittest.mock import patch

BASE=Path('/private/tmp/astra-otel-supervision-review-20261010-a')
ROOT=Path('/Users/erik/Projects/ashlar')
source=BASE.with_suffix('.source.py')
def digest(path):
    data=path.read_bytes(); assert len(data)<200000
    return {'path':str(path),'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
inputs={name:digest(ROOT/path) for name,path in {
 'owner_source':'src/ashlar_host/otel.py','author_tests':'tests/test_otel_supervision.py',
 'config':'src/ashlar_host/config.py','diagnostics':'src/ashlar_host/diagnostics.py',
 'contract':'docs/helix/02-design/contracts/CONTRACT-006-diagnostics.md'}.items()}
inputs['executed_snapshot']=digest(source)
assert inputs['executed_snapshot']['sha256']=='1dadd52fe6c24d81f4e8cc047db4c29151c91123a9aee48be934860850c6624c'
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
        self.exitcode=exitcode;self.stdin=Stream(80,first);self.stdout=Stream(81,second)
        self.waits=[];self.group_alive=True;self.kills=[]
    def poll(self):return self.exitcode
    def wait(self,timeout):self.waits.append(timeout);self.exitcode=0;return 0
    def kill(self,pid,signal):
        assert pid==self.pid
        self.kills.append(signal);self.group_alive=False
def run(process=None):
    r=mod.OtelRun.__new__(mod.OtelRun)
    r._process=process or Process();r._closed=False;r._lock=threading.Lock();r._local=threading.local()
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
    check('reproduced cleanup cancellation suppressed by prior ordinary cleanup',isinstance(error,mod.DiagnosticsError))
    check('cleanup still attempts streams and wait',p.stdin.closes==1 and p.stdout.closes==1 and len(p.waits)==1)
    observations.append({'case':'cleanup_late_cancellation','expected':'original KeyboardInterrupt','observed':type(error).__name__})

    primary=KeyboardInterrupt('original');p=Process(first=OSError('ordinary'));r=run(p)
    with patch.object(mod.os,'killpg',side_effect=p.kill):
        check('preexisting cancellation preserved by disposal',caught(lambda:r._dispose(primary=primary)) is None and primary.cleanup_failed)

    primary=KeyboardInterrupt('select');clock=Clock();p=Process();r=run(p)
    pipe=Pipe(b'',clock,selector_error=primary,close_error=OSError('selector close'))
    with contextlib.ExitStack() as stack:
        pipe.patches(stack);stack.enter_context(patch.object(mod.time,'monotonic',clock.monotonic))
        stack.enter_context(patch.object(mod.os,'killpg',side_effect=p.kill))
        error=caught(lambda:r._invoke({'op':'context'}))
    check('reproduced selector exit replacing cancellation',isinstance(error,mod.DiagnosticsError) and error is not primary)
    check('selector cancellation replacement still disposes child',not p.group_alive and len(p.waits)==1 and not r._lock.locked())
    observations.append({'case':'selector_cleanup_masks_primary','expected':'original KeyboardInterrupt','observed':type(error).__name__})

    p=Process(exitcode=0);r=run(p)
    with patch.object(mod.os,'killpg',side_effect=p.kill),patch.object(r,'_exchange',return_value=good_loss()):
        loss=r.shutdown(11.)
    check('reproduced leader-exited live-group skip',p.group_alive and p.kills==[] and loss==good_loss())
    observations.append({'case':'leader_exited_group_alive','killpg_calls':0,'shutdown_loss_complete':True,'fake_group_survives':True,
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

log=json.dumps({'checks':checks,'observations':observations},indent=2,sort_keys=True)+'\n'
BASE.with_suffix('.log').write_text(log)
receipt={'status':'provisional candidate review; not frozen approval','inputs':inputs,
 'script':digest(Path(__file__)),'log':digest(BASE.with_suffix('.log')),'checks':len(checks),
 'findings':[
 {'id':'SUP-001','lines':[99,114],'issue':'With primary=None, an ordinary cleanup failure before the first cleanup cancellation makes _dispose raise fixed DiagnosticsError and suppress the original cancellation.','smallest_correction':'With primary=None, retain the first non-Exception cancellation over preceding ordinary diagnostic cleanup errors; finish all cleanup and propagate that original object. Preserve an existing operation primary.'},
 {'id':'SUP-002','lines':[125,145],'issue':'Selector context-manager cleanup can replace the original select/read/write cancellation, which _invoke then turns into DiagnosticsError.','smallest_correction':'Close selectors with primary-preserving cleanup, including register/select/read/write failure paths.'},
 {'id':'SUP-003','lines':[89,94],'issue':'_dispose skips killpg when direct child already exited, allowing an owned descendant group to survive while shutdown returns successful complete accounting in a fake process model.','smallest_correction':'Dispose the owned process group independently of leader liveness, then reap the direct child; scope process-group ownership carefully.'}],
 'observations':[{'id':'SUP-O01','issue':'Startup/control failure cleanup creates a fresh two-second wait budget; explicit shutdown deadline is retained.','disposition':'Do not describe all startup/control lifecycle work as covered by its request deadline. Either include cleanup in the original total or explicitly document this separate allowance; no claimed shutdown refresh defect.'}],
 'limitations':['Only finite fake process, clock, pipe and selector ports executed; no child/SDK/network/receiver/native/Git','Source was changing; executed snapshot pinned and later source not approved','No actual POSIX scheduling, process launch or process-group behavior qualified'],
 'closing_owner_source':digest(ROOT/'src/ashlar_host/otel.py')}
BASE.with_suffix('.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')
print(json.dumps({'receipt':str(BASE.with_suffix('.json')),'sha256':digest(BASE.with_suffix('.json'))['sha256'],'checks':len(checks),'source':inputs['executed_snapshot']['sha256']}))
