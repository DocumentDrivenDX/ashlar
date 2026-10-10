"""Independent spanless source controls: real selected SDK, inert byte sink only."""
import ast
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import asdict
from hashlib import sha256
from importlib import metadata
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

R = Path('/Users/erik/Projects/ashlar')
BASE = Path('/private/tmp/astra-spanless-worker-review-20261010-d')
EXPECTED = {
    R/'src/ashlar_host/_otel_worker.py': '143e6baf0d47c0886c69269a589883af4b4b100afa16fd161710eaa885a25a57',
    R/'tests/test_otel_worker.py': '596af3ff74a532cfe686440a6e8da186c98573a0667aca5e311ce15f9a35ae37',
    Path('/private/tmp/ashlar-spanless-source-controls-20261010-d.json'): '49e262c4d438b37041591c302bdc4868597f4b86c7ff631e647d97f9f0cdd10c',
}
def pin(path):
    raw = Path(path).read_bytes()
    return {'path': str(path), 'bytes': len(raw), 'sha256': sha256(raw).hexdigest()}

opening = [pin(p) for p in EXPECTED]
assert all(p['sha256'] == EXPECTED[Path(p['path'])] for p in opening)
governing = pin(R/'docs/helix/02-design/contracts/CONTRACT-006-diagnostics.md')
predecessor_worker=Path('/private/tmp/astra-spanless-source-snapshot-20261010-c/src/ashlar_host/_otel_worker.py')
predecessor_tests=Path('/private/tmp/astra-spanless-source-snapshot-20261010-c/tests/test_otel_worker.py')
assert pin(predecessor_worker)['sha256']=='ec6f21728cdf4a732a0b8fa65b79f18f93467836d8ca3fa4e51b10b1e5103181'
assert pin(predecessor_tests)['sha256']=='a3581467df49e9e1e86e8c9551296e4b53d8e8f17e80ec44db0d25ed5b348bfa'
prior_line='        finish(primary, (() if actual is not None else (lambda: detach(token),)))\n'
fixed_line='        finish(primary, (() if actual is not None else (lambda: detach(token),)), diagnostic_only=True)\n'
assert predecessor_worker.read_text().count(prior_line)==1
assert predecessor_worker.read_text().replace(prior_line,fixed_line)==(R/'src/ashlar_host/_otel_worker.py').read_text()
snapshot_pins=[]
for source,suffix in ((R/'src/ashlar_host/_otel_worker.py','.worker.py'),(R/'tests/test_otel_worker.py','.tests.py')):
    target=Path(str(BASE)+suffix)
    if target.exists():assert target.read_bytes()==source.read_bytes()
    else:target.write_bytes(source.read_bytes())
    snapshot_pins.append(pin(target))

# Establish that the changed source surface really is the requested projection delta.
def definitions(raw):
    result = {}
    def visit(nodes, prefix=''):
        for node in nodes:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                result[prefix + node.name] = ast.dump(node, include_attributes=False)
            elif isinstance(node, ast.ClassDef):
                visit(node.body, prefix + node.name + '.')
    visit(ast.parse(raw).body)
    return result

base_worker = subprocess.check_output(['git','show','HEAD:src/ashlar_host/_otel_worker.py'], cwd=R)
base_tests = subprocess.check_output(['git','show','HEAD:tests/test_otel_worker.py'], cwd=R)
old = definitions(base_worker); new = definitions((R/'src/ashlar_host/_otel_worker.py').read_bytes())
changed = sorted(k for k in old if new.get(k) != old[k])
assert changed == ['SDKWorker.close', 'SDKWorker.context', 'SDKWorker.emit']
assert set(old) == set(new)
old_tests = definitions(base_tests); new_tests = definitions((R/'tests/test_otel_worker.py').read_bytes())
assert sorted(k for k in old_tests if new_tests.get(k) != old_tests[k]) == ['SDKWorkerTests.context', 'SDKWorkerTests.emit']
assert len(set(new_tests) - set(old_tests)) == 7
c_tests=definitions(predecessor_tests.read_bytes())
assert all(new_tests[k]==v for k,v in c_tests.items())
assert sorted(set(new_tests)-set(c_tests))==[
    'SDKWorkerTests.test_spanless_constructor_first_cancellation_survives_later_cancellation',
    'SDKWorkerTests.test_spanless_detach_first_cancellation_beats_ordinary_constructor_failure']

sys.path[:0] = [str(R/'src'), str(R/'tests')]
from ashlar_host.config import DiagnosticsLimits
from ashlar_host._otel_worker import SDKWorker, Settings, WorkerError
from ashlar_host.diagnostics import CATALOG, OPERATIONS, OUTCOMES, SCOPE
from opentelemetry.context import Context, attach, detach, set_value, get_current
from opentelemetry._logs import LogRecord as ActualLogRecord
from opentelemetry.trace import (NonRecordingSpan, SpanContext, TraceFlags,
    set_span_in_context, get_current_span)
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
import test_otel_worker

checks = []
failed_checks = []
def check(condition, label):
    checks.append(label)
    if not condition: failed_checks.append(label)

def wire():
    return {'endpoint':'http://127.0.0.1:4318/','headers':[],
        'tls':'loopback-test','ca_file':None,'environment':'test',
        'service_version':'0.1.0.dev0',
        'limits':asdict(DiagnosticsLimits(4096,128,524288,4096,16384,10000,64,100,1000,60))}

def request(identity, phase='started', mode=True, operation='publication'):
    value = {'op':'context','attempt_id':identity,'operation':operation,
             'event_name':'ashlar.operation.'+phase,'parent':None,'retry_link':None}
    if phase == 'started': value['create_span'] = mode
    return value

def event(worker, identity, sequence, phase, trace=None, operation='publication', outcome='succeeded'):
    attrs = {'ashlar.run.id':'a'*32,'ashlar.attempt.id':identity,'ashlar.emitter.id':'host',
             'ashlar.sequence':sequence,'ashlar.operation':operation}
    severity = 9
    if phase == 'phase': attrs.update({'ashlar.phase':'guard','ashlar.phase.state':'completed'})
    if phase == 'finished':
        attrs.update({'ashlar.outcome':outcome,'ashlar.cleanup_failed':False})
        if outcome != 'succeeded':
            attrs['ashlar.error.category'] = 'internal'
            severity = 17 if outcome == 'failed' else 13
    value = {'schema_version':'ashlar.diagnostic.event/0.1',
        'timestamp_unix_nano':str(1800000000000000000+sequence),
        'observed_timestamp_unix_nano':str(1800000000000000000+sequence),
        'severity_number':severity,'severity_text':{9:'INFO',13:'WARN',17:'ERROR'}[severity],
        'event_name':'ashlar.operation.'+phase,'body':CATALOG['ashlar.operation.'+phase],
        'resource':worker.resource,'scope':SCOPE,'attributes':attrs}
    if trace is not None: value['trace'] = trace
    return json.dumps(value,separators=(',',':'))+'\n'

def attrs(values):
    return {a.key:getattr(a.value,a.value.WhichOneof('value')) for a in values}

workers = []
sent_by_worker = []
def worker():
    sent = []
    def inert(signal, raw, deadline):
        sent.append((signal,raw))
        return 'success'
    w = SDKWorker(Settings.from_wire(wire()),inert)
    workers.append(w); sent_by_worker.append(sent)
    return w, sent

def state(w):
    # Keep native span object identity while copying all mutable attempt facts.
    return ({k:dict(v) for k,v in w.attempts.items()},w.run_id,w.last_sequence,
        dict(w.metric_state),{k:(v.submitted,v.dropped,len(v.units)) for k,v in w.queues.items()})

def refused(w, value, label):
    before = state(w)
    try: w.context(value)
    except WorkerError as exc: check(str(exc)=='diagnostics-configuration', label+' fixed refusal')
    else: raise AssertionError(label+' accepted')
    check(state(w)==before,label+' no admitted effects')

network_attempts = []
def blocked(*args, **kwargs):
    network_attempts.append(True)
    raise AssertionError('Network use forbidden by independent spanless control')

actual_version = metadata.version
with ExitStack() as stack:
    stack.enter_context(patch.dict(os.environ,{},clear=True))
    for name in ('socket','create_connection','getaddrinfo'):
        stack.enter_context(patch.object(socket,name,side_effect=blocked))
    stack.enter_context(patch('ashlar_host._otel_worker.metadata.version',
        side_effect=lambda name:'0.1.0.dev0' if name=='ashlar-graph-toolkit' else actual_version(name)))
    try:
        w,sent = worker()
        identity = 'b'*32
        for value in (None,0,1,-1,0.0,1.0,'false','true',[],{}):
            refused(w,request(identity,mode=value),'exact bool '+repr(value))
        missing = request(identity); del missing['create_span']
        refused(w,missing,'started missing selection')
        parent = {'trace_id':'c'*32,'span_id':'d'*16,'trace_flags':255,'is_remote':True}
        for key in ('parent','retry_link'):
            value = request(identity,mode=False); value[key] = parent
            refused(w,value,'spanless with '+key)
        with patch.object(w.tracer,'start_span',side_effect=AssertionError('spanless called start_span')):
            value = request(identity,mode=False)
            check(w.context(value) is None,'spanless returns no context')
            value['create_span'] = True
            check(w.attempts[identity]['span'] is None,'caller mutation cannot change selection')
        for mode in (False,True): refused(w,request(identity,mode=mode),'duplicate started '+str(mode))
        for phase in ('phase','finished'):
            for mode in (False,True):
                value = request(identity,phase); value['create_span']=mode
                refused(w,value,phase+' cannot rebind '+str(mode))
        before = state(w)
        try: w.emit(event(w,identity,1,'started',{k:parent[k] for k in ('trace_id','span_id','trace_flags')}))
        except WorkerError: pass
        else: raise AssertionError('spanless trace accepted')
        check(state(w)==before,'spanless supplied trace refused before admission')
        check(sent==[],'refusals and context produce no transport')
        w.close(time.monotonic()+.8)

        # Exercise real SDK log construction under an intentionally contaminating
        # ambient Context. All 4 operations x 5 outcomes get one attempt per mode.
        w,sent = worker()
        ambient = SpanContext(int('e'*32,16),int('f'*16,16),False,TraceFlags(255))
        token = attach(set_span_in_context(NonRecordingSpan(ambient),set_value('review-sentinel','ambient',Context())))
        emitted_records = []
        original_emit = w.logger.emit
        def capture(record):
            emitted_records.append(record)
            return original_emit(record)
        sequence = 0; contexts = {}; attempts = []
        try:
            with patch.object(w.logger,'emit',side_effect=capture):
                for i,(op,outcome) in enumerate((op,outcome) for op in OPERATIONS for outcome in OUTCOMES):
                    for mode in (False,True):
                        identity = format(100+i*2+int(mode),'032x')
                        trace = w.context(request(identity,mode=mode,operation=op))
                        contexts[identity] = trace
                        attempts.append((identity,op,outcome,mode))
                        for phase in ('started','phase','finished'):
                            if phase != 'started':
                                check(w.context(request(identity,phase,operation=op))==trace,
                                      'immutable context '+identity+' '+phase)
                            sequence += 1
                            w.emit(event(w,identity,sequence,phase,trace,op,outcome))
                check(get_current_span().get_span_context() is ambient,'ambient caller context retained')
        finally: detach(token)
        check(len(emitted_records)==120,'real SDK 120 emitted log records')
        for record in emitted_records:
            identity = record.attributes['ashlar.attempt.id']
            trace = contexts[identity]
            if trace is None:
                check(dict(record.context)=={},'explicit empty SDK Context '+identity+' '+record.event_name)
                check((record.trace_id,record.span_id,int(record.trace_flags))==(0,0,0),
                      'zero SDK trace fields '+identity+' '+record.event_name)
            else:
                check(record.trace_id==int(trace['trace_id'],16) and record.span_id==int(trace['span_id'],16),
                      'traced SDK correspondence '+identity+' '+record.event_name)
                check(record.trace_id!=ambient.trace_id,'traced root excludes ambient parent '+identity+' '+record.event_name)
        loss = w.close(time.monotonic()+.8)
        check(loss['logs']=={'submitted':120,'handed_off':120,'dropped':0,'unknown':False,'flush':'complete'},'log units exact')
        check(loss['spans']=={'submitted':20,'handed_off':20,'dropped':0,'unknown':False,'flush':'complete'},'only 20 traced spans admitted')
        check(loss['metrics']=={'submitted':20,'handed_off':20,'dropped':0,'unknown':False,'flush':'complete'},'20 metric series units exact')
        logs=[]; spans=[]; points=[]
        for signal,raw in sent:
            if signal=='logs':
                msg=ExportLogsServiceRequest.FromString(raw)
                logs.extend(msg.resource_logs[0].scope_logs[0].log_records)
            elif signal=='spans':
                msg=ExportTraceServiceRequest.FromString(raw)
                spans.extend(msg.resource_spans[0].scope_spans[0].spans)
            else:
                msg=ExportMetricsServiceRequest.FromString(raw)
                metric=msg.resource_metrics[0].scope_metrics[0].metrics[0]
                check(metric.name=='ashlar.operation.completed' and metric.unit=='{operation}', 'metric identity unchanged')
                check(metric.sum.is_monotonic and metric.sum.aggregation_temporality==2,'metric cumulative monotonic unchanged')
                points.extend(metric.sum.data_points)
        for log,record in zip(logs,emitted_records):
            identity=attrs(log.attributes)['ashlar.attempt.id']; trace=contexts[identity]
            check(attrs(log.attributes)==dict(record.attributes),'OTLP attributes preserved '+identity+' '+log.event_name)
            check(log.body.string_value==record.body and log.event_name==record.event_name
                  and log.severity_number==record.severity_number.value and log.severity_text==record.severity_text,
                  'OTLP log fields preserved '+identity+' '+log.event_name)
            if trace is None:
                check(log.trace_id==b'' and log.span_id==b'' and log.flags==0,
                      'OTLP spanless trace absent '+identity+' '+log.event_name)
            else:
                check(log.trace_id.hex()==trace['trace_id'] and log.span_id.hex()==trace['span_id'] and log.flags==trace['trace_flags'],
                      'OTLP traced correspondence '+identity+' '+log.event_name)
        span_attempts={attrs(s.attributes)['ashlar.attempt.id'] for s in spans}
        check(span_attempts=={identity for identity,op,outcome,mode in attempts if mode},'spanless generates no span payload')
        actual_points={tuple(sorted(attrs(p.attributes).items())):p.as_int for p in points}
        expected_points={tuple(sorted({'ashlar.operation':op,'ashlar.outcome':outcome}.items())):2 for op in OPERATIONS for outcome in OUTCOMES}
        check(actual_points==expected_points,'all 20 operation/outcome series count both modes exactly')
        check(all(not p.exemplars for p in points),'all counter exemplars remain absent')
        first_record=emitted_records[0];first_log=logs[0]
        counterexample={'attemptId':first_record.attributes['ashlar.attempt.id'],
            'selectedCreateSpan':False,'eventHadTrace':False,
            'suppliedAmbientTraceId':format(ambient.trace_id,'032x'),
            'suppliedAmbientSpanId':format(ambient.span_id,'016x'),
            'suppliedAmbientFlags':int(ambient.trace_flags),
            'actualSDKTraceId':format(first_record.trace_id,'032x'),
            'actualSDKSpanId':format(first_record.span_id,'016x'),
            'actualSDKFlags':int(first_record.trace_flags),
            'actualOTLPTraceId':first_log.trace_id.hex(),
            'actualOTLPSpanId':first_log.span_id.hex(),'actualOTLPFlags':first_log.flags,
            'spanlessAttemptSpan':None}

        # A traced attempt cannot have correlation stripped to mimic spanless mode.
        w,sent=worker();identity='9'*32;trace=w.context(request(identity))
        before=state(w)
        try:w.emit(event(w,identity,1,'started'))
        except WorkerError:pass
        else:raise AssertionError('trace stripping accepted')
        check(state(w)==before,'traced omission rejected before admission')
        w.emit(event(w,identity,1,'started',trace))
        for mode in (False,True):
            value=request(identity,'finished');value['create_span']=mode
            refused(w,value,'existing span cannot reselect '+str(mode))
        w.context(request(identity,'finished'));w.emit(event(w,identity,2,'finished',trace))
        w.close(time.monotonic()+.8)

        for mode,emit_started in ((False,False),(False,True),(True,False),(True,True)):
            w,sent=worker();identity='7'*32;trace=w.context(request(identity,mode=mode))
            if emit_started:w.emit(event(w,identity,1,'started',trace))
            loss=w.close(time.monotonic()+.8)
            check(loss['spans']['submitted']==0,'unfinished has no submitted span '+str((mode,emit_started)))
            check(loss['spans']['unknown'] is mode,'unfinished uncertainty corresponds to actual span '+str((mode,emit_started)))
            check(loss['metrics']['submitted']==0 and w.metric_state=={},'unfinished has no invented completion '+str((mode,emit_started)))

        # Construction is diagnostic-only, so an ordinary construction failure
        # must not conceal the first non-Exception cancellation during detach.
        restoration_cases=[]
        kinds=(None,OSError,KeyboardInterrupt,SystemExit,GeneratorExit)
        for constructor_kind in kinds:
            for cleanup_kind in kinds:
                w,sent=worker();identity='6'*32;w.context(request(identity,mode=False))
                constructor_error=constructor_kind('construction fixture') if constructor_kind else None
                cleanup_error=cleanup_kind('detach fixture') if cleanup_kind else None
                label='constructor '+str(constructor_kind)+' detach '+str(cleanup_kind)
                expected=constructor_error or cleanup_error
                if isinstance(constructor_error,Exception) and cleanup_error is not None and not isinstance(cleanup_error,Exception):
                    expected=cleanup_error
                ambient_context=set_span_in_context(NonRecordingSpan(ambient),set_value('review-sentinel','restoration',Context()))
                token=attach(ambient_context);actions=[]
                def construct(*args,**kwargs):
                    actions.append('construct')
                    check(dict(get_current())=={},'empty context during construction '+label)
                    if constructor_error is not None:raise constructor_error
                    return ActualLogRecord(*args,**kwargs)
                def restore(value):
                    actions.append('detach');detach(value)
                    if cleanup_error is not None:raise cleanup_error
                original_emit=w.logger.emit
                def emit_after_restore(record):
                    actions.append('emit')
                    check(get_current() is ambient_context,'restoration precedes emit '+label)
                    return original_emit(record)
                caught=None
                try:
                    with patch('opentelemetry._logs.LogRecord',side_effect=construct),patch('opentelemetry.context.detach',side_effect=restore),patch.object(w.logger,'emit',side_effect=emit_after_restore):
                        try:w.emit(event(w,identity,1,'started'))
                        except BaseException as exc:caught=exc
                    check(get_current() is ambient_context,'ambient identity restored '+label)
                    check(caught is expected,'first non-Exception cancellation preserved '+label)
                    check(actions==(['construct','detach','emit'] if expected is None else ['construct','detach']),
                          'cleanup order and no emit after failure '+label)
                    check(not sent,'restoration faults trigger no transport '+label)
                    if constructor_error is not None and cleanup_error is not None:
                        check(getattr(caught,'cleanup_failed',False),'additional cleanup failure recorded '+label)
                    restoration_cases.append({'constructor':constructor_kind.__name__ if constructor_kind else None,
                        'detach':cleanup_kind.__name__ if cleanup_kind else None,
                        'expected':type(expected).__name__ if expected is not None else None,
                        'actual':type(caught).__name__ if caught is not None else None,
                        'identityPreserved':caught is expected,'contextRestored':get_current() is ambient_context,
                        'order':actions})
                finally:detach(token)
                w.close(time.monotonic()+.8)

        # Run the exact existing owner worker suite, with the same network tripwire.
        stream=io.StringIO()
        result=unittest.TextTestRunner(stream=stream,verbosity=2).run(unittest.defaultTestLoader.loadTestsFromModule(test_otel_worker))
        print(stream.getvalue(),end='')
        check(result.wasSuccessful() and not result.skipped,'all owner worker tests pass without skips')
        check(not network_attempts,'network tripwire remained unused')
    finally:
        for w in workers:
            if not w.closed:w.close(time.monotonic()+.8)

closing=[pin(p) for p in EXPECTED]
assert closing==opening
assert pin(governing['path'])==governing
assert all(pin(p['path'])==p for p in snapshot_pins)
def control_block(path):
    return Path(path).read_text().split('with ExitStack() as stack:\n',1)[1].split('\nclosing=[pin(p) for p in EXPECTED]',1)[0]
assert control_block(__file__)==control_block('/private/tmp/astra-spanless-worker-review-20261010-c.py')
assert not failed_checks, failed_checks
receipt={
    'verdict':'changes-requested-source-only' if failed_checks else 'approved-source-only',
    'scope':'Independent narrow spanless worker source delta to HEAD; actual selected SDK constructors and encoders with inert byte transport. No receiver, socket, native work, installed application qualification, or provider exporter.',
    'sourcePins':opening[:2], 'ownerReceipt':opening[2], 'openingClosingEqual':True,
    'sourceSnapshots':snapshot_pins,
    'productionDeltaFromC':{'onlyChange':'diagnostic_only=True keyword on the new constructor/context restoration finish call','exactByteReplacementVerified':True,'predecessorWorker':pin(predecessor_worker),'predecessorTests':pin(predecessor_tests),'existingOwnerTestDefinitionsUnchanged':True,'twoAddedOwnerCancellationTests':True,'independentExecutedControlBlockByteIdenticalToC':True},
    'governing':governing,
    'baselineWorker':{'bytes':len(base_worker),'sha256':sha256(base_worker).hexdigest()},
    'delta':{'onlyChangedWorkerDefinitions':changed,'existingOwnerTestsUnchanged':True,'ownerHelperChanges':['context','emit'],'newOwnerTests':7},
    'independentCheckCount':len(checks),'failedCheckCount':len(failed_checks),
    'failedChecks':failed_checks,'independentChecks':checks,
    'ownerTests':{'run':result.testsRun,'skipped':len(result.skipped),'failures':len(result.failures),'errors':len(result.errors)},
    'selectedRuntime':{'python':sys.version,'pythonExecutable':sys.executable,'sdk':actual_version('opentelemetry-sdk'),'api':actual_version('opentelemetry-api'),'proto':actual_version('opentelemetry-proto'),'applicationVersion':'0.1.0.dev0 explicit metadata fixture'},
    'finiteProjection':{'attempts':40,'logs':120,'spans':20,'metricSeries':20,'countPerSeries':2,'ambientContextLeak':counterexample['actualOTLPTraceId']!=''},
    'ambientContextCounterexample':counterexample,
    'restorationCases':restoration_cases,
    'sdkConstructorSource':pin('/private/tmp/ashlar-otel-sdk-env-20261010-a/lib/python3.11/site-packages/opentelemetry/_logs/_internal/__init__.py'),
    'resolved':['SW-001: real SDK LogRecord construction and OTLP encoding exclude ambient context in all 60 spanless logs while retaining all 60 traced logs and caller context.','SW-002: all 25 constructor/detach combinations preserve expected primary or first non-Exception cancellation identity, mark additional cleanup failures, restore ambient context, and refuse emission after construction/restoration failure.'],
    'findings':[{'id':'SW-002','sourceLines':[562],
        'issue':'New finish(primary, detach) call uses owner-primary precedence for a diagnostic-only constructor error. Ordinary LogRecord construction failure then a first non-Exception cancellation during detach suppresses that cancellation.',
        'evidence':'Three exact callsite controls: OSError construction followed by detach KeyboardInterrupt, SystemExit, or GeneratorExit returns the original ordinary OSError. Remaining 22 restoration combinations preserve expected identity and all 25 restore caller context before return.',
        'suggestion':'Pass diagnostic_only=True at the new finish callsite; preserve preexisting non-Exception constructor identity and restore before logger emission.'}] if failed_checks else [],
    'predecessors':[pin('/private/tmp/astra-spanless-worker-review-20261010-a.json'),pin('/private/tmp/astra-spanless-worker-review-20261010-c.json')],
    'limitations':['No facade ContextVar or RPC review is implied; parent owns that boundary.','Actual installed spanless receiver and local-capture correspondence remains a separate gate.','No broader transport, dependency, native workflow, performance, or full C006 qualification.','SDK/provider object memory and abrupt process loss are outside this bounded component evidence.'],
    'script':pin(__file__),
}
out=BASE.with_suffix('.json');out.write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(pin(out)))
