"""Read-only frozen-worker review using actual SDK and inert send ports only."""
import contextlib
import ast
from dataclasses import asdict
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from unittest.mock import patch

BASE = Path('/private/tmp/astra-otel-worker-projection-review-20261010-c')
FREEZE = Path('/private/tmp/ashlar-otel-worker-source-freeze-20261010-c')
ROOT = Path('/Users/erik/Projects/ashlar')
INPUTS = {
    'manifest': FREEZE / 'manifest.json',
    'worker': FREEZE / 'src/ashlar_host/_otel_worker.py',
    'author_tests': FREEZE / 'tests/test_otel_worker.py',
    'config': ROOT / 'src/ashlar_host/config.py',
    'diagnostics': ROOT / 'src/ashlar_host/diagnostics.py',
    'contract': ROOT / 'docs/helix/02-design/contracts/CONTRACT-006-diagnostics.md',
    'adr': ROOT / 'docs/helix/02-design/adr/ADR-001-delta-canonical-and-serving-layout.md',
    'successor_manifest': Path('/private/tmp/ashlar-otel-worker-source-freeze-20261010-d/manifest.json'),
    'successor_worker': Path('/private/tmp/ashlar-otel-worker-source-freeze-20261010-d/src/ashlar_host/_otel_worker.py'),
}
def digest(path):
    raw = path.read_bytes()
    assert len(raw) <= 200000
    return {'path': str(path), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}
opening = {name: digest(path) for name, path in INPUTS.items()}
assert opening['manifest']['sha256'] == '0b0248c2d04a01a2e763de170bef4446ef03650f20db1b46482ad9ae0b209dcd'
assert opening['worker']['sha256'] == 'c37de88b4b0e3f3505d5fd7808cfb6fad2c64685f253a390876240322347ffd3'
assert opening['successor_manifest']['sha256'] == '26823f128a63d31c1acd1f4e472f552625f911332c4508c5e54c25a8e282b9f2'
assert opening['successor_worker']['sha256'] == '41c57b1bf3f07e1dd850521e1da3039f880ee597641f734512042ad06a20b0b1'
sys.path.insert(0, str(ROOT / 'src'))
spec = importlib.util.spec_from_file_location('ashlar_host._otel_worker', INPUTS['worker'])
worker_module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = worker_module
spec.loader.exec_module(worker_module)
from ashlar_host.config import DiagnosticsLimits
from ashlar_host.diagnostics import decode_event
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest

observations = []
checks = []
workers = []
sequence = 0
def check(name, condition):
    assert condition, name
    checks.append(name)
def prohibited(*args, **kwargs):
    raise AssertionError('network/process port forbidden in independent review')
def settings():
    return worker_module.Settings.from_wire({
        'endpoint': 'http://127.0.0.1:4318/', 'headers': [['authorization', 'review-private-sentinel']],
        'tls': 'loopback-test', 'ca_file': None, 'environment': 'test', 'service_version': '0.1.0.dev0',
        'limits': asdict(DiagnosticsLimits(4096, 128, 524288, 4096, 16384, 10000, 64, 100, 1000, 60))})
def create():
    sent = []
    def send(signal, raw, deadline):
        check('request bounded', len(raw) <= 1048576)
        sent.append((signal, raw))
        return 'success'
    worker = worker_module.SDKWorker(settings(), send)
    workers.append(worker)
    return worker, sent
def context(worker, identity, kind='started', operation='publication', parent=None, link=None):
    return worker.context({'op': 'context', 'attempt_id': identity, 'operation': operation,
        'event_name': 'ashlar.operation.' + kind, 'parent': parent, 'retry_link': link})
def event(worker, identity, trace, kind='started', outcome='succeeded', operation='publication', source=True):
    global sequence
    sequence += 1
    attrs = {'ashlar.run.id': 'a' * 32, 'ashlar.attempt.id': identity, 'ashlar.emitter.id': 'host',
             'ashlar.sequence': sequence, 'ashlar.operation': operation}
    if kind == 'finished':
        attrs.update({'ashlar.outcome': outcome, 'ashlar.cleanup_failed': outcome != 'succeeded'})
        if outcome != 'succeeded': attrs['ashlar.error.category'] = 'internal'
    severity = 17 if kind == 'finished' and outcome == 'failed' else 13 if kind == 'finished' and outcome != 'succeeded' else 9
    value = {'schema_version': 'ashlar.diagnostic.event/0.1', 'observed_timestamp_unix_nano': str(time.time_ns()),
        'severity_number': severity, 'severity_text': {9:'INFO',13:'WARN',17:'ERROR'}[severity],
        'event_name': 'ashlar.operation.' + kind, 'body': {'started':'Operation started', 'finished':'Operation finished'}[kind],
        'resource': dict(worker.resource), 'scope': {'name':'ashlar.host.diagnostics','version':'0.1.0'}, 'attributes': attrs}
    if source: value['timestamp_unix_nano'] = value['observed_timestamp_unix_nano']
    if trace is not None: value['trace'] = dict(trace)
    return value
def raw(value): return (json.dumps(value, separators=(',', ':')) + '\n').encode()
def emit(worker, value): worker.emit(raw(value).decode())
def attrs(values):
    check('no duplicate attribute keys', len(values) == len({v.key for v in values}))
    return {v.key: (v.value.WhichOneof('value'), getattr(v.value, v.value.WhichOneof('value'))) for v in values}
def expected_attrs(values):
    return {k: ({str:'string_value',int:'int_value',bool:'bool_value'}[type(v)],v) for k,v in values.items()}
def decode(sent, signal):
    cls = {'logs':ExportLogsServiceRequest,'spans':ExportTraceServiceRequest,'metrics':ExportMetricsServiceRequest}[signal]
    result = []
    for name, payload in sent:
        check('private settings absent in payload', b'review-private-sentinel' not in payload)
        if name == signal:
            pb = cls.FromString(payload)
            before = pb.SerializeToString(deterministic=True); pb.DiscardUnknownFields()
            check('no unknown protobuf fields', before == pb.SerializeToString(deterministic=True))
            result.append(pb)
    return result

captured_out, captured_err = io.StringIO(), io.StringIO()
actual_version = worker_module.metadata.version
with contextlib.ExitStack() as stack:
    stack.enter_context(patch.dict(os.environ, {}, clear=True))
    stack.enter_context(patch.object(worker_module.metadata, 'version', side_effect=lambda name: '0.1.0.dev0' if name == 'ashlar-graph-toolkit' else actual_version(name)))
    for owner, name in ((socket.socket,'connect'),(socket.socket,'connect_ex'),(socket,'create_connection'),(socket,'getaddrinfo'),(subprocess,'Popen'),(os,'system')):
        stack.enter_context(patch.object(owner, name, side_effect=prohibited))
    stack.enter_context(contextlib.redirect_stdout(captured_out)); stack.enter_context(contextlib.redirect_stderr(captured_err))
    try:
        w, sent = create(); identity = 'b' * 32
        trace = context(w, identity)
        start = event(w, identity, trace, source=False)
        emit(w, start)
        context(w, identity, 'finished')
        end = event(w, identity, trace, 'finished', 'failed')
        emit(w, end)
        loss = w.close(time.monotonic() + .8)
        logs = decode(sent, 'logs'); spans = decode(sent, 'spans'); metrics = decode(sent, 'metrics')
        record = logs[0].resource_logs[0].scope_logs[0].log_records[0]
        span = spans[0].resource_spans[0].scope_spans[0].spans[0]
        span_attrs = attrs(span.attributes); log_attrs = attrs(record.attributes)
        observations.append({'case':'exact_mapping', 'resource_schema_urls': [logs[0].resource_logs[0].schema_url, spans[0].resource_spans[0].schema_url, metrics[0].resource_metrics[0].schema_url],
          'absent_source_timestamp_omitted': record.time_unix_nano == 0,
          'log_schema_attribute_present':'ashlar.diagnostic.schema_version' in log_attrs,
          'span_name':span.name, 'span_attributes':span_attrs, 'log_flags':record.flags, 'actual_flags':trace['trace_flags'], 'span_flags':span.flags, 'loss':loss})
        check('corrected Resource schema URLs exact', all(x == 'https://opentelemetry.io/schemas/1.44.0' for x in observations[-1]['resource_schema_urls']))
        check('corrected absent source timestamp omitted', observations[-1]['absent_source_timestamp_omitted'])
        check('corrected log attribute names values primitive types exact', log_attrs == expected_attrs({**start['attributes'],'ashlar.diagnostic.schema_version':start['schema_version']}))
        check('corrected span catalog name exact', span.name == 'ashlar.publication')
        check('corrected failure span attributes exact', span_attrs == expected_attrs({'ashlar.run.id':'a'*32,'ashlar.attempt.id':identity,'ashlar.operation':'publication','ashlar.outcome':'failed','ashlar.cleanup_failed':True,'ashlar.error.category':'internal'}))
        for pb,field,scope_field in ((logs[0],'resource_logs','scope_logs'),(spans[0],'resource_spans','scope_spans'),(metrics[0],'resource_metrics','scope_metrics')):
            container=getattr(pb,field)[0];scope=getattr(container,scope_field)[0]
            check('six Resource attributes exact values and types',attrs(container.resource.attributes)==expected_attrs(w.resource) and container.resource.dropped_attributes_count==0)
            check('instrumentation scope exact no extra attributes',scope.scope.name=='ashlar.host.diagnostics' and scope.scope.version=='0.1.0' and not scope.scope.attributes and scope.schema_url=='' and scope.scope.dropped_attributes_count==0)
        for pb,value in zip(logs,(start,end)):
            log_record=pb.resource_logs[0].scope_logs[0].log_records[0]
            check('log exact catalog severity event body observed time',log_record.event_name==value['event_name'] and log_record.severity_text==value['severity_text'] and log_record.severity_number==value['severity_number'] and log_record.body.WhichOneof('value')=='string_value' and log_record.body.string_value==value['body'] and log_record.observed_time_unix_nano==int(value['observed_timestamp_unix_nano']))
            check('log optional source timestamp exact',log_record.time_unix_nano==int(value.get('timestamp_unix_nano',0)))
            check('log all admitted attrs plus schema marker exact',attrs(log_record.attributes)==expected_attrs({**value['attributes'],'ashlar.diagnostic.schema_version':value['schema_version']}))
            check('log no dropped attributes',log_record.dropped_attributes_count==0)
        check('span Internal root exact context no extras',span.kind==1 and not span.parent_span_id and not span.links and not span.trace_state and span.trace_id==bytes.fromhex(trace['trace_id']) and span.span_id==bytes.fromhex(trace['span_id']) and span.dropped_attributes_count==span.dropped_events_count==span.dropped_links_count==0)
        check('actual log flags exact', record.flags == trace['trace_flags'])
        check('corrected native span low8 flags', span.flags & 255 == trace['trace_flags'])
        check('span failure status no description/events', span.status.code == 2 and span.status.message == '' and not span.events)
        check('span interval positive', 0 < span.start_time_unix_nano <= span.end_time_unix_nano)
        check('exact queue close accounting', [(x['submitted'],x['handed_off'],x['dropped'],x['unknown']) for x in loss.values()] == [(2,2,0,False),(1,1,0,False),(1,1,0,False)])

        w, sent = create(); identity = 'c' * 32; trace = context(w, identity)
        untraced = event(w, identity, None)
        check('untraced event admitted by C006 validator', decode_event(raw(untraced)) == untraced)
        rejected = False
        try: emit(w, untraced)
        except worker_module.WorkerError: rejected = True
        check('reproduced no untraced worker path', rejected and w.queues['logs'].submitted == 0)
        observations.append({'case':'untraced_event', 'contract_valid':True, 'worker_rejected':rejected})
        valid = event(w, identity, trace)
        for location in ('attribute','body','resource'):
            bad = json.loads(json.dumps(valid))
            if location == 'attribute': bad['attributes']['unknown'] = 'review-private-sentinel'
            if location == 'body': bad['body'] = 'review-private-sentinel'
            if location == 'resource': bad['resource']['service.version'] = 'review-private-sentinel'
            try: emit(w, bad)
            except (worker_module.WorkerError, ValueError): pass
            else: raise AssertionError('private unadmitted value accepted')
        check('privacy rejection before SDK admission', w.queues['logs'].submitted == 0)
        emit(w, valid); context(w, identity, 'finished'); emit(w,event(w,identity,trace,'finished'))
        w.close(time.monotonic()+.8); decode(sent,'logs')

        w, sent = create()
        for index, flag in enumerate((0,1,2,3,254,255),1):
            identity = '%032x' % index
            incoming = {'trace_id':'d'*32,'span_id':'e'*16,'trace_flags':flag,'is_remote':bool(index%2)}
            trace = context(w,identity,link=incoming); emit(w,event(w,identity,trace))
            context(w,identity,'finished'); emit(w,event(w,identity,trace,'finished'))
        w.close(time.monotonic()+.8)
        for index,pb in enumerate(decode(sent,'spans'),1):
            span = pb.resource_spans[0].scope_spans[0].spans[0]; link = span.links[0]
            check('retry root no parent', not span.parent_span_id)
            check('full link flags preserved', link.flags & 255 == (0,1,2,3,254,255)[index-1])
            check('link remote bits preserved', bool(link.flags & 512) == bool(index%2) and bool(link.flags & 256))
            check('links contain no attributes/tracestate', not link.attributes and not link.trace_state)
        observations.append({'case':'retry_flags', 'tested':[0,1,2,3,254,255], 'passed':True})

        w, sent = create()
        for number in (1,2,3):
            identity = '%032x' % number; trace = context(w,identity); emit(w,event(w,identity,trace))
            context(w,identity,'finished'); emit(w,event(w,identity,trace,'finished'))
            if number == 1: w.reader.collect()
        w.reader.collect()
        check('metric occupied slot counts point drop', w.queues['metrics'].submitted == 2 and w.queues['metrics'].dropped == 1)
        w.queues['metrics'].drain('metrics',w.send,time.monotonic()+.5)
        loss = w.close(time.monotonic()+.8)
        values=[]
        for pb in decode(sent,'metrics'):
            metric=pb.resource_metrics[0].scope_metrics[0].metrics[0]
            point=metric.sum.data_points[0]; values.append(point.as_int)
            check('counter exact name unit monotonic cumulative', metric.name == 'ashlar.operation.completed' and metric.unit == '{operation}' and metric.sum.is_monotonic and metric.sum.aggregation_temporality == 2)
            check('integer point no exemplars/histogram', point.WhichOneof('value') == 'as_int' and not point.exemplars and metric.WhichOneof('data') == 'sum')
            check('metric closed dimension names/types', attrs(point.attributes) == {'ashlar.operation':('string_value','publication'),'ashlar.outcome':('string_value','succeeded')})
            check('metric finite ordered time', 0 < point.start_time_unix_nano <= point.time_unix_nano < 2**64)
        check('cumulative measurement not reset on drop', values == [1,3])
        check('metric point accounting separate from values', loss['metrics'] == {'submitted':3,'handed_off':2,'dropped':1,'unknown':False,'flush':'complete'})
        observations.append({'case':'metric_snapshot', 'values':values,'loss':loss['metrics']})

        w,sent=create();number=100
        for operation in ('publication','source-admission','held-read','ack-reconciliation'):
            for outcome in ('succeeded','refused','failed','cancelled','uncertain'):
                identity='%032x'%number;number+=1
                trace=context(w,identity,operation=operation);emit(w,event(w,identity,trace,operation=operation))
                context(w,identity,'finished',operation=operation);emit(w,event(w,identity,trace,'finished',outcome,operation))
        loss=w.close(time.monotonic()+.8)
        points=decode(sent,'metrics')[0].resource_metrics[0].scope_metrics[0].metrics[0].sum.data_points
        combinations={(dict((v.key,v.value.string_value) for v in p.attributes)['ashlar.operation'],dict((v.key,v.value.string_value) for v in p.attributes)['ashlar.outcome']) for p in points}
        check('all20 bounded metric series exact Cartesian catalog',len(points)==20 and combinations=={(op,outcome) for op in ('publication','source-admission','held-read','ack-reconciliation') for outcome in ('succeeded','refused','failed','cancelled','uncertain')})
        check('all20 points integer one no exemplars',all(p.WhichOneof('value')=='as_int' and p.as_int==1 and len(p.attributes)==2 and not p.exemplars for p in points))
        check('all20 logical datapoints accounting',loss['metrics']=={'submitted':20,'handed_off':20,'dropped':0,'unknown':False,'flush':'complete'})
        for pb in decode(sent,'spans'):
            span=pb.resource_spans[0].scope_spans[0].spans[0];values=attrs(span.attributes)
            operation=values['ashlar.operation'][1];outcome=values['ashlar.outcome'][1]
            expected={'ashlar.run.id','ashlar.attempt.id','ashlar.operation','ashlar.outcome','ashlar.cleanup_failed'}|({'ashlar.error.category'} if outcome!='succeeded' else set())
            check('all outcomes exact span names attributes status',span.name=='ashlar.'+operation and set(values)==expected and span.status.code==(0 if outcome=='succeeded' else 2) and span.status.message=='' and not span.events and values['ashlar.cleanup_failed'][0]=='bool_value')
        observations.append({'case':'all20_catalog_combinations','metric_points':len(points),'spans':20,'passed':True})

        q=worker_module.Queue(1,4); q.offer(b'ab')
        def inflight(signal,payload,deadline):
            check('reservation retained in send', q.bytes == 2 and len(q.units) == 1)
            q.offer(b'c'); return 'success'
        q.drain('logs',inflight,time.monotonic()+.1)
        check('inflight newest dropped exact units',q.loss()=={'submitted':2,'handed_off':1,'dropped':1,'unknown':False,'flush':'complete'})
    finally:
        for w in workers:
            if not w.closed: w.close(time.monotonic()+.5)
check('no runtime stdout/stderr disclosure', captured_out.getvalue() == '' and captured_err.getvalue() == '')
def projection_segments(path):
    text=path.read_text();tree=ast.parse(text);result={}
    for node in tree.body:
        if isinstance(node,(ast.ClassDef,ast.FunctionDef)) and node.name in ('Settings','Queue','parse'):
            result[node.name]=ast.get_source_segment(text,node)
        if isinstance(node,ast.ClassDef) and node.name=='SDKWorker':
            for method in node.body:
                if isinstance(method,ast.FunctionDef) and method.name in ('__init__','context','emit'):
                    result['SDKWorker.'+method.name]=ast.get_source_segment(text,method)
    return result
current_segments=projection_segments(INPUTS['worker']);successor_segments=projection_segments(INPUTS['successor_worker'])
check('six explicit projection/input/accounting sections selected',len(current_segments)==6)
for name,value in current_segments.items():check('C D exact section bytes '+name,value==successor_segments[name])
section_hashes={name:hashlib.sha256(value.encode()).hexdigest() for name,value in current_segments.items()}
closing = {name: digest(path) for name,path in INPUTS.items()}
check('inputs remained byte-identical', opening == closing)
log = json.dumps({'checks':checks,'observations':observations},indent=2,sort_keys=True)+'\n'
BASE.with_suffix('.log').write_text(log)
receipt = {'scope':'Frozen private worker projections only; actual pinned SDK, fake send ports, no receiver/network/native/process or source edits',
  'status':'WP-001 and WP-002 fixed for tested frozen C projection scope; no full worker approval', 'inputs':opening, 'script':digest(Path(__file__)), 'log':digest(BASE.with_suffix('.log')),
  'check_count':len(checks),'findings':[],
  'resolved':['WP-001 Resource schema URLs, schema marker, and optional source Timestamp','WP-002 catalog span names, run identity, cleanup_failed and nonsuccess category'],
  'successor_mapping_equivalence':{'executed':'C only','source_only_compared':'D','exact_section_byte_hashes':section_hashes,'qualification':'Six projection/input/accounting sections match exactly; D lifecycle changes reviewed separately by parent, no D full worker approval inferred here'},
  'capability_gaps':[{'id':'WP-G01','lines':[489,499],'issue':'Closed-schema valid untraced event cannot enter current worker emit path; all events require a worker-created span context.',
    'qualification':'Not established as a reachable parent-composition failure: selected operation context creates a span, and the parent facade does not exist in this freeze. This worker path alone cannot support the separately required no-active-context receiver control.',
    'follow_up':'During parent integration, review and test the no-valid-span behavior without fabricated context; do not infer complete C006 integration from current traced worker controls.'}],
  'positive_controls':['root span and log low8 flags','retry link full low8 and remote bits','monotonic cumulative integer counter, no exemplars or histogram','cumulative 1 then 3 with one occupied-slot drop','logical-unit queue accounting retaining inflight reservation','privacy rejection before admission','no duplicate attributes or unknown protobuf fields'],
  'limitations':['Installed application version mocked only as an author-test fixture; pinned SDK metadata checked by worker','No parent facade, process lifetime, transport, receiver or full host conformance qualification','Parent separately owns known C cleanup-cancellation defect WT002 and successor D review; this projection pass does not override it','No SDK object/process-memory bound claim']}
BASE.with_suffix('.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')
print(json.dumps({'receipt':str(BASE.with_suffix('.json')),'sha256':digest(BASE.with_suffix('.json'))['sha256'],'checks':len(checks),'findings':0,'capability_gaps':1}))
