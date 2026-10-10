"""Finite worker public SDK/OTLP correspondence with inert HTTP ports only."""
from dataclasses import asdict
from importlib import metadata
import io
import json
import os
from pathlib import Path
import struct
import sys
import time
import unittest
from unittest.mock import patch

from ashlar_host.config import DiagnosticsLimits
from ashlar_host._otel_worker import (SDKWorker, Settings, Queue, HTTPTransport,
    WorkerError, finish, parse, read_frame, write_frame, serve)

try:
    import opentelemetry.sdk
    HAS_SDK = sys.version_info[:2] == (3, 11)
except ImportError:
    HAS_SDK = False


def settings_wire():
    return {'endpoint':'http://127.0.0.1:4318/','headers':[['authorization','private-secret']],
            'tls':'loopback-test','ca_file':None,'environment':'test',
            'service_version':'0.1.0.dev0',
            'limits':asdict(DiagnosticsLimits(4096,128,524288,4096,16384,10000,64,100,1000,60))}


class WorkerProtocolTests(unittest.TestCase):
    def test_frames_closed_bounded_duplicate_numeric_depth(self):
        output=io.BytesIO();write_frame(output,{'ok':True,'value':None})
        self.assertEqual(read_frame(io.BytesIO(output.getvalue())),{'ok':True,'value':None})
        for raw in (b'{"op":1,"op":2}',b'{"n":'+b'1'*1000+b'}',b'{"n":NaN}',
                    b'{"n":'+b'['*13+b'0'+b']'*13+b'}'):
            with self.assertRaises(WorkerError):parse(raw)
        with self.assertRaises(WorkerError):read_frame(io.BytesIO(struct.pack('>I',65537)))
        with self.assertRaises(WorkerError):read_frame(io.BytesIO(b'\0\0\0\5{}'))

    def test_settings_explicit_policy_and_no_secret_repr(self):
        settings=Settings.from_wire(settings_wire())
        self.assertNotIn('private-secret',repr(settings))
        for key,value in (('endpoint','http://example.org:4318'),('environment','unknown'),
                          ('headers',[['Host','value']]),('limits',{'extra':1})):
            wire=settings_wire();wire[key]=value
            with self.subTest(key=key),self.assertRaises((WorkerError,ValueError)):Settings.from_wire(wire)
        wire=settings_wire();wire['limits']['max_queue_records']=True
        with self.assertRaises(ValueError):Settings.from_wire(wire)

    def test_byte_reservations_before_serialization_and_exact_units(self):
        q=Queue(1,4);calls=[]
        q.offer_sized(5,lambda:calls.append(True))
        self.assertEqual(calls,[]);self.assertEqual(q.loss()['dropped'],1)
        q.offer(b'ab',2);q.offer(b'c',3)
        self.assertEqual(q.submitted,6);self.assertEqual(q.dropped,4)
        seen=[]
        def send(signal,raw,deadline):
            self.assertEqual(q.bytes,2);self.assertEqual(len(q.units),1)
            seen.append(raw);return 'success'
        q.drain('logs',send,time.monotonic()+1)
        self.assertEqual(seen,[b'ab']);self.assertEqual(q.loss(),{'submitted':6,'handed_off':2,'dropped':4,'unknown':False,'flush':'complete'})
        q=Queue(2,20);q.offer(b'abc');q.drain('logs',lambda *a:'unknown',time.monotonic()+1)
        self.assertTrue(q.loss()['unknown']);self.assertIsNone(q.loss()['handed_off'])
        q=Queue(2,20);q.offer(b'abc');q.drain('logs',lambda *a:self.fail('late send'),time.monotonic()-1)
        self.assertEqual(q.loss()['flush'],'incomplete');self.assertEqual(q.loss()['dropped'],1)

    def test_protocol_refusal_and_primary_preserved_through_worker_cleanup(self):
        request=io.BytesIO();write_frame(request,{'op':'init','settings':settings_wire()})
        write_frame(request,{'op':'context','attempt_id':'b'*32,'operation':'publication',
            'event_name':'ashlar.operation.started','parent':None,'retry_link':None});request.seek(0)
        primary=KeyboardInterrupt();calls=[]
        class Provider:
            def shutdown(self,**kwargs):calls.append(True);raise OSError('private-payload')
        class Worker:
            def __init__(self,*args):self.closed=False;self.logs=self.traces=self.metrics=Provider()
            def context(self,value):raise primary
        output=io.BytesIO()
        with patch('ashlar_host._otel_worker.SDKWorker',Worker),patch('ashlar_host._otel_worker.logging.disable'):
            with self.assertRaises(BaseException)as caught:serve(request,output)
        self.assertIs(caught.exception,primary);self.assertTrue(primary.cleanup_failed);self.assertEqual(len(calls),3)
        self.assertNotIn(b'private-payload',output.getvalue())
        output=io.BytesIO();request=io.BytesIO();write_frame(request,{'op':'init','settings':{}});request.seek(0)
        with patch('ashlar_host._otel_worker.logging.disable'):serve(request,output)
        self.assertEqual(read_frame(io.BytesIO(output.getvalue())),{'ok':False,'value':'diagnostics-configuration'})

    def test_cleanup_all_attempted_original_and_cleanup_only_identity(self):
        class Cancel(KeyboardInterrupt):
            def __setattr__(self,name,value):
                if name=='cleanup_failed':raise RuntimeError('private')
                super().__setattr__(name,value)
        primary=Cancel();calls=[]
        def bad():calls.append(1);raise OSError('private')
        with self.assertRaises(BaseException)as caught:finish(primary,(bad,lambda:calls.append(2)))
        self.assertIs(caught.exception,primary);self.assertEqual(calls,[1,2])
        cancel=GeneratorExit()
        def stop():raise cancel
        with self.assertRaises(BaseException)as caught:finish(None,(stop,lambda:calls.append(3)))
        self.assertIs(caught.exception,cancel);self.assertEqual(calls[-1],3)


@unittest.skipUnless(HAS_SDK,'selected SDK Python3.11 environment required')
class SDKWorkerTests(unittest.TestCase):
    def setUp(self):
        self.env=patch.dict(os.environ,{},clear=True);self.env.start();self.addCleanup(self.env.stop)
        actual=metadata.version
        self.version=patch('ashlar_host._otel_worker.metadata.version',side_effect=lambda name:'0.1.0.dev0' if name=='ashlar-graph-toolkit' else actual(name))
        self.version.start();self.addCleanup(self.version.stop)
        self.sent=[];self.workers=[]
        self.addCleanup(self.cleanup)
        self.sequence=0

    def cleanup(self):
        for worker in self.workers:
            if not worker.closed:
                worker.close(time.monotonic()+.5)

    def worker(self,wire=None,send=None):
        def transport(signal,raw,deadline):self.sent.append((signal,raw));return 'success'
        worker=SDKWorker(Settings.from_wire(wire or settings_wire()),send or transport)
        self.workers.append(worker);return worker

    def context(self,worker,identity,name='started',parent=None,retry=None,operation='publication'):
        return worker.context({'op':'context','attempt_id':identity,'operation':operation,
            'event_name':'ashlar.operation.'+name,'parent':parent,'retry_link':retry})

    def emit(self,worker,identity,trace,name='started',outcome='succeeded',operation='publication'):
        self.sequence+=1
        attributes={'ashlar.run.id':'a'*32,'ashlar.attempt.id':identity,'ashlar.emitter.id':'host',
                    'ashlar.sequence':self.sequence,'ashlar.operation':operation}
        severity=9
        if name=='finished':
            attributes.update({'ashlar.outcome':outcome,'ashlar.cleanup_failed':False})
            if outcome!='succeeded':attributes['ashlar.error.category']='internal';severity=17 if outcome=='failed' else 13
        elif name=='phase':attributes.update({'ashlar.phase':'guard','ashlar.phase.state':'completed'})
        now=str(time.time_ns())
        event={'schema_version':'ashlar.diagnostic.event/0.1','timestamp_unix_nano':now,
            'observed_timestamp_unix_nano':now,'severity_number':severity,
            'severity_text':{9:'INFO',13:'WARN',17:'ERROR'}[severity],
            'event_name':'ashlar.operation.'+name,'body':{'started':'Operation started','phase':'Operation phase observed','finished':'Operation finished'}[name],
            'resource':worker.resource,'scope':{'name':'ashlar.host.diagnostics','version':'0.1.0'},
            'trace':trace,'attributes':attributes}
        raw=json.dumps(event,separators=(',',':'))+'\n';worker.emit(raw);return raw

    def test_actual_sdk_logs_span_counter_and_low8_remote_flags(self):
        from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
        from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
        from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
        worker=self.worker();identity='b'*32
        parent={'trace_id':'c'*32,'span_id':'d'*16,'trace_flags':255,'is_remote':True}
        trace=self.context(worker,identity,parent=parent);self.emit(worker,identity,trace)
        self.assertEqual(trace['trace_id'],parent['trace_id'])
        self.assertEqual(trace,self.context(worker,identity,'finished'));self.emit(worker,identity,trace,'finished')
        loss=worker.close(time.monotonic()+.8)
        self.assertEqual([v['submitted'] for v in loss.values()],[2,1,1])
        self.assertTrue(all(not v['unknown'] and v['handed_off']==v['submitted'] for v in loss.values()))
        logs=[];spans=[];points=[]
        for signal,raw in self.sent:
            self.assertNotIn(b'private-secret',raw)
            if signal=='logs':
                pb=ExportLogsServiceRequest.FromString(raw);logs.extend(pb.resource_logs[0].scope_logs[0].log_records)
            elif signal=='spans':
                pb=ExportTraceServiceRequest.FromString(raw);spans.extend(pb.resource_spans[0].scope_spans[0].spans)
            else:
                pb=ExportMetricsServiceRequest.FromString(raw);metric=pb.resource_metrics[0].scope_metrics[0].metrics[0]
                self.assertEqual(metric.name,'ashlar.operation.completed');self.assertTrue(metric.sum.is_monotonic)
                self.assertEqual(metric.sum.aggregation_temporality,2);points.extend(metric.sum.data_points)
        self.assertEqual(len(logs),2);self.assertEqual(logs[0].flags&255,trace['trace_flags'])
        self.assertEqual(spans[0].flags&255,trace['trace_flags']);self.assertTrue(spans[0].flags&256);self.assertTrue(spans[0].flags&512)
        self.assertEqual(spans[0].parent_span_id,bytes.fromhex(parent['span_id']))
        self.assertEqual(spans[0].trace_id,logs[0].trace_id);self.assertEqual(spans[0].span_id,logs[0].span_id)
        self.assertEqual(points[0].as_int,1);self.assertEqual(len(points[0].attributes),2);self.assertEqual(len(points[0].exemplars),0)

    def test_retry_link_actual_context_no_simultaneous_parent_or_ambient(self):
        from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
        worker=self.worker();identity='b'*32
        link={'trace_id':'c'*32,'span_id':'d'*16,'trace_flags':254,'is_remote':True}
        with self.assertRaises(WorkerError):self.context(worker,identity,parent=link,retry=link)
        trace=self.context(worker,identity,retry=link);self.assertNotEqual(trace['trace_id'],link['trace_id']);self.emit(worker,identity,trace)
        self.context(worker,identity,'finished');self.emit(worker,identity,trace,'finished',outcome='failed')
        worker.close(time.monotonic()+.8)
        pb=ExportTraceServiceRequest.FromString(next(raw for signal,raw in self.sent if signal=='spans'))
        span=pb.resource_spans[0].scope_spans[0].spans[0]
        self.assertEqual(span.parent_span_id,b'');self.assertEqual(span.links[0].flags&255,254)
        self.assertTrue(span.links[0].flags&512);self.assertEqual(span.links[0].trace_id,bytes.fromhex(link['trace_id']))
        self.assertEqual(span.status.code,2);self.assertEqual(span.status.message,'')

    def test_all20_series_cumulative_values_and_no_identity_dimensions(self):
        from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
        worker=self.worker();number=1
        for operation in ('publication','source-admission','held-read','ack-reconciliation'):
            for outcome in ('succeeded','refused','failed','cancelled','uncertain'):
                identity='%032x'%number;number+=1;trace=self.context(worker,identity,operation=operation);self.emit(worker,identity,trace,operation=operation)
                self.context(worker,identity,'finished',operation=operation);self.emit(worker,identity,trace,'finished',outcome=outcome,operation=operation)
        loss=worker.close(time.monotonic()+.8)
        self.assertEqual(loss['metrics']['submitted'],20)
        raw=next(raw for signal,raw in self.sent if signal=='metrics');self.assertLessEqual(len(raw),65536)
        points=ExportMetricsServiceRequest.FromString(raw).resource_metrics[0].scope_metrics[0].metrics[0].sum.data_points
        self.assertEqual(len(points),20);self.assertTrue(all(point.as_int==1 and len(point.attributes)==2 for point in points))

    def test_cumulative_snapshot_slot_drop_counts_points_not_measurements(self):
        from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
        worker=self.worker()
        for number in (1,2,3):
            identity='%032x'%number;trace=self.context(worker,identity);self.emit(worker,identity,trace)
            self.context(worker,identity,'finished');self.emit(worker,identity,trace,'finished')
            if number==1:worker.reader.collect()
        worker.reader.collect()  # Occupied snapshot rejects newest point, retains counter3.
        queue=worker.queues['metrics'];self.assertEqual(queue.submitted,2);self.assertEqual(queue.dropped,1)
        queue.drain('metrics',worker.send,time.monotonic()+.5)
        loss=worker.close(time.monotonic()+.8)
        self.assertEqual(loss['metrics']['submitted'],3);self.assertEqual(loss['metrics']['handed_off'],2);self.assertEqual(loss['metrics']['dropped'],1)
        values=[ExportMetricsServiceRequest.FromString(raw).resource_metrics[0].scope_metrics[0].metrics[0].sum.data_points[0].as_int for signal,raw in self.sent if signal=='metrics']
        self.assertEqual(values,[1,3])

    def test_pending_spans_unknown_queue_drop_and_context_correspondence(self):
        wire=settings_wire();wire['limits']['max_queue_records']=1;worker=self.worker(wire)
        identity='b'*32;trace=self.context(worker,identity);raw=self.emit(worker,identity,trace)
        self.context(worker,identity,'phase');event=json.loads(raw);event['event_name']='ashlar.operation.phase';event['body']='Operation phase observed';event['attributes'].update({'ashlar.sequence':2,'ashlar.phase':'guard','ashlar.phase.state':'completed'});event['trace']['span_id']='e'*16
        with self.assertRaises(WorkerError):worker.emit(json.dumps(event)+'\n')
        event['trace']=trace;worker.emit(json.dumps(event)+'\n')
        loss=worker.close(time.monotonic()+.8)
        self.assertEqual(loss['logs']['submitted'],2);self.assertEqual(loss['logs']['dropped'],1)
        self.assertTrue(loss['spans']['unknown']);self.assertIsNone(loss['spans']['handed_off'])

    def test_locally_lost_start_context_does_not_invent_a_log_or_outcome(self):
        worker=self.worker();identity='b'*32;trace=self.context(worker,identity)
        # Parent did not emit the started event after its local bound refused it.
        self.context(worker,identity,'finished');self.emit(worker,identity,trace,'finished')
        loss=worker.close(time.monotonic()+.8)
        self.assertEqual(loss['logs']['submitted'],1)
        self.assertEqual(loss['spans']['submitted'],1)
        self.assertEqual(loss['metrics']['submitted'],1)

    def test_actual_sdk_shutdown_preserves_primary_and_cleanup_only_cancellation(self):
        worker=self.worker();identity='b'*32;trace=self.context(worker,identity);self.emit(worker,identity,trace)
        primary=KeyboardInterrupt()
        worker.send=lambda *args: (_ for _ in ()).throw(primary)
        calls=[]
        with patch.object(worker.logs,'shutdown',side_effect=OSError('private-payload')),patch.object(worker.traces,'shutdown',side_effect=lambda:calls.append('trace')),patch.object(worker.metrics,'shutdown',side_effect=lambda **kw:calls.append('metric')):
            with self.assertRaises(BaseException)as caught:worker.close(time.monotonic()+.8)
        self.assertIs(caught.exception,primary);self.assertTrue(primary.cleanup_failed);self.assertEqual(calls,['trace','metric'])
        worker=self.worker();primary=GeneratorExit();calls=[]
        with patch.object(worker.logs,'shutdown',side_effect=primary),patch.object(worker.traces,'shutdown',side_effect=lambda:calls.append('trace')),patch.object(worker.metrics,'shutdown',side_effect=lambda **kw:calls.append('metric')):
            with self.assertRaises(BaseException)as caught:worker.close(time.monotonic()+.8)
        self.assertIs(caught.exception,primary);self.assertEqual(calls,['trace','metric'])

    def test_ambient_settings_and_versions_refuse_before_providers(self):
        with patch.dict(os.environ,{'OTEL_SDK_DISABLED':'true'}),self.assertRaises(WorkerError):self.worker()
        with patch('ashlar_host._otel_worker.metadata.version',return_value='bad'),self.assertRaises(WorkerError):self.worker()

    def test_http_response_caps_partial_redirect_cleanup_and_cancel(self):
        from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceResponse
        class Response:
            status=200
            def __init__(self,body=b'',headers=None):self.body=io.BytesIO(body);self.headers=headers or {};self.closed=False
            def getheader(self,name,default=None):return self.headers.get(name,default)
            def read1(self,n):return self.body.read(n)
            def close(self):self.closed=True
        class Connection:
            sock=None
            def __init__(self,response):self.response=response;self.closed=False;self.calls=[]
            def request(self,*args,**kwargs):self.calls.append((args,kwargs))
            def getresponse(self):return self.response
            def close(self):self.closed=True
        transport=HTTPTransport(Settings.from_wire(settings_wire()))
        cases=[(Response(), 'success'),(Response(headers={'Content-Length':'65537'}),None),
               (Response(body=b'x'*65537),None),(Response(headers={'Content-Encoding':'gzip'}),None)]
        partial=ExportLogsServiceResponse();partial.partial_success.rejected_log_records=1
        cases.append((Response(partial.SerializeToString()),'unknown'))
        redirect=Response();redirect.status=302;cases.append((redirect,None))
        for response,expected in cases:
            connection=Connection(response)
            with patch('ashlar_host._otel_worker.http.client.HTTPConnection',return_value=connection):
                if expected is None:
                    with self.assertRaises(WorkerError):transport('logs',b'x',time.monotonic()+1)
                else:self.assertEqual(transport('logs',b'x',time.monotonic()+1),expected)
            self.assertTrue(response.closed);self.assertTrue(connection.closed);self.assertEqual(len(connection.calls),1)
        primary=KeyboardInterrupt();response=Response();connection=Connection(response)
        with patch('ashlar_host._otel_worker.http.client.HTTPConnection',return_value=connection),patch.object(connection,'request',side_effect=primary),patch.object(connection,'close',side_effect=OSError('secret')):
            with self.assertRaises(BaseException)as caught:transport('logs',b'x',time.monotonic()+1)
        self.assertIs(caught.exception,primary);self.assertTrue(primary.cleanup_failed)
