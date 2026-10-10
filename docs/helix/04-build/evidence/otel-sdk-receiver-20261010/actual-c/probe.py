"""Small actual SDK/loopback mapping probe; no production adapter qualification."""
import hashlib
import http.client
import json
import os
from pathlib import Path
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from uuid import uuid4

from opentelemetry.context import Context
from opentelemetry._logs import LogRecord, SeverityNumber
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk._logs import LoggerProvider, LogRecordLimits
from opentelemetry.sdk._logs.export import LogRecordExporter, LogRecordExportResult, SimpleLogRecordProcessor
from opentelemetry.sdk.trace import TracerProvider, SpanLimits
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult, SimpleSpanProcessor
from opentelemetry.trace import SpanKind, TraceFlags
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import MetricReader, AggregationTemporality
from opentelemetry.sdk.metrics import Counter
from opentelemetry.sdk.metrics import AlwaysOffExemplarFilter
from opentelemetry.exporter.otlp.proto.common._log_encoder import encode_logs
from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
from opentelemetry.exporter.otlp.proto.common.metrics_encoder import encode_metrics
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest

OUTPUT = Path('/private/tmp/ashlar-otel-sdk-receiver-probe-20261010-c/result.json')
RESOURCE = {'service.name': 'ashlar-host', 'service.version': '0.1.0.dev0',
            'deployment.environment.name': 'test', 'telemetry.sdk.name': 'opentelemetry',
            'telemetry.sdk.language': 'python', 'telemetry.sdk.version': '1.45.1'}
SCHEMA = 'https://opentelemetry.io/schemas/1.44.0'
SCOPE = ('ashlar.host.diagnostics', '0.1.0')
received = []
failures = []


def mark(primary):
    try:
        primary.cleanup_failed = True
    except BaseException:
        pass


def write_closed(path, raw):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    primary = None
    try:
        offset = 0
        while offset < len(raw):
            count = os.write(fd, raw[offset:]); assert count > 0
            offset += count
    except BaseException as error:
        primary = error
    try:
        os.close(fd)
    except BaseException as error:
        if primary is None: primary = error
        else: mark(primary)
    if primary is not None:
        raise primary
    assert path.read_bytes() == raw


class OwnedServer(HTTPServer):
    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(0.5)
        return connection, address


class Receiver(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_POST(self):
        try:
            assert self.path in ('/v1/logs', '/v1/traces', '/v1/metrics')
            assert self.headers['Content-Type'] == 'application/x-protobuf'
            assert self.headers.get('Content-Encoding') is None
            length = int(self.headers['Content-Length']); assert 0 < length <= 1048576
            raw = self.rfile.read(length); assert len(raw) == length
            received.append((self.path, raw))
            self.send_response(200); self.send_header('Content-Length', '0'); self.end_headers()
        except Exception:
            failures.append('receiver-refused')
            self.send_response(400); self.send_header('Content-Length', '0'); self.end_headers()


def post(signal, raw):
    assert 0 < len(raw) <= 1048576
    connection = http.client.HTTPConnection('127.0.0.1', server.server_port, timeout=0.5)
    primary = None
    try:
        connection.request('POST', '/v1/' + signal, body=raw,
                           headers={'Content-Type': 'application/x-protobuf'})
        response = connection.getresponse()
        assert response.status == 200 and response.read(65537) == b''
    except BaseException as error:
        primary = error
    try:
        connection.close()
    except BaseException as error:
        if primary is None:
            primary = error
        else:
            mark(primary)
    if primary is not None:
        raise primary


class Logs(LogRecordExporter):
    def export(self, batch):
        post('logs', encode_logs(batch).SerializeToString())
        return LogRecordExportResult.SUCCESS

    def force_flush(self, timeout_millis=1000):
        return True

    def shutdown(self):
        pass


class Spans(SpanExporter):
    def export(self, spans):
        post('traces', encode_spans(spans).SerializeToString())
        return SpanExportResult.SUCCESS

    def shutdown(self):
        pass


class Metrics(MetricReader):
    def __init__(self):
        super().__init__(preferred_temporality={Counter: AggregationTemporality.CUMULATIVE})

    def _receive_metrics(self, data, timeout_millis=1000):
        post('metrics', encode_metrics(data).SerializeToString())

    def shutdown(self, timeout_millis=1000, **kwargs):
        pass


def attrs(values):
    result = {}
    for item in values:
        kind = item.value.WhichOneof('value')
        assert kind in ('string_value', 'int_value', 'bool_value')
        assert item.key not in result
        result[item.key] = getattr(item.value, kind)
    return result


def envelope(group, scopes):
    assert attrs(group.resource.attributes) == RESOURCE
    assert group.schema_url == SCHEMA
    assert len(scopes) == 1
    assert scopes[0].scope.name == SCOPE[0] and scopes[0].scope.version == SCOPE[1]
    assert not scopes[0].schema_url and not scopes[0].scope.attributes
    return scopes[0]


server = OwnedServer(('127.0.0.1', 0), Receiver)
server.timeout = 0.5
thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.01},
                          name='ashlar-sdk-probe-receiver')
primary = None
logger_provider = tracer_provider = meter_provider = None
started = time.monotonic()
try:
    thread.start()
    resource = Resource(RESOURCE, schema_url=SCHEMA)
    reader = Metrics()
    meter_provider = MeterProvider(metric_readers=[reader], resource=resource,
                                   exemplar_filter=AlwaysOffExemplarFilter(), shutdown_on_exit=False)
    tracer_provider = TracerProvider(resource=resource, sampler=ALWAYS_ON,
                                     span_limits=SpanLimits(max_attributes=16, max_events=0, max_links=1),
                                     shutdown_on_exit=False, meter_provider=meter_provider)
    tracer_provider.add_span_processor(SimpleSpanProcessor(Spans()))
    logger_provider = LoggerProvider(resource=resource, shutdown_on_exit=False,
                                     meter_provider=meter_provider,
                                     log_record_limits=LogRecordLimits(max_attributes=16))
    logger_provider.add_log_record_processor(SimpleLogRecordProcessor(Logs()))
    logger = logger_provider.get_logger(*SCOPE)
    tracer = tracer_provider.get_tracer(*SCOPE)
    counter = meter_provider.get_meter(*SCOPE).create_counter('ashlar.operation.completed', unit='{operation}')
    run, attempt = uuid4().hex, uuid4().hex
    base = {'ashlar.run.id': run, 'ashlar.attempt.id': attempt,
            'ashlar.emitter.id': 'host', 'ashlar.operation': 'held-read',
            'ashlar.diagnostic.schema_version': 'ashlar.diagnostic.event/0.1'}
    span = tracer.start_span('ashlar.held-read', context=Context(), kind=SpanKind.INTERNAL,
                             attributes={key: base[key] for key in ('ashlar.run.id', 'ashlar.attempt.id', 'ashlar.operation')})
    context = span.get_span_context(); assert context.is_valid and (context.trace_flags & TraceFlags.SAMPLED) and 0 <= int(context.trace_flags) <= 255
    observed = time.time_ns(); timestamp = observed - 10
    logger.emit(LogRecord(timestamp=timestamp, observed_timestamp=observed,
                         trace_id=context.trace_id, span_id=context.span_id, trace_flags=context.trace_flags,
                         severity_text='INFO', severity_number=SeverityNumber.INFO,
                         body='Operation started', event_name='ashlar.operation.started',
                         attributes={**base, 'ashlar.sequence': 1}, context=Context()))
    logger.emit(LogRecord(observed_timestamp=observed + 1, trace_id=0, span_id=0, trace_flags=TraceFlags(0),
                         severity_text='INFO', severity_number=SeverityNumber.INFO,
                         body='Operation phase observed', event_name='ashlar.operation.phase',
                         attributes={**base, 'ashlar.sequence': 2, 'ashlar.phase': 'guard',
                                     'ashlar.phase.state': 'completed'}, context=Context()))
    span.set_attribute('ashlar.outcome', 'succeeded'); span.set_attribute('ashlar.cleanup_failed', False)
    span.end()
    counter.add(1, {'ashlar.operation': 'held-read', 'ashlar.outcome': 'succeeded'})
    reader.collect(timeout_millis=1000)
    assert not failures and [item[0] for item in received] == ['/v1/logs', '/v1/logs', '/v1/traces', '/v1/metrics']
    log_rows = []
    for _, raw in received[:2]:
        request = ExportLogsServiceRequest.FromString(raw); assert len(request.resource_logs) == 1
        scope = envelope(request.resource_logs[0], request.resource_logs[0].scope_logs)
        assert len(scope.log_records) == 1
        log_rows.append(scope.log_records[0])
    first, second = log_rows
    assert first.time_unix_nano == timestamp and first.observed_time_unix_nano == observed
    assert first.trace_id == context.trace_id.to_bytes(16, 'big') and first.span_id == context.span_id.to_bytes(8, 'big')
    assert first.flags == int(context.trace_flags) and first.event_name == 'ashlar.operation.started'
    assert first.body.string_value == 'Operation started' and first.severity_number == 9 and first.severity_text == 'INFO'
    assert attrs(first.attributes) == {**base, 'ashlar.sequence': 1}
    assert second.time_unix_nano == 0 and second.observed_time_unix_nano == observed + 1
    assert not second.trace_id and not second.span_id and second.flags == 0
    assert second.event_name == 'ashlar.operation.phase' and second.body.string_value == 'Operation phase observed'
    assert attrs(second.attributes) == {**base, 'ashlar.sequence': 2, 'ashlar.phase': 'guard', 'ashlar.phase.state': 'completed'}
    request = ExportTraceServiceRequest.FromString(received[2][1]); assert len(request.resource_spans) == 1
    scope = envelope(request.resource_spans[0], request.resource_spans[0].scope_spans); assert len(scope.spans) == 1
    actual_span = scope.spans[0]
    assert actual_span.trace_id == first.trace_id and actual_span.span_id == first.span_id
    assert actual_span.name == 'ashlar.held-read' and actual_span.kind == 1
    assert actual_span.flags == 256  # exact pinned SDK native root-parent carrier, not context TraceFlags
    assert not actual_span.parent_span_id and not actual_span.events and not actual_span.links
    assert actual_span.status.code == 0 and not actual_span.status.message
    assert attrs(actual_span.attributes) == {key: base[key] for key in ('ashlar.run.id', 'ashlar.attempt.id', 'ashlar.operation')} | {'ashlar.outcome': 'succeeded', 'ashlar.cleanup_failed': False}
    request = ExportMetricsServiceRequest.FromString(received[3][1]); assert len(request.resource_metrics) == 1
    scope = envelope(request.resource_metrics[0], request.resource_metrics[0].scope_metrics); assert len(scope.metrics) == 1
    metric = scope.metrics[0]; assert metric.name == 'ashlar.operation.completed' and metric.unit == '{operation}'
    assert metric.sum.is_monotonic and metric.sum.aggregation_temporality == 2 and len(metric.sum.data_points) == 1
    point = metric.sum.data_points[0]
    assert point.WhichOneof('value') == 'as_int' and point.as_int == 1 and not point.exemplars
    assert attrs(point.attributes) == {'ashlar.operation': 'held-read', 'ashlar.outcome': 'succeeded'}
except BaseException as error:
    primary = error
finally:
    for action in [lambda: logger_provider.shutdown() if logger_provider else None,
                   lambda: tracer_provider.shutdown() if tracer_provider else None,
                   lambda: meter_provider.shutdown(timeout_millis=1000) if meter_provider else None,
                   lambda: server.shutdown() if thread.ident is not None else None,
                   lambda: thread.join(timeout=1) if thread.ident is not None else None,
                   server.server_close]:
        try:
            action()
        except BaseException as error:
            if primary is None: primary = error
            else: mark(primary)
if primary is not None:
    raise primary
assert not thread.is_alive()
for index, (_signal, raw) in enumerate(received):
    write_closed(OUTPUT.parent / ('request-%02d.pb' % index), raw)
expected = {'resource': RESOURCE, 'resourceSchemaUrl': SCHEMA, 'scope': list(SCOPE),
            'attributes': base, 'timestampUnixNano': timestamp, 'observedUnixNano': observed,
            'actualSdkContext': {'traceId': context.trace_id.to_bytes(16, 'big').hex(),
                                 'spanId': context.span_id.to_bytes(8, 'big').hex(),
                                 'traceFlags': int(context.trace_flags)}}
write_closed(OUTPUT.parent / 'expected.json', (json.dumps(expected, indent=2) + '\n').encode())
result = (json.dumps({'format': 'ashlar-otel-sdk-receiver-probe/0.1', 'outcome': 'passed',
    'runtime': 'Python3.11.17 OTelSDK1.45.1', 'requests': [{'signal': signal, 'bytes': len(raw),
    'sha256': hashlib.sha256(raw).hexdigest()} for signal, raw in received],
    'logicalUnits': {'logs': 2, 'spans': 1, 'metrics': 1}, 'durationSeconds': time.monotonic() - started,
    'receiverThreadClosed': True, 'actualSdkTraceFlags': int(context.trace_flags),
    'actualOtlpSpanFlags': actual_span.flags,
    'spanFlagLimitation': 'Pinned SDK native span encoder omits actual context flags in low8 OTLP bits; this probe does not repair them or claim full span/context representation conformance.', 'scope': 'Actual SDK direct logger/span/cumulative integer Counter through OTLP encoders to real loopback HTTP receiver. Test resource service.version is a fixed fixture; no installed Ashlar service-version, production adapter, queue/deadline/privacy/failure, capture or operation integration claim.'}, indent=2) + '\n').encode()
stage = OUTPUT.parent / '.result-stage.json'
write_closed(stage, result)
os.link(stage, OUTPUT)  # exclusive availability commit after closed receiver and files
try:
    stage.unlink()
except Exception:
    pass  # complete final result survives ordinary stage housekeeping failure
