"""Unexecuted small installed-host receiver qualification candidate."""
from dataclasses import fields
from importlib import metadata
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
import hashlib
import json
import os
import threading
import time

from ashlar_host.config import DiagnosticsConfig, DiagnosticsLimits, SecretText
from ashlar_host.diagnostics import DiagnosticRun, read_diagnostics
from ashlar_host.otel import OtelRun
from opentelemetry.trace import SpanContext, TraceFlags, TraceState
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest

ROOT = Path('/private/tmp/ashlar-otel-installed-receiver-probe-20261010-b')
received, failures = [], []


def write(path, raw):
    assert len(raw) <= 1048576
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
    if primary is not None: raise primary
    assert path.read_bytes() == raw


def mark(primary):
    try: primary.cleanup_failed = True
    except BaseException: pass


def cleanup_failure(primary, error):
    if primary is None or (isinstance(primary, Exception) and not isinstance(error, Exception)):
        return error
    mark(primary)
    return primary


class Server(HTTPServer):
    def get_request(self):
        connection, address = super().get_request()
        try: connection.settimeout(0.5)
        except BaseException as primary:
            try: connection.close()
            except BaseException as cleanup: primary = cleanup_failure(primary, cleanup)
            raise primary
        return connection, address


class Receiver(BaseHTTPRequestHandler):
    def log_message(self, *_args): pass
    def do_POST(self):
        try:
            assert self.path in ('/v1/logs', '/v1/traces', '/v1/metrics')
            assert self.headers.get('Content-Type') == 'application/x-protobuf'
            assert self.headers.get('Content-Encoding') is None
            length = int(self.headers['Content-Length'])
            assert 0 < length <= 65536 and len(received) < 19
            assert sum(len(raw) for _, raw in received) + length <= 1048576
            raw = self.rfile.read(length); assert len(raw) == length
            received.append((self.path, raw))
            self.send_response(200); self.send_header('Content-Length', '0'); self.end_headers()
        except Exception:
            failures.append('receiver-refused')
            self.close_connection = True


def attributes(items):
    result = {}
    for item in items:
        assert item.key not in result
        kind = item.value.WhichOneof('value')
        assert kind in ('string_value', 'int_value', 'bool_value')
        result[item.key] = getattr(item.value, kind)
    return result


def scope(group, scopes, resource):
    assert attributes(group.resource.attributes) == resource
    assert group.resource.dropped_attributes_count == 0
    assert group.schema_url == 'https://opentelemetry.io/schemas/1.44.0'
    assert len(scopes) == 1
    value = scopes[0]
    assert value.scope.name == 'ashlar.host.diagnostics' and value.scope.version == '0.1.0'
    assert not value.scope.attributes and not value.schema_url
    return value


def main():
    assert not (ROOT / 'result.json').exists() and not (ROOT / 'capture').exists()
    server = thread = owner = run = None
    manifest = None
    primary = None
    started = time.monotonic()
    parent = SpanContext(0x1234567890abcdef1234567890abcdef, 0x1234567890abcdef, True, TraceFlags(3), TraceState())
    link = SpanContext(0xabcdef1234567890abcdef1234567890, 0xabcdef1234567890, True, TraceFlags(3), TraceState())
    try:
        server = Server(('127.0.0.1', 0), Receiver)
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.01}, name='ashlar-installed-receiver')
        thread.start()
        capture = ROOT / 'capture'; capture.mkdir(mode=0o700)
        limits = DiagnosticsLimits(4096, 128, 524288, 16384, 65536, 100, 8, 1000, 2000, 3600)
        origins = {name: 'explicit' for name in ('profile', 'capture_root', 'endpoint', 'headers', 'tls', 'ca_file', 'environment')}
        origins.update({'limits.' + field.name: 'explicit' for field in fields(limits)})
        config = DiagnosticsConfig('ashlar-host-otel-http/0.1', capture,
            SecretText('http://127.0.0.1:%d/' % server.server_port), (), 'loopback-test', None, 'test', limits, origins)
        owner = OtelRun(config)
        run = DiagnosticRun(config, owner.sink)
        with owner.operation_context(parent_context=parent):
            first = run.begin_attempt('held-read')
        run.phase(first, 'guard', 'completed')
        run.finish_attempt(first, 'succeeded')
        with owner.operation_context(retry_link=link):
            second = run.begin_attempt('publication')
        run.phase(second, 'closing', 'completed')
        run.finish_attempt(second, 'refused', 'guard')
        manifest = run.close()
        assert manifest is not None and manifest['complete']
        assert not failures
    except BaseException as error:
        primary = error
    finally:
        # Always close the public signal owner, including failed run construction.
        actions = [lambda: owner.shutdown(time.monotonic() + 2) if owner is not None else None,
                   lambda: server.shutdown() if thread is not None and thread.is_alive() else None,
                   lambda: thread.join(timeout=1) if thread is not None and thread.ident is not None else None,
                   lambda: server.server_close() if server is not None else None]
        for action in actions:
            try: action()
            except BaseException as error: primary = cleanup_failure(primary, error)
        if thread is not None and thread.is_alive(): primary = cleanup_failure(primary, RuntimeError('receiver-cleanup-failed'))
    # Preserve bounded receiver bytes even when lifecycle or later oracle fails.
    inventory = []
    for index, (path, raw) in enumerate(received):
        try:
            name = 'request-%02d.pb' % index; write(ROOT / name, raw)
            inventory.append({'path': path, 'file': name, 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()})
        except BaseException as error: primary = cleanup_failure(primary, error)
    try:
        write(ROOT / 'receiver-observations.json', (json.dumps({'state': 'provisional', 'requests': inventory, 'receiverFailures': failures}, indent=2) + '\n').encode())
    except BaseException as error: primary = cleanup_failure(primary, error)
    if primary is not None: raise primary
    assert len(received) == 9 and [sum(path == route for path, _ in received) for route in ('/v1/logs', '/v1/traces', '/v1/metrics')] == [6, 2, 1]
    snapshot = read_diagnostics(run.directory)
    events = [record['event'] for record in snapshot['records']]
    assert snapshot['capture_complete'] and snapshot['matched'] == 6 and len(events) == 6
    resource = events[0]['resource']
    assert resource['service.version'] == metadata.version('ashlar-graph-toolkit')
    logs, spans, points = [], [], []
    for path, raw in received:
        if path == '/v1/logs':
            message = ExportLogsServiceRequest.FromString(raw); assert len(message.resource_logs) == 1
            group = message.resource_logs[0]; logs.extend(scope(group, group.scope_logs, resource).log_records)
        elif path == '/v1/traces':
            message = ExportTraceServiceRequest.FromString(raw); assert len(message.resource_spans) == 1
            group = message.resource_spans[0]; spans.extend(scope(group, group.scope_spans, resource).spans)
        else:
            message = ExportMetricsServiceRequest.FromString(raw); assert len(message.resource_metrics) == 1
            group = message.resource_metrics[0]; metrics = scope(group, group.scope_metrics, resource).metrics
            assert len(metrics) == 1
            metric = metrics[0]
            assert metric.name == 'ashlar.operation.completed' and metric.unit == '{operation}'
            assert metric.WhichOneof('data') == 'sum' and metric.sum.is_monotonic and metric.sum.aggregation_temporality == 2
            points.extend(metric.sum.data_points)
    assert len(logs) == 6 and len(spans) == 2 and len(points) == 2
    for event, log in zip(events, logs):
        assert log.time_unix_nano == 0 and log.observed_time_unix_nano == int(event['observed_timestamp_unix_nano'])
        assert log.severity_number == event['severity_number'] and log.severity_text == event['severity_text']
        assert log.event_name == event['event_name'] and log.body.WhichOneof('value') == 'string_value' and log.body.string_value == event['body']
        assert attributes(log.attributes) == {**event['attributes'], 'ashlar.diagnostic.schema_version': event['schema_version']}
        trace = event['trace']
        assert log.trace_id.hex() == trace['trace_id'] and log.span_id.hex() == trace['span_id'] and log.flags == trace['trace_flags']
        assert log.dropped_attributes_count == 0
    for index, span in enumerate(spans):
        start, phase, finish = events[index * 3:index * 3 + 3]
        trace = start['trace']; assert phase['trace'] == trace == finish['trace']
        attrs = start['attributes']; outcome = finish['attributes']['ashlar.outcome']
        expected = {key: attrs[key] for key in ('ashlar.run.id', 'ashlar.attempt.id', 'ashlar.operation')}
        expected.update({key: value for key, value in finish['attributes'].items() if key in ('ashlar.outcome', 'ashlar.cleanup_failed', 'ashlar.error.category')})
        assert attributes(span.attributes) == expected
        assert span.trace_id.hex() == trace['trace_id'] and span.span_id.hex() == trace['span_id']
        assert span.flags & 255 == trace['trace_flags'] and span.kind == 1 and span.name == 'ashlar.' + attrs['ashlar.operation']
        assert 0 < int(start['observed_timestamp_unix_nano']) <= span.start_time_unix_nano <= int(phase['observed_timestamp_unix_nano']) <= span.end_time_unix_nano
        assert span.end_time_unix_nano == int(finish['observed_timestamp_unix_nano'])
        assert not span.events and span.dropped_attributes_count == span.dropped_events_count == span.dropped_links_count == 0
        assert span.status.code == (0 if outcome == 'succeeded' else 2) and not span.status.message
        if index == 0:
            assert span.trace_id.hex() == '%032x' % parent.trace_id and span.parent_span_id.hex() == '%016x' % parent.span_id
            assert span.flags & 768 == 768 and not span.links
        else:
            assert not span.parent_span_id and span.trace_id.hex() != '%032x' % link.trace_id and len(span.links) == 1
            actual = span.links[0]
            assert actual.trace_id.hex() == '%032x' % link.trace_id and actual.span_id.hex() == '%016x' % link.span_id
            assert actual.flags == 771 and not actual.attributes and not actual.trace_state
    metric_bag = []
    for point in points:
        assert point.WhichOneof('value') == 'as_int' and point.as_int == 1 and not point.exemplars
        assert 0 < point.start_time_unix_nano <= point.time_unix_nano
        metric_bag.append(attributes(point.attributes))
    assert sorted(metric_bag, key=lambda item: item['ashlar.operation']) == [
        {'ashlar.operation': 'held-read', 'ashlar.outcome': 'succeeded'},
        {'ashlar.operation': 'publication', 'ashlar.outcome': 'refused'}]
    for signal, count in (('logs', 6), ('spans', 2), ('metrics', 2)):
        assert manifest['loss'][signal] == {'submitted': count, 'handed_off': count, 'dropped': 0, 'unknown': False, 'flush': 'complete'}
    write(ROOT / 'expected.json', (json.dumps({'events': events, 'resource': resource, 'parent': {'trace_id': '%032x' % parent.trace_id, 'span_id': '%016x' % parent.span_id, 'flags': 3}, 'retry_link': {'trace_id': '%032x' % link.trace_id, 'span_id': '%016x' % link.span_id, 'flags': 3}}, indent=2) + '\n').encode())
    write(ROOT / '.result-stage.json', (json.dumps({'format': 'ashlar-installed-host-receiver/0.1', 'outcome': 'passed', 'requests': inventory, 'durationSeconds': time.monotonic() - started, 'receiverThreadClosed': True, 'scope': 'Installed public DiagnosticRun/OtelRun, two sequential synthetic attempts, explicit parent and external retry-link context, closed local capture and real bounded loopback receiver.', 'gaps': ['No concurrent attempt test', 'No untraced event support claim', 'No real publication/query/source/ACK workflow', 'No credential/privacy sentinel or outage/deadline/fault pilot', 'No hermetic runtime closure or process census; public owner shutdown completion only']}, indent=2) + '\n').encode())
    os.link(ROOT / '.result-stage.json', ROOT / 'result.json')
    try: (ROOT / '.result-stage.json').unlink()
    except Exception: pass  # Final availability already committed; maintenance only.


if __name__ == '__main__': main()
