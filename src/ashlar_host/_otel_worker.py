"""C006 isolated SDK projection and bounded close-only OTLP transport.

The parent owns run identity, local capture and the hard deadline/child group.
This worker owns no publication authority and never installs global providers.
Queue/transport budgets cover retained payload, not SDK/runtime object memory.
One initial HTTP attempt per unit is intentional; no retry/delivery inference.
Parent supervision of explicit per-request progress deadlines bounds blocking
DNS/header parsing as well as total shutdown; socket timeout alone does not.
"""
from dataclasses import dataclass
from importlib import metadata
import http.client
import ipaddress
import json
import logging
import math
import os
import stat
from pathlib import Path
import re
import ssl
import struct
import sys
import time
import unicodedata
from typing import BinaryIO, Callable, Optional
from urllib.parse import urlsplit

from .config import DiagnosticsLimits
from .diagnostics import CATALOG, OPERATIONS, OUTCOMES, SCOPE, MAX_COUNT

FRAME_LIMIT = 65536
REQUEST_LIMIT = 1048576
RESPONSE_LIMIT = 65536
VERSIONS = {'opentelemetry-api': '1.45.1', 'opentelemetry-sdk': '1.45.1',
            'opentelemetry-proto': '1.45.1',
            'opentelemetry-exporter-otlp-proto-common': '1.45.1',
            'opentelemetry-exporter-otlp-proto-http': '1.45.1',
            'opentelemetry-semantic-conventions': '0.66b1'}


class WorkerError(ValueError):
    """Fixed payload-free worker refusal."""


def require(condition: bool) -> None:
    if not condition:
        raise WorkerError('diagnostics-configuration')


def keys(value: object, expected: set) -> None:
    require(type(value) is dict and set(value) == expected)


def mark(primary: BaseException) -> None:
    try:
        primary.cleanup_failed = True
    except BaseException:
        pass


def finish(primary: Optional[BaseException], actions: tuple, *, diagnostic_only: bool = False) -> None:
    """Attempt every cleanup without replacing the first original exception."""
    owner_primary = primary is not None and not diagnostic_only
    for action in actions:
        try:
            action()
        except BaseException as exc:
            if primary is None:
                primary = exc
            elif not owner_primary and isinstance(primary, Exception) and not isinstance(exc, Exception):
                mark(exc)
                primary = exc
            else:
                mark(primary)
    if primary is not None:
        raise primary


def parse(raw: bytes) -> dict:
    require(type(raw) is bytes and 0 < len(raw) <= FRAME_LIMIT)
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result)
            result[key] = value
        return result
    def integer(token):
        require(len(token.lstrip('-')) <= 20)
        return int(token)
    def real(token):
        require(len(token) <= 32)
        value = float(token)
        require(math.isfinite(value))
        return value
    def invalid(token):
        raise WorkerError('diagnostics-configuration')
    depth = 0
    quoted = escaped = False
    for byte in raw:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
        elif byte == 34:
            quoted = True
        elif byte in (91, 123):
            depth += 1
            require(depth <= 12)
        elif byte in (93, 125):
            depth -= 1
            require(depth >= 0)
    try:
        value = json.loads(raw.decode('utf8'), object_pairs_hook=pairs,
                           parse_int=integer, parse_float=real, parse_constant=invalid)
    except (ValueError, UnicodeError, RecursionError):
        raise WorkerError('diagnostics-configuration') from None
    require(type(value) is dict)
    return value


def read_exact(stream: BinaryIO, count: int) -> bytes:
    parts = bytearray()
    while len(parts) < count:
        chunk = stream.read(count - len(parts))
        require(type(chunk) is bytes and bool(chunk))
        parts.extend(chunk)
    return bytes(parts)


def read_frame(stream: BinaryIO) -> dict:
    length = struct.unpack('>I', read_exact(stream, 4))[0]
    require(0 < length <= FRAME_LIMIT)
    return parse(read_exact(stream, length))


def write_frame(stream: BinaryIO, value: dict) -> None:
    raw = json.dumps(value, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode('utf8')
    require(len(raw) <= FRAME_LIMIT)
    pending = struct.pack('>I', len(raw)) + raw
    offset = 0
    while offset < len(pending):
        count = stream.write(pending[offset:])
        require(type(count) is int and count > 0)
        offset += count
    stream.flush()


@dataclass(frozen=True, repr=False)
class Settings:
    endpoint: str
    headers: tuple
    tls: str
    ca_file: Optional[str]
    environment: str
    limits: DiagnosticsLimits
    service_version: str

    @classmethod
    def from_wire(cls, value: dict) -> 'Settings':
        keys(value, {'endpoint', 'headers', 'tls', 'ca_file', 'environment', 'limits', 'service_version'})
        endpoint = value['endpoint']
        require(type(endpoint) is str and len(endpoint.encode('utf8')) <= 2048
                and not any(c.isspace() or c == '\\' or unicodedata.category(c) == 'Cc' for c in endpoint))
        url = urlsplit(endpoint)
        require(url.scheme in ('http', 'https') and url.hostname is not None
                and url.username is None and url.password is None and not url.query and not url.fragment
                and '?' not in endpoint and '#' not in endpoint
                and not url.path.rstrip('/').endswith(('/v1/logs', '/v1/traces', '/v1/metrics'))
                and (url.port is None or 1 <= url.port <= 65535))
        require(value['tls'] in ('system', 'custom-ca', 'loopback-test'))
        if value['tls'] == 'loopback-test':
            require(url.scheme == 'http')
            require(url.hostname == 'localhost' or ipaddress.ip_address(url.hostname).is_loopback)
        else:
            require(url.scheme == 'https')
        ca = value['ca_file']
        if value['tls'] == 'custom-ca':
            require(type(ca) is str and Path(ca).is_absolute() and not Path(ca).is_symlink())
            require(Path(ca).is_file() and Path(ca).stat().st_size <= 1048576)
        else:
            require(ca is None)
        headers = value['headers']
        require(type(headers) is list and len(headers) <= 8)
        names = set()
        snapshot = []
        for pair in headers:
            require(type(pair) is list and len(pair) == 2)
            name, content = pair
            require(type(name) is str and re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]{1,64}", name) is not None
                    and name.lower() not in names and name.lower() not in ('host', 'content-length', 'content-type', 'accept-encoding', 'connection', 'transfer-encoding'))
            require(type(content) is str and len(content.encode('utf8')) <= 2048
                    and not any(unicodedata.category(c) == 'Cc' for c in content))
            names.add(name.lower()); snapshot.append((name, content))
        require(type(value['environment']) is str and value['environment'] in ('development', 'test', 'staging', 'production'))
        require(type(value['service_version']) is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9.+_-]{0,63}', value['service_version']) is not None)
        keys(value['limits'], set(DiagnosticsLimits.__dataclass_fields__))
        limits = DiagnosticsLimits(**value['limits'])
        return cls(endpoint.rstrip('/'), tuple(snapshot), value['tls'], ca,
                   value['environment'], limits, value['service_version'])


class Queue:
    """Unit payload reservation retained through in-flight resolution."""
    def __init__(self, maximum_records: int, maximum_bytes: int):
        self.maximum_records, self.maximum_bytes = maximum_records, maximum_bytes
        self.units = []
        self.bytes = 0
        self.submitted = self.handed_off = self.dropped = 0
        self.unknown = False
        self.flush = 'not_attempted'

    def offer(self, raw: bytes, units: int = 1, overhead: int = 0) -> None:
        require(type(raw) is bytes)
        self.offer_sized(len(raw), lambda: raw, units, overhead)

    def offer_sized(self, size: int, serialize: Callable, units: int = 1, overhead: int = 0) -> None:
        require(type(size) is int and size >= 0 and type(units) is int and units >= 0)
        self.submitted += units
        if self.submitted > MAX_COUNT:
            self.unknown = True
        if (len(self.units) == self.maximum_records or self.bytes + size + overhead > self.maximum_bytes
                or size > REQUEST_LIMIT):
            self.dropped += units
            return
        raw = serialize()
        require(type(raw) is bytes and len(raw) == size)
        self.units.append((raw, units))
        self.bytes += size

    def drain(self, signal: str, send: Callable, deadline: float) -> None:
        self.flush = 'complete'
        while self.units:
            raw, count = self.units[0]
            if time.monotonic() >= deadline:
                self.dropped += sum(unit[1] for unit in self.units)
                self.units.clear(); self.bytes = 0; self.flush = 'incomplete'
                break
            try:
                status = send(signal, raw, deadline)
                require(status in ('success', 'unknown'))
                if status == 'success':
                    self.handed_off += count
                else:
                    self.unknown = True; self.flush = 'failed'
            except Exception:
                self.unknown = True; self.flush = 'failed'
            # No retry: the same reservation remains owned until this result.
            self.units.pop(0); self.bytes -= len(raw)

    def loss(self) -> dict:
        return {'submitted': None if self.submitted > MAX_COUNT else self.submitted,
                'handed_off': None if self.unknown else self.handed_off,
                'dropped': None if self.unknown else self.dropped,
                'unknown': self.unknown, 'flush': self.flush}


def read_ca(path: Path) -> bytes:
    """Regular no-follow bounded CA snapshot; no path enters signal evidence."""
    fd = None
    primary = None
    raw = bytearray()
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= 1048576)
        while len(raw) <= 1048576:
            part = os.read(fd, min(65536, 1048577 - len(raw)))
            if not part:
                break
            raw.extend(part)
        after = os.fstat(fd)
        current = path.lstat()
        def identity(info):
            return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_mode)
        require(identity(before) == identity(after) == identity(current) and len(raw) == before.st_size)
    except BaseException as exc:
        primary = exc
    finish(primary, () if fd is None else (lambda: os.close(fd),), diagnostic_only=True)
    return bytes(raw)


class HTTPTransport:
    """Direct stdlib HTTP; no ambient proxies, redirects or eager response body."""
    def __init__(self, settings: Settings, progress: Optional[Callable] = None):
        self.settings = settings
        self.progress = progress
        self.failed_cleanup = False
        self.ssl_context = None
        if settings.tls != 'loopback-test':
            self.ssl_context = ssl.create_default_context()
            if settings.ca_file is not None:
                raw = read_ca(Path(settings.ca_file))
                # PEM is UTF8 text; DER is an exact bounded byte snapshot.
                ca = raw.decode('ascii') if raw.startswith(b'-----BEGIN') else raw
                self.ssl_context.load_verify_locations(cadata=ca)

    def __call__(self, signal: str, raw: bytes, deadline: float) -> str:
        require(not self.failed_cleanup and signal in ('logs', 'spans', 'metrics') and len(raw) <= REQUEST_LIMIT)
        now = time.monotonic()
        request_deadline = min(deadline, now + self.settings.limits.export_timeout_ms / 1000)
        require(request_deadline > now)
        url = urlsplit(self.settings.endpoint)
        timeout = request_deadline - now
        if self.progress is not None:
            self.progress({'transport_deadline': request_deadline})
        primary = None
        response = connection = None
        try:
            if url.scheme == 'https':
                connection = http.client.HTTPSConnection(url.hostname, url.port, timeout=timeout, context=self.ssl_context)
            else:
                host = '127.0.0.1' if url.hostname == 'localhost' else url.hostname
                connection = http.client.HTTPConnection(host, url.port, timeout=timeout)
            headers = {name: value.encode('utf8') for name, value in self.settings.headers}
            headers.update({'Content-Type': 'application/x-protobuf', 'Accept-Encoding': 'identity', 'Connection': 'close'})
            route = {'logs': 'logs', 'spans': 'traces', 'metrics': 'metrics'}[signal]
            connection.request('POST', url.path.rstrip('/') + '/v1/' + route, body=raw, headers=headers)
            remaining = request_deadline - time.monotonic()
            require(remaining > 0)
            socket = connection.sock
            if socket is not None:
                socket.settimeout(remaining)
            response = connection.getresponse()
            require(time.monotonic() < request_deadline and response.status == 200
                    and response.getheader('Content-Encoding', 'identity').lower() == 'identity')
            length = response.getheader('Content-Length')
            if length is not None:
                require(re.fullmatch('[0-9]{1,10}', length) is not None and int(length) <= RESPONSE_LIMIT)
            body = bytearray()
            while True:
                remaining = request_deadline - time.monotonic()
                require(remaining > 0)
                if socket is not None:
                    socket.settimeout(remaining)
                part = response.read1(min(4096, RESPONSE_LIMIT + 1 - len(body)))
                if not part:
                    break
                body.extend(part)
                require(len(body) <= RESPONSE_LIMIT)
            require(time.monotonic() <= request_deadline)
            from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceResponse
            from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceResponse
            from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceResponse
            message = {'logs': ExportLogsServiceResponse, 'spans': ExportTraceServiceResponse,
                       'metrics': ExportMetricsServiceResponse}[signal]()
            message.ParseFromString(bytes(body))
            # Even zero rejected-count with an explanatory partial-success is ambiguous.
            result = 'unknown' if message.HasField('partial_success') else 'success'
        except BaseException as exc:
            primary = exc
        def cleanup(action):
            def owned():
                try:
                    action()
                except BaseException:
                    self.failed_cleanup = True
                    raise
            return owned
        actions = tuple(cleanup(action) for action in
                        (response.close if response is not None else None,
                         connection.close if connection is not None else None) if action is not None)
        outcome = None
        try:
            finish(primary, actions, diagnostic_only=True)
        except BaseException as exc:
            outcome = exc
        # Failed ownership cleanup cannot reset the parent's request deadline.
        # Refuse later sends; process termination is the remaining closing fence.
        if not self.failed_cleanup and self.progress is not None:
            finish(outcome, (lambda: self.progress({'transport_complete': True}),), diagnostic_only=True)
        elif outcome is not None:
            raise outcome
        return result


class SDKWorker:
    """A single run's SDK projections; no process-global provider ownership."""
    def __init__(self, settings: Settings, send: Optional[Callable] = None,
                 progress: Optional[Callable] = None):
        require(type(settings) is Settings and sys.version_info[:2] == (3, 11))
        require(metadata.version('ashlar-graph-toolkit') == settings.service_version)
        require(all(metadata.version(name) == version for name, version in VERSIONS.items()))
        # Root constructs a clean environment. Refuse accidental ambient SDK knobs.
        require(not any(name.startswith('OTEL_') or name.lower() in ('http_proxy', 'https_proxy', 'all_proxy', 'no_proxy', 'ssl_cert_file', 'ssl_cert_dir', 'requests_ca_bundle', 'curl_ca_bundle') for name in os.environ))
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider, SpanLimits, SpanProcessor
        from opentelemetry.sdk.trace.sampling import ALWAYS_ON
        from opentelemetry.sdk._logs import LoggerProvider, LogRecordProcessor, ReadableLogRecord, LogRecordLimits
        from opentelemetry.sdk.metrics import MeterProvider, AlwaysOffExemplarFilter
        from opentelemetry.sdk.metrics.export import MetricReader, AggregationTemporality
        from opentelemetry.sdk.metrics import Counter
        from opentelemetry.sdk.metrics.view import View
        from opentelemetry.exporter.otlp.proto.common._log_encoder import encode_logs
        from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
        from opentelemetry.exporter.otlp.proto.common.metrics_encoder import encode_metrics
        self.settings, self.send = settings, send or HTTPTransport(settings, progress)
        self.queues = {name: Queue(settings.limits.max_queue_records, settings.limits.max_queue_bytes)
                       for name in ('logs', 'spans')}
        self.queues['metrics'] = Queue(1, RESPONSE_LIMIT)
        self.attempts = {}
        self.run_id = None
        self.last_sequence = 0
        self.metric_state = {}
        self.closed = False
        self.resource = {'service.name': 'ashlar-host', 'service.version': settings.service_version,
                         'deployment.environment.name': settings.environment,
                         'telemetry.sdk.name': 'opentelemetry', 'telemetry.sdk.language': 'python',
                         'telemetry.sdk.version': '1.45.1'}
        resource = Resource(self.resource, schema_url='https://opentelemetry.io/schemas/1.44.0')
        owner = self
        class Logs(LogRecordProcessor):
            def on_emit(self, record):
                readable = ReadableLogRecord(record.log_record, record.resource, record.instrumentation_scope, record.limits)
                encoded = encode_logs((readable,))
                require(readable.dropped_attributes == 0)
                owner.queues['logs'].offer_sized(encoded.ByteSize(), encoded.SerializeToString)
            def shutdown(self):
                pass
            def force_flush(self, timeout_millis=30000):
                return not owner.queues['logs'].units
        class Spans(SpanProcessor):
            def on_end(self, span):
                encoded = encode_spans((span,))
                wire = encoded.resource_spans[0].scope_spans[0].spans[0]
                wire.flags = (wire.flags & ~255) | (int(span.get_span_context().trace_flags) & 255)
                for original, link in zip(span.links, wire.links):
                    link.flags = (link.flags & ~255) | (int(original.context.trace_flags) & 255)
                owner.queues['spans'].offer_sized(encoded.ByteSize(), encoded.SerializeToString)
            def shutdown(self):
                pass
            def force_flush(self, timeout_millis=30000):
                return not owner.queues['spans'].units
        class Reader(MetricReader):
            def __init__(self):
                super().__init__(preferred_temporality={Counter: AggregationTemporality.CUMULATIVE})
            def _receive_metrics(self, data, timeout_millis=10000, **kwargs):
                units = 0
                for resource_metrics in data.resource_metrics:
                    for scope_metrics in resource_metrics.scope_metrics:
                        for metric in scope_metrics.metrics:
                            require(metric.name == 'ashlar.operation.completed' and metric.unit == '{operation}')
                            require(metric.data.is_monotonic and metric.data.aggregation_temporality == AggregationTemporality.CUMULATIVE)
                            for point in metric.data.data_points:
                                require(set(point.attributes) == {'ashlar.operation', 'ashlar.outcome'}
                                        and type(point.value) is int and point.value >= 0 and not point.exemplars)
                                units += 1
                require(units <= 20)
                accumulator = len(json.dumps([[*key, count] for key, count in owner.metric_state.items()], separators=(',', ':')).encode())
                encoded = encode_metrics(data)
                owner.queues['metrics'].offer_sized(encoded.ByteSize(), encoded.SerializeToString, units, accumulator)
            def shutdown(self, timeout_millis=30000, **kwargs):
                pass
        self.reader = Reader()
        self.metrics = MeterProvider(metric_readers=(self.reader,), resource=resource,
            exemplar_filter=AlwaysOffExemplarFilter(), shutdown_on_exit=False,
            views=(View(instrument_name='ashlar.operation.completed',
                        attribute_keys={'ashlar.operation', 'ashlar.outcome'}),))
        self.counter = self.metrics.get_meter(SCOPE['name'], SCOPE['version']).create_counter('ashlar.operation.completed', unit='{operation}')
        self.traces = TracerProvider(resource=resource, sampler=ALWAYS_ON, shutdown_on_exit=False,
            span_limits=SpanLimits(max_attributes=8, max_events=0, max_links=1,
                max_link_attributes=0, max_attribute_length=128), meter_provider=self.metrics)
        self.traces.add_span_processor(Spans())
        self.tracer = self.traces.get_tracer(SCOPE['name'], SCOPE['version'])
        self.logs = LoggerProvider(resource=resource, shutdown_on_exit=False, meter_provider=self.metrics,
            log_record_limits=LogRecordLimits(max_attributes=12, max_attribute_length=128))
        self.logs.add_log_record_processor(Logs())
        self.logger = self.logs.get_logger(SCOPE['name'], SCOPE['version'])

    def context(self, request: dict) -> dict:
        keys(request, {'op', 'attempt_id', 'operation', 'event_name', 'parent', 'retry_link'})
        require(not self.closed and request['op'] == 'context'
                and type(request['attempt_id']) is str and re.fullmatch('[0-9a-f]{32}', request['attempt_id']) is not None
                and request['operation'] in OPERATIONS and request['event_name'] in CATALOG
                and not (request['parent'] is not None and request['retry_link'] is not None))
        from opentelemetry.context import Context
        from opentelemetry.trace import SpanContext, TraceFlags, NonRecordingSpan, set_span_in_context, SpanKind, Link
        def incoming(value):
            if value is None:
                return None
            keys(value, {'trace_id', 'span_id', 'trace_flags', 'is_remote'})
            require(type(value['trace_id']) is str and re.fullmatch('[0-9a-f]{32}', value['trace_id']) is not None and int(value['trace_id'], 16) != 0
                    and type(value['span_id']) is str and re.fullmatch('[0-9a-f]{16}', value['span_id']) is not None and int(value['span_id'], 16) != 0
                    and type(value['trace_flags']) is int and 0 <= value['trace_flags'] <= 255 and type(value['is_remote']) is bool)
            return SpanContext(int(value['trace_id'], 16), int(value['span_id'], 16), value['is_remote'], TraceFlags(value['trace_flags']))
        parent, link = incoming(request['parent']), incoming(request['retry_link'])
        identity = request['attempt_id']
        if request['event_name'] == 'ashlar.operation.started':
            require(identity not in self.attempts and len(self.attempts) < self.settings.limits.max_attempts)
            context = set_span_in_context(NonRecordingSpan(parent), Context()) if parent is not None else Context()
            span = self.tracer.start_span('ashlar.' + request['operation'], context=context, kind=SpanKind.INTERNAL,
                attributes={'ashlar.attempt.id': identity, 'ashlar.operation': request['operation']},
                links=() if link is None else (Link(link),), record_exception=False, set_status_on_exception=False)
            actual = span.get_span_context()
            require(actual.is_valid)
            self.attempts[identity] = {'span': span, 'operation': request['operation'], 'finished': False,
                                      'started': False, 'pending': None}
        else:
            require(identity in self.attempts and parent is None and link is None)
        attempt = self.attempts[identity]
        require(not attempt['finished'] and attempt['operation'] == request['operation'] )
        actual = attempt['span'].get_span_context()
        result = {'trace_id': '%032x' % actual.trace_id, 'span_id': '%016x' % actual.span_id,
                  'trace_flags': int(actual.trace_flags) & 255}
        attempt['pending'] = request['event_name']
        return result

    def emit(self, text: str) -> None:
        require(not self.closed and type(text) is str)
        raw = text.encode('utf8')
        require(len(raw) <= self.settings.limits.max_event_bytes and raw.endswith(b'\n'))
        from .diagnostics import decode_event
        event = decode_event(raw)
        attributes = event['attributes']
        identity = attributes['ashlar.attempt.id']
        require(identity in self.attempts)
        attempt = self.attempts[identity]
        actual = attempt['span'].get_span_context()
        require(event['event_name'] == attempt['pending'] and not attempt['finished']
                and event['resource'] == self.resource and event['scope'] == SCOPE
                and attributes['ashlar.operation'] == attempt['operation']
                and attributes['ashlar.sequence'] > self.last_sequence
                and attributes['ashlar.sequence'] <= self.settings.limits.max_emissions
                and event.get('trace', {}).get('trace_id') == '%032x' % actual.trace_id
                and event.get('trace', {}).get('span_id') == '%016x' % actual.span_id
                and event.get('trace', {}).get('trace_flags') == (int(actual.trace_flags) & 255))
        if self.run_id is None:
            self.run_id = attributes['ashlar.run.id']
        require(attributes['ashlar.run.id'] == self.run_id)
        if event['event_name'] == 'ashlar.operation.started':
            require(not attempt['started'])
            attempt['started'] = True
        # A locally dropped started log can leave this span without that log;
        # context creation still attests the owned invocation, not its outcome.
        from opentelemetry._logs import LogRecord, SeverityNumber
        from opentelemetry.context import Context
        from opentelemetry.trace import set_span_in_context, Status, StatusCode
        attempt['span'].set_attribute('ashlar.run.id', attributes['ashlar.run.id'])
        log_attributes = {**attributes, 'ashlar.diagnostic.schema_version': event['schema_version']}
        record = LogRecord(timestamp=int(event['timestamp_unix_nano']) if 'timestamp_unix_nano' in event else None, observed_timestamp=int(event['observed_timestamp_unix_nano']),
            context=set_span_in_context(attempt['span'], Context()), trace_id=actual.trace_id, span_id=actual.span_id,
            trace_flags=actual.trace_flags, severity_number=SeverityNumber(event['severity_number']),
            severity_text=event['severity_text'], body=event['body'], attributes=log_attributes, event_name=event['event_name'])
        self.logger.emit(record)
        attempt['pending'] = None
        self.last_sequence = attributes['ashlar.sequence']
        if event['event_name'] == 'ashlar.operation.finished':
            outcome = attributes['ashlar.outcome']
            require(outcome in OUTCOMES)
            attempt['span'].set_attribute('ashlar.outcome', outcome)
            attempt['span'].set_attribute('ashlar.cleanup_failed', attributes['ashlar.cleanup_failed'])
            if outcome != 'succeeded':
                attempt['span'].set_attribute('ashlar.error.category', attributes['ashlar.error.category'])
            attempt['span'].set_status(Status(StatusCode.UNSET if outcome == 'succeeded' else StatusCode.ERROR))
            attempt['span'].end(end_time=int(event.get('timestamp_unix_nano', event['observed_timestamp_unix_nano'])))
            attempt['finished'] = True
            key = (attempt['operation'], outcome)
            self.metric_state[key] = self.metric_state.get(key, 0) + 1
            self.counter.add(1, {'ashlar.operation': key[0], 'ashlar.outcome': key[1]}, context=Context())

    def close(self, deadline: float) -> dict:
        require(not self.closed and type(deadline) in (float, int) and type(deadline) is not bool
                and math.isfinite(deadline) and time.monotonic() <= deadline
                <= time.monotonic() + self.settings.limits.shutdown_timeout_ms / 1000 + .01)
        self.closed = True
        primary = None
        try:
            self.reader.collect(timeout_millis=max(1, (deadline - time.monotonic()) * 1000))
            for signal in ('logs', 'spans', 'metrics'):
                self.queues[signal].drain(signal, self.send, deadline)
            if any(not attempt['finished'] for attempt in self.attempts.values()):
                self.queues['spans'].unknown = True
                self.queues['spans'].flush = 'failed'
        except BaseException as exc:
            primary = exc
        finish(primary, (self.logs.shutdown, self.traces.shutdown,
                         lambda: self.metrics.shutdown(timeout_millis=max(1, (deadline - time.monotonic()) * 1000))), diagnostic_only=True)
        return {signal: queue.loss() for signal, queue in self.queues.items()}


def serve(stdin: BinaryIO, stdout: BinaryIO) -> None:
    """Sequential framed protocol; every ordinary refusal is a fixed reply."""
    logging.disable(sys.maxsize)
    worker = None
    primary = None
    try:
        request = read_frame(stdin)
        keys(request, {'op', 'settings'}); require(request['op'] == 'init')
        worker = SDKWorker(Settings.from_wire(request['settings']),
                           progress=lambda value: write_frame(stdout, {'ok': True, 'value': value}))
        write_frame(stdout, {'ok': True, 'value': 'ready'})
        while True:
            request = read_frame(stdin)
            require(type(request.get('op')) is str)
            if request['op'] == 'context':
                value = worker.context(request)
            elif request['op'] == 'emit':
                keys(request, {'op', 'event'}); worker.emit(request['event']); value = None
            elif request['op'] == 'close':
                keys(request, {'op', 'deadline'}); value = worker.close(request['deadline'])
                write_frame(stdout, {'ok': True, 'value': value}); return
            else:
                raise WorkerError('diagnostics-configuration')
            write_frame(stdout, {'ok': True, 'value': value})
    except Exception:
        try:
            write_frame(stdout, {'ok': False, 'value': 'diagnostics-configuration'})
        except BaseException as exc:
            primary = exc
    except BaseException as exc:
        primary = exc
    finally:
        # No transport on protocol failure; parent owns termination of this worker.
        if worker is not None and not worker.closed:
            worker.closed = True
            try:
                finish(primary, (worker.logs.shutdown, worker.traces.shutdown,
                                 lambda: worker.metrics.shutdown(timeout_millis=1)), diagnostic_only=True)
            except Exception:
                raise WorkerError('diagnostics-configuration') from None
        elif primary is not None:
            raise primary


if __name__ == '__main__':
    try:
        serve(sys.stdin.buffer, sys.stdout.buffer)
    except BaseException:
        # Process protocol never prints even exception arguments or tracebacks.
        raise SystemExit(1) from None
