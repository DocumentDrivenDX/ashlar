"""Read-only independent review of retained receiver D; never execute candidate."""
import ast
import collections
import email.parser
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys

ROOT = Path('/private/tmp/ashlar-otel-installed-receiver-probe-20261010-d')
PREP = Path('/private/tmp/ashlar-otel-installed-preparation-20261010-b')
SITE = PREP / 'environment/lib/python3.11/site-packages'
CONTRACT = Path('/Users/erik/Projects/ashlar/docs/helix/02-design/contracts/CONTRACT-006-diagnostics.md')
OUT = Path('/private/tmp/astra-otel-installed-receiver-protobuf-review-20261010-d.json')
checks = 0
wire_fields = 0
wire_messages = 0
pins = {}

def check(value, label):
    global checks
    checks += 1
    if not value:
        raise AssertionError(label)

def read(path, maximum=1048576):
    path = Path(path)
    info = path.lstat()
    check(stat.S_ISREG(info.st_mode) and not path.is_symlink(), 'regular input: ' + str(path))
    check(info.st_size <= maximum, 'bounded input: ' + str(path))
    raw = path.read_bytes()
    check(len(raw) == info.st_size, 'stable length: ' + str(path))
    pin = {'path': str(path), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}
    if str(path) in pins:
        check(pins[str(path)] == pin, 'unchanged input: ' + str(path))
    pins[str(path)] = pin
    return raw

def pairs(items):
    result = {}
    for key, value in items:
        check(key not in result, 'no duplicate JSON member')
        result[key] = value
    return result

def reject_constant(value):
    raise AssertionError('nonfinite JSON')

def parse(raw):
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=reject_constant)

def fields(message, names):
    check({field.name for field, _ in message.ListFields()} == set(names.split()),
          'closed message fields: ' + message.DESCRIPTOR.full_name)

def attributes(items, expected):
    check(len(items) == len(expected), 'exact attribute cardinality')
    seen = set()
    for item in items:
        fields(item, 'key value')
        check(item.key not in seen and item.key in expected, 'closed unique attribute keys')
        seen.add(item.key)
        value = expected[item.key]
        kind = {str: 'string_value', int: 'int_value', bool: 'bool_value'}[type(value)]
        fields(item.value, kind)
        check(item.value.WhichOneof('value') == kind, 'exact AnyValue primitive type')
        check(getattr(item.value, kind) == value, 'exact attribute value')

def audit(event, args):
    if event.startswith(('socket.', 'subprocess.')) or event in (
        'os.system', 'os.fork', 'os.forkpty', 'os.posix_spawn', 'os.exec',
        'os.kill', 'os.killpg', 'ctypes.dlopen'):
        raise RuntimeError('review refuses effects: ' + event)

sys.addaudithook(audit)
check(sys.flags.isolated and sys.flags.no_site and sys.dont_write_bytecode,
      'isolated no-site no-bytecode review interpreter')
sys.path.append(str(SITE))
custody = parse(read(PREP / 'installed-custody.json'))
dependencies = parse(read(PREP / 'dependency-payload-custody.json'))
records = {item['path']: item for item in custody['recordFiles']}
decoder_pins = []
for item in dependencies['installedSDKWheelMemberCopies']:
    if item['member'].startswith(('google/protobuf/', 'google/_upb/', 'opentelemetry/proto/')):
        raw = read(item['path'], 16777216)
        check(len(raw) == item['bytes'] and hashlib.sha256(raw).hexdigest() == item['sha256'],
              'selected decoder bytes match retained wheel custody')
        decoder_pins.append(pins[item['path']])

from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest

# A second wire scanner refuses unknown fields, duplicate singular fields,
# wrong wire types, noncanonical varints and overlong/truncated lengths before
# relying on the protobuf decoder's merged representation.
def varint(raw, position):
    start = position
    value = 0
    shift = 0
    while True:
        check(position < len(raw) and shift < 70, 'bounded varint')
        byte = raw[position]
        position += 1
        value |= (byte & 127) << shift
        if not byte & 128:
            break
        shift += 7
    check(value < 2 ** 64, '64-bit varint')
    check(position - start == max(1, (value.bit_length() + 6) // 7), 'canonical varint')
    return value, position

def wire(raw, descriptor, depth=0):
    global wire_fields, wire_messages
    wire_messages += 1
    check(depth <= 12, 'bounded protobuf depth')
    position = 0
    seen = set()
    oneofs = set()
    while position < len(raw):
        tag, position = varint(raw, position)
        number, kind = tag >> 3, tag & 7
        check(number in descriptor.fields_by_number, 'no unknown protobuf field')
        field = descriptor.fields_by_number[number]
        wire_fields += 1
        if not field.is_repeated:
            check(number not in seen, 'no duplicate singular wire field')
            seen.add(number)
        if field.containing_oneof is not None:
            name = field.containing_oneof.name
            check(name not in oneofs, 'single wire member per oneof')
            oneofs.add(name)
        expected_kind = (1 if field.type in (1, 6, 16) else
                         5 if field.type in (2, 7, 15) else
                         2 if field.type in (9, 11, 12) else 0)
        check(kind == expected_kind, 'exact protobuf wire type')
        if kind == 0:
            _, position = varint(raw, position)
        elif kind in (1, 5):
            position += 8 if kind == 1 else 4
            check(position <= len(raw), 'bounded fixed width field')
        elif kind == 2:
            length, position = varint(raw, position)
            end = position + length
            check(end <= len(raw), 'bounded length-delimited field')
            value = raw[position:end]
            if field.type == 11:
                wire(value, field.message_type, depth + 1)
            elif field.type == 9:
                value.decode('utf8', 'strict')
            position = end
    check(position == len(raw), 'all wire bytes consumed')

metadata_path = SITE / 'ashlar_graph_toolkit-0.1.0.dev0.dist-info/METADATA'
metadata_raw = read(metadata_path)
metadata = email.parser.Parser().parsestr(metadata_raw.decode('utf8'))
check(metadata.get_all('Name') == ['ashlar-graph-toolkit'] and metadata.get_all('Version') == ['0.1.0.dev0'],
      'actual installed distribution identity')
check(pins[str(metadata_path)] == records[str(metadata_path)], 'metadata equals installed RECORD custody')
version = metadata['Version']
resource = {'service.name': 'ashlar-host', 'service.version': version,
            'deployment.environment.name': 'test', 'telemetry.sdk.name': 'opentelemetry',
            'telemetry.sdk.language': 'python', 'telemetry.sdk.version': '1.45.1'}
scope = {'name': 'ashlar.host.diagnostics', 'version': '0.1.0'}
contract = read(CONTRACT)
check(hashlib.sha256(contract).hexdigest() == '06a8919aaecdfbf6f83b53bfbfa0cb09574cc654b2dcb827226cb55c6f8607cd',
      'selected C006 remains exact')
candidate = read(ROOT / 'candidate.py')
check(hashlib.sha256(candidate).hexdigest() == 'a5b2d63ccf5b2fb7dd0b699976728057881e5f56ee40e0008434eb0c45f67ab9',
      'reviewed candidate source exact')
source_gate = read(Path('/private/tmp/astra-otel-installed-receiver-delta-source-review-20261010-d.json'))
check(hashlib.sha256(source_gate).hexdigest() == 'ecd1f0012f3a213dff84fb0b9c78a32267030a047fd5b12e9c6b23c04b8d96f0',
      'exact prior D source-oracle gate')
check(b"with owner.operation_context(create_span=False):\n            third = run.begin_attempt('source-admission')\n        run.phase(third, 'admission', 'completed')\n        run.finish_attempt(third, 'succeeded')" in candidate,
      'spanless selection exits before phase/finish in actual executed source')
# Read source correspondence only. No ashlar, host or SDK provider imports.
for name in ('ashlar_host/otel.py', 'ashlar_host/_otel_worker.py', 'ashlar_host/diagnostics.py'):
    path = SITE / name
    raw = read(path)
    check(pins[str(path)] == records[str(path)], 'installed source matches retained custody')
worker = (SITE / 'ashlar_host/_otel_worker.py').read_text()
for fragment in ("span = self.tracer.start_span('ashlar.' + request['operation']",
                 "actual = attempt['span'].get_span_context()",
                 "'span_id': '%016x' % actual.span_id",
                 "event.get('trace', {}).get('span_id') == '%016x' % actual.span_id",
                 'trace_flags=actual.trace_flags'):
    check(fragment in worker, 'source corresponds to actual SDK context ownership')
tree = ast.parse(candidate)
contexts = {}
for node in ast.walk(tree):
    if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
        if node.targets[0].id in ('parent', 'link') and isinstance(node.value, ast.Call):
            call = node.value
            check(isinstance(call.func, ast.Name) and call.func.id == 'SpanContext', 'explicit fixture SpanContext')
            check(len(call.args) == 5 and not call.keywords, 'closed fixture arguments')
            flags = call.args[3]
            check(isinstance(flags, ast.Call) and isinstance(flags.func, ast.Name) and flags.func.id == 'TraceFlags', 'fixture flags')
            contexts[node.targets[0].id] = {'trace_id': f'{ast.literal_eval(call.args[0]):032x}',
                'span_id': f'{ast.literal_eval(call.args[1]):016x}',
                'is_remote': ast.literal_eval(call.args[2]), 'flags': ast.literal_eval(flags.args[0])}
check(set(contexts) == {'parent', 'link'} and all(c['is_remote'] is True and c['flags'] == 3 for c in contexts.values()),
      'two explicit remote fixture contexts with actual flags 3')
parent, link = contexts['parent'], contexts['link']

capture_dirs = list((ROOT / 'capture').iterdir())
check(len(capture_dirs) == 1 and capture_dirs[0].is_dir() and not capture_dirs[0].is_symlink(), 'one local run')
run_dir = capture_dirs[0]
manifest = parse(read(run_dir / 'run.json', 65536))
raw_events = read(run_dir / 'events-0000.jsonl', 65536)
check(raw_events.endswith(b'\n'), 'complete newline-terminated local records')
lines = raw_events.splitlines(keepends=True)
check(len(lines) == 9 and all(len(line) <= 4096 for line in lines), 'nine bounded local events')
events = [parse(line) for line in lines]
check(manifest['state'] == 'closed' and manifest['complete'] is True, 'closed locally complete manifest')
check(manifest['run_id'] == run_dir.name and re.fullmatch('[0-9a-f]{32}', run_dir.name), 'local opaque run identity')
check(manifest['segments'] == [{'source': 'events-0000.jsonl', 'bytes': len(raw_events),
       'sha256': hashlib.sha256(raw_events).hexdigest(), 'records': 9}], 'exact original segment custody')
check(manifest['resource'] == resource and manifest['scope'] == scope, 'manifest resource and scope')
check(manifest['loss']['local_attempted'] == manifest['loss']['local_written'] == 9 and
      manifest['loss']['local_dropped'] == 0 and manifest['loss']['local_unknown'] is False, 'known local nine-unit capture')
expected = parse(read(ROOT / 'expected.json'))
check(set(expected) == {'events', 'resource', 'parent', 'retry_link'}, 'closed expected fixture document')
check(expected['events'] == events and expected['resource'] == resource, 'expected copied events equal original local segment')
for key, original in [('parent', parent), ('retry_link', link)]:
    check(expected[key] == {k: v for k, v in original.items() if k != 'is_remote'}, 'expected fixture equals actual candidate binding')

for index, event in enumerate(events):
    check(set(event) == {'schema_version', 'observed_timestamp_unix_nano', 'severity_text', 'severity_number',
                       'event_name', 'body', 'resource', 'scope', 'attributes'} | ({'trace'} if index < 6 else set()), 'closed original event fields including required spanless trace absence')
    check(event['schema_version'] == 'ashlar.diagnostic.event/0.1', 'local event version')
    check(event['resource'] == resource and event['scope'] == scope, 'event resource/scope')
    step = index % 3
    operation = ('held-read', 'publication', 'source-admission')[index // 3]
    outcome = 'refused' if index == 5 else 'succeeded'
    suffix = ('started', 'phase', 'finished')[step]
    check(event['event_name'] == 'ashlar.operation.' + suffix, 'catalog event name')
    check(event['body'] == ('Operation started', 'Operation phase observed', 'Operation finished')[step], 'fixed event body')
    check(type(event['severity_number']) is int and event['severity_number'] == (13 if index == 5 else 9)
          and event['severity_text'] == ('WARN' if index == 5 else 'INFO'), 'catalog severity')
    a = event['attributes']
    common = {'ashlar.run.id', 'ashlar.attempt.id', 'ashlar.emitter.id', 'ashlar.sequence', 'ashlar.operation'}
    extra = set() if step == 0 else {'ashlar.phase', 'ashlar.phase.state'} if step == 1 else {'ashlar.outcome', 'ashlar.cleanup_failed'}
    if index == 5: extra.add('ashlar.error.category')
    check(set(a) == common | extra, 'closed local attributes')
    check(a['ashlar.run.id'] == run_dir.name and a['ashlar.emitter.id'] == 'host' and a['ashlar.operation'] == operation,
          'exact local identities and operation')
    check(type(a['ashlar.sequence']) is int and a['ashlar.sequence'] == index + 1, 'ordered unrepaired sequences')
    check(type(a['ashlar.attempt.id']) is str and re.fullmatch('[0-9a-f]{32}', a['ashlar.attempt.id']), 'opaque attempt ID shape')
    if step == 1:
        check(a['ashlar.phase'] == ('guard', 'closing', 'admission')[index // 3] and a['ashlar.phase.state'] == 'completed', 'fixture phase')
    if step == 2:
        check(a['ashlar.outcome'] == outcome and a['ashlar.cleanup_failed'] is False, 'finished outcome and Boolean cleanup')
        if index == 5: check(a['ashlar.error.category'] == 'guard', 'safe refusal category')
    if index < 6:
        t = event['trace']
        check(set(t) == {'trace_id', 'span_id', 'trace_flags'}, 'closed local context')
        check(type(t['trace_id']) is str and re.fullmatch('[0-9a-f]{32}', t['trace_id']) and int(t['trace_id'], 16), 'valid trace ID')
        check(type(t['span_id']) is str and re.fullmatch('[0-9a-f]{16}', t['span_id']) and int(t['span_id'], 16), 'valid span ID')
        check(type(t['trace_flags']) is int and t['trace_flags'] == 3, 'full observed context flags')
    else:
        check('trace' not in event, 'spanless start/phase/finish genuinely omit local context')
    check(type(event['observed_timestamp_unix_nano']) is str and re.fullmatch('[0-9]{19}', event['observed_timestamp_unix_nano']), 'unsigned local nanosecond timestamp')
    observed = int(event['observed_timestamp_unix_nano'])
    check(0 < int(manifest['started_unix_nano']) <= observed <= int(manifest['ended_unix_nano']) < 2 ** 64, 'observations bounded by run lifetime')
    if index:
        check(int(events[index - 1]['observed_timestamp_unix_nano']) < observed, 'observed timestamp order for this fixture')
check(manifest['attempts'] == [{'attempt_id': events[i]['attributes']['ashlar.attempt.id'], 'operation': events[i]['attributes']['ashlar.operation']} for i in (0, 3, 6)],
      'exact manifest invocation inventory')
check(len({events[i]['attributes']['ashlar.attempt.id'] for i in (0, 3, 6)}) == 3, 'distinct invocation IDs')
check(len({events[i]['attributes']['ashlar.attempt.id'] for i in (6, 7, 8)}) == 1, 'one spanless invocation identity through phase and finish')

result = parse(read(ROOT / 'result.json'))
provisional = parse(read(ROOT / 'receiver-observations.json'))
check(result['outcome'] == 'passed' and provisional['state'] == 'provisional' and provisional['receiverFailures'] == [], 'retained receiver outcomes')
check(result['requests'] == provisional['requests'] and len(result['requests']) == 12, 'independent inventory agreement')
inventory = result['requests']
requests = []
logs = []
spans = []
points = []
schema_url = 'https://opentelemetry.io/schemas/1.44.0'
for index, item in enumerate(inventory):
    route = '/v1/logs' if index < 9 else '/v1/traces' if index < 11 else '/v1/metrics'
    name = f'request-{index:02d}.pb'
    check(item['file'] == name and item['path'] == route and set(item) == {'path', 'file', 'bytes', 'sha256'}, 'exact route/file inventory')
    raw = read(ROOT / name, 65536)
    check(len(raw) == item['bytes'] and hashlib.sha256(raw).hexdigest() == item['sha256'], 'actual retained request custody')
    cls = ExportLogsServiceRequest if index < 9 else ExportTraceServiceRequest if index < 11 else ExportMetricsServiceRequest
    wire(raw, cls.DESCRIPTOR)
    message = cls.FromString(raw)
    check(message.SerializeToString(deterministic=True) == raw, 'canonical exact protobuf byte roundtrip')
    clean = cls()
    clean.CopyFrom(message)
    clean.DiscardUnknownFields()
    check(clean.SerializeToString(deterministic=True) == raw, 'no unknown protobuf fields recursively')
    plural = 'logs' if index < 9 else 'spans' if index < 11 else 'metrics'
    fields(message, 'resource_' + plural)
    groups = getattr(message, 'resource_' + plural)
    check(len(groups) == 1, 'one Resource group per request')
    group = groups[0]
    fields(group, 'resource scope_' + plural + ' schema_url')
    fields(group.resource, 'attributes')
    attributes(group.resource.attributes, resource)
    check(group.schema_url == schema_url, 'resource schema URL')
    scopes = getattr(group, 'scope_' + plural)
    check(len(scopes) == 1, 'one InstrumentationScope group')
    signal_group = scopes[0]
    item_field = 'log_records' if index < 9 else 'spans' if index < 11 else 'metrics'
    fields(signal_group, 'scope ' + item_field)
    fields(signal_group.scope, 'name version')
    check(signal_group.scope.name == scope['name'] and signal_group.scope.version == scope['version'], 'exact instrumentation identity; no attributes/schema URL')
    items = getattr(signal_group, item_field)
    check(len(items) == 1, 'one logical record/span/instrument per request')
    if index < 9: logs.append(items[0])
    elif index < 11: spans.append(items[0])
    else:
        metric = items[0]
        fields(metric, 'name unit sum')
        check(metric.name == 'ashlar.operation.completed' and metric.unit == '{operation}' and metric.WhichOneof('data') == 'sum', 'exact metric identity/type; no description/metadata')
        fields(metric.sum, 'data_points aggregation_temporality is_monotonic')
        check(metric.sum.aggregation_temporality == 2 and metric.sum.is_monotonic is True, 'cumulative monotonic Sum')
        points.extend(metric.sum.data_points)
    requests.append({**item, 'decodedMessage': cls.DESCRIPTOR.full_name})
check(len(logs) == 9 and len(spans) == 2 and len(points) == 3, 'separate requests and logical units')
check(sum(item['bytes'] for item in inventory) == 8149, 'exact twelve-request aggregate bytes')
for index, (event, log) in enumerate(zip(events, logs)):
    fields(log, 'observed_time_unix_nano severity_number severity_text body attributes event_name' + (' flags trace_id span_id' if index < 6 else ''))
    check(log.time_unix_nano == 0, 'unknown source Timestamp omitted, never substituted')
    check(log.observed_time_unix_nano == int(event['observed_timestamp_unix_nano']), 'exact ObservedTimestamp')
    check(log.severity_number == event['severity_number'] and log.severity_text == event['severity_text'], 'exact log severity')
    fields(log.body, 'string_value')
    check(log.body.string_value == event['body'] and log.event_name == event['event_name'], 'catalog body and dedicated EventName field')
    attributes(log.attributes, {**event['attributes'], 'ashlar.diagnostic.schema_version': event['schema_version']})
    if index < 6:
        check(log.trace_id.hex() == event['trace']['trace_id'] and log.span_id.hex() == event['trace']['span_id'] and log.flags == event['trace']['trace_flags'], 'exact log context and all flags')
    else:
        check('trace' not in event and log.trace_id == b'' and log.span_id == b'' and log.flags == 0,
              'spanless log wire IDs and flags are unset, not substituted or manufactured')

span_summaries = []
for index, span in enumerate(spans):
    start, phase, finish = events[index * 3:index * 3 + 3]
    t = start['trace']
    check(phase['trace'] == t == finish['trace'], 'single actual context across invocation')
    check(len({e['attributes']['ashlar.attempt.id'] for e in (start, phase, finish)}) == 1, 'one attempt identity per invocation')
    names = 'trace_id span_id name kind start_time_unix_nano end_time_unix_nano attributes status flags'
    names += ' parent_span_id' if index == 0 else ' links'
    fields(span, names)
    check(span.trace_id.hex() == t['trace_id'] and span.span_id.hex() == t['span_id'], 'same wire span and captured SDK context')
    check(span.kind == 1 and span.name == 'ashlar.' + start['attributes']['ashlar.operation'], 'INTERNAL catalog operation span')
    check(span.flags == (771 if index == 0 else 259) and span.flags & 255 == t['trace_flags'], 'full span flags, known remote parent bits')
    check(0 < int(start['observed_timestamp_unix_nano']) <= span.start_time_unix_nano <= int(phase['observed_timestamp_unix_nano']) <= span.end_time_unix_nano == int(finish['observed_timestamp_unix_nano']), 'SDK span creation time and exact observed end timestamp')
    attrs = {k: start['attributes'][k] for k in ('ashlar.run.id', 'ashlar.attempt.id', 'ashlar.operation')}
    attrs.update({k: finish['attributes'][k] for k in ('ashlar.outcome', 'ashlar.cleanup_failed')})
    if index: attrs['ashlar.error.category'] = 'guard'
    attributes(span.attributes, attrs)
    fields(span.status, '' if index == 0 else 'code')
    check(span.status.code == (0 if index == 0 else 2) and span.status.message == '', 'UNSET success/ERROR refusal, no description')
    check(not span.events and not span.trace_state and span.dropped_attributes_count == span.dropped_events_count == span.dropped_links_count == 0, 'no automatic exceptions/events/tracestate/drop counters')
    check(t['span_id'] not in (parent['span_id'], link['span_id']), 'observed child span ID differs from explicit fixtures')
    if index == 0:
        check(t['trace_id'] == parent['trace_id'] and span.parent_span_id.hex() == parent['span_id'] and not span.links, 'explicit parent supplies only parent context')
    else:
        check(t['trace_id'] not in (parent['trace_id'], link['trace_id']) and not span.parent_span_id and len(span.links) == 1, 'retry is separate root with one external link')
        actual = span.links[0]
        fields(actual, 'trace_id span_id flags')
        check(actual.trace_id.hex() == link['trace_id'] and actual.span_id.hex() == link['span_id'] and actual.flags == 771, 'exact external retry-link context, flags and remote bits')
    span_summaries.append({'operation': attrs['ashlar.operation'], 'attemptId': attrs['ashlar.attempt.id'],
        'traceId': t['trace_id'], 'spanId': t['span_id'], 'flags': span.flags,
        'parentSpanId': span.parent_span_id.hex() or None, 'links': len(span.links),
        'startTimeUnixNano': str(span.start_time_unix_nano), 'endTimeUnixNano': str(span.end_time_unix_nano),
        'statusCode': span.status.code})
check(spans[0].span_id != spans[1].span_id, 'distinct observed SDK span IDs')
check(all(e['trace']['trace_id'] != e['attributes']['ashlar.run.id'] and e['trace']['trace_id'] != e['attributes']['ashlar.attempt.id'] for e in events[:6]), 'context IDs not copied from run/attempt IDs')
check(all(events[6]['attributes']['ashlar.attempt.id'] not in {a.value.string_value for a in span.attributes if a.key == 'ashlar.attempt.id'} for span in spans), 'spanless invocation submits no ended span')

point_summaries = []
for index, point in enumerate(points):
    fields(point, 'attributes start_time_unix_nano time_unix_nano as_int')
    operation, outcome = (('held-read', 'succeeded'), ('publication', 'refused'), ('source-admission', 'succeeded'))[index]
    dimensions = {'ashlar.operation': operation, 'ashlar.outcome': outcome}
    attributes(point.attributes, dimensions)
    check(point.WhichOneof('value') == 'as_int' and point.as_int == 1, 'integer observed invocation count')
    check(not point.exemplars and point.flags == 0, 'no exemplars or data point flags')
    check(int(events[index * 3 + 2]['observed_timestamp_unix_nano']) <= point.start_time_unix_nano <= point.time_unix_nano <= int(manifest['ended_unix_nano']), 'metric times follow measurement and precede closure')
    point_summaries.append({'dimensions': dimensions, 'value': point.as_int,
        'startTimeUnixNano': str(point.start_time_unix_nano), 'timeUnixNano': str(point.time_unix_nano)})
check(len({point.time_unix_nano for point in points}) == 1, 'one metric collection instant')
for signal, units in [('logs', 9), ('spans', 2), ('metrics', 3)]:
    check(manifest['loss'][signal] == {'submitted': units, 'handed_off': units, 'dropped': 0, 'unknown': False, 'flush': 'complete'}, 'manifest counts logical units for ' + signal)

imported = []
for name, module in sorted(sys.modules.items()):
    file = getattr(module, '__file__', None)
    if file and str(SITE) in file:
        path = Path(file)
        check(str(path) in records, 'decoder import belongs to installed custody')
        raw = read(path, 16777216)
        check(pins[str(path)] == records[str(path)], 'actual imported decoder exact hash')
        imported.append({'module': name, **pins[str(path)]})
check(not any(name.startswith(('ashlar', 'opentelemetry.sdk', 'opentelemetry.exporter')) for name in sys.modules), 'no host/provider/exporter imports')
opening_pins = list(pins.values())
for pin in opening_pins:
    read(pin['path'], 16777216)
report = {
    'format': 'astra-independent-installed-receiver-protobuf-review/0.1',
    'verdict': 'approved-retained-wire-semantics-with-stated-scope',
    'root': str(ROOT), 'checks': checks,
    'method': 'Read-only retained bytes; isolated Python -I -S -B; installed immutable protobuf decoder plus independent descriptor-guided raw wire scanner; strict effect denial; no candidate/worker/receiver/native rerun.',
    'fullWireAudit': {'messages': wire_messages, 'fields': wire_fields, 'unknownFields': 0,
                      'duplicateSingularFields': 0, 'duplicateAttributeKeys': 0, 'roundtripExactRequests': 12},
    'requests': requests,
    'logicalUnits': {'logs': 9, 'spans': 2, 'metricsNumberDataPoints': 3, 'metricRequests': 1},
    'resource': resource, 'scope': scope, 'resourceSchemaUrl': schema_url,
    'sourceTimestamp': 'Absent in all nine original events and omitted from all nine OTLP logs; ObservedTimestamp equals exact original nanoseconds.',
    'explicitFixtures': contexts,
    'observedSpans': span_summaries,
    'metricPoints': point_summaries,
    'spanlessEvidence': {'operation': 'source-admission', 'attemptId': events[6]['attributes']['ashlar.attempt.id'],
        'localEventSequences': [7, 8, 9], 'localTracePresent': False, 'wireTraceId': '', 'wireSpanId': '', 'wireFlags': 0,
        'spanSubmitted': False, 'completedCounter': 1,
        'qualification': 'Reviewed candidate D begins this attempt under create_span=False and leaves the binding before phase/finish. Installed source stores no span for the attempt; retained start/phase/finish and wire context are absent throughout. No source or record repair was performed in this review.'},
    'contextEvidence': 'Parent and link are explicitly authored remote SpanContext fixtures. The two distinct child span IDs and retry-root trace ID are observed in both original local capture and retained OTLP. Inspected installed source gets them from Tracer.start_span/get_span_context and validates every log against that owned context. This is source-to-retained-output correspondence, not an independently captured in-memory worker context transcript or proof of randomness.',
    'qualifications': [
        'Three sequential synthetic public DiagnosticRun/OtelRun attempts only: held-read succeeded; publication refused with guard category; genuinely spanless source-admission succeeded.',
        'No native publication/source/query/ACK workflow or concurrent-attempt receiver control.',
        'No credential/privacy sentinel, outage/deadline/cancellation pilot, memory/runtime overhead measurement, or complete C006 support claim.',
        'Receiver bytes prove this observed handoff mapping; they do not prove durable ingestion or exactly-once delivery.',
        'No process census or runtime effect was performed by this reviewer. Parent review owns full installed/input/output custody and execution/cleanup interpretation.'
    ],
    'importedDecoderModules': imported,
    'decoderFilesCheckedAgainstRetainedWheelCustody': len(decoder_pins),
    'openingClosingInputPins': opening_pins,
    'script': {'path': str(Path(__file__)), 'bytes': Path(__file__).stat().st_size,
               'sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},
    'findings': []
}
raw = (json.dumps(report, indent=2, sort_keys=True) + '\n').encode()
with OUT.open('xb') as stream:
    stream.write(raw)
print(json.dumps({'receipt': str(OUT), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(),
                  'checks': checks, 'wireMessages': wire_messages, 'wireFields': wire_fields,
                  'importedDecoderModules': len(imported), 'findings': 0}, sort_keys=True))
