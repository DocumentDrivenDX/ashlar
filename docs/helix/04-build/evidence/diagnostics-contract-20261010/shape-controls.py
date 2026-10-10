import copy
import hashlib
import importlib.metadata
import json
import platform
import re
from pathlib import Path

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

ROOT = Path('/Users/erik/Projects/ashlar')
CONTRACT = ROOT / 'docs/helix/02-design/contracts/CONTRACT-006-diagnostics.md'
BASE = CONTRACT.parent / 'schemas'
EVENT = json.loads((BASE / 'diagnostic-event-v0.1.schema.json').read_text())
RUN = json.loads((BASE / 'diagnostic-run-v0.1.schema.json').read_text())
retrievals = []

def refuse(uri):
    retrievals.append(uri)
    raise AssertionError('external schema retrieval forbidden')

registry = Registry(retrieve=refuse).with_resources([
    (s['$id'], Resource.from_contents(s)) for s in (EVENT, RUN)
])
for schema in (EVENT, RUN):
    Draft202012Validator.check_schema(schema)
validators = {
    'event': Draft202012Validator(EVENT, registry=registry),
    'run': Draft202012Validator(RUN, registry=registry),
}
checks = []

def check(name, value, kind, expected):
    actual = validators[kind].is_valid(value)
    assert actual is expected, (name, [e.message for e in validators[kind].iter_errors(value)])
    checks.append({'name': name, 'valid': actual, 'kind': kind})

def mutate(name, original, path, value, kind='event', expected=False):
    obj = copy.deepcopy(original)
    target = obj
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    check(name, obj, kind, expected)

event = json.loads(re.findall(r'```json\n(.*?)\n```', CONTRACT.read_text(), re.S)[0])
check('contract-untraced-example', event, 'event', True)
trace = {'trace_id': '4bf92f3577b34da6a3ce929d0e0e4736', 'span_id': '00f067aa0ba902b7', 'trace_flags': 1}
traced = dict(event, trace=trace)
check('contract-traced-example', traced, 'event', True)
for flags in [0, 3, 255]:
    mutate('actual-byte-flags-' + str(flags), traced, ['trace', 'trace_flags'], flags, expected=True)
for state in ['started', 'completed', 'failed', 'uncertain']:
    phase = copy.deepcopy(event)
    phase.update(event_name='ashlar.operation.phase', body='Operation phase observed')
    phase['attributes'].update({'ashlar.phase': 'guard', 'ashlar.phase.state': state})
    check('phase-' + state, phase, 'event', True)
for outcome in ['succeeded', 'failed', 'cancelled', 'refused', 'uncertain']:
    finished = copy.deepcopy(event)
    finished.update(event_name='ashlar.operation.finished', body='Operation finished')
    finished['attributes'].update({'ashlar.outcome': outcome, 'ashlar.cleanup_failed': False})
    if outcome != 'succeeded':
        finished['attributes']['ashlar.error.category'] = 'guard'
        finished.update(severity_text='ERROR' if outcome == 'failed' else 'WARN', severity_number=17 if outcome == 'failed' else 13)
    check('finished-' + outcome, finished, 'event', True)
    mutate('finished-wrong-severity-' + outcome, finished, ['severity_number'], 17 if outcome != 'failed' else 9)
    if outcome != 'succeeded':
        missing = copy.deepcopy(finished)
        del missing['attributes']['ashlar.error.category']
        check('finished-missing-category-' + outcome, missing, 'event', False)

for name, path, value in [
    ('unknown-field', ['extra'], 'raw'),
    ('wrong-version', ['schema_version'], 'ashlar.diagnostic.event/0.2'),
    ('free-body', ['body'], 'raw source body'),
    ('wrong-event', ['event_name'], 'ashlar.operation.sql'),
    ('severity-mismatch', ['severity_number'], 13),
    ('free-attribute', ['attributes', 'sql'], 'select private'),
    ('extra-phase-on-start', ['attributes', 'ashlar.phase'], 'guard'),
    ('extra-outcome-on-start', ['attributes', 'ashlar.outcome'], 'failed'),
    ('bad-operation', ['attributes', 'ashlar.operation'], 'query-value'),
    ('id-newline', ['attributes', 'ashlar.run.id'], '1'*32+'\n'),
    ('negative-sequence', ['attributes', 'ashlar.sequence'], -1),
    ('huge-sequence', ['attributes', 'ashlar.sequence'], 10001),
    ('boolean-sequence', ['attributes', 'ashlar.sequence'], True),
    ('free-resource', ['resource', 'host.name'], 'private-host'),
    ('bad-sdk-pin', ['resource', 'telemetry.sdk.version'], '1.45.0'),
    ('resource-newline', ['resource', 'service.version'], '1.0\n'),
    ('free-environment', ['resource', 'deployment.environment.name'], 'private-tenant'),
    ('bad-scope', ['scope', 'name'], 'sql-logger'),
    ('unknown-scope-field', ['scope', 'extra'], 'private'),
    ('zero-time', ['observed_timestamp_unix_nano'], '0'),
    ('time-newline', ['observed_timestamp_unix_nano'], '12\n'),
    ('numeric-time', ['observed_timestamp_unix_nano'], 12),
    ('too-long-time', ['observed_timestamp_unix_nano'], '1'*21),
]:
    mutate(name, event, path, value)
for name, path, value in [
    ('zero-trace', ['trace', 'trace_id'], '0'*32),
    ('zero-span', ['trace', 'span_id'], '0'*16),
    ('uppercase-trace', ['trace', 'trace_id'], 'A'*32),
    ('trace-newline', ['trace', 'trace_id'], '1'*32+'\n'),
    ('negative-flags', ['trace', 'trace_flags'], -1),
    ('overflow-flags', ['trace', 'trace_flags'], 256),
    ('boolean-flags', ['trace', 'trace_flags'], True),
    ('free-context', ['trace', 'baggage'], 'private'),
]:
    mutate(name, traced, path, value)
for member in trace:
    missing = copy.deepcopy(traced)
    del missing['trace'][member]
    check('partial-context-' + member, missing, 'event', False)

limits = {
    'max_event_bytes': 4096, 'max_queue_records': 128, 'max_queue_bytes': 524288,
    'max_segment_bytes': 4194304, 'max_capture_bytes': 16777216,
    'max_emissions': 10000, 'max_attempts': 64,
    'export_timeout_ms': 1000, 'shutdown_timeout_ms': 2000, 'retention_seconds': 86400,
}
export = {'submitted': 1, 'handed_off': 1, 'dropped': 0, 'unknown': False, 'flush': 'complete'}
raw_event = (json.dumps(event, separators=(',', ':'))+'\n').encode()
run = {
    'schema_version': 'ashlar.diagnostic.run/0.1', 'profile': 'ashlar-host-otel-http/0.1',
    'run_id': event['attributes']['ashlar.run.id'], 'state': 'closed',
    'started_unix_nano': '1791590400000000000', 'ended_unix_nano': '1791590401000000000',
    'expires_unix_nano': '1791676801000000000',
    'resource': event['resource'], 'scope': event['scope'],
    'attempts': [{'attempt_id': event['attributes']['ashlar.attempt.id'], 'operation': 'held-read'}],
    'configuration_origins': {k: 'explicit' for k in RUN['properties']['configuration_origins']['required']},
    'limits': limits, 'sampling': {'logs': 'all', 'new_spans': 'always_on', 'metrics': 'all'},
    'segments': [{'source': 'events-0000.jsonl', 'bytes': len(raw_event), 'sha256': hashlib.sha256(raw_event).hexdigest(), 'records': 1}],
    'loss': {'local_attempted': 1, 'local_written': 1, 'local_dropped': 0, 'local_unknown': False,
             'logs': copy.deepcopy(export), 'spans': copy.deepcopy(export), 'metrics': copy.deepcopy(export)},
    'complete': True,
}
check('complete-run', run, 'run', True)
opened = copy.deepcopy(run)
opened.update(state='open', complete=False, segments=[])
for key in ('ended_unix_nano', 'expires_unix_nano'):
    del opened[key]
check('initial-open-run', opened, 'run', True)
unknown = copy.deepcopy(run)
unknown['complete'] = False
unknown['loss'].update(local_attempted=None, local_unknown=True)
check('unknown-local-loss', unknown, 'run', True)
remote = copy.deepcopy(run)
remote['loss']['logs'].update(handed_off=None, unknown=True, flush='incomplete')
check('local-complete-remote-unknown', remote, 'run', True)
metric_unknown = copy.deepcopy(run)
metric_unknown['loss']['metrics'].update(handed_off=None, unknown=True, flush='complete')
check('metrics-unit-handoff-unknown-after-local-drain', metric_unknown, 'run', True)
mutate('metrics-null-handoff-needs-unknown', metric_unknown, ['loss','metrics','unknown'], False, 'run')
metric_points = copy.deepcopy(run)
metric_points['loss']['metrics'].update(submitted=2, handed_off=2)
check('two-cumulative-points-separate-from-measurement-value', metric_points, 'run', True)
for name, path, value in [
    ('unknown-run-field', ['extra'], 'private'),
    ('wrong-run-version', ['schema_version'], 'ashlar.diagnostic.run/0.2'),
    ('wrong-profile', ['profile'], 'auto'),
    ('open-complete', ['state'], 'open'),
    ('complete-with-drop', ['loss', 'local_dropped'], 1),
    ('complete-with-unknown', ['loss', 'local_unknown'], True),
    ('null-hidden-local', ['loss', 'local_attempted'], None),
    ('null-hidden-export', ['loss', 'logs', 'submitted'], None),
    ('unknown-loss-field', ['loss', 'raw_exception'], 'private'),
    ('free-export-state', ['loss', 'logs', 'flush'], 'delivered-exactly-once'),
    ('negative-export', ['loss', 'spans', 'dropped'], -1),
    ('huge-counter', ['loss', 'local_attempted'], 9007199254740992),
    ('unsafe-segment', ['segments', 0, 'source'], '../events.jsonl'),
    ('segment-newline', ['segments', 0, 'source'], 'events-0000.jsonl\n'),
    ('sha-newline', ['segments', 0, 'sha256'], '1'*64+'\n'),
    ('overbyte-segment', ['segments', 0, 'bytes'], 4194305),
    ('oversized-queue', ['limits', 'max_queue_bytes'], 524289),
    ('boolean-limit', ['limits', 'max_emissions'], True),
    ('unbounded-retention', ['limits', 'retention_seconds'], 604801),
    ('sampling-change', ['sampling', 'logs'], 'sampled'),
    ('unknown-origin-key', ['configuration_origins', 'secret_value'], 'raw'),
    ('unknown-origin-value', ['configuration_origins', 'endpoint'], 'automatic-discovery'),
]:
    mutate(name, run, path, value, 'run')
for field in ('ended_unix_nano', 'expires_unix_nano'):
    obj = copy.deepcopy(run)
    del obj[field]
    check('closed-missing-' + field, obj, 'run', False)

# Shape deliberately cannot prove clocks, SDK provenance, UTF-8 byte accounting,
# equalities between records or runtime cleanup. Document that boundary explicitly.
for field in ('attempts', 'segments'):
    obj = copy.deepcopy(run)
    obj[field] *= 2
    check('exact-duplicate-' + field, obj, 'run', False)
assert len(raw_event) <= limits['max_event_bytes']
assert not retrievals
paths = [CONTRACT, BASE/'diagnostic-event-v0.1.schema.json', BASE/'diagnostic-run-v0.1.schema.json']
receipt = {
    'format': 'ashlar-diagnostics-contract-shape-check/0.1',
    'scope': 'Author shape validation only; no SDK, receiver, native engine, runtime implementation or semantic assurance claim.',
    'python': platform.python_version(),
    'packages': {name: importlib.metadata.version(name) for name in ['jsonschema', 'referencing']},
    'sources': {str(p.relative_to(ROOT)): {'bytes': p.stat().st_size, 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in paths},
    'schema_draft': '2020-12', 'external_retrieval_count': len(retrievals),
    'checks': checks, 'passed': len(checks),
    'semantic_checks_not_claimed': ['exact uint64 upper bound', 'actual SDK context/resource provenance', 'capture/queue UTF-8 byte accounting', 'attempt/source identity uniqueness across different objects', 'cross-record sequence and counters', 'closing snapshot and filesystem safety', 'privacy of admitted metadata provenance', 'bounded cancellation and receiver behavior'],
}
out=Path('/private/tmp/ashlar-diagnostics-contract-shape-check-20261010-b.json')
out.write_text(json.dumps(receipt, indent=2)+'\n')
print(json.dumps({'passed':len(checks),'receipt':str(out),'sha256':hashlib.sha256(out.read_bytes()).hexdigest()}))
