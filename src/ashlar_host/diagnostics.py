"""CONTRACT-006 sanitized capture/retrieval, independent of SDK and work authority.

Injected signal ports must honor their total monotonic deadline and return only
closed logical-unit accounting. This owner neither proves SDK boundedness nor
receiver delivery. Filesystem custody assumes cooperating private parent writers;
there is no fsync, power-loss, abrupt-process-death or hostile-writer guarantee.
"""
from dataclasses import asdict, dataclass
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import re
import stat
import sys
import threading
import time
from typing import Callable, Optional
from uuid import uuid4

from .config import DiagnosticsConfig, DiagnosticsLimits


class DiagnosticsError(ValueError):
    """Fixed payload-free diagnostic refusal."""


OPERATIONS = ('publication', 'source-admission', 'held-read', 'ack-reconciliation')
PHASES = ('configuration', 'admission', 'prepare', 'apply', 'commit', 'ack',
          'guard', 'capture', 'closing', 'cleanup')
STATES = ('started', 'completed', 'uncertain', 'failed')
OUTCOMES = ('succeeded', 'refused', 'failed', 'cancelled', 'uncertain')
CATEGORIES = ('configuration', 'source', 'compiler', 'guard', 'capture', 'decoder',
              'closing', 'cleanup', 'authority', 'io', 'internal')
CATALOG = {'ashlar.operation.started': 'Operation started',
           'ashlar.operation.phase': 'Operation phase observed',
           'ashlar.operation.finished': 'Operation finished'}
SCOPE = {'name': 'ashlar.host.diagnostics', 'version': '0.1.0'}
MAX_COUNT = 9007199254740991
MAX_TIME = 18446744073709551615


def _require(value: bool, code: str = 'diagnostics-snapshot-invalid') -> None:
    if not value:
        raise DiagnosticsError(code)


def _keys(value: object, required: set, optional: set = frozenset()) -> None:
    _require(type(value) is dict and required <= set(value) <= required | optional)


def _id(value: object) -> bool:
    return type(value) is str and re.fullmatch('[0-9a-f]{32}', value) is not None


def _nano(value: object) -> int:
    _require(type(value) is str and re.fullmatch('[1-9][0-9]{0,19}', value) is not None)
    number = int(value)
    _require(number <= MAX_TIME)
    return number


def _mark(primary: BaseException) -> None:
    try:
        primary.cleanup_failed = True
    except BaseException:
        pass


def _encoded(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=True, separators=(',', ':'),
                       allow_nan=False) + '\n').encode('utf8')


def _decode(raw: bytes) -> dict:
    def integer(token: str) -> int:
        _require(len(token.lstrip('-')) <= 20)
        return int(token)
    def refuse(token: str) -> None:
        raise DiagnosticsError('diagnostics-snapshot-invalid')
    def pairs(items: list) -> dict:
        result = {}
        for key, value in items:
            _require(key not in result)
            result[key] = value
        return result
    # Preflight nesting before JSON allocation, respecting quoted escapes.
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
            _require(depth <= 12)
        elif byte in (93, 125):
            depth -= 1
            _require(depth >= 0)
    try:
        result = json.loads(raw.decode('utf8'), object_pairs_hook=pairs,
                            parse_int=integer, parse_float=refuse, parse_constant=refuse)
    except (ValueError, UnicodeError, RecursionError):
        raise DiagnosticsError('diagnostics-snapshot-invalid') from None
    _require(type(result) is dict)
    return result


def _read(path: Path, limit: int) -> tuple:
    fd = None
    primary = None
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        before = os.fstat(fd)
        _require(stat.S_ISREG(before.st_mode) and stat.S_IMODE(before.st_mode) == 0o600
                 and before.st_size <= limit)
        raw = bytearray()
        while len(raw) <= limit:
            part = os.read(fd, min(65536, limit + 1 - len(raw)))
            if not part:
                break
            raw.extend(part)
        after = os.fstat(fd)
        current = path.lstat()
        fingerprint = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns,
                                 s.st_ctime_ns, s.st_mode)
        _require(len(raw) <= limit and fingerprint(before) == fingerprint(after)
                 == fingerprint(current) and len(raw) == before.st_size)
        return bytes(raw), fingerprint(before)
    except BaseException as exc:
        primary = exc
        raise
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except BaseException:
                if primary is None:
                    raise
                _mark(primary)


def _write(path: Path, raw: bytes) -> None:
    fd = None
    primary = None
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
                     | os.O_NONBLOCK, 0o600)
        os.fchmod(fd, 0o600)
        offset = 0
        while offset < len(raw):
            written = os.write(fd, raw[offset:])
            _require(written > 0)
            offset += written
    except BaseException as exc:
        primary = exc
        raise
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except BaseException:
                if primary is None:
                    raise
                _mark(primary)


def _resource(resource: dict) -> None:
    _keys(resource, {'service.name', 'service.version', 'deployment.environment.name',
                     'telemetry.sdk.name', 'telemetry.sdk.language', 'telemetry.sdk.version'})
    _require(resource['service.name'] == 'ashlar-host'
             and resource['telemetry.sdk.name'] == 'opentelemetry'
             and resource['telemetry.sdk.language'] == 'python'
             and resource['telemetry.sdk.version'] == '1.45.1'
             and resource['deployment.environment.name'] in ('development', 'test', 'staging', 'production'))
    version = resource['service.version']
    _require(type(version) is str and len(version) <= 64
             and re.fullmatch('[A-Za-z0-9][A-Za-z0-9.+_-]*', version) is not None)


def _trace(trace: dict) -> None:
    _keys(trace, {'trace_id', 'span_id', 'trace_flags'})
    _require(_id(trace['trace_id']) and trace['trace_id'] != '0' * 32
             and type(trace['span_id']) is str
             and re.fullmatch('[0-9a-f]{16}', trace['span_id']) is not None
             and trace['span_id'] != '0' * 16
             and type(trace['trace_flags']) is int and 0 <= trace['trace_flags'] <= 255)


def validate_event(event: dict) -> None:
    """Validate the closed sanitized projection, never infer an SDK context."""
    _keys(event, {'schema_version', 'observed_timestamp_unix_nano', 'severity_text',
                  'severity_number', 'event_name', 'body', 'resource', 'scope', 'attributes'},
          {'timestamp_unix_nano', 'trace'})
    _require(event['schema_version'] == 'ashlar.diagnostic.event/0.1'
             and type(event['event_name']) is str and event['event_name'] in CATALOG
             and event['body'] == CATALOG[event['event_name']]
             and event['scope'] == SCOPE)
    _nano(event['observed_timestamp_unix_nano'])
    if 'timestamp_unix_nano' in event:
        _nano(event['timestamp_unix_nano'])
    if 'trace' in event:
        _trace(event['trace'])
    _resource(event['resource'])
    base = {'ashlar.run.id', 'ashlar.attempt.id', 'ashlar.emitter.id',
            'ashlar.sequence', 'ashlar.operation'}
    attributes = event['attributes']
    name = event['event_name']
    extra = (set() if name.endswith('started') else
             {'ashlar.phase', 'ashlar.phase.state'} if name.endswith('phase') else
             {'ashlar.outcome', 'ashlar.cleanup_failed'})
    optional = {'ashlar.error.category'} if name.endswith('finished') else set()
    _keys(attributes, base | extra, optional)
    _require(_id(attributes['ashlar.run.id']) and _id(attributes['ashlar.attempt.id'])
             and attributes['ashlar.emitter.id'] == 'host'
             and type(attributes['ashlar.sequence']) is int
             and 1 <= attributes['ashlar.sequence'] <= 10000
             and type(attributes['ashlar.operation']) is str
             and attributes['ashlar.operation'] in OPERATIONS)
    severity = 9
    if name.endswith('phase'):
        _require(attributes['ashlar.phase'] in PHASES and attributes['ashlar.phase.state'] in STATES)
    if name.endswith('finished'):
        outcome = attributes['ashlar.outcome']
        _require(outcome in OUTCOMES and type(attributes['ashlar.cleanup_failed']) is bool)
        if outcome == 'succeeded':
            _require('ashlar.error.category' not in attributes and not attributes['ashlar.cleanup_failed'])
        else:
            _require(attributes.get('ashlar.error.category') in CATEGORIES)
            severity = 17 if outcome == 'failed' else 13
    _require(type(event['severity_number']) is int and event['severity_number'] == severity
             and event['severity_text'] == {9: 'INFO', 13: 'WARN', 17: 'ERROR'}[severity])


def _export(value: dict, closed: bool = True) -> None:
    _keys(value, {'submitted', 'handed_off', 'dropped', 'unknown', 'flush'})
    _require(type(value['unknown']) is bool and value['flush'] in
             ('not_attempted', 'complete', 'incomplete', 'failed'))
    for key in ('submitted', 'handed_off', 'dropped'):
        _require(value[key] is None or type(value[key]) is int and 0 <= value[key] <= MAX_COUNT)
    if any(value[k] is None for k in ('submitted', 'handed_off', 'dropped')):
        _require(value['unknown'])
    else:
        total = value['handed_off'] + value['dropped']
        _require(total <= value['submitted'])
        if closed and not value['unknown']:
            _require(total == value['submitted'])


def _private_directory(path: Path) -> None:
    _require(isinstance(path, Path) and path.is_absolute())
    for ancestor in reversed((path,) + tuple(path.parents)):
        info = ancestor.lstat()
        _require(stat.S_ISDIR(info.st_mode))
    _require(stat.S_IMODE(path.lstat().st_mode) == 0o700)


@dataclass(frozen=True)
class DiagnosticSignalSink:
    """Trusted injected SDK port; immutable bytes, bounded shutdown, no discovery.

    trace_context returns actual SDK context or None before local/log admission.
    shutdown returns closed logs/spans/metrics logical-unit loss maps and must
    honor the supplied absolute monotonic deadline without surviving workers.
    """
    emit: Callable[[bytes], None]
    trace_context: Callable[[str, str, str], Optional[dict]]
    shutdown: Callable[[float], dict]

    def __post_init__(self) -> None:
        _require(all(callable(p) for p in (self.emit, self.trace_context, self.shutdown)),
                 'diagnostics-configuration')


@dataclass(frozen=True)
class DiagnosticAttempt:
    attempt_id: str
    operation: str


class DiagnosticRun:
    """Own one private process-local run; diagnostics never authorizes work."""
    def __init__(self, config: DiagnosticsConfig, sink: DiagnosticSignalSink):
        try:
            _require(type(config) is DiagnosticsConfig and type(sink) is DiagnosticSignalSink,
                     'diagnostics-configuration')
            try:
                version = metadata.version('ashlar-graph-toolkit')
            except Exception:
                raise DiagnosticsError('diagnostics-configuration') from None
            self.resource = {'service.name': 'ashlar-host', 'service.version': version,
                             'deployment.environment.name': config.environment,
                             'telemetry.sdk.name': 'opentelemetry', 'telemetry.sdk.language': 'python',
                             'telemetry.sdk.version': '1.45.1'}
            _resource(self.resource)
            self.config, self.sink = config, sink
            self.run_id = uuid4().hex
            self.directory = config.capture_root / self.run_id
            _private_directory(config.capture_root)
            self.directory.mkdir(mode=0o700, exist_ok=False)
            self.pid = os.getpid()
            self.lock = threading.RLock()
            self.attempts = {}
            self.finished = set()
            self.sequence = self.written = self.dropped = self.used = 0
            self.segments = []
            self.active = None
            self.closed = False
            self.closing = False
            self.notice_sent = False
            self.started = str(time.time_ns())
            self.loss = {key: {'submitted': 0, 'handed_off': 0, 'dropped': 0,
                              'unknown': False, 'flush': 'not_attempted'}
                         for key in ('logs', 'spans', 'metrics')}
            self.manifest = {'schema_version': 'ashlar.diagnostic.run/0.1',
                             'profile': config.profile, 'run_id': self.run_id, 'state': 'open',
                             'started_unix_nano': self.started, 'resource': self.resource.copy(),
                             'scope': SCOPE.copy(), 'attempts': [], 'limits': asdict(config.limits),
                             'configuration_origins': dict(config.origins),
                             'sampling': {'logs': 'all', 'new_spans': 'always_on', 'metrics': 'all'},
                             'segments': [], 'loss': self._loss(), 'complete': False}
            raw = _encoded(self.manifest)
            _require(len(raw) <= 65536)
            _write(self.directory / 'run.json', raw)
        except Exception:
            raise DiagnosticsError('diagnostics-configuration') from None

    def _loss(self) -> dict:
        return dict(local_attempted=self.sequence if self.sequence <= MAX_COUNT else None, local_written=self.written,
                    local_dropped=self.dropped if self.dropped <= MAX_COUNT else None, local_unknown=self.sequence > MAX_COUNT,
                    **{k: dict(v) for k, v in self.loss.items()})

    def _event_guard(self, condition: bool, code: str = 'diagnostics-event-refused') -> None:
        if not condition:
            self.sequence += 1
            self.dropped += 1
            raise DiagnosticsError(code)

    def _owned(self) -> None:
        _require(not self.closed and not self.closing and self.pid == os.getpid(), 'diagnostics-run-closed')

    def begin_attempt(self, operation: str) -> DiagnosticAttempt:
        with self.lock:
            self._owned()
            self._event_guard(type(operation) is str and operation in OPERATIONS, 'diagnostics-event-refused')
            self._event_guard(len(self.attempts) < self.config.limits.max_attempts, 'diagnostics-attempt-limit')
            attempt = DiagnosticAttempt(uuid4().hex, operation)
            self.attempts[attempt.attempt_id] = attempt
            self._emit(attempt, 'ashlar.operation.started', {})
            return attempt

    def phase(self, attempt: DiagnosticAttempt, phase: str, state: str) -> None:
        with self.lock:
            self._owned()
            self._attempt(attempt)
            self._event_guard(type(phase) is str and phase in PHASES and type(state) is str
                     and state in STATES, 'diagnostics-event-refused')
            self._emit(attempt, 'ashlar.operation.phase', {'ashlar.phase': phase, 'ashlar.phase.state': state})

    def finish_attempt(self, attempt: DiagnosticAttempt, outcome: str,
                       error_category: Optional[str] = None, cleanup_failed: bool = False) -> None:
        with self.lock:
            self._owned()
            self._attempt(attempt)
            self._event_guard(type(outcome) is str and outcome in OUTCOMES
                     and type(cleanup_failed) is bool, 'diagnostics-event-refused')
            self._event_guard((outcome == 'succeeded' and error_category is None and not cleanup_failed)
                     or (outcome != 'succeeded' and type(error_category) is str
                         and error_category in CATEGORIES), 'diagnostics-event-refused')
            attributes = {'ashlar.outcome': outcome, 'ashlar.cleanup_failed': cleanup_failed}
            if error_category is not None:
                attributes['ashlar.error.category'] = error_category
            self.finished.add(attempt.attempt_id)
            self._emit(attempt, 'ashlar.operation.finished', attributes)

    def _attempt(self, attempt: DiagnosticAttempt) -> None:
        self._event_guard(type(attempt) is DiagnosticAttempt
                 and self.attempts.get(attempt.attempt_id) is attempt
                 and attempt.attempt_id not in self.finished, 'diagnostics-event-refused')

    def _emit(self, attempt: DiagnosticAttempt, name: str, extra: dict) -> None:
        self.sequence += 1
        if self.sequence > self.config.limits.max_emissions:
            self.dropped += 1
            return
        attributes = {'ashlar.run.id': self.run_id, 'ashlar.attempt.id': attempt.attempt_id,
                      'ashlar.emitter.id': 'host', 'ashlar.sequence': self.sequence,
                      'ashlar.operation': attempt.operation, **extra}
        severity = 9 if name != 'ashlar.operation.finished' or extra['ashlar.outcome'] == 'succeeded' else (17 if extra['ashlar.outcome'] == 'failed' else 13)
        event = {'schema_version': 'ashlar.diagnostic.event/0.1',
                 'observed_timestamp_unix_nano': str(time.time_ns()),
                 'severity_text': {9: 'INFO', 13: 'WARN', 17: 'ERROR'}[severity],
                 'severity_number': severity, 'event_name': name, 'body': CATALOG[name],
                 'resource': self.resource.copy(), 'scope': SCOPE.copy(), 'attributes': attributes}
        try:
            trace = self.sink.trace_context(attempt.attempt_id, attempt.operation, name)
            if trace is not None:
                trace = dict(trace)
                _trace(trace)
                event['trace'] = trace
            validate_event(event)
            raw = _encoded(event)
            _require(len(raw) <= self.config.limits.max_event_bytes, 'diagnostics-event-refused')
        except Exception:
            self.dropped += 1
            return
        except BaseException:
            self.dropped += 1
            raise
        self._capture(raw)
        try:
            print(CATALOG[name], file=sys.stderr)
        except Exception:
            self._notice()
        try:
            _require(self.sink.emit(raw) is None, 'diagnostics-sink-refused')
        except Exception:
            # Port cannot prove a unit never entered transmission. Unknown is honest.
            self.loss = {k: {'submitted': None, 'handed_off': None, 'dropped': None,
                             'unknown': True, 'flush': 'not_attempted'} for k in self.loss}
        except BaseException:
            raise

    def _notice(self) -> None:
        if not self.notice_sent:
            self.notice_sent = True
            try:
                print('ashlar diagnostics incomplete', file=sys.stderr)
            except Exception:
                pass

    def _invalidate(self, primary: Optional[BaseException] = None) -> None:
        if self.active is None:
            return
        segment = self.active
        self.active = None
        self.written -= segment['records']
        self.dropped += segment['records']
        try:
            if segment['fd'] >= 0:
                fd = segment['fd']
                segment['fd'] = -1
                os.close(fd)
        except BaseException as exc:
            if primary is not None:
                _mark(primary)
            elif not isinstance(exc, Exception):
                raise
        try:
            current = segment['path'].lstat()
            if (current.st_dev, current.st_ino) == segment['identity']:
                segment['path'].unlink()
        except BaseException as exc:
            if primary is not None:
                _mark(primary)
            elif not isinstance(exc, Exception):
                raise

    def _seal(self) -> None:
        if self.active is None:
            return
        segment = self.active
        try:
            if segment['fd'] >= 0:
                fd = segment['fd']
                segment['fd'] = -1
                os.close(fd)
            raw, _ = _read(segment['path'], self.config.limits.max_segment_bytes)
            _require(len(raw) == segment['bytes'] and raw.endswith(b'\n'))
            self.segments.append({'source': segment['path'].name, 'bytes': len(raw),
                                  'sha256': hashlib.sha256(raw).hexdigest(),
                                  'records': segment['records']})
            self.active = None
        except BaseException as exc:
            self._invalidate(exc)
            raise

    def _capture(self, raw: bytes) -> None:
        limits = self.config.limits
        if self.used + len(raw) > limits.max_capture_bytes:
            self.dropped += 1
            return
        try:
            if self.active is not None and self.active['bytes'] + len(raw) > limits.max_segment_bytes:
                self._seal()
            if self.active is None:
                if len(self.segments) == 4:
                    self.dropped += 1
                    return
                path = self.directory / ('events-%04d.jsonl' % len(self.segments))
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
                self.active = {'path': path, 'fd': fd, 'identity': None,
                               'records': 0, 'bytes': 0}
                os.fchmod(fd, 0o600)
                info = os.fstat(fd)
                self.active['identity'] = (info.st_dev, info.st_ino)
            self.used += len(raw)  # includes partial/abandoned bytes, never recycled
            offset = 0
            while offset < len(raw):
                n = os.write(self.active['fd'], raw[offset:])
                _require(n > 0)
                offset += n
            self.active['bytes'] += len(raw)
            self.active['records'] += 1
            self.written += 1
        except BaseException as exc:
            self.dropped += 1
            self._invalidate(exc)
            if not isinstance(exc, Exception):
                raise

    def close(self, primary: Optional[BaseException] = None) -> Optional[dict]:
        """Close local resources and attempt one bounded sink drain; preserve primary."""
        with self.lock:
            self._owned()
            self.closing = True
            cancellation = primary
            deadline = time.monotonic() + self.config.limits.shutdown_timeout_ms / 1000
            try:
                self._seal()
            except BaseException as exc:
                if cancellation is not None:
                    _mark(cancellation)
                elif not isinstance(exc, Exception):
                    cancellation = exc
            try:
                loss = self.sink.shutdown(deadline)
                _keys(loss, {'logs', 'spans', 'metrics'})
                for value in loss.values():
                    _export(value)
                self.loss = {k: dict(v) for k, v in loss.items()}
                if time.monotonic() > deadline:
                    for value in self.loss.values():
                        value['flush'] = 'incomplete'
            except BaseException as exc:
                self.loss = {k: {'submitted': None, 'handed_off': None, 'dropped': None,
                                 'unknown': True, 'flush': 'failed'} for k in self.loss}
                if cancellation is not None:
                    _mark(cancellation)
                elif not isinstance(exc, Exception):
                    cancellation = exc
            if self.dropped or any(v['unknown'] or v['dropped'] or v['flush'] in ('failed', 'incomplete') for v in self.loss.values()):
                try:
                    self._notice()
                except BaseException as exc:
                    if cancellation is not None:
                        _mark(cancellation)
                    else:
                        cancellation = exc
            ended = time.time_ns()
            manifest = {**self.manifest, 'state': 'closed', 'ended_unix_nano': str(ended),
                        'expires_unix_nano': str(ended + self.config.limits.retention_seconds * 1000000000),
                        'attempts': [asdict(a) for a in self.attempts.values()],
                        'segments': list(self.segments), 'loss': self._loss(),
                        'complete': self.dropped == 0 and self.sequence <= MAX_COUNT}
            published = False
            try:
                validate_manifest(manifest)
                raw = _encoded(manifest)
                _require(len(raw) <= 65536)
                temporary = self.directory / '.run-closed.tmp'
                _write(temporary, raw)
                os.replace(temporary, self.directory / 'run.json')
                published = True
            except BaseException as exc:
                if cancellation is not None:
                    _mark(cancellation)
                elif not isinstance(exc, Exception):
                    cancellation = exc
                try:
                    self._notice()
                except BaseException as notice_error:
                    if cancellation is not None:
                        _mark(cancellation)
                    elif not isinstance(notice_error, Exception):
                        cancellation = notice_error
            self.closed = True
            if cancellation is not None:
                raise cancellation
            return _decode(raw) if published else None


def validate_manifest(manifest: dict) -> None:
    """Closed run shape and cross-counter/time/segment consistency."""
    required = {'schema_version', 'profile', 'run_id', 'state', 'started_unix_nano',
                'resource', 'scope', 'attempts', 'limits', 'configuration_origins',
                'sampling', 'segments', 'loss', 'complete'}
    _keys(manifest, required, {'ended_unix_nano', 'expires_unix_nano'})
    _require(manifest['schema_version'] == 'ashlar.diagnostic.run/0.1'
             and manifest['profile'] == 'ashlar-host-otel-http/0.1'
             and _id(manifest['run_id']) and manifest['state'] in ('open', 'closed')
             and type(manifest['complete']) is bool and manifest['scope'] == SCOPE)
    _resource(manifest['resource'])
    start = _nano(manifest['started_unix_nano'])
    _keys(manifest['limits'], set(DiagnosticsLimits.__dataclass_fields__))
    try:
        limits = DiagnosticsLimits(**manifest['limits'])
    except Exception:
        raise DiagnosticsError('diagnostics-snapshot-invalid') from None
    origin_keys = {'profile', 'capture_root', 'endpoint', 'headers', 'tls', 'ca_file', 'environment'} | {'limits.' + k for k in manifest['limits']}
    _keys(manifest['configuration_origins'], origin_keys)
    _require(all(type(v) is str and v in ('explicit', 'environment-secret', 'development-env',
                  'environment-toml', 'default-toml', 'field-default') for v in manifest['configuration_origins'].values()))
    _require(all(manifest['configuration_origins'][k] in ('explicit', 'environment-secret', 'development-env') for k in ('capture_root', 'endpoint', 'headers', 'tls', 'environment')))
    _require(manifest['sampling'] == {'logs': 'all', 'new_spans': 'always_on', 'metrics': 'all'})
    attempts = manifest['attempts']
    _require(type(attempts) is list and len(attempts) <= limits.max_attempts)
    ids = set()
    for attempt in attempts:
        _keys(attempt, {'attempt_id', 'operation'})
        _require(_id(attempt['attempt_id']) and attempt['attempt_id'] not in ids
                 and attempt['operation'] in OPERATIONS)
        ids.add(attempt['attempt_id'])
    segments = manifest['segments']
    _require(type(segments) is list and len(segments) <= 4)
    names = set()
    total = records = 0
    for segment in segments:
        _keys(segment, {'source', 'bytes', 'sha256', 'records'})
        _require(type(segment['source']) is str and re.fullmatch(r'events-000[0-3]\.jsonl', segment['source']) is not None
                 and segment['source'] not in names and type(segment['bytes']) is int
                 and 1 <= segment['bytes'] <= limits.max_segment_bytes
                 and type(segment['records']) is int and 1 <= segment['records'] <= limits.max_emissions
                 and type(segment['sha256']) is str and re.fullmatch('[0-9a-f]{64}', segment['sha256']) is not None)
        names.add(segment['source'])
        total += segment['bytes']
        records += segment['records']
    _require(total <= limits.max_capture_bytes)
    loss = manifest['loss']
    _keys(loss, {'local_attempted', 'local_written', 'local_dropped', 'local_unknown', 'logs', 'spans', 'metrics'})
    _require(type(loss['local_unknown']) is bool)
    for key in ('local_attempted', 'local_written', 'local_dropped'):
        _require(loss[key] is None or type(loss[key]) is int and 0 <= loss[key] <= MAX_COUNT)
    if any(loss[k] is None for k in ('local_attempted', 'local_written', 'local_dropped')):
        _require(loss['local_unknown'])
    else:
        _require(loss['local_attempted'] == loss['local_written'] + loss['local_dropped'])
    _require(loss['local_written'] == records)
    for key in ('logs', 'spans', 'metrics'):
        _export(loss[key], manifest['state'] == 'closed')
    if manifest['state'] == 'open':
        _require(not manifest['complete'] and not segments
                 and 'ended_unix_nano' not in manifest and 'expires_unix_nano' not in manifest)
    else:
        _require(start <= _nano(manifest.get('ended_unix_nano')))
        _require(_nano(manifest.get('expires_unix_nano')) == int(manifest['ended_unix_nano']) + limits.retention_seconds * 1000000000)
    if manifest['complete']:
        _require(manifest['state'] == 'closed' and loss['local_dropped'] == 0
                 and not loss['local_unknown'])


def read_diagnostics(run_directory: Path, attempt_id: Optional[str] = None,
                     min_severity: int = 9, event_name: Optional[str] = None,
                     limit: int = 50) -> dict:
    """Read a bounded immutable closed snapshot; never tail or authorize work."""
    _require(isinstance(run_directory, Path) and run_directory.is_absolute()
             and (attempt_id is None or _id(attempt_id)) and type(min_severity) is int
             and min_severity in (9, 13, 17) and (event_name is None or type(event_name) is str
             and event_name in CATALOG) and type(limit) is int and 1 <= limit <= 100)
    try:
        if not run_directory.exists():
            raise DiagnosticsError('diagnostics-unavailable')
        _private_directory(run_directory)
        opening_dir = run_directory.stat()
        raw, fingerprint = _read(run_directory / 'run.json', 65536)
        manifest = _decode(raw)
        validate_manifest(manifest)
        _require(manifest['run_id'] == run_directory.name)
        _require(manifest['state'] == 'closed', 'diagnostics-open')
        _require(time.time_ns() < int(manifest['expires_unix_nano']), 'diagnostics-expired')
        allowed = {'run.json'} | {'events-%04d.jsonl' % i for i in range(4)}
        count = stored_bytes = 0
        with os.scandir(run_directory) as iterator:
            for entry in iterator:
                count += 1
                _require(count <= 5 and entry.name in allowed and entry.is_file(follow_symlinks=False))
                info = entry.stat(follow_symlinks=False)
                _require(stat.S_IMODE(info.st_mode) == 0o600)
                if entry.name != 'run.json':
                    _require(not manifest['complete'] or entry.name in {s['source'] for s in manifest['segments']})
                    stored_bytes += info.st_size
                    _require(info.st_size <= manifest['limits']['max_segment_bytes'] and stored_bytes <= manifest['limits']['max_capture_bytes'])
        matches = []
        matched = previous = 0
        started_attempts, finished_attempts, phased_attempts = set(), set(), set()
        attempts = {a['attempt_id']: a['operation'] for a in manifest['attempts']}
        declared = manifest['segments']
        segment_fingerprints = {}
        _require([s['source'] for s in declared] == sorted(s['source'] for s in declared))
        for segment in declared:
            value, before = _read(run_directory / segment['source'], segment['bytes'])
            _require(len(value) == segment['bytes'] and hashlib.sha256(value).hexdigest() == segment['sha256']
                     and value.endswith(b'\n'))
            lines = value.splitlines(keepends=True)
            _require(len(lines) == segment['records'])
            for line, record in enumerate(lines, 1):
                _require(len(record) <= manifest['limits']['max_event_bytes'] and record.endswith(b'\n'))
                event = _decode(record)
                validate_event(event)
                attrs = event['attributes']
                sequence = attrs['ashlar.sequence']
                _require(previous < sequence <= manifest['limits']['max_emissions']
                         and (manifest['loss']['local_attempted'] is None or sequence <= manifest['loss']['local_attempted'])
                         and attrs['ashlar.run.id'] == manifest['run_id']
                         and attempts.get(attrs['ashlar.attempt.id']) == attrs['ashlar.operation']
                         and event['resource'] == manifest['resource'] and event['scope'] == manifest['scope'])
                previous = sequence
                identity = attrs['ashlar.attempt.id']
                if event['event_name'].endswith('started'):
                    _require(identity not in started_attempts and identity not in finished_attempts and identity not in phased_attempts)
                    started_attempts.add(identity)
                elif event['event_name'].endswith('finished'):
                    _require(identity not in finished_attempts)
                    finished_attempts.add(identity)
                else:
                    _require(identity not in finished_attempts)
                    phased_attempts.add(identity)
                if (attempt_id is None or attrs['ashlar.attempt.id'] == attempt_id) and event['severity_number'] >= min_severity and (event_name is None or event['event_name'] == event_name):
                    matched += 1
                    if len(matches) < limit:
                        matches.append({'source': segment['source'], 'line': line, 'event': event})
            closing_value, after = _read(run_directory / segment['source'], segment['bytes'])
            _require(value == closing_value and before == after)
            segment_fingerprints[segment['source']] = before
        for segment in declared:
            final_raw, final_fingerprint = _read(run_directory / segment['source'], segment['bytes'])
            _require(final_fingerprint == segment_fingerprints[segment['source']]
                     and len(final_raw) == segment['bytes']
                     and hashlib.sha256(final_raw).hexdigest() == segment['sha256'])
        closing_raw, closing_fp = _read(run_directory / 'run.json', 65536)
        closing_dir = run_directory.stat()
        _require(raw == closing_raw and fingerprint == closing_fp
                 and (opening_dir.st_dev, opening_dir.st_ino, opening_dir.st_mtime_ns)
                 == (closing_dir.st_dev, closing_dir.st_ino, closing_dir.st_mtime_ns))
        result = {'schema_version': 'ashlar.diagnostic.query/0.1', 'run_id': manifest['run_id'],
                  'manifest_sha256': hashlib.sha256(raw).hexdigest(), 'capture_complete': manifest['complete'],
                  'loss': manifest['loss'], 'matched': matched, 'truncated': matched > len(matches),
                  'records': matches}
        while len(_encoded(result)) > 524288 and result['records']:
            result['records'].pop()
            result['truncated'] = True
        _require(len(_encoded(result)) <= 524288)
        return result
    except DiagnosticsError:
        raise
    except (OSError, ValueError, TypeError, KeyError, UnicodeError):
        raise DiagnosticsError('diagnostics-snapshot-invalid') from None
