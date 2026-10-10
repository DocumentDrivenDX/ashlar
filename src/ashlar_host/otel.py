"""Explicit C006 signal composition and owned isolated-worker supervision.

No SDK provider is constructed in this process. Process termination/reaping
relies on POSIX scheduling and signal semantics; transport-specific qualification
remains separate from this supervision boundary.
"""
from contextlib import contextmanager
from dataclasses import asdict
from importlib import metadata
import json
import math
import os
import selectors
import signal
import struct
import subprocess
import sys
import threading
import time
from typing import Optional

from .config import DiagnosticsConfig, with_diagnostics_transport
from .diagnostics import (DiagnosticSignalSink, DiagnosticsError, decode_event,
                          validate_signal_loss)


FRAME_LIMIT = 65536


def require(value: bool) -> None:
    if not value:
        raise DiagnosticsError('diagnostics-configuration')


def unknown_loss() -> dict:
    return {name: {'submitted': None, 'handed_off': None, 'dropped': None,
                   'unknown': True, 'flush': 'failed'} for name in ('logs', 'spans', 'metrics')}


class OtelRun:
    """One run's trusted signal sink; application composition owns its closure."""
    def __init__(self, config: DiagnosticsConfig):
        self._process = None
        self._lock = threading.Lock()
        self._cleanup_lock = threading.Lock()
        self._local = threading.local()
        self._closed = False
        self._group_termination_attempted = False
        try:
            require(type(config) is DiagnosticsConfig and os.name == 'posix'
                    and signal.getsignal(signal.SIGCHLD) == signal.SIG_DFL
                    and sys.version_info[:2] == (3, 11))
            self._config = config
            version = metadata.version('ashlar-graph-toolkit')
            # Startup precedes work effects; shutdown's selected budget governs
            # closure only. Construction has its own finite two-second ceiling.
            deadline = time.monotonic() + 2
            self._process = subprocess.Popen(
                [sys.executable, '-I', '-B', '-m', 'ashlar_host._otel_worker'],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL, env={'PATH': '/usr/bin:/bin'},
                start_new_session=True, close_fds=True, bufsize=0)
            for stream in (self._process.stdin, self._process.stdout):
                os.set_blocking(stream.fileno(), False)
            def initialize(endpoint, headers, tls, ca_file):
                return self._exchange({'op': 'init', 'settings': {
                    'endpoint': endpoint, 'headers': [list(pair) for pair in headers],
                    'tls': tls, 'ca_file': str(ca_file) if ca_file is not None else None,
                    'environment': config.environment, 'limits': asdict(config.limits),
                    'service_version': version}}, deadline)
            require(with_diagnostics_transport(config, initialize) == 'ready')
            self._sink = DiagnosticSignalSink(self.emit, self.trace_context, self.shutdown)
        except BaseException as primary:
            self._dispose(primary=primary)
            if isinstance(primary, Exception):
                raise DiagnosticsError('diagnostics-configuration') from None
            raise

    @property
    def sink(self) -> DiagnosticSignalSink:
        return self._sink

    def _dispose(self, deadline: Optional[float] = None,
                primary: Optional[BaseException] = None) -> None:
        """Terminate and reap only this new session; never wait for remote export."""
        self._closed = True
        process = self._process
        if process is None:
            return
        end = deadline if deadline is not None else time.monotonic() + 2
        try:
            acquired = self._cleanup_lock.acquire(timeout=max(0, end-time.monotonic()))
        except BaseException as cleanup:
            if primary is not None and not isinstance(primary, Exception):
                try:
                    primary.cleanup_failed = True
                except BaseException:
                    pass
                return
            raise
        if not acquired:
            if primary is not None:
                try:
                    primary.cleanup_failed = True
                except BaseException:
                    pass
                return
            raise DiagnosticsError('diagnostics-configuration')
        try:
            self._dispose_owned(process, end, primary)
        finally:
            self._cleanup_lock.release()

    def _dispose_owned(self, process, end: float,
                       primary: Optional[BaseException]) -> None:
        """Cleanup lock covers group action through reaping, including failures."""
        failure = None
        def terminate():
            # Do not reap the leader before this one-shot group action: its
            # unreaped PID protects the session identity from numeric reuse.
            if not self._group_termination_attempted:
                self._group_termination_attempted = True
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        actions = [terminate]
        actions.extend(stream.close for stream in (process.stdin, process.stdout)
                       if stream is not None)
        actions.append(lambda: process.wait(timeout=max(0, end - time.monotonic())))
        for action in actions:
            try:
                action()
            except BaseException as exc:
                if failure is None or (isinstance(failure, Exception)
                                       and not isinstance(exc, Exception)):
                    failure = exc
        if failure is not None:
            if (not isinstance(failure, Exception)
                    and (primary is None or isinstance(primary, Exception))):
                raise failure
            if primary is not None:
                try:
                    primary.cleanup_failed = True
                except BaseException:
                    pass
            elif isinstance(failure, Exception):
                raise DiagnosticsError('diagnostics-configuration') from None
            else:
                raise failure

    def _exchange(self, request: dict, deadline: float):
        raw = json.dumps(request, ensure_ascii=True, separators=(',', ':'),
                         allow_nan=False).encode('utf8')
        require(0 < len(raw) <= FRAME_LIMIT and not self._closed)
        process = self._process
        require(process is not None and process.returncode is None)
        def transfer(stream, size, outgoing=None):
            result = bytearray()
            offset = 0
            selector = selectors.DefaultSelector()
            primary = None
            try:
                selector.register(stream, selectors.EVENT_WRITE if outgoing is not None
                                  else selectors.EVENT_READ)
                while offset < size:
                    remaining = deadline - time.monotonic()
                    require(remaining > 0)
                    require(bool(selector.select(remaining)))
                    try:
                        if outgoing is not None:
                            count = os.write(stream.fileno(), outgoing[offset:])
                        else:
                            part = os.read(stream.fileno(), size - offset)
                            count = len(part)
                            result.extend(part)
                    except BlockingIOError:
                        continue
                    require(count > 0)
                    offset += count
            except BaseException as exc:
                primary = exc
                raise
            finally:
                try:
                    selector.close()
                except BaseException as cleanup:
                    if primary is None or (isinstance(primary, Exception)
                                           and not isinstance(cleanup, Exception)):
                        raise
                    try:
                        primary.cleanup_failed = True
                    except BaseException:
                        pass
            return bytes(result)
        payload = struct.pack('>I', len(raw)) + raw
        transfer(process.stdin, len(payload), payload)
        close_deadline = deadline
        active_transport = False
        # At most two log/span queues and 20 metric series, begin/end per unit.
        progress_limit = 2 * (2 * self._config.limits.max_queue_records + 20) + 1
        def pairs(items):
            result = {}
            for key, value in items:
                require(key not in result)
                result[key] = value
            return result
        def constant(token):
            raise DiagnosticsError('diagnostics-configuration')
        for _ in range(progress_limit):
            count = struct.unpack('>I', transfer(process.stdout, 4))[0]
            require(0 < count <= FRAME_LIMIT)
            response = json.loads(transfer(process.stdout, count),
                                  object_pairs_hook=pairs, parse_constant=constant)
            require(type(response) is dict and set(response) == {'ok', 'value'}
                    and response['ok'] is True and time.monotonic() <= deadline)
            value = response['value']
            if request['op'] == 'close' and type(value) is dict:
                if set(value) == {'transport_deadline'}:
                    end = value['transport_deadline']
                    require(not active_transport and type(end) in (int, float)
                            and math.isfinite(end) and end <= close_deadline
                            and end <= time.monotonic()
                            + self._config.limits.export_timeout_ms / 1000)
                    deadline = end
                    active_transport = True
                    continue
                if set(value) == {'transport_complete'}:
                    require(active_transport and value['transport_complete'] is True)
                    deadline = close_deadline
                    active_transport = False
                    continue
            require(not active_transport)
            return value
        raise DiagnosticsError('diagnostics-configuration')

    def _invoke(self, request: dict):
        require(self._lock.acquire(blocking=False))
        try:
            return self._exchange(request, time.monotonic()
                                 + self._config.limits.export_timeout_ms / 1000)
        except BaseException as primary:
            self._dispose(primary=primary)
            if isinstance(primary, Exception):
                raise DiagnosticsError('diagnostics-configuration') from None
            raise
        finally:
            self._lock.release()

    @contextmanager
    def operation_context(self, parent_context=None, retry_link=None):
        """Bind explicit SDK SpanContexts to this thread; never read ambient context."""
        require(parent_context is None or retry_link is None)
        def wire(value):
            if value is None:
                return None
            from opentelemetry.trace import SpanContext
            require(type(value) is SpanContext and value.is_valid)
            return {'trace_id': format(value.trace_id, '032x'),
                    'span_id': format(value.span_id, '016x'),
                    'trace_flags': int(value.trace_flags), 'is_remote': value.is_remote}
        previous = getattr(self._local, 'context', (None, None))
        self._local.context = (wire(parent_context), wire(retry_link))
        try:
            yield
        finally:
            self._local.context = previous

    def trace_context(self, attempt: str, operation: str, event: str) -> Optional[dict]:
        parent, retry = (getattr(self._local, 'context', (None, None))
                         if event == 'ashlar.operation.started' else (None, None))
        value = self._invoke({'op': 'context', 'attempt_id': attempt,
                             'operation': operation, 'event_name': event,
                             'parent': parent, 'retry_link': retry})
        require(value is None or (type(value) is dict
                and set(value) == {'trace_id', 'span_id', 'trace_flags'}))
        return value

    def emit(self, raw: bytes) -> None:
        decode_event(raw)
        require(self._invoke({'op': 'emit', 'event': raw.decode('utf8')}) is None)

    def shutdown(self, deadline: float) -> dict:
        if self._closed or not self._lock.acquire(blocking=False):
            self._dispose(deadline)
            return unknown_loss()
        primary = None
        try:
            value = self._exchange({'op': 'close', 'deadline': deadline}, deadline)
            require(type(value) is dict and set(value) == {'logs', 'spans', 'metrics'})
            for item in value.values():
                validate_signal_loss(item)
            return value
        except Exception:
            return unknown_loss()
        except BaseException as exc:
            primary = exc
            raise
        finally:
            try:
                self._dispose(deadline, primary)
            finally:
                self._lock.release()


def make_otel_run(config: Optional[DiagnosticsConfig]) -> Optional[OtelRun]:
    """None disables SDK discovery and subprocess construction."""
    return None if config is None else OtelRun(config)
