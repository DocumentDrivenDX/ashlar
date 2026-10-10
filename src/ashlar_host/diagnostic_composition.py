"""Explicit diagnostic ownership; no native, publication or ACK capabilities."""
from contextlib import contextmanager
from importlib import metadata
import os
import signal
import sys
import time

from .config import DiagnosticsConfig, HostError
from .diagnostics import DiagnosticRun, DiagnosticSignalSink
from .otel import OtelRun, OtelStartupError, unknown_loss
from .otel_profile import SDK_VERSIONS


def preflight(config):
    """Inspect selected installed metadata before local capture or startup."""
    try:
        if (type(config) is not DiagnosticsConfig or sys.version_info[:2] != (3, 11)
                or os.name != 'posix' or signal.getsignal(signal.SIGCHLD) != signal.SIG_DFL):
            raise ValueError()
        metadata.version('ashlar-graph-toolkit')
        for name, version in SDK_VERSIONS.items():
            if metadata.version(name) != version:
                raise ValueError()
    except Exception:
        raise HostError('diagnostics-configuration') from None


class DelegatingSink:
    """Retain local observation after an ordinary remote ownership failure."""
    def __init__(self):
        self.owner = None
        self.failed = False
        self.closed = False
        self.close_deadline = None

    def context(self, attempt, operation, event):
        if self.failed or self.owner is None:
            return None
        try:
            return self.owner.sink.trace_context(attempt, operation, event)
        except Exception:
            self.failed = True
            return None

    def emit(self, raw):
        if not self.failed and self.owner is not None:
            try:
                return self.owner.sink.emit(raw)
            except Exception:
                self.failed = True
        return None

    def shutdown(self, deadline):
        if self.closed:
            return unknown_loss()
        self.closed = True
        result = unknown_loss()
        if self.close_deadline is not None:
            deadline = min(deadline, self.close_deadline)
        if self.owner is not None:
            result = self.owner.sink.shutdown(deadline)
        return unknown_loss() if self.failed or self.owner is None else result

    def port(self):
        return DiagnosticSignalSink(self.emit, self.context, self.shutdown)


def choose(primary, cleanup):
    """A first cancellation wins diagnostic failures without mutating business state."""
    if primary is None or (isinstance(primary, Exception) and not isinstance(cleanup, Exception)):
        return cleanup
    return primary


@contextmanager
def diagnostic_run(config=None):
    """Own one explicit run and optional remote worker across caller work."""
    if config is None:
        yield None
        return
    preflight(config)
    delegate = DelegatingSink()
    run = None
    primary = None
    try:
        run = DiagnosticRun(config, delegate.port())
        try:
            delegate.owner = OtelRun(config)
        except Exception as error:
            # Only the trusted facade's public closure receipt admits fallback.
            # Unqualified constructor errors cannot prove an owned worker ended.
            if (type(error) is not OtelStartupError or error.cleanup_complete is not True
                    or error.dependency_admitted is not True):
                raise HostError('diagnostics-configuration') from None
            delegate.failed = True
        yield run
    except BaseException as error:
        primary = error
    finally:
        delegate.close_deadline = time.monotonic() + config.limits.shutdown_timeout_ms / 1000
        if run is not None:
            try:
                run.close()
            except BaseException as error:
                if not isinstance(error, Exception):
                    primary = choose(primary, error)
        # If local closure did not reach its sink, still close owned transport.
        if not delegate.closed and delegate.owner is not None:
            try:
                delegate.shutdown(delegate.close_deadline)
            except BaseException as error:
                if not isinstance(error, Exception):
                    primary = choose(primary, error)
    if primary is not None:
        if run is None and isinstance(primary, Exception):
            raise HostError('diagnostics-configuration') from None
        raise primary
