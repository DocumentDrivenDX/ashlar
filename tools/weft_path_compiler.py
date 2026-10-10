"""Bounded invocation of the reviewed local path compiler component.

This is a private local build profile, not an indexed distribution or remote
installation authority. The host must hold its filesystem custody; opening and
closing hashes alone cannot protect against an adversarial filesystem replacement.
"""
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import selectors
import signal
import subprocess
import time

SOURCE_COMMIT = 'c6fe2a6e9513f51b3e0c9e653c54e6746c4ed6fc'
BINARY_SHA256 = '8946ada68a7132a2ba6c1abdc1c9a0e904922cb1dce2c500922cb0dd0a96562b'
BINARY_BYTES = 8274496


class PathCompilerError(ValueError):
    def __init__(self, message, *, cleanup_failed=False):
        super().__init__(message)
        self.cleanup_failed = cleanup_failed


@dataclass(frozen=True)
class PathCompilerConfig:
    binary: Path
    maximum_request_bytes: int
    maximum_response_bytes: int
    timeout_seconds: int

    def __post_init__(self):
        if not isinstance(self.binary, Path) or not self.binary.is_absolute():
            raise PathCompilerError('Explicit absolute compiler path required')
        for limit in (self.maximum_request_bytes, self.maximum_response_bytes):
            if type(limit) is not int or not 1 <= limit <= 16 * 1024 * 1024:
                raise PathCompilerError('Explicit finite compiler byte limits required')
        if type(self.timeout_seconds) is not int or not 1 <= self.timeout_seconds <= 60:
            raise PathCompilerError('Explicit finite compiler timeout required')


def _identity(path):
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise PathCompilerError('Compiler custody refused')
    stat = path.stat()
    if not path.is_file() or stat.st_size != BINARY_BYTES:
        raise PathCompilerError('Compiler custody refused')
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        remaining = BINARY_BYTES
        while remaining:
            chunk = stream.read(min(65536, remaining))
            if not chunk: raise PathCompilerError('Compiler custody refused')
            digest.update(chunk)
            remaining -= len(chunk)
        if stream.read(1): raise PathCompilerError('Compiler custody refused')
    if digest.hexdigest() != BINARY_SHA256:
        raise PathCompilerError('Compiler custody refused')
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns


def compile_path_request(request: bytes, *, config: PathCompilerConfig) -> bytes:
    """Return untouched one-response UTF-8 bytes; discharge no host obligations."""
    if type(config) is not PathCompilerConfig or type(request) is not bytes or len(request) > config.maximum_request_bytes:
        raise PathCompilerError('Compiler input capacity refused')
    if os.name != 'posix':
        raise PathCompilerError('Compiler transport platform refused')
    process = None
    input_stream = None
    selector = None
    failure = None
    try:
        opening = _identity(config.binary)
        # A bounded temporary input avoids stdin/output pipe deadlocks. It is
        # owned and closed by this invocation, and never retained as diagnostics.
        import tempfile
        input_stream = tempfile.TemporaryFile()
        input_stream.write(request)
        input_stream.seek(0)
        deadline = time.monotonic() + config.timeout_seconds
        process = subprocess.Popen([str(config.binary)], stdin=input_stream,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   bufsize=0, start_new_session=True)
        output, errors = bytearray(), bytearray()
        selector = selectors.DefaultSelector()
        for stream, destination, limit in ((process.stdout, output, config.maximum_response_bytes),
                                           (process.stderr, errors, 4096)):
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, (destination, limit))
        # One transport deadline includes EOF on both pipes and direct-child
        # exit. A descendant holding a pipe open cannot extend it. Unbuffered
        # descriptors have no worker-held Python lock to block final close.
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PathCompilerError('Compiler execution refused')
            for key, _ in selector.select(remaining):
                destination, limit = key.data
                try:
                    chunk = os.read(key.fd, min(65536, limit + 1 - len(destination)))
                except BlockingIOError:
                    continue
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                destination.extend(chunk)
                if len(destination) > limit:
                    raise PathCompilerError('Compiler execution refused')
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise PathCompilerError('Compiler execution refused')
        code = process.wait(timeout=remaining)
        if code != 0 or errors:
            raise PathCompilerError('Compiler execution refused')
        if not output.endswith(b'\n') or output.count(b'\n') != 1:
            raise PathCompilerError('Compiler protocol refused')
        output.decode('utf-8')
        if _identity(config.binary) != opening:
            raise PathCompilerError('Compiler custody refused')
        return bytes(output)
    except PathCompilerError as error:
        failure = error
        raise
    except Exception:
        failure = PathCompilerError('Compiler execution refused')
        raise failure from None
    except BaseException as error:
        failure = error
        raise
    finally:
        cleanup_failed = False
        if selector is not None:
            try:
                selector.close()
            except Exception:
                cleanup_failed = True
        if process is not None:
            # This invocation created the session/process group. Kill its
            # remaining members even if the direct child has already exited.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except Exception:
                cleanup_failed = True
            try:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
            except Exception:
                cleanup_failed = True
            for stream in (process.stdout, process.stderr):
                try:
                    if stream is not None: stream.close()
                except Exception:
                    cleanup_failed = True
        if input_stream is not None:
            try:
                input_stream.close()
            except Exception:
                cleanup_failed = True
        if cleanup_failed:
            if failure is None:
                raise PathCompilerError('Compiler cleanup refused', cleanup_failed=True) from None
            if isinstance(failure, PathCompilerError): failure.cleanup_failed = True
