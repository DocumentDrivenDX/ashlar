"""Owned bounded producer transport; no semantic, SDK or publication authority."""
import os
import selectors
import sys
import signal
import stat
import subprocess
import time
from pathlib import Path
from typing import Mapping, Sequence
from .config import HostError


def snapshot(path: Path, maximum: int) -> bytes:
    """Read one bounded regular-file descriptor; no following final symlinks."""
    if type(maximum) is not int or maximum <= 0:
        raise HostError('evolution-producer-refused')
    fd = None
    primary = None
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise HostError('evolution-producer-refused')
        chunks = []
        remaining = maximum + 1
        while remaining:
            raw = os.read(fd, min(65536, remaining))
            if not raw:
                break
            chunks.append(raw)
            remaining -= len(raw)
        if not remaining:
            raise HostError('evolution-producer-refused')
        return b''.join(chunks)
    except BaseException as exc:
        primary = exc
        if isinstance(exc, Exception):
            raise HostError('evolution-producer-refused') from None
        raise
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except BaseException as cleanup:
                if primary is None or isinstance(primary, Exception) and not isinstance(cleanup, Exception):
                    raise
                try:
                    primary.cleanup_failed = True
                except BaseException:
                    pass


# Fixed transport program. Caller argv is passed as arguments, never code or shell.
# The supervisor holds the session leader alive until the owner kills the group.
_SUPERVISOR = """import os,signal,subprocess,sys
fd=int(sys.argv[1])
child=subprocess.Popen(sys.argv[2:],stdin=subprocess.DEVNULL,close_fds=True)
status=child.wait()
os.close(1)
os.close(2)
os.write(fd,(str(status)+'\\n').encode('ascii'))
os.close(fd)
while True: signal.pause()
"""

def capture(argv: Sequence[str], *, cwd: Path, environment: Mapping[str, str],
            timeout_seconds: int, maximum_output_bytes: int) -> tuple[bytes, bytes]:
    """Own one session, cap both streams and deadline, kill descendants on every outcome."""
    if (type(timeout_seconds) is not int or not 0 < timeout_seconds <= 60 or
            type(maximum_output_bytes) is not int or not 0 < maximum_output_bytes <= 1048576 or
            not argv or not Path(argv[0]).is_absolute()):
        raise HostError('evolution-producer-refused')
    process = None
    selector = None
    streams = []
    primary = None
    cleanup = None
    output = [bytearray(), bytearray(), bytearray()]
    control_read = None
    control_write = None
    producer_status = None
    deadline = time.monotonic() + timeout_seconds
    try:
        control_read, control_write = os.pipe()
        process = subprocess.Popen([sys.executable, '-c', _SUPERVISOR, str(control_write), *argv], cwd=cwd, env=dict(environment),
                                   stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, start_new_session=True, pass_fds=(control_write,))
        streams = [process.stdout, process.stderr]
        os.close(control_write)
        control_write = None
        control_stream = os.fdopen(control_read, 'rb', buffering=0)
        control_read = None
        streams.append(control_stream)
        selector = selectors.DefaultSelector()
        for index, stream in enumerate(streams):
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, index)
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise HostError('evolution-producer-refused')
            for key, _ in selector.select(min(remaining, .05)):
                bound = 16 if key.data == 2 else maximum_output_bytes
                raw = os.read(key.fileobj.fileno(), min(65536, bound - len(output[key.data]) + 1))
                if not raw:
                    selector.unregister(key.fileobj)
                    continue
                if len(output[key.data]) + len(raw) > bound:
                    raise HostError('evolution-producer-refused')
                output[key.data].extend(raw)
        status_bytes = bytes(output[2])
        producer_status = int(status_bytes)
        if status_bytes != (str(producer_status) + '\n').encode('ascii') or not -255 <= producer_status <= 255:
            raise HostError('evolution-producer-refused')
    except BaseException as exc:
        primary = exc
    finally:
        actions = []
        if process is not None:
            def kill():
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            actions.extend((kill, lambda: process.wait(timeout=1)))
        for fd in (control_read, control_write):
            if fd is not None:
                actions.append(lambda fd=fd: os.close(fd))
        if selector is not None:
            actions.append(selector.close)
        actions.extend(stream.close for stream in streams)
        for action in actions:
            try:
                action()
            except BaseException as exc:
                if cleanup is None or isinstance(cleanup, Exception) and not isinstance(exc, Exception):
                    cleanup = exc
        if primary is not None:
            if cleanup is not None:
                try:
                    primary.cleanup_failed = True
                except BaseException:
                    pass
            if not isinstance(primary, Exception):
                raise primary
            if cleanup is not None and not isinstance(cleanup, Exception):
                raise cleanup
            raise HostError('evolution-producer-refused') from None
        if cleanup is not None:
            if not isinstance(cleanup, Exception):
                raise cleanup
            raise HostError('evolution-producer-refused') from None
    if producer_status != 0:
        raise HostError('evolution-producer-refused')
    return bytes(output[0]), bytes(output[1])
