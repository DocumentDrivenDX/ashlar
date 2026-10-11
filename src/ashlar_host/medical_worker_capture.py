"""Owned medical worker capture with separate whole and native deadlines."""
from dataclasses import asdict
from pathlib import Path
import os
import selectors
import signal
import subprocess
import time

from .lifecycle import finish
from .original_medical_local_configuration import OriginalMedicalBounds


_SUPERVISOR = """import os,signal,subprocess,sys
status_fd=int(sys.argv[1]);phase_fd=int(sys.argv[2])
child=subprocess.Popen(sys.argv[3:]+['--native-phase-fd',str(phase_fd)],
 stdin=subprocess.DEVNULL,close_fds=True,pass_fds=(phase_fd,))
os.close(phase_fd)
status=child.wait()
os.close(1);os.close(2)
os.write(status_fd,(str(status)+'\\n').encode('ascii'))
os.close(status_fd)
while True: signal.pause()
"""


def capture_original_medical_worker(argv, *, cwd, environment, renew, bounds):
    """Capture fixed installed dispatch; kill/reap its group on every outcome.

    A retained session leader prevents a worker exit from losing ownership of
    descendants. The worker receives only the explicitly inherited phase pipe.
    Successful capture requires both phase markers and canonical exit status.
    """
    if (type(argv) not in (list, tuple) or not 1 <= len(argv) <= 16
            or any(type(v) is not str or not v or '\x00' in v
                   or len(v.encode()) > 4096 for v in argv)
            or not Path(argv[0]).is_absolute()):
        raise ValueError('Explicit bounded medical worker argv required')
    if (type(cwd) is not type(Path()) or not cwd.is_absolute()
            or type(environment) is not dict
            or any(type(k) is not str or type(v) is not str or '\x00' in k
                   or '\x00' in v for k, v in environment.items())
            or sum(len(k.encode()) + len(v.encode()) for k, v in environment.items()) > 65536
            or not callable(renew) or type(bounds) is not OriginalMedicalBounds):
        raise ValueError('Explicit medical worker custody and bounds required')
    bounds.__post_init__()
    selected = asdict(bounds)
    renew()
    process = None; selector = None; primary = None
    descriptors = []; streams = []; outputs = [bytearray() for _ in range(4)]
    state = 'ready'; native_deadline = None; pending = bytearray()
    status = None; cause = 'unknown'
    whole_deadline = time.monotonic() + bounds.whole_seconds
    next_renewal = time.monotonic()
    try:
        status_read, status_write = os.pipe(); descriptors.extend((status_read, status_write))
        phase_read, phase_write = os.pipe(); descriptors.extend((phase_read, phase_write))
        os.set_blocking(phase_write, False)
        process = subprocess.Popen(
            [argv[0], '-I', '-B', '-c', _SUPERVISOR, str(status_write), str(phase_write), *argv],
            cwd=cwd, env=dict(environment), stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True,
            pass_fds=(status_write, phase_write))
        streams.extend((process.stdout, process.stderr))
        for fd in (status_write, phase_write):
            os.close(fd); descriptors.remove(fd)
        for fd in (status_read, phase_read):
            streams.append(os.fdopen(fd, 'rb', buffering=0)); descriptors.remove(fd)
        selector = selectors.DefaultSelector()
        for index, stream in enumerate(streams):
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, index)
        while selector.get_map():
            now = time.monotonic()
            if now >= next_renewal:
                renew(); bounds.__post_init__()
                if asdict(bounds) != selected:
                    raise PermissionError('Original medical worker bounds changed')
                next_renewal = time.monotonic() + .5
            remaining = whole_deadline - time.monotonic()
            if native_deadline is not None:
                remaining = min(remaining, native_deadline - time.monotonic())
            if remaining <= 0:
                cause = 'native-deadline' if native_deadline is not None and native_deadline < whole_deadline else 'whole-deadline'
                raise TimeoutError('Original medical worker deadline exceeded')
            for key, _ in selector.select(min(remaining, .05)):
                observed = time.monotonic()
                if observed >= whole_deadline or native_deadline is not None and observed >= native_deadline:
                    cause = 'native-deadline' if native_deadline is not None and native_deadline < whole_deadline else 'whole-deadline'
                    raise TimeoutError('Original medical worker deadline exceeded')
                index = key.data
                cap = selected['stream_bytes'] if index < 2 else 16 if index == 2 else 32
                raw = os.read(key.fileobj.fileno(), min(65536, cap - len(outputs[index]) + 1))
                if not raw:
                    selector.unregister(key.fileobj)
                    if index == 3 and pending:
                        cause = 'phase-protocol'
                        raise ValueError('Truncated medical native phase')
                    continue
                if len(outputs[index]) + len(raw) > cap:
                    outputs[index].extend(raw[:cap - len(outputs[index])])
                    cause = 'capture-capacity'
                    raise ValueError('Medical worker capture bound exceeded')
                outputs[index].extend(raw)
                if index == 3:
                    pending.extend(raw)
                    while b'\n' in pending:
                        message, _, rest = pending.partition(b'\n'); pending[:] = rest
                        if message == b'native.begin' and state == 'ready':
                            state = 'begun'; native_deadline = time.monotonic() + selected['native_seconds']
                        elif message == b'native.end' and state == 'begun':
                            state = 'ended'; native_deadline = None
                        else:
                            cause = 'phase-protocol'
                            raise ValueError('Invalid medical native phase order')
        raw_status = bytes(outputs[2])
        status = int(raw_status)
        if raw_status != (str(status) + '\n').encode('ascii') or not -255 <= status <= 255:
            raise ValueError('Invalid medical worker exit observation')
        if status != 0:
            cause = 'child-exit'
            raise subprocess.CalledProcessError(status, argv)
        if state != 'ended' or pending:
            cause = 'phase-protocol'
            raise ValueError('Successful medical worker has unfinished native phase')
        renew()
        if asdict(bounds) != selected:
            raise PermissionError('Original medical worker bounds changed')
        if time.monotonic() >= whole_deadline:
            cause = 'whole-deadline'
            raise TimeoutError('Original medical worker deadline exceeded')
    except BaseException as error:
        primary = error
    callbacks = []
    if process is not None:
        def kill():
            try: os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError: pass
        callbacks.extend((kill, lambda: process.wait(timeout=1)))
    callbacks.extend(lambda fd=fd: os.close(fd) for fd in descriptors)
    if selector is not None: callbacks.append(selector.close)
    callbacks.extend(stream.close for stream in streams)
    def closing():
        renew(); bounds.__post_init__()
        if asdict(bounds) != selected:
            raise PermissionError('Original medical worker bounds changed')
    callbacks.append(closing)
    try:
        if primary is None:cause = 'closing-refusal'
        finish(primary, callbacks)
        if time.monotonic() >= whole_deadline:
            cause = 'whole-deadline'
            raise TimeoutError('Original medical worker deadline exceeded')
    except BaseException as error:
        # Private bounded prefixes, never automatic logging or exception text.
        # Keep the original primary identity, including KeyboardInterrupt.
        carrier = {
            'stdout': bytes(outputs[0]), 'stderr': bytes(outputs[1]),
            'status': status, 'phase': bytes(outputs[3]), 'complete': False,
            'scope': 'bounded owned stream prefixes',
            'failure': {'cause': 'cancel' if not isinstance(error,Exception) else cause,
                'status': status if type(status)is int and -255<=status<=255 else None,
                'native_phase': state},
        }
        # Bypass user exception attribute hooks without replacing the primary.
        BaseException.__dict__['__dict__'].__get__(error, type(error))['medical_capture'] = carrier
        raise
    return bytes(outputs[0]), bytes(outputs[1])
