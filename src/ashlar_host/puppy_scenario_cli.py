# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fixed installed original17 worker with owned group and container cleanup."""
import hashlib
import json
import os
from pathlib import Path
import secrets
import selectors
import signal
import stat
import subprocess
import sys
import tempfile
import time
from .evolution_cli import InvocationFile
from .evolution_producer import snapshot
from .lifecycle import finish
from .puppy_scenario_configuration import parse_puppy_scenario_configuration
from .puppy_scenario_cleanup import decode_scenario_creation, cleanup_scenario_container
from .puppy_scenario_processes import terminate_scenario_processes
from .truss_preparation import json_bytes


_SUPERVISOR = '''import os,signal,subprocess,sys
status_fd=int(sys.argv[1]);control_fd=int(sys.argv[2])
signal.signal(signal.SIGTERM,lambda *_:None)
child=subprocess.Popen([sys.executable,'-I','-B','-m','ashlar_host.puppy_scenario_worker',
 '--control-fd',str(control_fd),*sys.argv[3:]],stdin=subprocess.DEVNULL,
 close_fds=True,pass_fds=(control_fd,))
os.close(control_fd)
status=child.wait()
os.close(1);os.close(2)
os.write(status_fd,(str(status)+'\\n').encode('ascii'));os.close(status_fd)
while True:signal.pause()
'''


def run_puppy_scenario_command(configuration):
    """Keep the result private until worker, group, container and config close."""
    invocation = InvocationFile.read(Path(configuration), 262144)
    config = parse_puppy_scenario_configuration(invocation.raw)
    config_sha = hashlib.sha256(invocation.raw).hexdigest()
    nonce = secrets.token_hex(16)
    info = invocation.path.lstat()
    parent = config.output.parent
    parent_info = parent.lstat()
    if (info.st_uid != os.geteuid() or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600
            or parent.resolve() != parent or parent_info.st_uid != os.geteuid()
            or not stat.S_ISDIR(parent_info.st_mode) or stat.S_IMODE(parent_info.st_mode) != 0o700
            or config.output.exists() or config.output.is_symlink()
            or config.output == invocation.path or config.output in invocation.path.parents
            or invocation.path in config.output.parents):
        raise PermissionError('Fresh separate output and owned private configuration required')
    # Reserve cleanup time inside the explicitly selected whole ceiling.
    if config.bounds.whole_seconds <= 35:
        raise ValueError('Whole ceiling must include the owned cleanup reserve')
    started = time.monotonic()
    deadline = started + config.bounds.whole_seconds
    worker_deadline = deadline - 35
    evidence = Path(tempfile.mkdtemp(prefix='ashlar-original17-supervision-', dir=parent))
    def renew():
        invocation.renew(262144)
        named = invocation.path.lstat()
        current = parent.lstat()
        if ((named.st_dev, named.st_ino, named.st_uid, named.st_mode, named.st_nlink) !=
                (info.st_dev, info.st_ino, info.st_uid, info.st_mode, info.st_nlink)
                or (current.st_dev, current.st_ino, current.st_uid, current.st_mode) !=
                (parent_info.st_dev, parent_info.st_ino, parent_info.st_uid, parent_info.st_mode)):
            raise PermissionError('Original parent/configuration custody changed')
    process = None; selector = None; streams = []; descriptors = []
    output = [bytearray(), bytearray(), bytearray(), bytearray()]
    primary = None; status = None; receipt = None; cleanup = None; process_cleanup = None
    cleanup_errors = []
    try:
        renew()
        status_read, status_write = os.pipe(); descriptors.extend((status_read, status_write))
        control_read, control_write = os.pipe(); descriptors.extend((control_read, control_write))
        os.set_blocking(control_write, False)
        process = subprocess.Popen([sys.executable, '-I', '-B', '-c', _SUPERVISOR,
            str(status_write), str(control_write), '--configuration', str(invocation.path),
            '--invocation-nonce', nonce, '--configuration-sha256', config_sha],
            cwd=parent, env={'PATH': '/usr/bin:/bin', 'LANG': 'C', 'LC_ALL': 'C',
                'TZ': 'UTC', 'PYTHONUTF8': '1'}, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True,
            pass_fds=(status_write, control_write))
        streams.extend((process.stdout, process.stderr))
        for fd in (status_write, control_write): os.close(fd); descriptors.remove(fd)
        for fd in (status_read, control_read):
            streams.append(os.fdopen(fd, 'rb', buffering=0)); descriptors.remove(fd)
        selector = selectors.DefaultSelector()
        for index, stream in enumerate(streams):
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, index)
        caps = (config.bounds.stdout_bytes, config.bounds.stderr_bytes, 16, 4096)
        next_renewal = started
        while selector.get_map():
            now = time.monotonic()
            if now >= worker_deadline: raise TimeoutError('Owned scenario worker deadline elapsed')
            if now >= next_renewal: renew(); next_renewal = now+1
            for key, _ in selector.select(min(.05, worker_deadline-now)):
                index = key.data
                raw = os.read(key.fd, min(65536, caps[index]-len(output[index])+1))
                if not raw: selector.unregister(key.fileobj); continue
                if len(output[index])+len(raw) > caps[index]:
                    raise ValueError('Complete scenario worker stream cap exceeded')
                output[index].extend(raw)
                if index == 3 and b'\n' in output[3]:
                    receipt = decode_scenario_creation(bytes(output[3]), config, nonce)
        status = int(output[2])
        if bytes(output[2]) != (str(status)+'\n').encode() or not -255 <= status <= 255:
            raise ValueError('Canonical terminal worker status required')
        if output[3]: receipt = decode_scenario_creation(bytes(output[3]), config, nonce)
        if status != 0: raise RuntimeError('Original scenario worker refused')
        if receipt is None: raise RuntimeError('Actual creation outcome remains unknown')
    except BaseException as error:
        primary = error
    def close_group():
        nonlocal process_cleanup
        if process is None: return
        if status is None:
            # Worker translates SIGTERM into cancellation so nested owned source
            # processes can close before the final group kill.
            try: os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError: pass
            end = min(time.monotonic()+3, deadline-1)
            while time.monotonic() < end:
                time.sleep(.05)
        process_cleanup = terminate_scenario_processes(process)
    def close_container():
        nonlocal cleanup, receipt
        if receipt is None and output[3]:
            receipt = decode_scenario_creation(bytes(output[3]), config, nonce)
        if receipt is not None:
            cleanup = cleanup_scenario_container(config, receipt,
                invocation_nonce=nonce, deadline=deadline)
    def retain_terminal():
        terminal = {'profile': 'ashlar-original17-worker-supervision/0.1',
            'configuration_sha256': config_sha, 'invocation_nonce': nonce,
            'worker_status': status, 'error_type': None if primary is None else type(primary).__name__,
            'whole_seconds': config.bounds.whole_seconds, 'cleanup_reserve_seconds': 35,
            'native_seconds': config.bounds.native_seconds,
            'elapsed_seconds': str(time.monotonic()-started), 'container_cleanup': cleanup}
        terminal['process_cleanup'] = process_cleanup
        terminal['cleanup_error_types'] = list(cleanup_errors)
        for name, raw in (('stdout', bytes(output[0])), ('stderr', bytes(output[1])),
                          ('terminal.json', json_bytes(terminal, 65536))):
            with (evidence/name).open('xb') as stream: stream.write(raw)
            (evidence/name).chmod(0o600)
    def guarded(callback):
        def run():
            try: return callback()
            except BaseException as error:
                cleanup_errors.append(type(error).__name__)
                raise
        return run
    finish(primary, [guarded(close_group), guarded(close_container), guarded(renew),
        *[lambda fd=fd: os.close(fd) for fd in descriptors],
        *([] if selector is None else [selector.close]),
        *[stream.close for stream in streams], retain_terminal])
    if time.monotonic() >= deadline: raise TimeoutError('Complete scenario invocation deadline elapsed')
    path = config.output/'result.json'
    identity = path.lstat()
    if not stat.S_ISREG(identity.st_mode) or identity.st_uid != os.geteuid() or identity.st_nlink != 1 or stat.S_IMODE(identity.st_mode) != 0o600 or path.resolve() != path:
        raise PermissionError('Original private complete worker result required')
    raw = snapshot(path, 16777216)
    report = json.loads(raw)
    if (type(report) is not dict or set(report) != {'profile', 'configuration_sha256',
            'invocation_nonce', 'all_outer_closing_completed', 'result'}
            or report['profile'] != 'ashlar-original17-worker-result/0.1'
            or report['configuration_sha256'] != config_sha or report['invocation_nonce'] != nonce
            or report['all_outer_closing_completed'] is not True
            or report['result']['container_id'] != receipt.container_id):
        raise ValueError('Original complete worker result correspondence required')
    renew()
    if snapshot(path, 16777216) != raw or path.lstat() != identity:
        raise PermissionError('Original worker result changed')
    if time.monotonic() >= deadline: raise TimeoutError('Complete scenario invocation deadline elapsed')
    return {'profile': 'ashlar-original17-puppygraph-command-result/0.1',
        'result': str(path), 'result_sha256': hashlib.sha256(raw).hexdigest(),
        'supervision': str(evidence/'terminal.json'), 'protocol': config.runtime.protocol,
        'container_cleanup_completed': cleanup['cleanup_complete'],
        'cases': 17, 'scope': 'Original finite analytic query execution only.'}
