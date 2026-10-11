"""Fixed recipe diagnostics; records establish no admission authority."""
import contextlib, functools, json, os, stat, threading, time
from ashlar_host.lifecycle import finish
MAX_RECORDS = 4096
MAX_BYTES = 1048576
MAX_DEPTH = 64
RESERVE_BYTES = 512
# Closed recipe selection; class metadata is correspondence, never authority.
METHODS = {'driver': ('ashlar_host.driver',
            'NativeDriver',
            ('admit_evolution_run_ledger',
             '_validate_evolution_run_ledger',
             'admit_evolution_plan',
             'observe_completed_evolution_attempt',
             'plan_evolution_transaction',
             'publish_evolution_attempt_held',
             'validate_evolution_run',
             'source_admit')),
 'ledger': ('ashlar_host.evolution_run',
            'EvolutionRunLedger',
            ('_admitted_inventory', 'retain', 'transition')),
 'transport': ('ashlar_host.delta_custody',
               'LocalDeltaTransport',
               ('_detail', 'original_history', '_snapshot', 'inspect_committed')),
 'acks': ('ashlar_host.ack_sessions', 'RegisteredOutboxAcks', ('reconcile',))}
METHOD_IDS = frozenset((key + '.' + method for (key, (_, _, methods)) in METHODS.items() for method in methods))

class StageRefusal(RuntimeError):
    pass

class Observer:

    def __init__(self, directory):
        if type(directory) is not str:
            raise StageRefusal('Explicit private output directory required')
        self.owner_thread = threading.current_thread()
        self.directory = directory
        self.lock = threading.RLock()
        self.local = threading.local()
        self.pending = 0
        self.count = 0
        self.bytes = 0
        self.serial = 0
        self.started = time.monotonic_ns()
        self.closed = False
        self.dirfd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        self.fd = None
        try:
            s = os.fstat(self.dirfd)
            if s.st_uid != os.getuid() or stat.S_IMODE(s.st_mode) != 0o700:
                raise StageRefusal('Owned private directory required')
            self.identity = (s.st_dev, s.st_ino)
            self.fd = os.open('stage-events.jsonl', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=self.dirfd)
            s = os.fstat(self.fd)
            self.file_identity = (s.st_dev, s.st_ino)
            self.renew()
            os.fsync(self.dirfd)
        except BaseException as primary:
            callbacks = []
            if self.fd is not None:
                callbacks.append(lambda: os.close(self.fd))
            callbacks.append(lambda: os.close(self.dirfd))
            finish(primary, callbacks)

    def require_thread(self):
        if threading.current_thread() is not self.owner_thread:
            raise StageRefusal('Single owning observer thread required')

    def renew(self):
        self.require_thread()
        if self.closed:
            raise StageRefusal('Observer closed')
        s = os.stat(self.directory, follow_symlinks=False)
        d = os.fstat(self.dirfd)
        f = os.stat('stage-events.jsonl', dir_fd=self.dirfd, follow_symlinks=False)
        owned = os.fstat(self.fd)
        if (s.st_dev, s.st_ino) != self.identity or (d.st_dev, d.st_ino) != self.identity or (not stat.S_ISDIR(s.st_mode)) or (stat.S_IMODE(s.st_mode) != 0o700) or (s.st_uid != os.getuid()):
            raise StageRefusal('Original stage directory changed')
        if (f.st_dev, f.st_ino) != self.file_identity or (owned.st_dev, owned.st_ino) != self.file_identity or (not stat.S_ISREG(f.st_mode)) or (stat.S_IMODE(f.st_mode) != 0o600) or (f.st_uid != os.getuid()) or (f.st_size != self.bytes):
            raise StageRefusal('Original stage file changed')

    def emit(self, row):
        raw = (json.dumps(row, sort_keys=True, separators=(',', ':')) + '\n').encode()
        if len(raw) > RESERVE_BYTES or self.count >= MAX_RECORDS or self.bytes + len(raw) > MAX_BYTES:
            raise StageRefusal('Bounded stage records required')
        self.renew()
        at = 0
        while at < len(raw):
            n = os.write(self.fd, raw[at:])
            if type(n) is not int or n <= 0:
                raise StageRefusal('Complete stage write required')
            at += n
        self.bytes += len(raw)
        self.count += 1
        os.fsync(self.fd)
        self.renew()

    def invoke(self, method, original, args, kwargs):
        self.require_thread()
        if type(method) is not str or method not in METHOD_IDS:
            raise StageRefusal('Fixed stage method required')
        stack = getattr(self.local, 'stack', None)
        if stack is None:
            stack = []
            self.local.stack = stack
        with self.lock:
            if len(stack) >= MAX_DEPTH or self.count + self.pending + 2 > MAX_RECORDS or self.bytes + (self.pending + 2) * RESERVE_BYTES > MAX_BYTES:
                raise StageRefusal('Stage capacity before method required')
            self.serial += 1
            span = self.serial
            begin = time.monotonic_ns()
            self.emit({'event': 'begin', 'method': method, 'span': span, 'parent': stack[-1] if stack else None, 'depth': len(stack), 'at_ns': begin - self.started})
            self.pending += 1
            stack.append(span)
        primary = None
        value = None
        try:
            value = original(*args, **kwargs)
        except BaseException as error:
            primary = error
        status = 'ok' if primary is None else 'cancel' if not isinstance(primary, Exception) else 'error'
        def end():
            with self.lock:
                self.emit({'event': 'end', 'method': method, 'span': span,
                           'status': status, 'elapsed_ns': time.monotonic_ns() - begin})
        def unwind():
            with self.lock:
                self.pending -= 1
                if not stack or stack.pop() != span:
                    raise StageRefusal('Original stage nesting changed')
        finish(primary, [end, unwind])
        return value

    def close(self):
        self.require_thread()
        # Hold the gate throughout renewal and both closes; foreign threads refuse.
        with self.lock:
            if self.closed:
                return
            if self.pending:
                raise StageRefusal('Active stage methods must finish before observer close')
            try:
                finish(None, [self.renew, lambda: os.close(self.fd),
                              lambda: os.close(self.dirfd)])
            finally:
                self.closed = True

@contextlib.contextmanager
def observe_fixed_owners(owners, directory):
    if type(owners) is not dict or set(owners) != set(METHODS):
        raise StageRefusal('Complete fixed selected owners required')
    selections = []
    for (key, (module, name, methods)) in METHODS.items():
        cls = owners[key]
        if type(cls) is not type or cls.__module__ != module or cls.__name__ != name:
            raise StageRefusal('Exact selected owner class required')
        for method in methods:
            original = cls.__dict__.get(method)
            if not callable(original):
                raise StageRefusal('Exact existing owner method required')
            selections.append((cls, method, original, key + '.' + method))
    observer = Observer(directory)
    patched = []
    primary = None
    try:
        for (cls, method, original, identifier) in selections:

            def make_wrapper(selected, method_id):

                def wrapper(*args, **kwargs):
                    return observer.invoke(method_id, selected, args, kwargs)
                return functools.update_wrapper(wrapper, selected)
            wrapper = make_wrapper(original, identifier)
            setattr(cls, method, wrapper)
            patched.append((cls, method, original, wrapper))
        yield observer
    except BaseException as error:
        primary = error
    callbacks = []
    for cls, method, original, wrapper in reversed(patched):
        def restore(selected=cls, name=method, before=original, installed=wrapper):
            if selected.__dict__.get(name) is not installed:
                raise StageRefusal('Installed stage wrapper changed')
            setattr(selected, name, before)
        callbacks.append(restore)
    callbacks.append(observer.close)
    finish(primary, callbacks)
