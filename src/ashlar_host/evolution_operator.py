"""Existing local installation, cooperative POSIX lease and ordinary sessions.

No installation, roles, grants, runtime or credentials are discovered or created.
The lease excludes cooperating local operators. The transport and driver renew
actual native registry, original operation, source and protected ACK lineage;
this lease does not exclude arbitrary external Delta writers.
"""
from contextlib import contextmanager
from dataclasses import dataclass, field
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
from types import MappingProxyType
from typing import Callable, ContextManager, Mapping, Optional, Union
from urllib.parse import quote
from .ack_sessions import OutboxAckRegistration, RegisteredOutboxAcks
from .config import FreshCommerceEvolutionConfig, ResumeCommerceEvolutionConfig
from .delta_custody import DeltaTarget, LocalDeltaTransport, encoded, operation_capacity
from .driver import NativeDriver, PrivatePolicy
from .evolution_admission import EvolutionSourceSet, admit_commerce_evolution, original_equal
from .evolution_cli import FreshEvolutionInvocation, ResumeEvolutionInvocation
from .evolution_composition import NativeEvolutionRunPolicy
from .evolution_plan import EvolutionAttemptPlan, EvolutionPlanError, owned_fd
from .evolution_run import EvolutionRunDefinition, EvolutionRunLedger, MAXIMUM_RUN_BYTES
from .existing_outbox_authority import ExistingOutboxAuthority, OutboxAuthorityRegistration
from .lifecycle import finish, owned_context
from .source_sessions import OutboxSourceRegistration, RegisteredOutboxSources


def private(path, *, directory=False, maximum=128 * 1024 * 1024):
    if type(path) is not type(Path()) or not path.is_absolute() or path.resolve() != path:
        raise PermissionError('Canonical existing local owner path required')
    info = path.lstat()
    if ((stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)) is not True
            or info.st_uid != os.getuid() or info.st_mode & 0o022
            or (not directory and info.st_size > maximum)):
        raise PermissionError('Private bounded existing local owner required')
    return info.st_dev, info.st_ino


def read_original(path, maximum):
    identity = private(path, maximum=maximum)
    with owned_fd(os.open(path, os.O_RDONLY | os.O_NOFOLLOW)) as fd:
        info = os.fstat(fd)
        if (info.st_dev, info.st_ino) != identity:
            raise PermissionError('Original file opening identity changed')
        with os.fdopen(os.dup(fd), 'rb') as stream:
            raw = stream.read(maximum + 1)
    if len(raw) > maximum or private(path, maximum=maximum) != identity:
        raise PermissionError('Original bounded file custody changed')
    return raw


@dataclass(frozen=True, repr=False)
class ExistingEvolutionOperator:
    """Trusted provider inputs for a previously installed empty or retained run.

    native_session returns an owned context manager yielding the explicitly
    selected Spark session. Connections are supplied ordinary DB-API factories;
    their actual role/access/signature is checked, not inferred from metadata.
    """
    installation_id: str
    journal_path: Path
    reservation_path: Path
    targets: tuple[DeltaTarget, ...]
    registrations: tuple[OutboxAuthorityRegistration, ...]
    capacity: Optional[Mapping[str, Union[str, int]]]
    native_session: Callable[[], ContextManager[object]] = field(repr=False, compare=False)

    def __post_init__(self):
        import re
        if (type(self.installation_id) is not str
                or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9:_-]{0,1023}', self.installation_id)
                or type(self.targets) is not tuple or len(self.targets) != 6
                or any(type(item) is not DeltaTarget for item in self.targets)
                or type(self.registrations) is not tuple or len(self.registrations) != 2
                or any(type(item) is not OutboxAuthorityRegistration for item in self.registrations)
                or not callable(self.native_session)):
            raise PermissionError('Complete existing native installation and ordinary session inputs required')
        if any(type(item.table) is not str or type(item.uuid) is not str
                or type(item.path) is not type(Path()) for item in self.targets):
            raise PermissionError('Exact original native registry carriers required')
        for path in (self.journal_path, self.reservation_path):
            if type(path) is not type(Path()) or not path.is_absolute():
                raise PermissionError('Explicit original local owner paths required')
        if (self.journal_path == self.reservation_path
                or self.journal_path.parent != self.reservation_path.parent):
            raise PermissionError('Distinct original reservation in existing private journal parent required')
        targets = tuple(DeltaTarget(item.table, item.path, item.uuid) for item in self.targets)
        roles = {item.table.split('.')[-1] for item in targets}
        if (roles != {'attempts', 'manifest', 'object_current', 'edge_current', 'tombstone', 'whole_source_history'}
                or len({item.table for item in targets}) != 6 or len({item.path for item in targets}) != 6):
            raise PermissionError('Complete distinct six-table original installation required')
        object.__setattr__(self, 'targets', targets)
        selected = tuple(OutboxAuthorityRegistration(item.source, item.source_role, item.ack_role,
            item.database, item.metadata_factory) for item in self.registrations)
        if len({item.source.scope.feed for item in selected}) != 2:
            raise PermissionError('Exactly two distinct original source registrations required')
        object.__setattr__(self, 'registrations', selected)
        if self.capacity is not None and (type(self.capacity) not in (dict, MappingProxyType)
                or len(self.capacity) != 2
                or any(type(key) is not str or type(value) not in (str, int)
                       for key, value in self.capacity.items())):
            raise PermissionError('Exact selected existing operation capacity required')
        object.__setattr__(self, 'capacity', operation_capacity(self.capacity))

    @contextmanager
    def open_evolution(self, invocation):
        if type(invocation) not in (FreshEvolutionInvocation, ResumeEvolutionInvocation):
            raise PermissionError('Exact fresh or resume invocation required')
        if invocation.ledger_path.parent != self.journal_path.parent or invocation.ledger_path in (
                self.journal_path, self.reservation_path):
            raise PermissionError('Distinct original ledger in existing private owner directory required')
        context = object()
        with owned_context(ExistingEvolutionLease.open(self, invocation, context)) as lease:
            with owned_context(self.native_session()) as spark:
                admissions = tuple(admit_commerce_evolution(invocation.producer,
                    source_system=item.source.scope.feed, epoch=item.source.scope.epoch)
                    for item in self.registrations)
                sources = EvolutionSourceSet(admissions)
                authority = ExistingOutboxAuthority(self.registrations, lease)
                policy = ExistingEvolutionNativePolicy(context, self.targets, lease)
                transport = LocalDeltaTransport(spark, self.journal_path, self.installation_id,
                    self.targets, policy, capacity=self.capacity)
                # Transfer ownership once: the driver factory closes on failure;
                # successful transport closes in the enclosing provider only.
                def acks(driver):
                    ack_policy = ExistingEvolutionAckPolicy(authority, driver)
                    return RegisteredOutboxAcks(tuple(OutboxAckRegistration(item.source,
                        item.metadata_factory, ack_policy) for item in self.registrations))
                driver = NativeDriver.with_registered_evolution(transport, policy, context,
                    {target.table.split('.')[-1]: target.table for target in self.targets},
                    sources.changes, sources.columns, source_admission=sources,
                    source_sessions=RegisteredOutboxSources(tuple(item.source for item in self.registrations),
                        authority), ack_factory=acks)
                primary = None
                try:
                    run_policy = NativeEvolutionRunPolicy(driver, lease)
                    if type(invocation) is FreshEvolutionInvocation:
                        config = FreshCommerceEvolutionConfig(driver, invocation.ledger_path, run_policy,
                            invocation.producer, context, invocation.request)
                    else:
                        config = ResumeCommerceEvolutionConfig(driver, invocation.ledger_path, run_policy,
                            invocation.producer, context, invocation.expected_sha256)
                    yield config
                    lease.renew(context)
                except BaseException as error:
                    primary = error
                finish(primary, [transport.close])


class ExistingEvolutionLease:
    """Owned cooperative writer lease and positive original run reservation.

    The preexisting native installation registry and target reservations authorize
    its current local owner before fresh original bytes are retained. The new
    file binds original bytes; it does not authorize an installation or repair.
    """
    def __init__(self, operator, invocation, context, fd, identity, parent_identity):
        self.operator = operator; self.invocation = invocation; self.context = context
        self.fd = fd; self.identity = identity; self.parent_identity = parent_identity
        self.closed = False
        self.lock_path = operator.journal_path.parent / '.evolution-operator.lock'
        registry = {'installation': operator.installation_id,
            'targets': [{'table': item.table, 'path': str(item.path), 'uuid': item.uuid}
                        for item in sorted(operator.targets, key=lambda item: item.table)]}
        if operator.capacity is not None: registry['operation_capacity'] = dict(operator.capacity)
        self.registry = encoded(registry)
        self.native_identity = private(operator.journal_path)
        self.created = False
        self.original_bytes = None

    @classmethod
    @contextmanager
    def open(cls, operator, invocation, context):
        parent = private(operator.journal_path.parent, directory=True)
        lock = operator.journal_path.parent / '.evolution-operator.lock'
        with owned_fd(os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)) as fd:
            identity = private(lock, maximum=4096)
            info = os.fstat(fd)
            if (info.st_dev, info.st_ino) != identity:
                raise PermissionError('Owned writer lease opening identity changed')
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            owner = None; primary = None
            try:
                owner = cls(operator, invocation, context, fd, identity, parent)
                owner.renew(context)
                if type(invocation) is FreshEvolutionInvocation:
                    if operator.reservation_path.exists() or operator.reservation_path.is_symlink():
                        raise PermissionError('Original run reservation exists; select resume')
                    if invocation.ledger_path.exists() or invocation.ledger_path.is_symlink():
                        raise PermissionError('Original run ledger exists; select resume')
                else:
                    raw = read_original(operator.reservation_path, MAXIMUM_RUN_BYTES)
                    if hashlib.sha256(raw).hexdigest() != invocation.expected_sha256:
                        raise PermissionError('Original retained run digest differs')
                    original = EvolutionRunDefinition(raw)
                    owner.admit_run(original, context)
                    EvolutionRunLedger.inspect(invocation.ledger_path, original, context=context,
                        custody=owner._ledger_custody)
                yield owner
            except BaseException as error:
                primary = error
            def close():
                if owner is not None: owner.closed = True
                fcntl.flock(fd, fcntl.LOCK_UN)
            finish(primary, ([lambda: owner.renew(context)] if owner is not None else []) + [close])

    def renew(self, context):
        if context is not self.context or self.closed:
            raise PermissionError('Current independently held original writer lease required')
        if (private(self.operator.journal_path.parent, directory=True) != self.parent_identity
                or private(self.lock_path, maximum=4096) != self.identity
                or private(self.operator.journal_path) != self.native_identity):
            raise PermissionError('Original local installation/lease custody changed')
        info = os.fstat(self.fd)
        if (info.st_dev, info.st_ino) != self.identity:
            raise PermissionError('Original held writer lease changed')
        expected = (encoded({'profile': 'ashlar-private-native-installation-reservation/0.1',
            'registry': json.loads(self.registry),
            'original_journal_path': str(self.operator.journal_path)}) + '\n').encode()
        for target in self.operator.targets:
            private(target.path, directory=True)
            if read_original(target.path / '.ashlar-local-installation-reservation.json', 4194304) != expected:
                raise PermissionError('Complete original target reservation differs')
        # Existing-only rw opening permits SQLite's WAL/SHM bookkeeping on the
        # selected local host. SQL is query-only; immutable=1 would ignore WAL.
        connection = sqlite3.connect('file:' + quote(str(self.operator.journal_path)) + '?mode=rw', uri=True)
        primary = None
        try:
            connection.execute('PRAGMA query_only=ON')
            if connection.execute('PRAGMA journal_mode').fetchone() != ('wal',):
                raise PermissionError('Existing original WAL journal profile required; no conversion')
            rows = connection.execute('SELECT id,substr(original,1,4194305) FROM local_installation LIMIT 2').fetchall()
            if (len(rows) != 1 or type(rows[0][0]) is not int or type(rows[0][1]) is not str
                    or rows != [(1, self.registry)]):
                raise PermissionError('Original native installation registry differs')
        except BaseException as error:
            primary = error
        finish(primary, [connection.close])
        if private(self.operator.journal_path) != self.native_identity:
            raise PermissionError('Closing original native registry custody changed')
        if (self.original_bytes is not None and read_original(self.operator.reservation_path,
                MAXIMUM_RUN_BYTES) != self.original_bytes):
            raise PermissionError('Current original run reservation changed')

    def admit_source_writer(self, registration, context):
        if type(registration) is not OutboxSourceRegistration:
            raise PermissionError('Exact original source registration required')
        registration = OutboxSourceRegistration(registration.scope, registration.source_schema,
            registration.source_signature_sha256, registration.connection_factory)
        self.renew(context)
        if not any(registration.metadata() == item.source.metadata() for item in self.operator.registrations):
            raise PermissionError('Original registered source writer differs')

    def admit_run(self, original, context):
        self.renew(context)
        if type(original) is not EvolutionRunDefinition:
            raise PermissionError('Exact original complete run required')
        installation = original.document()['installation']
        registry = {item.table: {'uuid': item.uuid, 'path': str(item.path)} for item in self.operator.targets}
        if installation['installation_id'] != self.operator.installation_id or installation['registry'] != registry:
            raise PermissionError('Original complete native run registry differs')
        if type(self.invocation) is FreshEvolutionInvocation:
            if encoded(original.document()['request']) != encoded(self.invocation.request.document()):
                raise PermissionError('Exact configured original schedule differs')
            if not self.created:
                with owned_fd(os.open(self.operator.reservation_path,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)) as fd:
                    with os.fdopen(os.dup(fd), 'wb') as stream:
                        stream.write(original.raw); stream.flush(); os.fsync(stream.fileno())
                with owned_fd(os.open(self.operator.reservation_path.parent,
                        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)) as parent:
                    os.fsync(parent)
                self.created = True
        elif original.sha256 != self.invocation.expected_sha256:
            raise PermissionError('Exact retained original run differs')
        if read_original(self.operator.reservation_path, MAXIMUM_RUN_BYTES) != original.raw:
            raise PermissionError('Durable original run bytes differ; no replacement')
        self.original_bytes = original.raw
        self.renew(context)

    def _ledger_custody(self, path, original, context):
        if type(path) is not type(Path()) or path != self.invocation.ledger_path:
            raise PermissionError('Original ledger location differs')
        self.admit_run(original, context)

    def admit_ledger(self, original, inventory, context):
        self.admit_run(original, context)
        actual = EvolutionRunLedger.inspect(self.invocation.ledger_path, original, context=context,
            custody=self._ledger_custody)
        if not original_equal(inventory, actual):
            raise PermissionError('Complete actual original reservation inventory differs')

    def admit_attempt(self, original, ordinal, plan, context):
        if type(ordinal) is not int or not 0 <= ordinal < 8 or type(plan) is not EvolutionAttemptPlan:
            raise PermissionError('Exact original ordinal and complete attempt required')
        actual = EvolutionRunLedger.inspect(self.invocation.ledger_path, original,
            context=context, custody=self._ledger_custody)
        _, state, raw, digest, _ = actual.slots[ordinal]
        if state != 'unprepared' and (raw != plan.raw or digest != plan.sha256):
            raise PermissionError('Original retained attempt must not be replaced')
        if ordinal and actual.slots[ordinal - 1][1] != 'completed':
            raise PermissionError('Actual original completed prefix required')


class ExistingEvolutionNativePolicy(PrivatePolicy):
    """Exact original native intent with mandatory existing writer renewals."""
    def __init__(self, context, targets, lease):
        if (type(lease) is not ExistingEvolutionLease or context is not lease.context
                or type(targets) is not tuple
                or any(type(item) is not DeltaTarget for item in targets)
                or targets != lease.operator.targets):
            raise PermissionError('Exact existing lease and native target registry required')
        lease.renew(context)
        super().__init__(context, targets)
        self.lease = lease; self.initializing = False

    @contextmanager
    def writer(self, *args):
        self.lease.renew(args[-1])
        primary = None
        try:
            with owned_context(super().writer(*args)):
                yield
        except BaseException as error:
            primary = error
        finish(primary, [lambda: self.lease.renew(args[-1])])

    def activate_evolution(self, plan, context, admission):
        self.lease.renew(context)
        super().activate_evolution(plan, context, admission)
        self.lease.renew(context)

    def admit(self, intent, context):
        self.lease.renew(context)
        super().admit(intent, context)
        self.lease.renew(context)


@dataclass(frozen=True)
class ExistingEvolutionAckPolicy:
    source: ExistingOutboxAuthority
    driver: NativeDriver

    def __post_init__(self):
        if type(self.source) is not ExistingOutboxAuthority or type(self.driver) is not NativeDriver:
            raise PermissionError('Actual ordinary source and native publication owners required')

    def admit_scope(self, scope, session, context):
        self.driver.require(context)
        return self.source.admit_scope(scope, session, context)

    def admit_publication(self, scope, request, resolved, session, context):
        self.source.admit_scope(scope, session, context)
        return self.driver.admit_publication(scope, request, resolved, session, context)
