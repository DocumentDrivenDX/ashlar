"""Fixed original eight-step run and positive pre-submission custody.

The owned local SQLite ledger is distinct from native-operation custody. Every
slot exists from creation; missing rows never mean absence. Current independent
policy must bind the original filesystem/installation and all native authority.
"""
from dataclasses import dataclass, field
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
from urllib.parse import quote
from .lifecycle import finish
from typing import Protocol
from .evolution_plan import EvolutionAttemptPlan, EvolutionPlanError, decode, owned_fd

PROFILE = 'ashlar-commerce-evolution-run/0.1'
MAXIMUM_RUN_BYTES = 2 * 1024 * 1024


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


@dataclass(frozen=True)
class EvolutionRunRequest:
    source_order: tuple[str, str]
    stream: str
    predecessor: str
    clocks: tuple[tuple[str, ...], tuple[str, ...]]
    publication_ids: tuple[str, ...]
    recorded_at: tuple[str, ...]

    def __post_init__(self):
        if (type(self.source_order) is not tuple or len(self.source_order) != 2
                or any(type(value) is not str or not 0 < len(value) <= 1024 for value in self.source_order)
                or len(set(self.source_order)) != 2
                or any(type(value) is not str or not 0 < len(value) <= 1024 for value in (self.stream, self.predecessor))
                or type(self.clocks) is not tuple or len(self.clocks) != 2
                or any(type(sequence) is not tuple or len(sequence) != 4 for sequence in self.clocks)
                or type(self.publication_ids) is not tuple or len(self.publication_ids) != 8
                or any(type(value) is not str or not 0 < len(value) <= 1024 for value in self.publication_ids)
                or len(set(self.publication_ids)) != 8 or self.predecessor in self.publication_ids
                or type(self.recorded_at) is not tuple or len(self.recorded_at) != 8):
            raise EvolutionPlanError('Exact bounded original eight-step configuration required')
        for clock in (clock for sequence in self.clocks for clock in sequence):
            if (type(clock) is not str or not re.fullmatch(
                    r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?\+00:00', clock)):
                raise EvolutionPlanError('Original UTC transaction clock required')
            instant = datetime.datetime.fromisoformat(clock)
            if instant.tzinfo is None or instant.utcoffset() != datetime.timedelta(0):
                raise EvolutionPlanError('Original UTC transaction clock required')
        for value in self.recorded_at:
            if (type(value) is not str or not re.fullmatch('[0-9]{1,19}', value)
                    or str(int(value)) != value or not 0 <= int(value) < 2**63):
                raise EvolutionPlanError('Canonical original recording clock required')

    def document(self):
        return {'source_order': list(self.source_order), 'stream': self.stream, 'predecessor': self.predecessor,
                'clocks': [list(sequence) for sequence in self.clocks],
                'publication_ids': list(self.publication_ids), 'recorded_at': list(self.recorded_at)}

    def schedule(self):
        return [{'source': self.source_order[ordinal % 2], 'prefix': ordinal // 2 + 1,
                 'publication_id': self.publication_ids[ordinal],
                 'materialized_at': self.clocks[ordinal % 2][ordinal // 2],
                 'recorded_at': self.recorded_at[ordinal]} for ordinal in range(8)]


def request_from_document(value):
    if type(value) is not dict or set(value) != {'source_order', 'stream', 'predecessor', 'clocks', 'publication_ids', 'recorded_at'}:
        raise EvolutionPlanError('Closed original run request required')
    for field in ('source_order', 'clocks', 'publication_ids', 'recorded_at'):
        if type(value[field]) is not list:
            raise EvolutionPlanError('Original run arrays required')
    if any(type(sequence) is not list for sequence in value['clocks']):
        raise EvolutionPlanError('Original clock arrays required')
    return EvolutionRunRequest(tuple(value['source_order']), value['stream'], value['predecessor'],
        tuple(tuple(sequence) for sequence in value['clocks']), tuple(value['publication_ids']), tuple(value['recorded_at']))


@dataclass(frozen=True)
class EvolutionRunDefinition:
    raw: bytes

    def __post_init__(self):
        if type(self.raw) is not bytes or not 0 < len(self.raw) <= MAXIMUM_RUN_BYTES:
            raise EvolutionPlanError('Bounded exact original run bytes required')
        value = decode(self.raw)
        if (len(self.raw) > MAXIMUM_RUN_BYTES or type(value) is not dict
                or set(value) != {'profile', 'request', 'schedule', 'installation'} or value['profile'] != PROFILE
                or type(value['installation']) is not dict or not value['installation']):
            raise EvolutionPlanError('Complete original run definition required')
        request = request_from_document(value['request'])
        if encoded(value['schedule']) != encoded(request.schedule()):
            raise EvolutionPlanError('Exact developer-owned alternating eight-step schedule required')

    def document(self): return decode(self.raw)
    @property
    def sha256(self): return hashlib.sha256(self.raw).hexdigest()


@dataclass(frozen=True, repr=False)
class EvolutionRunLedgerView:
    slots: tuple[tuple[int, str, bytes, str, bytes], ...]


class EvolutionRunPolicy(Protocol):
    def admit_run(self, original: EvolutionRunDefinition, context: object) -> None:
        """Independently bind original ledger identity/installation, runtime and current writer authority."""
    def admit_ledger(self, original: EvolutionRunDefinition, inventory: EvolutionRunLedgerView,
                     context: object) -> None:
        """Independently admit complete immutable eight-slot custody/current native lineage.

        Bind actual original path/installation and all submission authority to this
        reservation owner. A caller-selected state or missing native handle does
        not establish never-submitted status; changed/lost original custody refuses.
        """
    def admit_attempt(self, original: EvolutionRunDefinition, ordinal: int,
                      plan: EvolutionAttemptPlan, context: object) -> None:
        """Independently admit full original/native/source/ACK meaning, never a receipt flag."""


RUN_SQL = 'CREATE TABLE original_run(id INTEGER PRIMARY KEY CHECK(id=1), original BLOB NOT NULL, digest TEXT NOT NULL)'
SLOT_SQL = "CREATE TABLE original_slot(ordinal INTEGER PRIMARY KEY CHECK(ordinal BETWEEN 0 AND 7), state TEXT NOT NULL CHECK(state IN ('unprepared','retained','publication-started','completed')), original BLOB, digest TEXT, descriptor BLOB)"


class EvolutionRunLedger:
    """Owned exact local reservation inventory; no journal initialization on resume.

    FULL synchronous DELETE-journal SQLite on the selected local POSIX filesystem
    and the enclosing exclusive native writer are assumptions. Original rows and
    complete slots are required at every gate. Policy must independently establish
    current custody; this ledger does not infer submission absence from native rows.
    """
    def __init__(self, path, connection, original, policy, context):
        if not isinstance(path, Path) or type(connection) is not sqlite3.Connection or type(original) is not EvolutionRunDefinition:
            raise EvolutionPlanError('Owned exact original ledger handles required')
        self.path = path; self.connection = connection; self.original = original
        self.policy = policy; self.context = context; self.closed = False
        info = path.lstat(); self.identity = (info.st_dev, info.st_ino)
        parent = path.parent.lstat(); self.parent_identity = (parent.st_dev, parent.st_ino)

    @classmethod
    def open(cls, path: Path, policy: EvolutionRunPolicy, *, context, original=None, expected_sha256=None):
        if (not isinstance(path, Path) or not path.is_absolute()
                or any(not callable(getattr(policy, name, None)) for name in ('admit_run', 'admit_ledger', 'admit_attempt'))
                or (original is None) == (expected_sha256 is None)
                or (original is None and (type(expected_sha256) is not str or not re.fullmatch('[0-9a-f]{64}', expected_sha256)))):
            raise EvolutionPlanError('Explicit original run location, policy and fresh/resume selection required')
        parent = path.parent.lstat()
        if (not stat.S_ISDIR(parent.st_mode) or path.parent.resolve() != path.parent
                or parent.st_uid != os.getuid() or parent.st_mode & 0o022):
            raise EvolutionPlanError('Canonical private owned original ledger parent required')
        fresh = original is not None
        if fresh:
            if type(original) is not EvolutionRunDefinition or policy.admit_run(original, context) is not None:
                raise EvolutionPlanError('Current original run admission incomplete')
            with owned_fd(os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)) as parent:
                with owned_fd(os.open(path.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)) as fd:
                    os.fsync(fd)
                    created = os.fstat(fd)
                os.fsync(parent)
        info = path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022
                or info.st_size > 128 * 1024 * 1024):
            raise EvolutionPlanError('Private bounded original run ledger required')
        if fresh and (info.st_dev, info.st_ino) != (created.st_dev, created.st_ino):
            raise EvolutionPlanError('Exclusive original ledger identity changed')
        connection = sqlite3.connect('file:' + quote(str(path)) + '?mode=rw', uri=True)
        try:
            connection.execute('PRAGMA synchronous=FULL')
            if fresh:
                connection.execute('PRAGMA journal_mode=DELETE')
            elif connection.execute('PRAGMA journal_mode').fetchone() != ('delete',):
                raise EvolutionPlanError('Original DELETE-journal custody profile required; no resume conversion')
            if fresh:
                with connection:
                    connection.execute(RUN_SQL); connection.execute(SLOT_SQL)
                    connection.execute('INSERT INTO original_run VALUES(1,?,?)', (original.raw, original.sha256))
                    connection.executemany('INSERT INTO original_slot VALUES(?,\'unprepared\',NULL,NULL,NULL)', [(n,) for n in range(8)])
                with owned_fd(os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)) as parent:
                    os.fsync(parent)
            rows = connection.execute('SELECT original,digest FROM original_run WHERE id=1').fetchall()
            if len(rows) != 1: raise EvolutionPlanError('Complete original run custody required')
            retained = EvolutionRunDefinition(rows[0][0])
            if rows[0][1] != retained.sha256 or (fresh and retained.raw != original.raw) or (not fresh and retained.sha256 != expected_sha256):
                raise EvolutionPlanError('Original run bytes/digest differ')
            after = path.lstat()
            if (after.st_dev, after.st_ino) != (info.st_dev, info.st_ino) or not stat.S_ISREG(after.st_mode):
                raise EvolutionPlanError('Original ledger opening identity changed')
            owner = cls(path, connection, retained, policy, context)
            owner.admit()
            return owner
        except BaseException as error:
            finish(error, [connection.close])

    def _filesystem_custody(self):
        parent = self.path.parent.lstat()
        if (not stat.S_ISDIR(parent.st_mode) or self.path.parent.resolve() != self.path.parent
                or (parent.st_dev, parent.st_ino) != self.parent_identity
                or parent.st_uid != os.getuid() or parent.st_mode & 0o022):
            raise EvolutionPlanError('Original private parent custody changed')
        info = self.path.lstat()
        if (not stat.S_ISREG(info.st_mode) or (info.st_dev, info.st_ino) != self.identity
                or info.st_uid != os.getuid() or info.st_mode & 0o022 or info.st_size > 128 * 1024 * 1024):
            raise EvolutionPlanError('Original ledger file identity changed')

    def admit(self):
        if self.closed: raise EvolutionPlanError('Original run ledger closed')
        self._filesystem_custody()
        schema = dict(self.connection.execute("SELECT name,sql FROM sqlite_master WHERE type='table'"))
        if schema != {'original_run': RUN_SQL, 'original_slot': SLOT_SQL}:
            raise EvolutionPlanError('Original run ledger schema differs')
        if self.connection.execute('SELECT original,digest FROM original_run').fetchall() != [(self.original.raw, self.original.sha256)]:
            raise EvolutionPlanError('Original run custody changed')
        slots = self.connection.execute('SELECT ordinal,state,original,digest,descriptor FROM original_slot ORDER BY ordinal').fetchall()
        if len(slots) != 8 or [row[0] for row in slots] != list(range(8)):
            raise EvolutionPlanError('Complete eight original reservation slots required')
        gap = False
        for ordinal, state, raw, digest, descriptor in slots:
            if state == 'unprepared':
                if raw is not None or digest is not None or descriptor is not None:
                    raise EvolutionPlanError('Unprepared slot carries contradictory custody')
                gap = True
            else:
                if gap or state not in ('retained', 'publication-started', 'completed'):
                    raise EvolutionPlanError('Original reservation sequence differs')
                plan = EvolutionAttemptPlan(raw)
                if digest != plan.sha256 or (state == 'completed') != (descriptor is not None):
                    raise EvolutionPlanError('Complete original attempt/descriptor custody differs')
                if state != 'completed': gap = True
                if descriptor is not None:
                    if type(descriptor) is not bytes or len(descriptor) > 4194304 or type(decode(descriptor)) is not dict:
                        raise EvolutionPlanError('Bounded original publication descriptor required')
        if self.policy.admit_run(self.original, self.context) is not None:
            raise EvolutionPlanError('Current original ledger admission incomplete')
        if self.policy.admit_ledger(self.original, EvolutionRunLedgerView(tuple(slots)), self.context) is not None:
            raise EvolutionPlanError('Current complete original ledger lineage admission incomplete')
        if (self.connection.execute('SELECT original,digest FROM original_run').fetchall() != [(self.original.raw, self.original.sha256)]
                or self.connection.execute('SELECT ordinal,state,original,digest,descriptor FROM original_slot ORDER BY ordinal').fetchall() != slots):
            raise EvolutionPlanError('Original ledger changed during current admission')
        self._filesystem_custody()

    def slot(self, ordinal):
        if type(ordinal) is not int or not 0 <= ordinal < 8: raise EvolutionPlanError('Original schedule ordinal required')
        self.admit()
        return self.connection.execute('SELECT state,original,digest,descriptor FROM original_slot WHERE ordinal=?', (ordinal,)).fetchone()

    def retain(self, ordinal, plan):
        if type(plan) is not EvolutionAttemptPlan: raise EvolutionPlanError('Complete original attempt required')
        if self.policy.admit_attempt(self.original, ordinal, plan, self.context) is not None:
            raise EvolutionPlanError('Current original attempt admission incomplete')
        before = self.slot(ordinal)
        if ordinal and self.slot(ordinal - 1)[0] != 'completed':
            raise EvolutionPlanError('Actual preceding publication completion required')
        if before[0] != 'unprepared': raise EvolutionPlanError('Original attempt may not be replaced')
        with self.connection:
            count = self.connection.execute("UPDATE original_slot SET state='retained',original=?,digest=? WHERE ordinal=? AND state='unprepared'",
                (plan.raw, plan.sha256, ordinal)).rowcount
            if count != 1: raise EvolutionPlanError('Exclusive original retention lost')
        self.admit()

    def transition(self, ordinal, plan, state, descriptor=None):
        before = self.slot(ordinal)
        if before[1:3] != (plan.raw, plan.sha256): raise EvolutionPlanError('Exact original reservation differs')
        if self.policy.admit_attempt(self.original, ordinal, plan, self.context) is not None:
            raise EvolutionPlanError('Current original submission admission incomplete')
        if state == 'publication-started':
            if before[0] in ('publication-started', 'completed'): return
            if before[0] != 'retained': raise EvolutionPlanError('Positive original not-started reservation required')
        elif state == 'completed':
            if before[0] not in ('publication-started', 'completed') or type(descriptor) is not bytes:
                raise EvolutionPlanError('Original started publication and actual descriptor required')
            if before[0] == 'completed':
                if descriptor != before[3]: raise EvolutionPlanError('Original completed descriptor changed')
                return
        else: raise EvolutionPlanError('Closed original reservation transition required')
        with self.connection:
            count = self.connection.execute('UPDATE original_slot SET state=?,descriptor=? WHERE ordinal=? AND state=? AND original=? AND digest=?',
                (state, descriptor, ordinal, before[0], plan.raw, plan.sha256)).rowcount
            if count != 1: raise EvolutionPlanError('Original reservation transition raced or changed')
        self.admit()

    def attempt(self, ordinal): return EvolutionRunAttemptJournal(self, ordinal)
    def close(self):
        self.closed = True
        self.connection.close()


@dataclass(frozen=True)
class EvolutionRunAttemptJournal:
    ledger: EvolutionRunLedger = field(repr=False)
    ordinal: int

    def __post_init__(self):
        if type(self.ledger) is not EvolutionRunLedger or type(self.ordinal) is not int or not 0 <= self.ordinal < 8:
            raise EvolutionPlanError('Owned original run ledger and ordinal required')

    @property
    def policy(self): return self
    def admit(self, plan, context):
        if context is not self.ledger.context: raise EvolutionPlanError('Original run context required')
        state, raw, digest, descriptor = self.ledger.slot(self.ordinal)
        if (raw, digest) != (plan.raw, plan.sha256): raise EvolutionPlanError('Exact retained original run attempt required')
        if self.ledger.policy.admit_attempt(self.ledger.original, self.ordinal, plan, context) is not None:
            raise EvolutionPlanError('Current original run attempt admission incomplete')
    def load(self, *, expected_sha256, context):
        state, raw, digest, descriptor = self.ledger.slot(self.ordinal)
        if state == 'unprepared' or digest != expected_sha256:
            raise EvolutionPlanError('Retained original attempt digest required')
        plan = EvolutionAttemptPlan(raw); self.admit(plan, context); return plan
    def not_started(self, plan, context):
        self.admit(plan, context)
        if self.ledger.slot(self.ordinal)[0] != 'retained':
            raise EvolutionPlanError('Positive retained never-started submission custody required')
    def start(self, plan, context):
        self.admit(plan, context); self.ledger.transition(self.ordinal, plan, 'publication-started')
