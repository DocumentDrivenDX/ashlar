"""Private cooperative Fabric lifecycle storage (CONTRACT-003).

This module performs no cloud operations and implements no loaded-graph policy.
An active local pointer is correspondence ONLY: native load/bag/definition,
publication, source/ACK and HTTP owners must independently admit every use.
All participating local users must use these leases. Unmanaged writers are outside
this profile. The deliberately coarse lease excludes all mutations during reads.
"""
from contextlib import contextmanager
from dataclasses import asdict, dataclass
import fcntl
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
from uuid import UUID
from urllib.parse import quote

from ashlar_host.lifecycle import finish, owned_connection


class JournalRefusal(ValueError):
    """Invalid correspondence, stale predecessor or unavailable private lease."""


def _text(value):
    if type(value) is not str or not value or len(value) > 8192:
        raise JournalRefusal('expected bounded nonempty text')
    return value


def _sha(value):
    if type(value) is not str or re.fullmatch('[0-9a-f]{64}', value) is None:
        raise JournalRefusal('expected lowercase SHA256')


def _uuid(value):
    if type(value) is not str or str(UUID(value)) != value:
        raise JournalRefusal('expected canonical UUID')


@dataclass(frozen=True)
class ReleaseIntent:
    """Exact selected correspondence, never a grant of source/native authority.

    publication_json is the owner's complete UUID/version vector serialized as
    canonical JSON. mapping_json is the exact retained creation mapping JSON.
    sources names each fresh release-specific location and its content SHA256.
    """
    candidate_id: str
    workspace_id: str
    release_sha256: str
    publication_json: str
    mapping_json: str
    sources: tuple[tuple[str, str], ...]

    def __post_init__(self):
        _uuid(self.candidate_id)
        _uuid(self.workspace_id)
        _sha(self.release_sha256)
        for text in (self.publication_json, self.mapping_json):
            _text(text)
            value = json.loads(text)
            if type(value) is not dict or not value or _canonical(value) != text:
                raise JournalRefusal('expected nonempty canonical JSON object')
        if type(self.sources) is not tuple or not 1 <= len(self.sources) <= 128:
            raise JournalRefusal('expected bounded immutable source locations')
        locations = []
        for pair in self.sources:
            if type(pair) is not tuple or len(pair) != 2:
                raise JournalRefusal('expected location/digest pairs')
            locations.append(_text(pair[0]))
            _sha(pair[1])
        if len(set(locations)) != len(locations):
            raise JournalRefusal('duplicate source location')


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


@dataclass(frozen=True)
class LocalActive:
    revision: int
    intent: ReleaseIntent
    graph_model_id: str
    creation_handle: str


def _private(path, kind):
    info = path.lstat()
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise JournalRefusal('private owner-only path required')
    if not kind(info.st_mode):
        raise JournalRefusal('unexpected path kind')
    return info.st_dev, info.st_ino


def _unchanged(path, identity):
    if _private(path, stat.S_ISREG) != identity:
        raise JournalRefusal('journal/lease identity changed')


def _uri(path, mode):
    return f'file:{quote(str(path), safe="/")}?mode={mode}'


class FabricReleaseJournal:
    """One existing canonical mode-0700 directory; no implicit initialization.

    SQLite FULL synchronous transactions retain intents, uncertain submissions
    and predecessor CAS together. A real process-shared flock also excludes
    mutation while a local reader holds its pointer. Busy leases refuse promptly.
    """
    def __init__(self, root: Path):
        if type(root) is not Path and not isinstance(root, Path):
            raise JournalRefusal('root must be Path')
        if not root.is_absolute() or root.resolve() != root:
            raise JournalRefusal('root must be canonical absolute path')
        self.root = root
        self.identity = _private(root, stat.S_ISDIR)

    @classmethod
    def initialize(cls, root: Path):
        root.mkdir(mode=0o700)  # exclusive: never reopen or replace an old journal
        owner = cls(root)
        for name in ('lease', 'journal.sqlite'):
            fd = os.open(root / name, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(fd)
        with owner._lease(False), owner._db() as db:
            db.executescript('''
                CREATE TABLE candidates (id TEXT PRIMARY KEY, intent TEXT NOT NULL,
                    state TEXT NOT NULL, model TEXT UNIQUE, handle TEXT UNIQUE);
                CREATE TABLE active (singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    revision INTEGER NOT NULL, candidate TEXT);
                INSERT INTO active VALUES (1,0,NULL);
            ''')
        fd = os.open(root, os.O_RDONLY)
        primary = None
        try:
            os.fsync(fd)
        except BaseException as error:
            primary = error
        finish(primary, [lambda: os.close(fd)])
        return owner

    def _check(self):
        if _private(self.root, stat.S_ISDIR) != self.identity:
            raise JournalRefusal('journal root changed')

    @contextmanager
    def _lease(self, shared):
        self._check()
        path = self.root / 'lease'
        expected = _private(path, stat.S_ISREG)
        fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
        primary = None
        try:
            info = os.fstat(fd)
            if (info.st_dev, info.st_ino) != expected or info.st_nlink != 1:
                raise JournalRefusal('lease identity changed')
            try:
                fcntl.flock(fd, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise JournalRefusal('lease busy') from exc
            yield
        except BaseException as error:
            primary = error
        finish(primary, [self._check, lambda: _unchanged(path, expected), lambda: os.close(fd)])

    @contextmanager
    def _db(self):
        path = self.root / 'journal.sqlite'
        identity = _private(path, stat.S_ISREG)
        if path.stat().st_nlink != 1:
            raise JournalRefusal('journal hardlink refused')
        db = sqlite3.connect(_uri(path, 'rw'), uri=True, timeout=0)
        primary = None
        try:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('BEGIN IMMEDIATE')
            yield db
            self._check()
            _unchanged(path, identity)
            db.commit()
        except BaseException as error:
            primary = error
        finish(primary, ([db.rollback] if primary is not None else []) + [db.close])

    def record_intent(self, intent: ReleaseIntent):
        if type(intent) is not ReleaseIntent:
            raise JournalRefusal('exact ReleaseIntent required')
        with self._lease(False), self._db() as db:
            # Every retained candidate reserves its paths, even unknown/abandoned.
            for (raw,) in db.execute('SELECT intent FROM candidates'):
                prior = json.loads(raw)
                if set(x[0] for x in prior['sources']) & set(x[0] for x in intent.sources):
                    raise JournalRefusal('source location already reserved')
            db.execute('INSERT INTO candidates VALUES (?,?,?,NULL,NULL)',
                       (intent.candidate_id, _canonical(asdict(intent)), 'recorded'))

    def begin_creation(self, candidate_id: str):
        """Durably mark unknown BEFORE the one external creation submission.

        A crash or lost response leaves unknown; callers must recover the original
        operation. This API never authorizes a second submission.
        """
        with self._lease(False), self._db() as db:
            if db.execute('UPDATE candidates SET state=? WHERE id=? AND state=?',
                          ('unknown', candidate_id, 'recorded')).rowcount != 1:
                raise JournalRefusal('creation already submitted or missing intent')

    def record_created(self, candidate_id: str, graph_model_id: str, creation_handle: str):
        """Record original response/recovery correspondence; no load admission."""
        _uuid(graph_model_id)
        _text(creation_handle)
        with self._lease(False), self._db() as db:
            if db.execute('UPDATE candidates SET state=?,model=?,handle=? WHERE id=? AND state=?',
                          ('created', graph_model_id, creation_handle, candidate_id, 'unknown')).rowcount != 1:
                raise JournalRefusal('missing original uncertain submission')

    def candidate(self, candidate_id: str):
        """Recover exact original intent/status/handles; never discover replacements.

        The returned tuple is correspondence, not a native success receipt.
        """
        _uuid(candidate_id)
        with self._lease(True):
            path = self.root / 'journal.sqlite'
            identity = _private(path, stat.S_ISREG)
            with owned_connection(sqlite3.connect(_uri(path, 'ro'), uri=True)) as db:
                row = db.execute('SELECT intent,state,model,handle FROM candidates WHERE id=?',
                                 (candidate_id,)).fetchone()
            if _private(path, stat.S_ISREG) != identity:
                raise JournalRefusal('journal identity changed')
            if row is None:
                raise JournalRefusal('missing original intent')
            raw = json.loads(row[0])
            raw['sources'] = tuple(tuple(x) for x in raw['sources'])
            return ReleaseIntent(**raw), row[1], row[2], row[3]

    def activate_local(self, candidate_id: str, expected_revision: int):
        """Local CAS ONLY. Future native owner must admit BEFORE calling this.

        This storage primitive cannot prove native load/readback/bags or any
        source authority and is deliberately not a loaded_graph_policy port.
        """
        if type(expected_revision) is not int or expected_revision < 0:
            raise JournalRefusal('invalid predecessor revision')
        with self._lease(False), self._db() as db:
            row = db.execute('SELECT state FROM candidates WHERE id=?', (candidate_id,)).fetchone()
            if row != ('created',):
                raise JournalRefusal('original created item required')
            if db.execute('UPDATE active SET revision=revision+1,candidate=? WHERE singleton=1 AND revision=?',
                          (candidate_id, expected_revision)).rowcount != 1:
                raise JournalRefusal('stale activation predecessor')
            return expected_revision + 1

    @contextmanager
    def reader(self):
        """Hold one whole local binding and mutation exclusion until closing."""
        with self._lease(True):
            # Short SQLite read; process lease remains held through result use.
            path = self.root / 'journal.sqlite'
            identity = _private(path, stat.S_ISREG)
            with owned_connection(sqlite3.connect(_uri(path, 'ro'), uri=True)) as db:
                row = db.execute('SELECT revision,intent,model,handle FROM active JOIN candidates ON candidate=id WHERE singleton=1').fetchone()
            if row is None:
                raise JournalRefusal('no local active release')
            raw = json.loads(row[1])
            raw['sources'] = tuple(tuple(x) for x in raw['sources'])
            primary = None
            try:
                yield LocalActive(row[0], ReleaseIntent(**raw), row[2], row[3])
            except BaseException as error:
                primary = error
            finish(primary, [lambda: _unchanged(path, identity)])
