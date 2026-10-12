"""Stable exact native reader type; no journal initialization or mutations."""
import sqlite3
import json
import os
import stat
from pathlib import Path
from urllib.parse import quote
from .delta_custody import LocalDeltaTransport, LocalDeltaError, DeltaTarget, operation_capacity, encoded
from .lifecycle import finish, owned_context


def original_read_capacity(journal, installation, targets):
    """Select existing closed registration correspondence; never grant authority.

    The transport subsequently admits the same registry, reservations and actual
    native UUIDs under its current policy. No journal or reservation is created.
    """
    if (type(journal) is not type(Path()) or not journal.is_absolute()
            or journal.resolve()!=journal or journal.is_symlink() or not journal.is_file()
            or type(installation) is not str or type(targets) not in (list,tuple)
            or not targets or len(targets)>64):
        raise LocalDeltaError('Exact original read registration required')
    for target in targets:
        if (type(target) is not DeltaTarget or type(target.table) is not str
                or type(target.path) is not type(Path()) or type(target.uuid) is not str
                or target.path.resolve()!=target.path or target.path.is_symlink()):
            raise LocalDeltaError('Exact original target required')
        target.__post_init__()
    if len({t.table for t in targets})!=len(targets) or len({t.path for t in targets})!=len(targets):
        raise LocalDeltaError('Complete distinct original targets required')
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise LocalDeltaError('Duplicate original registry key')
            result[key]=value
        return result
    def integer(text):
        if len(text)>7:raise LocalDeltaError('Bounded original registry integer required')
        return int(text)
    connection=sqlite3.connect('file:'+quote(str(journal))+'?mode=ro',uri=True)
    primary=None;rows=None
    try:
        rows=connection.execute('SELECT length(original),substr(original,1,1048577) FROM local_installation WHERE id=1').fetchall()
    except BaseException as error:primary=error
    finally:finish(primary,[connection.close])
    if len(rows)!=1 or type(rows[0][0]) is not int or not 0<rows[0][0]<=1048576 or type(rows[0][1]) is not str or len(rows[0][1].encode('utf8'))>1048576:
        raise LocalDeltaError('Bounded single original registry required')
    raw=rows[0][1]
    registry=json.loads(raw,object_pairs_hook=pairs,parse_int=integer,
        parse_constant=lambda value:(_ for _ in ()).throw(LocalDeltaError('Finite original registry required')))
    expected={'installation':installation,'targets':[{'table':t.table,'path':str(t.path),'uuid':t.uuid} for t in sorted(targets,key=lambda t:t.table)]}
    if type(registry) is not dict or set(registry) not in (set(expected),set(expected)|{'operation_capacity'}):
        raise LocalDeltaError('Closed original registry required')
    capacity=None
    if 'operation_capacity' in registry:
        if registry['operation_capacity'] is None:raise LocalDeltaError('Present original capacity required')
        capacity=operation_capacity(registry['operation_capacity'])
        expected['operation_capacity']=dict(capacity)
    if registry!=expected or raw!=encoded(expected):
        raise LocalDeltaError('Original installation or complete targets differ')
    reservation=(encoded({'profile':'ashlar-private-native-installation-reservation/0.1',
        'registry':expected,'original_journal_path':str(journal)})+'\n').encode()
    for target in targets:
        path=target.path/'.ashlar-local-installation-reservation.json';info=path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode&0o022:
            raise LocalDeltaError('Original private reservation required')
        with owned_context(path.open('rb')) as stream:payload=stream.read(1048577)
        if payload!=reservation:raise LocalDeltaError('Original complete reservation differs')
    return capacity


class ReadOnlyDeltaTransport(LocalDeltaTransport):
    def _journal(self, registry, *, create):
        if create:
            raise LocalDeltaError('Read-only consumer cannot initialize')
        self.db = sqlite3.connect('file:' + quote(str(self.journal_path.resolve())) + '?mode=ro', uri=True)
        try:
            if self.db.execute('SELECT original FROM local_installation WHERE id=1').fetchall() != [(registry,)]:
                raise LocalDeltaError('Original journal registration differs')
        except BaseException as error:
            finish(error, [self.db.close])

    def mutation(self, *args, **kwargs):
        raise LocalDeltaError('Read-only publication consumer')

    def recover(self, *args, **kwargs):
        raise LocalDeltaError('Read-only publication consumer')


class ReadOnlyTransport:
    @staticmethod
    def open(spark, journal, installation, targets, policy, *, capacity=None, verification_bounds=None):
        """Select optional constructor capture bounds without changing read authority."""
        if verification_bounds is None:
            return ReadOnlyDeltaTransport(spark, journal, installation, targets, policy, capacity=capacity)
        return ReadOnlyDeltaTransport(spark, journal, installation, targets, policy,
            capacity=capacity, verification_bounds=verification_bounds)
