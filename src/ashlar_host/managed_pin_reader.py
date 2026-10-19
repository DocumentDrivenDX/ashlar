# SPDX-License-Identifier: MIT OR Apache-2.0
"""Explicit direct ordinary pin-reader login; no service or credential discovery.

This owner supplies real transaction lifetime only. Managed publication/source
admission and the original registered PinVector remain independent prerequisites.
"""
from __future__ import annotations
from contextlib import contextmanager
from dataclasses import dataclass,field
import importlib
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
from .lifecycle import finish,owned_context
from .postgres import Session
from ashlar.pins import INVENTORY_SQL

PROFILE='ashlar-managed-pin-reader/0.1'

class ManagedPinReaderError(ValueError):
    """Fixed safe refusal; never contains driver/credential diagnostics."""

@dataclass(frozen=True)
class ManagedPinReaderConfig:
    profile:str
    host:str
    hostaddr:str
    port:int
    expected_server_address:str
    expected_server_port:int
    dbname:str
    user:str
    sslmode:str
    passfile:Path=field(repr=False)
    connect_timeout_seconds:int
    statement_timeout_ms:int
    lock_timeout_ms:int

    def __post_init__(self):
        def exact(n,lo,hi):return type(n)is int and lo<=n<=hi
        try:
            valid=(type(self.profile)is str and self.profile==PROFILE
                and type(self.host)is str and self.host=='127.0.0.1'
                and type(self.hostaddr)is str and self.hostaddr=='127.0.0.1'
                and type(self.sslmode)is str and self.sslmode=='disable'
                and exact(self.port,1024,65535) and exact(self.expected_server_port,1,65535)
                and type(self.expected_server_address)is str
                and '%' not in self.expected_server_address
                and bool(ipaddress.ip_address(self.expected_server_address))
                and all(type(v)is str and re.fullmatch('[a-z][a-z0-9_]{0,62}',v) for v in (self.dbname,self.user))
                and type(self.passfile)is type(Path()) and self.passfile.is_absolute()
                and self.passfile.resolve()==self.passfile
                and exact(self.connect_timeout_seconds,2,5)
                and exact(self.statement_timeout_ms,1,15000)
                and exact(self.lock_timeout_ms,1,5000)
                and self.lock_timeout_ms<=self.statement_timeout_ms)
        except Exception:valid=False
        if not valid:raise ManagedPinReaderError('managed-pin-reader-configuration') from None


def parse_managed_pin_reader(raw:bytes)->ManagedPinReaderConfig:
    """Decode the closed C002 object, without opening credential files or SDKs."""
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError()
            result[key]=value
        return result
    def refuse(_):raise ValueError()
    try:
        if type(raw)is not bytes or len(raw)>16384:raise ValueError()
        value=json.loads(raw,object_pairs_hook=pairs,parse_float=refuse,parse_constant=refuse)
        if type(value)is not dict or set(value)!=set(ManagedPinReaderConfig.__dataclass_fields__):raise ValueError()
        if type(value['passfile'])is not str:raise ValueError()
        value['passfile']=Path(value['passfile'])
        return ManagedPinReaderConfig(**value)
    except Exception:raise ManagedPinReaderError('managed-pin-reader-configuration') from None


def _file_identity(path):
    try:
        facts=path.lstat()
        if (path.resolve()!=path or not stat.S_ISREG(facts.st_mode) or facts.st_uid!=os.getuid()
                or stat.S_IMODE(facts.st_mode)!=0o600 or not 0<facts.st_size<=65536):raise ValueError()
        return (facts.st_dev,facts.st_ino,facts.st_size,facts.st_mtime_ns,facts.st_ctime_ns)
    except Exception:raise ManagedPinReaderError('managed-pin-reader-configuration') from None


def _safe(action):
    try:return action()
    except Exception:raise ManagedPinReaderError('managed-pin-reader-connection') from None


class _CursorConnection:
    """Keep existing Session SQL/bounds while preserving cursor body primaries."""
    def __init__(self,connection):self.connection=connection
    @contextmanager
    def cursor(self):
        with owned_context(self.connection.cursor()) as cursor:yield cursor


class ManagedPinSession(Session):
    def __init__(self,connection):super().__init__(_CursorConnection(connection))
    def query(self,sql,parameters):
        allowed=(INVENTORY_SQL,_ADMISSION,
            'SELECT ashlar_pins.assert_uuid_available(:uuid)',
            "SELECT ashlar_pins.assert_active(:id,:authority,:kind,:scope,:table,:uuid,CAST(:version AS bigint),decode(:digest,'hex'))")
        if type(sql)is not str or sql not in allowed:raise ManagedPinReaderError('managed-pin-reader-statement')
        return _safe(lambda:super(ManagedPinSession,self).query(sql,parameters))


# Names/signatures are the fixed existing pin service, never caller SQL/options.
_ADMISSION="""SELECT current_user::text AS current_user,session_user::text AS session_user,
current_database()::text AS database,inet_server_addr()::text AS server_address,
inet_server_port()::text AS server_port,
r.rolcanlogin AS login,r.rolsuper AS superuser,r.rolcreaterole AS create_role,
r.rolcreatedb AS create_db,r.rolreplication AS replication,r.rolbypassrls AS bypass_rls,
has_schema_privilege(current_user,'ashlar_pins','USAGE') AS schema_usage,
has_schema_privilege(current_user,'ashlar_pins','CREATE') AS schema_create,
has_table_privilege(current_user,'ashlar_pins.pin','SELECT') AS inventory_read,
has_table_privilege(current_user,'ashlar_pins.pin','INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') AS table_write,
has_function_privilege(current_user,'ashlar_pins.assert_active(text,text,text,text,text,text,bigint,bytea)','EXECUTE') AS assert_active,
has_function_privilege(current_user,'ashlar_pins.assert_uuid_available(text)','EXECUTE') AS assert_available,
has_function_privilege(current_user,'ashlar_pins.register(text,text,text,text,text,text,bigint,bytea)','EXECUTE') AS register,
has_function_privilege(current_user,'ashlar_pins.release(text,text,bytea)','EXECUTE') AS release,
has_function_privilege(current_user,'ashlar_pins.assert_unpinned(text,text,bigint)','EXECUTE') AS maintenance,
has_function_privilege(current_user,'ashlar_pins.assert_table_unpinned(text,text)','EXECUTE') AS table_maintenance,
has_function_privilege(current_user,'ashlar_pins.quarantine_cleanup(text,text,text,bytea)','EXECUTE') AS quarantine_cleanup,
has_function_privilege(current_user,'ashlar_pins.close_cleanup_quarantine(text,bytea,bytea)','EXECUTE') AS close_quarantine,
has_table_privilege(current_user,'ashlar_pins.maintenance_quarantine','INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') AS quarantine_write,
pg_has_role(current_user,'ashlar_pin_writer','MEMBER') AS writer_member,
pg_has_role(current_user,'ashlar_pin_maintenance','MEMBER') AS maintenance_member,
pg_has_role(current_user,'ashlar_pin_recovery','MEMBER') AS recovery_member,
(SELECT setting FROM pg_settings WHERE name='statement_timeout') AS statement_timeout,
(SELECT setting FROM pg_settings WHERE name='lock_timeout') AS lock_timeout
FROM pg_roles r WHERE r.rolname=current_user"""
_REQUIRED=('login','schema_usage','inventory_read','assert_active','assert_available')
_FORBIDDEN=('superuser','create_role','create_db','replication','bypass_rls','schema_create','table_write','register','release','maintenance','table_maintenance','quarantine_cleanup','close_quarantine','quarantine_write','writer_member','maintenance_member','recovery_member')


def _admit(connection,config):
    session=ManagedPinSession(connection)
    try:
        rows=session.query(_ADMISSION,{}).rows
        if len(rows)!=1:raise ValueError()
        row=rows[0]
        keys={'current_user','session_user','database','server_address','server_port','statement_timeout','lock_timeout',*_REQUIRED,*_FORBIDDEN}
        if set(row)!=keys:raise ValueError()
        if (row['current_user']!=config.user or row['session_user']!=config.user or row['database']!=config.dbname
                or type(row['server_address'])is not str or '%' in row['server_address']
                or ipaddress.ip_address(row['server_address'])!=ipaddress.ip_address(config.expected_server_address)
                or row['server_port']!=str(config.expected_server_port)
                or row['statement_timeout']!=str(config.statement_timeout_ms)
                or row['lock_timeout']!=str(config.lock_timeout_ms)
                or any(row[key]is not True for key in _REQUIRED)
                or any(row[key]is not False for key in _FORBIDDEN)):raise ValueError()
    except Exception:raise ManagedPinReaderError('managed-pin-reader-connection') from None
    finally:session.active=False


class ManagedPinReader:
    """Concrete transaction port for PostgresPins; never registers a pin itself."""
    def __init__(self,config:ManagedPinReaderConfig,context):
        if type(config)is not ManagedPinReaderConfig:raise ManagedPinReaderError('managed-pin-reader-configuration')
        config.__post_init__()
        self.config=config;self.context=context;self.active=False
        self._original=(config,context,tuple(getattr(config,name) for name in ManagedPinReaderConfig.__dataclass_fields__))

    @contextmanager
    def transaction(self,context):
        config,original_context,settings=self._original
        if self.config is not config or self.context is not original_context or context is not original_context or self.active or tuple(getattr(config,name) for name in ManagedPinReaderConfig.__dataclass_fields__)!=settings:
            raise ManagedPinReaderError('managed-pin-reader-context')
        config.__post_init__()
        if any(name.startswith('PG') for name in os.environ):raise ManagedPinReaderError('managed-pin-reader-configuration')
        identity=_file_identity(config.passfile);connection=None;session=None;primary=None
        self.active=True
        def renew():
            if tuple(getattr(config,name) for name in ManagedPinReaderConfig.__dataclass_fields__)!=settings:raise ManagedPinReaderError('managed-pin-reader-configuration')
            if _file_identity(config.passfile)!=identity:raise ManagedPinReaderError('managed-pin-reader-configuration')
        try:
            driver=importlib.import_module('psycopg')
            connection=_safe(lambda:driver.connect(host=config.host,hostaddr=config.hostaddr,port=config.port,
                dbname=config.dbname,user=config.user,sslmode=config.sslmode,gssencmode='disable',passfile=str(config.passfile),
                connect_timeout=config.connect_timeout_seconds,options='-c statement_timeout='+str(config.statement_timeout_ms)+' -c lock_timeout='+str(config.lock_timeout_ms),autocommit=False))
            _safe(lambda:connection.add_notice_handler(lambda notice:None))
            renew()
            if connection.autocommit or connection.info.transaction_status!=0:raise ManagedPinReaderError('managed-pin-reader-connection')
            _admit(connection,config)
            session=ManagedPinSession(connection)
            try:yield session
            except BaseException as error:primary=error
        except BaseException as error:
            primary=error if not isinstance(error,Exception) or isinstance(error,ManagedPinReaderError) else ManagedPinReaderError('managed-pin-reader-connection')
        closing_failed=False
        def checked(action):
            nonlocal closing_failed
            try:return action()
            except BaseException:
                closing_failed=True
                raise
        def conclude():
            if primary is not None or closing_failed:return _safe(connection.rollback)
            try:return _safe(connection.commit)
            except BaseException as error:finish(error,[lambda:_safe(connection.rollback)])
        def end():
            if session is not None:session.active=False
            self.active=False
        actions=[]
        if connection is not None:
            if session is not None:actions.append(lambda:checked(lambda:_admit(connection,config)))
            actions.extend([lambda:checked(renew),conclude,lambda:_safe(connection.close)])
        else:actions.append(renew)
        actions.append(end)
        finish(primary,actions)
