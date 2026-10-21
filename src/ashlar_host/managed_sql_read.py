# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bounded existing-warehouse read transport inside an owned supervised child.

Routing accepts trusted internal SELECT/DESCRIBE/SHOW only; it is not an SQL
sandbox or publication policy. SDK HTTP pools belong to this child process:
SDK0.117 has no public client close port. Outer supervision must bound/terminate
the child; local HTTP/retry limits alone are not whole-request guarantees.
"""
from __future__ import annotations
from contextlib import contextmanager
from dataclasses import dataclass,field
import hashlib
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import stat
import time
from urllib.parse import urlsplit
from ashlar.native import SQLResult
from .lifecycle import finish

SHARED_ENDPOINT=('https://adb-7405607548213398.18.azuredatabricks.net','2439e1f2e37ac563')

class ManagedSQLReadError(ValueError):pass

@dataclass(frozen=True)
class ManagedSQLReadConfig:
    host:str
    profile:str
    config_file:Path=field(repr=False)
    cli:Path=field(repr=False)
    warehouse_id:str
    warehouse_name:str
    warehouse_creator:str
    actor_id:str
    actor_name:str
    statement_seconds:int
    http_seconds:int
    maximum_rows:int
    maximum_result_bytes:int
    maximum_statements:int=64

    def __post_init__(self):
        def bounded(v,a,b):return type(v)is int and a<=v<=b
        try:
            host=urlsplit(self.host)
            good=(type(self.host)is str and self.host=='https://'+host.netloc and host.hostname and host.netloc==host.hostname and host.port is None
                and host.username is None and host.password is None and not host.query and not host.fragment
                and all(type(v)is str and 0<len(v.encode())<=255 and '\0' not in v for v in (self.profile,self.warehouse_name,self.warehouse_creator,self.actor_id,self.actor_name))
                and type(self.warehouse_id)is str and re.fullmatch('[0-9a-f]{16}',self.warehouse_id)
                and (self.host,self.warehouse_id)!=SHARED_ENDPOINT
                and all(type(p)is type(Path()) and p.is_absolute() and p.resolve()==p for p in (self.config_file,self.cli))
                and bounded(self.statement_seconds,1,60) and bounded(self.http_seconds,1,10)
                and bounded(self.maximum_rows,1,1024) and bounded(self.maximum_result_bytes,1,1048576)
                and bounded(self.maximum_statements,1,128))
        except Exception:good=False
        if not good:raise ManagedSQLReadError('managed-sql-configuration') from None

@dataclass(frozen=True)
class ManagedSQLChildContext:
    pid:int
    session:int
    group:int


def managed_sql_child_context()->ManagedSQLChildContext:
    value=ManagedSQLChildContext(os.getpid(),os.getsid(0),os.getpgrp())
    _require_child(value)
    return value


def _require_child(context):
    if (type(context)is not ManagedSQLChildContext or context.pid!=os.getpid()
            or context.session!=os.getsid(0) or context.group!=os.getpgrp()
            or context.session!=os.getppid() or context.group!=context.session
            or context.pid==context.session):
        raise ManagedSQLReadError('managed-sql-supervised-child-required')


def _safe(action):
    try:return action()
    except Exception:raise ManagedSQLReadError('managed-sql-refused') from None


def _encoded(value):return json.dumps(value,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()


def _file(path,private):
    try:
        value=path.lstat()
        if (path.resolve()!=path or not stat.S_ISREG(value.st_mode) or value.st_uid!=os.getuid()
                or private and (stat.S_IMODE(value.st_mode)!=0o600 or not 0<value.st_size<=65536)
                or not private and (not os.access(path,os.X_OK) or value.st_mode&0o022)):
            raise ValueError()
        return(value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns)
    except Exception:raise ManagedSQLReadError('managed-sql-configuration') from None


def decode_managed_sql_result(response,*,maximum_rows,maximum_bytes):
    """Retain only complete single-inline-chunk exact STRING/null carriers."""
    try:
        if type(maximum_rows)is not int or not 1<=maximum_rows<=1024 or type(maximum_bytes)is not int or not 1<=maximum_bytes<=1048576:raise ValueError()
        if type(response)is not dict or response['status']['state']!='SUCCEEDED':raise ValueError()
        if len(_encoded(response))>maximum_bytes+131072:raise ValueError()
        manifest=response['manifest'];result=response.get('result',{})
        if (manifest['format']!='JSON_ARRAY' or manifest['truncated']is not False
                or type(manifest['total_row_count'])is not int or not 0<=manifest['total_row_count']<=maximum_rows
                or type(manifest['total_chunk_count'])is not int or manifest['total_chunk_count'] not in (0,1)):
            raise ValueError()
        schema=manifest['schema'];columns=schema['columns']
        if type(columns)is not list or not 1<=len(columns)<=64 or type(schema['column_count'])is not int or schema['column_count']!=len(columns):raise ValueError()
        names=[];types=[]
        for index,column in enumerate(columns):
            if (type(column)is not dict or type(column['position'])is not int or column['position']!=index
                    or any(type(column[k])is not str or not 0<len(column[k].encode())<=4096 for k in ('name','type_text'))):raise ValueError()
            names.append(column['name']);types.append(column['type_text'])
        if len(set(names))!=len(names):raise ValueError()
        count=manifest['total_row_count'];chunks=manifest['total_chunk_count'];rows=result.get('data_array',[])
        if (type(result)is not dict or result.get('external_links') or result.get('next_chunk_index')is not None
                or result.get('next_chunk_internal_link')is not None or type(rows)is not list or len(rows)!=count):raise ValueError()
        if chunks==0:
            if count!=0 or rows or manifest.get('chunks',[]):raise ValueError()
        else:
            if (type(result.get('chunk_index'))is not int or result['chunk_index']!=0
                    or type(result.get('row_offset'))is not int or result['row_offset']!=0
                    or type(result.get('row_count'))is not int or result['row_count']!=count):raise ValueError()
            chunk=manifest['chunks']
            if (type(chunk)is not list or len(chunk)!=1 or any(type(chunk[0].get(k))is not int for k in ('chunk_index','row_offset','row_count')) or chunk[0].get('chunk_index')!=0
                    or chunk[0].get('row_offset')!=0 or chunk[0].get('row_count')!=count):raise ValueError()
        size=0;retained=[]
        for row in rows:
            if type(row)is not list or len(row)!=len(names) or any(v is not None and type(v)is not str for v in row):raise ValueError()
            size+=len(_encoded(row))
            if size>maximum_bytes:raise ValueError()
            retained.append(dict(zip(names,row)))
        return SQLResult(retained,tuple(zip(names,types)))
    except Exception:raise ManagedSQLReadError('managed-sql-result-refused') from None


class ManagedSQLReader:
    """Factory-owned live SDK/session port; dedication is an operator prerequisite."""
    def __init__(self,config,context):
        if type(config)is not ManagedSQLReadConfig:raise ManagedSQLReadError('managed-sql-configuration')
        config.__post_init__();_require_child(context)
        if any(name.startswith(('DATABRICKS_','ARM_','AZURE_','MSI_')) or name.upper() in ('HTTP_PROXY','HTTPS_PROXY','ALL_PROXY') for name in os.environ):raise ManagedSQLReadError('managed-sql-configuration')
        self.config=config;self.context=context;self.active=True;self.pending=set();self.observations=[];self.count=0;self.submission_outcome_unknown=False
        self._settings=tuple(getattr(config,n) for n in config.__dataclass_fields__)
        self._files=(_file(config.config_file,True),_file(config.cli,False))
        if importlib.metadata.version('databricks-sdk')!='0.155.0':raise ManagedSQLReadError('managed-sql-sdk-version')
        sdk=importlib.import_module('databricks.sdk')
        self.workspace=_safe(lambda:sdk.WorkspaceClient(host=config.host,profile=config.profile,config_file=str(config.config_file),
            auth_type='databricks-cli',databricks_cli_path=str(config.cli),http_timeout_seconds=config.http_seconds,retry_timeout_seconds=1))
        self._workspace=self.workspace
        self.renew()

    def renew(self):
        _require_child(self.context);c=self.config
        if (not self.active or self.workspace is not self._workspace
                or tuple(getattr(c,n) for n in c.__dataclass_fields__)!=self._settings
                or (_file(c.config_file,True),_file(c.cli,False))!=self._files):raise ManagedSQLReadError('managed-sql-custody')
        def observe():
            cfg=self.workspace.config
            if (cfg.host!=c.host or cfg.profile!=c.profile or cfg.auth_type!='databricks-cli'
                    or cfg.databricks_cli_path!=str(c.cli) or cfg.config_file!=str(c.config_file)):raise ValueError()
            actor=self.workspace.current_user.me();warehouse=self.workspace.warehouses.get(c.warehouse_id)
            state=getattr(warehouse.state,'value',None)
            if (actor.id!=c.actor_id or actor.user_name!=c.actor_name or warehouse.id!=c.warehouse_id
                    or warehouse.name!=c.warehouse_name or warehouse.creator_name!=c.warehouse_creator
                    or state!='RUNNING' or warehouse.enable_serverless_compute is not True):raise ValueError()
        _safe(observe)

    def _state(self,response,expected_id=None):
        try:
            sid=response['statement_id'];state=response['status']['state']
            if type(sid)is not str or re.fullmatch('[A-Za-z0-9-]{1,128}',sid)is None or expected_id is not None and sid!=expected_id:raise ValueError()
            if state not in ('PENDING','RUNNING','SUCCEEDED','FAILED','CANCELED','CLOSED'):raise ValueError()
            return sid,state
        except Exception:raise ManagedSQLReadError('managed-sql-statement-refused') from None

    def _request(self,method,path,body=None):
        return _safe(lambda:self.workspace.api_client.do(method,path,body=body))

    def query(self,sql,parameters):
        self.renew();c=self.config
        if (type(sql)is not str or not sql.strip() or not 0<len(sql.encode())<=65536 or sql.lstrip().split(None,1)[0].upper() not in ('SELECT','DESCRIBE','SHOW')
                or type(parameters)is not dict or len(parameters)>64
                or any(type(k)is not str or re.fullmatch('[a-z_]+',k)is None or type(v)is not str or len(v.encode())>65536 for k,v in parameters.items())
                or self.pending or self.count>=c.maximum_statements):raise ManagedSQLReadError('managed-sql-statement-refused')
        self.count+=1;deadline=time.monotonic()+c.statement_seconds
        try:
            response=self._request('POST','/api/2.0/sql/statements',dict(warehouse_id=c.warehouse_id,statement=sql,
                parameters=[dict(name=k,type='STRING',value=v) for k,v in parameters.items()],wait_timeout='0s',on_wait_timeout='CONTINUE',
                disposition='INLINE',format='JSON_ARRAY',row_limit=c.maximum_rows+1,byte_limit=c.maximum_result_bytes))
        except BaseException:
            self.submission_outcome_unknown=True
            raise
        sid=response.get('statement_id') if type(response)is dict else None
        if type(sid)is str and re.fullmatch('[A-Za-z0-9-]{1,128}',sid):self.pending.add(sid)
        else:self.submission_outcome_unknown=True
        sid,state=self._state(response)
        def returned(state):
            if state not in ('PENDING','RUNNING'):
                self.pending.discard(sid);self.observations.append((sid,state))
            if time.monotonic()>=deadline:raise ManagedSQLReadError('managed-sql-statement-timeout')
        returned(state)
        while state in ('PENDING','RUNNING'):
            if time.monotonic()>=deadline:raise ManagedSQLReadError('managed-sql-statement-timeout')
            time.sleep(min(.1,max(0,deadline-time.monotonic())))
            response=self._request('GET','/api/2.0/sql/statements/'+sid);_,state=self._state(response,sid)
            returned(state)
        if state!='SUCCEEDED':raise ManagedSQLReadError('managed-sql-statement-refused')
        result=decode_managed_sql_result(response,maximum_rows=c.maximum_rows,maximum_bytes=c.maximum_result_bytes)
        self.renew()
        if time.monotonic()>=deadline:raise ManagedSQLReadError('managed-sql-statement-timeout')
        return result

    @property
    def statement_observations(self):return tuple(self.observations)

    def close(self):
        primary=None
        def settle(sid):
            self._request('POST','/api/2.0/sql/statements/'+sid+'/cancel')
            end=time.monotonic()+self.config.statement_seconds
            while True:
                response=self._request('GET','/api/2.0/sql/statements/'+sid);_,state=self._state(response,sid)
                if state not in ('PENDING','RUNNING'):
                    self.pending.remove(sid);self.observations.append((sid,state));return
                if time.monotonic()>=end:raise ManagedSQLReadError('managed-sql-close-incomplete')
                time.sleep(.1)
        def uncertainty():
            if self.submission_outcome_unknown:raise ManagedSQLReadError('managed-sql-submission-unknown')
        def deactivate():self.active=False
        finish(primary,[lambda sid=sid:settle(sid) for sid in sorted(self.pending)]+[self.renew,uncertainty,deactivate])


@contextmanager
def open_managed_sql_read(config:ManagedSQLReadConfig,*,context:ManagedSQLChildContext):
    reader=ManagedSQLReader(config,context);primary=None
    try:yield reader
    except BaseException as error:primary=error
    finish(primary,[reader.close])
