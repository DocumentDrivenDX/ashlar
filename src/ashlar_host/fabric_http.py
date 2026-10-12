"""Finite private-pipe HTTP execution; live credential/runtime support unqualified.

Loopback profiles exercise actual processes/sockets and cannot authorize Fabric.
"""
import base64
from contextlib import contextmanager, ExitStack
from dataclasses import dataclass, fields
import json
import math
import os
import re
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import time
from urllib.parse import urlsplit
from .fabric_http_configuration import FabricHttpConfig, fabric_http_config_values
from .fabric_cli_runtime import (FabricInertCliRuntimeSelection, FabricLoopbackRuntimeSelection,
                                 FabricCliRuntimeSelection,FabricHttpRuntimeSelection,CLI_ROOT,HTTP_PYTHON,
                                 FabricRuntimeCustody,hold_fabric_cli_runtime)
from .lifecycle import finish

_WORKER = r'''
import sys,json,base64,urllib.request,urllib.error,ssl
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args,**kwargs):return None
try:
 v=json.loads(sys.stdin.buffer.read())
 handlers=[urllib.request.ProxyHandler({}),NoRedirect()]
 if v['url'].startswith('https:'):handlers.append(urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=v['ca'])))
 opener=urllib.request.build_opener(*handlers)
 req=urllib.request.Request(v['url'],data=base64.b64decode(v['body']) if v['method']=='POST' else None,method=v['method'],headers={'Authorization':'Bearer '+v['token'],'Content-Type':'application/json','Accept':'application/json','Accept-Encoding':'identity'})
 response=None
 try:
  try:response=opener.open(req,timeout=v['timeout'])
  except urllib.error.HTTPError as e:response=e
  raw=response.read(v['maximum']+1)
  headers={k.lower():response.headers.get(k) for k in ('Location','Retry-After','x-ms-operation-id','Content-Encoding') if response.headers.get(k) is not None}
  if sum(len(k)+len(x) for k,x in headers.items())>16384:raise ValueError('headers')
  result={'status':response.code,'raw':base64.b64encode(raw).decode(),'headers':headers,'overflow':len(raw)>v['maximum']}
 finally:
  if response is not None:response.close()
 sys.stdout.write(json.dumps(result,separators=(',',':')))
except BaseException as e:
 sys.stdout.write(json.dumps({'failure':type(e).__name__}))
 sys.exit(2)
'''


_SUPERVISOR = """import os,signal,subprocess,sys
fd=int(sys.argv[1])
child=subprocess.Popen(sys.argv[2:],stdin=sys.stdin.buffer,close_fds=True)
status=child.wait()
os.close(1);os.close(2)
os.write(fd,(str(status)+'\\n').encode('ascii'));os.close(fd)
while True: signal.pause()
"""


def _capture(argv, payload, *, environment, cwd, timeout, maximum, deadline, total_maximum=None):
    """Private stdin and retained supervisor leader, killed before reap."""
    start=time.monotonic()
    if (type(timeout)not in (int,float) or type(timeout)is bool or not math.isfinite(timeout) or timeout<=0
            or type(deadline)not in (int,float) or type(deadline)is bool or not math.isfinite(deadline)):
        raise ValueError('Original finite absolute deadline required')
    end=min(deadline,start+timeout)
    process=None;selector=None;primary=None;streams=[];control_read=control_write=None
    output=[bytearray(),bytearray(),bytearray()]
    try:
        control_read,control_write=os.pipe()
        if time.monotonic()>=end:raise TimeoutError('Fabric deadline elapsed before spawn')
        process=subprocess.Popen([sys.executable,'-I','-S','-B','-c',_SUPERVISOR,str(control_write),*argv],
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,cwd=cwd,env=environment,
            start_new_session=True,pass_fds=(control_write,))
        streams=[process.stdin,process.stdout,process.stderr]
        if time.monotonic()>=end:raise TimeoutError('Fabric deadline elapsed during spawn/setup')
        os.close(control_write);control_write=None
        control=os.fdopen(control_read,'rb',buffering=0);control_read=None
        streams=[process.stdin,process.stdout,process.stderr,control]
        selector=selectors.DefaultSelector();offset=0
        for index,stream in enumerate((process.stdout,process.stderr,control)):
            os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,index)
        os.set_blocking(process.stdin.fileno(),False)
        if payload:selector.register(process.stdin,selectors.EVENT_WRITE,3)
        else:process.stdin.close()
        while selector.get_map():
            remaining=end-time.monotonic()
            if remaining<=0:raise TimeoutError('Finite Fabric process deadline exceeded')
            for key,_ in selector.select(min(remaining,.05)):
                if key.data==3:
                    if time.monotonic()>=end:raise TimeoutError('Fabric deadline elapsed before private payload write')
                    offset+=os.write(key.fileobj.fileno(),payload[offset:offset+65536])
                    if offset==len(payload):selector.unregister(key.fileobj);key.fileobj.close()
                else:
                    limit=maximum if key.data==0 else 16384 if key.data==1 else 16
                    block=os.read(key.fileobj.fileno(),min(65536,limit-len(output[key.data])+1))
                    if not block:selector.unregister(key.fileobj)
                    else:
                        output[key.data].extend(block)
                        if len(output[key.data])>limit or (total_maximum is not None and len(output[0])+len(output[1])>total_maximum):raise ValueError('Finite Fabric process output exceeded')
        status_bytes=bytes(output[2]);status=int(status_bytes)
        if status_bytes!=(str(status)+'\n').encode() or not -255<=status<=255:
            raise ValueError('Original child status required')
    except BaseException as error:primary=error
    actions=[]
    if process is not None:
        def kill():
            try:os.killpg(process.pid,signal.SIGKILL)
            except ProcessLookupError:pass
        actions.extend([kill,lambda:process.wait(timeout=1)])
    for fd in (control_read,control_write):
        if fd is not None:actions.append(lambda fd=fd:os.close(fd))
    if selector is not None:actions.append(selector.close)
    actions.extend(stream.close for stream in streams)
    finish(primary,actions)
    return status,bytes(output[0]),bytes(output[1])


def _decode(raw):
    def pairs(items):
        value={}
        for key,item in items:
            if key in value:raise ValueError('Duplicate native JSON member')
            value[key]=item
        return value
    def integer(token):
        if len(token.lstrip('-'))>20:raise ValueError('Native integer bound')
        return int(token)
    def refuse(token):raise ValueError('Noncanonical native JSON number')
    return json.loads(raw,object_pairs_hook=pairs,parse_int=integer,parse_float=refuse,parse_constant=refuse)


@dataclass(frozen=True)
class FabricLoopbackTestConfig:
    config: FabricHttpConfig
    cli_runtime: FabricInertCliRuntimeSelection
    http_runtime: FabricLoopbackRuntimeSelection

    def __post_init__(self):
        if (type(self.config)is not FabricHttpConfig or type(self.cli_runtime)is not FabricInertCliRuntimeSelection
                or type(self.http_runtime)is not FabricLoopbackRuntimeSelection
                or self.config.cli_runtime is not self.cli_runtime or self.config.http_runtime is not self.http_runtime
                or self.config.azure_cli!=self.cli_runtime.executable
                or type(self.http_runtime.port)is not int or not 1<=self.http_runtime.port<=65535
                or self.http_runtime.interpreter.resolve()!=self.http_runtime.interpreter):
            raise ValueError('Exact separate inert loopback profile required')


@dataclass(frozen=True)
class _HttpOriginal:
    config: FabricHttpConfig
    test: object
    context: object
    token: str
    account: tuple
    expiry: int
    deadline: float
    origin: str
    worker: str
    interpreter: Path
    ca: object
    environment: tuple
    runtime: object
    config_values: tuple
    fixed_config: FabricHttpConfig


@dataclass(frozen=True)
class _HttpAccounting:
    context:object
    requests:int
    bytes:int
    diagnostics:tuple
    overflow:bool=False

class FabricAuthenticatedHttp:
    def __init__(self,*args,**kwargs):raise PermissionError('Use owning Fabric HTTP acquisition')
    def __setattr__(self,name,value):
        if name in ('_anchor','_accounting') and hasattr(self,name):raise AttributeError('Original HTTP token scope is immutable')
        object.__setattr__(self,name,value)
    def __delattr__(self,name):
        if name in ('_anchor','_accounting'):raise AttributeError('Original HTTP token scope is immutable')
        object.__delattr__(self,name)
    def _require(self,context):
        original=self._anchor
        if (self._closed or type(original)is not _HttpOriginal or context is not original.context
                or self.context is not original.context or self._config is not original.config
                or fabric_http_config_values(self._config)!=original.config_values
                or self._fixed_config is not original.fixed_config
                or fabric_http_config_values(self._fixed_config)!=original.config_values
                or self._test is not original.test or self._token is not original.token
                or self._origin!=original.origin or _WORKER!=original.worker
                or self._http_interpreter!=original.interpreter or self._ca!=original.ca
                or tuple(sorted(self._http_environment.items()))!=original.environment
                or self._http_runtime is not original.runtime
                or type(self._account)is not tuple or self._account!=original.account
                or type(self._expiry)is not int or self._expiry!=original.expiry
                or type(self._deadline)is not float or self._deadline!=original.deadline
                or type(self._accounting)is not _HttpAccounting or self._accounting.context is not original.context
                or self._accounting.overflow or time.monotonic()>=original.deadline or time.time()>=original.expiry):
            raise PermissionError('Original finite token-held HTTP owner required')
    @property
    def _requests(self):return self._accounting.requests
    @property
    def _bytes(self):return self._accounting.bytes
    def _update_accounting(self,*,requests=None,byte_count=None,diagnostic=None):
        old=self._accounting
        if requests is not None and (type(requests)is not int or not old.requests<=requests<=old.requests+1):raise PermissionError('Accounted request reset refused')
        if byte_count is not None and (type(byte_count)is not int or byte_count<old.bytes):raise PermissionError('Accounted byte reset refused')
        overflow=old.overflow;rows=old.diagnostics
        if diagnostic is not None:
            if len(rows)>=self._fixed_config.maximum_requests+8:overflow=True
            else:rows=rows+(tuple(diagnostic.items()),)
        object.__setattr__(self,'_accounting',_HttpAccounting(old.context,old.requests if requests is None else requests,old.bytes if byte_count is None else byte_count,rows,overflow))
    def _route(self,url):
        base=self._origin
        prefix=base+'/v1/workspaces/'+self._fixed_config.workspace_id+'/graphModels/'+self._fixed_config.graph_model_id
        if type(url)is not str:raise ValueError('Exact selected route required')
        parsed=urlsplit(url);selected=urlsplit(self._origin)
        if (parsed.scheme,parsed.netloc)!=(selected.scheme,selected.netloc):raise PermissionError('Foreign HTTP origin refused')
        if parsed.fragment or parsed.username or parsed.password:raise ValueError('Unselected route')
        gql=prefix+'/executeQuery?beta=true'
        if url==gql:return 'POST'
        if url.startswith(gql+'&continuationToken='):
            token=url[len(gql+'&continuationToken='):]
            if 0<len(token)<=24576 and re.fullmatch(r'(?:[A-Za-z0-9_.~-]|%[0-9A-F]{2})+',token):return 'POST'
            raise PermissionError('Exact sole opaque continuation parameter required')
        if url==prefix+'/getDefinition':return 'POST'
        if url in self._operation_routes:return 'GET'
        raise PermissionError('Unselected HTTP route')
    def _request(self,url,body,context,timeout):
        status=None;byte_count=0;complete=False;submitted=False;failure=None
        route='unselected';method=None;primary_failure=None
        try:
            self._require(context);method=self._route(url);cfg=self._fixed_config
            path=urlsplit(url).path
            route='executeQuery'if path.endswith('/executeQuery')else'getDefinition'if path.endswith('/getDefinition')else'operationResult'if path.endswith('/result')else'operationState'
            if type(body)is not bytes or len(body)>cfg.maximum_request_bytes:raise ValueError('Exact bounded original request required')
            if type(timeout)not in (int,float) or type(timeout)is bool or not math.isfinite(timeout) or not 0<timeout<=60:raise ValueError('Finite request timeout required')
            if self._requests>=cfg.maximum_requests:raise ValueError('HTTP request capacity exceeded')
            remaining=cfg.maximum_total_bytes-self._bytes
            if remaining<=0:raise ValueError('HTTP cumulative byte capacity exceeded')
            maximum=min(cfg.maximum_response_bytes,remaining)
            if self._http_runtime is not None:self._http_runtime.renew()
            # Closure may consume the complete remainder; no credential payload/spawn before this check.
            remainder=self._anchor.deadline-time.monotonic()
            if remainder<=0:raise TimeoutError('Whole HTTP interval elapsed before submission')
            self._require(context)
            request={'url':url,'method':method,'body':base64.b64encode(body).decode(),'token':self._token,'timeout':min(timeout,remainder),'maximum':maximum,'ca':self._ca}
            self._update_accounting(requests=self._requests+1);submitted=True;primary=None
            try:
                code,out,err=_capture([str(self._http_interpreter),'-I','-S','-B','-c',original_worker(self)],
                    json.dumps(request,separators=(',',':')).encode(),environment=self._http_environment,cwd=cfg.azure_config_dir,
                    timeout=request['timeout'],maximum=(maximum+1)*2+32768,deadline=self._anchor.deadline)
            except BaseException as error:primary=error
            finish(primary,[]if self._http_runtime is None else[self._http_runtime.renew])
            if code or err:
                self._private.append(('worker',code,out,err));failure='worker-failure'
                raise ValueError('Private HTTP worker refused')
            result=_decode(out);raw=base64.b64decode(result['raw'],validate=True);status=result['status'];byte_count=len(raw);complete=not result['overflow']
            self._update_accounting(byte_count=self._bytes+byte_count)
            self._private.append((status,raw,result['headers'],result['overflow']))
            self._require(context)
            if self._token.encode()in raw or any(self._token in v for v in result['headers'].values()):
                failure='credential-echo';raise PermissionError('Credential-bearing HTTP result withheld')
            if result['overflow'] or result['headers'].get('content-encoding','identity')!='identity':
                failure='capacity-or-encoding';raise ValueError('Incomplete/encoded HTTP response refused')
            if 300<=status<400:failure='redirect';raise PermissionError('HTTP redirect refused')
            return status,raw,result['headers']
        except BaseException as error:
            primary_failure=error
            if failure is None:failure='deadline'if isinstance(error,TimeoutError)else'cancelled'if not isinstance(error,Exception)else'permission-or-custody'if isinstance(error,PermissionError)else'invalid-or-capacity'
            raise
        finally:
            finish(primary_failure,[lambda:self._update_accounting(diagnostic={'route':route,'method':method,'status':status,'bytes':byte_count,'complete':complete,'submitted':submitted,'failure':failure})])
    def post(self,url,body,context,timeout_seconds):
        delegated=False
        try:
            self._require(context)
            expected=self._origin+'/v1/workspaces/'+self._fixed_config.workspace_id+'/graphModels/'+self._fixed_config.graph_model_id+'/executeQuery?beta=true'
            if type(url)is not str or not (url==expected or url.startswith(expected+'&continuationToken=')):
                raise PermissionError('Public POST admits selected GQL route only')
            delegated=True
            status,raw,_=self._request(url,body,context,timeout_seconds)
            return status,raw
        except BaseException as error:
            if not delegated:
                finish(error,[lambda:self._update_accounting(diagnostic={'route':'executeQuery','method':'POST','status':None,'bytes':0,'complete':False,'submitted':False,'failure':'permission-or-custody'})])
            raise
    def observations(self):
        rows=tuple(dict(row)for row in self._accounting.diagnostics)
        return rows+({'route':'observationCapacity','status':None,'complete':False,'failure':'capacity'},)if self._accounting.overflow else rows
    def _operation_headers(self,headers,operation,location):
        if ('x-ms-operation-id'in headers and headers['x-ms-operation-id']!=operation
                or 'location'in headers and headers['location']!=location):
            raise PermissionError('Original operation response identity differs')
    def get_definition(self):
        try:return self._definition()
        except BaseException as error:
            self._update_accounting(diagnostic={'route':'definitionOutcome','method':None,'status':None,'bytes':0,'complete':False,'submitted':False,'failure':'deadline'if isinstance(error,TimeoutError)else'operation-refusal'})
            raise
    def _definition(self):
        self._require(self.context);cfg=self._fixed_config;base=self._origin
        url=base+'/v1/workspaces/'+cfg.workspace_id+'/graphModels/'+cfg.graph_model_id+'/getDefinition'
        status,raw,headers=self._request(url,b'',self.context,15)
        if status==200:return raw
        if status!=202:raise ValueError('Definition request refused')
        from .fabric_http_configuration import uuid
        operation=headers.get('x-ms-operation-id');uuid(operation)
        state=base+'/v1/operations/'+operation;result=state+'/result'
        if headers.get('location')!=state:raise PermissionError('Unselected operation location')
        self._operation_routes=(state,result)
        while True:
            delay=headers.get('retry-after','0')
            if type(delay)is not str or not delay.isascii() or not delay.isdecimal() or len(delay)>5:raise ValueError('Bounded Retry-After required')
            wait=int(delay)
            if wait>=self._anchor.deadline-time.monotonic():raise TimeoutError('Definition poll deadline refused')
            if wait:time.sleep(wait)
            status,raw,headers=self._request(state,b'',self.context,15)
            if status!=200:raise ValueError('Operation state refused')
            value=_decode(raw);phase=value.get('status')
            self._operation_headers(headers,operation,result if phase=='Succeeded'else state)
            if phase=='Succeeded':break
            if phase not in ('NotStarted','Running'):raise ValueError('Operation incomplete/refused')
        status,raw,headers=self._request(result,b'',self.context,15)
        self._operation_headers(headers,operation,result)
        if status!=200:raise ValueError('Operation result refused')
        return raw


def original_worker(owner):
    return owner._anchor.worker


@contextmanager
def open_fabric_http(config):
    live=type(config)is FabricHttpConfig
    if not live and type(config)is not FabricLoopbackTestConfig:raise ValueError('Exact owning HTTP configuration required')
    if live:
        cfg=config;cfg.__post_init__()
        if (type(cfg.cli_runtime)is not FabricCliRuntimeSelection or type(cfg.http_runtime)is not FabricHttpRuntimeSelection
                or cfg.azure_cli!=CLI_ROOT/'bin/az' or cfg.azure_cli_version!='2.90.0'
                or not cfg.azure_cli.is_file() or cfg.azure_cli.resolve()!=cfg.azure_cli):
            raise PermissionError('Exact supported operator-admitted installed profile required')
        cli=config.cli_runtime;configuration=cfg.azure_config_dir;test=None
    else:
        config.__post_init__();cfg=config.config;cfg.__post_init__();cli=config.cli_runtime;configuration=None;test=config
    original_cfg=cfg;values=fabric_http_config_values(cfg);cfg=FabricHttpConfig(**{field.name:getattr(cfg,field.name)for field in fields(cfg)})
    deadline=time.monotonic()+cfg.deadline_seconds
    with hold_fabric_cli_runtime(cli,configuration=configuration,deadline=deadline)as runtime:
        with ExitStack()as stack:
            http_runtime=stack.enter_context(hold_fabric_cli_runtime(cfg.http_runtime,deadline=deadline))if live else None
            owner=object.__new__(FabricAuthenticatedHttp);owner.context=object();owner._config=original_cfg;owner._original_config=original_cfg;owner._fixed_config=cfg
            owner._deadline=deadline;owner._closed=False;owner._accounting=_HttpAccounting(owner.context,0,0,());owner._private=[];owner._operation_routes=();owner._test=test
            owner._origin='https://api.fabric.microsoft.com'if live else'http://127.0.0.1:'+str(config.http_runtime.port)
            owner._http_runtime=http_runtime;owner._http_interpreter=HTTP_PYTHON if live else config.http_runtime.interpreter
            owner._http_environment=dict(http_runtime.environment)if live else{'PATH':'/usr/bin:/bin'}
            owner._ca=str(cfg.http_runtime.ca_bundle)if live else None
            primary=None
            def command(args):
                runtime.renew();remaining=deadline-time.monotonic()
                if remaining<=0:raise TimeoutError('Credential interval deadline exceeded')
                primary=None
                try:
                    code,out,err=_capture([*runtime.command_prefix,*args,'--output','json','--only-show-errors'],b'',
                        environment=dict(runtime.environment),cwd=cfg.azure_config_dir,
                        timeout=min(cfg.auth_timeout_seconds,remaining),maximum=cfg.maximum_auth_output_bytes,deadline=deadline,
                        total_maximum=cfg.maximum_auth_output_bytes)
                except BaseException as error:primary=error
                finish(primary,[runtime.renew])
                if code or err:raise PermissionError('Private credential tool refused')
                return _decode(out)
            def account():
                v=command(['account','show','--subscription',cfg.account_id]);selected=(v.get('id'),v.get('tenantId'),v.get('user',{}).get('name'),v.get('user',{}).get('type'),v.get('state'),v.get('environmentName'))
                expected=(cfg.account_id,cfg.tenant_id,cfg.account_user,'user','Enabled','AzureCloud')
                if selected!=expected:raise PermissionError('Original selected account differs')
                return selected
            try:
                owner._account=account();owner._original_account=owner._account
                token=command(['account','get-access-token','--subscription',cfg.account_id,'--resource','https://api.fabric.microsoft.com'])
                raw=token.get('accessToken');expiry=token.get('expires_on')
                if type(expiry)is str and re.fullmatch('0|[1-9][0-9]{0,19}',expiry):expiry=int(expiry)
                if (token.get('tenant')!=cfg.tenant_id or token.get('subscription')!=cfg.account_id or token.get('tokenType')!='Bearer'
                        or type(raw)is not str or not raw or len(raw)>65536 or any(ord(c)<33 or ord(c)>126 for c in raw)
                        or type(expiry)is not int or expiry<time.time()+max(0,deadline-time.monotonic())):
                    raise PermissionError('Original finite token correspondence refused')
                owner._token=raw;owner._original_token=raw;owner._expiry=expiry
                if account()!=owner._account:raise PermissionError('Closing acquisition account differs')
                owner._anchor=_HttpOriginal(original_cfg,test,owner.context,raw,owner._account,expiry,deadline,owner._origin,_WORKER,owner._http_interpreter,owner._ca,tuple(sorted(owner._http_environment.items())),owner._http_runtime,values,cfg)
                yield owner
            except BaseException as error:primary=error
            def close_owner():
                owner._closed=True;owner._token=None;owner._original_token=None;object.__setattr__(owner,'_anchor',None)
            def close_configuration():
                if hasattr(owner,'_anchor') and type(owner._anchor)is _HttpOriginal:owner._require(owner._anchor.context)
                if fabric_http_config_values(original_cfg)!=values or fabric_http_config_values(cfg)!=values:raise PermissionError('Original complete configuration changed')
            finish(primary,[close_configuration,account,close_owner])
