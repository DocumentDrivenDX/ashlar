# SPDX-License-Identifier: MIT OR Apache-2.0
"""Ordinary delegated Fabric authentication; publication authority stays with policy."""
from contextlib import contextmanager
from dataclasses import dataclass
import json,math,os,re,stat,time,urllib.request,urllib.error
from pathlib import Path
from .evolution_producer import capture
from .fabric_http_configuration import uuid,capacity
from .lifecycle import finish

@dataclass(frozen=True)
class FabricOrdinaryHttpConfig:
    azure_cli: Path
    azure_config_dir: Path
    account_id: str
    tenant_id: str
    account_user: str
    workspace_id: str
    graph_model_id: str
    maximum_request_bytes: int=65536
    maximum_response_bytes: int=1048576
    maximum_total_bytes: int=8388608
    maximum_requests: int=32
    deadline_seconds: int=60
    auth_timeout_seconds: int=15
    maximum_auth_output_bytes: int=65536
    def __post_init__(self):
        for value in (self.account_id,self.tenant_id,self.workspace_id,self.graph_model_id):uuid(value)
        for path in (self.azure_cli,self.azure_config_dir):
            if type(path)is not type(Path('/'))or not path.is_absolute():raise ValueError('Explicit absolute Azure CLI and configuration paths required')
        if type(self.account_user)is not str or not self.account_user or len(self.account_user)>256 or any(ord(c)<32 for c in self.account_user):raise ValueError('Explicit delegated user required')
        for name,limit in (('maximum_request_bytes',65536),('maximum_response_bytes',16777216),('maximum_total_bytes',67108864),('maximum_requests',128),('deadline_seconds',60),('auth_timeout_seconds',30),('maximum_auth_output_bytes',1048576)):capacity(getattr(self,name),limit)

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None

class FabricOrdinaryHttp:
    def __init__(self,*args,**kwargs):raise PermissionError('Use open_fabric_ordinary_http')
    def post(self,url,body,context,timeout_seconds):
        cfg=self._config
        if self._closed or context is not self.context or cfg!=self._original:raise PermissionError('Current ordinary HTTP interval required')
        endpoint='https://api.fabric.microsoft.com/v1/workspaces/'+cfg.workspace_id+'/graphModels/'+cfg.graph_model_id+'/executeQuery?beta=true'
        if (type(url)is not str or not (url==endpoint or (url.startswith(endpoint+'&continuationToken=')and re.fullmatch(r'(?:[A-Za-z0-9_.~-]|%[0-9A-Fa-f]{2}){1,24576}',url[len(endpoint+'&continuationToken='):])is not None))
                or type(body)is not bytes or not 0<len(body)<=cfg.maximum_request_bytes):raise ValueError('Selected bounded GQL route and body required')
        if type(timeout_seconds)not in (int,float)or not math.isfinite(timeout_seconds)or timeout_seconds<=0:raise ValueError('Finite HTTP timeout required')
        remaining=self._deadline-time.monotonic()
        if remaining<=0 or self._requests>=cfg.maximum_requests:raise TimeoutError('Ordinary HTTP interval capacity exhausted')
        self._requests+=1
        request=urllib.request.Request(url,data=body,method='POST',headers={'Authorization':'Bearer '+self._token,'Content-Type':'application/json','Accept':'application/json','Accept-Encoding':'identity'})
        response=None;primary=None;raw=bytearray();status=None;complete=False
        try:
            try:response=self._opener.open(request,timeout=min(timeout_seconds,remaining,15))
            except urllib.error.HTTPError as error:response=error
            status=response.code
            if 300<=response.code<400:raise PermissionError('Redirect refused')
            encoding=response.headers.get('Content-Encoding')
            if encoding not in (None,'identity'):raise ValueError('Encoded response refused')
            limit=min(cfg.maximum_response_bytes,cfg.maximum_total_bytes-self._bytes)
            while True:
                if time.monotonic()>=self._deadline:raise TimeoutError('Ordinary HTTP deadline exceeded')
                block=response.read(min(65536,limit-len(raw)+1))
                if not block:break
                available=limit-len(raw)
                retained=block[:available];raw.extend(retained);self._bytes+=len(retained)
                if len(block)>available:raise ValueError('Ordinary HTTP response capacity exceeded')
            complete=True
        except BaseException as error:primary=error
        self._observations.append((status,bytes(raw),complete))
        finish(primary,[response.close]if response is not None else [])
        return status,bytes(raw)

    def selected_configuration(self):
        """Public immutable account/route selection; never expose the bearer token."""
        if self._closed or self._config != self._original or time.monotonic() >= self._deadline:
            raise PermissionError('Current ordinary HTTP interval required')
        self._original.__post_init__()
        return self._original

    def observations(self):
        """Bounded original response bytes, including incomplete failures; no credentials."""
        return tuple(self._observations)

@contextmanager
def open_fabric_ordinary_http(config):
    """Own selected delegated account/token; never admits a publication or release."""
    if type(config)is not FabricOrdinaryHttpConfig:raise ValueError('Exact ordinary HTTP configuration required')
    config.__post_init__()
    if not config.azure_cli.is_file()or not os.access(config.azure_cli,os.X_OK)or not config.azure_config_dir.is_dir():raise ValueError('Selected CLI executable and configuration directory required')
    directory=config.azure_config_dir.stat()
    if directory.st_uid!=os.geteuid()or stat.S_IMODE(directory.st_mode)&0o022:raise PermissionError('Selected CLI configuration directory must be user-owned and not writable by group or others')
    original=config;deadline=time.monotonic()+config.deadline_seconds
    environment={'PATH':'/usr/bin:/bin','AZURE_CONFIG_DIR':str(config.azure_config_dir),'HOME':str(Path.home())}
    def command(args):
        remaining=deadline-time.monotonic()
        if remaining<1:raise TimeoutError('Ordinary authentication deadline exceeded')
        out,err=capture([str(config.azure_cli),*args,'--output','json','--only-show-errors'],cwd=config.azure_config_dir,environment=environment,timeout_seconds=min(config.auth_timeout_seconds,int(remaining)),maximum_output_bytes=config.maximum_auth_output_bytes)
        if err:raise PermissionError('Ordinary authentication refused')
        return json.loads(out)
    def account():
        value=command(['account','show','--subscription',config.account_id])
        if (value.get('id'),value.get('tenantId'),value.get('user',{}).get('name'),value.get('user',{}).get('type'),value.get('state'),value.get('environmentName'))!=(config.account_id,config.tenant_id,config.account_user,'user','Enabled','AzureCloud'):raise PermissionError('Selected delegated account differs')
    owner=None;primary=None
    try:
        account()
        token=command(['account','get-access-token','--subscription',config.account_id,'--resource','https://api.fabric.microsoft.com'])
        if (token.get('tenant')!=config.tenant_id or token.get('subscription')!=config.account_id or token.get('tokenType')!='Bearer' or type(token.get('accessToken'))is not str or not token['accessToken'] or len(token['accessToken'])>32768 or any(ord(c)<=32 or ord(c)>=127 for c in token['accessToken']) or type(token.get('expires_on'))not in (str,int)or int(token['expires_on'])<=time.time()+config.deadline_seconds):raise PermissionError('Selected finite delegated token refused')
        owner=object.__new__(FabricOrdinaryHttp);owner.context=object();owner._config=config;owner._original=original;owner._deadline=deadline;owner._token=token['accessToken'];owner._requests=0;owner._bytes=0;owner._closed=False;owner._observations=[]
        owner._opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),_NoRedirect())
        yield owner
    except BaseException as error:primary=error
    def close():
        if owner is not None:owner._closed=True;owner._token=None
    finish(primary,[account,close])
