"""Owned local PuppyGraph seam for the two original finite STRING cases.

No runtime inventory or canonical-source grant. Export permissions are local
use of historical immutable bytes; source semantics use the real public owner.
"""
import base64,hashlib,http.client,json,os,re,time,urllib.request,urllib.error
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from .evolution_producer import capture,snapshot
from .finite_dataset import FinitePackSource
from .local_graph_export import LocalGraphExportUse
from .lifecycle import finish,owned_context
from .puppy_finite_pack import project_finite_pack_strings,PuppyFinitePackConfiguration
from .truss_preparation import json_bytes

class FiniteExportPreparationPolicy:
    def __init__(self,source,export):
        if type(source)is not FinitePackSource or type(export)is not LocalGraphExportUse or source.context is not export.context:raise ValueError('Actual source and local export owners with one context required')
        self.source=source;self.export=export;self.context=export.context
        self.original=(source,export,source.metadata(),export.release,export.custody_bytes,export.source)
        self.active=False
    def renew(self):
        source,export,metadata,release,custody,historical=self.original
        if self.source is not source or self.export is not export or self.context is not export.context or source.metadata()!=metadata:raise PermissionError('Original current finite source owner changed')
        export.renew()
    @contextmanager
    def hold(self,context):
        if context is not self.context or self.active:raise PermissionError('Exact nonnested local export interval required')
        with owned_context(self.original[1].writer(context)):
            primary=None
            try:self.active=True;self.renew();yield
            except BaseException as error:primary=error
            try:finish(primary,[self.renew])
            finally:self.active=False
    def admit_original_release(self,release,context):
        if context is not self.context or not self.active or release!=self.original[3]:raise PermissionError('Original actual local export interval required')
        self.renew();self.original[1].admit_original_export(release,self.original[4],self.original[5],context)

@dataclass(frozen=True)
class PuppyFiniteLoopbackConfig:
    docker:Path
    docker_host:str
    image:str
    name:str
    prepared:Path
    username:str
    password:str
    http_port:int
    bolt_port:int
    deadline_seconds:int=120
    def __post_init__(self):
        if type(self.docker)is not type(Path('/'))or not self.docker.is_absolute()or type(self.prepared)is not type(Path('/'))or self.prepared.resolve()!=self.prepared or not self.prepared.is_dir():raise ValueError('Explicit existing Docker and prepared paths required')
        if type(self.docker_host)is not str or not self.docker_host.startswith('unix://')or type(self.image)is not str or not re.fullmatch('sha256:[0-9a-f]{64}',self.image)or type(self.name)is not str or not re.fullmatch('ashlar-[a-z0-9-]{1,80}',self.name):raise ValueError('Explicit local Docker socket/cached image/owned name required')
        if any(type(v)is not str or not v or len(v)>256 or '\n'in v or '\r'in v for v in (self.username,self.password)):raise ValueError('Explicit local credentials required')
        if any(type(p)is not int or not 1024<=p<=65535 for p in (self.http_port,self.bolt_port))or self.http_port==self.bolt_port or type(self.deadline_seconds)is not int or not 1<=self.deadline_seconds<=120:raise ValueError('Explicit finite distinct loopback ports required')

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None

class OwnedPuppyFiniteLocal(FiniteExportPreparationPolicy):
    def __init__(self,*args,**kwargs):raise PermissionError('Use open_puppy_finite_local')
    @property
    def config(self):return self._config
    @property
    def container_id(self):return self._container_id
    def _record(self,name,raw):
        if type(raw)is not bytes or sum(len(value)for _,value in self.observations)+len(raw)>8388608:raise ValueError('Complete model observation capacity exceeded')
        self.observations.append((name,raw))
    def _docker(self,args,*,cleanup=False):
        remaining=30 if cleanup else self.deadline-time.monotonic()
        if remaining<1:raise TimeoutError('Owned PuppyGraph deadline elapsed')
        out,err=capture([str(self.config.docker),*args],cwd=self.config.prepared,environment={'PATH':'/usr/bin:/bin','DOCKER_HOST':self.config.docker_host},timeout_seconds=min(30,int(remaining)),maximum_output_bytes=1048576)
        # capture already requires exit zero; successful bounded warnings do not
        # invalidate ownership of the returned create ID. Keep them private.
        self.docker_diagnostics.append(err)
        return out
    def _container(self):
        info=json.loads(self._docker(['inspect',self.container_id]))
        if type(info)is not list or len(info)!=1:raise PermissionError('Actual owned container required')
        row=info[0];host=row['HostConfig'];mounts=row['Mounts']
        expected={'8081/tcp':self.config.http_port,'7687/tcp':self.config.bolt_port}
        if row['Id']!=self.container_id or row['Image']!=self.config.image or row['Name']!='/'+self.config.name or not row['State']['Running']or host['NanoCpus']!=2000000000 or host['Memory']!=3*1024**3:raise PermissionError('Actual owned image/resources/status differ')
        selected=[m for m in mounts if m['Destination']=='/tmp/carrier.duckdb']
        if len(selected)!=1 or selected[0]['Source']!=str(self.database)or selected[0]['RW']is not False:raise PermissionError('Original read-only carrier mount differs')
        for key,port in expected.items():
            if row['NetworkSettings']['Ports'].get(key)!=[{'HostIp':'127.0.0.1','HostPort':str(port)}]:raise PermissionError('Actual loopback endpoint differs')
    def _http(self,path,body=None):
        if path not in ('/schemajson','/schema?postUploadBehavior=none'):raise ValueError('Closed schema route required')
        request=urllib.request.Request('http://127.0.0.1:'+str(self.config.http_port)+path,data=body,headers={'Authorization':'Basic '+base64.b64encode((self.config.username+':'+self.config.password).encode()).decode(),'Content-Type':'application/json'})
        remaining=self.deadline-time.monotonic()
        if remaining<=0:raise TimeoutError('Owned HTTP deadline elapsed')
        with self.opener.open(request,timeout=min(5,remaining))as response:
            raw=response.read(1048577)
            if response.status!=200 or len(raw)>1048576:raise ValueError('Bounded complete schema response required')
        return raw
    def _wait_ready(self):
        """Startup connection failures only; auth/schema failures never retry."""
        while True:
            try:
                raw=self._http('/schemajson')
                if type(json.loads(raw))is not dict:raise ValueError('Original schema object required')
                self._record('before-model',raw);return
            except urllib.error.HTTPError:
                raise
            except (urllib.error.URLError,http.client.RemoteDisconnected,TimeoutError):
                if time.monotonic()>=self.deadline:raise TimeoutError('Owned engine readiness deadline elapsed')
                self._container() # An exited/foreign engine fails immediately.
                time.sleep(min(.2,max(0,self.deadline-time.monotonic())))
    def renew(self):
        FiniteExportPreparationPolicy.renew(self)
        if self.closed:raise PermissionError('Owned engine interval closed')
        if self.database.resolve()!=self.database or self.database.stat().st_mode&0o222:raise PermissionError('Canonical read-only prepared carrier required')
        if snapshot(self.database,33554432)!=self.original_database or snapshot(self.config.prepared/'model.json',1048576)!=self.original_model:raise PermissionError('Original prepared carrier bytes changed')
        self._container()
        if self.loaded:
            raw=self._http('/schemajson');value=json.loads(raw);self._record('current-model',raw)
            for section in ('node','edge'):
                if value.get(section)!=self.requested_model[section]:raise PermissionError('Current native model differs')
            if 'catalog'in value and value['catalog']!=self.requested_model['catalog']:raise PermissionError('Current native catalog differs')
            self.catalog_visibility='present'if'catalog'in value else'omitted'
            # Retain original GET bytes, including every unrecognized field.
    def query(self,language,script,context):
        if language!='Cypher'or context is not self.context or not self.active:raise PermissionError('Actual held original Cypher interval required')
        if self.deadline-time.monotonic()<=0:raise TimeoutError('Owned query deadline elapsed')
        self.admit_original_release(self.original[3],context)
        self.renew()
        if self.deadline-time.monotonic()<=0:raise TimeoutError('Owned query deadline elapsed')
        from neo4j import Query,GraphDatabase
        remaining=self.deadline-time.monotonic()
        if remaining<=0:raise TimeoutError('Owned query deadline elapsed')
        timeout=min(5,remaining);driver=None;session=None;primary=None;rows=[];total=0
        try:
            driver=GraphDatabase.driver('bolt://127.0.0.1:'+str(self.config.bolt_port),auth=(self.config.username,self.config.password),connection_timeout=timeout,connection_acquisition_timeout=timeout)
            session=driver.session()
            result=session.run(Query(script,timeout=min(15,remaining)))
            for record in result:
                if time.monotonic()>=self.deadline:raise TimeoutError('Owned query deadline elapsed')
                row=record.data();raw=json_bytes(row,1048576);total+=len(raw)
                if len(rows)>=1000 or total>1048576:raise ValueError('Complete native query capacity exceeded')
                rows.append(row)
        except BaseException as error:primary=error
        finish(primary,([session.close]if session is not None else [])+([driver.close]if driver is not None else []))
        self.renew();return rows

@contextmanager
def open_puppy_finite_local(config,case,*,source,export):
    """Create/start/stop only its returned container ID; cached image, no pull."""
    if type(config)is not PuppyFiniteLoopbackConfig or type(case)is not PuppyFinitePackConfiguration:raise ValueError('Exact local engine/case selections required')
    config.__post_init__();projection=project_finite_pack_strings(case)
    owner=object.__new__(OwnedPuppyFiniteLocal);FiniteExportPreparationPolicy.__init__(owner,source,export)
    if export.release.payload!=case.release or source.model!=case.model or source.graph!=case.graph:raise PermissionError('Original selected source/export bytes differ')
    owner._config=config;owner.docker_diagnostics=[];owner.deadline=time.monotonic()+config.deadline_seconds;owner.database=config.prepared/'carrier.duckdb'
    if owner.database.resolve()!=owner.database or owner.database.stat().st_mode&0o222:raise PermissionError('Canonical read-only prepared carrier required')
    owner.original_database=snapshot(owner.database,33554432);owner.original_model=snapshot(config.prepared/'model.json',1048576)
    if json.loads(snapshot(config.prepared/'receipt.json',16777216))!=projection:raise ValueError('Complete original preparation receipt differs')
    model=json.loads(owner.original_model)
    # Explicit native wire spelling: preserve the prepared original separately.
    if set(model)!={'catalogs','node','edge'}:raise ValueError('Exact prepared finite model required')
    owner.requested_model={'catalog':model['catalogs'],'node':model['node'],'edge':model['edge']}
    for catalog in owner.requested_model['catalog']:catalog['jdbc']['jdbcUri']='jdbc:duckdb:/tmp/carrier.duckdb'
    owner.requested_bytes=json_bytes(owner.requested_model,1048576);owner.observations=[('prepared-model',owner.original_model),('submitted-model',owner.requested_bytes)]
    owner._container_id=None;owner.driver=None;owner.closed=False;owner.loaded=False;primary=None
    owner.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),_NoRedirect())
    try:
        image=json.loads(owner._docker(['image','inspect',config.image]))
        if len(image)!=1 or image[0]['Id']!=config.image:raise PermissionError('Existing exact cached image required')
        raw=owner._docker(['create','--pull=never','--name',config.name,'--cpus=2','--memory=3g','--publish','127.0.0.1:'+str(config.http_port)+':8081','--publish','127.0.0.1:'+str(config.bolt_port)+':7687','--mount','type=bind,src='+str(owner.database)+',dst=/tmp/carrier.duckdb,readonly',config.image])
        identifier=raw.decode().strip()
        if not re.fullmatch('[0-9a-f]{64}',identifier):raise ValueError('Complete actual container ID required')
        owner._container_id=identifier;owner._docker(['start',identifier]);owner._container()
        owner._wait_ready()
        outcome=owner._http('/schema?postUploadBehavior=none',owner.requested_bytes);owner._record('submission',outcome)
        if json.loads(outcome).get('ok')is not True:raise RuntimeError('Native model submission refused')
        owner.loaded=True
        owner.renew();yield owner
    except BaseException as error:primary=error
    actions=[]
    if owner.container_id is not None:
        if owner.loaded:actions.append(owner.renew)
        if owner.driver is not None:actions.append(owner.driver.close)
        # Cleanup commands retain their own finite allowance after query expiry.
        actions.extend([lambda:owner._docker(['stop','--time','5',owner.container_id],cleanup=True),lambda:owner._docker(['rm',owner.container_id],cleanup=True)])
    def closed():owner.closed=True
    actions.append(closed);finish(primary,actions)
