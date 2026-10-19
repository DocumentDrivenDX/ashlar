# SPDX-License-Identifier: MIT OR Apache-2.0
"""Owned local PuppyGraph seam for the two original finite STRING cases.

No runtime inventory or canonical-source grant. Export permissions are local
use of historical immutable bytes; source semantics use the real public owner.
"""
import base64,hashlib,http.client,json,os,re,time,urllib.request,urllib.error
from contextlib import contextmanager
from dataclasses import dataclass,field,fields
from pathlib import Path
from typing import Optional
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
    username:str=field(repr=False)
    password:str=field(repr=False)
    http_port:int
    bolt_port:int
    deadline_seconds:int=120
    def __post_init__(self):
        if type(self.docker)is not type(Path('/'))or not self.docker.is_absolute()or type(self.prepared)is not type(Path('/'))or self.prepared.resolve()!=self.prepared or not self.prepared.is_dir():raise ValueError('Explicit existing Docker and prepared paths required')
        if type(self.docker_host)is not str or not self.docker_host.startswith('unix://')or type(self.image)is not str or not re.fullmatch('sha256:[0-9a-f]{64}',self.image)or type(self.name)is not str or not re.fullmatch('ashlar-[a-z0-9-]{1,80}',self.name):raise ValueError('Explicit local Docker socket/cached image/owned name required')
        if any(type(v)is not str or not v or len(v)>256 or '\n'in v or '\r'in v for v in (self.username,self.password)):raise ValueError('Explicit local credentials required')
        if any(type(p)is not int or not 1024<=p<=65535 for p in (self.http_port,self.bolt_port))or self.http_port==self.bolt_port or type(self.deadline_seconds)is not int or not 1<=self.deadline_seconds<=180:raise ValueError('Explicit finite distinct loopback ports required')

@dataclass(frozen=True)
class PuppyGremlinLoopbackConfig(PuppyFiniteLoopbackConfig):
    gremlin_port:int=0
    def __post_init__(self):
        super().__post_init__()
        if type(self.gremlin_port)is not int or not 1024<=self.gremlin_port<=65535 or self.gremlin_port in (self.http_port,self.bolt_port):
            raise ValueError('Three distinct explicit loopback ports required')

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None

class PuppyLocalRuntime:
    def __init__(self,*args,**kwargs):raise PermissionError('Use open_puppy_finite_local')
    @property
    def config(self):return self._config
    @property
    def container_id(self):return self._container_id
    def _record(self,name,raw):
        if type(raw)is not bytes or sum(len(value)for _,value in self.observations)+len(raw)>8388608:raise ValueError('Complete model observation capacity exceeded')
        self.observations.append((name,raw))
    def record_observation(self,name,raw):
        self._record(name,raw)
    def _docker(self,args,*,cleanup=False):
        remaining=30 if cleanup else self.deadline-time.monotonic()
        if remaining<1:raise TimeoutError('Owned PuppyGraph deadline elapsed')
        out,err=capture([str(self.config.docker),*args],cwd=self.config.prepared,environment={'PATH':'/usr/bin:/bin','DOCKER_HOST':self.config.docker_host},timeout_seconds=min(30,int(remaining)),maximum_output_bytes=1048576)
        # capture already requires exit zero; successful bounded warnings do not
        # invalidate ownership of the returned create ID. Keep them private.
        self.docker_diagnostics.append(err)
        return out
    def _create_arguments(self):
        config=self.config
        arguments=['create','--pull=never','--name',config.name,'--cpus=2','--memory=3g',
            '--env','PUPPYGRAPH_USERNAME='+config.username,'--env','PUPPYGRAPH_PASSWORD='+config.password,
            '--publish','127.0.0.1:'+str(config.http_port)+':8081','--publish','127.0.0.1:'+str(config.bolt_port)+':7687',
            ]
        if type(config)is PuppyGremlinLoopbackConfig:arguments+=['--publish','127.0.0.1:'+str(config.gremlin_port)+':8182']
        nonce=getattr(self,'_scenario_invocation_nonce',None)
        if nonce is not None:arguments+=['--label','ashlar.scenario.invocation='+nonce]
        return arguments+['--mount','type=bind,src='+str(self.database)+',dst=/tmp/'+self.database.name+',readonly',config.image]
    def _container(self):
        info=json.loads(self._docker(['inspect',self.container_id]))
        if type(info)is not list or len(info)!=1:raise PermissionError('Actual owned container required')
        row=info[0];host=row['HostConfig'];mounts=row['Mounts']
        nonce=getattr(self,'_scenario_invocation_nonce',None)
        if nonce is not None and row.get('Config',{}).get('Labels',{}).get('ashlar.scenario.invocation')!=nonce:
            raise PermissionError('Original scenario invocation label differs')
        expected={'8081/tcp':self.config.http_port,'7687/tcp':self.config.bolt_port}
        if type(self.config)is PuppyGremlinLoopbackConfig:expected['8182/tcp']=self.config.gremlin_port
        if row['Id']!=self.container_id or row['Image']!=self.config.image or row['Name']!='/'+self.config.name or not row['State']['Running']or host['NanoCpus']!=2000000000 or host['Memory']!=3*1024**3:raise PermissionError('Actual owned image/resources/status differ')
        selected=[m for m in mounts if m['Destination']=='/tmp/'+self.database.name]
        if len(selected)!=1 or selected[0]['Source']!=str(self.database)or selected[0]['RW']is not False:raise PermissionError('Original read-only carrier mount differs')
        for key,port in expected.items():
            if row['NetworkSettings']['Ports'].get(key)!=[{'HostIp':'127.0.0.1','HostPort':str(port)}]:raise PermissionError('Actual loopback endpoint differs')
    def _http(self,path,body=None):
        if path not in ('/schemajson','/schema?postUploadBehavior=none'):raise ValueError('Closed schema route required')
        request=urllib.request.Request('http://127.0.0.1:'+str(self.config.http_port)+path,data=body,headers={'Authorization':'Basic '+base64.b64encode((self.config.username+':'+self.config.password).encode()).decode(),'Content-Type':'application/json'})
        remaining=self.deadline-time.monotonic()
        if remaining<=0:raise TimeoutError('Owned HTTP deadline elapsed')
        with self.opener.open(request,timeout=min(30 if body is not None else 5,remaining))as response:
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
    def _wait_backend_ready(self):
        """Exercise the query backend before the single model submission."""
        from neo4j import Query,GraphDatabase
        from neo4j.exceptions import ServiceUnavailable,SessionExpired,TransientError
        while True:
            remaining=self.deadline-time.monotonic()
            if remaining<=0:raise TimeoutError('Owned backend readiness deadline elapsed')
            driver=None;session=None;primary=None;marker=None
            try:
                timeout=min(5,remaining)
                driver=GraphDatabase.driver('bolt://127.0.0.1:'+str(self.config.bolt_port),auth=(self.config.username,self.config.password),connection_timeout=timeout,connection_acquisition_timeout=timeout)
                session=driver.session()
                remaining=self.deadline-time.monotonic()
                if remaining<=0:raise TimeoutError('Owned backend readiness deadline elapsed')
                rows=[]
                for record in session.run(Query('RETURN 1 AS ready',timeout=min(5,remaining))):
                    if time.monotonic()>=self.deadline:raise TimeoutError('Owned backend readiness deadline elapsed')
                    if rows:raise ValueError('Exact backend readiness scalar required')
                    rows.append(record.data())
                if len(rows)!=1 or set(rows[0])!={'ready'} or type(rows[0]['ready'])is not int or rows[0]['ready']!=1:raise ValueError('Exact backend readiness scalar required')
                marker=[{'ready':1}]
            except BaseException as error:primary=error
            cleanup_failed=[]
            def closing(callback):
                def close():
                    try:callback()
                    except BaseException:
                        cleanup_failed.append(True)
                        raise
                return close
            try:
                def record():
                    code=getattr(primary,'code',None)if primary is not None else None
                    if type(code)is not str or re.fullmatch(r'Neo\.[A-Za-z0-9_.]{1,192}',code)is None:code=None
                    self._record('backend-readiness',json_bytes({'query':'RETURN 1 AS ready',
                        'rows':marker,'error_code':code,**safe_error_observation(primary)},1048576))
                finish(primary,[closing(record)]+([closing(session.close)]if session is not None else [])+([closing(driver.close)]if driver is not None else []))
            except (ServiceUnavailable,SessionExpired,TransientError)as error:
                if error is not primary or cleanup_failed:raise
                code=getattr(error,'code',None)
                if type(code)is str and '.Security.'in code:raise
                if time.monotonic()>=self.deadline:raise TimeoutError('Owned backend readiness deadline elapsed')
                self._container()
                time.sleep(min(.2,max(0,self.deadline-time.monotonic())))
                continue
            if time.monotonic()>=self.deadline:raise TimeoutError('Owned backend readiness deadline elapsed')
            return
    def renew_runtime(self):
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
        self.stage='query'
        if language!='Cypher'or context is not self.context or not self.active:raise PermissionError('Actual held original Cypher interval required')
        if self.deadline-time.monotonic()<=0:raise TimeoutError('Owned query deadline elapsed')
        self.admit_original_release(self.selected_release,context)
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
        self.renew();self.stage='caller';return rows

class OwnedPuppyFiniteLocal(PuppyLocalRuntime,FiniteExportPreparationPolicy):
    @property
    def selected_release(self):return self.original[3]
    def renew(self):
        FiniteExportPreparationPolicy.renew(self)
        self.renew_runtime()


@contextmanager
def open_puppy_finite_local(config,case,*,source,export,failure_observer=None):
    """Create/start/stop only its returned container ID; cached image, no pull."""
    if type(config)is not PuppyFiniteLoopbackConfig or type(case)is not PuppyFinitePackConfiguration:raise ValueError('Exact local engine/case selections required')
    if failure_observer is not None and not callable(failure_observer):raise ValueError('Trusted private failure observer must be callable')
    config.__post_init__()
    if config.deadline_seconds>120:raise ValueError('Finite STRING workflow deadline exceeds 120 seconds')
    projection=project_finite_pack_strings(case)
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
    owner._container_id=None;owner.driver=None;owner.closed=False;owner.loaded=False;owner.stage='image-preflight';owner.submission_outcome='not-submitted';primary=None
    owner.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),_NoRedirect())
    try:
        image=json.loads(owner._docker(['image','inspect',config.image]))
        if len(image)!=1 or image[0]['Id']!=config.image:raise PermissionError('Existing exact cached image required')
        owner.stage='container-create'
        raw=owner._docker(owner._create_arguments())
        identifier=raw.decode().strip()
        if not re.fullmatch('[0-9a-f]{64}',identifier):raise ValueError('Complete actual container ID required')
        owner._container_id=identifier;owner._docker(['start',identifier]);owner._container()
        owner.stage='startup'
        owner._wait_ready()
        owner._wait_backend_ready()
        owner.stage='model-submission';owner.submission_outcome='unknown'
        outcome=owner._http('/schema?postUploadBehavior=none',owner.requested_bytes);owner._record('submission',outcome)
        if json.loads(outcome).get('ok')is not True:
            owner.submission_outcome='refused';raise RuntimeError('Native model submission refused')
        owner.submission_outcome='accepted';owner.stage='model-observation'
        owner.loaded=True
        owner.renew();owner.stage='caller';yield owner
    except BaseException as error:primary=error
    _close_owned_engine(owner,primary,failure_observer)


def safe_error_observation(error):
    """Bounded exception class and code site; never messages, arguments or locals."""
    if error is None:return {'error_type':None,'error_origin':None}
    cls=type(error);origin=None;tb=error.__traceback__
    while tb is not None:
        module=tb.tb_frame.f_globals.get('__name__','')
        origin={'module':str(module)[:256],'function':tb.tb_frame.f_code.co_name[:256],'line':tb.tb_lineno}
        tb=tb.tb_next
    return {'error_type':(cls.__module__+'.'+cls.__name__)[:256],'error_origin':origin}


def _close_owned_engine(owner,primary,failure_observer):
    """Private observation failures never prevent original-ID cleanup."""
    identifier=owner.container_id;prior_types=[]
    if primary is not None:prior_types.append(safe_error_observation(primary)['error_type'])
    if identifier is not None and owner.loaded:
        try:finish(primary,[owner.renew])
        except BaseException as error:
            primary=error;kind=safe_error_observation(error)['error_type']
            if kind not in prior_types:prior_types.append(kind)
    actions=[]
    if primary is not None:
        observation={'stage':owner.stage,'submission_outcome':owner.submission_outcome,
            'container_id':identifier,'model_observations':tuple(owner.observations),
            'stdout':None,'stderr':None,**safe_error_observation(primary),
            'prior_error_types':prior_types,'cleanup_error_types':[]}
        if identifier is not None:
            def logs():
                observation['stdout']=owner._docker(['logs','--tail','200',identifier],cleanup=True)
                observation['stderr']=owner.docker_diagnostics[-1]
            actions.append(logs)
    if owner.driver is not None:actions.append(owner.driver.close)
    if identifier is not None:
        actions.extend([lambda:owner._docker(['stop','--time','5',identifier],cleanup=True),
                        lambda:owner._docker(['rm',identifier],cleanup=True)])
    def closed():owner.closed=True
    actions.append(closed)
    if primary is not None:
        def guarded(callback):
            def run():
                try:callback()
                except BaseException as error:
                    kind=safe_error_observation(error)['error_type']
                    observation['cleanup_error_types'].append(kind)
                    if kind not in observation['prior_error_types']:observation['prior_error_types'].append(kind)
                    raise
            return run
        actions=[guarded(callback)for callback in actions]
        # Snapshot the original failure before cleanup; persist after all owned
        # cleanup attempts so a secondary failure cannot erase its class/site.
        if failure_observer is not None:actions.append(lambda:failure_observer(observation))
    finish(primary,actions)

class ImmutableExportPreparationPolicy:
    """Historical immutable file use, never a current source/ACK grant."""
    def __init__(self,export):
        if type(export)is not LocalGraphExportUse:raise PermissionError('Actual immutable export owner required')
        self.export=export;self.context=export.context;self.selected_release=export.release
        self.original=(export,export.release,export.custody_bytes,export.source);self.active=False
    def renew(self):
        export,release,custody,source=self.original
        if self.export is not export or self.context is not export.context or self.selected_release!=release or export.custody_bytes!=custody or export.source is not source:
            raise PermissionError('Original immutable export owners changed')
        export.renew()
    @contextmanager
    def writer(self,context):
        if context is not self.context or self.active:raise PermissionError('Nonnested original immutable release interval required')
        with owned_context(self.export.writer(context)):
            primary=None
            try:self.active=True;self.renew();yield
            except BaseException as error:primary=error
            try:finish(primary,[self.renew])
            finally:self.active=False
    def admit_original_release(self,release,context):
        if context is not self.context or not self.active or release!=self.selected_release:raise PermissionError('Exact held immutable release required')
        self.renew();self.export.admit_original_export(release,self.original[2],self.original[3],context)
    def admit(self,handle,context):self.admit_original_release(handle.release,context)

class OwnedPuppyExportLocal(PuppyLocalRuntime,ImmutableExportPreparationPolicy):
    def renew(self):
        ImmutableExportPreparationPolicy.renew(self);self.renew_runtime()
    def schema_http(self,path,body,context):
        if context is not self.context or not self.active:raise PermissionError('Actual held schema owner required')
        self.renew()
        if body is not None:self.stage='model-submission';self.submission_outcome='unknown'
        raw=self._http(path,body);self._record('schema',raw)
        if body is not None:
            if json.loads(raw).get('ok')is not True:
                self.submission_outcome='refused';raise RuntimeError('Native model submission refused')
            self.submission_outcome='accepted';self.requested_model=json.loads(body);self.loaded=True;self.stage='caller'
        return raw
    def native_runtime(self,context):
        if context is not self.context or not self.active:raise PermissionError('Actual held runtime owner required')
        self.renew()
        # Exact independently selected and inspected image digest is the runtime
        # revision; do not infer a PuppyGraph product version from Ubuntu labels.
        return self.container_id,self.config.image
    def query_gremlin(self,script,context,*,count_fields=()):
        if type(self.config)is not PuppyGremlinLoopbackConfig or context is not self.context or not self.active:
            raise PermissionError('Actual held explicit Gremlin owner required')
        self.admit_original_release(self.selected_release,context);self.renew()
        from .puppy_gremlin_transport import query_gremlin
        result=query_gremlin(self,script,count_fields=count_fields)
        self.renew();return result

@contextmanager
def open_puppy_export_local(config,*,export,receipt,failure_observer=None):
    """One bounded cached-image runtime for an actual immutable release pair."""
    if type(config)not in (PuppyFiniteLoopbackConfig,PuppyGremlinLoopbackConfig)or type(receipt)is not dict:raise ValueError('Exact local runtime and preparation receipt required')
    config.__post_init__()
    if config.deadline_seconds>120:raise ValueError('Immutable export workflow deadline exceeds 120 seconds')
    if failure_observer is not None and not callable(failure_observer):raise ValueError('Trusted failure observer required')
    owner=object.__new__(OwnedPuppyExportLocal);ImmutableExportPreparationPolicy.__init__(owner,export)
    if receipt.get('release_sha256')!=export.release.sha256 or receipt.get('profile')!='ashlar-puppy-release-carrier/0.1':raise ValueError('Original prepared release receipt required')
    name=receipt.get('database_name')
    if name!='ashlar_'+export.release.sha256+'.duckdb':raise ValueError('Original SHA-qualified carrier name required')
    owner._config=config;owner.database=config.prepared/name
    owner.original_database=snapshot(owner.database,33554432);owner.original_model=snapshot(config.prepared/'model.json',1048576)
    if hashlib.sha256(owner.original_database).hexdigest()!=receipt['database_sha256']or hashlib.sha256(owner.original_model).hexdigest()!=receipt['model_sha256']:raise ValueError('Original complete prepared bytes differ')
    if owner.database.stat().st_mode&0o222:raise PermissionError('Read-only prepared carrier required')
    owner.deadline=time.monotonic()+config.deadline_seconds;owner.docker_diagnostics=[];owner.observations=[];owner._container_id=None;owner.driver=None;owner.closed=False;owner.loaded=False;owner.stage='image-preflight';owner.submission_outcome='not-submitted'
    owner.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),_NoRedirect());primary=None
    try:
        ImmutableExportPreparationPolicy.renew(owner)
        image=json.loads(owner._docker(['image','inspect',config.image]))
        if len(image)!=1 or image[0]['Id']!=config.image:raise PermissionError('Existing exact cached image required')
        owner._record('image',json_bytes(image,1048576));owner.stage='container-create'
        identifier=owner._docker(owner._create_arguments()).decode().strip()
        if not re.fullmatch('[0-9a-f]{64}',identifier):raise ValueError('Complete actual container ID required')
        owner._container_id=identifier;owner._docker(['start',identifier]);owner._container();owner.stage='startup'
        owner._wait_ready();owner._wait_backend_ready();owner.stage='caller';yield owner
    except BaseException as error:primary=error
    _close_owned_engine(owner,primary,failure_observer)



def _scenario_witness(config,pack_bytes,sources,exports,preparation,language):
    """Recompute original inputs and all four tables before container effects."""
    from .puppy_scenario_preparation import project_puppy_scenario_pack
    from .pack_scenario_queries import query_plan,carrier_chunks,raw_projection
    from .graphframes_pack_scenarios import bag
    if (type(pack_bytes)is not tuple or len(pack_bytes)!=2 or any(type(b)is not bytes or not 0<len(b)<=1048576 for b in pack_bytes)
            or type(sources)is not tuple or type(exports)is not tuple or len(sources)!=2 or len(exports)!=2
            or any(type(s)is not FinitePackSource for s in sources)
            or any(type(e)is not LocalGraphExportUse for e in exports)or type(preparation)is not dict):
        raise ValueError('Exact ordered scenario inputs and owners required')
    receipt_raw=snapshot(config.prepared/'receipt.json',16777216);receipt=json.loads(receipt_raw)
    database=config.prepared/'scenarios.duckdb';database_raw=snapshot(database,33554432)
    model_raw=snapshot(config.prepared/'model.json',1048576)
    if (receipt!=preparation or receipt.get('format')!='ashlar-original17-puppygraph-preparation/0.1'
            or receipt.get('engine_executed')is not False
            or any(path.stat().st_mode&0o222 for path in (database,config.prepared/'model.json',config.prepared/'receipt.json'))
            or len(database_raw)!=receipt.get('database_bytes')or hashlib.sha256(database_raw).hexdigest()!=receipt.get('database_sha256')
            or len(model_raw)!=receipt.get('model_bytes')or hashlib.sha256(model_raw).hexdigest()!=receipt.get('model_sha256')):
        raise ValueError('Exact scenario database/model/receipt required')
    projects=[];metadata=[];bindings=[];inventory=set();expected_model={'catalog':[{'name':'ashlar_scenarios','type':'duckdb','jdbc':{'jdbcUri':'jdbc:duckdb:/tmp/scenarios.duckdb'}}],'node':[],'edge':[]};observations=[]
    import duckdb
    with duckdb.connect(str(database),read_only=True)as connection:
        connection.execute('SET threads=1');connection.execute("SET memory_limit='64MB'");connection.execute("SET temp_directory=''")
        if connection.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='carrier'").fetchone()[0]!=4:
            raise ValueError('Exactly four original tables required')
        actual_tables=connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='carrier'").fetchall()
        if sorted(r[0]for r in actual_tables)!=['archaeology_edges','archaeology_nodes','ecology_edges','ecology_nodes']:
            raise ValueError('Exact four scenario tables required')
        for name,source,export,pack in zip(('archaeology','ecology'),sources,exports,pack_bytes):
            if source.definition.name!=name or source.context is not export.context:raise ValueError('Original ordered scenario owners required')
            item=project_puppy_scenario_pack(name,source.model,source.graph,pack,export.release.payload)
            projects.append(item);metadata.append(source.metadata());_,bound=source.definition.build(source.model,source.graph);bindings.append(bound)
            label='Scenario'+name.title()
            for kind in ('node','edge'):
                rows=item[kind+'s'];names=list(rows[0]);longs=set(item['long_columns'])if kind=='node'else set();table=name+'_'+kind+'s'
                schema={n:'BIGINT'if n in longs else'VARCHAR'for n in names}
                column_count=connection.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='carrier' AND table_name=?",[table]).fetchone()[0]
                if column_count!=len(names):raise ValueError('Exact original column count required')
                columns=connection.execute("SELECT column_name,data_type FROM information_schema.columns WHERE table_schema='carrier' AND table_name=? ORDER BY ordinal_position",[table]).fetchall()
                if columns!=list(schema.items()):raise ValueError('Exact native scenario schema required')
                if connection.execute('SELECT COUNT(*) FROM carrier.'+table).fetchone()[0]!=len(rows):
                    raise ValueError('Exact original scenario row count required')
                def cell_bytes(value):
                    if value is None:return 0
                    if type(value)is str:return len(value.encode('utf8'))
                    if type(value)is int and -(2**63)<=value<2**63:return len(str(value))
                    raise ValueError('Original bounded scenario cell type required')
                maxima={column:max(cell_bytes(row[column])for row in rows)for column in names}
                original_total=sum(cell_bytes(row[column])for row in rows for column in names)
                # Size/count observations precede host allocation, including compressed tampered DBs.
                native_total=0
                for column in names:
                    expression='octet_length(encode("'+column+'"))'if schema[column]=='VARCHAR'else'length(CAST("'+column+'" AS VARCHAR))'
                    maximum,total=connection.execute('SELECT COALESCE(MAX('+expression+'),0),COALESCE(SUM('+expression+'),0) FROM carrier.'+table).fetchone()
                    native_total+=total
                    if maximum>maxima[column]or native_total>original_total:raise ValueError('Original scenario cell byte bound exceeded')
                cursor=connection.execute('SELECT * FROM carrier.'+table);actual=[];actual_total=0
                while True:
                    row=cursor.fetchone()
                    if row is None:break
                    if len(actual)>=len(rows):raise ValueError('Original scenario row count exceeded')
                    for column,value in zip(names,row):
                        size=cell_bytes(value)
                        if size>maxima[column]:raise ValueError('Original scenario cell byte bound exceeded')
                        actual_total+=size
                        if actual_total>original_total:raise ValueError('Original scenario total byte bound exceeded')
                    actual.append(dict(zip(names,row)))
                if len(actual)!=len(rows)or bag(actual)!=bag(rows):raise ValueError('Complete scenario table bag differs')
                observations.append({'table':table,'rows':len(rows),'schema':schema,'full_cell_parity':True})
                attrs=[n for n in names if n not in ('id','graph_id')];key=kind+'_key'
                entry={'label':label+kind.title(),'dataSourceGroup':{'externalDataSource':{'enabled':True,'catalog':'ashlar_scenarios','schema':'carrier','table':table,'mappedField':[{'sourceFieldName':n,'targetFieldName':n}for n in attrs]+[{'sourceFieldName':'graph_id','targetFieldName':key}]}},'id':[{'name':key,'type':'STRING'}],'attribute':[{'name':n,'type':'LONG'if n in longs else'STRING'}for n in attrs]}
                if kind=='edge':entry.update(fromNodeLabel=label+'Node',toNodeLabel=label+'Node',fromKey=[{'name':'src','type':'STRING'}],toKey=[{'name':'dst','type':'STRING'}])
                expected_model[kind].append(entry)
                for chunk in carrier_chunks(item['profile'],kind,language,accepted_bindings=bound):
                    script=raw_projection(item['profile'],kind,language,chunk,accepted_bindings=bound)[0];inventory.add(json_bytes([script,{}],65536))
            for case in item['profile']['cases']:
                script,parameters,_=query_plan(item['profile'],case['original_scenario']['id'],language);inventory.add(json_bytes([script,parameters],65536))
    if (receipt['reports']!=projects or receipt['observations']!=observations or receipt['source_admissions']!=metadata
            or receipt['release_sha256s']!=[e.release.sha256 for e in exports]or receipt['custody_sha256s']!=[e.custody_sha256 for e in exports]
            or json.loads(model_raw)!=expected_model):raise ValueError('Original complete scenario mapping differs')
    return receipt_raw,database_raw,model_raw,tuple(metadata),frozenset(inventory)


def _scenario_sdk_size(rows,maximum_bytes=1048576):
    """Sizing projection only; original SDK rows and wire remain unchanged."""
    from gremlin_python.statics import long as SDKLong
    active=set();nodes=0
    def size(value,depth=0):
        nonlocal nodes
        nodes+=1
        if depth>64 or nodes>250000:raise ValueError('Bounded SDK structure required')
        if type(value)is SDKLong:
            integer=int.__int__(value)
            if not -(2**63)<=integer<2**63:raise ValueError('Exact signed64 SDK token required')
            return len(str(integer))
        if type(value)is int:
            if not -(2**63)<=value<2**63:raise ValueError('Exact finite SDK integer required')
            return len(str(value))
        if value is None:return 4
        if type(value)is str:
            if len(value)>maximum_bytes:raise ValueError('Bounded SDK text required')
            return len(json.dumps(value,ensure_ascii=False).encode())
        if type(value)not in (dict,list):raise ValueError('Unknown SDK carrier refused')
        if id(value)in active:raise ValueError('Cyclic SDK carrier refused')
        active.add(id(value))
        try:
            total=2
            for item in (value.items()if type(value)is dict else value):
                if type(value)is dict:
                    key,entry=item
                    if type(key)is not str:raise ValueError('Exact SDK field name required')
                    total+=size(key,depth+1)+size(entry,depth+1)+2
                else:total+=size(item,depth+1)+1
                if total>maximum_bytes:raise ValueError('Bounded SDK rows required')
            return total
        finally:active.remove(id(value))
    return size(rows)


class OwnedPuppyScenarioLocal(PuppyLocalRuntime):
    """Minted only by the public fixed original17 factory; no free-script port."""
    def _record(self,name,raw):
        if type(name)is not str or not 0<len(name)<=256 or type(raw)is not bytes:
            raise ValueError('Exact bounded scenario observation required')
        if len(self.observations)>=512:raise ValueError('Scenario observation count exceeded')
        digest=hashlib.sha256(raw).hexdigest()
        existing=self._scenario_bodies.get(digest)
        if existing is not None:
            if existing!=raw:raise ValueError('Scenario observation digest collision')
            retained=existing
        else:
            if self._scenario_observation_bytes+len(raw)>8388608:
                raise ValueError('Unique scenario observation capacity exceeded')
            retained=raw;self._scenario_bodies[digest]=raw;self._scenario_observation_bytes+=len(raw)
        self.observations.append((name,retained))
    def renew(self):
        def files():
            if (snapshot(self.database,33554432)!=self.original_database
                    or snapshot(self.config.prepared/'model.json',1048576)!=self.original_model
                    or snapshot(self.config.prepared/'receipt.json',16777216)!=self.original_receipt):
                raise PermissionError('Original scenario files changed')
        actions=[files]
        def construction_same():
            if (tuple(getattr(self.config,f.name)for f in fields(self.config)) != self._scenario_config or self.context is not self._scenario_context
                    or (self.maximum_rows,self.maximum_bytes)!=self._scenario_limits):
                raise PermissionError('Original scenario construction changed')
        actions.append(construction_same)
        for source,export,original,source_original,export_original in self._scenario_owners:
            def source_same(source=source,original=original,owned=source_original):
                if (source.context,source.definition,source.model,source.graph)!=owned or source.metadata()!=original:
                    raise PermissionError('Original scenario source changed')
            def export_same(export=export,owned=export_original):
                context,release,custody,historical=owned
                if (export.context is not context or export.release!=release or export.custody_bytes!=custody or export.source is not historical):
                    raise PermissionError('Original scenario export changed')
                export.admit_original_export(release,custody,historical,context)
            actions.extend([source_same,export_same])
        if self.container_id is not None:actions.append(self.renew_runtime)
        finish(None,actions)
    @contextmanager
    def hold(self,context):
        if context is not self.context or self.active:raise PermissionError('Exact nonnested scenario interval required')
        primary=None
        try:self.renew();self.active=True;yield
        except BaseException as error:primary=error
        try:finish(primary,[self.renew])
        finally:self.active=False
    def query_original(self,script,bindings,context):
        if (context is not self.context or not self.active or type(script)is not str or type(bindings)is not dict
                or json_bytes([script,bindings],65536)not in self._scenario_inventory):
            raise PermissionError('Only original admitted scenario/carrier query required')
        self.renew();remaining=self.deadline-time.monotonic()
        if remaining<=0:raise TimeoutError('Original scenario deadline elapsed')
        primary=None;rows=None;client=None;session=None;transports=[]
        try:
            if self.language=='Cypher':
                from neo4j import Query,GraphDatabase
                client=GraphDatabase.driver('bolt://127.0.0.1:'+str(self.config.bolt_port),auth=(self.config.username,self.config.password),connection_timeout=min(5,remaining),connection_acquisition_timeout=min(5,remaining))
                session=client.session();rows=[];size=0
                for record in session.run(Query(script,timeout=min(15,remaining)),parameters=bindings):
                    if time.monotonic()>=self.deadline:raise TimeoutError('Original scenario deadline elapsed')
                    row=record.data();size+=len(json_bytes(row,self.maximum_bytes))
                    if len(rows)>=self.maximum_rows or size>self.maximum_bytes:raise ValueError('Complete scenario result bound exceeded')
                    rows.append(row)
            else:
                from .puppy_gremlin_transport import require_gremlin_runtime,BoundedGremlinTransport,RetainedGraphSONSerializer
                from gremlin_python.driver.client import Client
                require_gremlin_runtime()
                directory=self.config.prepared.parent/'scenario-gremlin-wire';directory.mkdir(mode=0o700,exist_ok=True)
                selected=directory/str(sum(p.is_dir()for p in directory.iterdir())+1);selected.mkdir(mode=0o700)
                def transport_factory():
                    transport=BoundedGremlinTransport(self.deadline);transports.append(transport);return transport
                client=Client('ws://127.0.0.1:'+str(self.config.gremlin_port)+'/gremlin','g',username=self.config.username,password=self.config.password,message_serializer=RetainedGraphSONSerializer(selected,maximum_frame_bytes=self.maximum_bytes,maximum_total_bytes=self.maximum_bytes),pool_size=1,max_workers=2,transport_factory=transport_factory)
                reply=client.submit_async(script,bindings=bindings,request_options={'evaluationTimeout':int(min(15,remaining)*1000)}).result(timeout=min(15,remaining))
                from queue import Empty
                rows=[];size=2;query_deadline=min(self.deadline,time.monotonic()+15)
                while not reply.done.done() or not reply.stream.empty():
                    left=query_deadline-time.monotonic()
                    if left<=0:raise TimeoutError('Original scenario query deadline elapsed')
                    try:chunk=reply.stream.get(timeout=min(.05,left))
                    except Empty:continue
                    if type(chunk)is not list:raise ValueError('Exact SDK row batch required')
                    for row in chunk:
                        row_bytes=_scenario_sdk_size(row,self.maximum_bytes)
                        if len(rows)>=self.maximum_rows or size+row_bytes+1>self.maximum_bytes:
                            raise ValueError('Complete scenario SDK result bound exceeded')
                        size+=row_bytes+1;rows.append(row)
                reply.done.result(timeout=max(0,query_deadline-time.monotonic()))
        except BaseException as error:primary=error
        finish(primary,[transport.close for transport in transports]+([session.close]if session is not None else[])+([client.close]if client is not None else[])+[self.renew])
        return rows


@contextmanager
def _scenario_source_closing(sources):
    primary=None
    try:yield
    except BaseException as error:primary=error
    finish(primary,[source.renew for source in sources])


@dataclass(frozen=True)
class PuppyScenarioContainerReceipt:
    """Actual create ID plus explicit invocation selections, not inspection authority."""
    container_id:str
    image:str
    name:str
    invocation_nonce:str
    readonly_mount_source:Path
    readonly_mount_target:str
    http_port:int
    bolt_port:int
    gremlin_port:Optional[int]
    cpus:int
    memory_bytes:int


@contextmanager
def open_puppy_scenario_local(config,*,pack_bytes,sources,exports,preparation,native_map_profile,failure_observer=None,maximum_rows=1000,maximum_bytes=1048576,creation_observer=None,invocation_nonce=None):
    """Fresh four-table namespace, bounded readiness and one model POST only."""
    from contextlib import ExitStack
    from .pack_scenario_queries import MAP_PUPPY
    if type(config)not in (PuppyFiniteLoopbackConfig,PuppyGremlinLoopbackConfig):
        raise ValueError('Explicit original17 runtime required')
    if ((type(config)is PuppyGremlinLoopbackConfig and (type(native_map_profile)is not str or native_map_profile!=MAP_PUPPY))
            or (type(config)is PuppyFiniteLoopbackConfig and native_map_profile is not None)):
        raise ValueError('Explicit protocol-specific original map profile required')
    if failure_observer is not None and not callable(failure_observer):raise ValueError('Private bounded observer required')
    if ((creation_observer is None)!=(invocation_nonce is None)
            or (creation_observer is not None and (not callable(creation_observer)or type(invocation_nonce)is not str or re.fullmatch('[0-9a-f]{32}',invocation_nonce)is None))):
        raise ValueError('Paired explicit scenario creation observer and nonce required')
    if (type(maximum_rows)is not int or not 1<=maximum_rows<=1000 or type(maximum_bytes)is not int or not 1<=maximum_bytes<=1048576):
        raise ValueError('Explicit finite scenario capture bounds required')
    config.__post_init__();language='Gremlin'if type(config)is PuppyGremlinLoopbackConfig else'Cypher'
    if type(sources)is not tuple or len(sources)!=2 or any(type(source)is not FinitePackSource for source in sources):
        raise ValueError('Actual two source owners required')
    with ExitStack()as held:
        held.enter_context(_scenario_source_closing(sources))
        if type(exports)is not tuple or len(exports)!=2 or any(type(e)is not LocalGraphExportUse for e in exports):raise ValueError('Original export owners required')
        for export in exports:held.enter_context(owned_context(export.writer(export.context)))
        receipt,database,model,metadata,inventory=_scenario_witness(config,pack_bytes,sources,exports,preparation,language)
        owner=object.__new__(OwnedPuppyScenarioLocal);owner._config=config;owner.context=object();owner.active=False;owner._scenario_config=tuple(getattr(config,f.name)for f in fields(config));owner._scenario_context=owner.context;owner._scenario_owners=tuple((source,export,original,(source.context,source.definition,source.model,source.graph),(export.context,export.release,export.custody_bytes,export.source))for source,export,original in zip(sources,exports,metadata))
        owner.database=config.prepared/'scenarios.duckdb';owner.original_database=database;owner.original_model=model;owner.original_receipt=receipt;owner._scenario_inventory=inventory;owner.language=language;owner.maximum_rows=maximum_rows;owner.maximum_bytes=maximum_bytes;owner._scenario_limits=(maximum_rows,maximum_bytes)
        owner.requested_model=json.loads(model);owner.requested_bytes=model;owner.deadline=time.monotonic()+config.deadline_seconds
        owner.observations=[];owner.docker_diagnostics=[];owner._container_id=None;owner.driver=None;owner.closed=False;owner.loaded=False;owner.stage='image-preflight';owner.submission_outcome='not-submitted';owner._scenario_invocation_nonce=invocation_nonce;owner._scenario_bodies={};owner._scenario_observation_bytes=0
        owner.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),_NoRedirect());primary=None
        try:
            owner.renew()
            image=json.loads(owner._docker(['image','inspect',config.image]))
            if len(image)!=1 or image[0]['Id']!=config.image:raise PermissionError('Existing exact cached image required')
            identifier=owner._docker(owner._create_arguments()).decode().strip()
            if not re.fullmatch('[0-9a-f]{64}',identifier):raise ValueError('Owned original container ID required')
            owner._container_id=identifier
            if creation_observer is not None:
                creation_observer(PuppyScenarioContainerReceipt(identifier,config.image,config.name,invocation_nonce,
                    owner.database,'/tmp/'+owner.database.name,config.http_port,config.bolt_port,
                    config.gremlin_port if type(config)is PuppyGremlinLoopbackConfig else None,2,3*1024**3))
            owner._docker(['start',identifier]);owner._container();owner.stage='startup'
            owner._wait_ready();owner._wait_backend_ready();owner.stage='model-submission';owner.submission_outcome='unknown'
            reply=owner._http('/schema?postUploadBehavior=none',owner.requested_bytes);owner._record('submission',reply)
            if json.loads(reply).get('ok')is not True:owner.submission_outcome='refused';raise ValueError('Native original model refused')
            owner.submission_outcome='accepted';owner.loaded=True;owner.renew();owner.stage='caller';yield owner
        except BaseException as error:primary=error
        _close_owned_engine(owner,primary,failure_observer)
