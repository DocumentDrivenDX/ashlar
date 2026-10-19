# SPDX-License-Identifier: MIT OR Apache-2.0
"""Configured source custody for the existing public typed page adapter.

No new page compiler/executor or producer authority is introduced. Results remain
provisional until the enclosing configured runtime and alias owners close.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
import hashlib,importlib,json,threading,time,uuid
from ashlar.native import SQLResult
from ashlar.weft_binding import LAYOUT_SHA256
from .configured_local_read import ConfiguredLocalRead
from .configured_query_aliases import ConfiguredQueryAliases,open_configured_query_aliases
from .configured_typed_graph_sdk_binding import configured_typed_graph_sdk_request
from .configured_sdk_page_oracle import configured_sdk_page_oracle
from .delta_custody import encoded
from .path_capture import PathCaptureConfig,capture_string_frame
from .lifecycle import finish,owned_context


def _require(value):
    if not value:raise ValueError('Configured typed SDK page custody refused')


@dataclass(frozen=True)
class ConfiguredSdkPagesConfig:
    compiler:object
    sql:str
    parameters_type:type
    result_type:type
    parameters:object
    page_size:int
    maximum_rows:int
    timeout_seconds:int
    capture:PathCaptureConfig
    def __post_init__(self):
        _require(all(callable(getattr(self.compiler,n,None))for n in ('compile_engine_page_json','check_engine_page_json','check_engine_read_json')))
        _require(type(self.sql)is str and 0<len(self.sql.encode())<=65536 and '\0'not in self.sql)
        _require(type(self.parameters)is self.parameters_type and callable(getattr(self.parameters,'model_dump',None)))
        values=self.parameters.model_dump(mode='python');_require(type(values)is dict and len(values)<=128 and all(type(k)is str and type(v)is str and len(v.encode())<=65536 for k,v in values.items()))
        _require(len(encoded(values).encode())<=1048576 and type(self.result_type)is type(self.parameters_type)and hasattr(self.result_type,'model_fields'))
        _require(type(self.page_size)is int and 1<=self.page_size<=999 and type(self.maximum_rows)is int and 1<=self.maximum_rows<=1000)
        _require(type(self.timeout_seconds)is int and 1<=self.timeout_seconds<=120 and type(self.capture)is PathCaptureConfig)
        self.capture.__post_init__();_require(self.capture.maximum_rows>=self.page_size+1 and self.capture.maximum_rows<=1000 and self.capture.maximum_cell_bytes<=65536 and self.capture.maximum_total_cell_bytes<=1048576)


class _Owner:
    def __init__(self,scope,aliases,query,entry,request,runtime_guard):
        _require(type(scope)is ConfiguredLocalRead and type(aliases)is ConfiguredQueryAliases and aliases.scope is scope and callable(runtime_guard))
        self.scope,self.aliases,self.query,self.entry,self.request=scope,aliases,query,entry,request
        self.context=scope.context;self.runtime_guard=runtime_guard;self.jars=tuple(map(str,runtime_guard()))
        _require(len(self.jars)==2)
        self.program=entry.artifact();self.original=entry.originals();self.original_request=encoded(request)
        self.oracle=configured_sdk_page_oracle(scope,request,self.program)
        self.columns=self.program['artifact']['columns'];self.key=self.program['artifact']['logicalPlan']['pageKey']
        self.record=self.program['artifact']['logicalPlan']['source']['record']
        self.profile=next(o['parameters']for o in self.program['artifact']['obligations']if o['id']=='ashlar.nativeProfile')
        self.token=encoded({'profile':'ashlar-configured-sdk-pages-local/0.1','owner':uuid.uuid4().hex,
            'source':scope.source.metadata(self.context),'manifest':dict(scope.descriptor.raw),
            'vector':dict(scope.vector.targets),'aliases':dict(aliases),'binding':request['target']['bindingSha256']}).encode()
        _require(len(self.token)<=65536)
        self.active=False;self.after=None;self.checked=None;self.retained=0;self.observations=[];self.frames=[]
    def current(self):
        self.scope.renew();self.aliases.renew()
        _require(tuple(map(str,self.runtime_guard()))==self.jars)
        spark=self.scope.transport.spark
        _require(spark.version=='4.0.1'and spark.sparkContext.master=='local[1]'
            and spark.conf.get('spark.sql.ansi.enabled')=='true'and spark.conf.get('spark.sql.session.timeZone')=='UTC'
            and set(spark.conf.get('spark.jars').split(','))==set(self.jars))
        _require(encoded(self.request)==self.original_request and self.entry.originals()==self.original)
    def bind_descriptor(self,descriptor,vector,context):
        _require(self.active and context is self.context);self.current();self.scope.bind_descriptor(descriptor,vector,context)
    def admit_artifact(self,request,artifact,descriptor,context):
        self.bind_descriptor(descriptor,self.scope.vector,context)
        rebuilt=configured_typed_graph_sdk_request(self.scope.source.configuration,dict(descriptor.raw),self.scope.vector,self.scope.manifest_uuid,dict(self.aliases),self.query.sql,request['parameters'],layout_sha256=LAYOUT_SHA256)
        _require(request==rebuilt and request==self.request)
        checked=json.loads(self.query.compiler.check_engine_page_json(self.original,encoded(request),encoded(None if self.after is None else list(self.after))))
        _require(checked.get('status')=='compiled'and checked['artifact']==artifact and checked['outputs']==self.program['outputs']
            and artifact['columns']==self.columns and artifact['logicalPlan']['pageKey']==self.key)
        self.checked=checked
    def verify_native_profile(self,profile,context):
        _require(self.active and context is self.context and profile==self.profile and profile['engineVersion']=='4.0.1'and profile['deltaVersion']=='4.0.0');self.current()
        spark=self.scope.transport.spark
        _require(spark.sql("SELECT ('a' COLLATE UTF8_BINARY) = ('A' COLLATE UTF8_BINARY) AS equal").first()['equal']is False)
    def authorize_query(self,request,descriptor,context):
        self.bind_descriptor(descriptor,self.scope.vector,context);_require(request==self.request and self.checked is not None)
    def admit_record_integrity(self,check,artifact,context):
        _require(self.active and context is self.context and self.checked is not None and artifact==self.checked['artifact']and check.get('record')==self.record)
        _require(any(check in o['parameters'].get('checks',[])for o in artifact['obligations']if o['id']=='ashlar.candidate.scalarIntegrity'));self.current()
    def admit_page_integrity(self,guard,artifact,context):
        _require(self.active and context is self.context and guard.get('key')==self.key and self.checked is not None and artifact==self.checked['artifact']);self.current()
    def page_context(self,descriptor,vector,context):
        self.bind_descriptor(descriptor,vector,context);return bytes(self.token)
    def decode_result(self,column,value,request,context):
        _require(self.active and context is self.context and request==self.request and column in self.columns and type(value)is str)
        _require(column['representation']=={'kind':'scalar','logicalType':{'family':'string','facets':{},'nullable':False},'carrier':'text','decoder':'text'})
        return value
    def authorize_result(self,rows,descriptor,context):
        self.bind_descriptor(descriptor,self.scope.vector,context)
        expected=self.oracle({n:v['value']for n,v in self.request['parameters'].items()},self.after)
        _require([list(r)for r in rows]==expected['rows'])
        self.observations.append({'after':None if self.after is None else list(self.after),'program':self.checked,'oracle':expected,'frames':self.frames})
    @contextmanager
    def hold_page(self,vector,*,context):
        _require(not self.active and context is self.context and vector==self.scope.vector)
        primary=None
        try:
            with owned_context(self.scope.pins.hold(vector,context=context)):
                self.active=True;self.current();yield vector
        except BaseException as error:primary=error
        def deactivate():self.active=False
        finish(primary,[self.scope.parity,self.current,deactivate])
    def query_frame(self,sql,parameters):
        _require(self.active and self.checked is not None and type(sql)is str and 0<len(sql.encode())<=1048576)
        artifact=self.checked['artifact'];slots={'p'+str(s['position']):s['value']for s in artifact['parameters']}
        allowed=[artifact['sql']]+[c['sql']for o in artifact['obligations']for c in o['parameters'].get('checks',[])]
        _require(sql in allowed and parameters==slots)
        remaining=self.query.capture.maximum_total_cell_bytes-self.retained;_require(remaining>0)
        bounds=PathCaptureConfig(self.query.capture.maximum_rows,min(self.query.capture.maximum_cell_bytes,remaining),remaining)
        observed=capture_string_frame(self.scope.transport.spark.sql(sql,args=parameters),config=bounds)
        self.retained+=sum(len(v.encode())for r in observed['rows']for v in r if v is not None)
        self.frames.append({'sql_sha256':hashlib.sha256(sql.encode()).hexdigest(),'schema':observed['schema'],'rows':observed['rows']})
        return SQLResult([dict(zip((n for n,_ in observed['schema']),r))for r in observed['rows']],tuple(tuple(p)for p in observed['schema']))
    @contextmanager
    def deadline(self,budget):
        spark=self.scope.transport.spark.sparkContext;group='configured-sdk-'+uuid.uuid4().hex
        keys=('spark.jobGroup.id','spark.job.description','spark.job.interruptOnCancel');previous={k:spark.getLocalProperty(k)for k in keys}
        timer=threading.Timer(budget.total_seconds(),lambda:spark.cancelJobGroup(group));timer.daemon=True;primary=None
        try:
            spark.setJobGroup(group,'bounded configured typed SDK pages',interruptOnCancel=True);timer.start();yield
        except BaseException as error:primary=error
        finish(primary,[timer.cancel,*[lambda k=k:spark.setLocalProperty(k,previous[k])for k in keys]])


def run_configured_sdk_pages(scope,transport,query,*,runtime_guard):
    """Use AshlarTarget pages, then close aliases before returning provisional JSON."""
    from ashlar.access import AshlarTarget
    from umf_access.engine_catalog import EngineRead
    import importlib.metadata
    _require(type(query)is ConfiguredSdkPagesConfig);query.__post_init__()
    _require(query.compiler is importlib.import_module('weft')and importlib.metadata.version('weft-sql')=='0.4.41')
    values=query.parameters.model_dump(mode='python');parameters={n:{'family':'string','value':v}for n,v in values.items()}
    started=time.monotonic();items=[];pages=[];original=None
    with owned_context(open_configured_query_aliases(scope,transport))as aliases:
        request=configured_typed_graph_sdk_request(scope.source.configuration,dict(scope.descriptor.raw),scope.vector,scope.manifest_uuid,dict(aliases),query.sql,parameters,layout_sha256=LAYOUT_SHA256)
        entry=EngineRead.build(query.compiler,request,parameters=query.parameters_type,result=query.result_type,method='ashlar-spark4-pages',page_size=query.page_size)
        owner=_Owner(scope,aliases,query,entry,request,runtime_guard)
        class Executor:
            def query(self,sql,parameters):return owner.query_frame(sql,parameters)
        target=AshlarTarget(compiler=query.compiler,compiler_method='ashlar-spark4-pages',executor=Executor(),backend=scope.backend,pins=owner,vector=scope.vector,policy=owner,context=scope.context,
            supported_profiles=['ashlar-delta/0.3'],supported_revisions={scope.source.configuration.schema_alias:[scope.source.configuration.document_revision]},binding_sha256=request['target']['bindingSha256'],
            platform=(('spark','4.0.1'),('delta','4.0.0')),default_timeout=timedelta(seconds=query.timeout_seconds),deadline=owner.deadline,catalog_entries=(entry,))
        after=None
        while len(items)<query.maximum_rows:
            owner.current();remaining=query.timeout_seconds-(time.monotonic()-started);_require(remaining>0)
            owner.after=None if after is None else tuple(after.values);owner.retained=0;owner.checked=None;owner.frames=[]
            page=target.page(entry,query.parameters,after=after,timeout=timedelta(seconds=remaining))
            observed=[v.model_dump(mode='python')for v in page.items];_require(all(type(v)is str for r in observed for v in r.values()))
            take=min(len(observed),query.maximum_rows-len(items));items.extend(observed[:take]);pages.append({'items':observed[:take],'has_more':page.next_after is not None or take<len(observed)})
            _require(len(encoded([items,pages,owner.observations,entry.artifact()]).encode())<=1048576)
            after=page.next_after
            if after is None:break
        owner.current();scope.parity();original=entry.originals();observations=owner.observations
    scope.renew();scope.parity()
    return {'profile':'ashlar-configured-sdk-pages/0.1','sql':query.sql,'items':items,'pages':pages,'has_more':bool(pages and pages[-1]['has_more']),'program':json.loads(original),'evidence':observations,
        'qualification':'Provisional required STRING pages over an original configured graph publication; enclosing runtime cleanup required. No protected ACK or PostgreSQL/remote authority.'}
