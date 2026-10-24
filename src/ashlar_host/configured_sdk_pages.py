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
from .configured_local_runtime_profile import JAR_SHA256,PARAMETER_SUBSTITUTION_SETTING
from ashlar.weft_current_distribution import CurrentWeftDistribution
from .configured_sdk_page_oracle import configured_sdk_page_oracle
from .delta_custody import encoded
from .path_capture import PathCaptureConfig,capture_string_frame
from .lifecycle import finish,owned_context


def _require(value):
    if not value:raise ValueError('Configured typed SDK page custody refused')


def _supported_page_compiler(value):
    return type(value)is CurrentWeftDistribution and value._verify()


def _require_frozen_page_response(frozen,observed):
    _require(type(frozen)is bytes and type(observed)is bytes and frozen==observed)


def _require_frozen_grouped_artifact(compiled,record):
    import hashlib
    _require(type(compiled)is dict and type(record)is dict
        and hashlib.sha256(compiled.get('requestBytes',b'')).hexdigest()==record.get('requestSha256')
        and hashlib.sha256(compiled.get('responseBytes',b'')).hexdigest()==record.get('responseSha256')
        and compiled.get('responseBytes')==compiled.get('recompiledBytes'))


def _require_grouped_exact_rows(evidence,expected_rows):
    """Check lexical carrier custody and the independent exact-value oracle."""
    from ashlar.weft_decode import ExactScalar
    _require(type(evidence)is dict and type(expected_rows)is list
        and type(evidence.get('native_result'))is dict
        and type(evidence['native_result'].get('rows'))is list
        and type(evidence.get('decoded'))is list)
    carriers=evidence['native_result']['rows'];decoded=evidence['decoded']
    _require(len(carriers)==len(decoded))
    values=[]
    for carrier_row,decoded_row in zip(carriers,decoded):
        _require(type(carrier_row)is list and type(decoded_row)is list
            and len(carrier_row)==len(decoded_row))
        value_row=[];original_row=[]
        for carrier,scalar in zip(carrier_row,decoded_row):
            _require(type(scalar)is ExactScalar and scalar.original==carrier)
            value_row.append(scalar.value);original_row.append(scalar.original)
        _require(original_row==carrier_row)
        values.append(value_row)
    _require(values==expected_rows)


def _require_r02_ordinal1_vector(live_vector,pinned_vector,pinned_sha256):
    """Refuse before SQL unless live scope pins the retained first publication."""
    import hashlib
    from .delta_custody import encoded
    _require(type(live_vector)is dict and type(pinned_vector)is dict
        and type(pinned_sha256)is str and len(pinned_sha256)==64
        and hashlib.sha256((encoded(pinned_vector)+'\n').encode()).hexdigest()==pinned_sha256
        and live_vector==pinned_vector)


def _visible_page_rows(program,carrier_rows):
    """Retain compiler carriers but compare SDK rows using visible outputs only."""
    page=program.get('page')if type(program)is dict else None
    visible=page.get('visibleOutputs')if type(page)is dict else None
    key_outputs=page.get('keyOutputs')if type(page)is dict else None
    _require(type(visible)is int and visible>0 and type(key_outputs)is list and len(key_outputs)>0
        and type(carrier_rows)is list and all(type(row)is list and len(row)==visible+len(key_outputs)for row in carrier_rows))
    return [list(row[:visible])for row in carrier_rows]


@dataclass(frozen=True)
class ConfiguredR02Weft053Selection:
    """Private invocation data for the R02 packet runner; not CLI selectable."""
    attempt_root:object
    attempt_id:str
    source_revision:str
    prior_publication_source_revision:str
    runner_sha256:str
    gate_sha256:str
    runtime:dict
    oracle_sha256:str
    def __post_init__(self):
        from pathlib import Path
        from types import MappingProxyType
        valid_runtime=isinstance(self.runtime,(dict,MappingProxyType))
        _require(isinstance(self.attempt_root,Path) and self.attempt_root.is_absolute()
            and type(self.attempt_id)is str and len(self.attempt_id)==32
            and all(c in '0123456789abcdef'for c in self.attempt_id)
            and type(self.source_revision)is str and len(self.source_revision)==40
            and type(self.prior_publication_source_revision)is str and len(self.prior_publication_source_revision)==40
            and all(c in '0123456789abcdef'for c in self.source_revision+self.prior_publication_source_revision)
            and type(self.runner_sha256)is str and len(self.runner_sha256)==64
            and type(self.gate_sha256)is str and len(self.gate_sha256)==64
            and valid_runtime and self.oracle_sha256=='f04f74668ac9adc46ac9928b01a4155ccad3aab6421cd297d018b7e45348aab0'
        )
        object.__setattr__(self,'runtime',MappingProxyType(dict(self.runtime)))


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
    def __init__(self,scope,aliases,query,entry,request,runtime_guard,*,page_gate=None,
                 expected_page1_artifact=None,allow_053=False):
        _require(type(scope)is ConfiguredLocalRead and type(aliases)is ConfiguredQueryAliases and aliases.scope is scope and callable(runtime_guard))
        self.scope,self.aliases,self.query,self.entry,self.request=scope,aliases,query,entry,request
        self.page_gate=page_gate;self.expected_page1_artifact=expected_page1_artifact;self.allow_053=allow_053
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
        self.native_profile_verified=False
        self.continuation_checked=None
    def current(self):
        self.scope.renew();self.aliases.renew()
        if type(self.query.compiler)is CurrentWeftDistribution:
            _require(self.query.compiler._verify())
        else:
            from ashlar.weft_053_distribution import Weft053Distribution
            _require(self.allow_053 and self.page_gate is not None
                and type(self.query.compiler)is Weft053Distribution and self.query.compiler._verify())
        _require(tuple(map(str,self.runtime_guard()))==self.jars)
        spark=self.scope.transport.spark
        _require(spark.version=='4.2.0'and spark.sparkContext.master=='local[1]'
            and spark.sparkContext._jvm.java.lang.System.getProperty('java.specification.version')=='21'
            and spark.conf.get('spark.sql.ansi.enabled')=='true'and spark.conf.get('spark.sql.session.timeZone')=='UTC'
            and spark.conf.get(PARAMETER_SUBSTITUTION_SETTING)=='true'
            and set(spark.conf.get('spark.jars').split(','))==set(self.jars))
        from .configured_local_runtime_profile import verify_loaded_delta_class
        verify_loaded_delta_class(spark,[__import__('pathlib').Path(p) for p in self.jars])
        _require(encoded(self.request)==self.original_request and self.entry.originals()==self.original)
    def bind_descriptor(self,descriptor,vector,context):
        _require(self.active and context is self.context);self.current();self.scope.bind_descriptor(descriptor,vector,context)
    def admit_artifact(self,request,artifact,descriptor,context):
        self.bind_descriptor(descriptor,self.scope.vector,context)
        rebuilt=configured_typed_graph_sdk_request(self.scope.source.configuration,dict(descriptor.raw),self.scope.vector,self.scope.manifest_uuid,dict(self.aliases),self.query.sql,request['parameters'],layout_sha256=LAYOUT_SHA256)
        _require(request==rebuilt and request==self.request)
        checked_raw=self.query.compiler.check_engine_page_json(self.original,encoded(request),encoded(None if self.after is None else list(self.after)))
        checked=json.loads(checked_raw)
        _require(checked.get('status')=='compiled'and checked['artifact']==artifact and checked['outputs']==self.program['outputs']
            and artifact['columns']==self.columns and artifact['logicalPlan']['pageKey']==self.key)
        if self.after is None and self.expected_page1_artifact is not None:
            _require(artifact==self.expected_page1_artifact)
            _require_frozen_page_response(self.expected_page1_checked_response,checked_raw.encode())
        if self.after is not None and self.page_gate is not None and self.page_gate.stage=='post-page1':
            _require(self.continuation_checked==checked_raw.encode())
        self.checked=checked
    def verify_native_profile(self,profile,context):
        _require(self.active and context is self.context and profile==self.profile and profile['targetProfile']=='spark4.2.0-delta4.4.1-candidate'
            and profile['engineVersion']=='4.2.0'and profile['deltaVersion']=='4.4.1'and profile['jdkVersion']=='21'
            and profile['deltaJars']==JAR_SHA256
            and profile['settings']=={'spark.sql.ansi.enabled':'true','spark.sql.session.timeZone':'UTC',
                'spark.sql.legacy.parameterSubstitution.constantsOnly':'true','comparison':'explicit UTF8_BINARY','resultTransport':'exact native STRING'});self.current()
        if not self.native_profile_verified:
            spark=self.scope.transport.spark
            _require(spark.sql("SELECT ('a' COLLATE UTF8_BINARY) = ('A' COLLATE UTF8_BINARY) AS equal").first()['equal']is False)
            self.native_profile_verified=True
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
        def capture():return capture_string_frame(self.scope.transport.spark.sql(sql,args=parameters),config=bounds)
        if self.allow_053:
            gate=self.page_gate
            _require(gate is not None and gate.consumed and gate.executing_name is not None)
            if self.after is None:
                _require(gate.stage=='pre-sql' and gate.executing_name=='backend02-page1')
            else:
                _require(gate.stage=='post-page1' and gate.executing_name=='backend02-continuation'
                    and self.native_profile_verified)
        observed=capture()
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
    _require(type(query)is ConfiguredSdkPagesConfig);query.__post_init__()
    _require(_supported_page_compiler(query.compiler))
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
            platform=(('spark','4.2.0'),('delta','4.4.1')),default_timeout=timedelta(seconds=query.timeout_seconds),deadline=owner.deadline,catalog_entries=(entry,))
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


class ConfiguredSdk053LiveSession:
    """.53 SDK page owner usable only with the R02 two-stage review gate."""
    def __init__(self,scope,transport,query,aliases,*,review_gate,runtime_guard,expected_continuation_after):
        from ashlar.weft_053_distribution import Weft053Distribution
        from umf_access.engine_catalog import EngineRead
        _require(type(query)is ConfiguredSdkPagesConfig and type(query.compiler)is Weft053Distribution
            and type(aliases)is ConfiguredQueryAliases and aliases.scope is scope
            and callable(runtime_guard) and review_gate.stage=='pre-sql')
        query.__post_init__();_require(query.compiler._verify())
        selected_jars=tuple(map(str,runtime_guard()));_require(len(selected_jars)==2)
        spark=scope.transport.spark
        _require(spark.version=='4.2.0'and spark.sparkContext.master=='local[1]'
            and spark.sparkContext._jvm.java.lang.System.getProperty('java.specification.version')=='21'
            and spark.conf.get('spark.sql.ansi.enabled')=='true'and spark.conf.get('spark.sql.session.timeZone')=='UTC'
            and spark.conf.get(PARAMETER_SUBSTITUTION_SETTING)=='true'
            and set(spark.conf.get('spark.jars').split(','))==set(selected_jars))
        from .configured_local_runtime_profile import verify_loaded_delta_class
        verify_loaded_delta_class(spark,[__import__('pathlib').Path(p)for p in selected_jars])
        self.scope,self.transport,self.query,self.aliases=scope,transport,query,aliases
        self.review_gate=review_gate;self.expected_continuation_after=expected_continuation_after
        values=query.parameters.model_dump(mode='python')
        parameters={n:{'family':'string','value':v}for n,v in values.items()}
        request=configured_typed_graph_sdk_request(scope.source.configuration,dict(scope.descriptor.raw),
            scope.vector,scope.manifest_uuid,dict(aliases),query.sql,parameters,layout_sha256=LAYOUT_SHA256)
        envelope={'version':'weft.engine-page/0.1','compiler':'ashlar-spark4-pages',
            'request':request,'pageSize':query.page_size}
        request_raw=encoded(envelope)
        first=query.compiler.compile_engine_page_json(request_raw)
        second=query.compiler.compile_engine_page_json(request_raw)
        _require(type(first)is str and first==second and 0<len(first.encode())<=1048576)
        self.page1_response=first.encode();self.page1_request=request_raw.encode()
        self.entry=EngineRead.from_original(first,parameters=query.parameters_type,result=query.result_type)
        program=json.loads(first);self.page1_artifact=program['artifact']
        self.expected_page1_carrier_rows=configured_sdk_page_oracle(scope,request,program)(values,None)['rows'][:query.page_size]
        self.expected_page1_rows=_visible_page_rows(program,self.expected_page1_carrier_rows)
        self.owner=_Owner(scope,aliases,query,self.entry,request,runtime_guard,page_gate=review_gate,
            expected_page1_artifact=self.page1_artifact,allow_053=True)
        _require(self.owner.jars==selected_jars)
        self.page1_checked_response=None
        class Executor:
            def query(_self,sql,parameters):return self.owner.query_frame(sql,parameters)
        from ashlar.access import AshlarTarget
        self.target=AshlarTarget(compiler=query.compiler,compiler_method='ashlar-spark4-pages',executor=Executor(),
            backend=scope.backend,pins=self.owner,vector=scope.vector,policy=self.owner,context=scope.context,
            binding_sha256=request['target']['bindingSha256'],
            supported_profiles=['ashlar-delta/0.3'],
            supported_revisions={scope.source.configuration.schema_alias:[scope.source.configuration.document_revision]},
            platform=(('spark','4.2.0'),('delta','4.4.1')),
            default_timeout=timedelta(seconds=query.timeout_seconds),deadline=self.owner.deadline,catalog_entries=(self.entry,))
    def frozen_page1_artifact(self,continuation_after):
        _require(continuation_after==self.expected_continuation_after)
        _require(self.page1_checked_response is None)
        checked=self.query.compiler.check_engine_page_json(self.entry.originals(),
            encoded(self.entry.artifact()['request']),encoded(None))
        parsed=json.loads(checked)
        _require(parsed.get('status')=='compiled' and parsed.get('artifact')==self.page1_artifact)
        self.page1_checked_response=checked.encode()
        self.owner.expected_page1_checked_response=self.page1_checked_response
        return {'request':self.page1_request,'response':self.page1_checked_response,
            'programResponse':self.page1_response,
            'expected':{'rows':self.expected_page1_rows,'carrierRows':self.expected_page1_carrier_rows,
                'continuationAfter':continuation_after}}
    def page(self,after=None):
        # The page-1 read is one ordered operation in the pre-SQL review
        # receipt.  A caller cannot use this .53 adapter as an ungated SQL
        # route, even though the public .48 runner remains available.
        if after is None:
            _require(self.review_gate.stage=='pre-sql' and self.review_gate.consumed
                and self.review_gate.approved and self.review_gate.executing_name=='backend02-page1')
        else:
            _require(self.review_gate.stage=='post-page1' and self.review_gate.prior_stage_sha is not None
                and type(after.values)is tuple)
        self.owner.current()
        self.owner.after=None if after is None else tuple(after.values)
        self.owner.retained=0;self.owner.checked=None;self.owner.frames=[]
        if after is None:
            # Give each native page operation a fresh timeout after review;
            # reviewer wait is bounded by the separate process deadline.
            return self.target.page(self.entry,self.query.parameters,after=None,
                timeout=timedelta(seconds=self.query.timeout_seconds))
        # Freeze the exact continuation check before page execution.  The gate
        # then encloses the complete target.page call, including every SQL
        # statement the SDK may issue for this page.
        request=self.entry.artifact()['request']
        checked_raw=self.query.compiler.check_engine_page_json(self.entry.originals(),
            encoded(request),encoded(list(after.values)))
        checked=json.loads(checked_raw)
        _require(checked.get('status')=='compiled')
        request_raw=encoded({'version':'weft.engine-page/0.1','compiler':'ashlar-spark4-pages',
            'request':request,'pageSize':self.query.page_size}).encode()
        after_raw=encoded(list(after.values))
        expected=self.owner.oracle({n:v['value']for n,v in request['parameters'].items()},tuple(after.values))
        self.owner.continuation_checked=checked_raw.encode()
        self.review_gate.freeze({'backend02-continuation':{'request':request_raw,
            'response':checked_raw.encode(),'expected':expected,'after':after_raw}})
        def execute_page2():
            return self.target.page(self.entry,self.query.parameters,after=after,
                timeout=timedelta(seconds=self.query.timeout_seconds))
        return self.review_gate.execute_after_review(self.review_gate.root/'post-page1-review.json',
            lambda run:run('backend02-continuation',execute_page2))


def run_configured_r02_weft053(scope,transport,query,aliases,selection,*,runtime_guard):
    """Run the packet's exact ordered checks under one live alias owner.

    This is deliberately R02-specific. It compiles both grouped requests with
    the selected .53 compiler under the live aliases, then freezes them with
    the singleton and page-1 artifacts. Those four actions run only through
    the pre-SQL gate. Page 2 freezes its response in the same scope and waits
    for its own review.
    """
    from .delta_custody import encoded
    from .configured_weft_query import query_configured_projection_precompiled
    from .configured_weft_compile import compile_configured_count_star_053
    from ashlar.weft_path_decode import PathDecodeConfig
    from pathlib import Path
    import importlib.util
    import hashlib,json
    _require(type(selection)is ConfiguredR02Weft053Selection);selection.__post_init__()
    _require(scope.context is not None and type(query.compiler).__name__=='Weft053Distribution')
    roles=('edge_current','object_current','tombstone','whole_source_history')
    by_role={table.rsplit('.',1)[1]:(table,pair)for table,pair in scope.vector.targets.items()}
    _require(set(by_role)==set(roles))
    vector={'publicationId':scope.descriptor.publication_id,'manifestUuid':scope.manifest_uuid,
        'tables':{role:{'uuid':by_role[role][1][0],'version':by_role[role][1][1]}for role in roles}}
    _require_r02_ordinal1_vector(vector,selection.runtime.get('ordinal1PublicationVector'),
        selection.runtime.get('ordinal1PublicationVectorSha256'))
    oracle_path=Path(__file__).resolve().parents[2]/'docs/helix/04-build/evidence/current-upstream-release-20261024/r02-native-execution-packet-20261024/source-oracle.json'
    oracle_raw=oracle_path.read_bytes();_require(hashlib.sha256(oracle_raw).hexdigest()==selection.oracle_sha256)
    oracle=json.loads(oracle_raw);inputs=oracle['inputs'];source_metadata=scope.source.metadata(scope.context)
    _require(source_metadata['model_sha256']==inputs['modelSha256']
        and source_metadata['configuration_sha256']==inputs['configurationSha256']
        and source_metadata['source_sha256']==inputs['sourceJsonlSha256']
        and hashlib.sha256(scope.source.configuration.binding_source).hexdigest()==inputs['bindingSha256'])
    source_objects=scope.source_rows()['object_current']
    singleton_rows=[r for r in source_objects if r.get('type_id')=='17'and r.get('id')=='1']
    _require(len(singleton_rows)==1)
    singleton_expected=dict(singleton_rows[0])
    oracle_singletons=oracle['firstPublicationSourceDerivedExpectations']['objectSingletons']
    oracle_singleton=[item for item in oracle_singletons if item.get('typeId')==17 and item.get('entityId')==1]
    _require(len(oracle_singleton)==1 and singleton_expected.get('type_id')=='17'
        and singleton_expected.get('id')=='1'
        and json.loads(singleton_expected['props_json'])==oracle_singleton[0]['properties'])
    type_id=17;entity_id=1
    singleton_request=encoded({'profile':'ashlar-r02-source-bound-singleton-request/0.1',
        'publicationId':scope.descriptor.publication_id,'manifestUuid':scope.manifest_uuid,
        'typeId':type_id,'entityId':entity_id,'vector':dict(scope.vector.targets)}).encode()
    original_sql=('SELECT p.person_key, COUNT(*) AS paths, COUNT_DISTINCT_PATH_TARGETS(x) AS targets '
        'FROM Person p CROSS JOIN EXPAND_PATHS(p.knows, located) AS x GROUP BY p.person_key '
        'HAVING COUNT(*) > 0 ORDER BY p.person_key')
    positive_sql=original_sql.replace('COUNT(*) > 0','COUNT(*) > 1')
    def observe(*_):return None
    paths=query.compiler.paths
    backend03_original=compile_configured_count_star_053(scope,dict(aliases),original_sql,paths,observe=observe)
    backend03_positive=compile_configured_count_star_053(scope,dict(aliases),positive_sql,paths,observe=observe)
    _require(backend03_original['request']['sql']==original_sql and backend03_positive['request']['sql']==positive_sql)
    grouped_oracle=oracle['firstPublicationSourceDerivedExpectations']['backend03GroupedPaths']
    page_oracle=oracle['firstPublicationSourceDerivedExpectations']['backend02SdkPage']
    _require(grouped_oracle['having']=='COUNT(*) > 0'
        and grouped_oracle['positiveVariant']['having']=='COUNT(*) > 1')
    group_rows=grouped_oracle['exactExpectedResultRows']
    _require(group_rows==grouped_oracle['positiveVariant']['expectedResultRows'])
    page1_after=json.dumps(page_oracle['continuationAfter'],separators=(',',':'),ensure_ascii=False)
    page2_rows=page_oracle['continuationVisibleRows']
    gate_path=Path(__file__).resolve().parents[2]/'docs/helix/04-build/evidence/current-upstream-release-20261024/r02-native-execution-packet-20261024/r02_live_alias_gate.py'
    _require(hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==selection.runner_sha256
        and hashlib.sha256(gate_path.read_bytes()).hexdigest()==selection.gate_sha256)
    gate_spec=importlib.util.spec_from_file_location('_ashlar_r02_live_alias_gate',gate_path)
    _require(gate_spec is not None and gate_spec.loader is not None)
    gate_module=importlib.util.module_from_spec(gate_spec);gate_spec.loader.exec_module(gate_module)
    aliases_by_role={table.rsplit('.',1)[1]:aliases[table]for table in aliases}
    runtime=dict(selection.runtime)
    runtime.update({'continuationRunnerSha256':selection.runner_sha256,'liveAliasGateSha256':selection.gate_sha256,
        'priorPublicationSourceRevision':selection.prior_publication_source_revision})
    _require(runtime.get('weftIndexSha256')==query.compiler.paths.index_sha256)
    def live_check():
        scope.renew();aliases.renew();_require(query.compiler._verify())
        jars=tuple(map(str,runtime_guard()));spark=transport.spark
        _require(spark.version=='4.2.0'and spark.sparkContext.master=='local[1]'
            and spark.sparkContext._jvm.java.lang.System.getProperty('java.specification.version')=='21'
            and spark.conf.get('spark.sql.ansi.enabled')=='true'and spark.conf.get('spark.sql.session.timeZone')=='UTC'
            and spark.conf.get(PARAMETER_SUBSTITUTION_SETTING)=='true'
            and set(spark.conf.get('spark.jars').split(','))==set(jars))
        from .configured_local_runtime_profile import verify_loaded_delta_class
        verify_loaded_delta_class(spark,[Path(p)for p in jars])
        _require(time.monotonic()<deadline)
    deadline,gate_budget=_r02_review_deadline(runtime)
    review_gate=gate_module.R02LiveAliasReviewGate(selection.attempt_root,attempt_id=selection.attempt_id,
        stage='pre-sql',source_revision=selection.source_revision,spark_application_id=aliases.application_id,
        vector=vector,aliases=aliases_by_role,runtime=runtime,live_check=live_check,deadline_seconds=gate_budget)
    expected_grouped_rows=group_rows;expected_page1_after=page1_after;expected_page2_rows=page2_rows
    capture=query.capture;decoder=PathDecodeConfig(query.capture.maximum_cell_bytes)
    singleton_operation=lambda:scope.singleton(type_id,entity_id)
    session=ConfiguredSdk053LiveSession(scope,transport,query,aliases,review_gate=review_gate,
        runtime_guard=runtime_guard,expected_continuation_after=expected_page1_after)
    _require(session.expected_page1_rows==page_oracle['firstPageVisibleRows'])
    artifacts={
        'singleton':{'request':singleton_request,'expected':singleton_expected},
        'backend03-original':{'request':backend03_original['requestBytes'],
            'response':backend03_original['responseBytes'],'expected':{'rows':expected_grouped_rows}},
        'backend03-positive':{'request':backend03_positive['requestBytes'],
            'response':backend03_positive['responseBytes'],'expected':{'rows':expected_grouped_rows}},
        'backend02-page1':session.frozen_page1_artifact(expected_page1_after),
    }
    manifest_path,_=review_gate.freeze(artifacts)
    frozen_records={record['name']:record for record in json.loads(manifest_path.read_bytes())['artifacts']}
    page1_holder={}
    def stage_one(run):
        singleton=run('singleton',singleton_operation)
        _require(singleton==singleton_expected)
        for name,sql,compiled in (
            ('backend03-original',original_sql,backend03_original),
            ('backend03-positive',positive_sql,backend03_positive)):
            _require_frozen_grouped_artifact(compiled,frozen_records[name])
            value=run(name,lambda sql=sql,compiled=compiled:query_configured_projection_precompiled(
                scope,aliases,sql,compiled,capture=capture,decoder=decoder))
            _require_grouped_exact_rows(value['evidence'],expected_grouped_rows)
        page=run('backend02-page1',session.page)
        rows=[[item.person_key,item.person_name]for item in page.items]
        _require(rows==session.expected_page1_rows and page.next_after is not None
            and encoded(list(page.next_after.values))==expected_page1_after)
        page1_holder['page']=page
        return {'continuationAfter':expected_page1_after}
    review_gate.execute_after_review(review_gate.root/'pre-sql-review.json',stage_one)
    _,stage2_gate_budget=_r02_review_deadline(runtime,stage='post-page1')
    review_gate.next_stage_after_page1(deadline_seconds=stage2_gate_budget)
    page1=page1_holder['page']
    page2=session.page(after=page1.next_after)
    observed=[[item.person_key,item.person_name]for item in page2.items]
    _require(observed==expected_page2_rows and page2.next_after is None)
    return {'singleton':singleton_expected,'backend03':{'original':expected_grouped_rows,
        'positive':expected_grouped_rows},'backend02':{'page1':session.expected_page1_rows,
        'page2':observed,'continuationAfter':expected_page1_after},
        'vector':dict(scope.vector.targets),'aliases':dict(aliases),'scopeKind':'ashlar-delta/0.3'}


def _r02_review_deadline(runtime,*,stage='pre-sql',now=None):
    """Return the stage-specific review deadline under the worker close budget."""
    now=time.monotonic() if now is None else now
    deadline_key={'pre-sql':'preSqlReviewDeadlineMonotonic','post-page1':'postPage1ReviewDeadlineMonotonic'}.get(stage)
    _require(deadline_key is not None)
    review_deadline=runtime.get(deadline_key);process_deadline=runtime.get('wholeProcessDeadlineMonotonic')
    post_deadline=runtime.get('postPage1ReviewDeadlineMonotonic')
    _require(type(review_deadline)is float and type(process_deadline)is float and type(post_deadline)is float
        and type(now)is float and now<review_deadline<=process_deadline-40)
    if stage=='pre-sql':
        _require(review_deadline-now<=155 and post_deadline-review_deadline>=100
            and post_deadline<=process_deadline-80)
        stage_deadline=review_deadline
    else:
        _require(review_deadline<=process_deadline-80)
        stage_deadline=min(review_deadline,now+90)
    _require(now<stage_deadline)
    return stage_deadline,max(1,int(stage_deadline-now))
