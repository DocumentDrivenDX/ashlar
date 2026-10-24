# SPDX-License-Identifier: MIT OR Apache-2.0
"""Original configured scalar and graph queries through held indexed Weft execution.

Results remain provisional until the enclosing alias, publication, source and
native runtime owners have closed. This module starts no services and writes no
source, publication, progress or ACK state.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib,json
from types import SimpleNamespace
from ashlar.publication import resolve_publication
from ashlar.weft_binding import LAYOUT_SHA256
from .configured_local_read import ConfiguredLocalRead
from .configured_query_aliases import ConfiguredQueryAliases
from .configured_weft_binding import configured_count_star_request
from .configured_weft_compile import ConfiguredWeftCompileConfig,compile_configured_count_star
from .count_star_admission import CountStarAdmissionConfig
from .count_star_execution import CountStarExecutionConfig,execute_commerce_count_star
from .delta_custody import encoded
from .lifecycle import finish
from .count_star_admission import CURRENT_BACKEND
from .configured_local_runtime_profile import JAR_SHA256,PARAMETER_SUBSTITUTION_SETTING
from pathlib import Path


def _require(value):
    if not value:raise ValueError('Configured held Weft execution refused')


@dataclass(frozen=True)
class _ConfiguredQueryDriver:
    transport:object


class ConfiguredWeftProvider:
    """A reader over the exact original scope and its actual external aliases."""
    def __setattr__(self,name,value):
        if getattr(self,'_sealed',False):raise AttributeError('Configured Weft provider is immutable')
        object.__setattr__(self,name,value)
    def __init__(self,scope,aliases,request):
        _require(type(scope)is ConfiguredLocalRead and type(aliases)is ConfiguredQueryAliases and aliases.scope is scope)
        scope.renew();aliases.renew()
        self.scope,self.aliases,self.context=scope,aliases,scope.context
        self.driver=_ConfiguredQueryDriver(scope.transport)
        self.request_json=encoded(request);self.active=False;self.closed=None
        self.current_profile=all(request.get('target',{}).get(k)==CURRENT_BACKEND[k]for k in ('backendId','backendVersion','targetProfile'))
        self.session=scope.transport.spark.sparkContext.applicationId
        self._sealed=True

    def runtime(self,context):
        _require(context is self.context and self.active and self.driver.transport is self.scope.transport
                 and self.aliases.scope is self.scope and self.context is self.scope.context)
        self.scope.renew();self.aliases.renew()
        spark=self.scope.transport.spark
        expected='4.2.0'if self.current_profile else '4.0.1'
        _require(spark.version==expected and spark.sparkContext.master=='local[1]'and spark.sparkContext.applicationId==self.session
                 and spark.conf.get('spark.sql.session.timeZone')=='UTC'
                 and spark.conf.get('spark.sql.ansi.enabled')=='true'
                 and spark.conf.get('spark.sql.decimalOperations.allowPrecisionLoss')=='false'
                 and (not self.current_profile or spark.conf.get(PARAMETER_SUBSTITUTION_SETTING)=='true'))
        if self.current_profile:
            jars=[Path(p) for p in spark.conf.get('spark.jars').split(',')]
            _require({p.name:hashlib.sha256(p.read_bytes()).hexdigest()for p in jars}==JAR_SHA256)
            from .configured_local_runtime_profile import verify_loaded_delta_class
            verify_loaded_delta_class(spark,jars)

    def resolve(self,context):
        self.runtime(context);scope=self.scope
        resolved=resolve_publication(scope.backend,scope.descriptor.publication_id,
            {t:p[0]for t,p in scope.vector.targets.items()},context=context,
            supported_profiles=['ashlar-delta/0.3'],
            supported_revisions={scope.source.configuration.schema_alias:[scope.source.configuration.document_revision]})
        scope.bind_descriptor(resolved.descriptor,scope.vector,context)
        _require(resolved.descriptor==scope.descriptor and
                 {t:(s.uuid,s.version)for t,s in resolved.snapshots.items()}==dict(scope.vector.targets))
        return resolved

    def admit_binding(self,binding,resolved,context):
        self.runtime(context)
        request=json.loads(self.request_json)
        current=configured_count_star_request(self.scope,dict(self.aliases),request['sql'],layout_sha256=LAYOUT_SHA256)
        _require(encoded(current)==self.request_json and encoded(binding)==encoded(json.loads(current['target']['bindingJson'])))
        expected={'id':resolved.descriptor.publication_id,'manifestUuid':self.scope.manifest_uuid,
                  'tables':[{'name':self.aliases[t].split('.'),'uuid':s.uuid,'version':s.version}for t,s in sorted(resolved.snapshots.items())]}
        _require(binding['publication']==expected)

    def native_table_schema(self,table,resolved,context):
        self.runtime(context)
        return self.aliases.native_table_schema(table,resolved,context)

    @contextmanager
    def interval(self,context):
        _require(context is self.context and not self.active)
        object.__setattr__(self,'closed',None);object.__setattr__(self,'active',True);primary=None;opening=None
        try:
            self.runtime(context)
            _require(self.scope.transport.spark.sql("SELECT ('a' COLLATE UTF8_BINARY) = ('A' COLLATE UTF8_BINARY) AS equal").first()['equal']is False)
            opening=self.resolve(context)
            yield
        except BaseException as error:primary=error
        def closing_resolve():
            _require(opening is not None and self.resolve(context)==opening)
        def deactivate():object.__setattr__(self,'active',False)
        finish(primary,[self.scope.parity,self.aliases.renew,closing_resolve,deactivate])
        receipt={'profile':'ashlar-configured-local-weft-interval/0.1',
                     'publication_id':opening.descriptor.publication_id,
                     'manifest_sha256':hashlib.sha256(encoded(dict(opening.descriptor.raw)).encode()).hexdigest(),
                     'source':self.scope.source.metadata(self.context),
                     'qualification':'Provisional local immutable-file interval; enclosing alias/publication/native cleanup remains required. No protected ACK.'}
        object.__setattr__(self,'closed',receipt)

    def closed_interval_custody(self,context):
        _require(context is self.context and not self.active and self.closed is not None)
        self.scope.renew();self.aliases.renew()
        return json.loads(encoded(self.closed))


def query_configured_count_star(scope,aliases,sql,compile_config,*,capture,decoder,observe,original_oracle):
    """Compile twice and execute unchanged statements under actual held owners.

    The independent oracle receives the whole original model and JSONL bytes,
    plus the original request. It must derive expected native result cells from
    that source, never from the generated SQL or observed native result.
    """
    return _query(scope,aliases,sql,compile_config,capture=capture,decoder=decoder,observe=observe,original_oracle=original_oracle,projection=False)


def query_configured_projection(scope,aliases,sql,compile_config,*,capture,decoder,observe):
    """Execute separately admitted original scalar or typed graph query shapes."""
    return _query(scope,aliases,sql,compile_config,capture=capture,decoder=decoder,observe=observe,original_oracle=None,projection=True)


def query_configured_projection_precompiled(scope,aliases,sql,compiled,*,capture,decoder):
    """Execute an already frozen, separately admitted artifact under live aliases.

    This seam lets a packet compile and hash-bind all requests in one alias
    context, pause for independent review, then execute those exact artifacts.
    It does not compile, alter, or repair compiler SQL.
    """
    _require(type(compiled)is dict and compiled.get('compilerVersion')=='0.4.53'
        and type(compiled.get('admission_config'))is CountStarAdmissionConfig
        and compiled['admission_config'].profile=='count-star-current-053'
        and all(type(compiled.get(k))is dict for k in ('request','response','recompiled'))
        and all(type(compiled.get(k))is bytes and 0<len(compiled[k])<=1048576
            for k in ('requestBytes','responseBytes','recompiledBytes')))
    compiled['admission_config'].__post_init__()
    try:
        _require(compiled['responseBytes']==compiled['recompiledBytes']
            and json.loads(compiled['responseBytes'])==compiled['response']
            and json.loads(compiled['recompiledBytes'])==compiled['recompiled'])
    except Exception:
        raise ValueError('Configured held Weft execution refused') from None
    return _query(scope,aliases,sql,None,capture=capture,decoder=decoder,observe=lambda *_:None,
        original_oracle=None,projection=True,precompiled=compiled)


def _query(scope,aliases,sql,compile_config,*,capture,decoder,observe,original_oracle,projection,precompiled=None):
    _require(type(scope)is ConfiguredLocalRead and type(aliases)is ConfiguredQueryAliases
             and aliases.scope is scope and (type(compile_config)is ConfiguredWeftCompileConfig if precompiled is None else compile_config is None)
             and callable(observe)and(callable(original_oracle)if not projection else original_oracle is None))
    scope.renew();aliases.renew()
    if precompiled is None:
        compiled=compile_configured_count_star(scope,dict(aliases),sql,compile_config,observe=observe)
    else:
        expected=configured_count_star_request(scope,dict(aliases),sql,layout_sha256=LAYOUT_SHA256)
        _require(encoded(precompiled['request'])==encoded(expected)
            and precompiled['requestBytes']==(json.dumps(expected,ensure_ascii=False,separators=(',',':')).encode()+b'\n')
            and precompiled['responseBytes']==precompiled['recompiledBytes']
            and json.loads(precompiled['responseBytes'])==precompiled['response']
            and json.loads(precompiled['recompiledBytes'])==precompiled['recompiled'])
        compiled=precompiled
    if projection:
        if scope.source.graph:
            from .configured_typed_graph_weft_projection import configured_typed_graph_projection_oracle
            original_oracle=configured_typed_graph_projection_oracle(scope,compiled['request'],compiled['response'])
        elif scope.source.typed:
            from .configured_typed_weft_projection import configured_typed_projection_oracle
            original_oracle=configured_typed_projection_oracle(scope,compiled['request'],compiled['response'])
        else:
            from .configured_weft_projection import configured_projection_oracle
            original_oracle=configured_projection_oracle(scope,compiled['request'],compiled['response'])
    provider=ConfiguredWeftProvider(scope,aliases,compiled['request'])
    source=scope.source;model=source.configuration.model_source;graph=source.original_source
    def native_files():
        scope.renew();aliases.renew()
        return {'model':hashlib.sha256(model).hexdigest(),'source':hashlib.sha256(graph).hexdigest(),
                'admission':hashlib.sha256(encoded(source.metadata(scope.context)).encode()).hexdigest()}
    opened=SimpleNamespace(provider=provider,context=scope.context,model=model,graph=graph,
                           native_files=native_files,original_native_files=native_files())
    config=CountStarExecutionConfig(compiled['admission_config'],capture,decoder,None,None)
    result=None;primary=None
    try:
        result=execute_commerce_count_star(opened,compiled['request'],compiled['response'],compiled['recompiled'],
                                          config=config,original_oracle=original_oracle)
    except BaseException as error:primary=error
    finish(primary,[scope.renew,aliases.renew])
    return {'requestBytes':compiled['requestBytes'],'responseBytes':compiled['responseBytes'],
            'recompiledBytes':compiled['recompiledBytes'],'evidence':result}
