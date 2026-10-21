# SPDX-License-Identifier: MIT OR Apache-2.0
"""Concrete isolated configured-source Delta runtime; no remote/protected ACK."""
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from collections.abc import Mapping
from types import MappingProxyType
import datetime,hashlib,json,os,re,stat,time
from ashlar.manifest import FIELDS
from ashlar.native import quote_table_identifier
from ashlar.protocol import ReaderProtocolProfile,validate_protocol_detail
from ashlar.retention import interval_microseconds
from .configured_source_admission import ConfiguredSourceAdmissionConfig
from .configured_source_local import open_configured_local_source
from .configured_local_publication import ConfiguredLocalPublicationDriver,ROLES
from .managed_carriers import load_managed_carriers
from .supply_chain_native import PrivateFiniteLease
from .configured_local_runtime_profile import attest as attest_runtime,PARAMETER_SUBSTITUTION_SETTING,verify_loaded_delta_class
from .delta_custody import LocalDeltaTransport,DeltaTarget,operation_capacity,encoded
from .driver import PrivatePolicy
from .evolution_cli import InvocationFile
from .evolution_producer import snapshot
from .lifecycle import finish,owned_context

PROFILE=ReaderProtocolProfile('ashlar-configured-local-delta/0.1',(1,2,3),(2,5,7),{'appendOnly','invariants','checkConstraints','changeDataFeed','generatedColumns','columnMapping','deletionVectors','timestampNtz','domainMetadata','rowTracking','clustering'})

def _require(value):
    if not value:raise ValueError('Configured local runtime refused')

def _path(path):
    _require(type(path)is type(Path())and path.is_absolute()and path.resolve()==path and not any(p.is_symlink()for p in (path,*path.parents))and not any(c in str(path)for c in ('`',"'",'\\','\0','\n','\r')))

@dataclass(frozen=True)
class ConfiguredLocalRuntimeConfig:
    installation:Path
    output:Path
    jars:Path
    java_home:Path
    admission:ConfiguredSourceAdmissionConfig
    stream:str
    materialization_clock_us:str
    operation_capacity:object
    installation_id:str
    installation_sha256:Optional[str]
    jar_inventory:Optional[Mapping]=None
    def __post_init__(self):
        for path in (self.installation,self.output,self.jars,self.java_home):_path(path)
        _require(type(self.admission)is ConfiguredSourceAdmissionConfig);self.admission.__post_init__()
        _require(self.admission.output==self.output/'admission')
        required_jars={'delta-spark_4.2_2.13-4.4.1.jar','delta-storage-4.4.1.jar'}
        inventory=self.jar_inventory
        _require(isinstance(inventory,Mapping)and set(inventory.keys())==required_jars)
        _require(all(type(name)is str and type(digest)is str and len(digest)==64 and
                     re.fullmatch('[0-9a-f]{64}',digest)is not None for name,digest in inventory.items()))
        # Copy every accepted Mapping, including MappingProxyType/custom mappings,
        # so a caller cannot mutate the runtime identity after validation.
        object.__setattr__(self,'jar_inventory',MappingProxyType(dict(inventory)))
        _require(self.output!=self.installation and self.output not in self.installation.parents and self.installation not in self.output.parents)
        _require(type(self.stream)is str and 0<len(self.stream.encode())<=1024 and '\0'not in self.stream)
        _require(type(self.installation_id)is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9:_-]{0,127}',self.installation_id)is not None)
        token=self.materialization_clock_us
        _require(type(token)is str and re.fullmatch('0|[1-9][0-9]{0,18}',token)is not None and int(token)<2**63)
        try:datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc)+datetime.timedelta(microseconds=int(token))
        except OverflowError:raise ValueError('Configured local clock outside finite UTC range')from None
        _require(self.installation_sha256 is None or type(self.installation_sha256)is str and re.fullmatch('[0-9a-f]{64}',self.installation_sha256)is not None)
        object.__setattr__(self,'operation_capacity',operation_capacity(self.operation_capacity))
        for path in (self.installation,self.output):
            for original in (self.admission.configuration,self.admission.producer.source,self.admission.producer.bun,self.admission.producer.git,self.jars,self.java_home):
                _require(path!=original and path not in original.parents and original not in path.parents)


def _write(path,raw):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600);primary=None
    try:_require(type(n:=os.write(fd,raw))is int and n==len(raw))
    except BaseException as exc:primary=exc
    finish(primary,[lambda:os.close(fd)])


def _runtime(config):
    config.__post_init__()
    _require(os.environ.get('JAVA_HOME')==str(config.java_home)and(config.java_home/'bin/java').is_file())
    _require(not any(key in os.environ for key in ('SPARK_REMOTE','MASTER','SPARK_CONNECT_MODE_ENABLED','PYSPARK_SUBMIT_ARGS','PYSPARK_GATEWAY_PORT','PYSPARK_GATEWAY_SECRET')))
    _,_,jars=attest_runtime(config)
    return jars

def open_finite_spark(config,jars):
    """Open current configured-local Spark without historical commerce pins."""
    selected=_runtime(config);_require(type(jars)in (list,tuple)and list(jars)==selected)
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    _require(SparkSession.getActiveSession()is None and getattr(SparkSession,'_instantiatedSession',None)is None)
    _require(getattr(SparkContext,'_active_spark_context',None)is None)
    return (SparkSession.builder.master('local[1]').appName('Ashlar configured-local Delta publication')
        .config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
        .config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.session.timeZone','UTC')
        .config(PARAMETER_SUBSTITUTION_SETTING,'true')
        .config('spark.sql.ansi.enabled','true').config('spark.sql.decimalOperations.allowPrecisionLoss','false')
        .config('spark.jars',','.join(str(p)for p in selected)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.sql.warehouse.dir',str(config.output/'warehouse')).getOrCreate())


def _fields():
    carriers=load_managed_carriers()
    return {**{r:carriers.ordered_fields(r)for r in ROLES},'attempts':tuple((n,'STRING',False)for n in ('stream','batch_id','phase','request_digest','payload_json','payload_digest')),'manifest':tuple((n,'TIMESTAMP'if n=='recorded_at'else 'STRING',False)for n in FIELDS)}

def _columns():
    carriers=load_managed_carriers()
    return {**{r:carriers.ordered_columns(r)for r in ROLES},'attempts':tuple((n,'STRING')for n in ('stream','batch_id','phase','request_digest','payload_json','payload_digest')),'manifest':tuple((n,'TIMESTAMP'if n=='recorded_at'else 'STRING')for n in FIELDS)}


def _scope(config,source):
    return {'installation_id':config.installation_id,'stream':config.stream,'materialization_clock_us':config.materialization_clock_us,
            'configuration_sha256':hashlib.sha256(source.configuration_file.raw).hexdigest(),'source_sha256':hashlib.sha256(source.original_source).hexdigest(),
            'model_sha256':source.configuration.model_sha256,'operation_capacity':None if config.operation_capacity is None else dict(config.operation_capacity)}


def _native(spark,target,columns):
    rows=spark.sql('DESCRIBE DETAIL delta.`'+str(target.path)+'`').limit(2).collect();_require(len(rows)==1)
    detail=rows[0].asDict(recursive=True)
    _require(detail['id']==target.uuid and detail['location'].removeprefix('file:').rstrip('/')==str(target.path)and detail['format']=='delta')
    # Native protocol inventory is admitted before any bag/row parity checks.
    validate_protocol_detail({'id':detail['id'],'format':detail['format'],'minReaderVersion':str(detail['minReaderVersion']),'minWriterVersion':str(detail['minWriterVersion']),'tableFeatures':json.dumps(detail.get('tableFeatures',[]))},uuid=target.uuid,profile=PROFILE)
    schema=spark.read.format('delta').load(str(target.path)).schema
    types={'string':'STRING','long':'BIGINT','timestamp':'TIMESTAMP'}
    _require(tuple((f.name,types.get(f.dataType.typeName()))for f in schema.fields)==columns)
    role=target.table.rsplit('.',1)[1];_require(tuple((f.name,types.get(f.dataType.typeName()),f.nullable)for f in schema.fields)==_fields()[role])
    props=detail.get('properties');_require(type(props)is dict)
    retention=min(interval_microseconds(props.get('delta.deletedFileRetentionDuration','interval 7 days')),interval_microseconds(props.get('delta.logRetentionDuration','interval 30 days')))
    return retention


class ConfiguredLocalAdmission:
    """Actual held local lease/registry/session/native admission, not bag parity."""
    def __init__(self,config,lease,spark,source,targets,columns,registry_file):
        self.config=config;self.lease=lease;self.spark=spark;self.source=source;self.targets=targets;self.columns=columns;self.registry_file=registry_file
        self.deadline=time.monotonic()+600;self._memo={};self._poisoned=False
        self.context=source.context;self.original_registry=encoded([{'table':t.table,'path':str(t.path),'uuid':t.uuid}for t in targets]);self.session=spark.sparkContext.applicationId
    def __call__(self,source,request,manifest,context):return self._admit(source,request,manifest,context,force=False)
    def closing(self,source,manifest,context):return self._admit(source,None,manifest,context,force=True)
    def _admit(self,source,request,manifest,context,*,force):
        _require(source is self.source and context is self.context and time.monotonic()<self.deadline)
        _require(force or not self._poisoned)
        finish(None,[self.lease.renew,lambda:source.renew(context),lambda:self.registry_file.renew(65536)])
        _require(self.spark.sparkContext.master=='local[1]'and self.spark.sparkContext.applicationId==self.session and self.spark.conf.get('spark.sql.session.timeZone')=='UTC'
            and self.spark.conf.get(PARAMETER_SUBSTITUTION_SETTING)=='true')
        _,identity,jars=attest_runtime(self.config)
        _require(self.spark.version=='4.2.0' and self.spark.sparkContext._jvm.java.lang.System.getProperty('java.specification.version')=='21')
        _require(self.spark.conf.get('spark.jars')==','.join(str(p)for p in jars))
        verify_loaded_delta_class(self.spark,jars)
        _require(encoded([{'table':t.table,'path':str(t.path),'uuid':t.uuid}for t in self.targets])==self.original_registry)
        if request is not None:source.admit_request(request,context)
        versions={}if manifest is None else json.loads(manifest['table_versions_json'])
        if manifest is not None:_require(set(versions)=={t.table for t in self.targets if t.table.rsplit('.',1)[1]in ROLES}and json.loads(manifest['schema_revisions_json'])=={source.configuration.schema_alias:source.configuration.document_revision})
        now=time.time_ns()//1000;key=encoded(versions)
        # Cooperative local profile: the continuously held private installation
        # lease excludes participating writers. This runtime never changes native
        # schema/protocol/retention. No remote-admin fencing is claimed. Reuse is
        # limited to this owner and exact version vector, never another invocation.
        if force or key not in self._memo:
            deadlines=[]
            def native(target):
                role=target.table.rsplit('.',1)[1];retention=_native(self.spark,target,self.columns[role])
                if target.table in versions:
                    rows=self.spark.sql('DESCRIBE HISTORY delta.`'+str(target.path)+'`').limit(1001).collect();_require(0<len(rows)<=1000)
                    matching=[r for r in rows if r['version']==versions[target.table]];_require(len(matching)==1)
                    stamp=matching[0]['timestamp'];_require(type(stamp)is datetime.datetime)
                    # Spark's selected session is explicitly UTC. This local
                    # convention does not infer a remote timestamp's timezone.
                    if stamp.tzinfo is None:stamp=stamp.replace(tzinfo=datetime.timezone.utc)
                    _require(stamp.utcoffset()==datetime.timedelta(0))
                    delta=stamp-datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc);micros=(delta.days*86400+delta.seconds)*1000000+delta.microseconds
                    _require(micros<=now<micros+retention-600000000);deadlines.append(micros+retention-600000000)
            try:finish(None,[lambda t=t:native(t)for t in self.targets])
            except BaseException:
                self._poisoned=True
                raise
            self._memo[key]=(now,min(deadlines)if deadlines else now+600000000)
        observed,expiry=self._memo[key];_require(observed<=now<expiry)
        finish(None,[self.lease.renew,lambda:source.renew(context),lambda:self.registry_file.renew(65536)])


def _run(config,ordinal,*,selector=None,query=None,export=False):
    _require(type(config)is ConfiguredLocalRuntimeConfig);config.__post_init__();jars=_runtime(config)
    initialize=ordinal is None
    _require(type(export)is bool and (not export or not initialize and selector is None and query is None))
    if query is not None:
        from .configured_local_query import ConfiguredLocalQueryConfig
        from .configured_sdk_pages import ConfiguredSdkPagesConfig
        _require(type(query)in (ConfiguredLocalQueryConfig,ConfiguredSdkPagesConfig));query.__post_init__()
    _require(query is None or selector is None and not initialize)
    _require(selector is None or type(selector)is tuple and len(selector)==2 and all(type(v)is int and -(2**63)<=v<2**63 for v in selector))
    _require(not initialize or selector is None)
    _require((initialize and config.installation_sha256 is None)or(not initialize and config.installation_sha256 is not None and type(ordinal)is int and 1<=ordinal<=4))
    parent=config.output.parent.lstat();_require(stat.S_ISDIR(parent.st_mode)and parent.st_uid==os.getuid()and not parent.st_mode&0o022)
    original_registry=None
    if not initialize:
        original_registry=InvocationFile.read(config.installation/'installation.json',65536)
        _require(hashlib.sha256(original_registry.raw).hexdigest()==config.installation_sha256)
    config.output.mkdir(mode=0o700)
    lease=None;spark=None;transport=None;source=None;primary=None;result=None;source_manager=None;admission=None;final_manifest=None;export_capture=None
    try:
        if initialize:config.installation.mkdir(mode=0o700)
        lease=PrivateFiniteLease(config.installation,create=initialize)
        context=object();source_manager=open_configured_local_source(config.admission,stream=config.stream,context=context);source=source_manager.__enter__()
        _require(not export or source.graph)
        if query is not None:_require(type(query)is not ConfiguredSdkPagesConfig or source.graph)
        _require(selector is None or selector[0]in source.type_ids)
        scope=_scope(config,source);columns=_columns();tables={r:'local.configured.'+r for r in columns}
        if not initialize:
            registry=json.loads(original_registry.raw);runtime_profile,runtime_identity,_=attest_runtime(config)
            _require(set(registry)=={'profile','scope','targets','columns','runtime_identity'}and registry['profile']=='ashlar-configured-local-installation/0.2'and registry['scope']==scope and registry['columns']=={r:[list(c)for c in cols]for r,cols in columns.items()}and registry['runtime_identity']=={'profile':runtime_profile,**runtime_identity})
        spark=open_finite_spark(config,jars)
        _require(spark.sparkContext.master=='local[1]'and spark.conf.get('spark.sql.session.timeZone')=='UTC'and spark.conf.get('spark.driver.memory')=='512m'
            and spark.conf.get(PARAMETER_SUBSTITUTION_SETTING)=='true')
        _require(spark.version=='4.2.0' and spark.sparkContext._jvm.java.lang.System.getProperty('java.specification.version')=='21')
        _require(spark.conf.get('spark.jars')==','.join(str(p)for p in jars))
        verify_loaded_delta_class(spark,jars)
        spark.sparkContext.setLogLevel('ERROR');targets=[]
        if initialize:
            for role,schema in columns.items():
                path=config.installation/role
                spark.sql('CREATE TABLE delta.`'+str(path)+'` ('+','.join(n+' '+f+(''if nullable else ' NOT NULL')for n,f,nullable in _fields()[role])+") USING DELTA TBLPROPERTIES ('delta.deletedFileRetentionDuration'='interval 7 days','delta.logRetentionDuration'='interval 30 days')")
                rows=spark.sql('DESCRIBE DETAIL delta.`'+str(path)+'`').limit(2).collect();_require(len(rows)==1)
                targets.append(DeltaTarget(tables[role],path,rows[0]['id']))
            runtime_profile,runtime_identity,_=attest_runtime(config)
            registry={'profile':'ashlar-configured-local-installation/0.2','runtime_identity':{'profile':runtime_profile,**runtime_identity},'scope':scope,'targets':[{'table':t.table,'path':str(t.path),'uuid':t.uuid}for t in targets],'columns':{r:[list(c)for c in cols]for r,cols in columns.items()}}
            _write(config.installation/'installation.json',(encoded(registry)+'\n').encode());original_registry=InvocationFile.read(config.installation/'installation.json',65536)
        else:
            _require(type(registry['targets'])is list and len(registry['targets'])==6)
            for item in registry['targets']:
                _require(set(item)=={'table','path','uuid'}and item['table']in tables.values())
                role=item['table'].rsplit('.',1)[1];_require(item['path']==str(config.installation/role));targets.append(DeltaTarget(item['table'],Path(item['path']),item['uuid']))
            _require(len({t.table for t in targets})==6)
        targets=tuple(targets);policy=PrivatePolicy(context,targets)
        admission=ConfiguredLocalAdmission(config,lease,spark,source,targets,columns,original_registry);admission(source,None,None,context)
        transport=(LocalDeltaTransport.initialize(spark,config.installation/'operations.sqlite',config.installation_id,targets,policy,context=context,capacity=config.operation_capacity)if initialize else LocalDeltaTransport(spark,config.installation/'operations.sqlite',config.installation_id,targets,policy,capacity=config.operation_capacity));policy.initializing=False
        if initialize:result={'profile':'ashlar-configured-local-initialized/0.2','installation_id':config.installation_id,'operation':'initialize','installation_sha256':hashlib.sha256(original_registry.raw).hexdigest(),'installation':registry,'published':False,'protected_ack':False}
        elif selector is not None or query is not None or export:
            from .file_progress import JournaledJsonlProgress
            from .configured_local_read import read_configured_local_singleton
            progress=JournaledJsonlProgress.open_existing(transport,source.configuration.source,source.metadata(context)['source_sha256'],stream=config.stream,feed=source.configuration.feed,epoch=source.configuration.epoch)
            entries=progress.retained_entries();_require(1<=ordinal<=len(entries));final_manifest=dict(entries[ordinal-1].descriptor.raw)
            if export:
                from .configured_local_read import open_configured_local_read
                from .configured_local_graph_release import read_configured_local_graph_release
                with owned_context(open_configured_local_read(transport,source,progress,ordinal=ordinal,tables=tables,context=context,materialization_clock_us=config.materialization_clock_us,current_admission=admission))as held:
                    release=read_configured_local_graph_release(held)
                    export_capture={'source':source.metadata(context),'binding_sha256':hashlib.sha256(source.configuration.binding_source).hexdigest(),
                        'ordinal':ordinal,'request':source.spec(ordinal,context).request()}
                result={'profile':'ashlar-configured-local-export-result/0.1','installation_id':config.installation_id,'operation':'export',
                    'manifest':final_manifest,'ordinal':ordinal,'local_checkpoint':progress.position(),'completed_batches':progress.completed_batches(),'protected_ack':False}
            elif query is None:
                row=read_configured_local_singleton(transport,source,progress,ordinal=ordinal,type_id=selector[0],entity_id=selector[1],tables=tables,context=context,materialization_clock_us=config.materialization_clock_us,current_admission=admission)
                result={'profile':'ashlar-configured-local-read-result/0.1','installation_id':config.installation_id,'operation':'read','manifest':final_manifest,'ordinal':ordinal,'local_checkpoint':progress.position(),'completed_batches':progress.completed_batches(),'selector':{'type_id':selector[0],'entity_id':selector[1]},'row':row,'protected_ack':False}
            else:
                from .configured_local_read import open_configured_local_read
                from .configured_local_query import run_configured_local_query
                with owned_context(open_configured_local_read(transport,source,progress,ordinal=ordinal,tables=tables,context=context,materialization_clock_us=config.materialization_clock_us,current_admission=admission))as held:
                    from .configured_sdk_pages import ConfiguredSdkPagesConfig,run_configured_sdk_pages
                    if type(query)is ConfiguredSdkPagesConfig:
                        answer=run_configured_sdk_pages(held,transport,query,runtime_guard=lambda:_runtime(config))
                    else:answer=run_configured_local_query(held,transport,query)
                result={'profile':'ashlar-configured-local-query-result/0.1','installation_id':config.installation_id,'operation':'query','manifest':final_manifest,'ordinal':ordinal,'local_checkpoint':progress.position(),'completed_batches':progress.completed_batches(),'query':answer,'protected_ack':False}
        else:
            driver=ConfiguredLocalPublicationDriver(transport,policy,source,tables=tables,context=context,materialization_clock_us=config.materialization_clock_us,current_admission=admission)
            driver.select_original(ordinal);descriptor=driver.publish_selected();final_manifest=dict(descriptor.raw);admission(source,driver.request,final_manifest,context)
            counts={}
            for role in ROLES:
                table=tables[role];rows=transport.query('SELECT cast(count(*) AS STRING) AS rows FROM '+quote_table_identifier(table)+' VERSION AS OF '+str(descriptor.versions[table]),{}).rows
                _require(len(rows)==1 and set(rows[0])=={'rows'}and type(rows[0]['rows'])is str and re.fullmatch('0|[1-9][0-9]{0,3}',rows[0]['rows'])is not None and int(rows[0]['rows'])<=1000);counts[role]=int(rows[0]['rows'])
            result={'profile':'ashlar-configured-local-publication-result/0.1','installation_id':config.installation_id,'operation':'replay'if driver.replay else 'publish','manifest':dict(descriptor.raw),'ordinal':ordinal,'local_checkpoint':driver.progress.position(),'completed_batches':driver.progress.completed_batches(),'replayed':driver.replay,'native_counts':counts,'protected_ack':False}
    except BaseException as exc:primary=exc
    callbacks=[]
    if source is not None:callbacks.append(lambda:source.renew(source.context))
    if original_registry is not None:callbacks.append(lambda:original_registry.renew(65536))
    if admission is not None:callbacks.append(lambda:admission.closing(source,final_manifest,source.context))
    if transport is not None:callbacks.append(transport.close)
    if spark is not None:callbacks.append(spark.stop)
    if source is not None:callbacks.append(lambda:source_manager.__exit__(None,None,None))
    callbacks.append(lambda:_runtime(config))
    if lease is not None:callbacks.extend([lease.renew,lease.close])
    finish(primary,callbacks)
    if export:
        from .configured_graph_custody import build_configured_graph_custody
        custody=build_configured_graph_custody(release,**export_capture)
        # Complete read, native/source, Spark and installation owners have closed.
        for name,payload in (('release.graph.json',release.payload),('custody.json',custody)):
            _write(config.output/name,payload)
        graph=json.loads(release.payload)
        result.update(release=str(config.output/'release.graph.json'),release_sha256=release.sha256,
            custody=str(config.output/'custody.json'),custody_sha256=hashlib.sha256(custody).hexdigest(),
            nodes=len(graph['nodes']),edges=len(graph['edges']),
            scope='Immutable configured JSONL export only; no protected ACK, current native availability or engine grant.')
    raw=(encoded(result)+'\n').encode();_require(len(raw)<=1048576);_write(config.output/'runtime-report.json',raw)
    return result


def initialize_configured_local(config):return _run(config,None)
def publish_configured_local(config,*,ordinal):return _run(config,ordinal)

def export_configured_local(config,*,ordinal):return _run(config,ordinal,export=True)

def read_configured_local(config,*,ordinal,type_id,entity_id):
    return _run(config,ordinal,selector=(type_id,entity_id))


def query_configured_local(config,*,ordinal,query):
    from .configured_local_query import ConfiguredLocalQueryConfig
    _require(type(query)is ConfiguredLocalQueryConfig);query.__post_init__()
    return _run(config,ordinal,query=query)


def page_configured_local(config,*,ordinal,query):
    """Typed Python page workflow; operator owns the selected optional SDK runtime."""
    from .configured_sdk_pages import ConfiguredSdkPagesConfig
    _require(type(query)is ConfiguredSdkPagesConfig);query.__post_init__()
    return _run(config,ordinal,query=query)
