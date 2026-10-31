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
CONFIGURED_LOCAL_CARRIERS=('ashlar-delta/0.3','ashlar-delta/0.4-xxhash64')

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
    carrier_profile:str='ashlar-delta/0.3'
    sdk058_admission:object=None
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
        _require(type(self.carrier_profile)is str and self.carrier_profile in CONFIGURED_LOCAL_CARRIERS)
        _require(self.sdk058_admission is None or self.carrier_profile=='ashlar-delta/0.4-xxhash64')
        if self.sdk058_admission is not None:
            from .typed_graph_sdk058_admission import TypedGraphSdk058AdmissionConfig
            _require(type(self.sdk058_admission)is TypedGraphSdk058AdmissionConfig)
            self.sdk058_admission.__post_init__()
        token=self.materialization_clock_us
        _require(type(token)is str and re.fullmatch('0|[1-9][0-9]{0,18}',token)is not None and int(token)<2**63)
        try:datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc)+datetime.timedelta(microseconds=int(token))
        except OverflowError:raise ValueError('Configured local clock outside finite UTC range')from None
        _require(self.installation_sha256 is None or type(self.installation_sha256)is str and re.fullmatch('[0-9a-f]{64}',self.installation_sha256)is not None)
        object.__setattr__(self,'operation_capacity',operation_capacity(self.operation_capacity))
        protected_paths=[self.admission.configuration,self.admission.producer.source,
            self.admission.producer.bun,self.admission.producer.git,self.jars,self.java_home]
        if self.sdk058_admission is not None:
            protected_paths.extend((self.sdk058_admission.node,self.sdk058_admission.sdk_package_root,
                self.sdk058_admission.sdk_archive,self.sdk058_admission.producer_closure,
                self.sdk058_admission.producer_receipt,self.sdk058_admission.install_custody,
                self.sdk058_admission.source_admission_manifest))
        for path in (self.installation,self.output):
            for original in protected_paths:
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

def open_configured_local_spark(config,jars,warehouse,*,app_name='Ashlar configured-local Delta publication',driver_memory='512m'):
    """Open the attested configured-local Spark session with an explicit warehouse."""
    selected=_runtime(config);_require(type(jars)in (list,tuple)and list(jars)==selected)
    _require(isinstance(warehouse,Path) and warehouse.is_absolute() and warehouse.resolve()==warehouse)
    _require(type(app_name)is str and 0<len(app_name)<=128 and driver_memory in ('512m','2g'))
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    _require(SparkSession.getActiveSession()is None and getattr(SparkSession,'_instantiatedSession',None)is None)
    _require(getattr(SparkContext,'_active_spark_context',None)is None)
    return (SparkSession.builder.master('local[1]').appName(app_name)
        .config('spark.driver.memory',driver_memory).config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
        .config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.session.timeZone','UTC')
        .config(PARAMETER_SUBSTITUTION_SETTING,'true')
        .config('spark.sql.ansi.enabled','true').config('spark.sql.decimalOperations.allowPrecisionLoss','false')
        .config('spark.jars',','.join(str(p)for p in selected)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.sql.warehouse.dir',str(warehouse)).getOrCreate())


def open_finite_spark(config,jars,*,r02=False):
    """Open the bounded local runtime; only the private R02 continuation gets 2 GiB."""
    _require(type(r02)is bool)
    return open_configured_local_spark(config,jars,config.output/'warehouse',
        app_name='Ashlar R02 Weft 0.53 continuation' if r02 else 'Ashlar configured-local Delta publication',
        driver_memory='2g' if r02 else '512m')


def verify_driver_memory(spark,*,r02):
    """Check Spark's selected heap setting and the live R02 JVM heap size."""
    expected='2g' if r02 else '512m'
    _require(spark.conf.get('spark.driver.memory')==expected)
    if r02:
        actual=int(spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory())
        _require(1792*1024*1024<=actual<=2304*1024*1024)


def _fields(carrier_profile='ashlar-delta/0.3'):
    if carrier_profile=='ashlar-delta/0.4-xxhash64':
        from .managed_current_carriers import load_current_managed_carriers
        from .schema_rows import LOCAL_CURRENT_ROLE_TO_UMF
        carriers=load_current_managed_carriers(carrier_profile)
        return {r:carriers.ordered_fields(umf) for r,umf in LOCAL_CURRENT_ROLE_TO_UMF.items()}
    carriers=load_managed_carriers()
    return {**{r:carriers.ordered_fields(r)for r in ROLES},'attempts':tuple((n,'STRING',False)for n in ('stream','batch_id','phase','request_digest','payload_json','payload_digest')),'manifest':tuple((n,'TIMESTAMP'if n=='recorded_at'else 'STRING',False)for n in FIELDS)}

def _columns(carrier_profile='ashlar-delta/0.3'):
    if carrier_profile=='ashlar-delta/0.4-xxhash64':
        from .managed_current_carriers import load_current_managed_carriers
        from .schema_rows import LOCAL_CURRENT_ROLE_TO_UMF
        carriers=load_current_managed_carriers(carrier_profile)
        return {r:tuple((name,kind) for name,kind,_nullable in carriers.ordered_fields(umf))
                for r,umf in LOCAL_CURRENT_ROLE_TO_UMF.items()}
    carriers=load_managed_carriers()
    return {**{r:carriers.ordered_columns(r)for r in ROLES},'attempts':tuple((n,'STRING')for n in ('stream','batch_id','phase','request_digest','payload_json','payload_digest')),'manifest':tuple((n,'TIMESTAMP'if n=='recorded_at'else 'STRING')for n in FIELDS)}


def _create_configured_xxhash64_tables(spark,installation,tables,creator):
    """Call the pinned UMF six-role realization with this installation's aliases."""
    _require(isinstance(installation,Path) and installation.is_absolute() and installation.resolve()==installation)
    created,profile,_schemas=creator(spark,installation,tables,table_prefix='local.configured')
    base_profile_keys={'profile','physical_profile','lookup_hash_profile','generated_carriers_sha256','umf_revision',
        'local_realization','umf_realization_profile','umf_candidate_commit','umf_realization_manifest_sha256',
        'umf_bridge_sha256','umf_build_inputs_sha256','writer_settings','roles'}
    sdk058=type(profile)is dict and profile.get('profile')=='ashlar-local-current-installation-profile/0.2'
    sdk010=type(profile)is dict and profile.get('profile')=='ashlar-local-current-installation-profile/0.3'
    sdk058_keys={'source_policy_revision','umf_selected_tag_commit','umf_source_tree','umf_archive_sha256'}
    sdk010_keys={'source_policy_revision','umf_selected_tag_commit','umf_archive_sha256','umf_source_policy_sha256'}
    expected_keys=(base_profile_keys|sdk058_keys if sdk058 else base_profile_keys|sdk010_keys if sdk010 else base_profile_keys)
    _require(type(profile)is dict and set(profile)==expected_keys
        and profile.get('profile')==('ashlar-local-current-installation-profile/0.2' if sdk058 else 'ashlar-local-current-installation-profile/0.3' if sdk010 else 'ashlar-local-current-installation-profile/0.1')
        and profile.get('physical_profile')=='ashlar-delta/0.4-xxhash64'
        and profile.get('lookup_hash_profile')=='spark-xxhash64-typed-key/0.1'
        and type(profile.get('roles'))is dict and set(profile['roles'])==set(tables))
    if sdk058 or sdk010:
        from .configured_local_delta4_publication import _valid_installation_profile_identity
        _require(_valid_installation_profile_identity(profile))
    _require(type(created)is list and len(created)==6
        and {target.table for target in created}==set(tables.values())
        and len({target.uuid for target in created})==6)
    by_table={target.table:target for target in created}
    role_keys={'carrier_role','table','path','uuid','fields','field_metadata','protocol','clustering_columns',
        'partition_columns','layout_properties','umf_source_bytes_sha256','umf_source_sha256','umf_schema_sha256',
        'umf_target_sha256','umf_proposal_sha256','umf_ddl_sha256','umf_dispositions','umf_writer_settings','umf_proposal'}
    _require(all(type(profile['roles'][role])is dict
        and set(profile['roles'][role])==role_keys
        and profile['roles'][role].get('table')==table
        and profile['roles'][role].get('uuid')==by_table[table].uuid
        and profile['roles'][role].get('path')==str(by_table[table].path)
        for role,table in tables.items()))
    return tuple(created),profile,None


def _scope(config,source):
    scope={'installation_id':config.installation_id,'stream':config.stream,'materialization_clock_us':config.materialization_clock_us,
            'configuration_sha256':hashlib.sha256(source.configuration_file.raw).hexdigest(),'source_sha256':hashlib.sha256(source.original_source).hexdigest(),
            'model_sha256':source.configuration.model_sha256,'operation_capacity':None if config.operation_capacity is None else dict(config.operation_capacity)}
    if config.carrier_profile!='ashlar-delta/0.3':scope['physical_profile']=config.carrier_profile
    if config.sdk058_admission is not None:
        from .typed_graph_sdk058_admission import SDK058_PRODUCER_REVISION
        scope['typed_graph_policy_revision']=SDK058_PRODUCER_REVISION
    return scope


def _native(spark,target,columns,carrier_profile='ashlar-delta/0.3'):
    rows=spark.sql('DESCRIBE DETAIL delta.`'+str(target.path)+'`').limit(2).collect();_require(len(rows)==1)
    detail=rows[0].asDict(recursive=True)
    _require(detail['id']==target.uuid and detail['location'].removeprefix('file:').rstrip('/')==str(target.path)and detail['format']=='delta')
    # Native protocol inventory is admitted before any bag/row parity checks.
    validate_protocol_detail({'id':detail['id'],'format':detail['format'],'minReaderVersion':str(detail['minReaderVersion']),'minWriterVersion':str(detail['minWriterVersion']),'tableFeatures':json.dumps(detail.get('tableFeatures',[]))},uuid=target.uuid,profile=PROFILE)
    schema=spark.read.format('delta').load(str(target.path)).schema
    types={'string':'STRING','long':'BIGINT','timestamp':'TIMESTAMP'}
    _require(tuple((f.name,types.get(f.dataType.typeName()))for f in schema.fields)==columns)
    role=target.table.rsplit('.',1)[1];_require(tuple((f.name,types.get(f.dataType.typeName()),f.nullable)for f in schema.fields)==_fields(carrier_profile)[role])
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
        if self.config.carrier_profile=='ashlar-delta/0.4-xxhash64':
            from .local_current_carrier_profile import verify_local_writer_settings
            verify_local_writer_settings(self.spark)
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
                role=target.table.rsplit('.',1)[1];retention=_native(self.spark,target,self.columns[role],self.config.carrier_profile)
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


def _run(config,ordinal,*,selector=None,query=None,export=False,r02=None):
    _require(type(config)is ConfiguredLocalRuntimeConfig);config.__post_init__()
    initialize=ordinal is None
    _require(config.sdk058_admission is None or initialize)
    jars=_runtime(config)
    # The .4 installer is separately versioned and intentionally has no
    # continuation, publication, read, or query route until those consumers
    # bind its six-role carrier profile and complete vector.
    _require(config.carrier_profile=='ashlar-delta/0.3' or initialize)
    _require(type(export)is bool and (not export or not initialize and selector is None and query is None))
    if query is not None:
        from .configured_local_query import ConfiguredLocalQueryConfig
        from .configured_sdk_pages import ConfiguredSdkPagesConfig
        _require(type(query)in (ConfiguredLocalQueryConfig,ConfiguredSdkPagesConfig));query.__post_init__()
    if r02 is not None:
        from .configured_sdk_pages import ConfiguredR02Weft053Selection,ConfiguredSdkPagesConfig
        from ashlar.weft_053_distribution import Weft053Distribution
        _require(type(r02)is ConfiguredR02Weft053Selection and type(query)is ConfiguredSdkPagesConfig
            and type(query.compiler)is Weft053Distribution)
    else:_require(query is None or type(query.compiler).__name__!='Weft053Distribution')
    _require(query is None or selector is None and not initialize and not export)
    _require(r02 is None or selector is None and not initialize and not export and query is not None)
    _require(selector is None or type(selector)is tuple and len(selector)==2 and all(type(v)is int and -(2**63)<=v<2**63 for v in selector))
    _require(not initialize or selector is None)
    _require((initialize and config.installation_sha256 is None)or(not initialize and config.installation_sha256 is not None and type(ordinal)is int and 1<=ordinal<=4))
    parent=config.output.parent.lstat();_require(stat.S_ISDIR(parent.st_mode)and parent.st_uid==os.getuid()and not parent.st_mode&0o022)
    original_registry=None
    if not initialize:
        original_registry=InvocationFile.read(config.installation/'installation.json',65536)
        _require(hashlib.sha256(original_registry.raw).hexdigest()==config.installation_sha256)
    config.output.mkdir(mode=0o700)
    lease=None;spark=None;transport=None;source=None;primary=None;result=None;source_manager=None;admission=None;final_manifest=None;export_capture=None;carrier_profile_file=None;installation_profile=None
    try:
        if initialize:config.installation.mkdir(mode=0o700)
        lease=PrivateFiniteLease(config.installation,create=initialize)
        context=object()
        if config.sdk058_admission is None:
            source_manager=open_configured_local_source(config.admission,stream=config.stream,context=context)
        else:
            source_manager=open_configured_local_source(config.admission,stream=config.stream,context=context,sdk058_admission=config.sdk058_admission)
        source=source_manager.__enter__()
        _require(not export or source.graph)
        if query is not None:_require(type(query)is not ConfiguredSdkPagesConfig or source.graph)
        _require(selector is None or selector[0]in source.type_ids)
        scope=_scope(config,source);columns=_columns(config.carrier_profile);tables={r:'local.configured.'+r for r in columns}
        if not initialize:
            registry=json.loads(original_registry.raw);runtime_profile,runtime_identity,_=attest_runtime(config)
            _require(set(registry)=={'profile','scope','targets','columns','runtime_identity'}and registry['profile']=='ashlar-configured-local-installation/0.2'and registry['scope']==scope and registry['columns']=={r:[list(c)for c in cols]for r,cols in columns.items()}and registry['runtime_identity']=={'profile':runtime_profile,**runtime_identity})
        spark=open_finite_spark(config,jars,r02=r02 is not None)
        _require(spark.sparkContext.master=='local[1]'and spark.conf.get('spark.sql.session.timeZone')=='UTC'
            and spark.conf.get(PARAMETER_SUBSTITUTION_SETTING)=='true')
        verify_driver_memory(spark,r02=r02 is not None)
        _require(spark.version=='4.2.0' and spark.sparkContext._jvm.java.lang.System.getProperty('java.specification.version')=='21')
        _require(spark.conf.get('spark.jars')==','.join(str(p)for p in jars))
        verify_loaded_delta_class(spark,jars)
        spark.sparkContext.setLogLevel('ERROR');targets=[]
        if initialize:
            carrier_profile_record=None
            if config.carrier_profile=='ashlar-delta/0.4-xxhash64':
                from .local_current_carrier_profile import (
                    create_local_current_xxhash64_tables,
                    create_local_current_xxhash64_sdk058_tables,
                )
                creator=(create_local_current_xxhash64_sdk058_tables
                         if config.sdk058_admission is not None
                         else create_local_current_xxhash64_tables)
                targets,carrier_profile_record,_=_create_configured_xxhash64_tables(
                    spark,config.installation,tables,creator)
                carrier_profile_bytes=(encoded(carrier_profile_record)+'\n').encode()
                _write(config.installation/'carrier-profile.json',carrier_profile_bytes)
                carrier_profile_file=InvocationFile.read(config.installation/'carrier-profile.json',65536)
                _require(carrier_profile_file.raw==carrier_profile_bytes)
                installation_profile=carrier_profile_record
            else:
                for role,schema in columns.items():
                    path=config.installation/role
                    spark.sql('CREATE TABLE delta.`'+str(path)+'` ('+','.join(n+' '+f+(''if nullable else ' NOT NULL')for n,f,nullable in _fields(config.carrier_profile)[role])+") USING DELTA TBLPROPERTIES ('delta.deletedFileRetentionDuration'='interval 7 days','delta.logRetentionDuration'='interval 30 days')")
                    rows=spark.sql('DESCRIBE DETAIL delta.`'+str(path)+'`').limit(2).collect();_require(len(rows)==1)
                    targets.append(DeltaTarget(tables[role],path,rows[0]['id']))
            runtime_profile,runtime_identity,_=attest_runtime(config)
            registry={'profile':'ashlar-configured-local-installation/0.2',
                'runtime_identity':{'profile':runtime_profile,**runtime_identity},'scope':scope,'targets':[{'table':t.table,'path':str(t.path),'uuid':t.uuid}for t in targets],
                'columns':{r:[list(c)for c in cols]for r,cols in columns.items()}}
            if carrier_profile_record is not None:
                registry.update(profile='ashlar-configured-local-installation/0.3',physical_profile=config.carrier_profile,
                    carrier_profile_sha256=hashlib.sha256(carrier_profile_bytes).hexdigest())
            _write(config.installation/'installation.json',(encoded(registry)+'\n').encode());original_registry=InvocationFile.read(config.installation/'installation.json',65536)
        else:
            _require(type(registry['targets'])is list and len(registry['targets'])==6)
            for item in registry['targets']:
                _require(set(item)=={'table','path','uuid'}and item['table']in tables.values())
                role=item['table'].rsplit('.',1)[1];_require(item['path']==str(config.installation/role));targets.append(DeltaTarget(item['table'],Path(item['path']),item['uuid']))
            _require(len({t.table for t in targets})==6)
        targets=tuple(targets);policy=PrivatePolicy(context,targets)
        admission=ConfiguredLocalAdmission(config,lease,spark,source,targets,columns,original_registry);admission(source,None,None,context)
        transport_options={'capacity':config.operation_capacity}
        if installation_profile is not None:transport_options['installation_profile']=installation_profile
        transport=(LocalDeltaTransport.initialize(spark,config.installation/'operations.sqlite',config.installation_id,targets,policy,context=context,**transport_options)if initialize else LocalDeltaTransport(spark,config.installation/'operations.sqlite',config.installation_id,targets,policy,**transport_options));policy.initializing=False
        if initialize:result={'profile':'ashlar-configured-local-initialized/0.3' if config.carrier_profile=='ashlar-delta/0.4-xxhash64' else 'ashlar-configured-local-initialized/0.2','installation_id':config.installation_id,'operation':'initialize','installation_sha256':hashlib.sha256(original_registry.raw).hexdigest(),'installation':registry,'published':False,'protected_ack':False}
        elif selector is not None or query is not None or export or r02 is not None:
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
                    if r02 is not None:
                        from .configured_query_aliases import open_configured_query_aliases
                        from .configured_sdk_pages import run_configured_r02_weft053
                        _require(source.graph and type(query)is ConfiguredSdkPagesConfig)
                        with owned_context(open_configured_query_aliases(held,transport))as aliases:
                            answer=run_configured_r02_weft053(held,transport,query,aliases,r02,
                                runtime_guard=lambda:_runtime(config))
                    elif type(query)is ConfiguredSdkPagesConfig:
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
    if carrier_profile_file is not None:callbacks.append(lambda:carrier_profile_file.renew(65536))
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


def run_configured_local_delta4_transaction1(config):
    """Run only the frozen typed-graph transaction 1 on an existing `.4` install.

    This separately versioned local composition owns the source, lease, Spark
    runtime, initialized role profile, Delta journal and coordinator together.
    It retains a local output-completion receipt but does not write a manifest,
    acknowledge a source, or claim a consumer/publication result.

    This is a one-fresh-attempt entrypoint. A partial or completed attempt no
    longer satisfies fresh-empty admission and must be reconciled separately;
    this function is not a retry or recovery route.
    """
    from .configured_local_delta4_publication import (
        admit_delta4_fresh_empty_installation,
        build_delta4_transaction1_effect_plan,
        open_configured_local_delta4_authority,
        plan_configured_local_delta4_transaction1,
        retain_delta4_output_completion_receipt,
    )
    from .local_current_carrier_profile import apply_local_writer_settings

    _require(type(config) is ConfiguredLocalRuntimeConfig
             and config.carrier_profile == 'ashlar-delta/0.4-xxhash64'
             and config.installation_sha256 is not None)
    config.__post_init__()
    jars = _runtime(config)
    parent = config.output.parent.lstat()
    _require(stat.S_ISDIR(parent.st_mode) and parent.st_uid == os.getuid()
             and not parent.st_mode & 0o022)
    registry_file = InvocationFile.read(config.installation / 'installation.json', 65536)
    _require(hashlib.sha256(registry_file.raw).hexdigest() == config.installation_sha256)
    config.output.mkdir(mode=0o700)

    lease = spark = transport = source = admission = carrier_file = None
    source_manager = None
    primary = None
    result = None
    try:
        lease = PrivateFiniteLease(config.installation, create=False)
        context = object()
        if config.sdk058_admission is None:
            source_manager = open_configured_local_source(
                config.admission, stream=config.stream, context=context)
        else:
            source_manager = open_configured_local_source(
                config.admission, stream=config.stream, context=context,
                sdk058_admission=config.sdk058_admission)
        source = source_manager.__enter__()
        _require(source.graph)
        scope = _scope(config, source)
        columns = _columns(config.carrier_profile)
        local_tables = {role: 'local.configured.' + role for role in columns}
        from .schema_rows import LOCAL_CURRENT_ROLE_TO_UMF
        tables = {carrier_role: local_tables[local_role]
                  for local_role, carrier_role in LOCAL_CURRENT_ROLE_TO_UMF.items()}
        registry = json.loads(registry_file.raw)
        runtime_profile, runtime_identity, _runtime_jars = attest_runtime(config)
        _require(set(registry) == {'profile', 'scope', 'targets', 'columns',
                                   'runtime_identity', 'physical_profile',
                                   'carrier_profile_sha256'}
                 and registry.get('profile') == 'ashlar-configured-local-installation/0.3'
                 and registry.get('physical_profile') == config.carrier_profile
                 and registry.get('scope') == scope
                 and registry.get('columns') == {
                     role: [list(column) for column in selected]
                     for role, selected in columns.items()}
                 and registry.get('runtime_identity') == {
                     'profile': runtime_profile, **runtime_identity})
        carrier_file = InvocationFile.read(
            config.installation / 'carrier-profile.json', 65536)
        _require(hashlib.sha256(carrier_file.raw).hexdigest()
                 == registry.get('carrier_profile_sha256')
                 and carrier_file.raw.endswith(b'\n'))
        installation_profile = json.loads(carrier_file.raw)
        _require(type(installation_profile) is dict
                 and installation_profile.get('profile') in (
                    'ashlar-local-current-installation-profile/0.1',
                    'ashlar-local-current-installation-profile/0.2',
                    'ashlar-local-current-installation-profile/0.3')
                 and installation_profile.get('physical_profile')
                    == config.carrier_profile)

        targets = []
        roles = set(columns)
        _require(type(registry.get('targets')) is list
                 and len(registry['targets']) == len(roles))
        for item in registry['targets']:
            _require(type(item) is dict and set(item) == {'table', 'path', 'uuid'}
                     and type(item['table']) is str
                     and item['table'].startswith('local.configured.'))
            role = item['table'].rsplit('.', 1)[1]
            _require(role in roles and item['path'] == str(config.installation / role)
                     and installation_profile['roles'][role].get('uuid') == item['uuid'])
            targets.append(DeltaTarget(item['table'], Path(item['path']), item['uuid']))
        _require({target.table for target in targets} == set(tables.values()))

        spark = open_finite_spark(config, jars)
        _require(spark.sparkContext.master == 'local[1]'
                 and spark.conf.get('spark.sql.session.timeZone') == 'UTC'
                 and spark.conf.get(PARAMETER_SUBSTITUTION_SETTING) == 'true')
        verify_driver_memory(spark, r02=False)
        _require(spark.version == '4.2.0'
                 and spark.sparkContext._jvm.java.lang.System.getProperty(
                     'java.specification.version') == '21'
                 and spark.conf.get('spark.jars') == ','.join(str(path) for path in jars))
        verify_loaded_delta_class(spark, jars)
        spark.sparkContext.setLogLevel('ERROR')
        # The carrier requires ZSTD for writes. Each worker stage opens a new
        # Spark session, so apply the UMF setting here before admission checks
        # the session; admission and close only verify it and never repair drift.
        apply_local_writer_settings(spark)
        if installation_profile.get('profile') in (
                'ashlar-local-current-installation-profile/0.2',
                'ashlar-local-current-installation-profile/0.3'):
            from .local_current_carrier_profile import verify_local_current_profile
            verify_local_current_profile(spark, installation_profile, tuple(
                DeltaTarget(item['table'],Path(item['path']),item['uuid'])
                for item in registry['targets']), config.carrier_profile)

        policy = PrivatePolicy(context, tuple(targets))
        admission = ConfiguredLocalAdmission(
            config, lease, spark, source, tuple(targets), columns, registry_file)
        admission(source, None, None, context)
        transport = LocalDeltaTransport(
            spark, config.installation / 'operations.sqlite', config.installation_id,
            tuple(targets), policy, capacity=config.operation_capacity,
            installation_profile=installation_profile)
        policy.initializing = False

        source_configuration=(source.configuration.base
                              if config.sdk058_admission is not None
                              else source.configuration)
        plan = plan_configured_local_delta4_transaction1(
            source_configuration, stream=config.stream,
            materialized_at_us=config.materialization_clock_us)
        request = source.spec(1, context).request()
        _require(plan.batch_id == 'typed-graph-1' and plan.request == request)
        fresh = admit_delta4_fresh_empty_installation(transport, installation_profile)
        effect_plan = build_delta4_transaction1_effect_plan(
            plan, tables=tables, installation_profile=installation_profile)
        authority = open_configured_local_delta4_authority(
            plan, effect_plan, transport, installation_profile, fresh, source,
            admission, context, policy)
        with authority.hold() as held:
            held.prepare_and_execute_effect_plan()
            observation = held._observe_effect_prefix()
            versions = {role: observation['heads'][role]['version']
                        for role in ('edge_current', 'object_current', 'tombstone',
                                     'whole_source_history')}
            phase_head = observation['heads']['publication_attempt_phase']
            _require(phase_head['version'] == fresh.heads['publication_attempt_phase']['version'] + 2
                     and versions == {
                         'edge_current': fresh.heads['edge_current']['version'] + 1,
                         'object_current': fresh.heads['object_current']['version'] + 1,
                         'tombstone': fresh.heads['tombstone']['version'],
                         'whole_source_history': fresh.heads['whole_source_history']['version'] + 1}
                     and observation['heads']['publication_manifest']['version']
                        == fresh.heads['publication_manifest']['version'])
            receipt = retain_delta4_output_completion_receipt(held)
            receipt_reference = {
                'profile': receipt['profile'],
                'request_digest': receipt['request_digest'],
                'receipt_sha256': receipt['receipt_sha256'],
            }
            result = {
                'profile': 'ashlar-configured-local-delta4-transaction1-result/0.1',
                'installation_id': config.installation_id,
                'operation': 'transaction1-effects-validated',
                'batch_id': 'typed-graph-1',
                'publication': False,
                'protected_ack': False,
                'completion_receipt': receipt_reference,
                'native_observation': observation,
            }
    except BaseException as error:
        primary = error
    callbacks = []
    if source is not None:
        callbacks.append(lambda: source.renew(source.context))
    if registry_file is not None:
        callbacks.append(lambda: registry_file.renew(65536))
    if carrier_file is not None:
        callbacks.append(lambda: carrier_file.renew(65536))
    if admission is not None:
        callbacks.append(lambda: admission.closing(source, None, source.context))
    if transport is not None:
        callbacks.append(transport.close)
    if spark is not None:
        callbacks.append(spark.stop)
    if source is not None:
        callbacks.append(lambda: source_manager.__exit__(None, None, None))
    callbacks.append(lambda: _runtime(config))
    if lease is not None:
        callbacks.extend((lease.renew, lease.close))
    finish(primary, callbacks)
    raw = (encoded(result) + '\n').encode()
    _require(len(raw) <= 1_048_576)
    _write(config.output / 'runtime-report.json', raw)
    return result

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


def run_configured_local_r02_weft053(config,*,ordinal,query,selection):
    """Private R02 packet entry; deliberately absent from generic CLI parsing."""
    from .configured_sdk_pages import ConfiguredR02Weft053Selection,ConfiguredSdkPagesConfig
    from ashlar.weft_053_distribution import Weft053Distribution
    _require(ordinal==1 and type(ordinal)is int and type(query)is ConfiguredSdkPagesConfig and type(query.compiler)is Weft053Distribution
        and type(selection)is ConfiguredR02Weft053Selection)
    query.__post_init__();selection.__post_init__()
    return _run(config,ordinal,query=query,r02=selection)
