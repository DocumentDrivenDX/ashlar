"""Fresh-only native creation from pinned public UMF-generated local carriers."""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
from .commerce import runtime_paths
from .driver import PrivatePolicy
from .delta_custody import DeltaTarget,LocalDeltaTransport
from .lifecycle import finish,owned_context
from .original_commerce_local_configuration import OriginalCommerceLocalInitializationConfig
from .original_commerce_publication import write_observation
from .source import original_inputs,recompute_dataset

_ROLES=('object_current','edge_current','tombstone','whole_source_history','attempts','manifest')
_PROVENANCE='fb2bff45bc7849d298208d0436d99295c01696bdcdd8101f63caa8069e122e9a'


def generated_carriers():
    """Compatibility wrapper for the fixed public physical-carrier owner."""
    from .local_physical_carriers import original_local_physical_carriers, ORIGINAL_LOCAL_PHYSICAL_PROVENANCE
    if _PROVENANCE!=ORIGINAL_LOCAL_PHYSICAL_PROVENANCE:
        raise ValueError('Original generated provenance differs')
    return original_local_physical_carriers()


def _identity(path,fd,directory,mode):
    named=path.lstat();held=os.fstat(fd)
    if (path.resolve()!=path or named.st_uid!=os.getuid()or stat.S_IMODE(named.st_mode)!=mode or
        not (stat.S_ISDIR(named.st_mode)if directory else stat.S_ISREG(named.st_mode))or
        (named.st_dev,named.st_ino)!=(held.st_dev,held.st_ino)):
        raise PermissionError('Actual private named/held owner required')
    return held.st_dev,held.st_ino


def _directory_facts(path):
    info=path.lstat()
    if path.resolve()!=path or not stat.S_ISDIR(info.st_mode):raise PermissionError('Original canonical directory required')
    return info.st_dev,info.st_ino,info.st_uid,stat.S_IMODE(info.st_mode)


def _retain_at(path,fd,original,raw):
    if _identity(path,fd,True,0o700)!=original:raise PermissionError('Original observation directory changed')
    output=None;primary=None
    try:
        output=os.open('installation.json',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=fd)
        offset=0
        while offset<len(raw):
            count=os.write(output,raw[offset:])
            if count<=0:raise OSError('Incomplete installation correspondence')
            offset+=count
        os.fsync(output)
    except BaseException as error:primary=error
    finish(primary,([lambda:os.close(output)]if output is not None else [])+[lambda:_identity(path,fd,True,0o700)])



class _Bootstrap(PrivatePolicy):
    def __init__(self,context,config,fd,gate,inputs,output_fd,parents):
        super().__init__(context,());self.config=config;self.fd=fd;self.gate=gate;self.inputs=inputs;self.output_fd=output_fd;self.parents=parents
        self.root_identity=_identity(config.root,fd,True,0o700)
        self.output_identity=_identity(config.output,output_fd,True,0o700)
        self.gate_identity=_identity(config.root/'.evolution-operator.lock',gate,False,0o600)
    def renew(self):
        if any(_directory_facts(path)!=facts for path,facts in self.parents)or _identity(self.config.output,self.output_fd,True,0o700)!=self.output_identity:
            raise PermissionError('Original initialization parent/observation custody changed')
        if (_identity(self.config.root,self.fd,True,0o700)!=self.root_identity or
            _identity(self.config.root/'.evolution-operator.lock',self.gate,False,0o600)!=self.gate_identity or
            original_inputs(self.config.model,self.config.graph)!=self.inputs):
            raise PermissionError('Original fresh installation/source custody changed')
        if self.config.custody is not None:self.config.custody.renew()
        self.config.runtime.renew()
        runtime_paths(self.config.publication_inputs(),True,require_fresh=False)
    @contextmanager
    def writer(self,*args):
        self.renew();primary=None
        try:
            with owned_context(super().writer(*args)):yield
        except BaseException as error:primary=error
        finish(primary,[self.renew])
    def admit(self,intent,context):
        self.renew();return super().admit(intent,context)


@contextmanager
def _native_session(jars):
    from pyspark.sql import SparkSession
    spark=None;primary=None
    try:
        if SparkSession.getActiveSession() is not None:
            raise PermissionError('Fresh selected Spark session required')
        spark=(SparkSession.builder.master('local[1]').appName('Ashlar original local initializer')
            .config('spark.driver.memory','512m').config('spark.ui.enabled','false')
            .config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1')
            .config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','true')
            .config('spark.jars',','.join(str(p)for p in jars))
            .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
            .config('spark.executorEnv.PYTHONDONTWRITEBYTECODE','1').getOrCreate())
        if spark.version!='3.5.3':raise ValueError('Exact selected publication Spark required')
        expected={'spark.driver.memory':'512m','spark.ui.enabled':'false','spark.sql.shuffle.partitions':'1',
            'spark.sql.session.timeZone':'UTC','spark.sql.ansi.enabled':'true',
            'spark.executorEnv.PYTHONDONTWRITEBYTECODE':'1'}
        if spark.sparkContext.master!='local[1]' or any(spark.conf.get(key)!=value for key,value in expected.items()):
            raise PermissionError('Exact selected Spark configuration required')
        yield spark
    except BaseException as error:primary=error
    finish(primary,[spark.stop]if spark is not None else [])


def _observe_native(spark,target,expected_schema,registration=None):
    from urllib.parse import urlsplit,unquote
    table='delta.`'+str(target.path).replace('`','``')+'`'
    detail=spark.sql('DESCRIBE DETAIL '+table).first().asDict()
    history=spark.sql('DESCRIBE HISTORY '+table).select('version').limit(2).collect()
    frame=spark.read.format('delta').load(str(target.path))
    location=urlsplit(detail['location'])
    expected_properties={}
    version=0
    history_ok=len(history)==1 and type(history[0][0])is int and history[0][0]==0
    if registration is not None:
        version=1
        expected_properties={'ashlar.local.installation.id':registration.installation_id,
            'ashlar.local.registry.sha256':registration.registry_sha,'ashlar.local.journal.sha256':registration.journal_sha}
        original=registration.original_history(target)
        metadata=json.dumps({'profile':'ashlar-local-delta-installation-commit/0.1',
            'installation_id':registration.installation_id,'registry_sha256':registration.registry_sha,
            'journal_sha256':registration.journal_sha,'uuid':target.uuid},sort_keys=True,separators=(',',':'))
        history_ok=(len(original)==2 and all(type(row.get('version'))is int for row in original)
            and [row['version']for row in original]==[1,0]
            and original[0].get('operation')=='SET TBLPROPERTIES'and original[0].get('userMetadata')==metadata
            and original[1].get('userMetadata')is None)
    if (location.scheme!='file'or location.netloc or Path(unquote(location.path)).resolve()!=target.path or
        detail['id']!=target.uuid or not history_ok or
        frame.limit(1).count()!=0 or json.loads(frame.schema.json())!=expected_schema or
        detail['partitionColumns']or detail['properties']!=expected_properties or
        type(detail['minReaderVersion'])is not int or detail['minReaderVersion']!=1 or
        type(detail['minWriterVersion'])is not int or detail['minWriterVersion']!=2):
        raise ValueError('Current empty native generated identity/schema/protocol differs')
    return {'uuid':target.uuid,'version':version,'location':detail['location'],'schema':expected_schema,
            'minReaderVersion':detail['minReaderVersion'],'minWriterVersion':detail['minWriterVersion']}


def initialize_original_commerce_local(config):
    """Create six empty native tables and journal under an exclusive fresh root lease."""
    if type(config)is not OriginalCommerceLocalInitializationConfig:raise ValueError('Exact fresh local configuration required')
    config.__post_init__();config.runtime.renew();generated=generated_carriers();inputs=original_inputs(config.model,config.graph)
    jars=runtime_paths(config.publication_inputs(),True)
    if config.root.exists()or config.root.is_symlink()or config.output.exists()or config.output.is_symlink():
        raise ValueError('Fresh native and observation directories required')
    # Partial failures remain retained; this API never resets a native installation.
    parents=tuple((path,_directory_facts(path))for path in sorted({config.root.parent,config.output.parent}))
    fd=None;gate=None;output_fd=None;policy=None;transport=None;primary=None;result=None
    try:
        if any(_directory_facts(path)!=facts for path,facts in parents):raise PermissionError('Original parent changed before initialization')
        config.output.mkdir(mode=0o700);created_output=config.output.lstat()
        output_fd=os.open(config.output,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        if _identity(config.output,output_fd,True,0o700)!=(created_output.st_dev,created_output.st_ino):raise PermissionError('Created observation directory replaced')
        recompute_dataset(config.producer,config.model,config.graph,config.output/'public-dataset.json')
        config.root.mkdir(mode=0o700);created=config.root.lstat()
        fd=os.open(config.root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        if _identity(config.root,fd,True,0o700)!=(created.st_dev,created.st_ino):raise PermissionError('Created root replaced')
        gate=os.open(config.root/'.evolution-operator.lock',os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        fcntl.flock(gate,fcntl.LOCK_EX|fcntl.LOCK_NB)
        context=object();policy=_Bootstrap(context,config,fd,gate,inputs,output_fd,parents)
        with owned_context(_native_session(jars))as spark:
            from pyspark.sql.types import StructType
            targets=[];native_primary=None;witnesses={}
            try:
                for role in _ROLES:
                    policy.renew();path=config.root/role;schema=json.loads(generated['carriers'][role]['schemaJson'])
                    spark.createDataFrame([],StructType.fromJson(schema)).write.format('delta').save(str(path))
                    table='delta.`'+str(path).replace('`','``')+'`'
                    detail=spark.sql('DESCRIBE DETAIL '+table).first().asDict()
                    history=spark.sql('DESCRIBE HISTORY '+table).select('version').limit(2).collect()
                    if (len(history)!=1 or type(history[0][0])is not int or history[0][0]!=0 or
                        spark.read.format('delta').load(str(path)).limit(1).count()!=0 or
                        json.loads(spark.read.format('delta').load(str(path)).schema.json())!=schema or
                        detail['partitionColumns']or detail['properties']or type(detail['id'])is not str):
                        raise ValueError('Actual fresh generated native correspondence differs')
                    targets.append(DeltaTarget('local.original.'+role,path,detail['id']))
                    witnesses[role]=_observe_native(spark,targets[-1],schema)
                policy.targets=tuple(targets)
                transport=LocalDeltaTransport.initialize(spark,config.root/'operations.sqlite',config.installation_id,tuple(targets),policy,context=context,capacity=config.capacity)
                result={'profile':'ashlar-original-commerce-local-installation/0.1','installation_id':config.installation_id,
                        'journal':str(config.root/'operations.sqlite'),'targets':[{'table':t.table,'path':str(t.path),'uuid':t.uuid}for t in targets],
                        'capacity':None if config.capacity is None else dict(config.capacity),'generated_provenance_sha256':_PROVENANCE,
                        'qualification':'Fresh isolated local installation; existing publication requires current ordinary source/ACK admission.'}
            except BaseException as error:native_primary=error
            def close_native():
                def check(target,registration):
                    role=target.table.split('.')[-1]
                    observed=_observe_native(spark,target,json.loads(generated['carriers'][role]['schemaJson']),registration)
                    if role in witnesses:
                        original=dict(witnesses[role]);original['version']=1 if registration is not None else 0
                        if observed!=original:raise ValueError('Original opening native witness changed')
                if transport is None:
                    finish(None,[lambda target=target:check(target,None)for target in targets])
                else:
                    # Reopen through the public existing-installation owner: exact complete
                    # registry, reservations and canonical journal are renewed before witnesses.
                    registration=None;registration_primary=None
                    try:
                        registration=LocalDeltaTransport(spark,config.root/'operations.sqlite',
                            config.installation_id,tuple(targets),policy,capacity=config.capacity)
                    except BaseException as error:registration_primary=error
                    finish(registration_primary,[lambda target=target:check(target,transport)for target in targets]+
                        ([registration.close]if registration is not None else []))
            def close_transport():
                nonlocal_transport=transport
                # The surrounding owner clears its reference before selecting cleanup errors.
                nonlocal_transport.close()
            callbacks=[close_native,policy.renew]+([close_transport]if transport is not None else [])
            try:finish(native_primary,callbacks)
            finally:transport=None
    except BaseException as error:primary=error
    try:
        finish(primary,([transport.close]if transport is not None else [])+([policy.renew]if policy is not None else [])+
               ([lambda:os.close(gate)]if gate is not None else [])+([lambda:os.close(fd)]if fd is not None else []))
        primary=None
    except BaseException as error:primary=error
    if primary is not None:
        finish(primary,[lambda:os.close(output_fd)]if output_fd is not None else [])
    primary=None
    try:
        if config.custody is not None:config.custody.renew()
        config.runtime.renew()
        if any(_directory_facts(path)!=facts for path,facts in parents):raise PermissionError('Original parent changed before final correspondence')
        _retain_at(config.output,output_fd,policy.output_identity,(json.dumps(result,sort_keys=True,separators=(',',':'))+'\n').encode())
        if any(_directory_facts(path)!=facts for path,facts in parents):raise PermissionError('Original parent changed after final correspondence')
        if _identity(config.output,output_fd,True,0o700)!=policy.output_identity:raise PermissionError('Original output changed after final correspondence')
    except BaseException as error:primary=error
    finish(primary,[lambda:os.close(output_fd)])
    return result


def original_commerce_local_source_ports(selection):
    """Ordinary transports only; existing source/native owners decide authority."""
    from .original_commerce_local_configuration import OriginalCommerceLocalSource
    from .commerce_publication_sources import OriginalCommerceSourcePorts
    from .source_sessions import OutboxSourceRegistration
    from .existing_outbox_authority import OutboxAuthorityRegistration
    if type(selection)is not OriginalCommerceLocalSource:raise ValueError('Exact isolated ordinary source selection required')
    selection.__post_init__()
    def connect(role):
        selection.__post_init__()
        if type(role)is not str or role not in (selection.writer_role,selection.reader_role,selection.ack_role):
            raise ValueError('Closed distinct ordinary role required')
        if any(name.startswith('PG')for name in os.environ):
            raise PermissionError('Ambient PostgreSQL settings refused for isolated ordinary login')
        import psycopg
        return psycopg.connect(host=selection.host,hostaddr=selection.host,port=selection.port,dbname=selection.database,user=role,
            password='',passfile='/dev/null',sslmode='disable',gssencmode='disable',
            options='-c statement_timeout=15000 -c lock_timeout=5000',connect_timeout=5,autocommit=False)
    source=OutboxSourceRegistration(selection.scope,selection.source_schema,selection.source_signature_sha256,
        lambda context:connect(selection.reader_role))
    registration=OutboxAuthorityRegistration(source,selection.reader_role,selection.ack_role,selection.database,
        lambda context:connect(selection.ack_role))
    return OriginalCommerceSourcePorts(registration,selection.writer_role,lambda context:connect(selection.writer_role))


def publish_existing_original_commerce_local(config,*,ports=None):
    """Compose the existing public publisher; the handoff supplies correspondence only."""
    from .original_commerce_local_configuration import OriginalCommerceLocalPublicationConfig
    from .original_commerce_publication import OriginalCommercePublicationOperator,publish_original_commerce
    from .commerce_publication_sources import OriginalCommerceSourcePorts
    from .evolution_cli import InvocationFile
    from .delta_custody import operation_capacity
    if type(config)is not OriginalCommerceLocalPublicationConfig:
        raise ValueError('Exact existing publication configuration/current ordinary ports required')
    config.__post_init__()
    if ports is None:ports=original_commerce_local_source_ports(config.source)
    if type(ports)is not OriginalCommerceSourcePorts:raise ValueError('Exact ordinary original source ports required')
    config.runtime.renew();ports.__post_init__()
    if ports.metadata()!=original_commerce_local_source_ports(config.source).metadata():raise ValueError('Original configured source/ACK registration differs')
    handoff=InvocationFile.read(config.installation,262144)
    if hashlib.sha256(handoff.raw).hexdigest()!=config.installation_sha256:raise ValueError('Selected installation correspondence changed')
    value=json.loads(handoff.raw)
    if (type(value)is not dict or set(value)!={'profile','installation_id','journal','targets','capacity','generated_provenance_sha256','qualification'}or
        value['profile']!='ashlar-original-commerce-local-installation/0.1'or value['generated_provenance_sha256']!=_PROVENANCE or
        type(value['targets'])is not list or len(value['targets'])!=6):
        raise ValueError('Closed original installation correspondence required')
    if type(value['installation_id'])is not str or value['installation_id']!=config.source.scope.installation_id:
        raise ValueError('Original installation and protected source scope must match')
    journal=Path(value['journal'])
    if not journal.is_absolute()or journal.name!='operations.sqlite':raise ValueError('Original journal correspondence required')
    targets=[]
    for row in value['targets']:
        if type(row)is not dict or set(row)!={'table','path','uuid'}or any(type(row[k])is not str for k in row):
            raise ValueError('Complete passive original targets required')
        role=row['table'].removeprefix('local.original.')
        if role not in _ROLES or row['table']!='local.original.'+role or Path(row['path'])!=journal.parent/role:
            raise ValueError('Exact original local target binding required')
        targets.append(DeltaTarget(row['table'],Path(row['path']),row['uuid']))
    if len({target.table for target in targets})!=6:raise ValueError('Complete original roles required')
    capacity=operation_capacity(value['capacity']);jars=runtime_paths(config.publication,True)
    @contextmanager
    def selected_session():
        primary=None
        try:
            if config.custody is not None:config.custody.renew()
            handoff.renew(262144);config.runtime.renew()
            with owned_context(_native_session(jars))as spark:yield spark
        except BaseException as error:primary=error
        finish(primary,[lambda:handoff.renew(262144),config.runtime.renew]+([config.custody.renew]if config.custody is not None else []))
    operator=OriginalCommercePublicationOperator(value['installation_id'],journal,tuple(targets),ports,capacity,selected_session)
    primary=None;result=None
    try:
        handoff.renew(262144)
        result=publish_original_commerce(config.publication,operator=operator)
    except BaseException as error:primary=error
    finish(primary,[lambda:handoff.renew(262144),config.runtime.renew]+([config.custody.renew]if config.custody is not None else []))
    return result
