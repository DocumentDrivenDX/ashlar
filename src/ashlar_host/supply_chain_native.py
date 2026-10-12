"""Explicit tiny finite-file Delta setup and original five-query composition.

Exclusive private local directories and actual POSIX leases govern this profile.
This supplies no Unity Catalog, remote-source, Truss or protected ACK authority.
SDKs remain lazy and tools are never imported into the installed library.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl,hashlib,importlib.metadata,json,os,stat,sys
from pathlib import Path
from typing import Protocol,Sequence,Optional,TYPE_CHECKING
from ashlar.publisher import publish_batch
from ashlar.staging import batch_row
from ashlar.manifest import FIELDS
from ashlar.native import quote_table_identifier
from ashlar.weft_count_star_distribution import CountStarDistributionPaths,installed_count_star_schema_bundle
from ashlar.weft_path_decode import PathDecodeConfig
from .commerce import QUERY_JARS
from .count_star_admission import CountStarAdmissionConfig
from .count_star_schema import make_offline_count_star_schema_validation
from .count_star_execution import CountStarExecutionConfig
from .path_capture import PathCaptureConfig
from .delta_custody import DeltaTarget,LocalDeltaTransport,encoded,operation_capacity
from .driver import PrivatePolicy
from .publication_reader import ReadOnlyTransport
from .schema_rows import fixture_columns
from .resources import RESOURCE_ROOT
from .evolution_producer import snapshot
from .lifecycle import finish,owned_context
from .exact_report import report_body
from .evolution_cli import InvocationFile
from .supply_chain_producer import SupplyChainProducerConfig,HeldSupplyChainDataset
from .supply_chain_finite_source import FiniteSupplyChainSource,UMF_REVISION
from .supply_chain_publication import FiniteSupplyChainDriver,GRAPH_ROLES
from .supply_chain_request import SOURCE_SYSTEM,SOURCE_SHA,supply_chain_cases,source_snapshot
from .supply_chain_query import query_supply_chain_cases,finite_public_source
if TYPE_CHECKING:
    from .supply_chain_two_hop_query import SupplyChainTwoHopQueryConfig

@dataclass(frozen=True)
class SupplyChainNativeConfig:
    output:Path
    jars:Path
    pack:Path
    model:Path
    graph:Path
    producer:SupplyChainProducerConfig
    compiler:CountStarDistributionPaths
    operation_capacity:object=None
    two_hop:Optional['SupplyChainTwoHopQueryConfig']=None
    def __post_init__(self):
        if any(type(p)is not type(Path('/'))or not p.is_absolute()or any(q.is_symlink()for q in (p,*p.parents))for p in (self.output,self.jars,self.pack,self.model,self.graph)):
            raise ValueError('Exact absolute finite native paths required')
        if type(self.producer)is not SupplyChainProducerConfig or type(self.compiler)is not CountStarDistributionPaths or self.compiler.package is not None:
            raise ValueError('Typed selected producer and existing compiler installation required')
        self.producer.__post_init__();object.__setattr__(self,'operation_capacity',operation_capacity(self.operation_capacity))
        if self.two_hop is not None:
            from .supply_chain_two_hop_query import SupplyChainTwoHopQueryConfig
            if type(self.two_hop)is not SupplyChainTwoHopQueryConfig:raise ValueError('Exact selected two-hop configuration required')
            self.two_hop.__post_init__()
            capture=self.two_hop.capture;decoder=self.two_hop.decoder
            capture.__post_init__();decoder.__post_init__()
            if capture.maximum_rows>128 or capture.maximum_cell_bytes>16777216 or capture.maximum_total_cell_bytes>16777216:
                raise ValueError('Explicit tiny original two-hop capture bounds required')
            for path in (self.two_hop.distribution.index,self.two_hop.distribution.output):
                if type(path)is not type(Path('/'))or not path.is_absolute()or path.resolve()!=path or any(q.is_symlink()for q in (path,*path.parents)):
                    raise ValueError('Canonical existing Paths selections required')
                if self.output==path or self.output in path.parents or path in self.output.parents:raise ValueError('Separate fresh native output required')

        if any(any(c in str(p)for c in ('`',"'",'\\','\x00','\n','\r'))for p in (self.output,self.jars,self.pack,self.model,self.graph)):
            raise ValueError('Closed local native SQL path profile required')
        if self.producer.maximum_receipt_bytes<8*1024*1024:raise ValueError('Explicit whole original dataset8MiB receipt budget required')

class PrivateFiniteLease:
    """Actually held cooperating local installation lease; never a descriptor grant."""
    def __init__(self,root,*,create):
        self.root=Path(root);self.path=self.root/'.finite-installation.lock';self.fd=None
        if self.root.is_symlink()or not self.root.is_dir()or self.root.stat().st_uid!=os.getuid()or self.root.stat().st_mode&0o077:
            raise PermissionError('Owned private finite installation directory required')
        root_info=self.root.stat();self.root_identity=(root_info.st_dev,root_info.st_ino)
        flags=os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK|(os.O_CREAT|os.O_EXCL if create else 0)
        self.fd=os.open(self.path,flags,0o600)
        try:
            fcntl.flock(self.fd,fcntl.LOCK_EX|fcntl.LOCK_NB);info=os.fstat(self.fd)
            if not stat.S_ISREG(info.st_mode)or info.st_uid!=os.getuid()or info.st_mode&0o077:raise PermissionError('Owned actual finite lease required')
            self.identity=(info.st_dev,info.st_ino);self.renew()
        except BaseException as primary:
            fd=self.fd;self.fd=None
            finish(primary,[lambda:os.close(fd)])
    def renew(self):
        if self.fd is None:raise PermissionError('Finite lease is not held')
        info=os.fstat(self.fd);named=self.path.lstat();root_info=self.root.stat()
        if self.root.is_symlink()or (root_info.st_dev,root_info.st_ino)!=self.root_identity or root_info.st_uid!=os.getuid()or root_info.st_mode&0o077 or stat.S_ISLNK(named.st_mode)or (info.st_dev,info.st_ino)!=self.identity or (named.st_dev,named.st_ino)!=self.identity:
            raise PermissionError('Original finite lease identity changed')
        fcntl.flock(self.fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
    def close(self):
        if self.fd is not None:
            fd=self.fd;self.fd=None
            finish(None,[lambda:fcntl.flock(fd,fcntl.LOCK_UN),lambda:os.close(fd)])

def _write(path,raw):
    with owned_context(path.open('xb'))as stream:
        if stream.write(raw)!=len(raw):raise ValueError('Complete finite evidence write refused')

def _runtime(config):
    config.__post_init__()
    if sys.version_info[:2]!=(3,11)or any(importlib.metadata.version(k)!=v for k,v in {'pyspark':'4.0.1','delta-spark':'4.0.0'}.items()):
        raise ValueError('Selected Python3.11 Spark4.0.1 Delta4.0.0 required')
    if not config.jars.is_dir()or {p.name for p in config.jars.iterdir()}!=set(QUERY_JARS):raise ValueError('Exact selected Delta4 JAR inventory required')
    jars=[]
    for name,digest in sorted(QUERY_JARS.items()):
        p=config.jars/name
        if hashlib.sha256(snapshot(p,32*1024*1024)).hexdigest()!=digest:raise ValueError('Selected Delta JAR bytes differ')
        jars.append(p)
    return jars

def _spark(config,jars):
    from pyspark.sql import SparkSession
    if SparkSession.getActiveSession()is not None:raise ValueError('Fresh independent native process required')
    return (SparkSession.builder.master('local[1]').appName('Original finite supply-chain Delta publication and041 queries')
        .config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
        .config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.session.timeZone','UTC')
        .config('spark.sql.ansi.enabled','true').config('spark.sql.decimalOperations.allowPrecisionLoss','false')
        .config('spark.jars',','.join(str(p)for p in jars)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.sql.warehouse.dir',str(config.output/'warehouse')).getOrCreate())

def _current_lease(lease,context,tables,targets):
    original={t.table:(str(t.path),t.uuid)for t in targets}
    def admit(source,request,manifest,supplied):
        if supplied is not context:raise PermissionError('Original finite installation context required')
        lease.renew();source.metadata()
        if request is not None:source.admit_request(request)
        if manifest is not None:
            if json.loads(manifest['schema_revisions_json'])!={SOURCE_SYSTEM:SOURCE_SHA}or set(json.loads(manifest['table_versions_json']))!={tables[r]for r in GRAPH_ROLES}:
                raise PermissionError('Complete original finite source/vector custody required')
        if {t.table:(str(t.path),t.uuid)for t in targets}!=original:raise PermissionError('Original complete finite target registry changed')
        lease.renew()
    return admit

def _request(batch):
    row=batch_row(batch);value={'stream':'original-finite-supplychain','batch_id':batch.batch_id,'predecessor':'explicit-finite-supplychain-origin',
        'schema_revisions_json':encoded({SOURCE_SYSTEM:SOURCE_SHA}),'source_batch_json':row['batch_json'],'source_batch_digest':row['batch_digest']}
    value['request_digest']=hashlib.sha256(encoded(value).encode()).hexdigest();return value

def _schemas(config):
    raw=dict(installed_count_star_schema_bundle(config.compiler));selected={n:raw[n]for n in ('compile-request-v0.4.1.schema.json','compile-response-v0.4.1.schema.json','logical-plan-v0.4.1.schema.json')}
    return make_offline_count_star_schema_validation(selected)

def _queries(config,driver,pack,aliases):
    from .supply_chain_compile import SupplyChainCompileConfig
    maximum=16*1024*1024
    execution=CountStarExecutionConfig(CountStarAdmissionConfig(maximum,_schemas(config)),PathCaptureConfig(128,maximum,maximum),PathDecodeConfig(maximum),finite_public_source(driver.source,maximum),UMF_REVISION)
    observed=0
    native_observed=0
    count_records=[];path_records=[]
    with owned_context((config.output/'compiler-observations.jsonl').open('xb'))as log, owned_context((config.output/'native-observations.jsonl').open('xb'))as native_log:
        def observe_native(name,captured):
            nonlocal native_observed
            body=report_body({'id':name,'captured':captured},64*1024*1024-native_observed)
            if native_log.write(body)!=len(body):raise ValueError('Finite native observation write refused')
            native_log.flush();native_observed+=len(body)
        def retain_protocol(name,iteration,request,response):
            nonlocal observed
            if any(type(b)is not bytes or len(b)>1048576 for b in (request,response)):raise ValueError('Original finite compiler observation bound')
            raw=(json.dumps({'id':name,'iteration':iteration,'requestHex':request.hex(),'responseHex':response.hex()},separators=(',',':'))+'\n').encode();observed+=len(raw)
            if observed>8*1024*1024:raise ValueError('Complete finite compiler observation bound')
            if log.write(raw)!=len(raw):raise ValueError('Finite compiler record write refused')
            log.flush()
        def observe(name,iteration,request,response):
            retain_protocol(name,iteration,request,response);count_records.append((name,iteration))
        authored=query_supply_chain_cases(driver,pack,aliases,SupplyChainCompileConfig(config.compiler,maximum),execution,observe=observe,observe_native=observe_native)
        if config.two_hop is None:return authored
        def observe_paths(ordinal,iteration,request,response):
            if type(ordinal)is not int or not 0<=ordinal<12 or type(iteration)is not int or iteration not in (0,1):raise ValueError('Exact two-hop protocol ordinal required')
            if (ordinal,iteration) in path_records or len(path_records)>=24:raise ValueError('Complete unique two-hop protocol observations required')
            if any(type(raw)is not bytes or len(raw)>config.two_hop.maximum_artifact_bytes for raw in (request,response)):raise ValueError('Selected two-hop protocol artifact bound')
            retain_protocol('two-hop-'+str(ordinal),iteration,request,response);path_records.append((ordinal,iteration))
        from .supply_chain_two_hop_query import query_supply_chain_two_hop
        two_hop=query_supply_chain_two_hop(driver,pack,aliases,config.two_hop,observe=observe_paths)
        if (len(authored['cases'])!=5 or len(count_records)!=10
                or len(set(count_records))!=10 or set(path_records)!={(i,j)for i in range(12)for j in (0,1)}
                or type(two_hop)is not dict or type(two_hop.get('queries'))is not list
                or [q.get('ordinal')for q in two_hop['queries']]!=list(range(12))):
            raise ValueError('Complete original five plus twelve query evidence required')
        for query in two_hop['queries']:observe_native('two-hop-'+str(query['ordinal']),query['evidence'])
        return {**authored,'two_hop':two_hop,'two_hop_queries':12}

def _finish_report(config,result):
    body=report_body(result,64*1024*1024);_write(config.output/'report.json',body)
    summary={'report':str(config.output/'report.json'),'cases':5,'qualification':result['qualification']}
    if config.two_hop is not None:summary['two_hop_queries']=12
    return summary

def publish_query_supply_chain_native(config,*,config_custody=None):
    """One fresh private finite publication, exact replay and unchanged five queries."""
    if type(config)is not SupplyChainNativeConfig:raise ValueError('Typed finite native configuration required')
    if config_custody is not None:
        if type(config_custody)is not InvocationFile:raise ValueError('Exact invocation custody required')
        config_custody.__post_init__();config_custody.renew(65536)
    paths_schema_owner=None
    if config.two_hop is not None:
        from ashlar.weft_paths_distribution import installed_paths_schema_bundle
        paths_schema_owner=installed_paths_schema_bundle
    jars=_runtime(config);pack=snapshot(config.pack,1024*1024)
    supply_chain_cases(pack,snapshot(config.model,1024*1024),snapshot(config.graph,1024*1024))
    config.output.mkdir(mode=0o700);lease=PrivateFiniteLease(config.output,create=True)
    spark=None;transport=None;source=None;result=None;primary=None
    count_bundle=None;path_bundle=None
    try:
        producer=HeldSupplyChainDataset(config.producer,config.model,config.graph,config.output/'public-dataset.json')
        source=FiniteSupplyChainSource(config.model,config.graph,current_dataset=producer)
        spark=_spark(config,jars);graph_columns=fixture_columns(RESOURCE_ROOT)
        columns={**graph_columns,'attempts':tuple((n,'STRING')for n in ('stream','batch_id','phase','request_digest','payload_json','payload_digest')),
            'manifest':tuple((n,'TIMESTAMP'if n=='recorded_at'else 'STRING')for n in FIELDS)}
        tables={r:'local.supplychainfinite.'+r for r in columns};targets=[]
        for role,schema in columns.items():
            path=config.output/role
            spark.sql('CREATE TABLE delta.`'+str(path)+'` ('+','.join(n+' '+f for n,f in schema)+') USING DELTA')
            targets.append(DeltaTarget(tables[role],path,spark.sql('DESCRIBE DETAIL delta.`'+str(path)+'`').first()['id']))
        context=object();policy=PrivatePolicy(context,tuple(targets))
        transport=LocalDeltaTransport.initialize(spark,config.output/'operations.sqlite','original-finite-supplychain',tuple(targets),policy,context=context,capacity=config.operation_capacity);policy.initializing=False
        driver=FiniteSupplyChainDriver(transport,policy,context,tables,graph_columns,source,'original-finite-supplychain-publication',current_admission=_current_lease(lease,context,tables,targets))
        request=_request(source.batch);_write(config.output/'request.json',(encoded(request)+'\n').encode());driver.prepare_original(request)
        backend=driver.backend();descriptor=publish_batch(backend,request['stream'],source.batch,predecessor=request['predecessor'],schema_revisions_json=request['schema_revisions_json'],context=context)
        before={t:transport.original_history(transport.targets[t])for t in descriptor.versions}
        replay=publish_batch(backend,request['stream'],source.batch,predecessor=request['predecessor'],schema_revisions_json=request['schema_revisions_json'],context=context)
        if replay!=descriptor or any(transport.original_history(transport.targets[t])!=h for t,h in before.items()):raise ValueError('Exact finite replay changed native graph publication')
        registry={'installation':'original-finite-supplychain','tables':tables,'targets':[{'table':t.table,'path':str(t.path),'uuid':t.uuid}for t in targets],
            'operation_capacity':None if config.operation_capacity is None else dict(config.operation_capacity),'publication_id':driver.publication_id}
        _write(config.output/'registry.json',(encoded(registry)+'\n').encode())
        aliases={tables[r]:'spark_catalog.supplychainfinite.'+r for r in GRAPH_ROLES}
        spark.sql('CREATE DATABASE IF NOT EXISTS supplychainfinite')
        for native,alias in aliases.items():spark.sql('CREATE TABLE '+quote_table_identifier(alias)+' USING DELTA LOCATION \''+str(transport.targets[native].path)+"'")
        # Reopen through the public read-only journal and actual committed phases.
        transport.close();transport=None
        transport=ReadOnlyTransport.open(spark,config.output/'operations.sqlite',registry['installation'],tuple(targets),policy,capacity=registry['operation_capacity'])
        reader=FiniteSupplyChainDriver(transport,policy,context,tables,graph_columns,source,registry['publication_id'],current_admission=_current_lease(lease,context,tables,targets));reader.restore_committed(request)
        if config.two_hop is not None:
            count_bundle=dict(installed_count_star_schema_bundle(config.compiler))
            path_bundle=dict(paths_schema_owner(config.two_hop.distribution))
        queried=_queries(config,reader,pack,aliases)
        result={'cases':queried['cases'],'manifest':dict(descriptor.raw),'source':source.metadata(),'producer':producer.metadata(),'exact_replay_unchanged':True,'reader_reopened_from_committed_phases':True,
            'qualification':'Fresh original finite immutable-file local Spark4/Delta4 publication and read-only reopened committed five-query execution after complete independent source bags/current lease holds. No PostgreSQL/outbox/protected ACK/UC/Truss/remote source authority.'}
        if config.two_hop is not None:
            result.update(two_hop=queried['two_hop'],two_hop_queries=12)
            result['qualification']='Fresh original finite immutable-file local publication/replay/read-only restored five authored and twelve two-hop query evidence; all required outer closing completed before report. Other six common graph cases and protected ACK/Truss/cloud remain separate.'
    except BaseException as error:primary=error
    callbacks=[]
    if transport is not None:callbacks.append(transport.close)
    if spark is not None:callbacks.append(spark.stop)
    if source is not None:callbacks.append(source.renew)
    callbacks.extend([lambda:_runtime(config),lease.renew,lease.close])
    def compiler_closing(paths,original,owner):
        if original is not None and dict(owner(paths))!=original:raise ValueError('Original compiler schema bundle changed')
    if config.two_hop is not None:
        callbacks.extend([lambda:compiler_closing(config.compiler,count_bundle,installed_count_star_schema_bundle),lambda:compiler_closing(config.two_hop.distribution,path_bundle,paths_schema_owner)])
    def pack_closing():
        if snapshot(config.pack,1048576)!=pack:raise ValueError('Closing original pack changed')
    callbacks.append(pack_closing)
    if config_custody is not None:callbacks.append(lambda:config_custody.renew(65536))
    finish(primary,callbacks)
    return _finish_report(config,result)

class FiniteRuntimeConfiguration(Protocol):
    output:Path
    jars:Path
    def __post_init__(self)->None:...

# Both finite compositions validate their own immutable configuration before SDK use.
def finite_runtime(config:FiniteRuntimeConfiguration)->list[Path]:
    """Validate the explicitly selected finite runtime and exact cached JARs."""
    return _runtime(config)

def open_finite_spark(config:FiniteRuntimeConfiguration,jars:Sequence[Path])->object:
    """Open one owned finite Spark session; the calling composition must stop it."""
    selected=finite_runtime(config)
    if type(jars)not in (list,tuple)or any(type(p)is not type(Path('/'))for p in jars)or list(jars)!=selected:raise ValueError('Exact current selected finite JAR sequence required')
    return _spark(config,selected)
