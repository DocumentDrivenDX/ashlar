"""Original closed finite packs to six native Delta tables and committed reopen.

Explicit fresh private installation only; no remote source, UC or protected ACK
admission. The owned local lease and actual native checks remain mandatory.
"""
from dataclasses import dataclass
from pathlib import Path
import datetime,hashlib,json
from ashlar.publisher import publish_batch
from ashlar.staging import batch_row
from ashlar.manifest import FIELDS
from .finite_dataset import FiniteDatasetConfig,HeldFiniteDataset,FinitePackSource
from .finite_publication import FiniteFileDriver,GRAPH_ROLES
from .supply_chain_native import PrivateFiniteLease,finite_runtime,open_finite_spark
from .delta_custody import LocalDeltaTransport,DeltaTarget,operation_capacity,encoded
from .driver import PrivatePolicy
from .publication_reader import ReadOnlyTransport
from .schema_rows import fixture_columns
from .resources import RESOURCE_ROOT
from .evolution_producer import snapshot
from .lifecycle import finish,owned_context

@dataclass(frozen=True)
class FinitePackNativeConfig:
    output:Path
    jars:Path
    pack:Path
    model:Path
    graph:Path
    producer:FiniteDatasetConfig
    operation_capacity:object=None
    def __post_init__(self):
        for p in (self.output,self.jars,self.pack,self.model,self.graph):
            if type(p)is not type(Path('/'))or not p.is_absolute()or any(q.is_symlink()for q in (p,*p.parents))or any(c in str(p)for c in ('`',"'",'\\','\x00','\n','\r')):raise ValueError('Exact closed absolute finite native path required')
        if type(self.producer)is not FiniteDatasetConfig:raise ValueError('Owned compact public producer required')
        self.producer.__post_init__();object.__setattr__(self,'operation_capacity',operation_capacity(self.operation_capacity))

@dataclass(frozen=True,init=False)
class FiniteInstallationSourcePolicy:
    """Actually held private local finite-source ownership, not a receipt grant."""
    lease:PrivateFiniteLease
    context:object
    original:str
    def __init__(self,lease,definition,context):
        from .finite_pack import FinitePackDefinition
        if type(lease)is not PrivateFiniteLease or type(definition)is not FinitePackDefinition:raise ValueError('Owned actual finite lease and definition required')
        lease.renew();object.__setattr__(self,'lease',lease);object.__setattr__(self,'context',context);object.__setattr__(self,'original',encoded(definition.facts()))
    def admit_source(self,facts,context):
        if context is not self.context or encoded(dict(facts))!=self.original:raise PermissionError('Original finite installation/source context required')
        self.lease.renew()

def _write(path,raw):
    with owned_context(path.open('xb'))as stream:
        if stream.write(raw)!=len(raw):raise ValueError('Whole original finite artifact required')

def finite_pack_request(source):
    if type(source)is not FinitePackSource:raise ValueError('Owned original finite source required')
    facts=source.metadata();row=batch_row(source.batch)
    request={'stream':'original-finite-'+source.definition.name,'batch_id':source.batch.batch_id,'predecessor':'explicit-finite-'+source.definition.name+'-origin',
        'schema_revisions_json':encoded({facts['source_system']:facts['model_sha256']}),'source_batch_json':row['batch_json'],'source_batch_digest':row['batch_digest']}
    request['request_digest']=hashlib.sha256(encoded(request).encode()).hexdigest();source.admit_request(request);return request

def native_history_evidence(history):
    """Copy selected native history cells with explicit timestamp carriers.

    Spark's selected session timezone is UTC. A naive Python timestamp remains
    explicitly naive; it is not rewritten into an inferred timezone or string.
    This report representation never replaces values used for native replay checks.
    """
    count=0;active=set()
    def copy(value,depth):
        nonlocal count
        count+=1
        if count>100000 or depth>128:raise ValueError('Bounded native history evidence required')
        if type(value)is datetime.datetime:
            if value.tzinfo is not None and value.tzinfo is not datetime.timezone.utc:raise ValueError('Unsupported native history timestamp timezone')
            return {'profile':'ashlar-native-history-timestamp/0.1','native_type':'TIMESTAMP','session_timezone':'UTC',
                'python_iso8601':value.isoformat(timespec='microseconds'),'python_tzinfo':None if value.tzinfo is None else 'UTC','fold':value.fold}
        if type(value)in (type(None),bool):return value
        if type(value)is int:
            if not -(2**63)<=value<2**63:raise ValueError('Native history integer outside selected BIGINT carrier')
            return value
        if type(value)is str:
            if len(value)>4*1024*1024:raise ValueError('Bounded native history text required')
            return value
        if type(value)not in (dict,list):raise ValueError('Unsupported native history carrier')
        identity=id(value)
        if identity in active:raise ValueError('Cyclic native history evidence refused')
        active.add(identity)
        try:
            if type(value)is list:return [copy(item,depth+1)for item in value]
            if any(type(key)is not str for key in value):raise ValueError('Exact native history member names required')
            return {copy(key,depth+1):copy(item,depth+1)for key,item in value.items()}
        finally:active.remove(identity)
    return copy(history,0)

def publish_finite_pack_native(config):
    """Fresh publication, exact no-effect replay and read-only committed restore."""
    if type(config)is not FinitePackNativeConfig:raise ValueError('Typed finite native configuration required')
    jars=finite_runtime(config);originals=tuple(snapshot(p,1048576)for p in (config.model,config.graph,config.pack))
    facts=config.producer.definition.inputs(*originals)
    config.output.mkdir(mode=0o700);lease=PrivateFiniteLease(config.output,create=True)
    source=None;spark=None;transport=None;primary=None;result=None;context=object()
    try:
        producer=HeldFiniteDataset(config.producer,config.model,config.graph,config.output/'public-dataset.json')
        source=FinitePackSource(config.producer.definition,config.model,config.graph,producer,policy=FiniteInstallationSourcePolicy(lease,config.producer.definition,context),context=context)
        spark=open_finite_spark(config,jars);graph_columns=fixture_columns(RESOURCE_ROOT)
        columns={**graph_columns,'attempts':tuple((n,'STRING')for n in ('stream','batch_id','phase','request_digest','payload_json','payload_digest')),
            'manifest':tuple((n,'TIMESTAMP'if n=='recorded_at'else 'STRING')for n in FIELDS)}
        tables={r:'local.finitepack.'+r for r in columns};targets=[]
        for role,schema in columns.items():
            path=config.output/role
            spark.sql('CREATE TABLE delta.`'+str(path)+'` ('+','.join(n+' '+f for n,f in schema)+') USING DELTA')
            targets.append(DeltaTarget(tables[role],path,spark.sql('DESCRIBE DETAIL delta.`'+str(path)+'`').first()['id']))
        targets=tuple(targets);policy=PrivatePolicy(context,targets)
        original_registry=encoded([{'table':t.table,'path':str(t.path),'uuid':t.uuid}for t in targets])
        def admit(current,request,manifest,supplied):
            if supplied is not context or current is not source:raise PermissionError('Original finite current source/installation required')
            lease.renew();source.renew()
            if request is not None:source.admit_request(request)
            if manifest is not None and (manifest['schema_revisions_json']!=encoded({facts['source_system']:facts['model_sha256']})or set(json.loads(manifest['table_versions_json']))!={tables[r]for r in GRAPH_ROLES}):raise PermissionError('Complete original finite source/native vector required')
            if encoded([{'table':t.table,'path':str(t.path),'uuid':t.uuid}for t in targets])!=original_registry:raise PermissionError('Original finite registry changed')
            lease.renew()
        installation='original-finite-'+config.producer.definition.name
        transport=LocalDeltaTransport.initialize(spark,config.output/'operations.sqlite',installation,targets,policy,context=context,capacity=config.operation_capacity);policy.initializing=False
        driver=FiniteFileDriver(transport,policy,context,tables,graph_columns,source,installation+'-publication',current_admission=admit)
        request=finite_pack_request(source);_write(config.output/'request.json',(encoded(request)+'\n').encode());driver.prepare_original(request)
        backend=driver.backend();descriptor=publish_batch(backend,request['stream'],source.batch,predecessor=request['predecessor'],schema_revisions_json=request['schema_revisions_json'],context=context)
        histories={t:transport.original_history(transport.targets[t])for t in descriptor.versions}
        replay=publish_batch(backend,request['stream'],source.batch,predecessor=request['predecessor'],schema_revisions_json=request['schema_revisions_json'],context=context)
        if replay!=descriptor or any(transport.original_history(transport.targets[t])!=h for t,h in histories.items()):raise ValueError('Original finite exact replay changed native history')
        registry={'installation':installation,'tables':tables,'targets':json.loads(original_registry),'operation_capacity':None if config.operation_capacity is None else dict(config.operation_capacity),'publication_id':driver.publication_id}
        _write(config.output/'registry.json',(encoded(registry)+'\n').encode())
        transport.close();transport=None
        transport=ReadOnlyTransport.open(spark,config.output/'operations.sqlite',installation,targets,policy,capacity=registry['operation_capacity'])
        reader=FiniteFileDriver(transport,policy,context,tables,graph_columns,source,driver.publication_id,current_admission=admit);restored=reader.restore_committed(request)
        if restored!=descriptor:raise ValueError('Original committed finite restore differs')
        captured=reader.capture_committed()
        result={**captured,'source':source.metadata(),'manifest':dict(restored.raw),'native_histories':native_history_evidence(histories),'registry':registry,'qualification':'Original finite local Delta publication/replay/read-only committed reopen only; no SQL execution/remote-source/Unity Catalog/protected ACK claim.'}
    except BaseException as error:primary=error
    callbacks=[]
    if source is not None:callbacks.append(source.renew)
    if transport is not None:callbacks.append(transport.close)
    if spark is not None:callbacks.append(spark.stop)
    callbacks.append(lambda:finite_runtime(config))
    if source is not None:callbacks.append(source.renew)
    callbacks.extend([lease.renew,lease.close]);finish(primary,callbacks)
    if tuple(snapshot(p,1048576)for p in (config.model,config.graph,config.pack))!=originals:raise ValueError('Closing original finite inputs changed')
    raw=bytearray()
    for chunk in json.JSONEncoder(separators=(',',':'),ensure_ascii=True).iterencode(result):
        piece=chunk.encode()
        if len(raw)+len(piece)+1>64*1024*1024:raise ValueError('Complete finite report bound')
        raw.extend(piece)
    raw.extend(b'\n');_write(config.output/'report.json',raw);return result
