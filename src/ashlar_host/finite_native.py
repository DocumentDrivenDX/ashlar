# SPDX-License-Identifier: MIT OR Apache-2.0
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

def _retained_file(path):
    import stat
    before=path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_uid!=__import__('os').getuid():raise ValueError('Owned retained finite file required')
    raw=snapshot(path,1048576);after=path.lstat()
    identity=(before.st_dev,before.st_ino)
    if identity!=(after.st_dev,after.st_ino):raise ValueError('Retained finite file replaced')
    return raw,identity

def _retained_json(raw):
    from .supply_chain_request import source_snapshot
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate retained finite member')
            result[key]=value
        return result
    def integer(text):
        if len(text)>20:raise ValueError('Bounded retained finite integer')
        return int(text)
    def refuse(value):raise ValueError('Unsupported retained finite numeric carrier')
    return source_snapshot(json.loads(raw,object_pairs_hook=pairs,parse_int=integer,parse_float=refuse,parse_constant=refuse))

def _finite_registry(config,registry):
    """Correspondence only; actual existing journal and native admission follow."""
    from .supply_chain_request import source_snapshot
    roles=GRAPH_ROLES|{'attempts','manifest'};installation='original-finite-'+config.producer.definition.name
    tables={role:'local.finitepack.'+role for role in roles}
    if type(registry)is not dict or set(registry)!={'installation','tables','targets','operation_capacity','publication_id'}:raise ValueError('Complete retained finite registry required')
    capacity=None if config.operation_capacity is None else dict(config.operation_capacity)
    if encoded(source_snapshot(registry['tables']))!=encoded(tables)or encoded(source_snapshot(registry['operation_capacity']))!=encoded(capacity)or registry['installation']!=installation or registry['publication_id']!=installation+'-publication':raise ValueError('Original finite registry configuration differs')
    if type(registry['targets'])is not list or len(registry['targets'])!=6:raise ValueError('Complete retained finite target inventory required')
    targets=[]
    for value in registry['targets']:
        if type(value)is not dict or set(value)!={'table','path','uuid'}or any(type(v)is not str for v in value.values()):raise ValueError('Exact retained finite target required')
        if value['table']not in tables.values():raise ValueError('Unknown retained finite role')
        role=value['table'].split('.')[-1]
        if value['path']!=str(config.output/role):raise ValueError('Original finite target location differs')
        if (config.output/role).is_symlink():raise ValueError('Symlink finite native target refused')
        targets.append(DeltaTarget(value['table'],config.output/role,value['uuid']))
    if len({t.table for t in targets})!=6 or len({t.uuid for t in targets})!=6:raise ValueError('Injective original finite target inventory required')
    return installation,tables,tuple(targets)

def _query_aliases(driver,*,register=False):
    from ashlar.native import quote_table_identifier
    if type(driver)is not FiniteFileDriver or type(driver.source)is not FinitePackSource or driver.manifest is None:raise ValueError('Actual restored finite pack driver required')
    driver.require(driver.context);driver.admit_current(driver.manifest)
    namespace='ashlarfinite_'+driver.source.definition.name.replace('-','_')
    aliases={driver.tables[role]:'spark_catalog.'+namespace+'.'+role for role in sorted(GRAPH_ROLES)}
    spark=driver.transport.spark
    if register:spark.sql('CREATE DATABASE IF NOT EXISTS spark_catalog.`'+namespace+'`')
    for native,alias in aliases.items():
        target=driver.transport.targets[native]
        if not spark.catalog.tableExists(alias):
            if not register:raise ValueError('Original finite query alias missing')
            spark.sql('CREATE TABLE '+quote_table_identifier(alias)+' USING DELTA LOCATION '+"'"+str(target.path)+"'")
        detail=spark.sql('DESCRIBE DETAIL '+quote_table_identifier(alias)).first().asDict()
        if type(detail.get('id'))is not str or detail['id']!=target.uuid or type(detail.get('location'))is not str or detail['location'].removeprefix('file:').rstrip('/')!=str(target.path):raise ValueError('Original finite query alias UUID/location differs')
    driver.admit_current(driver.manifest);return aliases

def finite_pack_query_aliases(driver):
    """Fresh copied aliases, admitted against actual current native identities."""
    if type(driver)is not FiniteFileDriver or driver.request is None:raise ValueError('Restored finite driver required')
    if driver.held:return _query_aliases(driver)
    with driver.writer(driver.request['stream'],driver.context):result=_query_aliases(driver)
    return result

from contextlib import contextmanager
@contextmanager
def open_finite_pack_publication(config,fresh_receipt_output):
    """Hold an existing committed installation and its ordinary query aliases.

    No canonical Delta writes, journal initialization or missing-custody repair.
    Catalog aliases register existing paths; current local ownership and native
    checks are mandatory throughout. A returned report supplies no authority.
    """
    if type(config)is not FinitePackNativeConfig:raise ValueError('Typed finite native configuration required')
    config.__post_init__();jars=finite_runtime(config)
    import os,stat
    if type(fresh_receipt_output)is not type(Path('/'))or not fresh_receipt_output.is_absolute()or fresh_receipt_output.exists()or any(p.is_symlink()for p in (fresh_receipt_output,*fresh_receipt_output.parents)):raise ValueError('Fresh absolute finite receipt path required')
    parent=fresh_receipt_output.parent
    def receipt_parent():
        current=parent.lstat()
        if not stat.S_ISDIR(current.st_mode)or current.st_uid!=os.getuid()or stat.S_IMODE(current.st_mode)!=0o700 or any(p.is_symlink()for p in (parent,*parent.parents)):raise ValueError('Owned private finite observation directory required')
        return current.st_dev,current.st_ino
    parent_identity=receipt_parent()
    protected=(config.output,config.jars,config.model,config.graph,config.pack,config.producer.source,config.producer.bun,config.producer.git)
    if any(parent==p or parent in p.parents or p in parent.parents for p in protected):raise ValueError('Separate finite observation directory required')
    if fresh_receipt_output in (config.model,config.graph,config.pack,config.output/'registry.json',config.output/'request.json',config.output/'operations.sqlite'):raise ValueError('Original finite custody cannot be replaced')
    def renew_receipt_parent():
        if receipt_parent()!=parent_identity:raise ValueError('Original finite observation directory replaced')
    paths=(config.model,config.graph,config.pack,config.output/'registry.json',config.output/'request.json')
    retained=tuple(_retained_file(path)for path in paths)
    registry=_retained_json(retained[3][0]);request=_retained_json(retained[4][0]);installation,tables,targets=_finite_registry(config,registry)
    facts=config.producer.definition.inputs(*(item[0]for item in retained[:3]))
    lease=PrivateFiniteLease(config.output,create=False);source=None;spark=None;transport=None;driver=None;primary=None;context=object()
    def renew_files():
        renew_receipt_parent()
        if tuple(_retained_file(path)for path in paths)!=retained:raise ValueError('Original retained finite input/custody changed')
    try:
        producer=HeldFiniteDataset(config.producer,config.model,config.graph,fresh_receipt_output)
        source=FinitePackSource(config.producer.definition,config.model,config.graph,producer,policy=FiniteInstallationSourcePolicy(lease,config.producer.definition,context),context=context)
        source.admit_request(request);spark=open_finite_spark(config,jars);policy=PrivatePolicy(context,targets);policy.initializing=False
        def admit(current,original,manifest,supplied):
            if current is not source or supplied is not context:raise PermissionError('Original finite held source/context required')
            lease.renew();renew_files();source.renew()
            if original is not None:source.admit_request(original)
            if manifest is not None and (manifest['schema_revisions_json']!=encoded({facts['source_system']:facts['model_sha256']})or set(json.loads(manifest['table_versions_json']))!={tables[r]for r in GRAPH_ROLES}):raise PermissionError('Complete original finite publication vector required')
            lease.renew()
        transport=ReadOnlyTransport.open(spark,config.output/'operations.sqlite',installation,targets,policy,capacity=config.operation_capacity)
        driver=FiniteFileDriver(transport,policy,context,tables,fixture_columns(RESOURCE_ROOT),source,registry['publication_id'],current_admission=admit)
        driver.restore_committed(request);driver.capture_committed()
        with driver.writer(request['stream'],context):_query_aliases(driver,register=True)
        yield driver
    except BaseException as error:primary=error
    callbacks=[]
    if driver is not None and driver.manifest is not None:
        callbacks.extend([lambda:finite_pack_query_aliases(driver),driver.capture_committed])
    if source is not None:callbacks.append(source.renew)
    if transport is not None:callbacks.append(transport.close)
    if spark is not None:callbacks.append(spark.stop)
    callbacks.extend([lambda:finite_runtime(config),renew_files,lease.renew,lease.close])
    finish(primary,callbacks)
