"""Explicit original publication on a separately initialized local installation."""
from contextlib import contextmanager
from dataclasses import dataclass,field
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
from types import MappingProxyType
from typing import Callable
from ashlar.apply import empty_state
from ashlar.attempt_store import DeltaAttemptStore
from ashlar.outbox import publish_outbox_transaction
from ashlar.stored_publisher import StoredPublisherBackend
from .commerce_publication_sources import OriginalCommerceSourcePorts,OriginalCommerceSourceOwner,FEED
from .commerce_admission import CommerceAdmission,original_commerce_oracle
from .config import ProducerConfig
from .delta_custody import LocalDeltaTransport,DeltaTarget,encoded,operation_capacity
from .driver import NativeDriver,PrivatePolicy,ROOT,CLOCK,AttemptExecutor,CarrierPolicy,ManifestPort,local_effect_plan,request_for,progress_union
from .graph_sql import graph_sql_plan
from .lifecycle import finish,owned_context
from .schema_rows import fixture_columns
from .source import original_inputs,recompute_dataset
from .source_identity import build_transaction,SOURCE_SHA,GRAPH_SHA
from .evolution_producer import snapshot
from .exact_report import report_body

ROLES={'attempts','manifest','object_current','edge_current','tombstone','whole_source_history'}

@dataclass(frozen=True)
class OriginalCommercePublicationConfig:
    output:Path
    model:Path
    graph:Path
    jars:Path
    producer:ProducerConfig
    def __post_init__(self):
        if any(type(p) is not type(Path()) or not p.is_absolute() for p in (self.output,self.model,self.graph,self.jars)) or type(self.producer) is not ProducerConfig:
            raise ValueError('Exact explicit original publication inputs required')
        self.producer.__post_init__()

def identity(path,directory=False):
    if type(path) is not type(Path()) or path.resolve()!=path:raise PermissionError('Original canonical owner required')
    s=path.lstat()
    if (not (stat.S_ISDIR(s.st_mode) if directory else stat.S_ISREG(s.st_mode)) or s.st_uid!=os.getuid() or s.st_mode&0o022):
        raise PermissionError('Original private owner required')
    return s.st_dev,s.st_ino

class OriginalCommerceInstallationLease:
    """Actual POSIX cooperating-operator lease plus complete existing registry."""
    def __init__(self,operator,context,fd,lock):
        self.operator=operator;self.context=context;self.fd=fd;self.lock=lock;self.closed=False
        self.parent=identity(operator.journal.parent,True);self.journal=identity(operator.journal)
        self.lock_identity=identity(lock)
        if any('`' in str(x.path) or '\x00' in str(x.path) for x in operator.targets):raise PermissionError('Closed native path spelling required')
        self.registry=encoded({'installation':operator.installation_id,'targets':[{'table':x.table,'path':str(x.path),'uuid':x.uuid} for x in sorted(operator.targets,key=lambda x:x.table)],**({} if operator.capacity is None else {'operation_capacity':dict(operator.capacity)})})
        self.reservation=encoded({'profile':'ashlar-private-native-installation-reservation/0.1','registry':json.loads(self.registry),'original_journal_path':str(operator.journal)})+'\n'
        self.original_targets=tuple((x.table,str(x.path),x.uuid) for x in sorted(operator.targets,key=lambda x:x.table))
        self.target_identities=tuple((x.path,identity(x.path,True)) for x in operator.targets)
    def renew(self,context):
        if type(self.operator) is not OriginalCommercePublicationOperator:raise PermissionError('Exact installed owner required')
        self.operator.__post_init__()
        if self.closed or context is not self.context or identity(self.operator.journal.parent,True)!=self.parent or identity(self.operator.journal)!=self.journal or identity(self.lock)!=self.lock_identity:
            raise PermissionError('Original held installation lease changed')
        s=os.fstat(self.fd)
        if (s.st_dev,s.st_ino)!=self.lock_identity:raise PermissionError('Original lock descriptor changed')
        for path,original in self.target_identities:
            if identity(path,True)!=original:raise PermissionError('Original native target directory changed')
            p=path/'.ashlar-local-installation-reservation.json'
            if p.is_symlink() or snapshot(p,4194304)!=self.reservation.encode():raise PermissionError('Original complete target reservation changed')
    def verify(self,context,transport):
        self.renew(context)
        if type(transport) is not LocalDeltaTransport or transport.journal_path!=self.operator.journal or transport.reservation!=self.reservation:
            raise PermissionError('Actual original existing transport required')
        actual=tuple((x.table,str(x.path),x.uuid) for x in sorted(transport.targets.values(),key=lambda x:x.table))
        expected=tuple((x.table,str(x.path),x.uuid) for x in sorted(self.operator.targets,key=lambda x:x.table))
        if actual!=self.original_targets or expected!=self.original_targets or transport.db.execute('SELECT original FROM local_installation WHERE id=1').fetchall()!=[(self.registry,)]:
            raise PermissionError('Complete original native registry changed')
    @classmethod
    @contextmanager
    def open(cls,operator,context):
        if type(operator) is not OriginalCommercePublicationOperator:raise ValueError('Exact original publication operator required')
        operator.__post_init__();identity(operator.journal.parent,True);identity(operator.journal)
        lock=operator.journal.parent/'.evolution-operator.lock'
        fd=os.open(lock,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600);owner=None;primary=None
        try:
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);owner=cls(operator,context,fd,lock);owner.renew(context);yield owner
        except BaseException as error:primary=error
        def close():
            if owner is not None:owner.closed=True
            os.close(fd)
        finish(primary,([lambda:owner.renew(context)] if owner is not None else [])+[close])

@dataclass(frozen=True,repr=False)
class OriginalCommercePublicationOperator:
    installation_id:str
    journal:Path
    targets:tuple
    ports:OriginalCommerceSourcePorts
    capacity:object
    native_session:Callable=field(repr=False,compare=False)
    def __post_init__(self):
        import re
        if type(self.installation_id) is not str or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9:_-]{0,1023}',self.installation_id) or type(self.journal) is not type(Path()) or not self.journal.is_absolute() or type(self.targets) is not tuple or len(self.targets)!=6 or any(type(x)is not DeltaTarget or type(x.table)is not str or type(x.uuid)is not str or type(x.path)is not type(Path()) for x in self.targets) or type(self.ports)is not OriginalCommerceSourcePorts or not callable(self.native_session):
            raise ValueError('Complete existing original installation and ordinary ports required')
        if {x.table.split('.')[-1] for x in self.targets}!=ROLES or len({x.table for x in self.targets})!=6 or len({x.path for x in self.targets})!=6:
            raise ValueError('Complete six distinct original native tables required')
        self.ports.__post_init__();object.__setattr__(self,'targets',tuple(DeltaTarget(x.table,x.path,x.uuid) for x in self.targets))
        if self.capacity is not None and (type(self.capacity) not in (dict,MappingProxyType) or any(type(k)is not str or type(v)not in (str,int) for k,v in self.capacity.items())):
            raise ValueError('Exact current installed capacity required')
        object.__setattr__(self,'capacity',operation_capacity(self.capacity))

class OriginalCommerceNativePolicy(PrivatePolicy):
    def __init__(self,context,targets,lease):
        super().__init__(context,targets);self.initializing=False;self.lease=lease
    @contextmanager
    def writer(self,*args):
        self.lease.renew(args[-1]);primary=None
        try:
            with super().writer(*args):yield
        except BaseException as error:primary=error
        finish(primary,[lambda:self.lease.renew(args[-1])])
    def admit(self,intent,context):
        self.lease.renew(context);super().admit(intent,context);self.lease.renew(context)


def require_fresh_original_native(spark,driver,targets):
    if type(driver) is not NativeDriver or type(targets) is not tuple or len(targets)!=6 or any(type(x)is not DeltaTarget for x in targets):
        raise PermissionError('Exact complete original native owner required')
    for target in targets:
        rows=spark.sql('SELECT COUNT(*) AS n FROM delta.`'+str(target.path)+'`').collect()
        if len(rows)!=1 or type(rows[0]['n']) is not int or rows[0]['n']!=0:
            raise PermissionError('Complete fresh empty native installation required')
        history=driver.transport.original_history(target);version=history[0]['version']
        if type(version) is not int or version<0:raise PermissionError('Exact observed native version required')
        snapshot=driver.inspect_snapshot(target.table,version)
        from ashlar.publication import Snapshot
        if type(snapshot) is not Snapshot or snapshot.table!=target.table or type(snapshot.uuid)is not str or snapshot.uuid!=target.uuid or type(snapshot.version)is not int or snapshot.version!=version:
            raise PermissionError('Original actual native snapshot differs')

@contextmanager
def original_publication_custody(config,admission):
    from .commerce import runtime_paths
    primary=None
    try:yield
    except BaseException as error:primary=error
    finish(primary,[admission.metadata,lambda:original_inputs(config.model,config.graph),
        lambda:runtime_paths(config,True,require_fresh=False)])

def write_observation(path,raw):
    if type(raw) is not bytes or len(raw)>16*1024*1024:raise ValueError('Bounded exact observation required')
    with owned_context(path.open('xb')) as stream:
        if stream.write(raw)!=len(raw):raise OSError('Incomplete observation')
        stream.flush();os.fsync(stream.fileno())

def close_original_publication(primary,owner,driver,context,lease,transport):
    def renew_source():
        with owned_context(driver.writer('private-original-commerce',context)):
            owner.renew_scope(context)
    callbacks=([renew_source] if driver is not None else [])+[lambda:lease.verify(context,transport),transport.close]
    finish(primary,callbacks)

def publish_original_commerce(config,*,operator):
    """Publish/ACK exact original bytes, never initialize or discover resources."""
    if type(config) is not OriginalCommercePublicationConfig or type(operator) is not OriginalCommercePublicationOperator:
        raise ValueError('Exact original public publication configuration required')
    config.__post_init__();operator.__post_init__()
    from .commerce import runtime_paths
    runtime_paths(config,True)
    model,graph=original_inputs(config.model,config.graph)
    if config.output.exists() or config.output.is_symlink():raise ValueError('Fresh observation directory required')
    config.output.mkdir(mode=0o700)
    batch,bindings=build_transaction(model,graph,source_system=FEED)
    paths=[config.output/n for n in ('original-ontology.json','original-graph.json','development-bindings.json','public-dataset.json')]
    write_observation(paths[0],model);write_observation(paths[1],graph);write_observation(paths[2],(encoded(bindings)+'\n').encode())
    producer=recompute_dataset(config.producer,config.model,config.graph,paths[3])
    raw=batch.begin+b''.join(x.raw for x in batch.records)+batch.commit
    write_observation(config.output/'source.jsonl',raw)
    admission=CommerceAdmission(batch,bindings,*paths);context=object();report=None;primary=None
    with original_publication_custody(config,admission):
        with owned_context(OriginalCommerceInstallationLease.open(operator,context)) as lease:
            with owned_context(operator.native_session()) as spark:
                if type(spark.version) is not str or spark.version!='3.5.3':raise ValueError('Selected Spark3.5.3 publication required')
                policy=OriginalCommerceNativePolicy(context,operator.targets,lease)
                transport=LocalDeltaTransport(spark,operator.journal,operator.installation_id,operator.targets,policy,capacity=operator.capacity)
                driver=None;owner=None
                try:
                    lease.verify(context,transport)
                    for table in ('local_plan','local_operation','local_source_plan','local_publication_artifact'):
                        if transport.db.execute('SELECT 1 FROM '+table+' LIMIT 1').fetchone():raise PermissionError('Fresh separately initialized native journal required')
                    columns=fixture_columns(ROOT);tables={x.table.split('.')[-1]:x.table for x in operator.targets}
                    owner=OriginalCommerceSourceOwner(operator.ports,lease)
                    driver=NativeDriver.with_registered_evolution(transport,policy,context,tables,admission.changes,columns,source_admission=admission,source_sessions=owner.sources(),ack_factory=owner.bind)
                    require_fresh_original_native(spark,driver,operator.targets)
                    with driver.writer('private-original-commerce',context):transaction=owner.append_original(batch,context=context)
                    graph_tables={role:tables[role] for role in columns};_,generated=graph_sql_plan(empty_state(),batch,graph_tables,materialized_at=CLOCK,schema_policy=driver.schema_admit)
                    empty={role:[] for role in columns};steps,elisions=local_effect_plan(generated,empty,graph_tables);expected=original_commerce_oracle(model,graph,bindings,batch,columns)
                    request=request_for('private-original-commerce',transaction,'explicit-original-commerce-origin',{FEED:SOURCE_SHA});checkpoint=json.loads(request['source_checkpoint_json'])
                    policy.active={'request':request,'steps':steps,'expected':expected,'publication_id':'original-commerce-publication-1','previous_progress':{},'progress':progress_union({},checkpoint),'previous_expected':empty,'generated_steps':generated,'elisions':elisions}
                    target=transport.targets[tables['attempts']]
                    backend=StoredPublisherBackend(DeltaAttemptStore(AttemptExecutor(driver),CarrierPolicy(driver,'attempts'),target.table,target.uuid),driver,lambda original,supplied:ManifestPort(driver,original))
                    descriptor=publish_outbox_transaction(backend,'private-original-commerce',transaction,predecessor=request['predecessor'],schema_revisions_json=request['schema_revisions_json'],context=context)
                    history={table:transport.original_history(transport.targets[table]) for table in descriptor.versions}
                    repeated=publish_outbox_transaction(backend,'private-original-commerce',transaction,predecessor=request['predecessor'],schema_revisions_json=request['schema_revisions_json'],context=context)
                    if repeated!=descriptor or any(transport.original_history(transport.targets[t])!=history[t] for t in history):raise ValueError('Exact replay changed publication')
                    r=operator.ports.registration
                    report={'format':'ashlar-original-commerce-publication/0.1','public_umf_revision':producer['umf_pin'],'public_dataset_receipt_sha256':hashlib.sha256(paths[3].read_bytes()).hexdigest(),'original_model_sha256':SOURCE_SHA,'original_graph_sha256':GRAPH_SHA,'bindings_sha256':hashlib.sha256(paths[2].read_bytes()).hexdigest(),'source_transaction_sha256':hashlib.sha256(raw).hexdigest(),'native_manifest':dict(descriptor.raw),'table_registry':[{'table':x.table,'uuid':x.uuid,'path':str(x.path)} for x in operator.targets],'complete_original_oracle':expected,'record_count':11,'edge_count':10,'history_count':21,'protected_ack_scope':r.source.metadata()['scope'],'source_schema':r.source.source_schema,'source_signature_sha256':r.source.source_signature_sha256,'source_role':r.source_role,'ack_role':r.ack_role,'source_ports_profile':'ashlar-explicit-original-commerce-source/0.1','original_journal_path':str(operator.journal),'installation_id':operator.installation_id,'exact_replay_unchanged':True,'qualification':'Explicit isolated original-commerce development publication; no Truss/production authority.'}
                    lease.verify(context,transport)
                except BaseException as error:primary=error
                close_original_publication(primary,owner,driver,context,lease,transport)
    # Report stays provisional until all native/lease/session cleanup finishes.
    write_observation(config.output/'report.json',report_body(report,16*1024*1024))
    return report
