"""Original-ordinal singleton under a held local cooperative installation lease.

No PostgreSQL pin, remote source ACK, mutation, checkpoint write or latest fallback.
The runtime's actual current admission remains mandatory and independent of bags.
"""
from contextlib import contextmanager
import fcntl,json,os,stat
from ashlar.native import NativeBackend
from ashlar.singleton import read_singleton
from ashlar.manifest import manifest_pin_vector,bind_manifest_pins
from ashlar.attempt_store import DeltaAttemptStore,PHASES
from ashlar.stored_publisher import validate_applied_artifact
from ashlar.schema import decode_retained_json
from ashlar.effect_validation import validate_effect_snapshot
from .configured_source_local import ConfiguredLocalSource
from .configured_local_publication import ROLES
from .delta_custody import LocalDeltaTransport,encoded,sha
from .file_progress import JournaledJsonlProgress
from .managed_carriers import load_managed_carriers
from .managed_source_oracle import configured_prefix_carrier_rows
from .lifecycle import finish,owned_context


def _require(value):
    if not value:raise ValueError('Original configured local singleton refused')


def read_configured_local_singleton(transport,source,progress,*,ordinal,type_id,entity_id,tables,context,materialization_clock_us,current_admission):
    """Return an exact old-or-current object after original full-vector closure.

    The caller keeps the actual private installation lease/current-native owner
    alive. This owner additionally holds the existing publication writer lock,
    preventing participating local publishers from advancing during the read.
    """
    _require(type(transport)is LocalDeltaTransport and type(source)is ConfiguredLocalSource and type(progress)is JournaledJsonlProgress and source.context is context and callable(current_admission))
    _require(type(ordinal)is int and 1<=ordinal<=len(source.batches)and all(type(n)is int and -(2**63)<=n<2**63 for n in (type_id,entity_id)))
    _require(type_id in source.configuration.policy.types)
    _require(type(tables)is dict and set(tables)==set(ROLES)|{'attempts','manifest'}and len(set(tables.values()))==6)
    _require(getattr(progress,'read_only',False)is True)
    _require(progress.db is transport.db and progress.source==source.configuration.source and progress.source_sha256==source.metadata(context)['source_sha256']and progress.stream==source.stream and progress.feed==source.configuration.feed and progress.epoch==source.configuration.epoch)
    spec=source.spec(ordinal,context);request=spec.request();carriers=load_managed_carriers();columns={r:carriers.ordered_columns(r)for r in ROLES}
    expected=configured_prefix_carrier_rows(source.configuration,source.batches[:ordinal],materialization_clock_us=materialization_clock_us,carriers=carriers,maximum_rows=1000).rows()
    targets={r:transport.targets[tables[r]]for r in tables}
    original_targets=tuple((r,t.table,t.uuid,t.path)for r,t in targets.items());original_tables=encoded(tables)
    path=transport.journal_path.parent/'.configured-source-writer.lock';fd=None;held=False;primary=None;result=None
    descriptor=None;original_entries=None;original_phases=None
    def require():
        _require(held and source.context is context and encoded(tables)==original_tables and tuple((r,transport.targets[tables[r]].table,transport.targets[tables[r]].uuid,transport.targets[tables[r]].path)for r in tables)==original_targets)
        source.renew(context)
    def entries():
        require();values=progress.retained_entries();_require(ordinal<=len(values)<=len(source.batches))
        for i,value in enumerate(values,1):
            original=source.spec(i,context);_require(value.request()==original.request()and value.descriptor.publication_id==original.publication_id)
        return values
    class ReadExecutor:
        def query(self,sql,parameters):
            _require(sql.startswith(('SELECT ','DESCRIBE ','SHOW ')));return transport.query(sql,parameters)
    executor=ReadExecutor()
    class PhasePolicy:
        @contextmanager
        def writer(self,table,uuid,supplied):
            require();_require(supplied is context and(table,uuid)==(tables['attempts'],targets['attempts'].uuid));yield
        def admit(self,*args):raise PermissionError('Read-only phase observation')
    store=DeltaAttemptStore(executor,PhasePolicy(),tables['attempts'],targets['attempts'].uuid)
    def phases():
        require()
        with owned_context(store.session(context))as session:values=session.read(source.stream,request['batch_id'])
        _require(len(values)==5 and tuple(v.phase for v in values)==PHASES)
        for value in values:_require(decode_retained_json(value.payload_json.encode())['request']==request)
        payload=decode_retained_json(values[-1].payload_json.encode());artifact,original=validate_applied_artifact(payload['result_json'],request)
        _require(original==descriptor and decode_retained_json(payload['descriptor_json'].encode())==dict(descriptor.raw)and artifact['manifest']==dict(descriptor.raw))
        return values
    def current():
        require();_require(current_admission(source,request,None if descriptor is None else dict(descriptor.raw),context)is None);require()
    def closing_entries():_require(entries()==original_entries)
    def closing_phases():_require(phases()==original_phases)
    memo={'parity':False,'poisoned':False}
    def parity():
        require();_require(not memo['poisoned'])
        if not memo['parity']:
            try:finish(None,[lambda r=r:validate_effect_snapshot(executor,tables[r],targets[r].uuid,descriptor.versions[tables[r]],columns[r],expected[r])for r in ROLES])
            except BaseException:
                memo['poisoned']=True
                raise
            memo['parity']=True
    def lock_current():
        named=path.lstat();opened=os.fstat(fd);parent=path.parent.lstat()
        _require(stat.S_ISREG(named.st_mode)and named.st_uid==os.getuid()and not named.st_mode&0o022 and stat.S_ISDIR(parent.st_mode)and parent.st_uid==os.getuid()and not parent.st_mode&0o022)
        _require((named.st_dev,named.st_ino)==(opened.st_dev,opened.st_ino)==lock_identity and(parent.st_dev,parent.st_ino)==parent_identity)
    try:
        parent=path.parent.lstat();parent_identity=(parent.st_dev,parent.st_ino)
        _require(stat.S_ISDIR(parent.st_mode)and parent.st_uid==os.getuid()and not parent.st_mode&0o022)
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        item=os.fstat(fd);lock_identity=(item.st_dev,item.st_ino)
        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);held=True;lock_current();current()
        original_entries=entries();descriptor=original_entries[ordinal-1].descriptor
        _require(dict(descriptor.raw)['recorded_at']==materialization_clock_us and descriptor.validation_report.get('source_oracle_sha256')==sha(encoded(expected))and descriptor.validation_report.get('source_admission')==source.metadata(context))
        original_phases=phases();current()
        uuids={tables[r]:targets[r].uuid for r in ROLES};authority='local-owned-installation:'+transport.installation_id
        vector=manifest_pin_vector(dict(descriptor.raw),uuids,authority=authority)
        class Policy:
            def authorize(self,supplied,publication_id,requested):
                _require(supplied is context and publication_id==descriptor.publication_id and set(requested)==set(uuids));current()
            def validate_descriptor(self,value,supplied):
                _require(supplied is context and value==descriptor);current();parity()
            def validate_snapshot(self,table,uuid,version,actual_columns):
                role=table.rsplit('.',1)[1];_require(table in uuids and (uuid,version,tuple(actual_columns))==(uuids[table],descriptor.versions[table],columns[role]));current()
            def bind_descriptor(self,value,supplied_vector,supplied):
                _require(supplied is context and value==descriptor and supplied_vector==vector);bind_manifest_pins(value,vector,uuids,authority=authority);current()
            def authorize_row(self,value,table,row,supplied):
                self.bind_descriptor(value,vector,supplied);_require(table==tables['object_current'])
                matches=[r for r in expected['object_current']if(r['source_system'],r['type_id'],r['id'])==(source.configuration.policy.source_system,str(type_id),str(entity_id))]
                _require(len(matches)<=1 and ((row is None and not matches)or(row is not None and len(matches)==1 and dict(row)==matches[0])))
        class LocalPins:
            @contextmanager
            def hold(self,value,*,context):
                _require(value==vector and context is source.context);current();lock_current();yield value
        policy=Policy();backend=NativeBackend(executor,policy,tables['manifest'],targets['manifest'].uuid,{tables[r]:columns[r]for r in ROLES})
        result=read_singleton(executor,backend,LocalPins(),vector,policy,publication_id=descriptor.publication_id,table=tables['object_current'],kind='object',source=source.configuration.policy.source_system,type_id=type_id,entity_id=entity_id,context=context,supported_profiles=['ashlar-delta/0.3'],supported_revisions={source.configuration.schema_alias:[source.configuration.intake.document_revision]})
    except BaseException as exc:primary=exc
    callbacks=[]
    if held:
        callbacks=[current,lock_current]
        if original_entries is not None:callbacks.append(closing_entries)
        if original_phases is not None:callbacks.append(closing_phases)
    def release():
        nonlocal held
        memo.update(parity=False,poisoned=True);held=False
        if fd is not None:fcntl.flock(fd,fcntl.LOCK_UN)
    if fd is not None:callbacks.extend([release,lambda:os.close(fd)])
    finish(primary,callbacks)
    return dict(result)if result is not None else None
