"""Original medical publication/replay composition under external supervision.

The shared publisher owns phases and protected ACK recovery. This module owns
configured resource intervals; it neither provisions nor implements a deadline.
"""
from contextlib import contextmanager
from dataclasses import fields, is_dataclass
import hashlib
import json
import os
from pathlib import Path
import stat
from types import MappingProxyType
from ashlar.attempt_store import DeltaAttemptStore
from ashlar.medical_source import build_transaction
from ashlar.outbox import publish_outbox_transaction
from ashlar.publisher import PublicationError
from ashlar.stored_publisher import StoredPublisherBackend
from .delta_custody import LocalDeltaTransport, encoded
from .driver import NativeDriver, PrivatePolicy, AttemptExecutor, CarrierPolicy, ManifestPort
from .evolution_verification_types import VerificationBounds
from .lifecycle import finish, owned_context
from .local_physical_carriers import original_local_materialization_columns, original_local_physical_carriers
from .medical_installation import OriginalMedicalInstallationLease
from .medical_producer import MedicalFile, HeldMedicalDataset
from .medical_publication_plan import MedicalPublicationPlan, FEED, STREAM
from .medical_preflight_timing import MedicalPreflightTiming
from .medical_failure_location import retain_medical_failure_location
from .medical_publication_sources import OriginalMedicalSourceOwner
from .medical_process_supervision import OriginalMedicalNativePhase
from .original_medical_installation_handoff import validate_original_medical_installation_handoff
from .original_medical_local_configuration import OriginalMedicalPublicationConfig, OriginalMedicalReplayConfig, OriginalMedicalRecoveryConfig
from .original_medical_local_sources import original_medical_local_source_ports
from .original_medical_native_session import original_medical_native_session
from .original_medical_publication import MedicalAdmission
from .original_medical_runtime import OriginalMedicalRuntimeCustody

ROLES = ('object_current', 'edge_current', 'tombstone', 'whole_source_history', 'attempts', 'manifest')


def _plain(value):
    if is_dataclass(value):return {field.name:_plain(getattr(value,field.name)) for field in fields(value)}
    if type(value) in (dict, MappingProxyType):return {key:_plain(item) for key,item in value.items()}
    if type(value) in (tuple,list):return [_plain(item) for item in value]
    if isinstance(value,Path):return str(value)
    return value


def _bounded(value, maximum):
    parts=[];size=0
    for piece in json.JSONEncoder(ensure_ascii=False,sort_keys=True,separators=(',',':')).iterencode(value):
        raw=piece.encode();size+=len(raw)
        if size>maximum:raise ValueError('Bounded original medical evidence required')
        parts.append(raw)
    return b''.join(parts)+b'\n'


def _identity(path,fd=None):
    named=path.lstat()
    if path.resolve()!=path or not stat.S_ISDIR(named.st_mode) or named.st_uid!=os.getuid() or stat.S_IMODE(named.st_mode)!=0o700:
        raise PermissionError('Canonical private medical output directory required')
    value=(named.st_dev,named.st_ino,named.st_uid,named.st_mode)
    if fd is not None:
        held=os.fstat(fd)
        if (held.st_dev,held.st_ino,held.st_uid,held.st_mode)!=value:raise PermissionError('Held medical output directory changed')
    return value


class _Output:
    def __init__(self,config):
        self.config=config;self.fd=None;self.original=_bounded(_plain(config),config.bounds.input_bytes)
        self.parent=_identity(config.output.parent);self.files=[];self.originals=[]

    def acquire(self):
        if self.config.output.exists() or self.config.output.is_symlink():raise ValueError('Fresh medical publication output required')
        self.config.output.mkdir(mode=0o700)
        self.identity=_identity(self.config.output)
        self.fd=os.open(self.config.output,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        self.renew()

    def renew(self):
        self.config.__post_init__()
        if _bounded(_plain(self.config),self.config.bounds.input_bytes)!=self.original or _identity(self.config.output.parent)!=self.parent:
            raise PermissionError('Original medical publication selection changed')
        if hasattr(self,'identity') and _identity(self.config.output,self.fd)!=self.identity:raise PermissionError('Medical publication output changed')
        finish(None,[lambda pin=pin,raw=raw: self._same(pin,raw) for pin,raw in zip(self.files,self.originals)])

    @staticmethod
    def _same(pin,raw):
        if pin.path.resolve()!=pin.path or pin.read()!=raw or pin.path.resolve()!=pin.path:
            raise PermissionError('Retained medical original evidence changed')

    def write(self,name,raw,maximum):
        self.renew()
        if type(raw)is not bytes or not 0<len(raw)<=maximum:raise ValueError('Finite medical original output required')
        path=self.config.output/name;fd=None;primary=None
        try:
            fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
            offset=0
            while offset<len(raw):
                count=os.write(fd,raw[offset:])
                if type(count)is not int or not 0<count<=len(raw)-offset:raise OSError('Complete medical original write required')
                offset+=count
            os.fsync(fd)
        except BaseException as error:primary=error
        finish(primary,([] if fd is None else [lambda:os.close(fd)]))
        os.fsync(self.fd)
        pin=MedicalFile(path,min(maximum,4194304));self.files.append(pin);self.originals.append(raw)
        self.renew()

    def close(self):
        if self.fd is not None:
            fd=self.fd;self.fd=None;os.close(fd)


class MedicalPublicationPolicy(PrivatePolicy):
    """Existing medical installation and immutable original plan, no new grants."""
    def __init__(self,context,selection,lease,renew):
        super().__init__(context,selection.targets);self.initializing=False;self.lease=lease;self.renew_inputs=renew;self.plan=None
    def renew(self):
        finish(None,[self.renew_inputs,lambda:self.lease.renew(self.context)]+([] if self.plan is None else [self.plan.renew]))
    @contextmanager
    def writer(self,*args):
        primary=None
        try:
            self.renew()
            with owned_context(super().writer(*args)):yield
        except BaseException as error:primary=error
        finish(primary,[self.renew])
    def admit(self,intent,context):
        self.renew();super().admit(intent,context);self.renew()
    def activate(self,plan):
        if type(plan)is not MedicalPublicationPlan or self.plan is not None:raise PermissionError('One original medical plan required')
        view=plan.snapshot();self.plan=plan
        self.active={key:view[source] for key,source in (
            ('request','request'),('steps','selected_steps'),('expected','expected'),('publication_id','publication_id'),
            ('previous_progress','previous_progress'),('progress','progress'),('previous_expected','previous_expected'),
            ('generated_steps','generated_steps'),('elisions','zero_match_elisions'),('recorded_at','recorded_at'))}
        self.renew()


class _MedicalPublisherDriver:
    """Delegate native effects; require medical observation even for settled ACK."""
    def __init__(self,driver,plan,bounds,expected=None,*,stages=None):
        if stages is not None and type(stages)is not MedicalPreflightTiming:raise ValueError('Exact medical timing owner required')
        self.driver=driver;self.plan=plan;self.bounds=bounds;self.expected=expected;self.stages=stages
    @contextmanager
    def writer(self,stream,context):
        with owned_context(self.driver.writer(stream,context)):yield
    def prepare_original_apply(self,request,context):
        self.plan.require_request(request)
        return self.driver.prepare_original_medical_apply(self.plan,context=context,bounds=self.bounds)
    def apply(self,request,context):return self.driver.apply(request,context)
    def recover_apply(self,request,context):
        action=lambda:self.driver.recover_apply(request,context)
        return action()if self.stages is None else self.stages.call('effect-recovery',action)
    def validate(self,*args):return self.driver.validate(*args)
    def _observe(self,request,descriptor,context):
        self.plan.require_request(request)
        if self.expected is not None and _bounded(dict(descriptor.raw),self.bounds.input_bytes)!=self.expected:
            raise PermissionError('Pinned original medical publication differs')
        self.driver.policy.active['manifest']=dict(descriptor.raw)
        return self.driver.observe_original_medical_publication(self.plan,descriptor,context=context,bounds=self.bounds)
    def observe_committed_original(self,request,artifact,descriptor,context):
        self._observe(request,descriptor,context)
    def acknowledge(self,request,descriptor,context):
        self._observe(request,descriptor,context)
        return self.driver.acknowledge(request,descriptor,context)


class _MedicalPreparedBackend(StoredPublisherBackend):
    """Keep exact source and effect intent durable before prepared is submitted."""
    def __init__(self,attempts,driver,manifest_factory,*,context,stages=None):
        if stages is not None and type(stages)is not MedicalPreflightTiming:raise ValueError('Exact medical timing owner required')
        super().__init__(attempts,driver,manifest_factory)
        self.medical_context=context;self.stages=stages

    @contextmanager
    def writer(self,stream,context):
        manager=super().writer(stream,context)
        with (owned_context(manager)if self.stages is None else self.stages.context('publisher-session','publisher-closing',manager)) as permit:yield permit

    def observe(self,stream,batch_id):
        action=lambda:super(_MedicalPreparedBackend,self).observe(stream,batch_id)
        return action()if self.stages is None else self.stages.call('attempt-observation',action)

    def prepare(self,stream,request):
        if self.observe(stream,request['batch_id']) is not None:
            raise PublicationError('Existing medical attempt cannot be prepared again')
        if self.driver.prepare_original_apply(dict(request),self.medical_context) is not None:
            raise PublicationError('Complete original medical preparation required')
        return super().prepare(stream,request)


class _MedicalReconciliationBackend(_MedicalPreparedBackend):
    """Only the original committing/committed attempt can use the new owner."""
    def __init__(self,*args,request,**kwargs):
        super().__init__(*args,**kwargs);self.request=dict(request)
    def observe(self,stream,batch_id):
        attempt=super().observe(stream,batch_id)
        if attempt is None or attempt.phase not in ('committing','committed'):
            raise PublicationError('Original committing or committed medical attempt required')
        if attempt.phase=='committed':
            actual=self.manifest_factory(self.request,self.medical_context).reconcile(
                dict(attempt.descriptor.raw),context=self.medical_context)
            if actual!=dict(attempt.descriptor.raw):raise PublicationError('Prospective committed manifest differs')
        return attempt
    def recover_commit(self,stream,request,result,context):
        return self.reconcile_commit(stream,request,result,context)


class _SettledBackend(StoredPublisherBackend):
    """Refuse incomplete originals before the shared publisher can recover effects."""
    def observe(self,stream,batch_id):
        attempt=super().observe(stream,batch_id)
        if attempt is None or attempt.phase!='committed' or attempt.descriptor is None:
            raise PublicationError('Existing complete committed medical original required')
        return attempt


def _pin(selection,maximum):
    pin=MedicalFile(selection.path,min(maximum,4194304));raw=pin.read()
    if len(raw)!=selection.bytes or hashlib.sha256(raw).hexdigest()!=selection.sha256:raise PermissionError('Exact selected medical original bytes required')
    return pin,raw


def _fresh(driver,selection,bounds):
    """Require the separately initialized six empty registered targets."""
    for name in ('local_plan','local_operation','local_source_plan','local_publication_artifact'):
        if driver.transport.db.execute('SELECT 1 FROM '+name+' LIMIT 1').fetchone():
            raise PermissionError('Fresh separately initialized medical publication journal required')
    schemas=original_local_physical_carriers()['carriers'];primary=None;total=0
    def role(target):
        nonlocal total
        history=driver.transport.original_history(target)
        if [row['version'] for row in history]!=[1,0]:raise PermissionError('Fresh original registered target versions required')
        snapshot,schema=driver.transport.bounded_snapshot(target,1,remaining_bytes=bounds.total_bytes-total)
        size=len(encoded(snapshot).encode())+len(encoded(schema).encode())
        total+=size
        if total>bounds.total_bytes or snapshot['rows'] or encoded(schema)!=encoded(json.loads(schemas[target.table.split('.')[-1]]['schemaJson'])):
            raise PermissionError('Complete empty generated medical native schemas required')
    try:
        for target in selection.targets:role(target)
    except BaseException as error:primary=error
    total=0
    finish(primary,[lambda target=target:role(target) for target in selection.targets])


def _original_transaction(source,batch,*,context,existing):
    if type(source)is not OriginalMedicalSourceOwner or type(existing)is not bool:
        raise ValueError('Exact original medical source route required')
    return (source.read_original if existing else source.append_original)(batch,context=context)


@contextmanager
def _inspection_lease_boundary(inspection):
    """Complete observer renewal after native closing, before lease release."""
    primary=None
    try:yield
    except BaseException as error:primary=error
    finish(primary,[]if inspection is None else[inspection.renew])


def _operate(config,replay,supervision=None,*,recovery=False,preflight_only=False,inspection=None,prospective=None):
    if supervision is not None and type(supervision)is not OriginalMedicalNativePhase:
        raise ValueError('Exact original medical native phase required')
    if type(replay)is not bool or type(recovery)is not bool or replay and recovery:
        raise ValueError('Exact distinct medical operation required')
    if type(preflight_only)is not bool or preflight_only and not recovery:raise ValueError('Distinct medical recovery preflight required')
    if inspection is not None:
        from .medical_process_inspection import MedicalProcessInspection
        from .medical_journal_inspection import MedicalJournalInspection
        if type(inspection)not in (MedicalProcessInspection,MedicalJournalInspection) or not recovery or preflight_only:
            raise ValueError('Distinct actual read-only commit inspection required')
    if prospective is not None:
        from .medical_prospective_manifest_configuration import OriginalMedicalProspectiveManifestConfig
        if type(prospective)is not OriginalMedicalProspectiveManifestConfig or inspection is None:
            raise ValueError('Exact prospective medical reconciliation selection required')
    wanted=OriginalMedicalRecoveryConfig if recovery else OriginalMedicalReplayConfig if replay else OriginalMedicalPublicationConfig
    if type(config)is not wanted:raise ValueError('Exact original medical operation configuration required')
    config.__post_init__()
    if preflight_only and config.bounds.native_seconds>60:raise ValueError('Medical preflight native ceiling is60seconds')
    context=object();output=_Output(config);primary=None;answer=None;pins=[];stages=None;inspection_files=None;inspection_original=None
    try:
        output.acquire()
        if inspection is not None:
            from .medical_inspection_selection import medical_inspection_selection_bytes,renew_medical_inspection_selection
            inspection_original=medical_inspection_selection_bytes(inspection.config)
        if recovery:
            stages=MedicalPreflightTiming(output.write)
        installation,raw=_pin(config.installation,min(config.bounds.input_bytes,config.bounds.receipt_bytes));pins.append((installation,raw))
        selection=validate_original_medical_installation_handoff(raw,config)
        expected_request=expected_publication=None
        if recovery:
            request,expected_request=_pin(config.request,config.bounds.input_bytes);pins.append((request,expected_request))
        if replay:
            request,expected_request=_pin(config.request,config.bounds.input_bytes);publication,expected_publication=_pin(config.publication,config.bounds.input_bytes)
            pins.extend(((request,expected_request),(publication,expected_publication)))
        def originals():finish(None,[output.renew]+[lambda pin=pin,raw=raw:output._same(pin,raw) for pin,raw in pins])
        runtime_owner=OriginalMedicalRuntimeCustody.open(config.runtime,config.inputs,config.bounds,config.producer)
        with (owned_context(runtime_owner)if stages is None else stages.context('runtime-custody','runtime-closing',runtime_owner)) as custody:
            model,graph,baseline=custody.input_bytes
            dataset=HeldMedicalDataset(config.producer,config.inputs.model.path,config.inputs.graph.path,config.inputs.baseline.path,config.output/'public-revalidation.json')
            batch,bindings=build_transaction(model,graph,baseline,source_system=FEED,binding_profile='ashlar-medical-development-bindings/0.2')
            admission=MedicalAdmission(batch,bindings,config.inputs.model.path,config.inputs.graph.path,config.inputs.baseline.path,dataset)
            with owned_context(OriginalMedicalInstallationLease.open(selection,context,journal_bytes=config.bounds.journal_bytes)) as lease:
                if inspection is not None:inspection.before(lease,context)
                with _inspection_lease_boundary(inspection), owned_context(original_medical_native_session(custody) if supervision is None else original_medical_native_session(custody,phase=supervision)) as spark:
                    bounds=VerificationBounds(min(config.bounds.input_bytes,262144),config.bounds.rows,config.bounds.cell_bytes,config.bounds.total_bytes,config.bounds.receipt_bytes,config.bounds.stream_bytes)
                    policy=MedicalPublicationPolicy(context,selection,lease,lambda:finish(None,[originals,custody.renew,dataset.renew]))
                    transport=LocalDeltaTransport(spark,selection.journal,selection.installation_id,selection.targets,policy,capacity=selection.capacity,verification_bounds=bounds)
                    driver=None;source=None;native_primary=None
                    try:
                        if inspection is not None:inspection.bind()
                        lease.verify(context,transport)
                        with owned_context(transport.bounded_observations(bounds)):
                            source=OriginalMedicalSourceOwner(original_medical_local_source_ports(config.source),lease)
                            tables={role:target.table for role,target in zip(ROLES,selection.targets)}
                            driver=NativeDriver.with_registered_evolution(transport,policy,context,tables,admission.changes,original_local_materialization_columns(),source_admission=admission,source_sessions=source.sources(),ack_factory=source.bind)
                            with owned_context(driver.writer(STREAM,context)):
                                if not replay and not recovery:_fresh(driver,selection,bounds)
                                transaction=_original_transaction(source,batch,context=context,existing=replay or recovery)
                            plan=MedicalPublicationPlan(admission,transaction,tables);view=plan.snapshot();policy.activate(plan)
                            driver.bind_original_medical_publication(plan,bounds=bounds)
                            request_raw=_bounded(view['request'],config.bounds.input_bytes-1)
                            if (replay or recovery) and request_raw!=expected_request:raise PermissionError('Pinned original medical request differs')
                            output.write('request.json',request_raw,config.bounds.input_bytes)
                            if recovery:
                                with owned_context(driver.writer(STREAM,context)):
                                    if stages is None:driver.preflight_original_medical_recovery(plan,context=context,bounds=bounds)
                                    else:driver.preflight_original_medical_recovery(plan,context=context,bounds=bounds,stages=stages)
                            if inspection is not None and prospective is None:
                                from .medical_commit_inspection import inspect_medical_commit
                                with owned_context(driver.writer(STREAM,context)):
                                    observed,proposal=inspect_medical_commit(driver,plan,source,context=context,
                                        bounds=bounds,installation=json.loads(raw))
                                inspection_files=(observed,proposal)
                                answer={'profile':'ashlar-original-medical-local-commit-inspection/0.1',
                                    'classification':observed['classification'],'closing':True,
                                    'proposal_sha256':observed['proposal_sha256']}
                            elif preflight_only:
                                answer={'profile':'ashlar-original-medical-local-recovery-preflight/0.1','installation_id':selection.installation_id,'request_sha256':hashlib.sha256(request_raw).hexdigest(),'preflight_completed':True,'closing':True,'stage_records':stages.records}
                            else:
                                target=transport.targets[tables['attempts']]
                                adapter=_MedicalPublisherDriver(driver,plan,bounds,expected_publication,stages=stages)
                                backend_type=_MedicalReconciliationBackend if prospective is not None else _SettledBackend if replay else _MedicalPreparedBackend
                                def manifest_factory(request,context):
                                    if prospective is None:return ManifestPort(driver,request)
                                    from .medical_prospective_manifest import MedicalProspectiveManifest
                                    plan.require_request(request)
                                    return MedicalProspectiveManifest(driver,plan,source,inspection,context=context,
                                        bounds=bounds,installation=json.loads(raw),configuration=prospective)
                                options={}if replay else {'context':context,'stages':stages}
                                if prospective is not None:options['request']=view['request']
                                backend=backend_type(DeltaAttemptStore(AttemptExecutor(driver),CarrierPolicy(driver,'attempts'),target.table,target.uuid),adapter,manifest_factory,**options)
                                histories=None
                                if replay:
                                    with owned_context(backend.writer(STREAM,context)):
                                        descriptor=backend.observe_committed_original(STREAM,view['request'],context=context)
                                        histories={target.table:transport.original_history(target) for target in selection.targets}
                                def publish():
                                    return publish_outbox_transaction(backend,STREAM,transaction,predecessor=view['request']['predecessor'],schema_revisions_json=view['request']['schema_revisions_json'],context=context)
                                descriptor=publish()if stages is None else stages.call('publication-apply',publish)
                                with owned_context(driver.writer(STREAM,context)):
                                    def observe():return driver.observe_original_medical_publication(plan,descriptor,context=context,bounds=bounds)
                                    observation=observe()if stages is None else stages.call('publication-observation',observe)
                                    if histories is not None:
                                        def history(target):
                                            if transport.original_history(target)!=histories[target.table]:raise PermissionError('Settled medical native history changed')
                                        finish(None,[lambda target=target:history(target) for target in selection.targets])
                                output.write('publication.json',_bounded(dict(descriptor.raw),bounds.input_bytes-1),bounds.input_bytes)
                                observed_raw=_bounded(observation,min(config.bounds.total_bytes,4194304)-1)
                                output.write('observation.json',observed_raw,min(config.bounds.total_bytes,4194304))
                                answer={'profile':'ashlar-original-medical-local-'+('prospective-manifest' if prospective is not None else 'recovery' if recovery else 'replay' if replay else 'publication')+'/0.1','installation_id':selection.installation_id,'request_sha256':hashlib.sha256(request_raw).hexdigest(),'publication':dict(descriptor.raw),'observation':{'path':str(config.output/'observation.json'),'bytes':len(observed_raw),'sha256':hashlib.sha256(observed_raw).hexdigest()},'closing':True}
                                if prospective is not None:
                                    answer['profile']='ashlar-original-medical-prospective-manifest/0.1'
                                    answer['authorization_sha256']=prospective.authorization.sha256
                    except BaseException as error:native_primary=error
                    def closing():
                        with owned_context(transport.bounded_observations(bounds)):
                            total=0
                            def native(target):
                                nonlocal total
                                history=transport.original_history(target)
                                if not history or type(history[0]['version'])is not int:raise PermissionError('Complete current medical native history required')
                                snapshot,schema=transport.bounded_snapshot(target,history[0]['version'],remaining_bytes=bounds.total_bytes-total)
                                total+=len(encoded(snapshot).encode())+len(encoded(schema).encode())
                                if total>bounds.total_bytes:raise PermissionError('Complete medical closing capture exceeds budget')
                            callbacks=[policy.renew,lambda:lease.verify(context,transport)]+[lambda target=target:native(target) for target in selection.targets]
                            if driver is not None:
                                with owned_context(driver.writer(STREAM,context)):
                                    finish(None,callbacks+[lambda:source.renew_original(batch,context=context)])
                            else:
                                finish(None,callbacks)
                    finish(native_primary,[(closing if stages is None else lambda:stages.call('composition-closing',closing)),transport.close]+([]if inspection is None else[inspection.closing]))
            finish(None,[originals,custody.renew if stages is None else lambda:stages.call('runtime-custody',custody.renew),dataset.renew])
        if inspection is not None:
            renew_medical_inspection_selection(inspection.config,inspection_original)
        if inspection is not None and prospective is None:
            observed,proposal=inspection_files
            from .medical_journal_inspection import MedicalJournalInspection
            observed['journal_observations'if type(inspection)is MedicalJournalInspection else'process_observations']=inspection.observations
            output.write('proposal.json',proposal,config.bounds.input_bytes)
            output.write('commit-observation.json',_bounded(observed,config.bounds.receipt_bytes-1),config.bounds.receipt_bytes)
            answer['proposal']=str(config.output/'proposal.json')
            answer['observation']=str(config.output/'commit-observation.json')
        if recovery:answer['stage_records']=stages.records
        output.write('receipt.json',_bounded(answer,config.bounds.receipt_bytes-1),config.bounds.receipt_bytes)
    except BaseException as error:primary=error
    finish(primary,([] if primary is None else [lambda:retain_medical_failure_location(output,primary)])+[output.renew,output.close])
    return answer


def publish_original_medical_local(config,*,supervision=None):
    """Publish the original medical batch through the shared durable publisher."""
    return _operate(config,False,supervision)


def replay_original_medical_local(config,*,supervision=None):
    """Reopen a committed original; no append or replacement native effects."""
    return _operate(config,True,supervision)


def recover_original_medical_local(config,*,supervision=None):
    """Resume retained original intent through shared native/publisher recovery."""
    return _operate(config,False,supervision,recovery=True)


def preflight_original_medical_recovery_local(config: OriginalMedicalRecoveryConfig,*,supervision=None) -> dict:
    """Check the same retained original under existing authority; never publish/ACK.

    The existing isolated supervisor enforces the selected <=60s native ceiling.
    Direct callers must supply equivalent supervision; timings are evidence only.
    """
    return _operate(config,False,supervision,recovery=True,preflight_only=True)


def operate_original_medical_commit_inspection(config,observer,*,supervision=None):
    """Reuse actual medical ownership and closing without constructing a publisher."""
    return _operate(config,False,supervision,recovery=True,inspection=observer)


def reconcile_original_medical_commit_local(config,*,supervision=None):
    """Explicit new authorization, original attempt ownership and guarded ACK."""
    from .medical_prospective_manifest_configuration import OriginalMedicalProspectiveManifestConfig
    from .medical_inspection_selection import create_medical_inspection
    if type(config)is not OriginalMedicalProspectiveManifestConfig:raise ValueError('Exact prospective medical configuration required')
    config.__post_init__();selected=config.inspection_selection()
    return _operate(selected.recovery,False,supervision,recovery=True,
        inspection=create_medical_inspection(selected),prospective=config)
