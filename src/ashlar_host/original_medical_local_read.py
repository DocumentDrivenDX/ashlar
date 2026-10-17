"""Configured existing medical publication SELECT reopen; never a writer owner."""
from dataclasses import fields,is_dataclass
from types import MappingProxyType
import hashlib,json,os,stat
from pathlib import Path
from ashlar.medical_source import build_transaction
from ashlar.outbox import OutboxTransaction
from ashlar.manifest import manifest_pin_vector
from ashlar.publication import resolve_publication
from .delta_custody import LocalDeltaTransport,encoded
from .driver import NativeDriver
from .evolution_verification_types import VerificationBounds
from .lifecycle import finish,owned_context,owned_connection
from .local_physical_carriers import original_local_materialization_columns
from .medical_installation import require_medical_journal_budget
from .medical_producer import HeldMedicalDataset,MedicalFile,decode_medical
from .medical_publication_plan import MedicalPublicationPlan,FEED,ROLES
from .medical_process_supervision import OriginalMedicalNativePhase
from .original_medical_installation_handoff import validate_original_medical_installation_handoff
from .original_medical_local_configuration import OriginalMedicalReadConfig
from .original_medical_local_read_sources import original_medical_local_read_sources
from .original_medical_native_session import original_medical_native_session
from .original_medical_publication import MedicalAdmission
from .original_medical_runtime import OriginalMedicalRuntimeCustody
from .read_only_transport import ReadOnlyTransport,original_read_capacity


def _configuration(config):
    def plain(value):
        if is_dataclass(value):return {f.name:plain(getattr(value,f.name))for f in fields(value)}
        if type(value)in(dict,MappingProxyType):return {k:plain(v)for k,v in value.items()}
        if type(value)in(list,tuple):return [plain(v)for v in value]
        if type(value)is type(Path()):return str(value)
        return value
    return json.dumps(plain(config),sort_keys=True,separators=(',',':'),allow_nan=False).encode()

def _identity(path):
    value=path.lstat()
    if path.resolve()!=path or not stat.S_ISDIR(value.st_mode) or value.st_uid!=os.getuid() or stat.S_IMODE(value.st_mode)!=0o700:
        raise PermissionError('Original private medical read output parent required')
    return value.st_dev,value.st_ino


def _write_private_file(path,raw):
    """Exclusive complete byte retention; closing never replaces the primary."""
    fd=None;primary=None
    try:
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        if os.write(fd,raw)!=len(raw):raise PermissionError('Complete original medical file write required')
        os.fsync(fd)
    except BaseException as error:primary=error
    finish(primary,[lambda:os.close(fd)]if fd is not None else [])

class MedicalReadPolicy:
    """Passive immutable read selection, explicitly refusing every effect port."""
    def __init__(self,context,targets,active,renew):
        self.context=context;self.targets=targets;self.active=active;self.initializing=False;self.renew=renew
    def writer(self,*args):raise PermissionError('Medical read policy cannot acquire writer')
    def admit(self,*args):raise PermissionError('Medical read policy cannot admit mutation')


def read_original_medical_local(config,*,supervision=None,singleton_query=None,query_batch=None):
    if type(config)is not OriginalMedicalReadConfig:raise ValueError('Exact configured medical read required')
    if supervision is not None and type(supervision)is not OriginalMedicalNativePhase:raise ValueError('Exact medical native phase required')
    if singleton_query is not None:
        from .medical_singleton_query import MedicalSingletonQuery
        if type(singleton_query)is not MedicalSingletonQuery:raise ValueError('Exact nominal medical singleton selection required')
        singleton_query.__post_init__()
    if query_batch is not None:
        from .medical_query_batch import MedicalQueryBatch
        if singleton_query is not None or type(query_batch)is not MedicalQueryBatch:raise ValueError('One exact finite medical query selection required')
        query_batch.__post_init__()
    config.__post_init__();original_config=_configuration(config);parent=_identity(config.output.parent)
    if config.output.exists()or config.output.is_symlink():raise PermissionError('Fresh medical read output required')
    config.output.mkdir(mode=0o700);output_identity=_identity(config.output);pins=[];primary=None;answer=None;closings=[]
    def renew():
        config.__post_init__()
        if _configuration(config)!=original_config or _identity(config.output.parent)!=parent or _identity(config.output)!=output_identity:raise PermissionError('Original medical read selection/output changed')
        finish(None,[lambda f=f,raw=raw:compare_pin(f,raw)for f,raw in pins])
        require_medical_journal_budget(config.journal,config.bounds.journal_bytes)
    def compare_pin(file,raw):
        if file.read()!=raw:raise PermissionError('Original medical read selected bytes changed')
    def selected(pin):
        file=MedicalFile(pin.path,config.bounds.input_bytes);raw=file.read();pins.append((file,raw))
        if len(raw)!=pin.bytes or hashlib.sha256(raw).hexdigest()!=pin.sha256:raise PermissionError('Original medical read selected file differs')
        return raw
    def write(name,value):
        renew();raw=(encoded(value)+'\n').encode()
        if len(raw)>config.bounds.receipt_bytes:raise PermissionError('Bounded medical read receipt required')
        _write_private_file(config.output/name,raw)
        renew()
    try:
        handoff=selected(config.installation);selection=validate_original_medical_installation_handoff(handoff,config)
        request_raw=selected(config.request);manifest_raw=selected(config.publication)
        request=decode_medical(request_raw,config.bounds.input_bytes);manifest=decode_medical(manifest_raw,config.bounds.input_bytes)
        if request_raw!=(encoded(request)+'\n').encode()or manifest_raw!=(encoded(manifest)+'\n').encode():raise PermissionError('Canonical retained medical read carriers required')
        LocalDeltaTransport.require_existing_journal(selection.journal,selection.installation_id,selection.targets,capacity=selection.capacity)
        with owned_context(OriginalMedicalRuntimeCustody.open(config.runtime,config.inputs,config.bounds,config.producer))as custody:
            model,graph,baseline=custody.input_bytes
            dataset=HeldMedicalDataset(config.producer,config.inputs.model.path,config.inputs.graph.path,config.inputs.baseline.path,config.output/'public-revalidation.json');closings.append(dataset.renew)
            batch,bindings=build_transaction(model,graph,baseline,source_system=FEED,binding_profile='ashlar-medical-development-bindings/0.2')
            admission=MedicalAdmission(batch,bindings,config.inputs.model.path,config.inputs.graph.path,config.inputs.baseline.path,dataset)
            payload=batch.begin+b''.join(r.raw for r in batch.records)+batch.commit
            transaction=OutboxTransaction('ashlar-postgresql-outbox/0.1',batch.feed,batch.epoch,'0','1',hashlib.sha256(payload).hexdigest(),batch)
            tables={role:target.table for role,target in zip(ROLES,selection.targets)};plan=MedicalPublicationPlan(admission,transaction,tables);view=plan.snapshot();plan.require_request(request);closings.append(plan.renew)
            active={key:view[field]for key,field in [('request','request'),('steps','selected_steps'),('expected','expected'),('publication_id','publication_id'),('previous_progress','previous_progress'),('progress','progress'),('previous_expected','previous_expected'),('generated_steps','generated_steps'),('elisions','zero_match_elisions')]};active['manifest']=manifest
            context=object();policy=MedicalReadPolicy(context,selection.targets,active,lambda:finish(None,[renew,custody.renew,dataset.renew,plan.renew]))
            capacity=original_read_capacity(selection.journal,selection.installation_id,selection.targets)
            if capacity!=selection.capacity:raise PermissionError('Original read capacity differs')
            with owned_context(original_medical_native_session(custody,phase=supervision))as spark:
                bounds=VerificationBounds(min(config.bounds.input_bytes,262144),config.bounds.rows,config.bounds.cell_bytes,config.bounds.total_bytes,config.bounds.receipt_bytes,config.bounds.stream_bytes)
                with owned_connection(ReadOnlyTransport.open(spark,selection.journal,selection.installation_id,selection.targets,policy,capacity=capacity,verification_bounds=bounds))as transport:
                    with owned_context(transport.bounded_observations(bounds)):
                        rows=transport.db.execute('SELECT length(request),substr(request,1,?),length(artifact),substr(artifact,1,?) FROM local_publication_artifact WHERE request_digest=?',(config.bounds.input_bytes+1,config.bounds.input_bytes+1,request['request_digest'])).fetchall()
                        if len(rows)!=1 or type(rows[0][0])is not int or type(rows[0][2])is not int or rows[0][0]>config.bounds.input_bytes or rows[0][2]>config.bounds.input_bytes or rows[0][1]!=encoded(request)or decode_medical(rows[0][3].encode(),config.bounds.input_bytes)['manifest']!=manifest:raise PermissionError('Original retained medical request/artifact differs')
                        registration=original_medical_local_read_sources(config.source)
                        source=registration.registration.source
                        port={'scope':source.scope,'source':source.source_schema,'signature':source.source_signature_sha256,'reader':registration.registration.source_role,'role':registration.registration.ack_role}
                        driver=NativeDriver(transport,policy,context,tables,{FEED:port},admission.changes,original_local_materialization_columns(),source_admission=admission)
                        driver.bind_query_sources(registration);driver.bind_original_medical_read(plan,bounds)
                        versions=json.loads(manifest['table_versions_json']);required={table:transport.targets[table].uuid for table in versions}
                        vector=manifest_pin_vector(manifest,required,authority='private-local-process')
                        with owned_context(driver.query_read_interval(context)):
                            with owned_context(driver.hold(vector,context=context)):
                                resolved=resolve_publication(driver,manifest['publication_id'],required,context=context,supported_profiles=['ashlar-delta/0.3'],supported_revisions={k:[v]for k,v in json.loads(manifest['schema_revisions_json']).items()})
                                observation=driver.observe_original_medical_read(resolved.descriptor,context=context)
                                query_result=None
                                if singleton_query is not None:
                                    from .medical_singleton_query import execute_medical_singleton
                                    query_result=execute_medical_singleton(singleton_query,driver,context,resolved,model,graph,baseline,bindings,output=config.output,bounds=bounds)
                                if query_batch is not None:
                                    from .medical_query_batch import execute_medical_query_batch
                                    query_result=execute_medical_query_batch(query_batch,driver,context,resolved,model,graph,baseline,bindings,output=config.output,bounds=bounds)
                                policy.renew()
                                answer={'profile':'ashlar-original-medical-local-read/0.1','installation_id':selection.installation_id,'publication':dict(resolved.descriptor.raw),'observation':observation,'qualification':'Actual existing local medical SELECT reopen; no source/native/ACK advancement, UC or Truss claim'}
                                if query_result is not None:answer['query_batch'if query_batch is not None else'singleton_query']=query_result
                        policy.renew()
            finish(None,[renew,custody.renew,dataset.renew,plan.renew])
    except BaseException as error:primary=error
    finish(primary,[renew]+closings)
    if singleton_query is not None or query_batch is not None:
        from .medical_singleton_query import MedicalSingletonBuffer
        from .medical_query_batch import MedicalQueryBatchBuffer
        result_name='query_batch'if query_batch is not None else'singleton_query'
        buffered=answer[result_name]
        expected=MedicalQueryBatchBuffer if query_batch is not None else MedicalSingletonBuffer
        if type(buffered)is not expected:raise ValueError('Exact buffered medical query result required')
        buffered.__post_init__()
        for name,raw in buffered.protocol_files():
            renew()
            maximum=config.bounds.receipt_bytes if name.endswith('-evidence.json')else config.bounds.input_bytes
            if len(raw)>maximum:raise PermissionError('Bounded complete original medical query protocol/evidence required')
            _write_private_file(config.output/name,raw)
            file=MedicalFile(config.output/name,maximum);pins.append((file,raw))
            renew()
        directory=os.open(config.output,os.O_RDONLY);error=None
        try:os.fsync(directory)
        except BaseException as caught:error=caught
        finish(error,[lambda:os.close(directory)])
        answer[result_name]=buffered.report()
    write('receipt.json',answer)
    return answer
