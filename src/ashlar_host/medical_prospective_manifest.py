# SPDX-License-Identifier: MIT OR Apache-2.0
"""New authorized manifest intent; never replacement historical recovery custody."""
import hashlib,os,stat
from ashlar.manifest import DeltaManifestStore,validate_manifest_row
from ashlar.stored_publisher import validate_applied_artifact
from .delta_custody import LocalDeltaTransport,LocalOperationExecutor,child_operation,encoded,original_operation_intent
from .driver import NativeDriver,CarrierPolicy,RecoveryExecutor
from .lifecycle import finish
from .medical_commit_inspection import inspect_medical_commit
from .medical_commit_inspection_configuration import read_selected
from .medical_producer import decode_medical
from .medical_prospective_manifest_configuration import OriginalMedicalProspectiveManifestConfig,decode_authorization,prospective_selection_bytes,renew_prospective_selection
from .medical_publication_plan import MedicalPublicationPlan
from .medical_publication_sources import OriginalMedicalSourceOwner
from .medical_process_inspection import MedicalProcessInspection,validate_medical_process_observations
from .medical_journal_inspection import MedicalJournalInspection,validate_medical_journal_observations


def retain_prospective_authorization(path,raw):
    """One immutable private authorization per request, durable before intent."""
    fd=None;primary=None
    try:
        try:fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        except FileExistsError:
            require_prospective_authorization(path,raw)
            fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
        offset=len(raw)if os.fstat(fd).st_size else 0
        while offset<len(raw):
            count=os.write(fd,raw[offset:])
            if count<=0:raise OSError('Authorization retention incomplete')
            offset+=count
        os.fsync(fd)
    except BaseException as error:primary=error
    finish(primary,([]if fd is None else[lambda:os.close(fd)]))
    parent=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
    error=None
    try:os.fsync(parent)
    except BaseException as caught:error=caught
    finish(error,[lambda:os.close(parent)])
    require_prospective_authorization(path,raw)


def require_prospective_authorization(path,raw):
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK);primary=None
    try:
        before=os.fstat(fd)
        if not stat.S_ISREG(before.st_mode)or before.st_nlink!=1 or before.st_uid!=os.getuid()or stat.S_IMODE(before.st_mode)!=0o600 or before.st_size!=len(raw):
            raise PermissionError('Exact immutable private prospective authorization required')
        observed=bytearray()
        while len(observed)<=len(raw):
            part=os.read(fd,min(65536,len(raw)+1-len(observed)))
            if not part:break
            observed.extend(part)
        def identity(value):return (value.st_dev,value.st_ino,value.st_uid,value.st_mode,value.st_nlink,value.st_size,value.st_mtime_ns,value.st_ctime_ns)
        if bytes(observed)!=raw or identity(os.fstat(fd))!=identity(before)or identity(path.lstat())!=identity(before):raise PermissionError('Prospective authorization changed')
    except BaseException as error:primary=error
    finish(primary,[lambda:os.close(fd)])


def original_inspection_facts(value):
    """Separate historical process observations from stable original outcome facts."""
    if type(value)is not dict:raise ValueError('Original inspection object required')
    facts=dict(value)
    if 'process_observations'in facts and 'journal_observations'in facts:raise ValueError('One exact inspection evidence owner required')
    if 'journal_observations'in facts:
        if validate_medical_journal_observations(facts.pop('journal_observations'))is not None:raise ValueError('Journal observation validation incomplete')
    if 'process_observations'in facts:
        if validate_medical_process_observations(facts.pop('process_observations'))is not None:
            raise ValueError('Process observation validation incomplete')
    return facts


class MedicalProspectiveManifest:
    """Nominal actual-owner bridge for one newly selected, durably retained intent."""
    def __init__(self,driver,plan,source,observer,*,context,bounds,installation,configuration):
        if (type(driver)is not NativeDriver or type(driver.transport)is not LocalDeltaTransport
                or type(plan)is not MedicalPublicationPlan or type(source)is not OriginalMedicalSourceOwner
                or type(observer)not in (MedicalProcessInspection,MedicalJournalInspection) or type(configuration)is not OriginalMedicalProspectiveManifestConfig):
            raise ValueError('Actual original medical reconciliation owners required')
        if source.driver is not driver or observer.config!=configuration.inspection_selection():raise PermissionError('Same original source and observer selection required')
        self.driver=driver;self.plan=plan;self.source=source;self.observer=observer;self.context=context
        self.bounds=bounds;self.installation=decode_medical(encoded(installation).encode(),4194304);self.configuration=configuration
        self.original=prospective_selection_bytes(configuration)
        self.installation_raw=read_selected(configuration.recovery.installation)
        if decode_medical(self.installation_raw,4194304)!=self.installation:raise PermissionError('Exact pinned original installation required')
        self.authorization=decode_authorization(self.original[3]);self.authorization_digest=hashlib.sha256(self.original[3]).hexdigest()
        self.request=plan.snapshot()['request'];self.inspection=decode_medical(self.original[1],262144)
        self.proposal=decode_medical(self.original[2],262144);validate_manifest_row(self.proposal)
        self.inspection_facts=original_inspection_facts(self.inspection)
        a=self.authorization
        if (a['installation_id']!=installation['installation_id']or a['installation_id']!=driver.transport.installation_id
                or a['request_digest']!=self.request['request_digest']or a['inspection_sha256']!=configuration.inspection.sha256
                or a['proposal_sha256']!=configuration.proposal.sha256 or a['inspection_configuration_sha256']!=configuration.inspection_configuration.sha256
                or self.inspection.get('classification')!='observed-absent-at-anchor'
                or self.inspection.get('installation_id')!=a['installation_id']or self.inspection.get('request_digest')!=a['request_digest']
                or self.inspection.get('proposal_sha256')!=a['proposal_sha256']or self.inspection.get('manifest',{}).get('uuid')!=a['manifest_uuid']):
            raise PermissionError('Exact prospective authorization correspondence required')
        self.operation_prefix='reconciled-manifest:'+a['request_digest']+':'+self.authorization_digest
        self.sidecar=driver.transport.journal_path.parent/('prospective-manifest-'+a['installation_id']+'-'+a['request_digest']+'.authorization.json')
        self.target=driver.transport.targets[driver.tables['manifest']]
        if self.target.uuid!=a['manifest_uuid']:raise PermissionError('Original authorized manifest UUID differs')
        self.executor=LocalOperationExecutor(driver.transport,self.operation_prefix,a['request_digest'],context)
        self.store=DeltaManifestStore(self.executor,CarrierPolicy(driver,'manifest'),self.target.table,self.target.uuid)

    def require(self,context):
        if context is not self.context:raise PermissionError('Original reconciliation context required')
        renew_prospective_selection(self.configuration,self.original)
        self.source.lease.renew(context);self.observer.closing();self.driver.require(context)
        self.plan.require_request(self.request)
        if read_selected(self.configuration.recovery.installation)!=self.installation_raw or decode_medical(self.installation_raw,4194304)!=self.installation:
            raise PermissionError('Original installation selection changed')
        if (self.authorization!=decode_authorization(self.original[3])or self.proposal!=decode_medical(self.original[2],262144)
                or self.inspection!=decode_medical(self.original[1],262144)
                or self.inspection_facts!=original_inspection_facts(decode_medical(self.original[1],262144))):
            raise PermissionError('Original prospective in-memory selection changed')
        maximum=min(self.bounds.input_bytes,3*1024*1024)
        rows=self.driver.transport.db.execute('SELECT request,length(CAST(artifact AS BLOB)),substr(CAST(artifact AS BLOB),1,?) FROM local_publication_artifact WHERE request_digest=?',(maximum+1,self.request['request_digest'])).fetchall()
        if len(rows)!=1 or rows[0][0]!=encoded(self.request)or type(rows[0][1])is not int or not 0<rows[0][1]<=maximum or type(rows[0][2])is not bytes or len(rows[0][2])!=rows[0][1]:raise PermissionError('Complete original applied artifact required')
        raw=rows[0][2]
        if hashlib.sha256(raw).hexdigest()!=self.authorization['applied_artifact_sha256']:raise PermissionError('Authorized applied artifact changed')
        artifact,descriptor=validate_applied_artifact(raw.decode('utf-8','strict'),self.request)
        if artifact['manifest']!=self.proposal:raise PermissionError('Unchanged authorized manifest proposal required')
        self.driver.policy.active['manifest']=artifact['manifest']
        self.driver.validate(self.request,raw.decode('utf-8','strict'),descriptor,context)
        self.driver.source_admit(self.request)
        if self.sidecar.exists():require_prospective_authorization(self.sidecar,self.original[3])

    def retained(self,sql,parameters):
        transport=self.driver.transport;operation=child_operation(self.operation_prefix,sql,parameters)
        expected=original_operation_intent(transport.installation_id,operation,self.request['request_digest'],sql,parameters,self.target,transport.operation_capacity)
        rows=transport.db.execute('SELECT operation,intent,intent_sha,before_version,state,receipt FROM local_operation WHERE operation LIKE ? LIMIT 3',('reconciled-manifest:'+self.request['request_digest']+':%',)).fetchall()
        if not rows:return None
        if (len(rows)!=1 or rows[0][:4]!=(operation,expected,hashlib.sha256(expected.encode()).hexdigest(),1)
                or rows[0][4]not in ('prepared','submitted','committed')):raise PermissionError('Exact same prospective retained intent required')
        return rows[0]

    def reconcile(self,row,*,context):
        primary=None;answer=None;statement=None
        def receipt():
            if statement is None:return
            sql,parameters=statement
            retained=self.retained(sql,parameters)
            if retained is None or len(retained)<5 or retained[4]!='committed':
                if answer is not None:raise PermissionError('Committed prospective intent absent')
                return
            proof=self.driver.transport.inspect_committed(retained[0],sql,parameters,intent_digest=self.request['request_digest'],context=context)
            if proof['uuid']!=self.target.uuid or proof['before_version']!=1 or proof['version']!=2 or proof['snapshot']['rows']!=[row]:raise PermissionError('Exact prospective native receipt differs')
        try:
            self.require(context)
            if type(row)is not dict or row!=self.proposal:raise PermissionError('Exact unchanged proposed row required')
            sql,parameters=self.store.statement(row);statement=(sql,parameters)
            retained=self.retained(sql,parameters)
            if retained is None:
                if self.driver.transport.db.execute('SELECT 1 FROM local_operation WHERE operation LIKE ? LIMIT 1',('manifest:'+self.request['request_digest']+':%',)).fetchone():raise PermissionError('Conflicting ordinary manifest intent exists')
                observed,proposal=inspect_medical_commit(self.driver,self.plan,self.source,context=context,bounds=self.bounds,installation=self.installation)
                if encoded(observed)!=encoded(self.inspection_facts)or proposal!=self.original[2]:raise PermissionError('Original inspected absence no longer holds')
                retain_prospective_authorization(self.sidecar,self.original[3])
                self.require(context)
                self.store.prepare(row,context=context)
                retained=self.retained(sql,parameters)
                if retained is None:raise PermissionError('Durable new manifest intent absent')
            else:require_prospective_authorization(self.sidecar,self.original[3])
            # Recovery dispatch never prepares a replacement, including committed replies.
            recovery=DeltaManifestStore(RecoveryExecutor(self.executor),self.store.policy,self.store.table,self.store.uuid)
            answer=recovery.commit(row,context=context)
        except BaseException as error:primary=error
        finish(primary,[lambda:self.require(context),receipt])
        if answer is None:raise PermissionError('Prospective reconciliation completion absent')
        return answer
