# SPDX-License-Identifier: MIT OR Apache-2.0
"""Conditional retained-journal observations under an actual cooperating lease.

No process absence, native Delta observation or source/ACK authority is inferred.
Exclusive original storage and complete known execution selection remain premises.
"""
import hashlib,os,re,sqlite3,stat
from decimal import Decimal
from pathlib import Path
from uuid import UUID
from urllib.parse import quote
from .delta_custody import encoded,child_operation,local_journal_definitions
from .lifecycle import finish
from .medical_installation import OriginalMedicalInstallationLease
from .medical_producer import decode_medical,MedicalFile
from .medical_commit_inspection_configuration import read_selected
from .medical_journal_inspection_configuration import OriginalMedicalJournalInspectionConfig,journal_inspection_selection_bytes,renew_journal_inspection_selection,journal_file_fingerprint
from .original_medical_local_configuration import OriginalMedicalPublicationConfig,OriginalMedicalRecoveryConfig,parse_original_medical_local

OBSERVATION_PROFILE='ashlar-original-medical-journal-observation/0.1'
SCOPE='Conditional private cooperative original-journal lineage only; exclusive storage, complete known executions and no out-of-band rollback are assumptions. No process absence, native publication or source ACK authority.'
SCHEMA=local_journal_definitions()
COLUMNS={'local_installation':('id','original'),'local_operation':('operation','intent','intent_sha','before_version','state','receipt'),
 'local_plan':('operation','original','digest'),'local_publication_artifact':('request_digest','request','artifact'),'local_source_plan':('request_digest','original')}


def journal_snapshot(path,*,maximum_bytes,maximum_rows,immutable=False):
    """Read a complete closed SQLite inventory, never a successful truncated prefix."""
    if type(maximum_bytes)is not int or not 1<=maximum_bytes<=128*1024*1024 or type(maximum_rows)is not int or not 1<=maximum_rows<=10000:raise ValueError('Bounded complete journal selection required')
    uri='file:'+quote(str(path))+'?mode=ro'+('&immutable=1'if immutable else '')
    database=sqlite3.connect(uri,uri=True,timeout=1);primary=None;result=None
    try:
        database.execute('PRAGMA query_only=ON');database.execute('BEGIN')
        if database.execute('PRAGMA quick_check').fetchall()!=[('ok',)]:raise PermissionError('Complete valid SQLite journal required')
        schema=database.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY name").fetchall()
        tables={name:sql for kind,name,table,sql in schema if kind=='table'}
        if set(tables)!=set(SCHEMA)or any(' '.join(tables[name].upper().split())!=' '.join(expected.upper().split())for name,expected in SCHEMA.items()):raise PermissionError('Exact original journal table schemas required')
        if any(kind!='table'and not(kind=='index'and name.startswith('sqlite_autoindex_')and table in SCHEMA and sql is None)for kind,name,table,sql in schema):raise PermissionError('Unknown journal schema object refused')
        result={};size=0;count=0
        for table,columns in COLUMNS.items():
            if tuple(row[1]for row in database.execute('PRAGMA table_info('+table+')'))!=columns:raise PermissionError('Original complete journal columns required')
            rows=database.execute('SELECT '+','.join(columns)+' FROM '+table+' ORDER BY '+columns[0]+' LIMIT ?', (maximum_rows-count+1,)).fetchall()
            count+=len(rows)
            if count>maximum_rows:raise PermissionError('Complete journal row budget exceeded')
            for row in rows:
                for value in row:
                    if type(value)is str:size+=len(value.encode('utf-8','strict'))
                    elif type(value)is int:size+=8
                    elif value is not None:raise PermissionError('Original exact journal scalar types required')
                if size>maximum_bytes:raise PermissionError('Complete journal byte budget exceeded')
            result[table]={row[0]:tuple(row)for row in rows}
    except BaseException as error:primary=error
    finish(primary,[database.rollback,database.close])
    return result


def require_journal_prefix(previous,current):
    """Existing bytes cannot change; only original operation state may advance."""
    if set(previous)!=set(SCHEMA)or set(current)!=set(SCHEMA):raise PermissionError('Complete original journal inventories required')
    ranks={'prepared':0,'submitted':1,'committed':2}
    for table,rows in previous.items():
        for key,row in rows.items():
            found=current[table].get(key)
            if found is None:raise PermissionError('Original journal row disappeared')
            if table!='local_operation':
                if found!=row:raise PermissionError('Original journal bytes changed')
            elif (found[:4]!=row[:4]or row[4]not in ranks or found[4]not in ranks or ranks[found[4]]<ranks[row[4]]
                    or (row[5]is not None and found[5]!=row[5])):
                raise PermissionError('Original operation intent/receipt progression changed')


def require_execution(execution,recovery):
    original=parse_original_medical_local(read_selected(execution.configuration))
    if type(original)not in (OriginalMedicalPublicationConfig,OriginalMedicalRecoveryConfig):raise PermissionError('Complete original publication/recovery execution required')
    if any(getattr(original,key)!=getattr(recovery,key)for key in ('source','inputs','root','installation_id','installation','journal','targets','capacity')):raise PermissionError('Known execution selects another original source or installation')
    if type(original)is OriginalMedicalRecoveryConfig and read_selected(original.request)!=read_selected(recovery.request):raise PermissionError('Known recovery request differs')
    terminal=decode_medical(read_selected(execution.terminal),262144)
    if (type(terminal)is not dict or set(terminal)!={'state','exit_code','elapsed_seconds','argv'}or terminal['state']!='terminal'
            or type(terminal['exit_code'])is not int or not -255<=terminal['exit_code']<=255
            or type(terminal['argv'])is not list or any(type(value)is not str for value in terminal['argv'])):raise PermissionError('Actual terminal original invocation required')
    elapsed=terminal['elapsed_seconds']
    if type(elapsed)not in (int,Decimal)or (type(elapsed)is Decimal and not elapsed.is_finite())or not 0<=elapsed<2**63:raise PermissionError('Finite original terminal duration required')
    python=str(original.runtime.python.path)
    if execution.launcher is None:
        command='publish-original-medical'if type(original)is OriginalMedicalPublicationConfig else 'recover-original-medical'
        expected=[python,'-I','-B','-m','ashlar',command,'--configuration',str(execution.configuration.path)]
    else:
        if type(original)is not OriginalMedicalRecoveryConfig or execution.launcher.path.parent/'configuration.json'!=execution.configuration.path:raise PermissionError('Exact original diagnostic configuration required')
        expected=[python,'-I','-B',str(execution.launcher.path)]
    if terminal['argv']!=expected:raise PermissionError('Terminal invocation does not select original configuration')
    root=original.runtime.pyspark_root/'ashlar_host'
    for name,file in (('transport','delta_custody.py'),('driver','driver.py'),('capture','medical_worker_capture.py'),('lease','medical_installation.py')):
        if getattr(execution,name).path!=root/file:raise PermissionError('Original selected installed module origin required')


class MedicalJournalInspection:
    def __init__(self,config):
        if type(config)is not OriginalMedicalJournalInspectionConfig:raise ValueError('Exact original journal inspection configuration required')
        config.__post_init__();self.config=config;self.lineage=config.lineage_selection();self.original=journal_inspection_selection_bytes(config)
        self.recovery=config.recovery;self.observations=[];self.lease=None;self.context=None;self.started=False
        self.request=decode_medical(read_selected(self.recovery.request),self.recovery.bounds.input_bytes)
        from ashlar.attempt_store import verify_request_digest
        verify_request_digest(self.request)
        if (self.lineage.installation_id!=self.recovery.installation_id or self.lineage.installation!=self.recovery.installation
                or type(self.request)is not dict or self.request.get('request_digest')!=self.lineage.request_digest):raise PermissionError('Exact original journal lineage selection required')
        if sum(pin.bytes for pin in self.lineage.snapshots)>self.recovery.bounds.journal_bytes:raise PermissionError('Aggregate original snapshot budget exceeded')
        for execution in self.lineage.executions:require_execution(execution,self.recovery)
        installation_raw=read_selected(self.lineage.installation)
        self.installation=decode_medical(installation_raw,4194304)
        from .original_medical_installation_handoff import validate_original_medical_installation_handoff
        self.selection=validate_original_medical_installation_handoff(installation_raw,self.recovery)
        previous=None
        for pin in self.lineage.snapshots:
            current=journal_snapshot(pin.path,maximum_bytes=self.recovery.bounds.journal_bytes,maximum_rows=self.recovery.bounds.rows,immutable=True)
            self.validate(current)
            if previous is not None:require_journal_prefix(previous,current)
            previous=current
        self.baseline=previous;self._last_current=previous
        if any(len(self.baseline[table])!=1 for table in ('local_plan','local_source_plan','local_publication_artifact')):raise PermissionError('Complete final original plan/source/artifact baseline required')
        if any(row[4]!='committed'for row in self.baseline['local_operation'].values()):raise PermissionError('Every baseline operation must have a committed retained receipt')
        if any(key.startswith('manifest:')or key.startswith('reconciled-manifest:')for key in self.baseline['local_operation']):raise PermissionError('Original baseline manifest intent must be absent')
        self.renew()

    def validate(self,view):
        registry=view['local_installation']
        if set(registry)!={1}or type(registry[1][1])is not str:raise PermissionError('Exact original installation registry required')
        original=decode_medical(registry[1][1].encode(),4194304)
        targets=[{'table':target.table,'path':str(target.path),'uuid':target.uuid}for target in self.selection.targets]
        expected={'installation':self.recovery.installation_id,'targets':sorted(targets,key=lambda row:row['table']),**({}if self.recovery.capacity is None else{'operation_capacity':dict(self.recovery.capacity)})}
        if original!=expected:raise PermissionError('Original complete installation registry differs')
        for operation,row in view['local_operation'].items():
            _,intent,digest,before,state,receipt=row
            if (type(intent)is not str or hashlib.sha256(intent.encode()).hexdigest()!=digest or type(before)is not int or not 0<=before<2**63
                    or state not in ('prepared','submitted','committed')or (state=='committed'and(type(receipt)is not str or not receipt))or(state!='committed'and receipt is not None)):
                raise PermissionError('Exact original operation state/receipt required')
            value=decode_medical(intent.encode(),self.recovery.bounds.journal_bytes)
            fields={'profile','installation_id','operation','request_digest','statement','parameters','table','path','uuid'}|({'operation_capacity'}if self.recovery.capacity is not None else set())
            if (type(value)is not dict or set(value)!=fields or value.get('profile')!='ashlar-local-delta-operation/0.1'
                    or type(value.get('statement'))is not str or type(value.get('parameters'))is not dict
                    or value.get('operation_capacity')!=(None if self.recovery.capacity is None else dict(self.recovery.capacity))or value.get('operation')!=operation or value.get('installation_id')!=self.recovery.installation_id
                    or value.get('request_digest')!=self.lineage.request_digest or not any(value.get('table')==target['table']and value.get('path')==target['path']and value.get('uuid')==target['uuid']for target in targets)):
                raise PermissionError('Original operation source/target/request correspondence required')
        for key,row in view['local_plan'].items():
            value=decode_medical(row[1].encode(),self.recovery.bounds.journal_bytes)
            if hashlib.sha256(row[1].encode()).hexdigest()!=row[2]or value.get('installation_id')!=self.recovery.installation_id or value.get('intent_digest')!=self.lineage.request_digest or value.get('operation')!=key:raise PermissionError('Original effect plan differs')
        for key,row in view['local_source_plan'].items():
            value=decode_medical(row[1].encode(),self.recovery.bounds.journal_bytes)
            if key!=self.lineage.request_digest or value.get('request')!=self.request:raise PermissionError('Original source plan differs')
        for key,row in view['local_publication_artifact'].items():
            if key!=self.lineage.request_digest or row[1]!=encoded(self.request):raise PermissionError('Original retained artifact request differs')
            from ashlar.stored_publisher import validate_applied_artifact
            validate_applied_artifact(row[2],self.request)

    def renew(self):
        renew_journal_inspection_selection(self.config,self.original)
        if self.lease is not None:
            self.lease.renew(self.context)
            observed=self.recovery.journal.lstat()
            if (observed.st_dev,observed.st_ino)!=self.journal_identity or not stat.S_ISREG(observed.st_mode)or observed.st_uid!=os.getuid():raise PermissionError('Current held original journal identity changed')

    def before(self,lease,context):
        if type(lease)is not OriginalMedicalInstallationLease or self.lease is not None:raise ValueError('One actual original installation lease required')
        if lease.selection.installation_id!=self.recovery.installation_id or lease.selection.journal!=self.recovery.journal:raise PermissionError('Original held journal lease required')
        lease.renew(context);observed=self.recovery.journal.lstat();self.journal_identity=(observed.st_dev,observed.st_ino)
        self.lease=lease;self.context=context
        return self.observe('before')

    def observe(self,phase):
        if self.lease is None:raise PermissionError('Actual original journal lease not held')
        primary=None;answer=None
        try:
            self.renew();current=journal_snapshot(self.recovery.journal,maximum_bytes=self.recovery.bounds.journal_bytes,maximum_rows=self.recovery.bounds.rows)
            self.validate(current);require_journal_prefix(self.baseline,current);require_journal_prefix(self._last_current,current)
            for table in SCHEMA:
                if table!='local_operation'and current[table]!=self.baseline[table]:raise PermissionError('Unexpected current original journal inventory extension')
            for operation,row in current['local_operation'].items():
                if operation in self.baseline['local_operation']:continue
                self.require_extension(operation,row)
            answer={'profile':OBSERVATION_PROFILE,'phase':phase,'installation_id':self.lineage.installation_id,'request_digest':self.lineage.request_digest,
                'journal_device':self.journal_identity[0],'journal_inode':self.journal_identity[1],
                'snapshots':len(self.lineage.snapshots),'executions':len(self.lineage.executions),'baseline_operations':len(self.baseline['local_operation']),
                'current_operations':len(current['local_operation']),'scope':SCOPE}
            if len(self.observations)>=128:raise PermissionError('Bounded complete journal observation inventory exceeded')
            validate_medical_journal_observations([answer]);self.observations.append(answer)
        except BaseException as error:primary=error
        finish(primary,[self.renew])
        self._last_current=current
        return answer

    def require_extension(self,operation,row):
        value=decode_medical(row[1].encode(),self.recovery.bounds.journal_bytes)
        if re.fullmatch('reconciled-manifest:'+self.lineage.request_digest+':[0-9a-f]{64}:[0-9a-f]{64}',operation):
            from ashlar.manifest import DeltaManifestStore
            from .medical_prospective_manifest_configuration import decode_authorization
            target=next(target for target in self.selection.targets if target.table.endswith('.manifest'))
            artifact=self.baseline['local_publication_artifact'].get(self.lineage.request_digest)
            if artifact is None:raise PermissionError('Complete baseline artifact required')
            manifest=decode_medical(artifact[2].encode(),3*1024*1024)['manifest']
            sql,parameters=DeltaManifestStore(None,None,target.table,target.uuid).statement(manifest)
            prefix=operation.rsplit(':',1)[0]
            if operation!=child_operation(prefix,sql,parameters)or value.get('statement')!=sql or value.get('parameters')!=parameters or value['table']!=target.table:raise PermissionError('Exact original manifest SQL/parameter extension required')
            sidecar=self.recovery.journal.parent/('prospective-manifest-'+self.lineage.installation_id+'-'+self.lineage.request_digest+'.authorization.json')
            observed=sidecar.lstat()
            if observed.st_uid!=os.getuid()or stat.S_IMODE(observed.st_mode)!=0o600:raise PermissionError('Actual private prospective authorization required')
            raw=MedicalFile(sidecar,262144).read();authorization=decode_authorization(raw)
            if (hashlib.sha256(raw).hexdigest()!=prefix.rsplit(':',1)[1]or authorization['installation_id']!=self.lineage.installation_id
                    or authorization['request_digest']!=self.lineage.request_digest or authorization['manifest_uuid']!=target.uuid
                    or authorization['applied_artifact_sha256']!=hashlib.sha256(artifact[2].encode()).hexdigest()
                    or authorization['proposal_sha256']!=hashlib.sha256((encoded(manifest)+'\n').encode()).hexdigest()):raise PermissionError('Exact same-request prospective authorization required')
            return
        if not operation.startswith('attempt:'):raise PermissionError('Unknown new journal effect refused')
        parameters=value.get('parameters')
        if type(parameters)is not dict or set(parameters)!={'payload'}or type(parameters['payload'])is not str:raise PermissionError('Exact committed-attempt payload required')
        payload=decode_medical(parameters['payload'].encode(),self.recovery.bounds.journal_bytes)
        if (type(payload)is not dict or payload.get('phase')!='committed'or payload.get('request_digest')!=self.lineage.request_digest
                or payload.get('stream')!=self.request['stream']or payload.get('batch_id')!=self.request['batch_id']):raise PermissionError('Only original committed-attempt extension permitted')
        if (value.get('table')!=next(target.table for target in self.selection.targets if target.table.endswith('.attempts'))
                or value.get('statement')not in {decode_medical(old[1].encode(),self.recovery.bounds.journal_bytes).get('statement')for key,old in self.baseline['local_operation'].items()if key.startswith('attempt:')}
                or operation!=child_operation('attempt:'+self.lineage.request_digest,value.get('statement'),parameters)):
            raise PermissionError('Exact original committed-attempt SQL required')
        body=decode_medical(payload['payload_json'].encode(),self.recovery.bounds.journal_bytes)
        artifact=self.baseline['local_publication_artifact'].get(self.lineage.request_digest)
        if (artifact is None or body.get('request')!=self.request or body.get('result_json')!=artifact[2]or type(body.get('descriptor_json'))is not str
                or decode_medical(body['descriptor_json'].encode(),262144)!=decode_medical(artifact[2].encode(),3*1024*1024)['manifest']
                or hashlib.sha256(payload['payload_json'].encode()).hexdigest()!=payload.get('payload_digest')):raise PermissionError('Original committed-attempt artifact differs')

    def bind(self):
        if self.started:raise PermissionError('One current journal binding required')
        result=self.observe('bind');self.started=True;return result
    def closing(self):
        if not self.started:raise PermissionError('Current journal not bound')
        return self.observe('closing')


def validate_medical_journal_observations(observations):
    fields={'profile','phase','installation_id','request_digest','journal_device','journal_inode','snapshots','executions','baseline_operations','current_operations','scope'}
    if type(observations)is not list or not 1<=len(observations)<=128:raise ValueError('Bounded journal observations required')
    identity=None
    for value in observations:
        if type(value)is not dict or set(value)!=fields or value['profile']!=OBSERVATION_PROFILE or value['scope']!=SCOPE or value['phase']not in ('before','bind','closing'):raise ValueError('Closed qualified journal observation required')
        if type(value['installation_id'])is not str or str(UUID(value['installation_id']))!=value['installation_id']or type(value['request_digest'])is not str or re.fullmatch('[0-9a-f]{64}',value['request_digest'])is None:raise ValueError('Original journal observation identity required')
        for key in ('journal_device','journal_inode','snapshots','executions','baseline_operations','current_operations'):
            if type(value[key])is not int or not 0<=value[key]<2**63:raise ValueError('Exact journal observation counts required')
        if not 2<=value['snapshots']<=16 or not 1<=value['executions']<=16 or value['current_operations']<value['baseline_operations']:raise ValueError('Complete journal lineage counts required')
        selected=tuple(value[key]for key in ('installation_id','request_digest','journal_device','journal_inode','snapshots','executions','baseline_operations'))
        if identity is not None and selected!=identity:raise ValueError('Original journal observation selection changed')
        identity=selected
