"""Original-program enrollment and held native PostgreSQL actor/fence admission.

Local delegated execution only. Ordinary actors cannot enroll programs or supply
roles, store UUIDs, epochs, executor identities or paths to the admission call.
The administrator independently binds original Weft source and the local table.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
import hashlib,json,threading
from umf_access.native_program import canonical_json,program_json
from .delta_atomic import DeltaAtomic

class DeltaAdmissionRefused(PermissionError):pass

@dataclass(frozen=True)
class _DeniedAtomic:
    table_uuid:str
    epoch:str

class _Scope:
    def __init__(self,spark,atomic,actor,executor,allowed,connection,sha,until,subject,original_program):
        self.spark,self.atomic,self.authenticated,self.effective,self.allowed=spark,atomic,actor,executor,allowed
        self.connection,self.sha,self.until=connection,sha,until
        self.original_path=str(atomic.path) if hasattr(atomic,"path") else None
        self.subject=subject
        self.original_program=original_program
        self.attribution_json=canonical_json({'nativeActor':{'sessionUser':actor,'roleOid':subject[2],'principalIdentityHex':subject[0],'subjectKind':subject[1]},'nativeExecutor':{'sparkUser':executor}})
        self.frame=None;self.cached=[];self.view_created=False
    def recheck(self):
        if monotonic()>=self.until:raise TimeoutError('original local Delta admission deadline expired')
        row=self.connection.execute('SELECT allowed,authenticated,table_uuid,epoch,original_path,native_executor,subject_identity,subject_kind,role_oid FROM ashlar_sdk_actions.admit(%s,%s)',(self.sha,self.original_program)).fetchone()
        if row is None or row[:4]!=(self.allowed,self.authenticated,self.atomic.table_uuid,self.atomic.epoch) or row[5]!=self.effective or self.original_path is not None and row[4]!=self.original_path or row[6:]!=self.subject:
            raise DeltaAdmissionRefused('Original native actor/program/fence context changed')
    def stage(self,frame):
        self.frame=frame;self.view_created=True;frame.createOrReplaceTempView('__umf_delta_stage')
    def query(self,sql,parameters):
        self.recheck();frame=self.spark.sql(sql,args={'p'+str(i):value for i,value in enumerate(parameters,1)})
        return [tuple(row)for row in frame.limit(3).collect()]
    def candidate(self,sql,parameters):
        self.recheck();frame=self.spark.sql(sql,args={'p'+str(i):value for i,value in enumerate(parameters,1)})
        # Materialize this original compiler candidate before rebinding the next
        # stage; its plan remains the pinned original read, never latest data.
        frame=frame.localCheckpoint(eager=True);self.cached.append(frame);self.stage(frame)

class LocalDeltaActionAdmission:
    def __init__(self,spark,connection_factory,compiler,*,maximum_rows=10000):
        if not callable(connection_factory) or not callable(getattr(compiler,'compile_native_action_json',None)):raise DeltaAdmissionRefused('Original native connection factory and compiler required')
        self.spark,self.connection_factory,self.compiler,self.maximum_rows=spark,connection_factory,compiler,maximum_rows
        self._lease=threading.Lock();self.quarantined=False
    @staticmethod
    def enroll_subject(connection,*,native_role,principal_identity,subject_kind):
        if type(principal_identity)is not bytes or not 1<=len(principal_identity)<=4096 or subject_kind not in ('human','service'):
            raise DeltaAdmissionRefused('Original explicit Subject association required')
        result=connection.execute('INSERT INTO ashlar_sdk_actions.subject(role_oid,principal_identity,subject_kind) SELECT oid,%s,%s FROM pg_roles WHERE rolname=%s AND rolcanlogin AND NOT rolsuper AND NOT rolcreaterole AND NOT rolcreatedb AND NOT rolreplication AND NOT rolbypassrls RETURNING role_oid::text',(principal_identity,subject_kind,native_role)).fetchone()
        if result is None:raise DeltaAdmissionRefused('Actual eligible ordinary native role required')
        return result[0]
    @staticmethod
    def enroll(connection,compiler,program,*,path,table_uuid,epoch,native_executor):
        checked=program.checked()
        if checked.get('profile') not in ('umf.actions.native-delta/0.1','umf.actions.native-delta-bulk/0.1'):raise DeltaAdmissionRefused('Original Delta action program required')
        request={'version':'weft.native-action/0.1','target':'spark-delta4','binding':checked['binding'],'source':checked['source'],'security':checked['security'],'roleMappings':checked['roleMappings'],'sessionPrincipal':'human'}
        if checked.get('profile')=='umf.actions.native-delta-bulk/0.1':
            rebuilt=program_json(compiler.compile_native_bulk_json(canonical_json(checked['originalRequest'])))
        else:rebuilt=program_json(compiler.compile_native_action_json(canonical_json(request)))
        if rebuilt!=checked:raise DeltaAdmissionRefused('Original Weft source/program correspondence differs')
        roles=[checked['roleMappings'][r]for r in checked['action']['authorization']['roles']]
        original=canonical_json(checked).encode();sha=hashlib.sha256(original).hexdigest()
        connection.execute('INSERT INTO ashlar_sdk_actions.program(program_sha,original_program,table_uuid,epoch,original_path,native_executor,native_roles) VALUES(%s,%s,%s,%s,%s,%s,%s)',(sha,original,table_uuid,epoch,str(Path(path).absolute()),native_executor,roles))
        return sha
    @contextmanager
    def hold(self,program,request_json,*,deadline):
        if self.quarantined:raise DeltaAdmissionRefused('Original local Delta admission quarantined')
        if not self._lease.acquire(blocking=False):raise DeltaAdmissionRefused('Dedicated native Spark action context already in use')
        connection=None;scope=None;timer=None;prior=None
        try:
            if monotonic()>=deadline:raise TimeoutError('deadline expired before original native admission')
            connection=self.connection_factory()
            if connection.info.transaction_status!=0:raise DeltaAdmissionRefused('Owned idle native admission connection required')
            connection.execute('BEGIN');connection.execute("SELECT pg_catalog.set_config('statement_timeout',%s,true)",(str(max(1,min(60000,int((deadline-monotonic())*1000)))),))
            if connection.execute('SELECT session_user=current_user').fetchone()!=(True,):raise DeltaAdmissionRefused('Original native session actor required')
            original_program=canonical_json(program).encode();sha=hashlib.sha256(original_program).hexdigest()
            row=connection.execute('SELECT allowed,authenticated,table_uuid,epoch,original_path,native_executor,nonce,subject_identity,subject_kind,role_oid FROM ashlar_sdk_actions.admit(%s,%s)',(sha,original_program)).fetchone()
            if row is None:raise DeltaAdmissionRefused('Original native program admission unavailable')
            allowed,actor,identity,epoch,path,executor,nonce,*subject=row
            subject=tuple(subject)
            actual=self.spark.sparkContext.sparkUser()
            if actual!=executor:raise DeltaAdmissionRefused('Original native Spark executor differs')
            native_request=hashlib.sha256(canonical_json({'requestJson':request_json,'nativeAdmissionNonce':nonce,'programSha256':sha}).encode()).hexdigest()
            context=self.spark.sparkContext;names=('spark.jobGroup.id','spark.job.description','spark.job.interruptOnCancel');prior=[context.getLocalProperty(n)for n in names]
            group='umf-delta-action-'+nonce;context.setJobGroup(group,'original admitted Delta action',interruptOnCancel=True)
            timer=threading.Timer(max(0,deadline-monotonic()),lambda:context.cancelJobGroup(group));timer.daemon=True;timer.start()
            if any(self.spark.catalog.tableExists(name)for name in ('__umf_delta_stage','__umf_delta_before','__umf_delta_selected')):raise DeltaAdmissionRefused('Dedicated native stage view name already in use')
            atomic=DeltaAtomic(self.spark,path=path,table_uuid=identity,epoch=epoch,request_sha256=native_request,maximum_rows=self.maximum_rows)if allowed else _DeniedAtomic(identity,epoch)
            if allowed:
                schema=json.loads(atomic.transaction.metadata().schemaString())
                fields=schema.get('fields',[]);bound=program['binding']['tables'][0]['columns']
                types={'string':'string','boolean':'boolean','integer':'long'}
                if len(fields)!=len(bound) or any(f['name']!=c['native'] or f['type']!=types.get(c['carrier']) or f['nullable']!=c['nullable'] or f.get('metadata',{}) for f,c in zip(fields,bound,strict=True)):
                    raise DeltaAdmissionRefused('Original complete native Delta schema differs from source binding')
            scope=_Scope(self.spark,atomic,actor,executor,allowed,connection,sha,deadline,subject,original_program)
            if allowed:atomic.preimage.createOrReplaceTempView("__umf_delta_before")
            yield scope
            scope.recheck();connection.rollback()
        except BaseException:
            if connection is not None:connection.rollback()
            raise
        finally:
            if timer is not None:timer.cancel()
            if scope is not None:
                for frame in reversed(scope.cached):frame.unpersist()
                if scope.view_created:self.spark.catalog.dropTempView('__umf_delta_stage')
                if scope.allowed:
                    self.spark.catalog.dropTempView('__umf_delta_before')
                    self.spark.catalog.dropTempView('__umf_delta_selected')
            if prior is not None:
                for name,value in zip(('spark.jobGroup.id','spark.job.description','spark.job.interruptOnCancel'),prior):self.spark.sparkContext.setLocalProperty(name,value)
            if connection is not None:connection.close()
            self._lease.release()
    def quarantine(self,scope,error):
        self.quarantined=True
        # Original details/recovery retain scope and native submission identity.
        # No resubmission, source regeneration or uncertainty-to-abort inference.
