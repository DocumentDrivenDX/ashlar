"""Fresh-only medical installation; no publication/source/ACK authority grant."""
from contextlib import contextmanager
from dataclasses import asdict,is_dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
from urllib.parse import urlsplit,unquote
from .delta_custody import DeltaTarget,LocalDeltaTransport
from .driver import PrivatePolicy
from .lifecycle import finish,owned_context
from .local_physical_carriers import original_local_physical_carriers,ORIGINAL_LOCAL_PHYSICAL_PROVENANCE
from .medical_producer import HeldMedicalDataset,MedicalFile
from .original_medical_local_configuration import OriginalMedicalInitializationConfig
from .original_medical_runtime import OriginalMedicalRuntimeCustody
from .original_medical_native_session import original_medical_native_session

ROLES=('object_current','edge_current','tombstone','whole_source_history','attempts','manifest')


def _identity(path,fd=None,directory=True,mode=None):
    named=path.lstat()
    if path.resolve()!=path or named.st_uid!=os.getuid() or not (stat.S_ISDIR(named.st_mode)if directory else stat.S_ISREG(named.st_mode)) or (mode is not None and stat.S_IMODE(named.st_mode)!=mode):
        raise PermissionError('Canonical private medical installation resource required')
    facts=(named.st_dev,named.st_ino,named.st_uid,named.st_mode)
    if fd is not None:
        held=os.fstat(fd)
        if (held.st_dev,held.st_ino,held.st_uid,held.st_mode)!=facts:raise PermissionError('Named/held medical installation differs')
    return facts


def _bounded(value,maximum):
    pieces=[];size=0
    for part in json.JSONEncoder(default=str,ensure_ascii=False,separators=(',',':'),sort_keys=True).iterencode(value):
        raw=part.encode();size+=len(raw)
        if size>maximum:raise ValueError('Medical installation handoff exceeds selected bound')
        pieces.append(raw)
    return b''.join(pieces)


def _configuration(config):
    return {name:dict(value)if name=='capacity'else asdict(value)if is_dataclass(value)else value for name,value in vars(config).items()}


class MedicalBootstrapPolicy(PrivatePolicy):
    """Fresh initialization policy owns actual cooperating lock and input custody."""
    def __init__(self,context,config,custody,dataset,schemas,root_fd,output_fd,lock_fd,parents):
        super().__init__(context,())
        self.config=config;self.custody=custody;self.dataset=dataset;self.schemas=schemas
        self.root_fd=root_fd;self.output_fd=output_fd;self.lock_fd=lock_fd;self.parents=parents
        self.identities=(_identity(config.root,root_fd,mode=0o700),_identity(config.output,output_fd,mode=0o700),_identity(config.root/'.evolution-operator.lock',lock_fd,False,0o600))
        self.original_config=_bounded(_configuration(config),config.bounds.input_bytes)
    def renew(self):
        def config():
            self.config.__post_init__()
            if _bounded(_configuration(self.config),self.config.bounds.input_bytes)!=self.original_config:raise PermissionError('Medical bootstrap selection changed')
        def directories():
            for path,original in self.parents:
                if _identity(path)!=original:raise PermissionError('Medical initialization parent changed')
            actual=(_identity(self.config.root,self.root_fd,mode=0o700),_identity(self.config.output,self.output_fd,mode=0o700),_identity(self.config.root/'.evolution-operator.lock',self.lock_fd,False,0o600))
            if actual!=self.identities:raise PermissionError('Medical bootstrap root/output/lock changed')
        finish(None,[config,directories,self.custody.renew,self.dataset.renew,self.schemas])
    @contextmanager
    def writer(self,*args):
        primary=None
        try:
            self.renew()
            with owned_context(super().writer(*args)):yield
        except BaseException as error:primary=error
        finish(primary,[self.renew])
    def admit(self,intent,context):
        self.renew()
        if intent.get('profile')!='ashlar-local-delta-installation/0.1'or intent.get('installation')!=self.config.installation_id or intent.get('operation_capacity')!=dict(self.config.capacity):raise PermissionError('Exact fresh medical installation intent required')
        return super().admit(intent,context)


@contextmanager
def _bootstrap_interval(policy):
    primary=None
    try:yield
    except BaseException as error:primary=error
    finish(primary,[policy.renew])


def observe_empty_medical_target(spark,target,schema,registration=None):
    """Actual full generated schema/layout, UUID, empty bag and defined stage."""
    table='delta.`'+str(target.path).replace('`','``')+'`'
    detail=spark.sql('DESCRIBE DETAIL '+table).first().asDict()
    frame=spark.read.format('delta').load(str(target.path))
    location=urlsplit(detail['location']);version=0;properties={}
    if registration is None:
        history=spark.sql('DESCRIBE HISTORY '+table).select('version','userMetadata').limit(2).collect()
        valid=len(history)==1 and type(history[0]['version'])is int and history[0]['version']==0 and history[0]['userMetadata']is None
    else:
        version=1
        properties={'ashlar.local.installation.id':registration.installation_id,'ashlar.local.registry.sha256':registration.registry_sha,'ashlar.local.journal.sha256':registration.journal_sha}
        metadata=json.dumps({'profile':'ashlar-local-delta-installation-commit/0.1','installation_id':registration.installation_id,'registry_sha256':registration.registry_sha,'journal_sha256':registration.journal_sha,'uuid':target.uuid},sort_keys=True,separators=(',',':'))
        history=registration.original_history(target)
        valid=len(history)==2 and all(type(row.get('version'))is int for row in history)and [row['version']for row in history]==[1,0] and history[0].get('operation')=='SET TBLPROPERTIES'and history[0].get('userMetadata')==metadata and history[1].get('userMetadata')is None
    if (type(detail['id'])is not str or detail['id']!=target.uuid or location.scheme!='file'or location.netloc or Path(unquote(location.path)).resolve()!=target.path or not valid or frame.limit(1).count()!=0 or json.loads(frame.schema.json())!=schema or detail['partitionColumns']or detail['properties']!=properties or type(detail['minReaderVersion'])is not int or detail['minReaderVersion']!=1 or type(detail['minWriterVersion'])is not int or detail['minWriterVersion']!=2):
        raise ValueError('Actual medical empty native schema/identity/stage differs')
    return {'uuid':target.uuid,'version':version,'location':detail['location'],'schema':schema,'properties':properties,'minReaderVersion':1,'minWriterVersion':2,'history':[{'version':row['version'],'userMetadata':row['userMetadata'],**({'operation':row['operation']}if registration is not None and row['version']==1 else {})}for row in history]}


def initialize_original_medical_local(config):
    """Retain partial installations; release a bounded handoff only after closing."""
    if type(config)is not OriginalMedicalInitializationConfig:raise ValueError('Exact fresh medical initialization configuration required')
    config.__post_init__()
    if config.root.exists()or config.root.is_symlink()or config.output.exists()or config.output.is_symlink():raise ValueError('Fresh medical root and output required')
    generated=original_local_physical_carriers()
    schemas_file=MedicalFile(config.schemas.path,min(config.bounds.input_bytes,4194304));schemas_raw=schemas_file.read()
    if len(schemas_raw)!=config.schemas.bytes or hashlib.sha256(schemas_raw).hexdigest()!=config.schemas.sha256 or json.loads(schemas_raw)!=generated:raise ValueError('Exact checked generated six-role medical schema correspondence required')
    def schemas():
        config.schemas.__post_init__()
        if schemas_file.read()!=schemas_raw or original_local_physical_carriers()!=generated:raise PermissionError('Generated medical physical carrier changed')
    parents=tuple((p,_identity(p))for p in sorted({config.root.parent,config.output.parent}))
    root_fd=None;output_fd=None;lock_fd=None;policy=None;dataset=None;primary=None;result=None;created_root=None;created_output=None;created_lock=None
    original_config=_bounded(_configuration(config),config.bounds.input_bytes)
    def selection():
        config.__post_init__()
        if _bounded(_configuration(config),config.bounds.input_bytes)!=original_config:raise PermissionError('Original medical initialization selection changed')
        for p,facts in parents:
            if _identity(p)!=facts:raise PermissionError('Medical parent changed before effect')
    try:
        with owned_context(OriginalMedicalRuntimeCustody.open(config.runtime,config.inputs,config.bounds,config.producer))as custody:
            selection();config.output.mkdir(mode=0o700);created_output=_identity(config.output,mode=0o700);output_fd=os.open(config.output,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            if _identity(config.output,output_fd,mode=0o700)!=created_output:raise PermissionError('Created medical output replaced')
            dataset=HeldMedicalDataset(config.producer,config.inputs.model.path,config.inputs.graph.path,config.inputs.baseline.path,config.output/'public-revalidation.json')
            selection();config.root.mkdir(mode=0o700);created_root=_identity(config.root,mode=0o700);root_fd=os.open(config.root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            if _identity(config.root,root_fd,mode=0o700)!=created_root:raise PermissionError('Created medical root replaced')
            lock_fd=os.open(config.root/'.evolution-operator.lock',os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
            created_lock=_identity(config.root/'.evolution-operator.lock',lock_fd,False,0o600)
            fcntl.flock(lock_fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            context=object();policy=MedicalBootstrapPolicy(context,config,custody,dataset,schemas,root_fd,output_fd,lock_fd,parents)
            with owned_context(_bootstrap_interval(policy)):
                with owned_context(original_medical_native_session(custody))as spark:
                    from pyspark.sql.types import StructType
                    targets=[];witnesses={};attempted=[];transport=None;native_primary=None
                    try:
                        for role in ROLES:
                            policy.renew();path=config.root/role;schema=json.loads(generated['carriers'][role]['schemaJson']);attempted.append((role,path,schema))
                            spark.createDataFrame([],StructType.fromJson(schema)).write.format('delta').save(str(path))
                            detail=spark.sql('DESCRIBE DETAIL delta.`'+str(path).replace('`','``')+'`').first().asDict()
                            target=DeltaTarget('local.original_medical.'+role,path,detail['id']);targets.append(target)
                            witnesses[role]=observe_empty_medical_target(spark,target,schema)
                        policy.targets=tuple(targets)
                        transport=LocalDeltaTransport.initialize(spark,config.root/'operations.sqlite',config.installation_id,tuple(targets),policy,context=context,capacity=config.capacity)
                        result={'profile':'ashlar-original-medical-local-installation/0.1','installation_id':config.installation_id,'journal':str(config.root/'operations.sqlite'),'targets':[{'table':t.table,'path':str(t.path),'uuid':t.uuid}for t in targets],'capacity':dict(config.capacity),'generated_provenance_sha256':ORIGINAL_LOCAL_PHYSICAL_PROVENANCE,'qualification':'Fresh local medical physical installation only; no publication/source/ACK authority.'}
                    except BaseException as error:native_primary=error
                    def close_role(role,path,schema):
                        if not path.exists():
                            if role in witnesses or any(t.path==path for t in targets):raise PermissionError('Acquired medical target disappeared')
                            return
                        target=next((t for t in targets if t.path==path),None)
                        if target is None:
                            detail=spark.sql('DESCRIBE DETAIL delta.`'+str(path).replace('`','``')+'`').first().asDict();target=DeltaTarget('local.original_medical.'+role,path,detail['id'])
                        observed=observe_empty_medical_target(spark,target,schema,transport)
                        if role in witnesses:
                            original=dict(witnesses[role]);original['version']=1 if transport else 0
                            if transport:original['history']=observed['history']
                            if transport:original['properties']={'ashlar.local.installation.id':transport.installation_id,'ashlar.local.registry.sha256':transport.registry_sha,'ashlar.local.journal.sha256':transport.journal_sha}
                            if observed!=original:raise PermissionError('Medical opening native witness changed')
                        if result is not None:result.setdefault('native',{})[role]={'creation':witnesses[role],'registration':observed}
                    def journal():
                        if transport is not None:LocalDeltaTransport.require_existing_journal(config.root/'operations.sqlite',config.installation_id,tuple(targets),capacity=config.capacity)
                    finish(native_primary,[lambda role=r,path=p,schema=s:close_role(role,path,schema)for r,p,s in attempted]+[journal,policy.renew]+([transport.close]if transport is not None else []))
    except BaseException as error:primary=error
    callbacks=([dataset.renew]if dataset is not None else [])+[schemas,selection]
    if created_root is not None:callbacks.append(lambda:_identity(config.root,root_fd,mode=0o700)==created_root or (_ for _ in ()).throw(PermissionError('Created medical root changed')))
    if created_output is not None:callbacks.append(lambda:_identity(config.output,output_fd,mode=0o700)==created_output or (_ for _ in ()).throw(PermissionError('Created medical output changed')))
    if created_lock is not None:callbacks.append(lambda:_identity(config.root/'.evolution-operator.lock',lock_fd,False,0o600)==created_lock or (_ for _ in ()).throw(PermissionError('Created medical lock changed')))
    if policy is not None:
        callbacks.extend([lambda:_identity(config.root,root_fd,mode=0o700)==policy.identities[0] or (_ for _ in ()).throw(PermissionError('Medical root changed')),lambda:_identity(config.output,output_fd,mode=0o700)==policy.identities[1] or (_ for _ in ()).throw(PermissionError('Medical output changed')),lambda:_identity(config.root/'.evolution-operator.lock',lock_fd,False,0o600)==policy.identities[2] or (_ for _ in ()).throw(PermissionError('Medical lock changed'))])
    callbacks.extend(lambda p=p,f=f:_identity(p)==f or (_ for _ in ()).throw(PermissionError('Medical parent custody changed'))for p,f in parents)
    for fd in (lock_fd,root_fd,output_fd):
        if fd is not None:callbacks.append(lambda fd=fd:os.close(fd))
    finish(primary,callbacks)
    _bounded(result,config.bounds.receipt_bytes)
    return result
