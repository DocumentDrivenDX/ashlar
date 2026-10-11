"""Dedicated existing medical installation lease; no native or source grants."""
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl,json,os,stat
from pathlib import Path
from .delta_custody import DeltaTarget,LocalDeltaTransport,encoded,operation_capacity
from .evolution_producer import snapshot
from .lifecycle import finish
ROLES={'attempts','manifest','object_current','edge_current','tombstone','whole_source_history'}

@dataclass(frozen=True)
class OriginalMedicalInstallationSelection:
    installation_id:str
    journal:Path
    targets:tuple
    capacity:object=None
    def __post_init__(self):
        import uuid
        if type(self.installation_id)is not str or str(uuid.UUID(self.installation_id))!=self.installation_id or type(self.journal)is not type(Path()) or not self.journal.is_absolute() or type(self.targets)is not tuple or len(self.targets)!=6 or any(type(x)is not DeltaTarget for x in self.targets):raise ValueError('Exact complete existing medical installation required')
        if {x.table.split('.')[-1]for x in self.targets}!=ROLES or len({x.table for x in self.targets})!=6 or len({x.path for x in self.targets})!=6:raise ValueError('Six distinct medical target roles required')
        object.__setattr__(self,'targets',tuple(DeltaTarget(x.table,x.path,x.uuid)for x in self.targets))
        object.__setattr__(self,'capacity',operation_capacity(self.capacity))

def identity(path,directory=False):
    if type(path) is not type(Path()) or path.resolve()!=path:raise PermissionError('Original canonical owner required')
    s=path.lstat()
    if (not (stat.S_ISDIR(s.st_mode) if directory else stat.S_ISREG(s.st_mode)) or s.st_uid!=os.getuid() or s.st_mode&0o022):
        raise PermissionError('Original private owner required')
    return s.st_dev,s.st_ino

class OriginalMedicalInstallationLease:
    """Actual POSIX cooperating-operator lease plus complete existing registry."""
    def __init__(self,operator,context,fd,lock):
        self.selection=operator;self.context=context;self.fd=fd;self.lock=lock;self.closed=False
        self.parent=identity(operator.journal.parent,True);self.journal=identity(operator.journal)
        self.lock_identity=identity(lock)
        if any('`' in str(x.path) or '\x00' in str(x.path) for x in operator.targets):raise PermissionError('Closed native path spelling required')
        self.registry=encoded({'installation':operator.installation_id,'targets':[{'table':x.table,'path':str(x.path),'uuid':x.uuid} for x in sorted(operator.targets,key=lambda x:x.table)],**({} if operator.capacity is None else {'operation_capacity':dict(operator.capacity)})})
        self.reservation=encoded({'profile':'ashlar-private-native-installation-reservation/0.1','registry':json.loads(self.registry),'original_journal_path':str(operator.journal)})+'\n'
        self.original_targets=tuple((x.table,str(x.path),x.uuid) for x in sorted(operator.targets,key=lambda x:x.table))
        self.target_identities=tuple((x.path,identity(x.path,True)) for x in operator.targets)
    def renew(self,context):
        if type(self.selection) is not OriginalMedicalInstallationSelection:raise PermissionError('Exact installed owner required')
        self.selection.__post_init__()
        if self.closed or context is not self.context or identity(self.selection.journal.parent,True)!=self.parent or identity(self.selection.journal)!=self.journal or identity(self.lock)!=self.lock_identity:
            raise PermissionError('Original held installation lease changed')
        s=os.fstat(self.fd)
        if (s.st_dev,s.st_ino)!=self.lock_identity:raise PermissionError('Original lock descriptor changed')
        for path,original in self.target_identities:
            if identity(path,True)!=original:raise PermissionError('Original native target directory changed')
            p=path/'.ashlar-local-installation-reservation.json'
            if p.is_symlink() or snapshot(p,4194304)!=self.reservation.encode():raise PermissionError('Original complete target reservation changed')
        LocalDeltaTransport.require_existing_journal(self.selection.journal,self.selection.installation_id,self.selection.targets,capacity=self.selection.capacity)
    def verify(self,context,transport):
        self.renew(context)
        if type(transport) is not LocalDeltaTransport or transport.journal_path!=self.selection.journal or transport.reservation!=self.reservation:
            raise PermissionError('Actual original existing transport required')
        actual=tuple((x.table,str(x.path),x.uuid) for x in sorted(transport.targets.values(),key=lambda x:x.table))
        expected=tuple((x.table,str(x.path),x.uuid) for x in sorted(self.selection.targets,key=lambda x:x.table))
        if actual!=self.original_targets or expected!=self.original_targets or transport.db.execute('SELECT original FROM local_installation WHERE id=1').fetchall()!=[(self.registry,)]:
            raise PermissionError('Complete original native registry changed')
    @classmethod
    @contextmanager
    def open(cls,operator,context):
        if type(operator) is not OriginalMedicalInstallationSelection:raise ValueError('Exact original publication operator required')
        operator.__post_init__();identity(operator.journal.parent,True);identity(operator.journal)
        lock=operator.journal.parent/'.evolution-operator.lock'
        fd=None;owner=None;primary=None;locked=False
        try:
            fd=os.open(lock,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);locked=True
            LocalDeltaTransport.require_existing_journal(operator.journal,operator.installation_id,operator.targets,capacity=operator.capacity)
            owner=cls(operator,context,fd,lock);owner.renew(context);yield owner
        except BaseException as error:primary=error
        def close():
            if owner is not None:owner.closed=True
            if fd is not None:os.close(fd)
        finish(primary,([lambda:owner.renew(context)] if owner is not None else [])+([lambda:LocalDeltaTransport.require_existing_journal(operator.journal,operator.installation_id,operator.targets,capacity=operator.capacity)]if locked else[])+[close])

