# SPDX-License-Identifier: MIT OR Apache-2.0
"""Closed original-journal lineage selection; no native or publication grant."""
from dataclasses import dataclass
import hashlib,json,os,re,stat
from pathlib import Path
from uuid import UUID
from .medical_commit_inspection_configuration import pinned_selection,read_selected
from .medical_producer import decode_medical
from .lifecycle import finish
from .original_medical_local_configuration import MedicalPinnedFile,OriginalMedicalRecoveryConfig,parse_original_medical_local

PROFILE='ashlar-original-medical-local-journal-inspection/0.1'
LINEAGE_PROFILE='ashlar-original-medical-journal-lineage/0.1'
STORAGE_PROFILE='ashlar-private-exclusive-original-journal-no-out-of-band-rollback/0.1'
TRANSPORT_HASHES=frozenset(('0261d6c09783b15850591206ae373b288b879e0a212a9fc9cc12aea7085aabbc','3f4225eb773cf6baaa6df87f09ab92f190eb6ac03e733e85e6d40cbdecceda5b','b2944f04bbb4d69f4c25824455b62ffd57e902c8e0a7b34671cc63ffcd63141a'))
LAUNCHER_SHA='92a6b22ad2fb95a4d22e602c69969e70d35eed314a298e0dd391908cbde160fb'
CODE_HASHES={'driver':'713f7844233e380f1a0c29c57e176c13ff11bbdc7387c8f87f694bf7565fce3e','capture':'64e23a9c8a1e440a084a38303ae8a80dd387b8c971d3d4b99819c03e306e9d03','lease':'36c2133874ef4952b302dc255053627b7037c9569b7888ccb0035f0b865dcaff'}

@dataclass(frozen=True)
class MedicalJournalSnapshot:
    path:Path
    sha256:str
    bytes:int
    def __post_init__(self):
        if (type(self.path)is not type(Path('/'))or not self.path.is_absolute()or self.path.resolve()!=self.path
                or type(self.sha256)is not str or re.fullmatch('[0-9a-f]{64}',self.sha256)is None
                or type(self.bytes)is not int or not 1<=self.bytes<=128*1024*1024):raise ValueError('Exact bounded original SQLite snapshot required')

@dataclass(frozen=True)
class MedicalJournalExecution:
    configuration:MedicalPinnedFile
    terminal:MedicalPinnedFile
    transport:MedicalPinnedFile
    driver:MedicalPinnedFile
    capture:MedicalPinnedFile
    lease:MedicalPinnedFile
    launcher:object=None
    def __post_init__(self):
        for name in ('configuration','terminal','transport','driver','capture','lease'):
            pin=getattr(self,name)
            if type(pin)is not MedicalPinnedFile:raise ValueError('Exact original execution file pin required')
            pin.__post_init__()
            if pin.bytes>(262144 if name in ('configuration','terminal')else 4194304):raise ValueError('Original execution pin exceeds bound')
        if self.launcher is not None:
            if type(self.launcher)is not MedicalPinnedFile or self.launcher.sha256!=LAUNCHER_SHA or self.launcher.bytes!=698:raise PermissionError('Exact reviewed diagnostic launcher required')
            self.launcher.__post_init__()
        if self.transport.sha256 not in TRANSPORT_HASHES or any(getattr(self,name).sha256!=digest for name,digest in CODE_HASHES.items()):raise PermissionError('Independently reviewed original write-ahead code required')

@dataclass(frozen=True)
class MedicalJournalLineage:
    installation_id:str
    request_digest:str
    installation:MedicalPinnedFile
    snapshots:tuple
    executions:tuple
    def __post_init__(self):
        if type(self.installation_id)is not str or str(UUID(self.installation_id))!=self.installation_id or type(self.request_digest)is not str or re.fullmatch('[0-9a-f]{64}',self.request_digest)is None:raise ValueError('Original lineage installation and request required')
        if type(self.installation)is not MedicalPinnedFile or self.installation.bytes>4194304:raise ValueError('Bounded original installation pin required')
        self.installation.__post_init__()
        for values,kind,minimum in ((self.snapshots,MedicalJournalSnapshot,2),(self.executions,MedicalJournalExecution,1)):
            if type(values)is not tuple or not minimum<=len(values)<=16 or any(type(v)is not kind for v in values):raise ValueError('Closed bounded lineage inventory required')
            for value in values:value.__post_init__()
        if sum(pin.bytes for pin in self.snapshots)>128*1024*1024:raise ValueError('Aggregate snapshot budget exceeded')
        if len({p.path for p in self.snapshots})!=len(self.snapshots):raise ValueError('Distinct ordered original snapshots required')

def parse_medical_journal_lineage(raw):
    value=decode_medical(raw,262144)
    if type(value)is not dict or set(value)!={'profile','installation_id','request_digest','installation','snapshots','executions','storage_profile'}or value['profile']!=LINEAGE_PROFILE or value['storage_profile']!=STORAGE_PROFILE:raise ValueError('Closed original journal lineage required')
    if type(value['snapshots'])is not list or type(value['executions'])is not list:raise ValueError('Original lineage inventories required')
    snapshots=[];executions=[]
    for pin in value['snapshots']:
        if type(pin)is not dict or set(pin)!={'path','bytes','sha256'}or type(pin['path'])is not str:raise ValueError('Closed snapshot pin required')
        snapshots.append(MedicalJournalSnapshot(Path(pin['path']),pin['sha256'],pin['bytes']))
    for execution in value['executions']:
        if type(execution)is not dict or set(execution)!={'configuration','terminal','code','launcher'}or type(execution['code'])is not dict or set(execution['code'])!={'transport','driver','capture','lease'}:raise ValueError('Closed original execution required')
        executions.append(MedicalJournalExecution(pinned_selection(execution['configuration']),pinned_selection(execution['terminal']),*(pinned_selection(execution['code'][key])for key in ('transport','driver','capture','lease')),None if execution['launcher']is None else pinned_selection(execution['launcher'])))
    return MedicalJournalLineage(value['installation_id'],value['request_digest'],pinned_selection(value['installation']),tuple(snapshots),tuple(executions))

@dataclass(frozen=True)
class OriginalMedicalJournalInspectionConfig:
    recovery:OriginalMedicalRecoveryConfig
    lineage:MedicalPinnedFile
    def __post_init__(self):
        if type(self.recovery)is not OriginalMedicalRecoveryConfig or type(self.lineage)is not MedicalPinnedFile or self.lineage.bytes>262144:raise ValueError('Exact recovery and pinned journal lineage required')
        self.recovery.__post_init__();self.lineage.__post_init__()
    @property
    def runtime(self):return self.recovery.runtime
    @property
    def inputs(self):return self.recovery.inputs
    @property
    def bounds(self):return self.recovery.bounds
    @property
    def producer(self):return self.recovery.producer
    def lineage_selection(self):return parse_medical_journal_lineage(read_selected(self.lineage))

def parse_original_medical_journal_inspection(raw):
    value=decode_medical(raw,262144)
    if type(value)is not dict or set(value)!={'profile','recovery','lineage'}or value['profile']!=PROFILE:raise ValueError('Closed journal inspection configuration required')
    return OriginalMedicalJournalInspectionConfig(parse_original_medical_local(json.dumps(value['recovery'],allow_nan=False).encode()),pinned_selection(value['lineage']))

def journal_inspection_pins(config):
    if type(config)is not OriginalMedicalJournalInspectionConfig:raise ValueError('Actual journal inspection selection required')
    config.__post_init__();lineage=config.lineage_selection()
    return (config.lineage,lineage.installation,*lineage.snapshots,*(pin for execution in lineage.executions for pin in ((execution.configuration,execution.terminal,execution.transport,execution.driver,execution.capture,execution.lease)+(()if execution.launcher is None else(execution.launcher,)))))

def journal_file_fingerprint(pin):
    """Verify exact original bytes without allocating a whole SQLite backup."""
    pin.__post_init__();fd=os.open(pin.path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK);primary=None;answer=None
    try:
        before=os.fstat(fd)
        def identity(s):return(s.st_dev,s.st_ino,s.st_uid,s.st_mode,s.st_nlink,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
        if not stat.S_ISREG(before.st_mode)or before.st_nlink!=1 or before.st_size!=pin.bytes:raise PermissionError('Original complete regular lineage file required')
        digest=hashlib.sha256();size=0
        while True:
            part=os.read(fd,min(65536,pin.bytes-size+1))
            if not part:break
            size+=len(part)
            if size>pin.bytes:raise PermissionError('Lineage file exceeds exact pin')
            digest.update(part)
        if size!=pin.bytes or digest.hexdigest()!=pin.sha256 or identity(os.fstat(fd))!=identity(before)or identity(pin.path.lstat())!=identity(before):raise PermissionError('Original lineage file changed')
        answer=(str(pin.path),pin.bytes,pin.sha256,identity(before))
    except BaseException as error:primary=error
    finish(primary,[lambda:os.close(fd)])
    return answer

def journal_inspection_selection_bytes(config):return tuple(journal_file_fingerprint(pin)for pin in journal_inspection_pins(config))
def renew_journal_inspection_selection(config,original):
    if journal_inspection_selection_bytes(config)!=original:raise PermissionError('Original complete journal lineage changed')
