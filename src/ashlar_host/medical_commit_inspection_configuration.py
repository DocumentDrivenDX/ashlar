"""Closed passive selection for local medical outcome observation; no recovery grant."""
from dataclasses import dataclass
import hashlib,json,os
from pathlib import Path
from .medical_producer import MedicalFile,decode_medical
from .original_medical_local_configuration import MedicalPinnedFile,OriginalMedicalRecoveryConfig,parse_original_medical_local

PROFILE='ashlar-original-medical-local-commit-inspection/0.1'
STRICT_VISIBILITY='ashlar-private-local-same-uid-process-visibility/0.1'
MAPPED_VISIBILITY='ashlar-private-local-same-uid-mapped-vnode-visibility/0.1'

@dataclass(frozen=True)
class MedicalProcessSelection:
    ps:MedicalPinnedFile
    java:MedicalPinnedFile
    maximum_processes:int
    maximum_output_bytes:int
    timeout_seconds:int
    owner_uid:int
    visibility_profile:str
    mapped_vnode_helper:object=None
    def __post_init__(self):
        for pin in (self.ps,self.java):
            if type(pin)is not MedicalPinnedFile:raise ValueError('Explicit physical process executable pins required')
            pin.__post_init__()
        if self.ps.path!=Path('/bin/ps'):raise ValueError('Explicit native Darwin process observer required')
        if type(self.owner_uid)is not int or self.owner_uid!=os.getuid() or self.visibility_profile not in (STRICT_VISIBILITY,MAPPED_VISIBILITY):
            raise ValueError('Explicit current-user private development visibility profile required')
        if self.visibility_profile==STRICT_VISIBILITY:
            if self.mapped_vnode_helper is not None:raise ValueError('Strict visibility does not select a mapped helper')
        else:
            if type(self.mapped_vnode_helper)is not MedicalPinnedFile:raise ValueError('Explicit built mapped-vnode helper required')
            self.mapped_vnode_helper.__post_init__()

        for value,maximum in ((self.maximum_processes,4096),(self.maximum_output_bytes,1048576),(self.timeout_seconds,5)):
            if type(value)is not int or not 1<=value<=maximum:raise ValueError('Bounded process observation required')

@dataclass(frozen=True)
class OriginalMedicalCommitInspectionConfig:
    recovery:OriginalMedicalRecoveryConfig
    original_configurations:tuple
    process_observer:MedicalProcessSelection
    def __post_init__(self):
        if type(self.recovery)is not OriginalMedicalRecoveryConfig:raise ValueError('Exact original recovery selection required')
        self.recovery.__post_init__()
        if type(self.original_configurations)is not tuple or not 1<=len(self.original_configurations)<=32 or any(type(v)is not MedicalPinnedFile for v in self.original_configurations):
            raise ValueError('Bounded retained original operation configurations required')
        for pin in self.original_configurations:pin.__post_init__()
        if len({p.path for p in self.original_configurations})!=len(self.original_configurations):raise ValueError('Distinct original configuration pins required')
        if type(self.process_observer)is not MedicalProcessSelection:raise ValueError('Exact process observer selection required')
        self.process_observer.__post_init__()
        if self.process_observer.java!=self.recovery.runtime.java:raise ValueError('Same original selected JDK binary required')
        if any(pin.bytes>262144 for pin in self.original_configurations)or sum(pin.bytes for pin in self.original_configurations)>self.bounds.input_bytes:
            raise ValueError('Complete original configuration inventory exceeds input bound')
        for pin in inspection_pins(self):
            if self.recovery.output==pin.path or self.recovery.output in pin.path.parents or pin.path in self.recovery.output.parents:
                raise ValueError('Inspection output must be separate from retained selections')

    @property
    def runtime(self):return self.recovery.runtime
    @property
    def inputs(self):return self.recovery.inputs
    @property
    def bounds(self):return self.recovery.bounds
    @property
    def producer(self):return self.recovery.producer


def pinned_selection(raw):
    if type(raw)is not dict or set(raw)!={'path','bytes','sha256'}or type(raw['path'])is not str:raise ValueError('Closed medical file pin required')
    return MedicalPinnedFile(Path(raw['path']),raw['sha256'],raw['bytes'])


def parse_original_medical_commit_inspection(raw):
    value=decode_medical(raw,262144)
    if type(value)is not dict or set(value)!={'profile','recovery','original_configurations','process_observer'}or value['profile']!=PROFILE:
        raise ValueError('Closed medical commit inspection required')
    process=value['process_observer']
    fields={'ps','java','maximum_processes','maximum_output_bytes','timeout_seconds','owner_uid','visibility_profile'}
    if type(process)is dict and process.get('visibility_profile')==MAPPED_VISIBILITY:fields.add('mapped_vnode_helper')
    if type(process)is not dict or set(process)!=fields:
        raise ValueError('Closed physical process observation required')
    if type(value['original_configurations'])is not list:raise ValueError('Original configuration inventory required')
    return OriginalMedicalCommitInspectionConfig(parse_original_medical_local(json.dumps(value['recovery'],allow_nan=False).encode()),
        tuple(pinned_selection(v)for v in value['original_configurations']),
        MedicalProcessSelection(pinned_selection(process['ps']),pinned_selection(process['java']),process['maximum_processes'],process['maximum_output_bytes'],process['timeout_seconds'],process['owner_uid'],process['visibility_profile'],pinned_selection(process['mapped_vnode_helper'])if 'mapped_vnode_helper'in process else None))


def read_selected(pin):
    raw=MedicalFile(pin.path,min(pin.bytes,4194304)).read()
    if len(raw)!=pin.bytes or hashlib.sha256(raw).hexdigest()!=pin.sha256:raise PermissionError('Original inspection selection changed')
    return raw


def inspection_selection_bytes(config):
    config.__post_init__()
    return tuple(read_selected(pin)for pin in inspection_pins(config))

def renew_inspection_selection(config,original):
    if inspection_selection_bytes(config)!=original:raise PermissionError('Complete original inspection selection changed')


def inspection_pins(config):
    return (config.process_observer.ps,config.process_observer.java,*config.original_configurations)+(()if config.process_observer.mapped_vnode_helper is None else(config.process_observer.mapped_vnode_helper,))
