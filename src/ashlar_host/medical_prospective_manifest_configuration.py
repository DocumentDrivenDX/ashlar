"""Closed operator selection for a new prospective medical manifest submission."""
from dataclasses import dataclass,replace
import hashlib
from pathlib import Path
import re
from uuid import UUID
from .medical_producer import decode_medical
from .medical_commit_inspection_configuration import pinned_selection,read_selected
from .medical_inspection_selection import parse_medical_inspection,medical_inspection_selection_bytes
from .original_medical_local_configuration import MedicalPinnedFile

PROFILE='ashlar-original-medical-prospective-manifest/0.1'
AUTHORIZATION_PROFILE='ashlar-original-medical-prospective-manifest-authorization/0.1'
AUTHORIZATION_FIELDS={'profile','authorization_id','installation_id','request_digest','applied_artifact_sha256','proposal_sha256','inspection_sha256','inspection_configuration_sha256','manifest_uuid','anchor_version'}

def decode_authorization(raw):
    value=decode_medical(raw,262144)
    if type(value)is not dict or set(value)!=AUTHORIZATION_FIELDS or value['profile']!=AUTHORIZATION_PROFILE:
        raise ValueError('Closed prospective manifest authorization required')
    for name in ('authorization_id','installation_id','manifest_uuid'):
        if type(value[name])is not str or str(UUID(value[name]))!=value[name]:raise ValueError('Canonical authorization UUID required')
    for name in ('request_digest','applied_artifact_sha256','proposal_sha256','inspection_sha256','inspection_configuration_sha256'):
        if type(value[name])is not str or re.fullmatch('[0-9a-f]{64}',value[name])is None:raise ValueError('Exact authorization SHA256 required')
    if type(value['anchor_version'])is not int or value['anchor_version']!=1:raise ValueError('Original empty manifest anchor one required')
    return value

@dataclass(frozen=True)
class OriginalMedicalProspectiveManifestConfig:
    inspection_configuration:MedicalPinnedFile
    inspection:MedicalPinnedFile
    proposal:MedicalPinnedFile
    authorization:MedicalPinnedFile
    output:Path
    def __post_init__(self):
        for pin in (self.inspection_configuration,self.inspection,self.proposal,self.authorization):
            if type(pin)is not MedicalPinnedFile:raise ValueError('Exact prospective file pins required')
            pin.__post_init__()
            if pin.bytes>262144:raise ValueError('Prospective selection exceeds file bound')
        if type(self.output)is not type(Path('/'))or not self.output.is_absolute()or self.output.resolve()!=self.output:
            raise ValueError('Canonical absolute fresh prospective output required')
        pins=(self.inspection_configuration,self.inspection,self.proposal,self.authorization)
        if len({p.path for p in pins})!=4:raise ValueError('Distinct prospective selections required')
        if any(self.output==p.path or self.output in p.path.parents or p.path in self.output.parents for p in pins):
            raise ValueError('Prospective output must be separate from original selections')
    def inspection_selection(self):
        selected=parse_medical_inspection(read_selected(self.inspection_configuration))
        return replace(selected,recovery=replace(selected.recovery,output=self.output))
    @property
    def recovery(self):return self.inspection_selection().recovery
    @property
    def runtime(self):return self.recovery.runtime
    @property
    def inputs(self):return self.recovery.inputs
    @property
    def bounds(self):return self.recovery.bounds
    @property
    def producer(self):return self.recovery.producer

def parse_original_medical_prospective_manifest(raw):
    value=decode_medical(raw,262144)
    if type(value)is not dict or set(value)!={'profile','inspection_configuration','inspection','proposal','authorization','output'}or value['profile']!=PROFILE or type(value['output'])is not str:
        raise ValueError('Closed prospective manifest configuration required')
    return OriginalMedicalProspectiveManifestConfig(*(pinned_selection(value[k])for k in ('inspection_configuration','inspection','proposal','authorization')),Path(value['output']))

def prospective_selection_bytes(config):
    if type(config)is not OriginalMedicalProspectiveManifestConfig:raise ValueError('Actual prospective configuration required')
    config.__post_init__()
    raw=tuple(read_selected(pin)for pin in (config.inspection_configuration,config.inspection,config.proposal,config.authorization))
    decode_authorization(raw[3])
    return raw+medical_inspection_selection_bytes(config.inspection_selection())

def renew_prospective_selection(config,original):
    if prospective_selection_bytes(config)!=original:raise PermissionError('Original prospective selections changed')
