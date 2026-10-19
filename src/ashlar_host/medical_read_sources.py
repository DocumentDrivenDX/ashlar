# SPDX-License-Identifier: MIT OR Apache-2.0
"""Exact medical ordinary read registration; no publisher or source write grant."""
from dataclasses import dataclass
from .existing_outbox_authority import OutboxAuthorityRegistration
from .original_medical_publication import MedicalAdmission


@dataclass(frozen=True,repr=False)
class MedicalReadSourceFactory:
    registration:OutboxAuthorityRegistration

    def __post_init__(self):
        if type(self.registration)is not OutboxAuthorityRegistration:raise ValueError('Exact medical ordinary source registration required')
        r=self.registration
        object.__setattr__(self,'registration',OutboxAuthorityRegistration(r.source,r.source_role,r.ack_role,r.database,r.metadata_factory))
        if (r.source.scope.feed,r.source.scope.epoch)!=('private-original-medical-fixture','original-medical-v1'):raise ValueError('Exact original medical feed/epoch required')

    def metadata(self):
        self.__post_init__();r=self.registration
        return {'source':r.source.metadata(),'source_role':r.source_role,'ack_role':r.ack_role,'database':r.database}

    def require_admission(self,admission):
        self.__post_init__()
        if type(admission)is not MedicalAdmission:raise PermissionError('Exact original medical admission required')
        if (admission.batch.feed,admission.batch.epoch)!=(self.registration.source.scope.feed,self.registration.source.scope.epoch):raise PermissionError('Original medical admission/source scope differs')
        if admission.metadata()['profile']!='ashlar-original-medical-public-dataset-admission/0.1':raise PermissionError('Original medical read admission profile required')
