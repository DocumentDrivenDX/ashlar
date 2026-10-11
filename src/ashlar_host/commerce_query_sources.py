"""Closed ordinary source inputs; binding grants no provisioning or write rights."""
from dataclasses import dataclass
from .existing_outbox_authority import OutboxAuthorityRegistration


@dataclass(frozen=True, repr=False)
class CommerceQuerySourceFactory:
    registration: OutboxAuthorityRegistration

    def __post_init__(self):
        if type(self.registration) is not OutboxAuthorityRegistration:
            raise ValueError('Exact ordinary source registration required')
        r=self.registration
        object.__setattr__(self,'registration',OutboxAuthorityRegistration(
            r.source,r.source_role,r.ack_role,r.database,r.metadata_factory))

    def metadata(self):
        self.__post_init__()
        r=self.registration
        return {'source':r.source.metadata(),'source_role':r.source_role,
                'ack_role':r.ack_role,'database':r.database}
