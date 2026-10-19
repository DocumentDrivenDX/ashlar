# SPDX-License-Identifier: MIT OR Apache-2.0
"""Captured command inputs, independent of native execution authority."""
from dataclasses import dataclass
from .evolution_cli import InvocationFile, MAX_PROVIDER
from .lifecycle import finish


@dataclass(frozen=True)
class CommerceQueryInvocationCustody:
    provider: InvocationFile
    pack: InvocationFile

    def __post_init__(self):
        if type(self.provider) is not InvocationFile or type(self.pack) is not InvocationFile:
            raise ValueError('exact-query-invocation-custody-required')
        self.provider.__post_init__()
        self.pack.__post_init__()

    def renew(self):
        self.__post_init__()
        finish(None, [lambda: self.provider.renew(MAX_PROVIDER),
                      lambda: self.pack.renew(MAX_PROVIDER)])
