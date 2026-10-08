"""Explicit source/revision dispatch for already admitted schema policies.

This inventory establishes neither native catalog acceptance nor compatibility.
Each policy still owns exact source/schema/identity/operation/carrier admission.
"""
from types import MappingProxyType
from .apply import ApplyError


class SchemaPolicies:
    def __init__(self, policies):
        if not isinstance(policies, dict) or not 1 <= len(policies) <= 128:
            raise ApplyError('Complete bounded schema policy inventory required')
        for key, policy in policies.items():
            if (not isinstance(key, tuple) or len(key) != 2
                    or any(not isinstance(value, str) or not value or '\x00' in value
                           or len(value.encode('utf-8')) > 1024 for value in key)
                    or not callable(policy)):
                raise ApplyError('Explicit source/revision policy binding required')
        self.policies = MappingProxyType(dict(policies))

    def __call__(self, change):
        key = (change.state.key.source, change.state.schema_revision)
        if key not in self.policies:
            raise ApplyError('Source schema revision has no admitted policy')
        if self.policies[key](change) is not None:
            raise ApplyError('Selected schema policy did not complete')
