"""Explicit trusted operator code supplies ordinary sessions, never authority.

Captured provider bytes are executed once. The SHA proves correspondence only;
the reader still checks actual role, protected ACK and native publication pins.
Operator code and its imports are trusted, not sandboxed or hermetic.
"""
import hashlib
import re
from types import ModuleType
from .config import HostError
from .evolution_cli import InvocationFile, MAX_PROVIDER
from .ordinary_query_connection import OrdinaryQueryConnectionFactory
from .implicit_arithmetic_configuration import QueryCommerceImplicitArithmeticConfig
from .query_invocation_custody import CommerceQueryInvocationCustody


def run_implicit_arithmetic_command(config, *, provider_path, provider_sha256,
                                    pack_path, diagnostics=None):
    if type(config) is not QueryCommerceImplicitArithmeticConfig:
        raise HostError('invalid-configuration')
    config.__post_init__()
    if type(provider_sha256) is not str or re.fullmatch('[0-9a-f]{64}', provider_sha256) is None:
        raise HostError('query-provider-custody')
    pack = InvocationFile.read(pack_path, MAX_PROVIDER)
    from .commerce_original_scenarios import commerce_original_cases
    commerce_original_cases(pack.raw)
    provider = InvocationFile.read(provider_path, MAX_PROVIDER)
    if hashlib.sha256(provider.raw).hexdigest() != provider_sha256:
        raise HostError('query-provider-custody')
    module = ModuleType('ashlar_ordinary_query_' + provider_sha256)
    module.__file__ = str(provider.path)
    exec(compile(provider.raw, str(provider.path), 'exec'), module.__dict__)
    factory = getattr(module, 'ordinary_query_connection_factory', None)
    if not callable(factory):
        raise HostError('query-provider-configuration')
    port = factory(config)
    if type(port) is not OrdinaryQueryConnectionFactory:
        raise HostError('query-provider-configuration')
    port.__post_init__()
    custody = CommerceQueryInvocationCustody(provider, pack)
    custody.renew()
    from .implicit_arithmetic_query import query_commerce_implicit_arithmetic
    return query_commerce_implicit_arithmetic(config, ordinary_session=port,
        pack_bytes=pack.raw, diagnostics=diagnostics, invocation_custody=custody)
