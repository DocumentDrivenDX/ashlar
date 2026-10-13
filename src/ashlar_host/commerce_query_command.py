"""Captured trusted ordinary commerce provider; correspondence is not authority."""
import hashlib,re
from types import ModuleType
from .config import HostError
from .evolution_cli import InvocationFile,MAX_PROVIDER
from .ordinary_query_connection import OrdinaryQueryConnectionFactory
from .commerce_query_sources import CommerceQuerySourceFactory
from .implicit_arithmetic_configuration import QueryCommerceImplicitArithmeticConfig
from .count_star_configuration import QueryCommerceCountStarConfig
from .query_invocation_custody import CommerceQueryInvocationCustody


def load_commerce_query_provider(config,*,provider_path,provider_sha256,pack_path):
    """Capture operator bytes once and bind actual nominal session/source ports.

    Imported operator code is trusted. Hashes establish only exact selected bytes;
    live ordinary roles, source custody and native publication pins remain owning
    reader gates. No engine loading or native authority is provided here.
    """
    if type(config)not in (QueryCommerceImplicitArithmeticConfig,QueryCommerceCountStarConfig):
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
    source_builder=getattr(module,'commerce_query_source_factory',None)
    if not callable(source_builder):raise HostError('query-source-provider-configuration')
    source_factory=source_builder(config)
    if type(source_factory) is not CommerceQuerySourceFactory:raise HostError('query-source-provider-configuration')
    source_factory.__post_init__()
    custody = CommerceQueryInvocationCustody(provider, pack)
    custody.renew()
    return port,source_factory,custody
