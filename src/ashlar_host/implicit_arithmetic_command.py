"""Explicit trusted operator ordinary sessions for the original042 command."""
from .config import HostError
from .implicit_arithmetic_configuration import QueryCommerceImplicitArithmeticConfig
from .commerce_query_command import load_commerce_query_provider


def run_implicit_arithmetic_command(config, *, provider_path, provider_sha256,
                                    pack_path, diagnostics=None, selected_case_ids=()):
    if type(config) is not QueryCommerceImplicitArithmeticConfig:
        raise HostError('invalid-configuration')
    config.__post_init__()
    from .commerce_query_selection import select_commerce_query_cases
    try:select_commerce_query_cases('implicit-arithmetic',selected_case_ids)
    except ValueError:raise HostError('invalid-commerce-query-selection')from None
    port,source_factory,custody=load_commerce_query_provider(config,provider_path=provider_path,
        provider_sha256=provider_sha256,pack_path=pack_path)
    from .implicit_arithmetic_query import query_commerce_implicit_arithmetic
    return query_commerce_implicit_arithmetic(config, ordinary_session=port,source_factory=source_factory,
        pack_bytes=custody.pack.raw, diagnostics=diagnostics, invocation_custody=custody, selected_case_ids=selected_case_ids)
