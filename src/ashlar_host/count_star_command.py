"""Explicit trusted ordinary-provider entry for selected original041 queries."""
from .config import HostError
from .count_star_configuration import QueryCommerceCountStarConfig
from .implicit_arithmetic_configuration import ExplicitCommercePostgresEndpoint
from .commerce_query_command import load_commerce_query_provider
from .commerce_query_selection import select_commerce_query_cases


def run_count_star_command(config,*,provider_path,provider_sha256,pack_path,diagnostics=None,selected_case_ids=()):
    if type(config)is not QueryCommerceCountStarConfig or type(config.postgres)is not ExplicitCommercePostgresEndpoint:
        raise HostError('explicit-commerce-query-endpoint-required')
    config.__post_init__()
    try:select_commerce_query_cases('count-star',selected_case_ids)
    except ValueError:raise HostError('invalid-commerce-query-selection')from None
    ordinary,source,custody=load_commerce_query_provider(config,provider_path=provider_path,
        provider_sha256=provider_sha256,pack_path=pack_path)
    from .count_star_query import query_commerce_count_star
    return query_commerce_count_star(config,ordinary_session=ordinary,source_factory=source,
        invocation_custody=custody,diagnostics=diagnostics,selected_case_ids=selected_case_ids)
