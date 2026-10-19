# SPDX-License-Identifier: MIT OR Apache-2.0
"""Trusted explicit provider example; replace all three constants before use."""
from pathlib import Path

CONFIGURATION = Path('REPLACE_WITH_ABSOLUTE_PUBLICATION_CONFIGURATION_PATH')
CONFIGURATION_SHA256 = 'REPLACE_WITH_PUBLICATION_CONFIGURATION_SHA256'
CONTAINER = 'REPLACE_WITH_EXPLICIT_PRIVATE_FIXTURE_CONTAINER_NAME'


def selected(config):
    import hashlib
    import re
    from ashlar_host.evolution_cli import InvocationFile, MAX_PROVIDER
    from ashlar_host.original_commerce_local_cli import parse_original_commerce_local
    from ashlar_host.original_commerce_local_setup import original_commerce_local_source_ports
    from ashlar_host.implicit_arithmetic_configuration import (
        ExplicitCommercePostgresEndpoint, QueryCommerceImplicitArithmeticConfig)
    from ashlar_host.count_star_configuration import QueryCommerceCountStarConfig

    if (not isinstance(CONFIGURATION, Path) or not CONFIGURATION.is_absolute()
            or 'REPLACE_WITH' in str(CONFIGURATION)
            or type(CONFIGURATION_SHA256) is not str
            or re.fullmatch('[0-9a-f]{64}', CONFIGURATION_SHA256) is None
            or type(CONTAINER) is not str or not CONTAINER
            or 'REPLACE_WITH' in CONTAINER):
        raise ValueError('Explicit provider selection required')
    if (type(config) not in (QueryCommerceImplicitArithmeticConfig,QueryCommerceCountStarConfig)
            or type(config.postgres) is not ExplicitCommercePostgresEndpoint):
        raise ValueError('Explicit original fixture endpoint required')
    config.__post_init__()
    captured = InvocationFile.read(CONFIGURATION, MAX_PROVIDER)
    if hashlib.sha256(captured.raw).hexdigest() != CONFIGURATION_SHA256:
        raise ValueError('Exact original publication configuration required')
    source = parse_original_commerce_local(captured.raw, initialize=False).source
    endpoint = config.postgres
    if (endpoint.container, endpoint.host, endpoint.port, endpoint.database) != (
            CONTAINER, source.host, source.port, source.database):
        raise ValueError('Explicit original query endpoint differs')
    ports = original_commerce_local_source_ports(source)
    captured.renew(MAX_PROVIDER)
    return ports


def ordinary_query_connection_factory(config):
    from ashlar_host.ordinary_query_connection import OrdinaryQueryConnectionFactory
    registration = selected(config).registration

    def connection(role):
        if type(role) is not str:
            raise ValueError('Exact ordinary role required')
        if role == registration.source_role:
            return registration.source.connection_factory(None)
        if role == registration.ack_role:
            return registration.metadata_factory(None)
        raise ValueError('Unselected ordinary query role refused')

    return OrdinaryQueryConnectionFactory(connection)


def commerce_query_source_factory(config):
    from ashlar_host.commerce_query_sources import CommerceQuerySourceFactory
    return CommerceQuerySourceFactory(selected(config).registration)
