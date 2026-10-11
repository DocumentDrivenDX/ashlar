"""Explicit ordinary connection acquisition; native checks own authority.

The application supplies an already configured DB-API connection factory.
This port reads no environment, container configuration or credentials.
"""
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class OrdinaryQueryConnectionFactory:
    open: Callable[[str], object]

    def __post_init__(self):
        if not callable(self.open):
            raise ValueError('ordinary-query-connection-factory-required')


def acquire_ordinary_query_connection(factory, role):
    if type(factory) is not OrdinaryQueryConnectionFactory:
        raise ValueError('ordinary-query-connection-factory-required')
    factory.__post_init__()
    if type(role) is not str or not role:
        raise ValueError('ordinary-query-role-required')
    return factory.open(role)
