"""Explicit application configuration; no environment or credential discovery."""
from dataclasses import dataclass
from pathlib import Path


class HostError(ValueError):
    """Payload-free failure category for the installed host composition."""


def _path(value: Path) -> None:
    if not isinstance(value, Path) or not value.is_absolute():
        raise HostError('absolute-path-required')


def _bound(value: int, maximum: int) -> None:
    if type(value) is not int or not 0 < value <= maximum:
        raise HostError('invalid-finite-bound')


@dataclass(frozen=True)
class PrivatePostgresConfig:
    container: str
    host: str
    port: int
    database: str

    def __post_init__(self) -> None:
        if type(self.port) is not int or (self.container,self.host,self.port,self.database) != ('ashlar-e2e-truss-pg17','127.0.0.1',15432,'truss_e2e'):
            raise HostError('unsupported-private-postgres-profile')


@dataclass(frozen=True)
class ProducerConfig:
    source: Path
    bun: Path
    git: Path
    timeout_seconds: int
    maximum_output_bytes: int
    maximum_receipt_bytes: int

    def __post_init__(self) -> None:
        for p in (self.source,self.bun,self.git): _path(p)
        _bound(self.timeout_seconds,60)
        _bound(self.maximum_output_bytes,1024*1024)
        _bound(self.maximum_receipt_bytes,4*1024*1024)


@dataclass(frozen=True)
class PublishCommerceConfig:
    output: Path
    jars: Path
    model: Path
    graph: Path
    producer: ProducerConfig
    postgres: PrivatePostgresConfig
    source_system: str
    binding_profile: str

    def __post_init__(self) -> None:
        for p in (self.output,self.jars,self.model,self.graph): _path(p)
        if not isinstance(self.producer,ProducerConfig) or not isinstance(self.postgres,PrivatePostgresConfig):raise HostError('invalid-configuration')
        if self.source_system!='private-original-commerce-fixture' or self.binding_profile!='ashlar-commerce-development-bindings/0.2':raise HostError('unsupported-development-source-profile')


@dataclass(frozen=True)
class QueryCommerceConfig:
    index: Path
    installation: Path
    publication: Path
    output: Path
    jars: Path
    model: Path
    graph: Path
    producer: ProducerConfig
    postgres: PrivatePostgresConfig

    def __post_init__(self) -> None:
        for p in (self.index,self.installation,self.publication,self.output,self.jars,self.model,self.graph): _path(p)
        if not isinstance(self.producer,ProducerConfig) or not isinstance(self.postgres,PrivatePostgresConfig):raise HostError('invalid-configuration')
