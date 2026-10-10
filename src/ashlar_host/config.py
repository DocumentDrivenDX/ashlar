"""Explicit application configuration; no environment or credential discovery."""
from dataclasses import dataclass
from pathlib import Path
from ashlar.weft_path_decode import PathDecodeConfig
from .path_capture import PathCaptureConfig
import ipaddress
import re
import stat
import unicodedata
from types import MappingProxyType
from typing import Mapping, Literal, Optional
from urllib.parse import urlsplit


class SecretText:
    """Immutable host secret; reveal only at the owned transport boundary."""
    __slots__ = ('__value',)

    def __init__(self, value: str):
        if type(value) is not str:
            raise HostError('diagnostics-configuration')
        object.__setattr__(self, '_SecretText__value', value)

    def __setattr__(self, name, value):
        raise AttributeError('immutable-secret')

    def __delattr__(self, name):
        raise AttributeError('immutable-secret')

    def __repr__(self):
        return 'SecretText([redacted])'

    def __str__(self):
        return '[redacted]'

    def __reduce__(self):
        raise TypeError('secret-serialization-refused')

    def __deepcopy__(self, memo):
        return self

    def _reveal(self) -> str:
        return self.__value


@dataclass(frozen=True)
class DiagnosticsLimits:
    max_event_bytes: int
    max_queue_records: int
    max_queue_bytes: int
    max_segment_bytes: int
    max_capture_bytes: int
    max_emissions: int
    max_attempts: int
    export_timeout_ms: int
    shutdown_timeout_ms: int
    retention_seconds: int

    def __post_init__(self):
        bounds = ((512,4096), (1,128), (4096,524288), (4096,4194304),
                  (4096,16777216), (1,10000), (1,64), (1,1000),
                  (1,2000), (1,604800))
        for name, (minimum, maximum) in zip(self.__dataclass_fields__, bounds):
            value = getattr(self, name)
            if type(value) is not int or not minimum <= value <= maximum:
                raise HostError('diagnostics-configuration')
        if (self.max_event_bytes > min(self.max_queue_bytes, self.max_segment_bytes)
                or self.max_segment_bytes > self.max_capture_bytes):
            raise HostError('diagnostics-configuration')


@dataclass(frozen=True, repr=False)
class DiagnosticsConfig:
    """CONTRACT-006 values; no SDK import, ambient discovery or effects."""
    profile: str
    capture_root: Path
    endpoint: SecretText
    headers: tuple[tuple[str, SecretText], ...]
    tls: Literal['system', 'custom-ca', 'loopback-test']
    ca_file: Optional[Path]
    environment: str
    limits: DiagnosticsLimits
    origins: Mapping[str, str]

    def __repr__(self):
        return 'DiagnosticsConfig([redacted])'

    def __post_init__(self):
        try:
            self._validate()
        except (ValueError, TypeError, AttributeError, OSError, UnicodeError):
            raise HostError('diagnostics-configuration') from None

    def _validate(self):
        if (type(self.profile) is not str or self.profile != 'ashlar-host-otel-http/0.1'
                or not isinstance(self.capture_root, Path) or not self.capture_root.is_absolute()
                or type(self.endpoint) is not SecretText or type(self.limits) is not DiagnosticsLimits
                or type(self.environment) is not str
                or self.environment not in ('development','test','staging','production')
                or type(self.tls) is not str or self.tls not in ('system','custom-ca','loopback-test')):
            raise ValueError()
        raw = self.endpoint._reveal()
        if (not 1 <= len(raw.encode('utf8')) <= 2048
                or any(c.isspace() or c == '\\' or unicodedata.category(c) == 'Cc' for c in raw)):
            raise ValueError()
        endpoint = urlsplit(raw)
        host = endpoint.hostname
        if (not host or not endpoint.netloc or endpoint.username is not None
                or endpoint.password is not None or endpoint.query or endpoint.fragment
                or '?' in raw or '#' in raw or endpoint.path.rstrip('/').endswith(('/v1/logs','/v1/traces','/v1/metrics'))
                or (endpoint.port is not None and not 1 <= endpoint.port <= 65535)):
            raise ValueError()
        if self.tls == 'loopback-test':
            try:
                loopback = ipaddress.ip_address(host).is_loopback
            except ValueError:
                loopback = host == 'localhost'
            if endpoint.scheme != 'http' or not loopback:
                raise ValueError()
        elif endpoint.scheme != 'https':
            raise ValueError()
        if self.tls == 'custom-ca':
            if not isinstance(self.ca_file, Path) or not self.ca_file.is_absolute():
                raise ValueError()
            info = self.ca_file.lstat()
            if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= 1024 * 1024:
                raise ValueError()
        elif self.ca_file is not None:
            raise ValueError()
        if type(self.headers) is not tuple or len(self.headers) > 8:
            raise ValueError()
        names = set()
        for item in self.headers:
            if type(item) is not tuple or len(item) != 2:
                raise ValueError()
            name, secret = item
            if (type(name) is not str or not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]{1,64}", name)
                    or name.lower() in names or type(secret) is not SecretText):
                raise ValueError()
            names.add(name.lower())
            value = secret._reveal()
            if len(value.encode('utf8')) > 2048 or any(unicodedata.category(c) == 'Cc' for c in value):
                raise ValueError()
        if not isinstance(self.origins, Mapping):
            raise ValueError()
        origins = dict(self.origins)
        leaves = {'profile','capture_root','endpoint','headers','tls','ca_file','environment'}
        leaves.update('limits.' + name for name in self.limits.__dataclass_fields__)
        allowed = {'explicit','environment-secret','development-env','environment-toml','default-toml','field-default'}
        if set(origins) != leaves or any(type(v) is not str or v not in allowed for v in origins.values()):
            raise ValueError()
        operators = {'capture_root','endpoint','headers','tls','environment'}
        if self.tls == 'custom-ca':
            operators.add('ca_file')
        if any(origins[name] not in ('explicit','environment-secret','development-env') for name in operators):
            raise ValueError()
        object.__setattr__(self, 'origins', MappingProxyType(origins))


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


@dataclass(frozen=True)
class QueryCommercePathsConfig:
    """Separately selected Paths profile with explicit finite capture bounds."""
    index: Path
    installation: Path
    publication: Path
    output: Path
    jars: Path
    model: Path
    graph: Path
    producer: ProducerConfig
    postgres: PrivatePostgresConfig
    maximum_artifact_bytes: int
    capture: PathCaptureConfig
    decoder: PathDecodeConfig
    profile: str = 'paths'

    def __post_init__(self) -> None:
        if type(self.profile) is not str or self.profile not in ('paths', 'paths-keys'):
            raise HostError('invalid-paths-profile')
        for p in (self.index, self.installation, self.publication, self.output,
                  self.jars, self.model, self.graph):
            _path(p)
        if (type(self.producer) is not ProducerConfig
                or type(self.postgres) is not PrivatePostgresConfig
                or type(self.capture) is not PathCaptureConfig
                or type(self.decoder) is not PathDecodeConfig):
            raise HostError('invalid-configuration')
        _bound(self.maximum_artifact_bytes, 16 * 1024 * 1024)
        _bound(self.capture.maximum_rows, 1000)
        _bound(self.capture.maximum_cell_bytes, 16 * 1024 * 1024)
        _bound(self.capture.maximum_total_cell_bytes, 64 * 1024 * 1024)
        if self.decoder.maximum_cell_bytes > self.capture.maximum_cell_bytes:
            raise HostError('decoder-exceeds-capture-bound')
