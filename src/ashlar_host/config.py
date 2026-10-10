"""Explicit application configuration; no environment or credential discovery."""
from dataclasses import dataclass
from pathlib import Path
from ashlar.weft_path_decode import PathDecodeConfig
from .evolution_plan import EvolutionAttemptPlan, EvolutionPlanJournal
from .evolution_run import EvolutionRunRequest, EvolutionRunDefinition, EvolutionRunPolicy, EvolutionRunAttemptJournal
from .path_capture import PathCaptureConfig
import ipaddress
import json
import os
import re
import stat
import unicodedata
from types import MappingProxyType
from typing import Callable, Mapping, Literal, Optional, TypeVar, Protocol
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


_TransportResult = TypeVar('_TransportResult')


def load_diagnostics_config(path: Path) -> DiagnosticsConfig:
    """Load one explicit bounded JSON file; never discover ambient settings."""
    descriptor = None
    primary = None
    result = None
    try:
        if not isinstance(path, Path) or not path.is_absolute():
            raise ValueError()
        inspected = path.lstat()
        if not stat.S_ISREG(inspected.st_mode) or not 0 < inspected.st_size <= 32768:
            raise ValueError()
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        def identity(value):
            return (value.st_dev, value.st_ino, value.st_mode, value.st_size,
                    value.st_mtime_ns, value.st_ctime_ns)
        opening = os.fstat(descriptor)
        if identity(opening) != identity(inspected):
            raise ValueError()
        data = bytearray()
        while True:
            part = os.read(descriptor, min(4096, 32769 - len(data)))
            if not part:
                break
            data.extend(part)
            if len(data) > 32768:
                raise ValueError()
        if (len(data) != opening.st_size or identity(os.fstat(descriptor)) != identity(opening)
                or identity(path.lstat()) != identity(opening)):
            raise ValueError()
        def pairs(items):
            values = {}
            for name, value in items:
                if name in values:
                    raise ValueError()
                values[name] = value
            return values
        def nonfinite(_value):
            raise ValueError()
        def integer(value):
            if len(value.lstrip('-')) > 20:
                raise ValueError()
            return int(value)
        depth = 0
        quoted = escaped = False
        for byte in data:
            if quoted:
                if escaped:
                    escaped = False
                elif byte == 92:
                    escaped = True
                elif byte == 34:
                    quoted = False
            elif byte == 34:
                quoted = True
            elif byte in (91, 123):
                depth += 1
                if depth > 12:
                    raise ValueError()
            elif byte in (93, 125):
                depth -= 1
                if depth < 0:
                    raise ValueError()
        values = json.loads(bytes(data).decode('utf8'), object_pairs_hook=pairs,
                            parse_constant=nonfinite, parse_float=nonfinite, parse_int=integer)
        names = {'profile', 'capture_root', 'endpoint', 'headers', 'tls',
                 'ca_file', 'environment', 'limits'}
        if type(values) is not dict or set(values) != names:
            raise ValueError()
        if any(type(values[name]) is not str for name in
               ('profile', 'capture_root', 'endpoint', 'tls', 'environment')):
            raise ValueError()
        if values['ca_file'] is not None and type(values['ca_file']) is not str:
            raise ValueError()
        limits = values['limits']
        if type(limits) is not dict or set(limits) != set(DiagnosticsLimits.__dataclass_fields__):
            raise ValueError()
        headers = values['headers']
        if type(headers) is not list or any(type(pair) is not list or len(pair) != 2
                or any(type(value) is not str for value in pair) for pair in headers):
            raise ValueError()
        origins = {name: 'explicit' for name in names - {'limits'}}
        origins.update({'limits.' + name: 'explicit' for name in limits})
        result = DiagnosticsConfig(values['profile'], Path(values['capture_root']),
            SecretText(values['endpoint']), tuple((name, SecretText(value)) for name, value in headers),
            values['tls'], Path(values['ca_file']) if values['ca_file'] is not None else None,
            values['environment'], DiagnosticsLimits(**limits), origins)
    except BaseException as error:
        primary = error
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except BaseException as error:
                if primary is None or (isinstance(primary, Exception) and not isinstance(error, Exception)):
                    primary = error
                else:
                    try: primary.cleanup_failed = True
                    except BaseException: pass
    if primary is not None:
        if isinstance(primary, Exception):
            raise HostError('diagnostics-configuration') from None
        raise primary
    return result


def with_diagnostics_transport(
        config: DiagnosticsConfig,
        use: Callable[[str, tuple[tuple[str, str], ...], str, Optional[Path]], _TransportResult]
        ) -> _TransportResult:
    """Pass secrets only to the trusted transport initializer, never a logger.

    This purpose-specific port is an ownership boundary, not a security sandbox
    or a promise of Python object erasure. The callback must not retain, print,
    fingerprint or return these values to a diagnostic caller.
    """
    if type(config) is not DiagnosticsConfig or not callable(use):
        raise HostError('diagnostics-configuration')
    return use(config.endpoint._reveal(),
               tuple((name, value._reveal()) for name, value in config.headers),
               config.tls, config.ca_file)


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


@dataclass(frozen=True)
class EvolutionAdmissionConfig:
    """Explicit selected public producer resources; paths confer no authority."""
    source: Path
    bun: Path
    git: Path
    timeout_seconds: int
    maximum_output_bytes: int
    maximum_receipt_bytes: int

    def __post_init__(self) -> None:
        for p in (self.source, self.bun, self.git):
            _path(p)
        _bound(self.timeout_seconds, 60)
        _bound(self.maximum_output_bytes, 1024 * 1024)
        _bound(self.maximum_receipt_bytes, 4 * 1024 * 1024)


class EvolutionTransactionDriver(Protocol):
    def publish_evolution_attempt(self, journal: EvolutionPlanJournal, *, context: object,
                                  original_plan: Optional[EvolutionAttemptPlan] = None,
                                  expected_sha256: Optional[str] = None): ...


@dataclass(frozen=True, repr=False)
class FreshEvolutionTransactionConfig:
    """One already planned original transaction; no installation initialization."""
    driver: EvolutionTransactionDriver
    journal: EvolutionPlanJournal
    context: object
    plan_bytes: bytes

    def __post_init__(self):
        if (type(self.journal) is not EvolutionPlanJournal or not self.journal.path.is_absolute()
                or not callable(getattr(self.journal.policy, 'admit', None))
                or not callable(getattr(self.driver, 'publish_evolution_attempt', None))):
            raise HostError('evolution-transaction-configuration')
        EvolutionAttemptPlan(self.plan_bytes)


@dataclass(frozen=True, repr=False)
class ResumeEvolutionTransactionConfig:
    """One retained original transaction; no replacement plan or initializer."""
    driver: EvolutionTransactionDriver
    journal: EvolutionPlanJournal
    context: object
    expected_sha256: str

    def __post_init__(self):
        if (type(self.journal) is not EvolutionPlanJournal or not self.journal.path.is_absolute()
                or not callable(getattr(self.journal.policy, 'admit', None))
                or not callable(getattr(self.driver, 'publish_evolution_attempt', None))
                or type(self.expected_sha256) is not str
                or not re.fullmatch('[0-9a-f]{64}', self.expected_sha256)):
            raise HostError('evolution-transaction-configuration')


class CommerceEvolutionDriver(EvolutionTransactionDriver, Protocol):
    def writer(self, stream: str, context: object): ...
    def capture_evolution_run(self, request: EvolutionRunRequest, *, context: object) -> EvolutionRunDefinition: ...
    def validate_evolution_run(self, original: EvolutionRunDefinition, *, context: object) -> None: ...
    def verify_evolution_sources(self, sources: object, *, context: object) -> None: ...
    def plan_evolution_transaction(self, original: EvolutionRunDefinition, ordinal: int,
                                   previous_progress: Mapping[str, object], *, context: object) -> EvolutionAttemptPlan: ...
    def publish_evolution_attempt_held(self, journal: EvolutionRunAttemptJournal, *, context: object,
                                      expected_sha256: str): ...


def _evolution_run_ports(driver, path, policy, producer):
    if (not isinstance(path, Path) or not path.is_absolute() or type(producer) is not EvolutionAdmissionConfig
            or any(not callable(getattr(policy, name, None)) for name in ('admit_run', 'admit_ledger', 'admit_attempt'))
            or any(not callable(getattr(driver, name, None)) for name in ('writer', 'capture_evolution_run',
                'validate_evolution_run', 'verify_evolution_sources', 'plan_evolution_transaction', 'publish_evolution_attempt_held'))):
        raise HostError('evolution-run-configuration')
    producer.__post_init__()


@dataclass(frozen=True, repr=False)
class FreshCommerceEvolutionConfig:
    driver: CommerceEvolutionDriver
    ledger_path: Path
    policy: EvolutionRunPolicy
    producer: EvolutionAdmissionConfig
    context: object
    request: EvolutionRunRequest

    def __post_init__(self):
        _evolution_run_ports(self.driver, self.ledger_path, self.policy, self.producer)
        if type(self.request) is not EvolutionRunRequest:
            raise HostError('evolution-run-configuration')
        self.request.__post_init__()


@dataclass(frozen=True, repr=False)
class ResumeCommerceEvolutionConfig:
    driver: CommerceEvolutionDriver
    ledger_path: Path
    policy: EvolutionRunPolicy
    producer: EvolutionAdmissionConfig
    context: object
    expected_sha256: str

    def __post_init__(self):
        _evolution_run_ports(self.driver, self.ledger_path, self.policy, self.producer)
        if type(self.expected_sha256) is not str or not re.fullmatch('[0-9a-f]{64}', self.expected_sha256):
            raise HostError('evolution-run-configuration')
