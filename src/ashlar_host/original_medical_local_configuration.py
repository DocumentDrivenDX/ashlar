"""Closed historical medical selections; correspondence is not current authority.

Only values and bounded decoding live here. No initializer, publisher, ordinary
connection, native SDK import or successful operation port is supplied.
"""
from dataclasses import dataclass
import json
from pathlib import Path
import re
import stat
from uuid import UUID
from ashlar.medical_source import SOURCE_SHA, GRAPH_SHA, PUBLIC_SHA
from .ack import AckScope
from .config import ProducerConfig
from .delta_custody import operation_capacity

_ROLES = ('object_current', 'edge_current', 'tombstone', 'whole_source_history', 'attempts', 'manifest')


def _path(value):
    if (type(value) is not type(Path()) or not value.is_absolute()
            or str(value) != str(value.resolve()) or len(str(value).encode()) > 4096):
        raise ValueError('Exact canonical bounded absolute medical path required')


def _digest(value):
    if type(value) is not str or re.fullmatch('[0-9a-f]{64}', value) is None:
        raise ValueError('Explicit medical byte digest required')


def _text(value, maximum=1024):
    if type(value) is not str or not value or len(value.encode()) > maximum or any(ord(c) < 32 for c in value):
        raise ValueError('Bounded explicit medical selection required')


def _uuid(value):
    if type(value) is not str or str(UUID(value)) != value:
        raise ValueError('Canonical native target UUID required')


@dataclass(frozen=True)
class MedicalPinnedFile:
    path: Path
    sha256: str
    bytes: int

    def __post_init__(self):
        _path(self.path); _digest(self.sha256)
        if type(self.bytes) is not int or not 1 <= self.bytes <= 67108864:
            raise ValueError('Explicit nonempty bounded medical file selection required')
        if self.path.exists():
            observed = self.path.lstat()
            if not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1 or observed.st_size != self.bytes:
                raise ValueError('Original single-link regular selected file required')


@dataclass(frozen=True)
class OriginalMedicalInputs:
    model: MedicalPinnedFile
    graph: MedicalPinnedFile
    baseline: MedicalPinnedFile

    def __post_init__(self):
        values = (self.model, self.graph, self.baseline)
        if any(type(v) is not MedicalPinnedFile for v in values):
            raise ValueError('Exact typed original medical pins required')
        for value in values: value.__post_init__()
        if (tuple(v.sha256 for v in values) != (SOURCE_SHA, GRAPH_SHA, PUBLIC_SHA)
                or sum(v.bytes for v in values) > 4194304 or len({v.path for v in values}) != 3):
            raise ValueError('Complete distinct original medical selections required')


@dataclass(frozen=True)
class OriginalMedicalRuntime:
    python: MedicalPinnedFile
    python_version: str
    java_home: Path
    java: MedicalPinnedFile
    java_version: str
    jvm_config: MedicalPinnedFile
    pyspark_root: Path
    spark_version: str
    delta_root: Path
    delta_version: str
    jars: tuple
    qualification: MedicalPinnedFile

    def __post_init__(self):
        for value in (self.python, self.java, self.jvm_config, self.qualification):
            if type(value) is not MedicalPinnedFile: raise ValueError('Exact runtime provenance selections required')
            value.__post_init__()
        for value in (self.java_home, self.pyspark_root, self.delta_root): _path(value)
        for value in (self.python_version, self.java_version, self.spark_version, self.delta_version):
            _text(value, 128)
        if (self.java.path != self.java_home/'bin/java' or self.jvm_config.path != self.java_home/'lib/jvm.cfg'
                or type(self.jars) is not tuple or len(self.jars) != 2
                or any(type(v) is not MedicalPinnedFile for v in self.jars)):
            raise ValueError('Explicit complete local Delta runtime/JAR selections required')
        for value in self.jars: value.__post_init__()
        if self.java.bytes > 1048576 or self.jvm_config.bytes > 65536 or self.qualification.bytes > 1048576:
            raise ValueError('Bounded selected runtime descriptors required')
        if len({v.path for v in self.jars}) != 2 or any(v.path.suffix != '.jar' for v in self.jars):
            raise ValueError('Distinct explicitly selected JAR files required')
        # Version and qualification pins select evidence, never assert SDK
        # compatibility. The runtime owner must corroborate actual origins and
        # the selected qualified profile before acquiring any native session.


@dataclass(frozen=True)
class OriginalMedicalBounds:
    input_bytes: int
    rows: int
    cell_bytes: int
    total_bytes: int
    stream_bytes: int
    receipt_bytes: int
    journal_bytes: int
    native_seconds: int
    whole_seconds: int
    master: str
    driver_memory: str

    def __post_init__(self):
        for value, ceiling in zip((self.input_bytes, self.rows, self.cell_bytes, self.total_bytes,
                self.stream_bytes, self.receipt_bytes, self.journal_bytes),
                (4194304, 10000, 1048576, 16777216, 1048576, 1048576, 134217728)):
            if type(value) is not int or not 1 <= value <= ceiling:
                raise ValueError('Explicit finite medical capture bounds required')
        if (type(self.native_seconds) is not int or not 1 <= self.native_seconds <= 900
                or type(self.whole_seconds) is not int or not self.native_seconds < self.whole_seconds <= 960
                or type(self.master) is not str or self.master != 'local[1]'
                or type(self.driver_memory) is not str or self.driver_memory != '512m'):
            raise ValueError('Exact supervised small local medical limits required')


@dataclass(frozen=True)
class OriginalMedicalLocalSource:
    host: str
    port: int
    database: str
    authentication: str
    scope: AckScope
    source_schema: str
    source_signature_sha256: str
    writer_role: str
    reader_role: str
    ack_role: str
    connect_seconds: int
    statement_ms: int
    lock_ms: int

    def __post_init__(self):
        if (type(self.host) is not str or self.host != '127.0.0.1'
                or type(self.port) is not int or not 1024 <= self.port <= 65535
                or type(self.authentication) is not str or self.authentication != 'isolated-loopback-trust'
                or type(self.scope) is not AckScope):
            raise ValueError('Explicit isolated medical loopback fixture required')
        if type(self.database) is not str or re.fullmatch('[a-z][a-z0-9_]{0,62}', self.database) is None:
            raise ValueError('Explicit dedicated fixture database required')
        self.scope.__post_init__()
        if (self.scope.feed, self.scope.epoch) != ('private-original-medical-fixture', 'original-medical-v1'):
            raise ValueError('Original medical feed/epoch required')
        names = (self.source_schema, self.writer_role, self.reader_role, self.ack_role)
        if (any(type(v) is not str or re.fullmatch('[a-z][a-z0-9_]{0,62}', v) is None for v in names)
                or len(set(names[1:])) != 3):
            raise ValueError('Distinct explicit ordinary medical roles required')
        _digest(self.source_signature_sha256)
        for value, ceiling in ((self.connect_seconds, 10), (self.statement_ms, 60000), (self.lock_ms, 5000)):
            if type(value) is not int or not 1 <= value <= ceiling:
                raise ValueError('Explicit finite ordinary session bounds required')
        # Later session owners must refuse ambient PG configuration and pass
        # closed hostaddr/password/passfile/SSL/GSS options. No session is opened here.


@dataclass(frozen=True)
class OriginalMedicalTarget:
    table: str
    path: Path
    uuid: str

    def __post_init__(self):
        if type(self.table) is not str or self.table not in _ROLES: raise ValueError('Exact six medical native roles required')
        _path(self.path); _uuid(self.uuid)


@dataclass(frozen=True)
class _MedicalConfig:
    profile: str
    output: Path
    inputs: OriginalMedicalInputs
    producer: ProducerConfig
    runtime: OriginalMedicalRuntime
    source: OriginalMedicalLocalSource
    bounds: OriginalMedicalBounds

    def _common(self, operation):
        if type(self.profile) is not str or self.profile != 'ashlar-original-medical-local-'+operation+'/0.1':
            raise ValueError('Exact selected medical operation profile required')
        _path(self.output)
        for value, owner in ((self.inputs, OriginalMedicalInputs), (self.producer, ProducerConfig),
                (self.runtime, OriginalMedicalRuntime), (self.source, OriginalMedicalLocalSource),
                (self.bounds, OriginalMedicalBounds)):
            if type(value) is not owner: raise ValueError('Exact medical configuration owners required')
            value.__post_init__()
        if sum(v.bytes for v in (self.inputs.model, self.inputs.graph, self.inputs.baseline)) > self.bounds.input_bytes:
            raise ValueError('Selected medical input aggregate exceeds capture bound')
        files = (self.inputs.model.path, self.inputs.graph.path, self.inputs.baseline.path,
                 self.runtime.python.path, self.runtime.java.path, self.runtime.jvm_config.path,
                 self.runtime.qualification.path, *(v.path for v in self.runtime.jars))
        if len(set(files)) != len(files): raise ValueError('Distinct original/runtime selected file roles required')
        if any(self.output == p or self.output in p.parents or p in self.output.parents for p in self._protected_paths()):
            raise ValueError('Separate exclusive medical evidence output required')

    def _protected_paths(self):
        return (self.inputs.model.path, self.inputs.graph.path, self.inputs.baseline.path,
                self.producer.source, self.producer.bun, self.producer.git, self.runtime.python.path,
                self.runtime.java_home, self.runtime.pyspark_root, self.runtime.delta_root,
                self.runtime.qualification.path, *(v.path for v in self.runtime.jars),
                *(v.path.parent for v in self.runtime.jars))

    def _installation(self, root, installation_id, capacity):
        _path(root); _text(installation_id)
        if installation_id != self.source.scope.installation_id:
            raise ValueError('Native installation and original protected ACK scope must agree')
        if root == self.output or root in self.output.parents or self.output in root.parents:
            raise ValueError('Separate native and evidence roots required')
        for p in self._protected_paths():
            if p == root or root in p.parents or p in root.parents: raise ValueError('Selected original/runtime/producer paths cannot be native targets')
        selected = operation_capacity(capacity)
        if selected is None: raise ValueError('Explicit medical operation-intent capacity required')
        object.__setattr__(self, 'capacity', selected)


@dataclass(frozen=True)
class OriginalMedicalInitializationConfig(_MedicalConfig):
    root: Path
    installation_id: str
    capacity: object
    schemas: MedicalPinnedFile

    def __post_init__(self):
        self._common('initialization'); self._installation(self.root, self.installation_id, self.capacity)
        if type(self.schemas) is not MedicalPinnedFile: raise ValueError('Explicit checked generated schema correspondence required')
        self.schemas.__post_init__()
        if self.schemas.path in (self.root, self.output) or self.root in self.schemas.path.parents or self.output in self.schemas.path.parents:
            raise ValueError('Generated schemas cannot be replaced by native/evidence output')


@dataclass(frozen=True)
class _ExistingMedicalConfig(_MedicalConfig):
    root: Path
    installation_id: str
    capacity: object
    installation: MedicalPinnedFile
    journal: Path
    targets: tuple

    def _existing(self, operation):
        self._common(operation); self._installation(self.root, self.installation_id, self.capacity)
        if type(self.installation) is not MedicalPinnedFile: raise ValueError('Original immutable installation handoff required')
        self.installation.__post_init__(); _path(self.journal)
        if (self.journal != self.root/'operations.sqlite' or type(self.targets) is not tuple or len(self.targets) != 6
                or any(type(t) is not OriginalMedicalTarget for t in self.targets)):
            raise ValueError('Original journal and complete six native targets required')
        for target in self.targets: target.__post_init__()
        if (tuple(t.table for t in self.targets) != _ROLES or len({t.uuid for t in self.targets}) != 6
                or any(t.path != self.root/t.table for t in self.targets)
                or self.installation.path == self.root or self.root in self.installation.path.parents
                or self.output == self.installation.path or self.output in self.installation.path.parents
                or self.installation.path in self.output.parents):
            raise ValueError('Exact original six-target handoff correspondence required')


@dataclass(frozen=True)
class OriginalMedicalPublicationConfig(_ExistingMedicalConfig):
    def __post_init__(self): self._existing('publication')


@dataclass(frozen=True)
class OriginalMedicalRecoveryConfig(_ExistingMedicalConfig):
    request: MedicalPinnedFile

    def __post_init__(self):
        self._existing('recovery')
        if type(self.request) is not MedicalPinnedFile:
            raise ValueError('Exact retained original recovery request required')
        self.request.__post_init__()
        paths = (self.request.path, self.installation.path)
        if len(set(paths)) != 2 or any(self.output == p or self.output in p.parents or p in self.output.parents for p in paths):
            raise ValueError('Distinct retained recovery originals and exclusive evidence output required')


@dataclass(frozen=True)
class OriginalMedicalReplayConfig(_ExistingMedicalConfig):
    request: MedicalPinnedFile
    publication: MedicalPinnedFile

    def __post_init__(self):
        self._existing('replay'); _retained(self)


@dataclass(frozen=True)
class OriginalMedicalReadConfig(_ExistingMedicalConfig):
    request: MedicalPinnedFile
    publication: MedicalPinnedFile

    def __post_init__(self):
        self._existing('read'); _retained(self)


def _retained(config):
    for value in (config.request, config.publication):
        if type(value) is not MedicalPinnedFile: raise ValueError('Exact immutable original request/publication required')
        value.__post_init__()
    paths = (config.request.path, config.publication.path, config.installation.path)
    if len(set(paths)) != 3 or any(config.output == p or config.output in p.parents or p in config.output.parents for p in paths):
        raise ValueError('Distinct retained originals and exclusive evidence output required')


def _closed(value, keys):
    if type(value) is not dict or set(value) != set(keys): raise ValueError('Closed explicit medical members required')
    return value


def _decode_path(value):
    if type(value) is not str or not value or '\x00' in value or len(value.encode()) > 4096:
        raise ValueError('Bounded absolute medical path text required')
    result = Path(value)
    if str(result) != value: raise ValueError('Unnormalized medical path refused')
    _path(result); return result


def _pin(value):
    value = _closed(value, ('path', 'sha256', 'bytes'))
    return MedicalPinnedFile(_decode_path(value['path']), value['sha256'], value['bytes'])


def parse_original_medical_local(raw):
    """Decode one closed selection without reading inputs or acquiring authority."""
    if type(raw) is not bytes or not 0 < len(raw) <= 262144: raise ValueError('Bounded medical configuration bytes required')
    depth = 0; string = False; escaped = False
    for c in raw:
        if string:
            if escaped: escaped = False
            elif c == 92: escaped = True
            elif c == 34: string = False
        elif c == 34: string = True
        elif c in (91, 123):
            depth += 1
            if depth > 32: raise ValueError('Bounded medical configuration depth required')
        elif c in (93, 125): depth -= 1
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value: raise ValueError('Duplicate medical configuration member')
            value[key] = item
        return value
    def constant(_): raise ValueError('Nonfinite medical configuration refused')
    value = json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
    if type(value) is not dict or type(value.get('profile')) is not str: raise ValueError('Selected medical profile required')
    operations = {'ashlar-original-medical-local-'+op+'/0.1': op for op in ('initialization', 'publication', 'replay', 'read', 'recovery')}
    if value['profile'] not in operations: raise ValueError('Unsupported medical operation profile')
    operation = operations[value['profile']]
    common = {'profile', 'output', 'inputs', 'producer', 'runtime', 'source', 'bounds', 'root', 'installation_id', 'capacity'}
    extra = {'schemas'} if operation == 'initialization' else {'installation', 'journal', 'targets'}
    if operation in ('replay', 'read'): extra |= {'request', 'publication'}
    if operation == 'recovery': extra |= {'request'}
    _closed(value, common | extra)
    inputs = _closed(value['inputs'], ('model', 'graph', 'baseline'))
    inputs = OriginalMedicalInputs(*(_pin(inputs[k]) for k in ('model', 'graph', 'baseline')))
    p = _closed(value['producer'], ('source', 'bun', 'git', 'timeout_seconds', 'maximum_output_bytes', 'maximum_receipt_bytes'))
    producer = ProducerConfig(*(_decode_path(p[k]) for k in ('source', 'bun', 'git')),
                              p['timeout_seconds'], p['maximum_output_bytes'], p['maximum_receipt_bytes'])
    r = _closed(value['runtime'], OriginalMedicalRuntime.__dataclass_fields__)
    if type(r['jars']) is not list: raise ValueError('Explicit ordered JAR selections required')
    runtime = OriginalMedicalRuntime(_pin(r['python']), r['python_version'], _decode_path(r['java_home']),
        _pin(r['java']), r['java_version'], _pin(r['jvm_config']), _decode_path(r['pyspark_root']),
        r['spark_version'], _decode_path(r['delta_root']), r['delta_version'], tuple(_pin(v) for v in r['jars']), _pin(r['qualification']))
    s = _closed(value['source'], OriginalMedicalLocalSource.__dataclass_fields__)
    scope = _closed(s['scope'], AckScope.__dataclass_fields__)
    source = OriginalMedicalLocalSource(s['host'], s['port'], s['database'], s['authentication'], AckScope(**scope),
        s['source_schema'], s['source_signature_sha256'], s['writer_role'], s['reader_role'], s['ack_role'],
        s['connect_seconds'], s['statement_ms'], s['lock_ms'])
    b = _closed(value['bounds'], OriginalMedicalBounds.__dataclass_fields__)
    bounds = OriginalMedicalBounds(**b)
    values = dict(profile=value['profile'], output=_decode_path(value['output']), inputs=inputs, producer=producer,
        runtime=runtime, source=source, bounds=bounds, root=_decode_path(value['root']),
        installation_id=value['installation_id'], capacity=value['capacity'])
    if operation == 'initialization': return OriginalMedicalInitializationConfig(**values, schemas=_pin(value['schemas']))
    if type(value['targets']) is not list: raise ValueError('Explicit ordered target selections required')
    targets = []
    for target in value['targets']:
        t = _closed(target, ('table', 'path', 'uuid'))
        targets.append(OriginalMedicalTarget(t['table'], _decode_path(t['path']), t['uuid']))
    values.update(installation=_pin(value['installation']), journal=_decode_path(value['journal']), targets=tuple(targets))
    if operation == 'publication': return OriginalMedicalPublicationConfig(**values)
    if operation == 'recovery': return OriginalMedicalRecoveryConfig(**values, request=_pin(value['request']))
    values.update(request=_pin(value['request']), publication=_pin(value['publication']))
    return (OriginalMedicalReplayConfig if operation == 'replay' else OriginalMedicalReadConfig)(**values)
