"""Selected local host implementation; native qualification remains version-scoped."""
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl, hashlib, json, re, sqlite3, os, stat
from types import MappingProxyType
from urllib.parse import quote
from pathlib import Path
from ashlar.native import SQLResult
from ashlar.publication import validate_table_identifier
from .evolution_verification_types import VerificationBounds

class LocalDeltaError(ValueError):
    pass

class LocalDeltaUncertain(LocalDeltaError):
    pass
LOCAL_OPERATION_CAPACITY_8M = MappingProxyType({'profile': 'ashlar-private-local-operation-capacity/0.1', 'max_intent_bytes': 8 * 1024 * 1024})

def operation_capacity(value):
    if value is None:
        return None
    if type(value) not in (dict, MappingProxyType) or set(value) != set(LOCAL_OPERATION_CAPACITY_8M) or value.get('profile') != LOCAL_OPERATION_CAPACITY_8M['profile'] or (type(value.get('max_intent_bytes')) is not int) or (value['max_intent_bytes'] != LOCAL_OPERATION_CAPACITY_8M['max_intent_bytes']):
        raise LocalDeltaError('Explicit supported closed private local operation capacity required')
    return MappingProxyType(dict(value))

def bounded_local_json_bytes(value, maximum):
    total = 0

    def add(count):
        nonlocal total
        total += count
        if total > maximum:
            raise LocalDeltaError('Bounded original local operation required')

    def string(text):
        add(2)
        for character in text:
            point = ord(character)
            if character in ('\\', '"'):
                add(2)
            elif point < 32:
                add(2 if point in (8, 9, 10, 12, 13) else 6)
            elif 55296 <= point <= 57343:
                raise LocalDeltaError('Exact UTF8 operation text required')
            else:
                add(1 if point < 128 else 2 if point < 2048 else 3 if point < 65536 else 4)

    def visit(item):
        if type(item) is str:
            string(item)
        elif item is None:
            add(4)
        elif type(item) is bool:
            add(4 if item else 5)
        elif type(item) is int:
            add(len(str(item)))
        elif type(item) is dict:
            add(2 + max(0, len(item) - 1))
            for (key, child) in item.items():
                string(key)
                add(1)
                visit(child)
        elif type(item) is list:
            add(2 + max(0, len(item) - 1))
            for child in item:
                visit(child)
        else:
            raise LocalDeltaError('Closed exact operation JSON required')
    visit(value)
    return total

def original_operation_intent(installation_id, operation, intent_digest, sql, parameters, target, capacity=None):
    body = {'profile': 'ashlar-local-delta-operation/0.1', 'installation_id': installation_id, 'operation': operation, 'request_digest': intent_digest, 'statement': sql, 'parameters': parameters, 'table': target.table, 'path': str(target.path), 'uuid': target.uuid}
    capacity = operation_capacity(capacity)
    if capacity is not None:
        body['operation_capacity'] = dict(capacity)
    bounded_local_json_bytes(body, capacity['max_intent_bytes'] if capacity is not None else 4194304)
    return encoded(body)

def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)

def sha(value):
    return hashlib.sha256(value.encode()).hexdigest()

def child_operation(operation, statement, parameters):
    return operation + ':' + sha(encoded({'statement': statement, 'parameters': parameters}))

@dataclass(frozen=True)
class DeltaTarget:
    table: str
    path: Path
    uuid: str

    def __post_init__(self):
        validate_table_identifier(self.table)
        path = Path(self.path).resolve()
        if not path.is_dir() or any((c in str(path) for c in ('`', '\x00', '\n'))) or (not isinstance(self.uuid, str)) or (not self.uuid):
            raise LocalDeltaError('Existing explicit native table/path/UUID required')
        object.__setattr__(self, 'path', path)

class LocalDeltaTransport:

    def __init__(self, spark, journal_path, installation_id, targets, policy, *, capacity=None, verification_bounds=None):
        if verification_bounds is not None:
            if type(verification_bounds) is not VerificationBounds:
                raise LocalDeltaError('Exact constructor verification bounds required')
            verification_bounds.__post_init__()
        self.observation_bounds = verification_bounds
        try:
            self._open(spark, journal_path, installation_id, targets, policy, create=False, context=None, capacity=capacity)
        finally:
            self.observation_bounds = None

    @classmethod
    def initialize(cls, spark, journal_path, installation_id, targets, policy, *, context, capacity=None):
        result = cls.__new__(cls)
        result._open(spark, journal_path, installation_id, targets, policy, create=True, context=context, capacity=capacity)
        return result

    def _open(self, spark, journal_path, installation_id, targets, policy, *, create, context, capacity):
        self.operation_capacity = operation_capacity(capacity)
        if type(installation_id) is not str or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9:_-]{0,1023}', installation_id) or (not targets):
            raise LocalDeltaError('Explicit original installation/table registry required')
        if not callable(getattr(policy, 'writer', None)) or not callable(getattr(policy, 'admit', None)):
            raise LocalDeltaError('Mandatory current local writer/intent policy required')
        self.spark = spark
        self.policy = policy
        self.installation_id = installation_id
        self.targets = MappingProxyType({t.table: t for t in targets})
        self.journal_path = Path(journal_path).absolute()
        self._registered = False
        if len(self.targets) != len(targets) or len({t.path for t in targets}) != len(targets):
            raise LocalDeltaError('Closed distinct native table registry required')
        if self.journal_path.is_symlink():
            raise LocalDeltaError('Private non-symlink original journal required')
        registry_body = {'installation': installation_id, 'targets': [{'table': t.table, 'path': str(t.path), 'uuid': t.uuid} for t in sorted(targets, key=lambda t: t.table)]}
        if self.operation_capacity is not None:
            registry_body['operation_capacity'] = dict(self.operation_capacity)
        registry = encoded(registry_body)
        self.registry_sha = sha(registry)
        self.journal_sha = sha(str(self.journal_path.resolve()))
        self.reservation = encoded({'profile': 'ashlar-private-native-installation-reservation/0.1', 'registry': json.loads(registry), 'original_journal_path': str(self.journal_path.resolve())}) + '\n'
        self._profile()
        if create:
            initialization = {'profile': 'ashlar-local-delta-installation/0.1', **json.loads(registry)}
            admitted = False
            with self.policy.writer('initialize:' + installation_id, context) as permit:
                if permit is not None or self.policy.admit(initialization, context) is not None:
                    raise LocalDeltaError('Explicit fresh installation authority required')
                with self._lock():
                    if self.journal_path.exists():
                        raise LocalDeltaError('Fresh installation cannot replace an existing journal')
                    for target in self.targets.values():
                        if self._reservation_path(target).exists() or self._reservation_path(target).is_symlink():
                            raise LocalDeltaError('Original installation reservation already exists; no replacement initialization')
                        detail = self._detail(target)
                        history = self.original_history(target)
                        if any((k.startswith('ashlar.local.') for k in detail.get('properties', {}))) or len(history) != 1 or int(history[0]['version']) != 0 or (history[0].get('userMetadata') is not None) or (self.spark.read.format('delta').option('versionAsOf', 0).load(str(target.path)).count() != 0):
                            raise LocalDeltaError('Original empty fresh native tables required; lost journal cannot restart installation')
                    for target in self.targets.values():
                        descriptor = os.open(self._reservation_path(target), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 384)
                        with os.fdopen(descriptor, 'w') as file:
                            file.write(self.reservation)
                            file.flush()
                            os.fsync(file.fileno())
                        directory = os.open(target.path, os.O_RDONLY)
                        try:
                            os.fsync(directory)
                        finally:
                            os.close(directory)
                    for target in self.targets.values():
                        metadata = encoded({'profile': 'ashlar-local-delta-installation-commit/0.1', 'installation_id': installation_id, 'registry_sha256': self.registry_sha, 'journal_sha256': self.journal_sha, 'uuid': target.uuid})
                        prior = self.spark.conf.get('spark.databricks.delta.commitInfo.userMetadata', None)
                        self.spark.conf.set('spark.databricks.delta.commitInfo.userMetadata', metadata)
                        try:
                            self.spark.sql('ALTER TABLE delta.`' + str(target.path) + "` SET TBLPROPERTIES ('ashlar.local.installation.id'='" + installation_id + "','ashlar.local.registry.sha256'='" + self.registry_sha + "','ashlar.local.journal.sha256'='" + self.journal_sha + "')").collect()
                        finally:
                            if prior is None:
                                self.spark.conf.unset('spark.databricks.delta.commitInfo.userMetadata')
                            else:
                                self.spark.conf.set('spark.databricks.delta.commitInfo.userMetadata', prior)
                        if self._detail(target)['id'] != target.uuid or self.original_history(target)[0].get('userMetadata') != metadata or int(self.original_history(target)[0]['version']) != 1:
                            raise LocalDeltaUncertain('Original native installation registration uncertain; no replacement initialization')
                    self._registered = True
                    for target in self.targets.values():
                        self._detail(target)
                    if self.policy.admit(initialization, context) is not None:
                        raise LocalDeltaError('Closing fresh installation authority incomplete')
                    for target in self.targets.values():
                        self._detail(target)
                    self._profile()
                    self._journal(registry, create=True)
                    admitted = True
            if not admitted:
                raise LocalDeltaError('Fresh installation admission suppressed')
        else:
            if not self.journal_path.is_file():
                raise LocalDeltaError('Original journal missing; never recreate original operation custody')
            self._registered = True
            for target in self.targets.values():
                self._detail(target)
            self._journal(registry, create=False)

    def _profile(self):
        if self.spark.conf.get('spark.sql.session.timeZone') != 'UTC' or self.spark.conf.get('spark.sql.ansi.enabled').lower() != 'true':
            raise LocalDeltaError('Explicit UTC/strict ANSI profile required; scalar admission is separate host responsibility')

    def _reservation_path(self, target):
        return target.path / '.ashlar-local-installation-reservation.json'

    def _journal(self, registry, *, create):
        if create:
            descriptor = os.open(self.journal_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 384)
            os.close(descriptor)
        self.db = sqlite3.connect('file:' + quote(str(self.journal_path.resolve())) + '?mode=rw', uri=True)
        try:
            self.db.execute('PRAGMA journal_mode=WAL')
            self.db.execute('PRAGMA synchronous=FULL')
            if create:
                with self.db:
                    self.db.execute('CREATE TABLE local_installation(id INTEGER PRIMARY KEY CHECK(id=1), original TEXT NOT NULL)')
                    self.db.execute('INSERT INTO local_installation VALUES(1,?)', (registry,))
                    self.db.execute('CREATE TABLE local_operation(operation TEXT PRIMARY KEY,intent TEXT NOT NULL,intent_sha TEXT NOT NULL,before_version INTEGER NOT NULL,state TEXT NOT NULL,receipt TEXT)')
                    self.db.execute('CREATE TABLE local_plan(operation TEXT PRIMARY KEY,original TEXT NOT NULL,digest TEXT NOT NULL)')
                    self.db.execute('CREATE TABLE local_publication_artifact (request_digest TEXT PRIMARY KEY,request TEXT NOT NULL,artifact TEXT NOT NULL)')
                    self.db.execute('CREATE TABLE local_source_plan (request_digest TEXT PRIMARY KEY,original TEXT NOT NULL)')
            if self.db.execute('SELECT original FROM local_installation WHERE id=1').fetchall() != [(registry,)]:
                raise LocalDeltaError('Original journal installation/registry differs')
            for (name, columns) in {'local_installation': ['id', 'original'], 'local_operation': ['operation', 'intent', 'intent_sha', 'before_version', 'state', 'receipt'], 'local_plan': ['operation', 'original', 'digest']}.items():
                if [r[1] for r in self.db.execute('PRAGMA table_info(' + name + ')')] != columns:
                    raise LocalDeltaError('Original custody journal schema incomplete or changed')
        except BaseException as error:
            from .lifecycle import finish
            try:
                finish(error, [self.db.close])
            except sqlite3.Error as original:
                raise LocalDeltaError('Original custody journal missing or incomplete') from original

    @classmethod
    def require_existing_journal(cls, path, installation_id, targets, *, capacity=None):
        """Read-only complete canonical journal preflight before driver construction."""
        import stat
        from .lifecycle import finish
        if cls is not LocalDeltaTransport or type(path) is not type(Path()) or not path.is_absolute() or path.resolve() != path:
            raise LocalDeltaError('Canonical existing original journal required')
        identity = path.lstat()
        if not stat.S_ISREG(identity.st_mode) or identity.st_uid != os.getuid() or identity.st_mode & 0o022 or identity.st_size > 128 * 1024 * 1024:
            raise LocalDeltaError('Private bounded original journal required')
        registry = {'installation': installation_id, 'targets': [{'table': t.table, 'path': str(t.path), 'uuid': t.uuid} for t in sorted(targets, key=lambda t: t.table)]}
        selected = operation_capacity(capacity)
        if selected is not None: registry['operation_capacity'] = dict(selected)
        definitions = {
            'local_installation': 'CREATE TABLE local_installation(id INTEGER PRIMARY KEY CHECK(id=1), original TEXT NOT NULL)',
            'local_operation': 'CREATE TABLE local_operation(operation TEXT PRIMARY KEY,intent TEXT NOT NULL,intent_sha TEXT NOT NULL,before_version INTEGER NOT NULL,state TEXT NOT NULL,receipt TEXT)',
            'local_plan': 'CREATE TABLE local_plan(operation TEXT PRIMARY KEY,original TEXT NOT NULL,digest TEXT NOT NULL)',
            'local_publication_artifact': 'CREATE TABLE local_publication_artifact (request_digest TEXT PRIMARY KEY,request TEXT NOT NULL,artifact TEXT NOT NULL)',
            'local_source_plan': 'CREATE TABLE local_source_plan (request_digest TEXT PRIMARY KEY,original TEXT NOT NULL)'}
        connection = sqlite3.connect('file:' + quote(str(path)) + '?mode=ro', uri=True)
        primary = None
        def renew():
            current = path.lstat()
            if (current.st_dev, current.st_ino, current.st_mode, current.st_uid) != (identity.st_dev, identity.st_ino, identity.st_mode, identity.st_uid):
                raise LocalDeltaError('Original journal closing custody differs')
        try:
            connection.execute('PRAGMA query_only=ON')
            actual = dict(connection.execute("SELECT name,sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"))
            normalize = lambda text: re.sub(r'\s+', '', text).lower()
            if set(actual) != set(definitions) or any(type(actual[name]) is not str or normalize(actual[name]) != normalize(sql) for name, sql in definitions.items()):
                raise LocalDeltaError('Complete unchanged original journal schema required')
            if connection.execute('SELECT original FROM local_installation WHERE id=1').fetchall() != [(encoded(registry),)]:
                raise LocalDeltaError('Complete original journal registry differs')
        except BaseException as error:
            primary = error
        finally:
            finish(primary, [renew, connection.close])

    def close(self):
        self.db.close()

    @contextmanager
    def _lock(self):
        with open(str(self.journal_path) + '.lock', 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def _render(self, sql):
        text = sql
        for (table, target) in self.targets.items():
            text = text.replace('`' + '`.`'.join(table.split('.')) + '`', 'delta.`' + str(target.path) + '`')
        return text

    def _parameters(self, parameters):
        if type(parameters) is not dict or any((type(k) is not str or not re.fullmatch('[a-zA-Z_][a-zA-Z_0-9]*', k) or type(v) is not str for (k, v) in parameters.items())):
            raise LocalDeltaError('Exact named string parameter inventory required')

    def query(self, sql, parameters):
        self._parameters(parameters)
        bounds = getattr(self, 'observation_bounds', None)
        if bounds is not None:
            if type(sql) is not str or len(sql) > bounds.input_bytes or len(sql.encode()) > bounds.input_bytes:
                raise LocalDeltaError('Bounded native query text required')
            bounded_local_json_bytes(parameters, bounds.input_bytes)
        if not sql.strip() or sql.lstrip().split(None, 1)[0].upper() not in ('SELECT', 'DESCRIBE', 'SHOW'):
            raise LocalDeltaError('Mutation requires explicit original operation identity')
        frame = self.spark.sql(self._render(sql), args=parameters)
        rows = self._bounded_native_rows(frame) if getattr(self, 'observation_bounds', None) is not None else [r.asDict() for r in frame.collect()]
        return SQLResult(rows, tuple(((f.name, f.dataType.simpleString().upper()) for f in frame.schema.fields)))

    def _target(self, sql):
        match = re.match('\\s*(?:MERGE\\s+INTO|INSERT\\s+INTO)\\s+(`[^`]+`\\.`[^`]+`\\.`[^`]+`)', sql, re.I)
        if not match:
            raise LocalDeltaError('Trusted single-target INSERT/MERGE profile required')
        name = match.group(1).replace('`', '')
        if name not in self.targets or ';' in sql:
            raise LocalDeltaError('Admitted single native target required')
        return self.targets[name]

    def _detail(self, target):
        frame = self.spark.sql('DESCRIBE DETAIL delta.`' + str(target.path) + '`')
        if getattr(self, 'observation_bounds', None) is not None:
            rows = self._bounded_native_rows(frame, maximum_rows=1)
            if len(rows) != 1: raise LocalDeltaError('Exact single native detail required')
            detail = rows[0]
        else:
            detail = frame.first().asDict()
        if detail.get('id') != target.uuid:
            raise LocalDeltaError('Original native Delta UUID changed')
        if self._registered:
            reservation = self._reservation_path(target)
            if reservation.is_symlink() or not reservation.is_file() or reservation.stat().st_uid != os.getuid() or reservation.stat().st_mode & 18 or (self._reservation_text(reservation) != self.reservation):
                raise LocalDeltaError('Original private installation reservation missing or changed')
            properties = detail.get('properties', {})
            if properties.get('ashlar.local.installation.id') != self.installation_id or properties.get('ashlar.local.registry.sha256') != self.registry_sha or properties.get('ashlar.local.journal.sha256') != self.journal_sha:
                raise LocalDeltaError('Original native installation registration differs')
        return detail

    def _reservation_text(self, path):
        bounds = getattr(self, 'observation_bounds', None)
        if bounds is None: return path.read_text()
        from .lifecycle import finish
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW); primary = None; raw = None
        try:
            held = os.fstat(fd)
            if not stat.S_ISREG(held.st_mode) or held.st_size > bounds.input_bytes:
                raise LocalDeltaError('Bounded original reservation required')
            raw = os.read(fd, bounds.input_bytes+1)
            named = path.lstat(); current = os.fstat(fd)
            if len(raw) != held.st_size or (held.st_dev,held.st_ino,held.st_size,held.st_mtime_ns) != (current.st_dev,current.st_ino,current.st_size,current.st_mtime_ns) or (named.st_dev,named.st_ino) != (held.st_dev,held.st_ino):
                raise LocalDeltaError('Original reservation capture changed')
        except BaseException as error: primary = error
        finally: finish(primary, [lambda: os.close(fd)])
        return raw.decode()

    def original_history(self, target):
        frame = self.spark.sql('DESCRIBE HISTORY delta.`' + str(target.path) + '`')
        if getattr(self, 'observation_bounds', None) is not None:
            rows = self._bounded_native_rows(frame, maximum_rows=1000)
            if not rows: raise LocalDeltaError('Complete native history required')
            return rows
        rows = frame.limit(1001).collect()
        if not rows or len(rows) > 1000:
            raise LocalDeltaError('Bounded complete local history required')
        return [r.asDict() for r in rows]

    def _bounded_native_rows(self, frame, *, maximum_rows=None):
        """Admit finite native query payload before transferring original carriers.

        JSON expressions measure only; returned datetime/map/array values remain
        native. This guard is active only in an explicit verification interval.
        """
        from decimal import Decimal
        from pyspark.sql import functions as F
        from .lifecycle import finish
        bounds = getattr(self, 'observation_bounds', None)
        if type(bounds) is not VerificationBounds:
            raise LocalDeltaError('Active exact native query bounds required')
        limit = bounds.rows if maximum_rows is None else min(bounds.rows, maximum_rows)
        fields = frame.schema.fields
        nodes = 0; budget = 64
        def text(value):
            nonlocal budget
            if type(value) is not str or len(value) > bounds.cell_bytes:
                raise LocalDeltaError('Bounded native schema text required')
            size = len(value.encode())
            if size > bounds.cell_bytes: raise LocalDeltaError('Bounded native schema UTF8 required')
            budget += 32 + 6 * size
            if budget > bounds.input_bytes: raise LocalDeltaError('Native query schema exceeds bound')
        def metadata(value, depth):
            nonlocal nodes, budget
            nodes += 1; budget += 32
            if depth > 16 or nodes > 4096 or budget > bounds.input_bytes:
                raise LocalDeltaError('Bounded native schema structure required')
            if type(value) is str: text(value)
            elif value is None or type(value) in (bool, int, float):
                if type(value) is int and value.bit_length() > 64: raise LocalDeltaError('Bounded metadata integer required')
                if type(value) is float:
                    import math
                    if not math.isfinite(value): raise LocalDeltaError('Finite metadata required')
            elif type(value) in (list, dict) and len(value) <= 256:
                for key, item in (value.items() if type(value) is dict else enumerate(value)):
                    if type(value) is dict: text(key)
                    metadata(item, depth+1)
            else: raise LocalDeltaError('Closed native metadata required')
        def datatype(kind, depth):
            nonlocal nodes, budget
            nodes += 1; budget += 32
            if depth > 16 or nodes > 4096 or budget > bounds.input_bytes:
                raise LocalDeltaError('Bounded native type structure required')
            name = kind.typeName()
            if name in ('byte','short','integer','long','float','double','string','boolean','timestamp','date','binary','void'):
                return
            if name == 'decimal':
                if type(kind.precision) is not int or type(kind.scale) is not int or not 1 <= kind.precision <= 38 or not 0 <= kind.scale <= kind.precision:
                    raise LocalDeltaError('Bounded decimal type required')
            elif name == 'array': datatype(kind.elementType, depth+1)
            elif name == 'map': datatype(kind.keyType, depth+1); datatype(kind.valueType, depth+1)
            elif name == 'struct': schema(kind.fields, depth+1)
            else: raise LocalDeltaError('Supported bounded native type required')
        def schema(items, depth):
            if type(items) is not list or not 1 <= len(items) <= 256:
                raise LocalDeltaError('Bounded complete native fields required')
            names = set()
            for field in items:
                text(field.name)
                if not field.name or field.name in names or type(field.nullable) is not bool or type(field.metadata) is not dict:
                    raise LocalDeltaError('Exact complete native fields required')
                names.add(field.name); metadata(field.metadata, depth); datatype(field.dataType, depth)
        schema(fields, 0)
        names = [field.name for field in fields]
        # Per-cell struct serialization handles nested maps/arrays without
        # reinterpreting their native returned meaning or flattening carriers.
        aliases = ['_ashlar_verification_cell_'+str(index) for index in range(len(names))]
        if set(aliases) & set(names):
            raise LocalDeltaError('Reserved native measurement alias collision')
        measurements = [F.coalesce(F.length(F.encode(F.to_json(F.struct(F.col('`'+name.replace('`','``')+'`')), {'ignoreNullFields':'false'}), 'UTF-8')), F.lit(0)).cast('decimal(38,0)').alias(alias) for name, alias in zip(names,aliases)]
        lengths = [item for item in measurements]
        rows, _ = self._guarded_native_projection(frame, names, lengths,
            limit=limit, maximum=bounds.total_bytes, base=256+budget, row_constant=64)
        return rows

    def _guarded_native_projection(self, frame, names, measurements, *, limit,
            maximum, base, row_constant):
        """One iterator action with native global admission and a closed sentinel.

        Invalid real rows are filtered before transfer. The global window may
        shuffle/spill the input in the JVM; this is a driver-payload bound for
        the explicit tiny verification profile, not an executor heap promise.
        """
        from decimal import Decimal
        from pyspark.sql import functions as F, Window
        from .lifecycle import finish
        bounds = self.observation_bounds
        aliases = ['_ashlar_verification_cell_'+str(i) for i in range(len(names))]
        marker = '_ashlar_verification_real'
        stat_names = ['_ashlar_verification_count','_ashlar_verification_max','_ashlar_verification_total']
        status = '_ashlar_verification_valid'
        reserved = aliases+[marker]+stat_names+[status]
        if set(names)&set(reserved): raise LocalDeltaError('Reserved native guard alias collision')
        # Every real row and the one sentinel carry fixed scalar decimal(38,0),
        # boolean and byte markers. Bound the complete repeated wire envelope
        # together with the original payload, including the sentinel even if
        # it is transferred after all real rows.
        envelope = 512 + 128*len(reserved)
        if envelope > bounds.input_bytes or envelope > bounds.total_bytes:
            raise LocalDeltaError('Bounded native metric envelope required')
        quote = lambda name: F.col('`'+name.replace('`','``')+'`')
        real = frame.select(*[quote(name) for name in names],
            *[value.alias(alias) for value,alias in zip(measurements,aliases)],
            F.lit(1).cast('byte').alias(marker))
        sentinel = self.spark.range(1).select(
            *[F.lit(None).cast(field.dataType).alias(field.name) for field in frame.schema.fields],
            *[F.lit(0).cast('decimal(38,0)').alias(alias) for alias in aliases],
            F.lit(0).cast('byte').alias(marker))
        combined = real.unionByName(sentinel)
        window = Window.partitionBy().rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        lengths = [quote(alias) for alias in aliases]
        total = lengths[0]
        for item in lengths[1:]: total = total + item
        counted = combined.select('*',
            F.sum(quote(marker).cast('decimal(38,0)')).over(window).alias(stat_names[0]),
            F.max(F.greatest(*lengths) if len(lengths)>1 else lengths[0]).over(window).alias(stat_names[1]),
            F.sum(total).over(window).alias(stat_names[2]))
        count, cell, size = [quote(name) for name in stat_names]
        valid = (count.isNotNull() & cell.isNotNull() & size.isNotNull()
            & (count >= F.lit(0)) & (count <= F.lit(limit))
            & (cell >= F.lit(0)) & (cell <= F.lit(bounds.cell_bytes))
            & (size >= F.lit(0))
            & (F.lit(base)+count*F.lit(row_constant)+size*F.lit(6)+(count+F.lit(1))*F.lit(envelope) <= F.lit(maximum)))
        admitted = counted.select('*', F.coalesce(valid,F.lit(False)).alias(status))
        # Filtering is essential: overflow must not transfer arbitrarily many
        # masked real rows. Only the one fixed sentinel survives invalid input.
        filtered = admitted.filter((quote(marker)==F.lit(0)) | quote(status))
        guarded = filtered.select(
            *[F.when(quote(status)&(quote(marker)==F.lit(1)),quote(field.name))
                .otherwise(F.lit(None).cast(field.dataType)).alias(field.name)
                for field in frame.schema.fields],
            *[quote(name) for name in reserved])
        rows=[]; observed=None; sentinel_seen=False; actual_cell=actual_total=0
        iterator=None; primary=None
        def integer(value):
            if type(value) not in (int,Decimal) or value < 0 or int(value)!=value:
                raise LocalDeltaError('Exact finite native guard statistic required')
            return int(value)
        try:
            iterator=guarded.toLocalIterator()
            if not callable(getattr(iterator,'close',None)): raise LocalDeltaError('Closable native guard iterator required')
            for native in iterator:
                row=native.asDict()
                if type(row) is not dict or set(row)!=set(names+reserved): raise LocalDeltaError('Closed native guarded row required')
                if type(row[status]) is not bool or type(row[marker]) is not int or row[marker] not in (0,1):
                    raise LocalDeltaError('Exact native guard status and marker required')
                stats=tuple(integer(row[name]) for name in stat_names)
                if not row[status] or not 0<=stats[0]<=limit or stats[1]>bounds.cell_bytes or base+stats[0]*row_constant+6*stats[2]+(stats[0]+1)*envelope>maximum:
                    raise LocalDeltaError('Native bounds refused before payload transfer')
                if observed is not None and observed!=stats: raise LocalDeltaError('Native guard statistics changed')
                observed=stats
                values=[integer(row[alias]) for alias in aliases]
                if row[marker]==0:
                    if sentinel_seen or any(values) or any(row[name] is not None for name in names):
                        raise LocalDeltaError('Exactly one typed-null native sentinel required')
                    sentinel_seen=True
                    continue
                if len(rows)>=stats[0]: raise LocalDeltaError('Complete guarded native count required')
                actual_cell=max(actual_cell,max(values));actual_total+=sum(values)
                if actual_cell>stats[1] or actual_cell>bounds.cell_bytes or actual_total>stats[2] or base+(len(rows)+1)*row_constant+6*actual_total+(len(rows)+2)*envelope>maximum:
                    raise LocalDeltaError('Captured native payload exceeds admitted statistics or budget')
                rows.append({name:row[name] for name in names})
            if not sentinel_seen or observed is None or (len(rows),actual_cell,actual_total)!=observed:
                raise LocalDeltaError('Complete native sentinel/count/byte correspondence required')
        except BaseException as error: primary=error
        finally: finish(primary,[iterator.close] if iterator is not None and callable(getattr(iterator,'close',None)) else [])
        return rows, observed

    @contextmanager
    def bounded_observations(self, bounds):
        """Apply pre-transfer bounds to snapshots, queries, details and histories.

        This selects capture limits only; it does not grant native authority.
        Existing policy, identity and immutable-version checks remain required.
        """
        if type(bounds) is not VerificationBounds or getattr(self, 'observation_bounds', None) is not None:
            raise LocalDeltaError('Exact nonnested bounded observation interval required')
        bounds.__post_init__()
        self.observation_bounds = bounds
        try:
            yield
        finally:
            self.observation_bounds = None

    def bounded_snapshot(self, target, version, *, remaining_bytes=None):
        """Full String/null native capture with all-exit current identity renewal."""
        from .lifecycle import finish
        primary = None; result = None
        try:
            result = self._bounded_snapshot(target, version, remaining_bytes=remaining_bytes)
        except BaseException as error:
            primary = error
        finally:
            finish(primary, [lambda: self._detail(target)])
        return result

    def _bounded_snapshot(self, target, version, *, remaining_bytes=None):
        """Full String/null native capture after aggregate byte admission."""
        from decimal import Decimal
        from pyspark.sql import functions as F
        bounds = getattr(self, 'observation_bounds', None)
        if type(bounds) is not VerificationBounds or type(version) is not int or version < 0:
            raise LocalDeltaError('Active bounded immutable native observation required')
        maximum = bounds.total_bytes if remaining_bytes is None else remaining_bytes
        if type(maximum) is not int or not 1 <= maximum <= bounds.total_bytes:
            raise LocalDeltaError('Exact remaining cumulative capture bound required')
        self._detail(target)
        frame = self.spark.read.format('delta').option('versionAsOf', version).load(str(target.path))
        fields = frame.schema.fields
        if type(fields) is not list or not 1 <= len(fields) <= 256:
            raise LocalDeltaError('Bounded native schema field inventory required')
        # Bound native metadata before StructType.json creates its complete string.
        schema_budget = 64
        nodes = 0
        def metadata_size(value, depth=0):
            nonlocal nodes
            nodes += 1
            if depth > 16 or nodes > 4096:
                raise LocalDeltaError('Bounded native metadata structure required')
            if type(value) is str:
                if len(value) > bounds.cell_bytes:
                    raise LocalDeltaError('Bounded native metadata text required')
                size = len(value.encode())
                if size > bounds.cell_bytes:
                    raise LocalDeltaError('Bounded native metadata UTF8 required')
                return 2 + 6 * size
            if value is None or type(value) is bool: return 5
            if type(value) is int and value.bit_length() <= 64: return 21
            if type(value) is float:
                import math
                if math.isfinite(value): return 32
            if type(value) is list and len(value) <= 256:
                return 2 + len(value) + sum(metadata_size(item, depth+1) for item in value)
            if type(value) is dict and len(value) <= 256 and all(type(key) is str for key in value):
                return 2 + len(value) * 2 + sum(metadata_size(key, depth+1)+metadata_size(item, depth+1) for key,item in value.items())
            raise LocalDeltaError('Closed supported native metadata carriers required')
        for field in fields:
            if type(field.name) is not str or type(field.nullable) is not bool or type(field.metadata) is not dict:
                raise LocalDeltaError('Complete native schema carriers required')
            type_name = field.dataType.typeName()
            allowed_types = {'byte','short','integer','long','string','boolean','timestamp','decimal'}
            if type(type_name) is not str or type_name not in allowed_types:
                raise LocalDeltaError('Supported primitive native type required before serialization')
            if type_name == 'decimal' and (type(field.dataType.precision) is not int or type(field.dataType.scale) is not int
                    or not 1 <= field.dataType.precision <= 38 or not 0 <= field.dataType.scale <= field.dataType.precision):
                raise LocalDeltaError('Bounded exact native decimal type required')
            family = field.dataType.simpleString()
            if type(family) is not str or len(family) > 64:
                raise LocalDeltaError('Bounded native schema family required')
            schema_budget += 128 + metadata_size(field.name) + metadata_size(field.metadata) + 6 * len(family)
            if schema_budget > bounds.input_bytes:
                raise LocalDeltaError('Native complete schema bound exceeded before serialization')
        schema_json = frame.schema.json()
        if type(schema_json) is not str or len(schema_json.encode()) > bounds.input_bytes:
            raise LocalDeltaError('Bounded complete native schema required')
        schema = []; projection = []
        for field in frame.schema.fields:
            name = field.name; family = field.dataType.simpleString()
            if type(name) is not str or not name or len(name.encode()) > bounds.cell_bytes or len(schema) >= 256:
                raise LocalDeltaError('Bounded exact native column inventory required')
            column = F.col('`' + name.replace('`', '``') + '`')
            schema.append([name, family])
            if family in ('tinyint', 'smallint', 'int', 'bigint', 'string', 'boolean') or re.fullmatch(r'decimal\([0-9]+,[0-9]+\)', family):
                projection.append(column.cast('string').alias(name))
            elif family == 'timestamp':
                projection.append(F.unix_micros(column).cast('string').alias(name))
            else:
                raise LocalDeltaError('Unsupported exact native snapshot carrier family')
        if not schema or len({name for name, _ in schema}) != len(schema):
            raise LocalDeltaError('Complete distinct native schema required')
        projected = frame.select(*projection)
        lengths = [F.coalesce(F.length(F.encode(F.col('`' + name.replace('`', '``') + '`'), 'UTF-8')), F.lit(0)).cast('decimal(38,0)') for name, _ in schema]
        # Primitive lexical rows use the same single-action native guard, with
        # their original conservative JSON/schema budget retained.
        row_constant = 2 + len(schema) + sum(len(json.dumps(name).encode()) + 1 for name, _ in schema) + 6 * len(schema)
        base = 256 + len(schema_json.encode()) + 2
        rows, (count,cell,total) = self._guarded_native_projection(projected,
            [name for name,_ in schema], lengths, limit=bounds.rows,
            maximum=maximum, base=base, row_constant=row_constant+1)
        actual_cell=actual_total=0;captured_bytes=2
        for row in rows:
            for value in row.values():
                if value is not None and type(value) is not str: raise LocalDeltaError('Exact String/null projected native carrier required')
                size=0 if value is None else len(value.encode())
                actual_cell=max(actual_cell,size);actual_total+=size
            captured_bytes+=len(encoded(row).encode())+1
        if actual_cell!=cell or actual_total!=total or captured_bytes+len(schema_json.encode())+256>maximum or frame.schema.json()!=schema_json:
            raise LocalDeltaError('Native complete bound/schema correspondence differs')
        self._detail(target)
        rows.sort(key=encoded)
        return {'schema': schema, 'rows': rows, 'row_sha256': sha(encoded(rows))}, json.loads(schema_json)

    def _snapshot(self, target, version):
        if getattr(self, 'observation_bounds', None) is not None:
            return self.bounded_snapshot(target, version)[0]
        from pyspark.sql import functions as F
        self._detail(target)
        frame = self.spark.read.format('delta').option('versionAsOf', version).load(str(target.path))
        projection = []
        schema = []
        for field in frame.schema.fields:
            name = field.name
            family = field.dataType.simpleString()
            col = F.col('`' + name.replace('`', '``') + '`')
            schema.append([name, family])
            if family in ('tinyint', 'smallint', 'int', 'bigint', 'string', 'boolean') or family.startswith('decimal('):
                projection.append(col.cast('string').alias(name))
            elif family == 'timestamp':
                projection.append(F.unix_micros(col).cast('string').alias(name))
            else:
                raise LocalDeltaError('Unsupported exact native snapshot carrier family')
        rows = [r.asDict() for r in frame.select(*projection).limit(10001).collect()]
        if len(rows) > 10000:
            raise LocalDeltaError('Bounded full native row inventory required')
        self._detail(target)
        return {'schema': schema, 'rows': sorted(rows, key=encoded), 'row_sha256': sha(encoded(sorted(rows, key=encoded)))}

    def mutation(self, operation, sql, parameters, *, intent_digest, context):
        return self._execute(operation, sql, parameters, intent_digest, context, recovery=False)

    def recover(self, operation, sql, parameters, *, intent_digest, context):
        return self._execute(operation, sql, parameters, intent_digest, context, recovery=True)

    def inspect_committed(self, operation, sql, parameters, *, intent_digest, context):
        """Read exact original committed custody; never submit or update a journal.

        A prepared/submitted/missing handle remains a recovery concern and cannot
        be accepted as settled by this port. Native history, full snapshot and
        immutable receipt must independently agree at opening and closing.
        """
        if (type(sql) is not str or type(operation) is not str or not 0 < len(operation) <= 1024
                or type(intent_digest) is not str or not re.fullmatch('[0-9a-f]{64}', intent_digest)):
            raise LocalDeltaError('Exact original operation identity required')
        self._parameters(parameters); target = self._target(sql)
        intent = original_operation_intent(self.installation_id, operation, intent_digest,
            sql, parameters, target, self.operation_capacity)
        if len(intent.encode()) > (self.operation_capacity['max_intent_bytes'] if self.operation_capacity else 4194304):
            raise LocalDeltaError('Bounded original operation required')
        body_sha = sha(intent)
        metadata = encoded({'profile': 'ashlar-local-delta-commit/0.1',
            'installation_id': self.installation_id, 'operation': operation,
            'operation_sha256': body_sha, 'request_digest': intent_digest})
        from .lifecycle import owned_context
        result = None
        with owned_context(self.policy.writer(operation, context)) as permit:
            if permit is not None or self.policy.admit(json.loads(intent), context) is not None:
                raise LocalDeltaError('Current original committed admission incomplete')
            self._profile()
            with self._lock():
                row = self.db.execute('SELECT intent,intent_sha,before_version,state,receipt FROM local_operation WHERE operation=?',
                                      (operation,)).fetchone()
                if (row is None or row[:2] != (intent, body_sha) or row[3] != 'committed'
                        or type(row[2]) is not int or row[2] < 0 or type(row[4]) is not str
                        or not 0 < len(row[4].encode()) <= 4194304):
                    raise LocalDeltaUncertain('Exact committed original handle unavailable')
                self._detail(target)
                matches = [record for record in self.original_history(target)
                           if record.get('userMetadata') == metadata]
                if len(matches) != 1 or type(matches[0].get('version')) is not int or matches[0]['version'] != row[2] + 1:
                    raise LocalDeltaUncertain('Original named commit absent or ambiguous')
                version = matches[0]['version']; snapshot = self._snapshot(target, version)
                proof = {'profile': 'ashlar-local-delta-receipt/0.1', 'operation': operation,
                    'operation_sha256': body_sha, 'request_digest': intent_digest, 'table': target.table,
                    'uuid': target.uuid, 'before_version': row[2], 'version': version,
                    'commit_metadata': metadata, 'native_operation': matches[0]['operation'], 'snapshot': snapshot}
                if row[4] != encoded(proof):
                    raise LocalDeltaError('Original complete native receipt differs')
                self._detail(target)
                if self.policy.admit(json.loads(intent), context) is not None:
                    raise LocalDeltaError('Closing original committed admission incomplete')
                self._profile()
                closing_matches = [record for record in self.original_history(target)
                                   if record.get('userMetadata') == metadata]
                if closing_matches != matches or encoded(self._snapshot(target, version)) != encoded(snapshot):
                    raise LocalDeltaError('Closing original named commit/snapshot differs')
                if self.db.execute('SELECT intent,intent_sha,before_version,state,receipt FROM local_operation WHERE operation=?',
                                   (operation,)).fetchone() != row:
                    raise LocalDeltaError('Closing original committed handle changed')
                result = json.loads(encoded(proof))
        if result is None:
            raise LocalDeltaUncertain('Original committed observation completion suppressed')
        return result

    def _execute(self, operation, sql, parameters, intent_digest, context, *, recovery):
        self._parameters(parameters)
        target = self._target(sql)
        if not isinstance(operation, str) or not operation or len(operation) > 1024 or (not isinstance(intent_digest, str)) or (not re.fullmatch('[0-9a-f]{64}', intent_digest)):
            raise LocalDeltaError('Original operation/request digest required')
        intent = original_operation_intent(self.installation_id, operation, intent_digest, sql, parameters, target, self.operation_capacity)
        if len(intent.encode()) > (self.operation_capacity['max_intent_bytes'] if self.operation_capacity is not None else 4194304):
            raise LocalDeltaError('Bounded original local operation required')
        frozen = json.loads(intent)
        sql = frozen['statement']
        parameters = frozen['parameters']
        body_sha = sha(intent)
        metadata = encoded({'profile': 'ashlar-local-delta-commit/0.1', 'installation_id': self.installation_id, 'operation': operation, 'operation_sha256': body_sha, 'request_digest': intent_digest})
        result = None
        with self.policy.writer(operation, context) as permit:
            if permit is not None:
                raise LocalDeltaError('Current writer authority incomplete')
            if self.policy.admit(json.loads(intent), context) is not None:
                raise LocalDeltaError('Current original intent admission incomplete')
            self._profile()
            with self._lock():
                retained = self.db.execute('SELECT intent,intent_sha,before_version,state,receipt FROM local_operation WHERE operation=?', (operation,)).fetchone()
                if retained is None:
                    if recovery:
                        raise LocalDeltaError('Missing retained original operation; no replacement allowed')
                    self._detail(target)
                    original_history = self.original_history(target)
                    for record in original_history:
                        try:
                            prior = json.loads(record.get('userMetadata') or '{}')
                        except (TypeError, ValueError):
                            continue
                        if prior.get('profile') == 'ashlar-local-delta-commit/0.1' and prior.get('installation_id') == self.installation_id and (prior.get('operation') == operation):
                            raise LocalDeltaError('Original named native commit lacks journal custody; no replacement')
                    before = int(original_history[0]['version'])
                    with self.db:
                        self.db.execute('INSERT INTO local_operation VALUES(?,?,?,?,?,NULL)', (operation, intent, body_sha, before, 'prepared'))
                    retained = (intent, body_sha, before, 'prepared', None)
                if retained[0] != intent or retained[1] != body_sha:
                    raise LocalDeltaError('Original local operation intent conflict')
                (before, state, receipt) = retained[2:]
                self._detail(target)
                if state == 'prepared':
                    if int(self.original_history(target)[0]['version']) != before:
                        raise LocalDeltaError('Unrelated native writes crossed original prepared version')
                    with self.db:
                        self.db.execute("UPDATE local_operation SET state='submitted' WHERE operation=?", (operation,))
                    self._profile()
                    previous = self.spark.conf.get('spark.databricks.delta.commitInfo.userMetadata', None)
                    self.spark.conf.set('spark.databricks.delta.commitInfo.userMetadata', metadata)
                    try:
                        self.spark.sql(self._render(sql), args=parameters).collect()
                    finally:
                        if previous is None:
                            self.spark.conf.unset('spark.databricks.delta.commitInfo.userMetadata')
                        else:
                            self.spark.conf.set('spark.databricks.delta.commitInfo.userMetadata', previous)
                history = self.original_history(target)
                matches = [r for r in history if r.get('userMetadata') == metadata]
                if len(matches) != 1:
                    raise LocalDeltaUncertain('Original submitted Delta commit is absent or ambiguous; no replacement mutation permitted')
                commit = matches[0]
                version = int(commit['version'])
                if version != before + 1:
                    raise LocalDeltaError('Original operation commit differs from exact predecessor version')
                snapshot = self._snapshot(target, version)
                proof = {'profile': 'ashlar-local-delta-receipt/0.1', 'operation': operation, 'operation_sha256': body_sha, 'request_digest': intent_digest, 'table': target.table, 'uuid': target.uuid, 'before_version': before, 'version': version, 'commit_metadata': metadata, 'native_operation': commit['operation'], 'snapshot': snapshot}
                text = encoded(proof)
                if receipt is not None and receipt != text:
                    raise LocalDeltaError('Original native operation receipt differs')
                self._detail(target)
                self._profile()
                if self.policy.admit(json.loads(intent), context) is not None:
                    raise LocalDeltaError('Closing current original admission incomplete')
                self._detail(target)
                self._profile()
                with self.db:
                    self.db.execute("UPDATE local_operation SET state='committed',receipt=? WHERE operation=?", (text, operation))
                result = SQLResult([{'version': str(version), 'uuid': target.uuid, 'operation_sha256': body_sha, 'snapshot_sha256': snapshot['row_sha256']}], (('version', 'STRING'), ('uuid', 'STRING'), ('operation_sha256', 'STRING'), ('snapshot_sha256', 'STRING')))
        if result is None:
            raise LocalDeltaUncertain('Original operation completion suppressed; inspect retained custody')
        return result

class LocalOperationExecutor:

    def __init__(self, transport, operation, intent_digest, context):
        self.transport = transport
        self.operation = operation
        self.intent_digest = intent_digest
        self.context = context

    def query(self, sql, parameters):
        if sql.lstrip().split(None, 1)[0].upper() in ('SELECT', 'DESCRIBE', 'SHOW'):
            return self.transport.query(sql, parameters)
        return self.transport.mutation(child_operation(self.operation, sql, parameters), sql, parameters, intent_digest=self.intent_digest, context=self.context)

class LocalDeltaEffects:

    def __init__(self, transport):
        self.transport = transport

    def run(self, operation, intent_digest, steps, *, context):
        return self._execute(operation, intent_digest, steps, context, recovery=False)

    def recover(self, operation, intent_digest, steps, *, context):
        return self._execute(operation, intent_digest, steps, context, recovery=True)

    def original_plan(self, operation, intent_digest, steps):
        """Validate and freeze exact ordered intent; this grants no admission."""
        if (type(operation) is not str or not operation or len(operation) > 1024
                or type(intent_digest) is not str or not re.fullmatch('[0-9a-f]{64}', intent_digest)):
            raise LocalDeltaError('Original whole operation/request identity required')
        if type(steps) is not list or not 0 <= len(steps) <= 100 or any((type(s) is not dict or set(s) != {'statement', 'parameters'} for s in steps)):
            raise LocalDeltaError('Bounded exact ordered native effect plan required')
        body = {'profile': 'ashlar-local-delta-effects/0.1', 'installation_id': self.transport.installation_id, 'operation': operation, 'intent_digest': intent_digest, 'steps': steps}
        bounded_local_json_bytes(body, 4194304)
        original = encoded(body)
        digest = sha(original)
        if len(original.encode()) > 4194304:
            raise LocalDeltaError('Bounded exact effect intent required')
        frozen_steps = json.loads(original)['steps']
        for step in frozen_steps:
            self.transport._target(step['statement'])
            self.transport._parameters(step['parameters'])
        identities = ['local-effect:' + digest + ':' + str(i) for i in range(len(frozen_steps))]
        return original, digest, identities

    def prepare_unsubmitted(self, operation, intent_digest, steps, *, reservation, plan, context):
        """Prepare exact original bytes only with positive never-started run custody.

        Missing local records or native history alone never establish this right.
        The original run owner and independent current policy must positively
        admit its retained reservation; known phase/ordinal/native custody refuses.
        """
        from .evolution_run import EvolutionRunAttemptJournal
        from .evolution_plan import EvolutionAttemptPlan
        if type(reservation) is not EvolutionRunAttemptJournal or type(plan) is not EvolutionAttemptPlan:
            raise LocalDeltaError('Owned positive original submission reservation required')
        reservation.not_started(plan, context)
        original, digest, identities = self.original_plan(operation, intent_digest, steps)
        value = plan.document()
        if (value['request']['request_digest'] != intent_digest or value['selected_steps'] != json.loads(original)['steps']
                or value['operations'] != identities or operation != 'effects:' + intent_digest):
            raise LocalDeltaError('Exact retained original effect reservation differs')
        if self.transport.policy.admit(json.loads(original), context) is not None:
            raise LocalDeltaError('Current original never-submitted admission incomplete')
        self.transport._profile()
        with self.transport._lock():
            rows = self.transport.db.execute('SELECT intent FROM local_operation LIMIT 1001').fetchall()
            if len(rows) > 1000:
                raise LocalDeltaError('Bounded complete original submission inventory required')
            for row in rows:
                retained = json.loads(row[0])
                if retained.get('request_digest') == intent_digest or retained.get('operation') in identities:
                    raise LocalDeltaError('Known original native submission contradicts never-started reservation')
            for target in self.transport.targets.values():
                self.transport._detail(target)
                for record in self.transport.original_history(target):
                    try: metadata = json.loads(record.get('userMetadata') or '{}')
                    except (TypeError, ValueError): metadata = {}
                    if (type(metadata) is dict and metadata.get('profile') == 'ashlar-local-delta-commit/0.1'
                            and metadata.get('installation_id') == self.transport.installation_id
                            and (metadata.get('request_digest') == intent_digest or metadata.get('operation') in identities)):
                        raise LocalDeltaError('Known original named native commit contradicts never-started reservation')
        answer = self.prepare(operation, intent_digest, steps, context=context)
        reservation.not_started(plan, context)
        if self.transport.policy.admit(json.loads(original), context) is not None:
            raise LocalDeltaError('Closing original never-submitted admission incomplete')
        return answer

    def prepare(self, operation, intent_digest, steps, *, context):
        """Retain whole original intent before effects; no mutation or initialization.

        Exact repeated retention returns the same identities. Existing custody is
        never replaced; missing or changed original operation states still require
        the transport's independent native admission during execution/recovery.
        """
        original, digest, identities = self.original_plan(operation, intent_digest, steps)
        result = None
        with self.transport.policy.writer(operation, context) as permit:
            if permit is not None or self.transport.policy.admit(json.loads(original), context) is not None:
                raise LocalDeltaError('Whole original effect plan admission incomplete')
            self.transport._profile()
            with self.transport._lock():
                row = self.transport.db.execute('SELECT original,digest FROM local_plan WHERE operation=?', (operation,)).fetchone()
                if row is None:
                    placeholders = ','.join('?' for _ in identities)
                    if identities and self.transport.db.execute('SELECT 1 FROM local_operation WHERE operation IN (' + placeholders + ') LIMIT 1', identities).fetchone():
                        raise LocalDeltaError('Original ordinal custody exists without whole plan; no restoration')
                    with self.transport.db:
                        self.transport.db.execute('INSERT INTO local_plan VALUES(?,?,?)', (operation, original, digest))
                elif row != (original, digest):
                    raise LocalDeltaError('Original ordered effect plan differs')
            if self.transport.policy.admit(json.loads(original), context) is not None:
                raise LocalDeltaError('Closing whole original effect plan admission incomplete')
            self.transport._profile()
            result = {'plan_sha256': digest, 'intent_digest': intent_digest,
                      'operation': operation, 'operation_ids': identities}
        if result is None:
            raise LocalDeltaUncertain('Whole effect preparation completion suppressed')
        return result

    def observe(self, operation, intent_digest, steps, *, context):
        """Read only original whole-plan/ordinal custody under current admission.

        'no-operation-record' is a journal observation, not native absence proof.
        The mutation owner must still inspect original named native commit history
        before a first submission. Submitted or committed rows never authorize a
        replacement; the recovery owner independently reconciles exact receipts.
        """
        original, digest, identities = self.original_plan(operation, intent_digest, steps)
        result = None
        with self.transport.policy.writer(operation, context) as permit:
            if permit is not None or self.transport.policy.admit(json.loads(original), context) is not None:
                raise LocalDeltaError('Whole original effect plan admission incomplete')
            self.transport._profile()
            with self.transport._lock():
                plan = self.transport.db.execute('SELECT original,digest FROM local_plan WHERE operation=?', (operation,)).fetchone()
                if plan != (original, digest):
                    raise LocalDeltaError('Exact original effect plan missing or changed')
                states = []
                for key, step in zip(identities, json.loads(original)['steps']):
                    row = self.transport.db.execute('SELECT intent,intent_sha,state,receipt FROM local_operation WHERE operation=?', (key,)).fetchone()
                    if row is None:
                        states.append('no-operation-record')
                        continue
                    target = self.transport._target(step['statement'])
                    expected = original_operation_intent(self.transport.installation_id, key, intent_digest,
                        step['statement'], step['parameters'], target, self.transport.operation_capacity)
                    if (row[0] != expected or row[1] != sha(expected)
                            or row[2] not in ('prepared', 'submitted', 'committed')
                            or (row[2] == 'committed') != (row[3] is not None)):
                        raise LocalDeltaError('Exact original operation custody differs')
                    states.append(row[2])
            if self.transport.policy.admit(json.loads(original), context) is not None:
                raise LocalDeltaError('Closing whole original effect plan admission incomplete')
            self.transport._profile()
            result = {'plan_sha256': digest, 'intent_digest': intent_digest,
                      'operation': operation, 'operation_ids': identities, 'states': states}
        if result is None:
            raise LocalDeltaUncertain('Whole effect observation completion suppressed')
        return result

    def _execute(self, operation, intent_digest, steps, context, *, recovery):
        original, digest, identities = self.original_plan(operation, intent_digest, steps)
        result = None
        with self.transport.policy.writer(operation, context) as permit:
            if permit is not None:
                raise LocalDeltaError('Whole effect writer admission incomplete')
            if self.transport.policy.admit(json.loads(original), context) is not None:
                raise LocalDeltaError('Whole original effect plan admission incomplete')
            expected_versions = {}
            expected_hashes = {}
            for target in self.transport.targets.values():
                self.transport._detail(target)
                expected_versions[target.table] = int(self.transport.original_history(target)[0]['version'])
            with self.transport._lock():
                row = self.transport.db.execute('SELECT original,digest FROM local_plan WHERE operation=?', (operation,)).fetchone()
                if row is None:
                    if recovery:
                        raise LocalDeltaError('Original effect plan missing; no replacement')
                    placeholders = ','.join('?' for _ in identities)
                    if identities and self.transport.db.execute('SELECT 1 FROM local_operation WHERE operation IN (' + placeholders + ') LIMIT 1', identities).fetchone():
                        raise LocalDeltaError('Original ordinal custody exists without whole plan; no restoration')
                    with self.transport.db:
                        self.transport.db.execute('INSERT INTO local_plan VALUES(?,?,?)', (operation, original, digest))
                elif row != (original, digest):
                    raise LocalDeltaError('Original ordered effect plan differs')
            results = []
            for (i, step) in enumerate(json.loads(original)['steps']):
                key = identities[i]
                # Whole-plan custody remains mandatory. Existing ordinal custody
                # must enter the original recovery port; only an absent ordinal
                # reaches first submission, whose transport rechecks native history
                # and refuses a named commit with missing operation custody.
                function = self.transport.mutation
                if recovery and self.transport.db.execute('SELECT 1 FROM local_operation WHERE operation=?', (key,)).fetchone():
                    function = self.transport.recover
                receipt = function(key, step['statement'], step['parameters'], intent_digest=intent_digest, context=context).rows
                results.append(receipt)
                expected_versions[self.transport._target(step['statement']).table] = int(receipt[0]['version'])
                expected_hashes[self.transport._target(step['statement']).table] = receipt[0]['snapshot_sha256']
            for target in self.transport.targets.values():
                self.transport._detail(target)
                snapshot = self.transport._snapshot(target, expected_versions[target.table])
                if target.table in expected_hashes and snapshot['row_sha256'] != expected_hashes[target.table]:
                    raise LocalDeltaError('Closing original snapshot parity differs')
            if self.transport.policy.admit(json.loads(original), context) is not None:
                raise LocalDeltaError('Closing whole original effect plan admission incomplete')
            for target in self.transport.targets.values():
                self.transport._detail(target)
            result = {'plan_sha256': digest, 'intent_digest': intent_digest, 'original_native_receipts': results}
        if result is None:
            raise LocalDeltaUncertain('Whole effect completion suppressed')
        return result
