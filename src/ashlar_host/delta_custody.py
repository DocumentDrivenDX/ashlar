"""Selected local host implementation; native qualification remains version-scoped."""
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl, hashlib, json, re, sqlite3, os
from types import MappingProxyType
from urllib.parse import quote
from pathlib import Path
from ashlar.native import SQLResult
from ashlar.publication import validate_table_identifier

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

    def __init__(self, spark, journal_path, installation_id, targets, policy, *, capacity=None):
        self._open(spark, journal_path, installation_id, targets, policy, create=False, context=None, capacity=capacity)

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
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        if create:
            with self.db:
                self.db.execute('CREATE TABLE local_installation(id INTEGER PRIMARY KEY CHECK(id=1), original TEXT NOT NULL)')
                self.db.execute('INSERT INTO local_installation VALUES(1,?)', (registry,))
                self.db.execute('CREATE TABLE local_operation(operation TEXT PRIMARY KEY,intent TEXT NOT NULL,intent_sha TEXT NOT NULL,before_version INTEGER NOT NULL,state TEXT NOT NULL,receipt TEXT)')
                self.db.execute('CREATE TABLE local_plan(operation TEXT PRIMARY KEY,original TEXT NOT NULL,digest TEXT NOT NULL)')
        try:
            if self.db.execute('SELECT original FROM local_installation WHERE id=1').fetchall() != [(registry,)]:
                raise LocalDeltaError('Original journal installation/registry differs')
            for (name, columns) in {'local_installation': ['id', 'original'], 'local_operation': ['operation', 'intent', 'intent_sha', 'before_version', 'state', 'receipt'], 'local_plan': ['operation', 'original', 'digest']}.items():
                if [r[1] for r in self.db.execute('PRAGMA table_info(' + name + ')')] != columns:
                    raise LocalDeltaError('Original custody journal schema incomplete or changed')
        except BaseException as error:
            self.db.close()
            if isinstance(error, sqlite3.Error):
                raise LocalDeltaError('Original custody journal missing or incomplete') from error
            raise

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
        if not sql.strip() or sql.lstrip().split(None, 1)[0].upper() not in ('SELECT', 'DESCRIBE', 'SHOW'):
            raise LocalDeltaError('Mutation requires explicit original operation identity')
        frame = self.spark.sql(self._render(sql), args=parameters)
        return SQLResult([r.asDict() for r in frame.collect()], tuple(((f.name, f.dataType.simpleString().upper()) for f in frame.schema.fields)))

    def _target(self, sql):
        match = re.match('\\s*(?:MERGE\\s+INTO|INSERT\\s+INTO)\\s+(`[^`]+`\\.`[^`]+`\\.`[^`]+`)', sql, re.I)
        if not match:
            raise LocalDeltaError('Trusted single-target INSERT/MERGE profile required')
        name = match.group(1).replace('`', '')
        if name not in self.targets or ';' in sql:
            raise LocalDeltaError('Admitted single native target required')
        return self.targets[name]

    def _detail(self, target):
        detail = self.spark.sql('DESCRIBE DETAIL delta.`' + str(target.path) + '`').first().asDict()
        if detail.get('id') != target.uuid:
            raise LocalDeltaError('Original native Delta UUID changed')
        if self._registered:
            reservation = self._reservation_path(target)
            if reservation.is_symlink() or not reservation.is_file() or reservation.stat().st_uid != os.getuid() or reservation.stat().st_mode & 18 or (reservation.read_text() != self.reservation):
                raise LocalDeltaError('Original private installation reservation missing or changed')
            properties = detail.get('properties', {})
            if properties.get('ashlar.local.installation.id') != self.installation_id or properties.get('ashlar.local.registry.sha256') != self.registry_sha or properties.get('ashlar.local.journal.sha256') != self.journal_sha:
                raise LocalDeltaError('Original native installation registration differs')
        return detail

    def original_history(self, target):
        rows = self.spark.sql('DESCRIBE HISTORY delta.`' + str(target.path) + '`').limit(1001).collect()
        if not rows or len(rows) > 1000:
            raise LocalDeltaError('Bounded complete local history required')
        return [r.asDict() for r in rows]

    def _snapshot(self, target, version):
        from pyspark.sql import functions as F
        self._detail(target)
        frame = self.spark.read.format('delta').option('versionAsOf', version).load(str(target.path))
        projection = []
        schema = []
        for field in frame.schema.fields:
            name = field.name
            family = field.dataType.simpleString()
            col = F.col('`' + name.replace('`', '``') + '`')
            schema.append((name, family))
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
        if type(steps) is not list or not 1 <= len(steps) <= 100 or any((type(s) is not dict or set(s) != {'statement', 'parameters'} for s in steps)):
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
                    if self.transport.db.execute('SELECT 1 FROM local_operation WHERE operation IN (' + placeholders + ') LIMIT 1', identities).fetchone():
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
                    if self.transport.db.execute('SELECT 1 FROM local_operation WHERE operation IN (' + placeholders + ') LIMIT 1', identities).fetchone():
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
