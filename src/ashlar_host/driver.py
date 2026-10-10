from .lifecycle import owned_connection
from .resources import RESOURCE_ROOT
'Selected local host implementation; native qualification remains version-scoped.'
import fcntl, hashlib, json, subprocess, uuid
from contextlib import contextmanager
from pathlib import Path
from ashlar.manifest import DeltaManifestStore, FIELDS, manifest_pin_vector
from ashlar.outbox import PostgresOutbox
from ashlar.publication import Snapshot
from ashlar.source_checkpoint import outbox_checkpoint
from ashlar.staging import batch_row
from .delta_custody import LocalDeltaEffects, LocalOperationExecutor, child_operation, encoded, sha, LocalDeltaError
from .connection import connect, CONTAINER
from .postgres import Session
from .ack import AckScope, ProtectedOutboxAck, AckOutcomeUncertain, render_ddl
from .graph_sql import graph_sql_plan
from .source_sessions import RegisteredOutboxSources
from .ack_sessions import RegisteredOutboxAcks
ROOT = RESOURCE_ROOT
CLOCK = '2026-10-09T12:00:00+00:00'
PROFILE = 'ashlar-delta/0.3'

def progress_union(previous, checkpoint):
    result = json.loads(encoded(previous))
    old = result.get(checkpoint['feed'])
    if old is not None and (old['epoch'] != checkpoint['epoch'] or old['position'] != checkpoint['previous']):
        raise ValueError('Original contiguous source epoch/progress required')
    if old is None and checkpoint['previous'] != '0':
        raise ValueError('Fresh source must begin at zero')
    result[checkpoint['feed']] = dict(checkpoint)
    return result

def validate_union(previous, request, manifest):
    checkpoint = json.loads(request['source_checkpoint_json'])
    if json.loads(manifest['source_progress_json']) != progress_union(previous, checkpoint):
        raise ValueError('Complete independent source progress union differs')
    report = json.loads(manifest['validation_report_json'])
    if report['predecessor'] != request['predecessor'] or report['request_digest'] != request['request_digest']:
        raise ValueError('Global predecessor/request custody differs')

def local_effect_plan(generated, prior_rows, tables):
    """Elide only closed generated zero-match DELETE before any submission.

    Caller must independently verify complete original native prior rows/UUIDs
    under the held writer. This pure correspondence function grants no admission.
    """
    selected = []
    elisions = []
    for (ordinal, step) in enumerate(generated):
        found = None
        for (role, typed) in (('object_current', 'type_id'), ('edge_current', 'rel_type_id')):
            quoted = '`' + '`.`'.join(tables[role].split('.')) + '`'
            expected = 'MERGE INTO ' + quoted + " t USING (SELECT k.* FROM (SELECT explode(from_json(:keys,'ARRAY<STRUCT<source_system:STRING,type_id:STRING,id:STRING>>')) k)) s ON t.source_system=s.source_system AND t." + typed + '=cast(s.type_id AS BIGINT) AND t.id=cast(s.id AS BIGINT) WHEN MATCHED THEN DELETE'
            if step['statement'] != expected:
                continue
            if set(step['parameters']) != {'keys'}:
                raise ValueError('Closed generated delete key inventory required')
            keys = json.loads(step['parameters']['keys'])
            if not isinstance(keys, list) or not keys or any((set(k) != {'source_system', 'type_id', 'id'} or any((type(v) is not str for v in k.values())) for k in keys)):
                raise ValueError('Exact generated source-qualified delete keys required')
            matches = [row for row in prior_rows[role] if any(((row['source_system'], row[typed], row['id']) == (k['source_system'], k['type_id'], k['id']) for k in keys))]
            if not matches:
                found = {'ordinal': ordinal, 'original_step': json.loads(encoded(step)), 'table': tables[role], 'source_keys': keys, 'matched_rows': [], 'complete_prior_rows_sha256': sha(encoded(sorted(prior_rows[role], key=encoded)))}
        if found is None:
            selected.append(json.loads(encoded(step)))
        else:
            elisions.append(found)
    return (selected, elisions)

class LostAfterManifest(RuntimeError):
    pass

class PrivatePolicy:

    def __init__(self, context, targets):
        self.context = context
        self.targets = targets
        self.active = None
        self.initializing = True

    @contextmanager
    def writer(self, *args):
        if args[-1] is not self.context:
            raise PermissionError('Original held private writer context required')
        yield

    def admit(self, intent, context):
        if context is not self.context:
            raise PermissionError('Original private writer context required')
        profile = intent.get('profile')
        if profile == 'ashlar-local-delta-installation/0.1':
            expected = [{'table': t.table, 'path': str(t.path), 'uuid': t.uuid} for t in sorted(self.targets, key=lambda t: t.table)]
            if not self.initializing or intent['targets'] != expected:
                raise PermissionError('Original fresh target registry required')
            return
        active = self.active
        if active is None:
            raise PermissionError('Original native request admission missing')
        if profile == 'ashlar-local-delta-effects/0.1':
            if intent['steps'] != active['steps'] or intent['intent_digest'] != active['request']['request_digest']:
                raise PermissionError('Exact original effect plan required')
        elif profile == 'ashlar-local-delta-operation/0.1':
            if intent['request_digest'] != active['request']['request_digest']:
                raise PermissionError('Original native request digest differs')
            role = intent['table'].split('.')[-1]
            if role in ('attempts', 'manifest'):
                raw = intent['parameters'].get('payload' if role == 'attempts' else 'row')
                if raw is None:
                    raise PermissionError('Original closed metadata mutation required')
                value = json.loads(raw)
                if role == 'attempts' and json.loads(value['payload_json'])['request'] != active['request']:
                    raise PermissionError('Original phase request differs')
                if role == 'manifest' and value != active.get('manifest'):
                    raise PermissionError('Original manifest proposal differs')
            elif {'statement': intent['statement'], 'parameters': intent['parameters']} not in active['steps']:
                raise PermissionError('Original graph mutation missing exact plan')
        else:
            raise PermissionError('Unrecognized local original intent')

class CarrierPolicy:

    def __init__(self, owner, kind):
        self.owner = owner
        self.kind = kind

    @contextmanager
    def writer(self, table, native_uuid, context):
        self.owner.require(context)
        target = self.owner.transport.targets[table]
        if native_uuid != target.uuid:
            raise PermissionError('Original metadata target differs')
        yield

    def admit(self, row, context):
        self.owner.require(context)
        if self.kind == 'manifest':
            self.owner.validate_manifest(row, context)

class RecoveryExecutor:
    """Reads plus exact original recover dispatch; never creates a mutation."""

    def __init__(self, original):
        self.original = original

    def query(self, sql, parameters):
        original = self.original
        if sql.lstrip().split(None, 1)[0].upper() in ('SELECT', 'DESCRIBE', 'SHOW'):
            return original.transport.query(sql, parameters)
        return original.transport.recover(child_operation(original.operation, sql, parameters), sql, parameters, intent_digest=original.intent_digest, context=original.context)

class ManifestPort:
    """Generic manifest store, with recovery restricted to retained original SQL."""

    def __init__(self, owner, request):
        self.owner = owner
        self.executor = LocalOperationExecutor(owner.transport, 'manifest:' + request['request_digest'], request['request_digest'], owner.context)
        target = owner.transport.targets[owner.tables['manifest']]
        self.store = DeltaManifestStore(self.executor, CarrierPolicy(owner, 'manifest'), target.table, target.uuid)

    def commit(self, row, *, context):
        return self.store.commit(row, context=context)

    def recover(self, row, *, context):
        if not self.owner.transport.db.execute('SELECT 1 FROM local_operation WHERE operation LIKE ?', ('manifest:' + self.executor.intent_digest + ':%',)).fetchone():
            raise LocalDeltaError('Original manifest submission absent; recovery cannot replace it')
        store = DeltaManifestStore(RecoveryExecutor(self.executor), self.store.policy, self.store.table, self.store.uuid)
        return store.commit(row, context=context)

class NativeDriver:

    def __init__(self, transport, policy, context, tables, scope_ports, allowed_changes, columns, *, source_admission, source_sessions=None, ack_sessions=None):
        self.transport = transport
        self.policy = policy
        self.context = context
        self.tables = tables
        self.scope_ports = scope_ports
        self.allowed_changes = allowed_changes
        self.columns = columns
        if not callable(getattr(source_admission, 'admit', None)) or not callable(getattr(source_admission, 'metadata', None)):
            raise PermissionError('Explicit admitted source profile required')
        self.source_admission = source_admission
        self.original_admission = encoded(source_admission.metadata())
        facts = json.loads(self.original_admission)
        if type(facts) is not dict or not isinstance(facts.get('profile'), str) or (not facts['profile']) or (not isinstance(facts.get('qualification'), str)) or (not facts['qualification']) or (len(self.original_admission.encode()) > 1048576):
            raise PermissionError('Bounded explicit immutable source profile facts required')
        if facts['profile'] == 'ashlar-commerce-evolution-source-set/0.1' and source_sessions is None:
            raise PermissionError('Installed evolution requires explicit ordinary source sessions')
        if source_sessions is not None and type(source_sessions) is not RegisteredOutboxSources:
            raise PermissionError('Explicit registered ordinary source session owner required')
        self.source_sessions = source_sessions
        self.original_source_sessions = None if source_sessions is None else encoded(source_sessions.metadata())
        if source_sessions is not None and facts['profile'] == 'ashlar-commerce-evolution-source-set/0.1':
            expected = {(item['source_system'], item['epoch']) for item in facts['sources']}
            actual = {(item.scope.feed, item.scope.epoch) for item in source_sessions.registrations}
            if expected != actual:
                raise PermissionError('Complete original semantic/native source inventory differs')
        if ack_sessions is not None and type(ack_sessions) is not RegisteredOutboxAcks:
            raise PermissionError('Explicit registered ordinary protected ACK owner required')
        self.ack_sessions = ack_sessions
        self.original_ack_sessions = None if ack_sessions is None else encoded(ack_sessions.metadata())
        if ack_sessions is not None:
            if source_sessions is None or (ack_sessions.metadata()['registrations'] !=
                                          source_sessions.metadata()['registrations']):
                raise PermissionError('Complete original source/ACK registrations differ')
        self.held = False
        self.pin_held = False
        self.lose_manifest = False
        self.lose_ack = False
        self.acks = []
        if not transport.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='local_publication_artifact'").fetchone():
            if transport.db.execute('SELECT 1 FROM local_plan LIMIT 1').fetchone():
                raise LocalDeltaError('Original artifact custody table absent after effects; no recreation')
            with transport.db:
                transport.db.execute('CREATE TABLE local_publication_artifact (request_digest TEXT PRIMARY KEY,request TEXT NOT NULL,artifact TEXT NOT NULL)')
        if not transport.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='local_source_plan'").fetchone():
            if transport.db.execute('SELECT 1 FROM local_plan LIMIT 1').fetchone():
                raise LocalDeltaError('Original source plan custody missing; no recreation')
            with transport.db:
                transport.db.execute('CREATE TABLE local_source_plan (request_digest TEXT PRIMARY KEY,original TEXT NOT NULL)')

    def admission_facts(self):
        if encoded(self.source_admission.metadata()) != self.original_admission:
            raise PermissionError('Original admitted source profile metadata changed')
        return json.loads(self.original_admission)

    def require(self, context):
        if context is not self.context or not self.held:
            raise PermissionError('Original private writer interval required')

    @contextmanager
    def writer(self, stream, context):
        if context is not self.context or self.held:
            raise PermissionError('Exclusive original process writer required')
        with (self.transport.journal_path.parent / '.pipeline-writer.lock').open('a') as gate:
            fcntl.flock(gate, fcntl.LOCK_EX)
            self.held = True
            try:
                yield
            finally:
                self.held = False
                fcntl.flock(gate, fcntl.LOCK_UN)

    def schema_admit(self, change):
        self.admission_facts()
        if change not in self.allowed_changes:
            raise PermissionError('No independently admitted original source change')
        if self.source_admission.admit(change) is not None:
            raise PermissionError('Original source semantic admission incomplete')
        self.admission_facts()

    def schema_transition_admit(self, previous, change):
        """Exact source-profile transition policy; no revision-string inference."""
        self.schema_admit(change)
        callback = getattr(self.source_admission, 'admit_transition', None)
        if not callable(callback):
            raise PermissionError('Explicit source-profile schema transition policy required')
        if callback(previous, change) is not None:
            raise PermissionError('Original source schema transition admission incomplete')
        self.admission_facts()

    def plan_graph(self, prior, batch, tables, *, materialized_at):
        return graph_sql_plan(prior, batch, tables, materialized_at=materialized_at, schema_policy=self.schema_admit, schema_transition_policy=self.schema_transition_admit)

    def source_admit(self, request):
        self.admission_facts()
        if self.source_sessions is not None:
            self.require(self.context)
            if encoded(self.source_sessions.metadata()) != self.original_source_sessions:
                raise PermissionError('Original ordinary source registrations changed')
            if self.source_sessions.admit(request, context=self.context) is not None:
                raise PermissionError('Original ordinary source admission incomplete')
            if encoded(self.source_sessions.metadata()) != self.original_source_sessions:
                raise PermissionError('Closing original ordinary source registrations changed')
            self.admission_facts()
            return
        # Compatibility lane for the already qualified private local profile;
        # installed source-set composition is forbidden from choosing this path.
        checkpoint = json.loads(request['source_checkpoint_json'])
        port = self.scope_ports[checkpoint['feed']]
        connection = connect(port['reader'])
        with owned_connection(connection):
            tx = PostgresOutbox(Session(connection), feed=port['scope'].feed, epoch=port['scope'].epoch, schema=port['source']).read(checkpoint['previous'], limit=1)
            if len(tx) != 1 or outbox_checkpoint(tx[0]) != request['source_checkpoint_json'] or batch_row(tx[0].batch)['batch_json'] != request['source_batch_json']:
                raise PermissionError('Native original outbox group differs')
            connection.rollback()

    def _apply(self, request, context, recovery):
        self.require(context)
        self.source_admit(request)
        active = self.policy.active
        if active['request'] != request:
            raise PermissionError('Complete original applying request differs')
        effects = LocalDeltaEffects(self.transport)
        source_plan = {'request': request, 'generated_steps': active['generated_steps'], 'selected_steps': active['steps'], 'zero_match_elisions': active['elisions'], 'complete_prior_oracle': active['previous_expected']}
        retained_plan = self.transport.db.execute('SELECT original FROM local_source_plan WHERE request_digest=?', (request['request_digest'],)).fetchone()
        if retained_plan is None:
            if recovery:
                raise LocalDeltaError('Original source plan custody absent; no replacement')
            anchors = {}
            for (role, rows) in active['previous_expected'].items():
                target = self.transport.targets[self.tables[role]]
                version = int(self.transport.original_history(target)[0]['version'])
                snapshot = self.transport._snapshot(target, version)
                if snapshot['rows'] != sorted(rows, key=encoded):
                    raise ValueError('Complete original prior native state differs: ' + role)
                anchors[target.table] = {'uuid': target.uuid, 'version': version, 'snapshot': snapshot}
            for role in active['previous_expected']:
                self.transport._detail(self.transport.targets[self.tables[role]])
            source_plan['observed_native_prior'] = anchors
            original_plan = encoded(source_plan)
            with self.transport.db:
                self.transport.db.execute('INSERT INTO local_source_plan VALUES(?,?)', (request['request_digest'], original_plan))
        else:
            original_plan = retained_plan[0]
            retained = json.loads(original_plan)
            if {k: v for (k, v) in retained.items() if k != 'observed_native_prior'} != source_plan:
                raise LocalDeltaError('Original exact source plan differs')
            for (table, anchor) in retained['observed_native_prior'].items():
                target = self.transport.targets[table]
                if anchor['uuid'] != target.uuid or encoded(self.transport._snapshot(target, anchor['version'])) != encoded(anchor['snapshot']):
                    raise LocalDeltaError('Original no-op prior native evidence differs')
        if not self.transport.db.execute('SELECT 1 FROM local_plan WHERE operation=?', ('effects:' + request['request_digest'],)).fetchone() and recovery:
            raise LocalDeltaError('Original whole effect plan absent; no replacement')
        proof = (effects.recover if recovery else effects.run)('effects:' + request['request_digest'], request['request_digest'], active['steps'], context=context)
        row = self.transport.db.execute('SELECT request,artifact FROM local_publication_artifact WHERE request_digest=?', (request['request_digest'],)).fetchone()
        if row:
            if row[0] != encoded(request):
                raise PermissionError('Retained artifact original request differs')
            artifact = json.loads(row[1])
            active['manifest'] = artifact['manifest']
            self.validate_manifest(artifact['manifest'], context)
            return row[1]
        versions = {}
        parity = {}
        for role in ('object_current', 'edge_current', 'tombstone', 'whole_source_history'):
            target = self.transport.targets[self.tables[role]]
            version = int(self.transport.original_history(target)[0]['version'])
            snapshot = self.transport._snapshot(target, version)
            if snapshot['rows'] != sorted(active['expected'][role], key=encoded):
                raise ValueError('Independent complete source oracle differs: ' + role)
            versions[target.table] = version
            parity[role] = snapshot['row_sha256']
        proof = {**proof, 'original_generated_steps': active['generated_steps'], 'zero_match_elisions': active['elisions'], 'original_source_plan_sha256': sha(original_plan), 'observed_native_prior': json.loads(original_plan)['observed_native_prior']}
        manifest = {'publication_id': active['publication_id'], 'profile_version': PROFILE, 'table_versions_json': encoded(versions), 'schema_revisions_json': request['schema_revisions_json'], 'source_progress_json': encoded(active['progress']), 'validation_report_json': encoded({'complete': True, 'request_digest': request['request_digest'], 'predecessor': request['predecessor'], 'effect_parity': parity, 'original_source_plan_sha256': sha(original_plan), 'zero_match_elisions': active['elisions'], 'source_oracle_sha256': sha(encoded(active['expected'])), 'source_admission': self.admission_facts(), 'scope': self.admission_facts()['qualification']}), 'recorded_at': '1791547200000000'}
        active['manifest'] = manifest
        artifact = encoded({'effects': proof, 'manifest': manifest})
        with self.transport.db:
            self.transport.db.execute('INSERT INTO local_publication_artifact VALUES(?,?,?)', (request['request_digest'], encoded(request), artifact))
        self.validate_manifest(manifest, context)
        return artifact

    def apply(self, request, context):
        return self._apply(request, context, False)

    def recover_apply(self, request, context):
        return self._apply(request, context, True)

    def validate_manifest(self, row, context):
        self.require(context)
        active = self.policy.active
        if row != active['manifest']:
            raise PermissionError('Immutable original manifest differs')
        if json.loads(row['validation_report_json']).get('source_admission') != self.admission_facts():
            raise PermissionError('Manifest original admitted source profile differs')
        validate_union(active['previous_progress'], active['request'], row)
        self.source_admit(active['request'])
        for (role, expected) in active['expected'].items():
            target = self.transport.targets[self.tables[role]]
            version = json.loads(row['table_versions_json'])[target.table]
            if self.transport._snapshot(target, version)['rows'] != sorted(expected, key=encoded):
                raise ValueError('Original exact publication snapshot differs: ' + role)
        for role in active['expected']:
            self.transport._detail(self.transport.targets[self.tables[role]])

    def validate(self, request, artifact, descriptor, context):
        if json.loads(artifact)['manifest'] != dict(descriptor.raw):
            raise ValueError('Retained original applied manifest differs')
        self.validate_manifest(dict(descriptor.raw), context)

    @contextmanager
    def hold(self, vector, *, context):
        self.require(context)
        if self.pin_held:
            raise PermissionError('No nested private pin interval')
        row = self.policy.active['manifest']
        expected = manifest_pin_vector(row, {t: self.transport.targets[t].uuid for t in json.loads(row['table_versions_json'])}, authority='private-local-process')
        if vector != expected:
            raise PermissionError('Original complete held native vector differs')
        self.pin_held = True
        try:
            self.validate_manifest(row, context)
            yield
            self.validate_manifest(row, context)
        finally:
            self.pin_held = False

    def authorize(self, context, publication_id, tables):
        self.require(context)
        if not self.pin_held or publication_id != self.policy.active['publication_id'] or set(tables) != set(json.loads(self.policy.active['manifest']['table_versions_json'])):
            raise PermissionError('Original complete resolver interval required')

    def descriptors(self, publication_id):
        fields = ','.join(('cast(unix_micros(recorded_at) AS STRING) AS recorded_at' if k == 'recorded_at' else k for k in FIELDS))
        target = self.transport.targets[self.tables['manifest']]
        self.transport._detail(target)
        rows = self.transport.query('SELECT ' + fields + ' FROM `' + '`.`'.join(self.tables['manifest'].split('.')) + '` WHERE publication_id=:id', {'id': publication_id}).rows
        self.transport._detail(target)
        return rows

    def validate_descriptor(self, descriptor, context):
        self.validate_manifest(dict(descriptor.raw), context)

    def inspect_snapshot(self, table, version):
        target = self.transport.targets[table]
        self.transport._snapshot(target, version)
        return Snapshot(table, target.uuid, version)

    def admit_scope(self, scope, session, context):
        self.require(context)
        port = self.scope_ports[scope.feed]
        if scope != port['scope']:
            raise PermissionError('Original native source scope differs')
        binding = session.query('SELECT "' + scope.service_schema + '".scope_binding(CAST(:scope AS uuid)) AS binding', {'scope': scope.scope_id}).rows[0]['binding']
        if binding['source_schema'] != port['source'] or binding['source_signature_sha256'] != port['signature']:
            raise PermissionError('Original namespace/source installation changed')

    def admit_publication(self, scope, request, resolved, session, context):
        if request != self.policy.active['request']:
            raise PermissionError('Original ACK request differs')
        self.validate_manifest(dict(resolved.descriptor.raw), context)

    def acknowledge(self, request, descriptor, context):
        self.require(context)
        if self.lose_manifest:
            self.lose_manifest = False
            raise LostAfterManifest('Injected loss after real immutable manifest commit before PG ACK')
        if (self.admission_facts()['profile'] == 'ashlar-commerce-evolution-source-set/0.1'
                and self.ack_sessions is None):
            raise PermissionError('Installed evolution requires explicit ordinary protected ACK sessions')
        retained = self.transport.db.execute('SELECT request,artifact FROM local_publication_artifact WHERE request_digest=?', (request['request_digest'],)).fetchone()
        if retained is None or retained[0] != encoded(request):
            raise LocalDeltaError('Original applied artifact custody absent; no ACK')
        original_manifest = json.loads(retained[1])['manifest']
        if original_manifest != dict(descriptor.raw):
            raise LocalDeltaError('Original retained manifest differs before ACK')
        self.policy.active['manifest'] = original_manifest
        row = dict(descriptor.raw)
        vector = manifest_pin_vector(row, {t: self.transport.targets[t].uuid for t in descriptor.versions}, authority='private-local-process')

        port = None if self.ack_sessions is not None else self.scope_ports[json.loads(request['source_checkpoint_json'])['feed']]

        def factory(supplied):
            self.require(supplied)
            connection = connect(port['role'])
            if not self.lose_ack:
                return connection
            self.lose_ack = False

            class LostCommit:

                def __getattr__(self, name):
                    return getattr(connection, name)

                def commit(self):
                    connection.commit()
                    raise OSError('Injected response loss after actual PostgreSQL COMMIT')
            return LostCommit()
        if self.ack_sessions is None:
            ack = ProtectedOutboxAck(factory, self, self, self, port['scope'], supported_profiles=[PROFILE], supported_revisions={k: [v] for (k, v) in json.loads(request['schema_revisions_json']).items()})
            ack_ports = {}
        else:
            if encoded(self.ack_sessions.metadata()) != self.original_ack_sessions:
                raise PermissionError('Original ordinary ACK registrations changed')
            ack = self.ack_sessions
            ack_ports = {'pins': self, 'publication_backend': self, 'supported_profiles': [PROFILE],
                         'supported_revisions': {k: [v] for k, v in json.loads(request['schema_revisions_json']).items()}}

        raw_request = encoded(request).encode()
        raw_manifest = encoded(row).encode()
        try:
            receipt = ack.acknowledge(raw_request, raw_manifest, vector, context=context, **ack_ports)
        except AckOutcomeUncertain:
            receipt = ack.reconcile(raw_request, raw_manifest, vector, context=context, **ack_ports)
            if receipt is None:
                raise ValueError('Actual committed original ACK did not reconcile')
            self.acks.append({'publication_id': row['publication_id'], 'uncertain_commit_fresh_reconciled': True})
        else:
            self.acks.append({'publication_id': row['publication_id'], 'uncertain_commit_fresh_reconciled': False})
        if not receipt:
            raise ValueError('Exact protected native ACK receipt missing')
        if self.ack_sessions is not None and encoded(self.ack_sessions.metadata()) != self.original_ack_sessions:
            raise PermissionError('Closing original ordinary ACK registrations changed')
        self.admission_facts()

class AttemptExecutor:

    def __init__(self, driver):
        self.driver = driver

    def query(self, sql, parameters):
        if sql.lstrip().split(None, 1)[0].upper() in ('SELECT', 'DESCRIBE', 'SHOW'):
            return self.driver.transport.query(sql, parameters)
        request = self.driver.policy.active['request']
        return LocalOperationExecutor(self.driver.transport, 'attempt:' + request['request_digest'], request['request_digest'], self.driver.context).query(sql, parameters)

def request_for(stream, transaction, predecessor, revisions):
    row = batch_row(transaction.batch)
    request = {'stream': stream, 'batch_id': transaction.batch.batch_id, 'predecessor': predecessor, 'schema_revisions_json': encoded(revisions), 'source_batch_json': row['batch_json'], 'source_batch_digest': row['batch_digest'], 'source_checkpoint_json': outbox_checkpoint(transaction)}
    request['request_digest'] = hashlib.sha256(json.dumps(request, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return request

def provision_sources(output, fixtures):
    """Fresh dedicated ordinary-role sources only; retains exact submitted DDL/logs."""
    observed = []
    label = subprocess.check_output(['docker', 'inspect', CONTAINER, '--format', '{{index .Config.Labels "ashlar.purpose"}}']).decode().strip()
    if label != 'end-to-end-development':
        raise PermissionError('Original labeled private PostgreSQL required')

    def pg(statement):
        process = subprocess.run(['docker', 'exec', '-i', CONTAINER, 'psql', '-qAt', '-U', 'postgres', '-d', 'truss_e2e', '-v', 'ON_ERROR_STOP=1'], input=statement, text=True, capture_output=True)
        observed.append({'statement': statement, 'exit': process.returncode, 'stdout': process.stdout, 'stderr': process.stderr})
        (output / 'postgres-installation.json').write_text(encoded(observed) + '\n')
        if process.returncode:
            raise RuntimeError('Fresh native installation failed; preserve original namespaces: ' + process.stderr)
    suffix = uuid.uuid4().hex[:12]
    service = 'ashlar_ack_pipeline_' + suffix
    role = 'ashlar_ack_operator_' + suffix
    pg(render_ddl(service, role))
    ports = {}
    for selected in fixtures:
        source = 'ashlar_ack_source_' + suffix + '_' + selected['label']
        writer = source + '_writer'
        reader = source + '_reader'
        ddl = (ROOT / 'sql/ashlar-outbox/01-postgresql.sql').read_text().replace('ashlar_outbox_writer', writer).replace('ashlar_outbox_reader', reader).replace('ashlar_outbox', source)
        pg(ddl)
        scope = AckScope(service, str(uuid.uuid4()), str(uuid.uuid4()), 'private-local-pipeline', selected['feed'], selected['epoch'])
        pg(f"SELECT {service}.register_source('{scope.installation_id}','{source}'); SELECT {service}.register_consumer('{scope.scope_id}','{scope.installation_id}','{scope.consumer}','{scope.feed}','{scope.epoch}');")
        connection = connect(role)
        with owned_connection(connection):
            binding = Session(connection).query('SELECT "' + service + '".scope_binding(CAST(:scope AS uuid)) AS binding', {'scope': scope.scope_id}).rows[0]['binding']
            connection.rollback()
        ports[selected['feed']] = {'source': source, 'writer': writer, 'reader': reader, 'scope': scope, 'role': role, 'signature': binding['source_signature_sha256']}
    return ports
