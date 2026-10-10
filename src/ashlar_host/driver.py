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

def original_native_equal(supplied, expected):
    """Exact primitive/native containers; numeric equivalence is not original custody."""
    if type(supplied) is not type(expected):
        return False
    if type(expected) is dict:
        if any(type(key) is not str for key in supplied) or any(type(key) is not str for key in expected):
            return False
        return set(supplied) == set(expected) and all(original_native_equal(supplied[key], value)
                                                     for key, value in expected.items())
    if type(expected) in (list, tuple):
        return len(supplied) == len(expected) and all(original_native_equal(a, b) for a, b in zip(supplied, expected))
    if type(expected) in (str, int, bool, float, type(None)):
        return supplied == expected
    return False

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

    def activate_evolution(self, plan, context, admission):
        """Own active native intent only after independent current plan admission."""
        from .evolution_plan import EvolutionAttemptPlan
        if type(plan) is not EvolutionAttemptPlan or context is not self.context or self.initializing:
            raise PermissionError('Original noninitializing evolution writer required')
        if not callable(getattr(admission, 'admit', None)) or admission.admit(plan, context) is not None:
            raise PermissionError('Independent original evolution admission required')
        value = plan.document()
        self.active = {'request': value['request'], 'steps': value['selected_steps'],
            'expected': value['expected'], 'publication_id': value['publication_id'],
            'previous_progress': value['previous_progress'], 'progress': value['progress'],
            'previous_expected': value['previous_expected'], 'generated_steps': value['generated_steps'],
            'elisions': value['zero_match_elisions'], 'recorded_at': value['recorded_at']}

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

    @classmethod
    def with_registered_evolution(cls, transport, policy, context, tables, allowed_changes,
                                  columns, *, source_admission, source_sessions, ack_factory):
        """Transfer the transport into one complete existing evolution owner.

        The factory receives a construction-only driver; writer/publication ports
        remain closed until an exact copied ACK owner matches every source. On
        any construction failure the transferred transport is closed. Other
        supplied resources retain their enclosing provider's lifecycle ownership.
        """
        from .lifecycle import finish
        if (cls is not NativeDriver or type(source_sessions) is not RegisteredOutboxSources
                or not callable(ack_factory) or not callable(getattr(transport, 'close', None))):
            raise PermissionError('Exact existing evolution construction ports required')
        primary = None; result = None
        try:
            retained_tables = {row[0] for row in transport.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if not {'local_plan', 'local_operation', 'local_publication_artifact',
                    'local_source_plan'}.issubset(retained_tables):
                raise PermissionError('Complete preexisting original publication journal required')
            sources = RegisteredOutboxSources(source_sessions.registrations, source_sessions.policy)
            result = cls(transport, policy, context, tables, {}, allowed_changes, columns,
                source_admission=source_admission, source_sessions=sources)
            result._evolution_constructing = True
            supplied = ack_factory(result)
            if type(supplied) is not RegisteredOutboxAcks:
                raise PermissionError('Complete registered ordinary ACK owner required')
            acks = RegisteredOutboxAcks(supplied.registrations)
            if acks.metadata()['registrations'] != sources.metadata()['registrations']:
                raise PermissionError('Complete original source/ACK registrations differ')
            result.ack_sessions = acks
            result.original_ack_sessions = encoded(acks.metadata())
            result._evolution_constructing = False
        except BaseException as error:
            primary = error
        finish(primary, [] if primary is None else [transport.close])
        return result

    def evolution_run_identity(self, *, context):
        from .evolution_admission import EvolutionSourceSet
        self.require(context)
        if (type(self.source_admission) is not EvolutionSourceSet or len(self.source_admission.admissions) != 2
                or self.source_sessions is None or self.ack_sessions is None):
            raise PermissionError('Exactly two independently admitted source/session owners required')
        self.transport._profile()
        for target in self.transport.targets.values(): self.transport._detail(target)
        return {'installation_id': self.transport.installation_id,
            'registry': {target.table: {'uuid': target.uuid, 'path': str(target.path)}
                         for target in self.transport.targets.values()},
            'tables': dict(self.tables), 'source_admission': self.admission_facts(),
            'source_sessions': self.source_sessions.metadata(), 'ack_sessions': self.ack_sessions.metadata()}

    def verify_evolution_sources(self, sources, *, context):
        from .evolution_admission import EvolutionSourceSet
        self.require(context)
        if type(sources) is not EvolutionSourceSet or not original_native_equal(sources.metadata(), self.admission_facts()):
            raise PermissionError('Fresh public producer/source correspondence differs')

    def capture_evolution_run(self, request, *, context):
        from .evolution_run import EvolutionRunRequest, EvolutionRunDefinition, PROFILE as RUN_PROFILE
        if type(request) is not EvolutionRunRequest:
            raise PermissionError('Exact developer-owned evolution run request required')
        identity = self.evolution_run_identity(context=context)
        if set(request.source_order) != {item.prepared.source_system for item in self.source_admission.admissions}:
            raise PermissionError('Complete original two-source schedule inventory differs')
        for target in self.transport.targets.values():
            version = int(self.transport.original_history(target)[0]['version'])
            if self.transport._snapshot(target, version)['rows']:
                raise PermissionError('Explicit new empty authorized installation required')
        for table in ('local_operation', 'local_plan', 'local_source_plan', 'local_publication_artifact'):
            if self.transport.db.execute('SELECT 1 FROM ' + table + ' LIMIT 1').fetchone():
                raise PermissionError('Original installation already contains publication custody')
        return EvolutionRunDefinition(encoded({'profile': RUN_PROFILE, 'request': request.document(),
            'schedule': request.schedule(), 'installation': identity}).encode())

    def validate_evolution_run(self, original, *, context):
        from .evolution_run import EvolutionRunDefinition
        if type(original) is not EvolutionRunDefinition or not original_native_equal(
                original.document()['installation'], self.evolution_run_identity(context=context)):
            raise PermissionError('Complete original run installation/source/session identity differs')

    def evolution_sources(self, *, context):
        """Return this owner's already reconstructed immutable semantic source set."""
        self.evolution_run_identity(context=context)
        return self.source_admission

    def admit_evolution_run_ledger(self, original, inventory, *, context, attempt_admission):
        """Observe complete original lineage; never submit, prepare, repair or ACK.

        The independent reservation owner must admit the exact original ledger
        before and after this gate. Native absence cannot establish a retained
        not-started reservation. Started operations retain their existing recovery
        path, while completed slots require settled phase, manifest and ACK custody.
        """
        from .evolution_run import EvolutionRunLedgerView, decode
        from .evolution_plan import EvolutionAttemptPlan
        from .evolution_composition import OriginalRunCorrespondence
        from ashlar.attempt_store import DeltaAttemptStore
        from ashlar.stored_publisher import validate_applied_artifact
        from .lifecycle import owned_context
        self.validate_evolution_run(original, context=context)
        if (type(inventory) is not EvolutionRunLedgerView or type(inventory.slots) is not tuple
                or len(inventory.slots) != 8 or not callable(getattr(attempt_admission, 'admit', None))):
            raise PermissionError('Complete immutable original ledger and current plan authority required')
        # Validate the complete inventory before acquiring any native session.
        gap = False
        for index, slot in enumerate(inventory.slots):
            if type(slot) is not tuple or len(slot) != 5:
                raise PermissionError('Exact original slot carriers required')
            ordinal, state, raw, digest, descriptor = slot
            if type(ordinal) is not int or ordinal != index or type(state) is not str:
                raise PermissionError('Exact original ordered slot required')
            if state == 'unprepared':
                if any(value is not None for value in (raw, digest, descriptor)):
                    raise PermissionError('Unprepared original slot carries custody')
                gap = True
            else:
                if gap or state not in ('retained', 'publication-started', 'completed'):
                    raise PermissionError('Completed prefix and at most one active slot required')
                plan = EvolutionAttemptPlan(raw)
                if type(digest) is not str or plan.sha256 != digest:
                    raise PermissionError('Exact original slot bytes differ')
                if state == 'completed':
                    if type(descriptor) is not bytes or not 0 < len(descriptor) <= 4194304:
                        raise PermissionError('Complete original descriptor bytes required')
                elif descriptor is not None:
                    raise PermissionError('Premature original descriptor custody')
                else:
                    gap = True
        correspondence = OriginalRunCorrespondence(original, self.evolution_sources(context=context))
        previous_active = self.policy.active
        try:
            for ordinal, state, raw, digest, descriptor_bytes in inventory.slots:
                facts = correspondence.at(ordinal); request = facts['request']
                target = self.transport.targets[self.tables['attempts']]
                store = DeltaAttemptStore(AttemptExecutor(self), CarrierPolicy(self, 'attempts'),
                    target.table, target.uuid, original_request=request)
                records = None
                with owned_context(store.session(context)) as session:
                    records = session.read(request['stream'], request['batch_id'])
                if records is None:
                    raise PermissionError('Original phase observation completion suppressed')
                if state in ('unprepared', 'retained'):
                    checkpoint = json.loads(request['source_checkpoint_json'])
                    source = next(item for item in self.source_admission.admissions
                                  if item.prepared.source_system == checkpoint['feed'])
                    batch = source.prepared.batches[int(checkpoint['position']) - 1]
                    graph_tables = {role: self.tables[role] for role in self.source_admission.columns}
                    _, generated = self.plan_graph(self.source_admission.state_at(
                        facts['schema_state']['previous_prefixes']), batch, graph_tables,
                        materialized_at=facts['materialized_at'])
                    selected, _ = local_effect_plan(generated, facts['previous_expected'], graph_tables)
                    _, _, identities = LocalDeltaEffects(self.transport).original_plan(
                        'effects:' + request['request_digest'], request['request_digest'], selected)
                    known = self.transport.db.execute('SELECT operation,intent FROM local_operation LIMIT 1001').fetchall()
                    if len(known) > 1000:
                        raise PermissionError('Bounded complete original submission custody required')
                    for operation_key, intent_text in known:
                        if type(intent_text) is not str or len(intent_text.encode()) > 4194304:
                            raise PermissionError('Bounded original operation custody required')
                        intent = json.loads(intent_text)
                        if (operation_key in identities or intent.get('operation') in identities
                                or intent.get('request_digest') == request['request_digest']):
                            raise PermissionError('Known original submission contradicts not-started reservation')
                    for table in ('local_source_plan', 'local_publication_artifact'):
                        if self.transport.db.execute('SELECT 1 FROM ' + table + ' WHERE request_digest=? LIMIT 1',
                                (request['request_digest'],)).fetchone():
                            raise PermissionError('Known original artifact contradicts not-started reservation')
                    for observed_target in self.transport.targets.values():
                        for history in self.transport.original_history(observed_target):
                            metadata = json.loads(history.get('userMetadata') or '{}')
                            if (type(metadata) is dict and metadata.get('profile') == 'ashlar-local-delta-commit/0.1'
                                    and metadata.get('installation_id') == self.transport.installation_id
                                    and (metadata.get('request_digest') == request['request_digest']
                                         or metadata.get('operation') in identities)):
                                raise PermissionError('Named native commit contradicts not-started reservation')
                if state == 'unprepared':
                    if records:
                        raise PermissionError('Native original submission contradicts unprepared reservation')
                    continue
                plan = EvolutionAttemptPlan(raw); correspondence.admit_attempt(ordinal, plan)
                self.admit_evolution_plan(plan, context=context, fresh=False)
                self.policy.activate_evolution(plan, context, attempt_admission)
                value = plan.document(); operation = 'effects:' + request['request_digest']
                effects = LocalDeltaEffects(self.transport)
                saved = self.transport.db.execute('SELECT original,digest FROM local_plan WHERE operation=?',
                                                   (operation,)).fetchone()
                if state == 'retained':
                    if records:
                        raise PermissionError('Native phase contradicts original not-started reservation')
                    if saved is not None:
                        observation = effects.observe(operation, request['request_digest'], value['selected_steps'], context=context)
                        if any(status != 'no-operation-record' for status in observation['states']):
                            raise PermissionError('Native effect custody contradicts not-started reservation')
                    continue
                # Missing whole-plan custody after started never authorizes reconstruction.
                observation = effects.observe(operation, request['request_digest'], value['selected_steps'], context=context)
                if state != 'completed':
                    continue
                if not records or records[-1].phase != 'committed' or any(
                        status != 'committed' for status in observation['states']):
                    raise PermissionError('Completed original slot lacks settled phase/effect custody')
                payload = json.loads(records[-1].payload_json)
                retained = self.transport.db.execute('SELECT request,artifact FROM local_publication_artifact WHERE request_digest=?',
                    (request['request_digest'],)).fetchall()
                if retained != [(encoded(request), payload['result_json'])]:
                    raise PermissionError('Complete original applied artifact custody differs')
                artifact, descriptor = validate_applied_artifact(payload['result_json'], request)
                source_plan = {'request': request, 'generated_steps': value['generated_steps'],
                    'selected_steps': value['selected_steps'], 'zero_match_elisions': value['zero_match_elisions'],
                    'complete_prior_oracle': value['previous_expected'],
                    'observed_native_prior': value['observed_native_prior']}
                source_rows = self.transport.db.execute('SELECT original FROM local_source_plan WHERE request_digest=?',
                    (request['request_digest'],)).fetchall()
                if source_rows != [(encoded(source_plan),)] or artifact['effects'].get(
                        'original_source_plan_sha256') != sha(encoded(source_plan)):
                    raise PermissionError('Complete original source/effect plan custody differs')
                row = dict(descriptor.raw)
                if (not original_native_equal(decode(descriptor_bytes), row)
                        or not original_native_equal(json.loads(payload['descriptor_json']), row)
                        or row['publication_id'] != facts['publication_id']):
                    raise PermissionError('Completed original descriptor correspondence differs')
                self.policy.active['manifest'] = row
                self.validate_manifest(row, context)
                handles = self.transport.db.execute('SELECT operation,intent FROM local_operation LIMIT 1001').fetchall()
                if len(handles) > 1000:
                    raise PermissionError('Bounded complete original operation inventory required')
                expected_phases = {record.phase: {'stream': request['stream'], 'batch_id': request['batch_id'],
                    'phase': record.phase, 'request_digest': record.request_digest,
                    'payload_json': record.payload_json, 'payload_digest': record.payload_digest} for record in records}
                seen_graph = set(); seen_phases = set(); manifests = 0
                for key, intent_text in handles:
                    if type(intent_text) is not str or len(intent_text.encode()) > 4194304:
                        raise PermissionError('Bounded original operation intent required')
                    intent = json.loads(intent_text)
                    if intent.get('request_digest') != request['request_digest']:
                        continue
                    if key in value['operations']:
                        seen_graph.add(key)
                    elif intent.get('table') == self.tables['attempts']:
                        phase_row = json.loads(intent['parameters'].get('payload', 'null'))
                        phase = phase_row.get('phase') if type(phase_row) is dict else None
                        if phase in seen_phases or phase not in expected_phases or not original_native_equal(phase_row, expected_phases[phase]):
                            raise PermissionError('Original phase operation payload inventory differs')
                        seen_phases.add(phase)
                    elif intent.get('table') == self.tables['manifest']:
                        if not original_native_equal(json.loads(intent['parameters'].get('row', 'null')), row):
                            raise PermissionError('Original manifest operation payload differs')
                        manifests += 1
                    else:
                        raise PermissionError('Unknown original operation in completed lineage')
                    self.transport.inspect_committed(key, intent['statement'], intent['parameters'],
                        intent_digest=request['request_digest'], context=context)
                if seen_graph != set(value['operations']) or seen_phases != set(expected_phases) or manifests != 1:
                    raise PermissionError('Complete original graph/phase/manifest operation handles required')
                if not original_native_equal(self.descriptors(row['publication_id']), [row]):
                    raise PermissionError('Actual immutable native manifest missing or ambiguous')
                vector = manifest_pin_vector(row, {table: self.transport.targets[table].uuid
                    for table in descriptor.versions}, authority='private-local-process')
                receipt = self.ack_sessions.reconcile(encoded(request).encode(), encoded(row).encode(), vector,
                    context=context, pins=self, publication_backend=self, supported_profiles=[PROFILE],
                    supported_revisions={key: [revision] for key, revision in descriptor.revisions.items()})
                if receipt is None:
                    raise PermissionError('Completed original protected ACK readback absent')
                self.source_admit(request)
            if not any(slot[1] == 'publication-started' for slot in inventory.slots):
                completed = [slot[0] for slot in inventory.slots if slot[1] == 'completed']
                expected = (correspondence.at(completed[-1])['expected'] if completed else
                    correspondence.at(0)['previous_expected'])
                for role, rows in expected.items():
                    target = self.transport.targets[self.tables[role]]
                    version = int(self.transport.original_history(target)[0]['version'])
                    if not original_native_equal(self.transport._snapshot(target, version)['rows'],
                                                 sorted(rows, key=encoded)):
                        raise PermissionError('Actual settled original native prefix differs')
        finally:
            self.policy.active = previous_active
        self.validate_evolution_run(original, context=context)

    def plan_evolution_transaction(self, original, ordinal, previous_progress, *, context):
        """Generate one original plan from admitted schedule and actual current anchors."""
        from .evolution_plan import EvolutionAttemptPlan, PROFILE as PLAN_PROFILE
        from .evolution_run import request_from_document
        from ashlar.outbox import OutboxTransaction
        self.validate_evolution_run(original, context=context)
        if type(ordinal) is not int or not 0 <= ordinal < 8:
            raise PermissionError('Original eight-step ordinal required')
        definition = original.document(); request_config = request_from_document(definition['request'])
        step = definition['schedule'][ordinal]
        previous = {source: sum(item['source'] == source for item in definition['schedule'][:ordinal])
                    for source in request_config.source_order}
        following = dict(previous); following[step['source']] += 1
        clocks = dict(zip(request_config.source_order, request_config.clocks))
        source = next(item for item in self.source_admission.admissions
                      if item.prepared.source_system == step['source'])
        batch = source.prepared.batches[step['prefix'] - 1]
        raw = batch.begin + b''.join(item.raw for item in batch.records) + batch.commit
        transaction = OutboxTransaction('ashlar-postgresql-outbox/0.1', batch.feed, batch.epoch,
            str(previous[step['source']]), str(following[step['source']]), hashlib.sha256(raw).hexdigest(), batch)
        revisions = {item.prepared.source_system: item.prepared.schema_revisions[
            following[item.prepared.source_system] - 1] for item in self.source_admission.admissions
            if following[item.prepared.source_system]}
        predecessor = request_config.predecessor if ordinal == 0 else definition['schedule'][ordinal - 1]['publication_id']
        request = request_for(request_config.stream, transaction, predecessor, revisions)
        prior = self.source_admission.oracle(previous, materialized_at=clocks)
        expected = self.source_admission.oracle(following, materialized_at=clocks)
        graph_tables = {role: self.tables[role] for role in self.source_admission.columns}
        _, generated = self.plan_graph(self.source_admission.state_at(previous), batch,
                                      graph_tables, materialized_at=step['materialized_at'])
        selected, elisions = local_effect_plan(generated, prior, graph_tables)
        anchors = {}
        for role, table in graph_tables.items():
            target = self.transport.targets[table]
            version = int(self.transport.original_history(target)[0]['version'])
            snapshot = self.transport._snapshot(target, version)
            if not original_native_equal(snapshot['rows'], sorted(prior[role], key=encoded)):
                raise PermissionError('Complete previous publication/native prefix differs')
            anchors[table] = {'uuid': target.uuid, 'version': version, 'snapshot': snapshot}
        _, _, operations = LocalDeltaEffects(self.transport).original_plan(
            'effects:' + request['request_digest'], request['request_digest'], selected)
        plan = EvolutionAttemptPlan(encoded({'profile': PLAN_PROFILE, 'request': request,
            'generated_steps': generated, 'selected_steps': selected, 'zero_match_elisions': elisions,
            'observed_native_prior': anchors, 'publication_id': step['publication_id'],
            'materialized_at': step['materialized_at'], 'recorded_at': step['recorded_at'],
            'previous_expected': prior, 'expected': expected, 'previous_progress': previous_progress,
            'progress': progress_union(previous_progress, json.loads(request['source_checkpoint_json'])),
            'schema_state': {'previous_prefixes': previous, 'prefixes': following, 'clocks': clocks},
            'resource_registry': definition['installation']['registry'],
            'source_admission': self.admission_facts(), 'operations': operations}).encode())
        from .evolution_composition import OriginalRunCorrespondence
        OriginalRunCorrespondence(original, self.source_admission).admit_attempt(ordinal, plan)
        self.admit_evolution_plan(plan, context=context, fresh=True)
        return plan

    def publish_evolution_attempt_held(self, journal, *, context, expected_sha256):
        """Run coordinator entry; keep the actual original exclusive writer held."""
        self.require(context)
        return self._publish_evolution_attempt(journal, context=context, expected_sha256=expected_sha256)

    def admit_evolution_plan(self, plan, *, context, fresh):
        """Check complete original semantic/native correspondence under held writer.

        These observations do not replace the journal's mandatory independent
        current source/writer/installation policy or the transport's admission.
        """
        from .evolution_plan import EvolutionAttemptPlan
        from .evolution_admission import EvolutionSourceSet
        from .source_sessions import request_snapshot
        self.require(context)
        if (type(plan) is not EvolutionAttemptPlan or type(fresh) is not bool
                or type(self.source_admission) is not EvolutionSourceSet
                or self.source_sessions is None or self.ack_sessions is None
                or not callable(getattr(self.policy, 'activate_evolution', None))):
            raise PermissionError('Complete original evolution owners required')
        value = plan.document(); request, checkpoint = request_snapshot(value['request'])
        if not original_native_equal(value['source_admission'], self.admission_facts()):
            raise PermissionError('Original evolution source facts differ')
        schema = value['schema_state']
        if set(schema) != {'previous_prefixes', 'prefixes', 'clocks'}:
            raise PermissionError('Complete original prefixes and transaction clocks required')
        previous = self.source_admission.prefixes(schema['previous_prefixes'])
        following = self.source_admission.prefixes(schema['prefixes'])
        if (type(schema['clocks']) is not dict
                or any(type(sequence) is not list or len(sequence) != 4 for sequence in schema['clocks'].values())):
            raise PermissionError('Exact original four-clock inventory required')
        clocks = {source: tuple(sequence) for source, sequence in schema['clocks'].items()}
        feed = checkpoint['feed']
        if (feed not in previous or following != {source: prefix + (source == feed)
                for source, prefix in previous.items()} or not 1 <= following[feed] <= 4):
            raise PermissionError('Exactly one original source prefix must advance')
        admitted = next(item for item in self.source_admission.admissions
                        if item.prepared.source_system == feed)
        batch = admitted.prepared.batches[following[feed] - 1]
        if (checkpoint['epoch'] != admitted.prepared.epoch
                or batch_row(batch)['batch_json'] != request['source_batch_json']
                or value['materialized_at'] != clocks[feed][following[feed] - 1]):
            raise PermissionError('Original source transaction or clock differs')
        expected_previous = self.source_admission.oracle(previous, materialized_at=clocks)
        expected = self.source_admission.oracle(following, materialized_at=clocks)
        revisions = {item.prepared.source_system: item.prepared.schema_revisions[
            following[item.prepared.source_system] - 1] for item in self.source_admission.admissions
            if following[item.prepared.source_system]}
        if (not original_native_equal(value['previous_expected'], expected_previous) or not original_native_equal(value['expected'], expected)
                or not original_native_equal(json.loads(request['schema_revisions_json']), revisions)
                or not original_native_equal(value['progress'], progress_union(value['previous_progress'], checkpoint))
                or not value['recorded_at'].isascii() or not value['recorded_at'].isdigit()
                or len(value['recorded_at']) > 19 or str(int(value['recorded_at'])) != value['recorded_at']
                or not 0 <= int(value['recorded_at']) < 2**63):
            raise PermissionError('Complete original oracle/schema/progress differs')
        from ashlar.source_checkpoint import validate_outbox_checkpoint
        consumed = {source for source, prefix in previous.items() if prefix}
        if (set(value['previous_progress']) != consumed
                or checkpoint['previous'] != str(previous[feed])
                or not original_native_equal(self.columns, self.source_admission.columns)):
            raise PermissionError('Complete original consumed source progress/columns required')
        for item in self.source_admission.admissions:
            source = item.prepared.source_system
            if source in consumed:
                original_checkpoint = value['previous_progress'][source]
                validate_outbox_checkpoint(encoded(original_checkpoint), item.prepared.batches[previous[source] - 1])
                if original_checkpoint['position'] != str(previous[source]):
                    raise PermissionError('Original source prefix/native position differs')
        graph_tables = {role: self.tables[role] for role in self.source_admission.columns}
        _, generated = self.plan_graph(self.source_admission.state_at(previous), batch,
                                       graph_tables, materialized_at=value['materialized_at'])
        selected, elisions = local_effect_plan(generated, expected_previous, graph_tables)
        if (not original_native_equal(value['generated_steps'], generated) or not original_native_equal(value['selected_steps'], selected)
                or not original_native_equal(value['zero_match_elisions'], elisions)):
            raise PermissionError('Original generated/effective ordered plan differs')
        registry = {target.table: {'uuid': target.uuid, 'path': str(target.path)}
                    for target in self.transport.targets.values()}
        if not original_native_equal(value['resource_registry'], registry):
            raise PermissionError('Original observed installation registry differs')
        if set(value['observed_native_prior']) != set(graph_tables.values()):
            raise PermissionError('Complete original prior native anchors required')
        for role, table in graph_tables.items():
            target = self.transport.targets[table]; anchor = value['observed_native_prior'][table]
            if (set(anchor) != {'uuid', 'version', 'snapshot'} or anchor['uuid'] != target.uuid
                    or type(anchor['version']) is not int or anchor['version'] < 0
                    or not original_native_equal(anchor['snapshot'], self.transport._snapshot(target, anchor['version']))
                    or not original_native_equal(anchor['snapshot']['rows'], sorted(expected_previous[role], key=encoded))
                    or (fresh and int(self.transport.original_history(target)[0]['version']) != anchor['version'])):
                raise PermissionError('Original complete prior native evidence differs')
            self.transport._detail(target)
        effects = LocalDeltaEffects(self.transport)
        _, _, operations = effects.original_plan('effects:' + request['request_digest'],
            request['request_digest'], selected)
        if not original_native_equal(operations, value['operations']):
            raise PermissionError('Original effect operation identities differ')
        self.source_admit(request)
        return batch

    def publish_evolution_attempt(self, journal, *, context, original_plan=None, expected_sha256=None):
        from .lifecycle import owned_context
        with owned_context(self.writer('evolution-original-attempt', context)):
            return self._publish_evolution_attempt(journal, context=context,
                original_plan=original_plan, expected_sha256=expected_sha256)

    def _publish_evolution_attempt(self, journal, *, context, original_plan=None, expected_sha256=None):
        """One original transaction via real stored publisher; never initializes.

        Fresh retains exact full intent before native phase/effect submission.
        Resume loads only original bytes and preserves ordinary original recovery.
        The enclosing eight-step run and its schedule custody are separate owners.
        """
        from .evolution_plan import EvolutionPlanJournal, EvolutionAttemptPlan
        from .evolution_run import EvolutionRunAttemptJournal
        from ashlar.attempt_store import DeltaAttemptStore
        from ashlar.stored_publisher import StoredPublisherBackend
        from ashlar.publisher import publish_batch
        from .lifecycle import owned_context
        if (type(journal) not in (EvolutionPlanJournal, EvolutionRunAttemptJournal) or (original_plan is None) == (expected_sha256 is None)
                or (original_plan is not None and type(original_plan) is not EvolutionAttemptPlan)):
            raise PermissionError('Distinct original fresh or retained resume required')
        owner = self
        @contextmanager
        def _held_evolution_context():
            owner.require(context)
            yield
            owner.require(context)
        class Driver:
            @contextmanager
            def writer(self, stream, supplied):
                owner.require(supplied)
                with owned_context(_held_evolution_context()):
                    plan = original_plan if original_plan is not None else journal.load(
                        expected_sha256=expected_sha256, context=supplied)
                    owner.admit_evolution_plan(plan, context=supplied, fresh=original_plan is not None)
                    if original_plan is not None:
                        journal.retain(plan, context=supplied)
                    owner.policy.activate_evolution(plan, supplied, journal.policy)
                    value = plan.document()
                    effects = LocalDeltaEffects(owner.transport)
                    if original_plan is not None:
                        effects.prepare('effects:' + value['request']['request_digest'],
                            value['request']['request_digest'], value['selected_steps'], context=supplied)
                    elif type(journal) is EvolutionRunAttemptJournal and journal.ledger.slot(journal.ordinal)[0] == 'retained':
                        # Positive original run reservation proves this composition never entered
                        # any native phase; missing local/native records are not that proof.
                        journal.not_started(plan, supplied)
                        owner.admit_evolution_plan(plan, context=supplied, fresh=True)
                        effects.prepare_unsubmitted('effects:' + value['request']['request_digest'],
                            value['request']['request_digest'], value['selected_steps'],
                            reservation=journal, plan=plan, context=supplied)
                    else:
                        effects.observe('effects:' + value['request']['request_digest'],
                            value['request']['request_digest'], value['selected_steps'], context=supplied)
                    if type(journal) is EvolutionRunAttemptJournal:
                        journal.start(plan, supplied)
                    yield
                    closing = journal.load(expected_sha256=plan.sha256, context=supplied)
                    if closing.raw != plan.raw:
                        raise PermissionError('Closing exact original attempt differs')
                    owner.source_admit(value['request'])
                    owner.admission_facts()
            def apply(self, request, supplied): return owner.apply(request, supplied)
            def recover_apply(self, request, supplied): return owner.recover_apply(request, supplied)
            def validate(self, request, artifact, descriptor, supplied): return owner.validate(request, artifact, descriptor, supplied)
            def acknowledge(self, request, descriptor, supplied): return owner.acknowledge(request, descriptor, supplied)
        # Resume selection is read-only; the authoritative load is renewed held.
        plan = original_plan if original_plan is not None else journal.load(
            expected_sha256=expected_sha256, context=context)
        request = plan.document()['request']
        from ashlar.staging import batch_from_row
        raw = json.loads(request['source_batch_json'])
        batch = batch_from_row({'source_profile': raw['profile'], 'feed': raw['feed'], 'epoch': raw['epoch'],
            'batch_id': raw['batch_id'], 'cursor_before': raw['cursor_before'], 'cursor_after': raw['cursor_after'],
            'records_digest': raw['records_sha256'], 'batch_json': request['source_batch_json'],
            'batch_digest': request['source_batch_digest']})
        target = self.transport.targets[self.tables['attempts']]
        backend = StoredPublisherBackend(DeltaAttemptStore(AttemptExecutor(self), CarrierPolicy(self, 'attempts'),
            target.table, target.uuid, original_request=request if type(journal) is EvolutionRunAttemptJournal else None), Driver(), lambda original, supplied: ManifestPort(self, original))
        return publish_batch(backend, request['stream'], batch, predecessor=request['predecessor'],
            schema_revisions_json=request['schema_revisions_json'], context=context,
            source_checkpoint_json=request['source_checkpoint_json'])

    def admission_facts(self):
        if encoded(self.source_admission.metadata()) != self.original_admission:
            raise PermissionError('Original admitted source profile metadata changed')
        return json.loads(self.original_admission)

    def require(self, context):
        if getattr(self, '_evolution_constructing', False):
            raise PermissionError('Evolution registrations are not complete')
        if context is not self.context or not self.held:
            raise PermissionError('Original private writer interval required')

    @contextmanager
    def writer(self, stream, context):
        if getattr(self, '_evolution_constructing', False):
            raise PermissionError('Evolution registrations are not complete')
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
        manifest = {'publication_id': active['publication_id'], 'profile_version': PROFILE, 'table_versions_json': encoded(versions), 'schema_revisions_json': request['schema_revisions_json'], 'source_progress_json': encoded(active['progress']), 'validation_report_json': encoded({'complete': True, 'request_digest': request['request_digest'], 'predecessor': request['predecessor'], 'effect_parity': parity, 'original_source_plan_sha256': sha(original_plan), 'zero_match_elisions': active['elisions'], 'source_oracle_sha256': sha(encoded(active['expected'])), 'source_admission': self.admission_facts(), 'scope': self.admission_facts()['qualification']}), 'recorded_at': active.get('recorded_at', '1791547200000000')}
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
