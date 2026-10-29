# SPDX-License-Identifier: MIT OR Apache-2.0
from .publication_planning import local_effect_plan, request_for
from .lifecycle import owned_connection
from .resources import RESOURCE_ROOT
'Selected local host implementation; native qualification remains version-scoped.'
import fcntl, hashlib, json, subprocess, uuid, os
from contextlib import contextmanager
from pathlib import Path
from ashlar.manifest import DeltaManifestStore, FIELDS, manifest_pin_vector
from ashlar.outbox import PostgresOutbox
from ashlar.publication import Snapshot
from ashlar.source_checkpoint import outbox_checkpoint
from ashlar.staging import batch_row
from .delta_custody import LocalDeltaEffects, LocalOperationExecutor, child_operation, encoded, sha, LocalDeltaError, operation_capacity
from .connection import connect, CONTAINER
from .postgres import Session
from .ack import AckScope, ProtectedOutboxAck, AckOutcomeUncertain, render_ddl
from .graph_sql import graph_sql_plan
from .source_sessions import RegisteredOutboxSources
from .ack_sessions import RegisteredOutboxAcks
from .truss_graph_admission import (CURRENT_PROFILE as TRUSS_CURRENT_GRAPH_PROFILE,
                                    CURRENT_045_PROFILE as TRUSS_CURRENT_045_GRAPH_PROFILE,
                                    CURRENT_048_PROFILE as TRUSS_CURRENT_048_GRAPH_PROFILE,
                                    CURRENT_049_PROFILE as TRUSS_CURRENT_049_GRAPH_PROFILE,
                                    CURRENT_050_PROFILE as TRUSS_CURRENT_050_GRAPH_PROFILE,
                                    EMPTY_INTERVAL_PROFILE as TRUSS_EMPTY_INTERVAL_PROFILE)
from .truss_graph_admission import PROFILE as TRUSS_SCALAR_GRAPH_PROFILE
from .truss_graph_admission import TYPED_PROFILE as TRUSS_TYPED_GRAPH_PROFILE
ROOT = RESOURCE_ROOT
CLOCK = '2026-10-09T12:00:00+00:00'
PROFILE = 'ashlar-delta/0.3'
_MEDICAL_REQUEST_OMITTED = object()

_EVOLUTION_READ_MINT = object()

class HeldEvolutionPublication:
    """Read-only view minted by the active original NativeDriver interval only."""
    __slots__ = ('driver','context','vector','_row','_facts','_ports','_active')
    def __init__(self, token, driver, context, row, vector, facts):
        if token is not _EVOLUTION_READ_MINT:
            raise PermissionError('Actual original held evolution publication required')
        for name,value in (('driver',driver),('context',context),('vector',vector),
                ('_row',encoded(row)),('_facts',encoded(facts)),
                ('_ports',(driver.transport,driver.policy,driver.source_admission,driver.source_sessions,driver.ack_sessions)),('_active',True)):
            object.__setattr__(self,name,value)
        self.require(context,vector)
    def __setattr__(self,name,value):raise AttributeError('Immutable original held publication')
    def require(self, context, vector):
        from ashlar.pins import PinVector
        owner=self.driver
        if (not self._active or type(owner)is not NativeDriver or context is not self.context
                or type(vector)is not PinVector or vector!=self.vector
                or type(getattr(owner,'_held_original_vector',None))is not PinVector
                or owner._held_original_vector!=self.vector or not owner.pin_held
                or any(a is not b for a,b in zip((owner.transport,owner.policy,owner.source_admission,owner.source_sessions,owner.ack_sessions),self._ports))
                or encoded(owner.policy.active['manifest'])!=self._row):
            raise PermissionError('Exact active original publication/vector/context required')
        current=manifest_pin_vector(json.loads(self._row),{table:owner.transport.targets[table].uuid for table in self.vector.targets},authority='private-local-process')
        if current!=self.vector:raise PermissionError('Original complete native target identities changed')
        owner.require(context)
    @property
    def descriptor(self):
        self.require(self.context,self.vector);return json.loads(self._row)
    @property
    def facts(self):
        self.require(self.context,self.vector);return json.loads(self._facts)
    def _close(self):object.__setattr__(self,'_active',False)

class _OriginalGraphSourceRenewal:
    """Private binding created by the exact original reader factory only."""
    __slots__ = ('owner','_renew')
    def __init__(self, owner, renew):
        object.__setattr__(self,'owner',owner);object.__setattr__(self,'_renew',renew)
    def __setattr__(self,name,value):raise AttributeError('Original graph source binding is immutable')
    def renew(self, vector, context):self._renew(vector,context)

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

def _medical_json_budget(value, maximum, *, ascii_only=False):
    """Count closed protocol JSON before an encoder can allocate a large cell."""
    total = 0; nodes = 0
    def add(size):
        nonlocal total
        total += size
        if total > maximum:
            raise PermissionError('Original complete medical phase cell exceeds selected budget')
    def string(text):
        add(2)
        for character in text:
            point = ord(character)
            if character in ('\\', '"'):
                add(2)
            elif point < 32:
                add(2 if point in (8, 9, 10, 12, 13) else 6)
            elif 55296 <= point <= 57343:
                raise PermissionError('Exact UTF8 medical proof string required')
            elif ascii_only and point >= 128:
                add(6 if point <= 65535 else 12)
            else:
                add(1 if point < 128 else 2 if point < 2048 else 3 if point <= 65535 else 4)
    def visit(item, depth):
        nonlocal nodes
        nodes += 1
        if depth > 64 or nodes > 20000:
            raise PermissionError('Finite closed medical proof JSON required')
        if type(item) is str: string(item)
        elif item is None: add(4)
        elif type(item) is bool: add(4 if item else 5)
        elif type(item) is int:
            if item.bit_length() > 64:
                raise PermissionError('Bounded medical proof integer required')
            add(len(str(item)))
        elif type(item) is dict:
            add(2 + max(0, len(item)-1))
            for key, child in item.items():
                if type(key) is not str:
                    raise PermissionError('Closed medical proof key required')
                string(key); add(1); visit(child, depth+1)
        elif type(item) is list:
            add(2 + max(0, len(item)-1))
            for child in item: visit(child, depth+1)
        else:
            raise PermissionError('Closed original medical proof JSON required')
    visit(value, 0)
    return total

def _medical_descriptor_equal(supplied, expected):
    """Compare the public resolver's frozen containers to an already bounded raw view."""
    from types import MappingProxyType
    if type(expected) is dict:
        return (type(supplied) in (dict, MappingProxyType) and len(supplied) == len(expected)
            and all(type(key) is str and key in supplied and _medical_descriptor_equal(supplied[key], value)
                    for key, value in expected.items()))
    if type(expected) is list:
        return (type(supplied) in (list, tuple) and len(supplied) == len(expected)
            and all(_medical_descriptor_equal(a, b) for a,b in zip(supplied, expected)))
    return original_native_equal(supplied, expected)

def original_history_equal(supplied, expected):
    """Exact complete native history carriers, including Spark timestamp cells."""
    from datetime import datetime, timezone
    if type(supplied) is not type(expected):
        return False
    if type(expected) is datetime:
        return (type(supplied.tzinfo) is type(expected.tzinfo)
                and type(expected.tzinfo) in (type(None), timezone)
                and supplied == expected and supplied.fold == expected.fold
                and supplied.tzname() == expected.tzname()
                and supplied.utcoffset() == expected.utcoffset())
    if type(expected) is dict:
        return (all(type(key) is str for key in supplied) and all(type(key) is str for key in expected)
                and set(supplied) == set(expected)
                and all(original_history_equal(supplied[key], value) for key, value in expected.items()))
    if type(expected) in (list, tuple):
        return len(supplied) == len(expected) and all(original_history_equal(a, b) for a, b in zip(supplied, expected))
    return original_native_equal(supplied, expected)


def progress_union(previous, checkpoint):
    result = json.loads(encoded(previous))
    old = result.get(checkpoint['feed'])
    if old is not None and (old['epoch'] != checkpoint['epoch'] or old['position'] != checkpoint['previous']):
        raise ValueError('Original contiguous source epoch/progress required')
    if old is None and checkpoint['previous'] != '0':
        if checkpoint.get('profile')!='truss-native-feed/0.1':
            raise ValueError('Fresh source must begin at zero')
        import base64
        from ashlar.schema import decode_retained_json
        raw=base64.b64decode(checkpoint['previous'],validate=True)
        cursor=decode_retained_json(raw)
        if (base64.b64encode(raw).decode('ascii')!=checkpoint['previous'] or
                cursor.get('profile')!='truss-caller-asserted-feed/0.1' or
                cursor.get('sourceEpoch')!=checkpoint['epoch'] or cursor.get('nextXid')!='0'):
            raise ValueError('Original native Truss genesis cursor required')
    result[checkpoint['feed']] = dict(checkpoint)
    return result

def validate_union(previous, request, manifest):
    checkpoint = json.loads(request['source_checkpoint_json'])
    if json.loads(manifest['source_progress_json']) != progress_union(previous, checkpoint):
        raise ValueError('Complete independent source progress union differs')
    report = json.loads(manifest['validation_report_json'])
    if report['predecessor'] != request['predecessor'] or report['request_digest'] != request['request_digest']:
        raise ValueError('Global predecessor/request custody differs')


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

    def prepare(self, row, *, context):
        return self.store.prepare(row, context=context)

    def commit(self, row, *, context):
        return self.store.commit(row, context=context)

    def recover(self, row, *, context):
        if not self.owner.transport.db.execute('SELECT 1 FROM local_operation WHERE operation LIKE ?', ('manifest:' + self.executor.intent_digest + ':%',)).fetchone():
            raise LocalDeltaError('Original manifest submission absent; recovery cannot replace it')
        store = DeltaManifestStore(RecoveryExecutor(self.executor), self.store.policy, self.store.table, self.store.uuid)
        return store.commit(row, context=context)

class NativeDriver:

    def __init__(self, transport, policy, context, tables, scope_ports, allowed_changes, columns, *, source_admission, source_sessions=None, ack_sessions=None, lookup_hash_profile='sha256-json-v1'):
        from .truss_feed_sessions import TrussFeedSessions
        self.transport = transport
        self.policy = policy
        self.context = context
        self.tables = tables
        self.scope_ports = scope_ports
        self.allowed_changes = allowed_changes
        self.columns = columns
        if lookup_hash_profile not in ('sha256-json-v1','spark-xxhash64-typed-key/0.1'):
            raise PermissionError('Exact current lookup-hash interpretation required')
        self.lookup_hash_profile = lookup_hash_profile
        installed_profile=getattr(transport,'installation_profile',None)
        if lookup_hash_profile=='spark-xxhash64-typed-key/0.1':
            from .truss_current_catalog import CURRENT_TRUSS_048_VERSION, CURRENT_TRUSS_049_VERSION, CURRENT_TRUSS_050_VERSION
            if (type(installed_profile)is not dict
                    or installed_profile.get('physical_profile')!='ashlar-delta/0.4-xxhash64'
                    or installed_profile.get('lookup_hash_profile')!=lookup_hash_profile
                    or getattr(source_admission,'current_truss_version',None) not in (
                        CURRENT_TRUSS_048_VERSION, CURRENT_TRUSS_049_VERSION, CURRENT_TRUSS_050_VERSION)
                    or set(columns)!={'object_current','edge_current','tombstone','whole_source_history'}
                    or any(dict(columns[role]).get('lookup_hash')!='BIGINT'
                           for role in ('object_current','edge_current'))):
                raise PermissionError('Exact original `.4` installation and data-role schema required')
        elif installed_profile is not None:
            raise PermissionError('A non-`.4` driver cannot open a `.4` local installation')
        if not callable(getattr(source_admission, 'admit', None)) or not callable(getattr(source_admission, 'metadata', None)):
            raise PermissionError('Explicit admitted source profile required')
        self.source_admission = source_admission
        self.original_admission = encoded(source_admission.metadata())
        facts = json.loads(self.original_admission)
        if type(facts) is not dict or not isinstance(facts.get('profile'), str) or (not facts['profile']) or (not isinstance(facts.get('qualification'), str)) or (not facts['qualification']) or (len(self.original_admission.encode()) > 1048576):
            raise PermissionError('Bounded explicit immutable source profile facts required')
        if facts['profile'] == 'ashlar-commerce-evolution-source-set/0.1' and source_sessions is None:
            raise PermissionError('Installed evolution requires explicit ordinary source sessions')
        truss=facts['profile'] in ('ashlar-truss-note-semantic-admission/0.1',
                                  TRUSS_EMPTY_INTERVAL_PROFILE,
                                  TRUSS_SCALAR_GRAPH_PROFILE,TRUSS_TYPED_GRAPH_PROFILE,
                                  TRUSS_CURRENT_GRAPH_PROFILE,TRUSS_CURRENT_045_GRAPH_PROFILE,
                                  TRUSS_CURRENT_048_GRAPH_PROFILE,TRUSS_CURRENT_049_GRAPH_PROFILE,
                                  TRUSS_CURRENT_050_GRAPH_PROFILE)
        if facts['profile'] in (TRUSS_TYPED_GRAPH_PROFILE,TRUSS_CURRENT_GRAPH_PROFILE,
                                TRUSS_CURRENT_045_GRAPH_PROFILE,TRUSS_CURRENT_048_GRAPH_PROFILE,
                                TRUSS_CURRENT_049_GRAPH_PROFILE,TRUSS_CURRENT_050_GRAPH_PROFILE):
            from .truss_graph_admission import TrussGraphAdmission
            if type(source_admission)is not TrussGraphAdmission:raise PermissionError('Actual typed Truss catalog admission required')
        if facts['profile']==TRUSS_EMPTY_INTERVAL_PROFILE:
            from .truss_graph_admission import TrussEmptyIntervalAdmission
            if type(source_admission)is not TrussEmptyIntervalAdmission or source_admission.changes:
                raise PermissionError('Actual no-effect Truss interval admission required')
        if truss:
            if type(source_sessions)is not TrussFeedSessions or ack_sessions is not source_sessions:
                raise PermissionError('Complete original Truss source and ACK session owner required')
            native=source_sessions.metadata()
            if (native['feed'],native['epoch'],native['page_sha256'])!=(facts['feed'],facts['epoch'],facts['source_transaction_sha256']):
                raise PermissionError('Original Truss semantic and native page differs')
        if source_sessions is not None and type(source_sessions) is not (TrussFeedSessions if truss else RegisteredOutboxSources):
            raise PermissionError('Explicit registered ordinary source session owner required')
        self.source_sessions = source_sessions
        self.original_source_sessions = None if source_sessions is None else encoded(source_sessions.metadata())
        if source_sessions is not None and facts['profile'] == 'ashlar-commerce-evolution-source-set/0.1':
            expected = {(item['source_system'], item['epoch']) for item in facts['sources']}
            actual = {(item.scope.feed, item.scope.epoch) for item in source_sessions.registrations}
            if expected != actual:
                raise PermissionError('Complete original semantic/native source inventory differs')
        if ack_sessions is not None and type(ack_sessions) is not (TrussFeedSessions if truss else RegisteredOutboxAcks):
            raise PermissionError('Explicit registered ordinary protected ACK owner required')
        self.ack_sessions = ack_sessions
        self.original_ack_sessions = None if ack_sessions is None else encoded(ack_sessions.metadata())
        if ack_sessions is not None and not truss:
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
    def from_existing_truss(cls, transport, policy, context, tables, allowed_changes,
                            columns, *, source_admission, source_sessions, ack_sessions,
                            lookup_hash_profile='sha256-json-v1'):
        """Construct from complete existing journal custody, without recreating plans.

        This admits construction only. Original request/plan restoration and live
        source rereading remain mandatory before any retry or ACK reconciliation.
        The caller retains ownership of the already-open transport on refusal.
        """
        from .delta_custody import LocalDeltaTransport
        from .truss_graph_admission import TrussGraphAdmission,TrussEmptyIntervalAdmission
        from .truss_feed_sessions import TrussFeedSessions
        if (cls is not NativeDriver or type(transport) is not LocalDeltaTransport
                or type(policy) is not PrivatePolicy or policy.context is not context
                or transport.policy is not policy
                or type(source_admission) not in (TrussGraphAdmission,TrussEmptyIntervalAdmission)
                or type(source_sessions) is not TrussFeedSessions
                or ack_sessions is not source_sessions or source_sessions.context is not context):
            raise PermissionError('Exact existing Truss graph construction ports required')
        LocalDeltaTransport.require_existing_journal(transport.journal_path,
            transport.installation_id, tuple(transport.targets.values()),
            capacity=transport.operation_capacity,
            installation_profile=getattr(transport,'installation_profile',None))
        return cls(transport, policy, context, tables, {}, allowed_changes, columns,
            source_admission=source_admission, source_sessions=source_sessions,
            ack_sessions=ack_sessions,lookup_hash_profile=lookup_hash_profile)

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
        self._validate_evolution_run_ledger(original, inventory, context=context,
            attempt_admission=attempt_admission)

    def _validate_evolution_run_ledger(self, original, inventory, *, context,
                                     attempt_admission, selected_completed=None, collect_completed=False):
        from .immutable_snapshot_memo import immutable_snapshot_memo
        from ashlar.request_validation_memo import request_validation_memo
        with request_validation_memo(), immutable_snapshot_memo(self.transport):
            return self._validate_evolution_run_ledger_uncached(original,inventory,context=context,
                attempt_admission=attempt_admission,selected_completed=selected_completed,
                collect_completed=collect_completed)

    def _validate_evolution_run_ledger_uncached(self, original, inventory, *, context,
                                     attempt_admission, selected_completed=None, collect_completed=False):
        """Observe original lineage without new publication/phase/effect submission.

        Current protected ACK reconciliation observes exact retained receipts using
        SELECT locks and commits the observation transaction; it never submits
        or advances an ACK. This is not a PostgreSQL READ ONLY transaction claim.
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
        if (selected_completed is not None and (type(selected_completed) is not int
                or not 0 <= selected_completed < 8 or inventory.slots[selected_completed][1] != 'completed')):
            raise PermissionError('Exact original completed ordinal required')
        from .phase_absence import PhaseAbsenceOwner
        phase_absence=PhaseAbsenceOwner(self,original,inventory,correspondence,context)
        observed_descriptor = None
        completed_descriptors = []
        previous_active = self.policy.active
        histories = {}
        primary = None
        def renew_history(table, history):
            target = self.transport.targets[table]
            self.transport._detail(target)
            if not original_history_equal(self.transport.original_history(target), history):
                raise PermissionError('Complete original native history changed during admission')
        try:
            if any(slot[1] in ('unprepared', 'retained') for slot in inventory.slots):
                from copy import deepcopy
                for target in self.transport.targets.values():
                    self.transport._detail(target)
                    history = self.transport.original_history(target)
                    if not original_history_equal(history, history):
                        raise PermissionError('Exact complete native history carriers required')
                    histories[target.table] = deepcopy(history)
            for ordinal, state, raw, digest, descriptor_bytes in inventory.slots:
                facts = correspondence.at(ordinal); request = facts['request']
                target = self.transport.targets[self.tables['attempts']]
                store = DeltaAttemptStore(AttemptExecutor(self,phase_absence=phase_absence), CarrierPolicy(self, 'attempts'),
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
                        for history in histories[observed_target.table]:
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
                    context=context, pins=self, publication_backend=self,
                    supported_profiles=[self._selected_publication_profile()],
                    supported_revisions={key: [revision] for key, revision in descriptor.revisions.items()})
                if receipt is None:
                    raise PermissionError('Completed original protected ACK readback absent')
                self.source_admit(request)
                completed_descriptors.append(descriptor)
                if ordinal == selected_completed:
                    observed_descriptor = descriptor
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
        except BaseException as error:
            primary = error
        finally:
            phase_absence.close()
            self.policy.active = previous_active
        from .lifecycle import finish
        finish(primary, [lambda: self.require(context),
            *(lambda table=table, history=history: renew_history(table, history)
              for table, history in histories.items()),
            lambda: self.validate_evolution_run(original, context=context)])
        if selected_completed is not None and observed_descriptor is None:
            raise PermissionError('Complete original native descriptor observation required')
        return tuple(completed_descriptors) if collect_completed else observed_descriptor

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

    @contextmanager
    def hold_evolution_publication(self, original, inventory, ordinal, *, context, policy, bounds):
        """Authenticate one completed original publication inside its real full-vector hold.

        Full ledger/manifest/source/retention and protected receipt observation are
        mandatory. Reconcile observes receipts using SELECT locks and commits its
        transaction; this path never submits or advances an ACK.
        """
        from .evolution_composition import NativeEvolutionRunPolicy, OriginalRunCorrespondence
        from .evolution_plan import EvolutionAttemptPlan
        from .evolution_run import EvolutionRunDefinition, EvolutionRunLedgerView
        from .evolution_verification_types import VerificationBounds
        from .lifecycle import finish, owned_context
        self.require(context)
        if (type(original) is not EvolutionRunDefinition or type(inventory) is not EvolutionRunLedgerView
                or type(policy) is not NativeEvolutionRunPolicy or policy.driver is not self
                or type(bounds) is not VerificationBounds or type(ordinal) is not int or not 0 <= ordinal < 8
                or len(inventory.slots) != 8 or inventory.slots[ordinal][1] != 'completed'
                or getattr(self.transport, 'observation_bounds', None) is not bounds):
            raise PermissionError('Exact completed original bounded verification required')
        plan = EvolutionAttemptPlan(inventory.slots[ordinal][2])
        correspondence = OriginalRunCorrespondence(original, self.evolution_sources(context=context))
        correspondence.admit_attempt(ordinal, plan)
        facts = correspondence.at(ordinal); primary = None; result = None
        admission = policy.original_attempt_admission(original, inventory, context)
        def validate():
            policy.admit_run(original, context)
            if policy.authority.admit_ledger(original, inventory, context) is not None:
                raise PermissionError('Current original complete inventory required')
            return self._validate_evolution_run_ledger(original, inventory, context=context,
                attempt_admission=admission, selected_completed=ordinal)
        try:
            descriptor = validate()
            self.policy.activate_evolution(plan, context, admission)
            row = dict(descriptor.raw)
            self.policy.active['manifest'] = row
            roles = tuple(self.columns)
            if set(roles) != {'object_current', 'edge_current', 'tombstone', 'whole_source_history'} or set(descriptor.versions) != {self.tables[role] for role in roles}:
                raise PermissionError('Exact four materialization roles required')
            vector = manifest_pin_vector(row, {table: self.transport.targets[table].uuid for table in descriptor.versions}, authority='private-local-process')
            held = None
            with owned_context(self.hold(vector, context=context)):
                held = HeldEvolutionPublication(_EVOLUTION_READ_MINT, self, context, row, vector, facts)
                try:
                    yield held
                finally:
                    held._close()
        except BaseException as error:
            primary = error
        finally:
            finish(primary, [validate, lambda: self.require(context)])

    def verify_evolution_publication(self, original, inventory, ordinal, *, context, policy, bounds,
            native_schema_profile='legacy-sql-bootstrap/0.1'):
        """Verify one completed immutable publication without publisher or ACK submission.

        Complete ledger validation uses existing protected SELECT reconciliation.
        The returned rows remain provisional until the caller's acquired owners close.
        """
        from .evolution_verification_types import require_native_schema_profile, CANONICAL_NATIVE_SCHEMA_PROFILE
        from .lifecycle import finish, owned_context
        require_native_schema_profile(native_schema_profile)
        canonical = None
        if native_schema_profile == CANONICAL_NATIVE_SCHEMA_PROFILE:
            from .managed_carriers import load_managed_carriers
            canonical = load_managed_carriers()
        with owned_context(self.hold_evolution_publication(original, inventory, ordinal,
                context=context, policy=policy, bounds=bounds)) as held:
            row = held.descriptor; vector = held.vector; facts = held.facts
            roles = tuple(self.columns)
            observations = {}; schemas = {}; acquired_roles = []
            def capture_role(role, remaining):
                table = self.tables[role]
                snapshot, schema = self.transport.bounded_snapshot(self.transport.targets[table], vector.targets[table][1], remaining_bytes=remaining)
                if not original_native_equal(snapshot['rows'], sorted(facts['expected'][role], key=encoded)):
                    raise PermissionError('Complete original publication bag differs')
                expected = [(name, family.lower()) for name, family in self.columns[role]]
                if [(name, family) for name, family in snapshot['schema']] != expected:
                    raise PermissionError('Admitted original complete column profile differs')
                families = {'bigint': 'long', 'string': 'string', 'timestamp': 'timestamp'}
                if any(family not in families for _, family in expected):
                    raise PermissionError('Unsupported original local bootstrap schema profile')
                fields = (tuple((name, family.upper(), True) for name, family in expected)
                    if canonical is None else canonical.ordered_fields(role))
                if tuple((name, family.lower()) for name, family, _ in fields) != tuple(expected):
                    raise PermissionError('Exact canonical carrier column profile required')
                expected_schema = {'type': 'struct', 'fields': [{'name': name,
                    'type': families[family.lower()], 'nullable': nullable, 'metadata': {}}
                    for name, family, nullable in fields]}
                if not original_native_equal(schema, expected_schema):
                    raise PermissionError('Complete original local bootstrap schema differs')
                size = len(encoded({'schema': schema, 'rows': snapshot['rows']}).encode())
                if size > remaining:
                    raise PermissionError('Cumulative four-bag evidence bound exceeded')
                return snapshot, schema, size
            selected_primary = None; total = 0; closing_total = [0]
            try:
                for role in roles:
                    acquired_roles.append(role)
                    snapshot, schema, size = capture_role(role, bounds.total_bytes-total)
                    observations[role] = snapshot; schemas[role] = schema; total += size
            except BaseException as error:
                selected_primary = error
            def closing_role(role):
                snapshot, schema, size = capture_role(role, bounds.total_bytes-closing_total[0])
                closing_total[0] += size
                if role in observations and (not original_native_equal(snapshot, observations[role]) or not original_native_equal(schema, schemas[role])):
                    raise PermissionError('Closing complete native bags/schema differ')
            finish(selected_primary, [lambda: self.validate_manifest(row, context),
                lambda: self.source_admit(facts['request']),
                *(lambda role=role: closing_role(role) for role in acquired_roles)])
            result = {'descriptor': row, 'bags': observations, 'schemas': schemas,
                'completed_prefix': sum(slot[1] == 'completed' for slot in inventory.slots)}
            if canonical is not None:
                result.update(native_schema_profile=native_schema_profile, generated_carriers_sha256=canonical.generated_sha256)
        return result

    def observe_completed_evolution_prefix(self, ledger, *, context):
        """One full native pass plus each original stored-publisher observation.

        Reuse is confined to this held owner and unchanged complete inventory;
        the returned descriptors confer no later source or submission authority.
        """
        from .evolution_run import EvolutionRunLedger
        from .evolution_composition import NativeEvolutionRunPolicy
        self.require(context)
        if (type(ledger) is not EvolutionRunLedger
                or type(ledger.policy) is not NativeEvolutionRunPolicy
                or ledger.policy.driver is not self or ledger.context is not context):
            raise PermissionError('Exact original prefix owner required')
        policy = ledger.policy; inventory = ledger._inventory()
        def renew():
            policy.admit_run(ledger.original, context)
            if policy.authority.admit_ledger(ledger.original, inventory, context) is not None:
                raise PermissionError('Current original prefix authority incomplete')
            if ledger._inventory() != inventory:
                raise PermissionError('Original completed prefix changed')
        primary = None; result = None
        try:
            renew()
            descriptors = self._validate_evolution_run_ledger(ledger.original, inventory,
                context=context, attempt_admission=policy.original_attempt_admission(
                    ledger.original, inventory, context), collect_completed=True)
            result = tuple(self._observe_completed_evolution_attempt(ledger.attempt(index),
                context=context, expected_sha256=inventory.slots[index][3],
                prefix=(inventory, descriptors)) for index in range(len(descriptors)))
        except BaseException as error:
            primary = error
        from .lifecycle import finish
        finish(primary, [renew, lambda: self.require(context)])
        return inventory, result

    def observe_completed_evolution_attempt(self, journal, *, context, expected_sha256):
        return self._observe_completed_evolution_attempt(journal, context=context,
            expected_sha256=expected_sha256)

    def _observe_completed_evolution_attempt(self, journal, *, context, expected_sha256, prefix=None):
        """Stored-publisher committed replay, without phase/effect resubmission."""
        from .evolution_run import EvolutionRunAttemptJournal
        from .evolution_composition import NativeEvolutionRunPolicy
        from .evolution_plan import EvolutionAttemptPlan
        from .lifecycle import owned_context, finish
        from ashlar.attempt_store import DeltaAttemptStore
        from ashlar.stored_publisher import StoredPublisherBackend
        self.require(context)
        if (type(journal) is not EvolutionRunAttemptJournal
                or type(journal.ledger.policy) is not NativeEvolutionRunPolicy
                or journal.ledger.policy.driver is not self or journal.ledger.context is not context):
            raise PermissionError('Exact native original run policy/driver required')
        ledger = journal.ledger; policy = ledger.policy
        inventory = ledger._inventory()
        if prefix is not None and inventory != prefix[0]:
            raise PermissionError("Original completed prefix changed")
        _, state, raw, digest, retained = inventory.slots[journal.ordinal]
        if state != 'completed' or digest != expected_sha256:
            raise PermissionError('Exact original completed reservation required')
        plan = EvolutionAttemptPlan(raw); request = plan.document()['request']
        owner = self; primary = None; result = None
        target = self.transport.targets[self.tables['attempts']]
        store = DeltaAttemptStore(AttemptExecutor(self), CarrierPolicy(self, 'attempts'),
            target.table, target.uuid, original_request=request)
        def renew_local():
            if ledger._inventory() != inventory:
                raise PermissionError('Original complete ledger changed during committed replay')
        def renew_authority():
            policy.admit_run(ledger.original, context)
            if policy.authority.admit_ledger(ledger.original, inventory, context) is not None:
                raise PermissionError('Current original ledger authority incomplete')
        class Driver:
            @contextmanager
            def writer(self, stream, supplied):
                owner.require(supplied)
                if supplied is not context or stream != request['stream']:
                    raise PermissionError('Original committed replay context required')
                yield
            def observe_committed_original(self, original_request, artifact, descriptor, supplied):
                if supplied is not context or not original_native_equal(original_request, request):
                    raise PermissionError('Original committed request changed')
                fresh = (prefix[1][journal.ordinal] if prefix is not None else
                    owner._validate_evolution_run_ledger(ledger.original, inventory,
                        context=context, attempt_admission=policy.original_attempt_admission(ledger.original, inventory, context),
                        selected_completed=journal.ordinal))
                if (not original_native_equal(dict(fresh.raw), dict(descriptor.raw))
                        or not original_native_equal(json.loads(retained), dict(fresh.raw))):
                    raise PermissionError('Fresh completed descriptor differs from original custody')
        backend = StoredPublisherBackend(store, Driver(), None)
        try:
            renew_authority(); renew_local()
            with owned_context(backend.writer(request['stream'], context)):
                result = backend.observe_committed_original(request['stream'], request, context=context)
        except BaseException as error:
            primary = error
        # The existing session contexts do not renew on every exceptional path.
        # These independent callbacks run after session cleanup, including cancellation.
        finish(primary, [lambda: store.renew_identity(context), lambda: self.require(context),
            renew_authority, lambda: policy.admit_attempt(ledger.original, journal.ordinal, plan, context),
            renew_local])
        return result

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
        from .lifecycle import owned_context, finish
        if (type(journal) not in (EvolutionPlanJournal, EvolutionRunAttemptJournal) or (original_plan is None) == (expected_sha256 is None)
                or (original_plan is not None and type(original_plan) is not EvolutionAttemptPlan)):
            raise PermissionError('Distinct original fresh or retained resume required')
        owner = self
        selection = None
        selected_digest = original_plan.sha256 if original_plan is not None else expected_sha256
        @contextmanager
        def _held_evolution_context():
            owner.require(context)
            primary = None
            try:
                yield
            except BaseException as error:
                primary = error
            finish(primary, [lambda: owner.require(context)])
        class Driver:
            @contextmanager
            def writer(self, stream, supplied):
                owner.require(supplied)
                with owned_context(_held_evolution_context()):
                    plan = None; primary = None
                    def renew_plan():
                        closing = journal.load(expected_sha256=selected_digest, context=supplied)
                        if plan is not None and closing.raw != plan.raw:
                            raise PermissionError('Closing exact original attempt differs')
                        if selection is not None and (closing.sha256 != selection.plan_sha256
                                or encoded(closing.document()['request']).encode() != selection.request_json):
                            raise PermissionError('Closing original request differs from passive selection')
                    try:
                        plan = original_plan if original_plan is not None else journal.load(
                            expected_sha256=expected_sha256, context=supplied)
                        owner.admit_evolution_plan(plan, context=supplied, fresh=original_plan is not None)
                        if original_plan is not None:
                            journal.retain(plan, context=supplied)
                        if selection is not None:
                            if (plan.sha256 != selection.plan_sha256 or plan.sha256 != selected_digest
                                    or encoded(plan.document()['request']).encode() != selection.request_json):
                                raise PermissionError('Held original request differs from passive selection')
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
                    except BaseException as error:
                        primary = error
                    finish(primary, [renew_plan, lambda: owner.source_admit(request),
                        owner.admission_facts, lambda: owner.require(supplied)])
            def apply(self, request, supplied): return owner.apply(request, supplied)
            def recover_apply(self, request, supplied): return owner.recover_apply(request, supplied)
            def validate(self, request, artifact, descriptor, supplied): return owner.validate(request, artifact, descriptor, supplied)
            def acknowledge(self, request, descriptor, supplied): return owner.acknowledge(request, descriptor, supplied)
        # Passive routing grants no admission; the authoritative load is held.
        if original_plan is None and type(journal) is EvolutionRunAttemptJournal:
            selection = journal.select_original_request(expected_sha256=expected_sha256, context=context)
            request = selection.document()
        else:
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

    def bind_original_medical_publication(self, plan, *, bounds):
        """Bind exact pure correspondence; this port grants no native authority."""
        from .medical_publication_plan import MedicalPublicationPlan
        from .original_medical_publication import MedicalAdmission
        from .evolution_verification_types import VerificationBounds
        from .delta_custody import LocalDeltaTransport, DeltaTarget
        from dataclasses import asdict
        from types import MappingProxyType
        if (type(self) is not NativeDriver or type(plan) is not MedicalPublicationPlan
                or type(bounds) is not VerificationBounds or type(self.transport) is not LocalDeltaTransport
                or type(self.source_admission) is not MedicalAdmission or self.held or self.pin_held
                or getattr(self, 'query_source_factory', None) is not None
                or getattr(self, '_medical_publication_binding', None) is not None
                or type(self.source_sessions) is not RegisteredOutboxSources
                or type(self.ack_sessions) is not RegisteredOutboxAcks
                or getattr(self.transport, '_registered', False) is not True):
            raise PermissionError('Fresh exact registered medical publication binding required')
        bounds.__post_init__()
        value = plan.validated_snapshot(self.source_admission)
        roles = ('object_current', 'edge_current', 'tombstone', 'whole_source_history', 'attempts', 'manifest')
        if (self.tables != value['tables'] or set(self.transport.targets) != set(value['tables'].values())
                or any(type(self.transport.targets[value['tables'][role]]) is not DeltaTarget for role in roles)
                or self.source_sessions.metadata()['registrations'] != self.ack_sessions.metadata()['registrations']
                or len(self.source_sessions.registrations) != 1
                or (self.source_sessions.registrations[0].scope.feed, self.source_sessions.registrations[0].scope.epoch)
                    != ('private-original-medical-fixture', 'original-medical-v1')):
            raise PermissionError('Complete original medical native/source inventory required')
        binding = MappingProxyType(dict(plan=plan, bounds=bounds, view=encoded(value),
            bounds_view=encoded(asdict(bounds)), transport=self.transport, policy=self.policy,
            admission=self.source_admission, source=self.source_sessions, ack=self.ack_sessions,
            source_view=encoded(self.source_sessions.metadata()), ack_view=encoded(self.ack_sessions.metadata()),
            targets=tuple(self.transport.targets[value['tables'][role]] for role in roles),
            target_views=tuple((target.table, str(target.path), target.uuid) for target in
                (self.transport.targets[value['tables'][role]] for role in roles)),
            source_policy=self.source_sessions.policy,
            closing_source=RegisteredOutboxSources(self.source_sessions.registrations, self.source_sessions.policy),
            closing_ack=RegisteredOutboxAcks(self.ack_sessions.registrations),
            source_factories=tuple(item.connection_factory for item in self.source_sessions.registrations),
            ack_factories=tuple(item.connection_factory for item in self.ack_sessions.registrations),
            ack_policies=tuple(item.policy for item in self.ack_sessions.registrations)))
        self._require_medical_publication_binding(binding)
        self._medical_publication_binding = binding
        self._original_medical_publication_binding = binding

    def _require_medical_publication_binding(self, binding, *, request=_MEDICAL_REQUEST_OMITTED):
        from dataclasses import asdict
        if (getattr(self, '_original_medical_publication_binding', binding) is not binding
                or (getattr(self, '_original_medical_publication_binding', None) is not None
                    and getattr(self, '_medical_publication_binding', None) is not binding)):
            raise PermissionError('Original medical construction binding replaced')
        plan = binding['plan']
        value = (plan.validated_snapshot(self.source_admission) if request is _MEDICAL_REQUEST_OMITTED
                 else plan.validated_snapshot(self.source_admission, request=request))
        if (self.transport is not binding['transport'] or self.policy is not binding['policy']
                or self.source_admission is not binding['admission']
                or self.source_sessions is not binding['source'] or self.ack_sessions is not binding['ack']
                or getattr(self, 'query_source_factory', None) is not None
                or encoded(value) != binding['view'] or encoded(asdict(binding['bounds'])) != binding['bounds_view']
                or encoded(self.source_sessions.metadata()) != binding['source_view']
                or encoded(self.ack_sessions.metadata()) != binding['ack_view']
                or self.admission_facts() != value['source_admission'] or self.tables != value['tables']
                or set(self.transport.targets) != {target.table for target in binding['targets']}
                or any(self.transport.targets[target.table] is not target for target in binding['targets'])
                or tuple((target.table, str(target.path), target.uuid) for target in binding['targets']) != binding['target_views']
                or self.source_sessions.policy is not binding['source_policy']
                or any(a is not b for a,b in zip(binding['source_factories'],
                    (item.connection_factory for item in self.source_sessions.registrations)))
                or any(a is not b for a,b in zip(binding['ack_factories'],
                    (item.connection_factory for item in self.ack_sessions.registrations)))
                or any(a is not b for a,b in zip(binding['ack_policies'],
                    (item.policy for item in self.ack_sessions.registrations)))
                or getattr(self.transport, '_registered', False) is not True):
            raise PermissionError('Original medical publication binding changed')
        active = self.policy.active
        correspondences = {'request':'request', 'steps':'selected_steps', 'expected':'expected',
            'publication_id':'publication_id', 'previous_progress':'previous_progress', 'progress':'progress',
            'previous_expected':'previous_expected', 'generated_steps':'generated_steps',
            'elisions':'zero_match_elisions', 'recorded_at':'recorded_at'}
        if (type(active) is not dict or any(not original_native_equal(active.get(key), value[field])
                                          for key, field in correspondences.items())
                or encoded(self.columns) != encoded(value['columns'])):
            raise PermissionError('Original complete medical active plan differs')
        return value

    def _medical_publication_native(self, binding, target, versions):
        transport = binding['transport']
        detail = transport._detail(target, full=True)
        from urllib.parse import urlsplit, unquote
        location = detail.get('location')
        if type(location) is not str:
            raise PermissionError('Original medical native location required')
        uri = urlsplit(location)
        if (uri.scheme != 'file' or uri.netloc or uri.query or uri.fragment
                or Path(unquote(uri.path.rstrip('/'))) != target.path):
            raise PermissionError('Original medical native location changed')
        history = transport.original_history(target)
        if target.table in versions and not any(type(item.get('version')) is int
                and item['version'] == versions[target.table] for item in history):
            raise PermissionError('Selected original medical version not retained')
        return history

    def _medical_publication_ack_scope(self, binding, context):
        from .lifecycle import finish
        item = binding['closing_ack'].registrations[0]
        connection = binding['ack_factories'][0](context); error = None
        policy = binding['ack_policies'][0]
        try: policy.admit_scope(item.source.scope, Session(connection), context)
        except BaseException as caught: error = caught
        finish(error, [lambda: policy.admit_scope(item.source.scope, Session(connection), context),
                       connection.rollback, connection.close])

    def _medical_publication_capture(self, binding, role, versions, remaining, *, previous=False):
        value = json.loads(binding['view']); table = value['tables'][role]
        snapshot, schema = binding['transport'].bounded_snapshot(binding['transport'].targets[table],
            versions[table], remaining_bytes=remaining)
        if (not original_native_equal(schema, json.loads(value['schemas'][role]))
                or not original_native_equal(snapshot['schema'], [[name, family.lower()] for name, family in value['columns'][role]])
                or snapshot['row_sha256'] != sha(encoded(snapshot['rows']))
                or not original_native_equal(snapshot['rows'], sorted(value['previous_expected' if previous else 'expected'][role], key=encoded))):
            raise PermissionError('Complete independent original medical schema/bag differs')
        size = len(encoded({'schema': schema, 'rows': snapshot['rows']}).encode())
        if size > remaining:
            raise PermissionError('Cumulative medical four-bag budget exceeded')
        return snapshot, schema, size

    def _medical_publication_source(self, binding, context):
        from .lifecycle import finish
        source = binding['source']
        def current():
            if (self.source_sessions is not source or encoded(source.metadata()) != binding['source_view']
                    or source.policy is not binding['source_policy']
                    or any(a is not b for a,b in zip(binding['source_factories'],
                        (item.connection_factory for item in source.registrations)))):
                raise PermissionError('Original medical source closing registration changed')
        finish(None, [current, lambda: binding['closing_source'].admit(
            json.loads(binding['view'])['request'], context=context)])

    def _medical_publication_closing_callbacks(self, binding, context, versions):
        total = [0]
        def close_role(role):
            _, _, size = self._medical_publication_capture(binding, role, versions,
                binding['bounds'].total_bytes-total[0])
            total[0] += size
        return [*(lambda role=role: close_role(role) for role in
                    ('object_current', 'edge_current', 'tombstone', 'whole_source_history')),
            lambda: self._require_medical_publication_binding(binding),
            *(lambda target=target: self._medical_publication_native(binding, target, versions)
              for target in binding['targets']),
            lambda: self._medical_publication_source(binding, context),
            lambda: self._medical_publication_ack_scope(binding, context),
            lambda: self.require(context)]

    def observe_original_medical_publication(self, plan, descriptor, *, context, bounds):
        """Bounded independent full generated schemas and immutable native bags."""
        from ashlar.publication import Descriptor
        from .lifecycle import finish, owned_context
        from contextlib import nullcontext
        binding = getattr(self, '_medical_publication_binding', None)
        self.require(context)
        if (binding is None or plan is not binding['plan'] or bounds is not binding['bounds']
                or type(descriptor) is not Descriptor):
            raise PermissionError('Exact bound original medical observation required')
        from types import MappingProxyType
        if (type(descriptor.raw) not in (dict, MappingProxyType) or set(descriptor.raw) != set(FIELDS)
                or any(type(item) is not str or len(item) > bounds.input_bytes
                       or len(item.encode('utf-8')) > bounds.input_bytes for item in descriptor.raw.values())
                or len(encoded(dict(descriptor.raw)).encode()) > bounds.input_bytes
                or len(descriptor.versions) != 4):
            raise PermissionError('Bounded complete original medical descriptor required')
        row = dict(descriptor.raw)
        roles = ('object_current', 'edge_current', 'tombstone', 'whole_source_history')
        value = json.loads(binding['view'])
        if (row != self.policy.active.get('manifest') or descriptor.publication_id != value['publication_id']
                or descriptor.profile != PROFILE or dict(descriptor.versions) != json.loads(row['table_versions_json'])
                or set(descriptor.versions) != {value['tables'][role] for role in roles}
                or any(type(version) is not int or version < 0 for version in descriptor.versions.values())
                or dict(descriptor.revisions) != json.loads(value['request']['schema_revisions_json'])
                or not _medical_descriptor_equal(descriptor.source_progress, json.loads(row['source_progress_json']))
                or not _medical_descriptor_equal(descriptor.validation_report, json.loads(row['validation_report_json']))
                or not original_native_equal(json.loads(row['source_progress_json']), value['progress'])
                or row['schema_revisions_json'] != value['request']['schema_revisions_json']):
            raise PermissionError('Complete original medical descriptor differs')
        vector = manifest_pin_vector(row, {target.table: target.uuid for target in binding['targets']
                                          if target.table in descriptor.versions}, authority='private-local-process')
        observations = {}; schemas = {}; result = None
        transport = binding['transport']
        if self.pin_held and getattr(self, '_held_original_vector', None) != vector:
            raise PermissionError('Exact already-acquired original medical vector required')
        current_bounds = getattr(transport, 'observation_bounds', None)
        if current_bounds is not None and current_bounds is not bounds:
            raise PermissionError('Original medical capture bound interval differs')
        interval = nullcontext() if current_bounds is bounds else transport.bounded_observations(bounds)
        with owned_context(interval):
            pins = nullcontext() if self.pin_held else self.hold(vector, context=context)
            with owned_context(pins):
                error = None; total = 0
                try:
                    self._require_medical_publication_binding(binding)
                    if getattr(self, '_held_original_vector', None) != vector:
                        raise PermissionError('Original medical held vector changed')
                    self.validate_manifest(row, context)
                    for target in binding['targets']:
                        self._medical_publication_native(binding, target, descriptor.versions)
                    self._medical_publication_source(binding, context)
                    self._medical_publication_ack_scope(binding, context)
                    for role in roles:
                        snapshot, schema, size = self._medical_publication_capture(binding, role,
                            descriptor.versions, bounds.total_bytes-total)
                        observations[role] = snapshot; schemas[role] = schema; total += size
                except BaseException as caught: error = caught
                # Fixed four roles are independent closing owners even if opening failed.
                finish(error, self._medical_publication_closing_callbacks(binding, context, descriptor.versions))
                result = {'descriptor':row, 'vector': {table:{'uuid':transport.targets[table].uuid,
                    'version':version} for table,version in descriptor.versions.items()},
                    'bags':observations, 'schemas':schemas}
        return result

    def bind_original_medical_read(self, plan, bounds):
        """Select immutable medical read correspondence, never publication authority."""
        from dataclasses import asdict
        from types import MappingProxyType
        from .read_only_transport import ReadOnlyDeltaTransport
        from .medical_read_sources import MedicalReadSourceFactory
        from .medical_publication_plan import MedicalPublicationPlan
        from .evolution_verification_types import VerificationBounds
        if (type(self)is not NativeDriver or type(self.transport)is not ReadOnlyDeltaTransport
                or type(getattr(self,'query_source_factory',None))is not MedicalReadSourceFactory
                or type(plan)is not MedicalPublicationPlan or type(bounds)is not VerificationBounds
                or type(self.source_sessions)is not RegisteredOutboxSources
                or type(getattr(self,'_original_graph_source_renew',None))is not _OriginalGraphSourceRenewal
                or self._original_graph_source_renew.owner is not self
                or type(getattr(self,'_original_medical_read_closing',None))is not _OriginalGraphSourceRenewal
                or self._original_medical_read_closing.owner is not self
                or type(getattr(self,'_original_medical_read_context',None))is not _OriginalGraphSourceRenewal
                or self._original_medical_read_context.owner is not self
                or self.held or self.pin_held or getattr(self,'_original_medical_read_binding',None)is not None
                or getattr(self,'_original_medical_publication_binding',None)is not None):
            raise PermissionError('Fresh exact medical read binding required')
        bounds.__post_init__();view=plan.validated_snapshot(self.source_admission)
        if (self.tables!=view['tables'] or set(self.transport.targets)!=set(view['tables'].values())
                or any(not original_native_equal(self.policy.active.get(k),view[v])for k,v in [('request','request'),('expected','expected'),('publication_id','publication_id'),('progress','progress'),('previous_progress','previous_progress'),('previous_expected','previous_expected')])
                or encoded(self.columns)!=encoded(view['columns'])):
            raise PermissionError('Complete six-target medical read inventory required')
        self._original_medical_read_binding=MappingProxyType(dict(plan=plan,view=encoded(view),bounds=bounds,
            bounds_view=encoded(asdict(bounds)),transport=self.transport,policy=self.policy,context=self.context,active_view=encoded(self.policy.active),columns_view=encoded(self.columns),
            admission=self.source_admission,factory=self.query_source_factory,source=self.source_sessions,source_policy=self.source_sessions.policy,context_renewal=self._original_medical_read_context,closing=self._original_medical_read_closing,
            manifest=encoded(self.policy.active['manifest']),
            targets=tuple((t.table,str(t.path),t.uuid)for t in self.transport.targets.values())))
        from .medical_read_observation import MedicalReadObservation
        def renew():
            self.require(self.context)
            binding,_=self._require_original_medical_read_binding()
            binding['context_renewal'].renew(None,self.context)
        self._original_medical_read_observation=MedicalReadObservation(plan,self.transport,bounds,self.query_source_factory,
            self.source_sessions.policy,renew,self.transport._detail)
        self._original_medical_read_binding=MappingProxyType(dict(self._original_medical_read_binding,observation=self._original_medical_read_observation))

    def _require_original_medical_read_binding(self):
        from dataclasses import asdict
        binding=self._original_medical_read_binding
        view=binding['plan'].validated_snapshot(self.source_admission)
        if (self.transport is not binding['transport'] or self.policy is not binding['policy']
                or self.source_admission is not binding['admission'] or self.query_source_factory is not binding['factory']
                or self.source_sessions is not binding['source'] or self.source_sessions.policy is not binding['source_policy']
                or getattr(self,'_original_medical_read_context',None)is not binding['context_renewal']
                or getattr(self,'_original_medical_read_closing',None)is not binding['closing']
                or encoded(view)!=binding['view'] or encoded(asdict(binding['bounds']))!=binding['bounds_view']
                or self.context is not binding['context']or encoded(self.policy.active)!=binding['active_view']or encoded(self.columns)!=binding['columns_view']
                or encoded(self.policy.active['manifest'])!=binding['manifest'] or self.tables!=view['tables']
                or tuple((t.table,str(t.path),t.uuid)for t in self.transport.targets.values())!=binding['targets']):
            raise PermissionError('Original medical read binding changed')
        binding['factory'].require_admission(self.source_admission)
        if hasattr(self,'_original_medical_read_observation'):
            from .medical_read_observation import MedicalReadObservation
            if type(self._original_medical_read_observation)is not MedicalReadObservation or self._original_medical_read_observation is not binding.get('observation'):raise PermissionError('Original typed medical read observer replaced')
            self._original_medical_read_observation.require_selection()
            if self._original_medical_read_observation.plan is not binding['plan']or self._original_medical_read_observation.transport is not binding['transport']:raise PermissionError('Original typed medical read observer changed')
        return binding,view

    def _capture_original_medical_read(self,row,context):
        binding,_=self._require_original_medical_read_binding();self.require(context)
        if encoded(row)!=binding['manifest']:raise PermissionError('Original immutable medical read manifest differs')
        return self._original_medical_read_observation.capture(row,context)

    def observe_original_medical_read(self,descriptor,*,context):
        from ashlar.publication import Descriptor
        if type(descriptor)is not Descriptor:raise ValueError('Exact resolved medical descriptor required')
        binding,view=self._require_original_medical_read_binding();self.require(context)
        if not self.pin_held:raise PermissionError('Actual held medical read pins required')
        native=self._capture_original_medical_read(dict(descriptor.raw),context)
        ack=self._original_medical_read_observation.ack(encoded(view['request']).encode(),encoded(dict(descriptor.raw)).encode(),context)
        return {'native':native,'protected_ack':ack,'qualification':'Existing local medical publication read only; no ACK advancement or source/native writes'}

    def bind_query_sources(self, factory):
        """Bind one original read-only source before entering its held interval.

        This bridge proves quiescent read custody, not permission to publish,
        mutate native data, submit ACK, or provision PostgreSQL resources.
        """
        from .commerce_query_sources import CommerceQuerySourceFactory
        from .medical_read_sources import MedicalReadSourceFactory
        from .read_only_transport import ReadOnlyDeltaTransport
        from .existing_outbox_authority import ExistingOutboxAuthority
        from .delta_custody import DeltaTarget
        from types import MappingProxyType
        if (type(self) is not NativeDriver or type(self.transport) is not ReadOnlyDeltaTransport
                or type(factory) not in (CommerceQuerySourceFactory,MedicalReadSourceFactory) or self.held or self.pin_held
                or self.source_sessions is not None or getattr(self,'query_source_factory',None) is not None):
            raise PermissionError('Fresh exact original reader/source binding required')
        factory.__post_init__()
        medical=type(factory)is MedicalReadSourceFactory
        if medical:factory.require_admission(self.source_admission)
        expected_profile=('ashlar-original-medical-public-dataset-admission/0.1'if medical else'ashlar-original-commerce-public-dataset-admission/0.1')
        if self.admission_facts()['profile']!=expected_profile:raise PermissionError('Original selected read admission required')
        original_medical_admission=self.source_admission if medical else None
        selected=factory.registration
        original_connections=(selected.source.connection_factory,selected.metadata_factory)
        active=self.policy.active
        request=json.loads(encoded(active['request']))
        checkpoint=json.loads(request['source_checkpoint_json'])
        port=self.scope_ports.get(checkpoint['feed'])
        if (type(port) is not dict or selected.source.scope != port['scope']
                or selected.source.source_schema != port['source']
                or selected.source.source_signature_sha256 != port['signature']
                or selected.source_role != port['reader'] or selected.ack_role != port['role']
                or (selected.source.scope.feed,selected.source.scope.epoch)!=(checkpoint['feed'],checkpoint['epoch'])):
            raise PermissionError('Original reader source registration differs')
        original_factory=encoded(factory.metadata());original_manifest=encoded(active['manifest'])
        original_request=encoded(request)
        versions=json.loads(active['manifest']['table_versions_json'])
        def targets_snapshot(transport):
            targets=transport.targets
            if (type(targets) is not MappingProxyType or any(type(k) is not str or type(v) is not DeltaTarget
                    or type(v.uuid) is not str or type(v.table) is not str or v.table!=k for k,v in targets.items())):
                raise PermissionError('Exact original native target inventory required')
            return tuple((table,target.uuid,str(target.path)) for table,target in sorted(targets.items()))
        original_targets=targets_snapshot(self.transport)
        graph_roles=('object_current','edge_current','tombstone','whole_source_history')
        installation_roles=graph_roles+('attempts','manifest')
        table_mapping=getattr(self,'tables',None)
        original_role_tables=(tuple(sorted(table_mapping.items()))
            if type(table_mapping) in (dict,MappingProxyType)
            and all(type(k)is str and type(v)is str for k,v in table_mapping.items()) else None)
        original_versions=tuple(sorted(versions.items()))
        if any(type(version)is not int or version<0 for _,version in original_versions):raise PermissionError('Exact original native versions required')
        original_reservation=self.transport.reservation
        original_registry=encoded(json.loads(original_reservation)['registry'])
        original_journal=self.transport.journal_path
        journal_stat=original_journal.stat();journal_identity=(journal_stat.st_dev,journal_stat.st_ino)
        if self.transport.db.execute('SELECT original FROM local_installation WHERE id=1').fetchall()!=[(original_registry,)]:
            raise PermissionError('Original reader installation registry differs')
        owner=self
        original_transport=owner.transport;original_context=owner.context
        original_sources=None
        class ReadIdentity:
            def admit_source_writer(self, registration, context):
                owner.require(context)
                if medical:
                    if owner.source_admission is not original_medical_admission:raise PermissionError('Original actual medical admission changed')
                    factory.require_admission(owner.source_admission)
                current=factory.registration
                if any(a is not b for a,b in zip(original_connections,(current.source.connection_factory,current.metadata_factory))):raise PermissionError('Original reader/ACK connection owner changed')
                gate=getattr(owner,'_query_read_gate',None)
                if (type(owner.transport) is not ReadOnlyDeltaTransport or not owner.pin_held
                        or gate is None or owner.query_source_factory is not factory or encoded(factory.metadata())!=original_factory
                        or registration.metadata()!=selected.source.metadata()
                        or encoded(owner.policy.active['request'])!=original_request
                        or encoded(owner.policy.active['manifest'])!=original_manifest):
                    raise PermissionError('Current complete quiescent reader custody required')
                fd,identity,path=gate
                current=path.lstat();held=os.fstat(fd)
                if path.is_symlink() or (current.st_dev,current.st_ino)!=identity or (held.st_dev,held.st_ino)!=identity:
                    raise PermissionError('Original exclusive reader gate changed')
                current_targets=targets_snapshot(owner.transport)
                journal=owner.transport.journal_path
                current_journal=journal.lstat()
                if (not original_native_equal(current_targets,original_targets)
                        or journal!=original_journal or journal.is_symlink()
                        or (current_journal.st_dev,current_journal.st_ino)!=journal_identity
                        or owner.transport.reservation!=original_reservation
                        or owner.transport.db.execute('SELECT original FROM local_installation WHERE id=1').fetchall()!=[(original_registry,)]):
                    raise PermissionError('Original reader registry/reservation changed')
        read_identity=ReadIdentity()
        if medical:
            self._original_medical_read_context=_OriginalGraphSourceRenewal(self,lambda vector,context:read_identity.admit_source_writer(selected.source,context))
        class GraphIdentity:
            def __init__(self):self.interval=None;self.native=None
            def files(self):
                from .evolution_producer import snapshot
                result={};total=0
                for table,uuid,path in original_targets:
                    target=Path(path)
                    for member in sorted(target.rglob('*')):
                        if member.is_symlink():raise PermissionError('Original graph native alias refused')
                        if member.is_file():
                            raw=snapshot(member,67108864);total+=len(raw)
                            if total>1073741824:raise PermissionError('Bounded original graph native custody required')
                            result[str(member)]=hashlib.sha256(raw).hexdigest()
                        elif not member.is_dir():raise PermissionError('Original graph native carrier refused')
                return result
            def require(self, registration, context):
                read_identity.admit_source_writer(registration,context)
                if (owner.transport is not original_transport or owner.query_source_factory is not factory
                        or owner.source_sessions is not original_sources or owner.context is not original_context):
                    raise PermissionError('Original actual graph reader ports changed')
                token=getattr(owner,'_query_read_generation',None)
                if token is None:raise PermissionError('Actual acquired reader interval required')
                if self.interval is not token:
                    self.interval=token;self.native=self.files()
                if self.files()!=self.native:raise PermissionError('Original complete graph native bytes changed')
            def admit_source_writer(self, registration, context):
                self.require(registration,context)
            def observe_target(self, table, uuid, path):
                result=original_transport.query('DESCRIBE DETAIL `'+ '`.`'.join(table.split('.'))+'`',{})
                if (type(result.rows) is not list or len(result.rows)!=1
                        or type(result.rows[0]) is not dict
                        or type(result.rows[0].get('id')) is not str
                        or type(result.rows[0].get('location')) is not str
                        or result.rows[0]['id']!=uuid
                        or result.rows[0]['location'].removeprefix('file:').rstrip('/')!=path):
                    raise PermissionError('Current original complete graph native identity differs')
                if not Path(path).is_dir() or not (Path(path)/'_delta_log').is_dir():
                    raise PermissionError('Original retained graph version files absent')
            def observe(self, registration, context):
                self.require(registration,context)
                for table,uuid,path in original_targets:
                    self.observe_target(table,uuid,path)
        identity=GraphIdentity()
        class ReadCustody:
            def admit_source_writer(self, registration, context):
                read_identity.admit_source_writer(registration,context)
                for table,version in original_versions:
                    expected_uuid=dict((t,u) for t,u,p in original_targets)[table]
                    observed=owner.inspect_snapshot(table,version)
                    if (type(observed) is not Snapshot or type(observed.table) is not str
                            or type(observed.uuid) is not str or type(observed.version) is not int
                            or observed.table!=table or observed.uuid!=expected_uuid or observed.version!=version):
                        raise PermissionError('Original held native snapshot differs')
        graph_sources=RegisteredOutboxSources((selected.source,),ExistingOutboxAuthority((selected,),identity))
        def current_graph_source(vector, context):
            from ashlar.pins import PinVector
            current_tables=getattr(owner,'tables',None)
            if (type(vector) is not PinVector or original_role_tables is None
                    or type(current_tables) not in (dict,MappingProxyType)
                    or any(type(k)is not str or type(v)is not str for k,v in current_tables.items())
                    or tuple(sorted(current_tables.items()))!=original_role_tables):
                raise PermissionError('Exact complete original graph vector required')
            role_tables=dict(original_role_tables)
            if (set(role_tables)!=set(installation_roles)
                    or len(set(role_tables.values()))!=6
                    or {t for t,u,path in original_targets}!=set(role_tables.values())
                    or {t for t,v in original_versions}!={role_tables[r]for r in graph_roles}):
                raise PermissionError('Original declared graph data vector and installation roles differ')
            original_uuids={t:u for t,u,path in original_targets}
            expected=manifest_pin_vector(json.loads(original_manifest),{t:original_uuids[t]for t,v in original_versions},authority='private-local-process')
            if vector!=expected:raise PermissionError('Original graph vector differs')
            identity.observe(selected.source,context)
            if graph_sources.metadata()!=original_sources.metadata():raise PermissionError('Original graph source registration differs')
            graph_sources.admit(json.loads(original_request),context=context)
            identity.observe(selected.source,context)
        self._original_graph_source_renew=_OriginalGraphSourceRenewal(self,current_graph_source)
        class MedicalReadIdentity:
            def admit_source_writer(self, registration, context):
                from .medical_read_observation import MedicalReadObservation
                read_identity.admit_source_writer(registration,context)
                if type(getattr(owner,'_original_medical_read_binding',None))is not MappingProxyType:
                    raise PermissionError('Complete actual medical read binding required')
                binding,_=owner._require_original_medical_read_binding()
                observation=getattr(owner,'_original_medical_read_observation',None)
                closing=getattr(owner,'_original_medical_read_closing',None)
                if (type(observation)is not MedicalReadObservation or observation is not binding.get('observation')
                        or type(closing)is not _OriginalGraphSourceRenewal or closing.owner is not owner
                        or closing is not binding['closing']):
                    raise PermissionError('Actual complete medical read/closing owners required')
                row=json.loads(original_manifest)
                vector=manifest_pin_vector(row,{t:u for t,u,p in original_targets if t in dict(original_versions)},
                    authority='private-local-process')
                from ashlar.pins import PinVector
                held_vector=getattr(owner,'_held_original_vector',None)
                if type(held_vector)is not PinVector or held_vector!=vector:
                    raise PermissionError('Exact acquired original medical native vector required')
                # Source-role identity only. _hold validates full native bags before
                # yielding and independently at closing; no native result grant here.
        authority=ExistingOutboxAuthority((selected,),MedicalReadIdentity()if medical else ReadCustody())
        sources=RegisteredOutboxSources((selected.source,),authority)
        self.query_source_factory=factory
        self.source_sessions=sources;self.original_source_sessions=encoded(sources.metadata())
        original_sources=sources
        if medical:
            def close_snapshot(table,version):
                observed=owner.inspect_snapshot(table,version)
                expected_uuid={t:u for t,u,p in original_targets}[table]
                if type(observed)is not Snapshot or observed.table!=table or observed.uuid!=expected_uuid or type(observed.version)is not int or observed.version!=version:raise PermissionError('Closing original medical snapshot differs')
            def close_native(table,uuid,path,context):
                owner.require(context)
                identity.observe_target(table,uuid,path)
            def close_ack(context):
                from .lifecycle import finish
                connection=original_connections[1](context)
                primary=None
                try:
                    if connection.autocommit is not False:raise PermissionError('Original ordinary ACK closing transaction required')
                    graph_sources.policy.admit_scope(selected.source.scope,Session(connection),context)
                except BaseException as error:primary=error
                finish(primary,[lambda:connection.rollback(),lambda:connection.close()])
            def closing(vector,context):
                from .lifecycle import finish
                finish(None,[lambda:factory.require_admission(owner.source_admission)]
                    +[lambda t=t,v=v:close_snapshot(t,v)for t,v in original_versions]
                    +[lambda t=t,u=u,p=p:close_native(t,u,p,context)for t,u,p in original_targets]
                    +[lambda:identity.require(selected.source,context),
                      lambda:graph_sources.admit(json.loads(original_request),context=context),
                      lambda:close_ack(context)])
            self._original_medical_read_closing=_OriginalGraphSourceRenewal(self,closing)

    def renew_original_graph_read_source(self, vector, *, context):
        """Renew original ordinary source under the actual complete read reservation.

        This closed graph port grants no write authority and never changes the
        generic source_sessions full-snapshot admission path. Medical source-role
        identity requires its complete nominal read binding; its owning hold still
        performs full native snapshot admission before yielding and at closing.
        The graph interval must corroborate every original native byte and perform
        actual full snapshot admission at both boundaries.
        """
        from .read_only_transport import ReadOnlyDeltaTransport
        from .commerce_query_sources import CommerceQuerySourceFactory
        from .medical_read_sources import MedicalReadSourceFactory
        if (type(self) is not NativeDriver or type(self.transport) is not ReadOnlyDeltaTransport
                or type(getattr(self,'query_source_factory',None)) not in (CommerceQuerySourceFactory,MedicalReadSourceFactory)
                or context is not self.context or not self.held or not self.pin_held
                or type(getattr(self,'_original_graph_source_renew',None)) is not _OriginalGraphSourceRenewal
                or self._original_graph_source_renew.owner is not self):
            raise PermissionError('Actual original registered graph read interval required')
        self._original_graph_source_renew.renew(vector,context)

    @contextmanager
    def query_read_interval(self, context):
        from .lifecycle import finish
        from .read_only_transport import ReadOnlyDeltaTransport
        from .commerce_query_sources import CommerceQuerySourceFactory
        from .medical_read_sources import MedicalReadSourceFactory
        if (type(self) is not NativeDriver or type(self.transport) is not ReadOnlyDeltaTransport
                or type(getattr(self,'query_source_factory',None)) not in (CommerceQuerySourceFactory,MedicalReadSourceFactory)
                or context is not self.context or self.held):
            raise PermissionError('Original nonnested reader context required')
        medical_factory=self.query_source_factory if type(self.query_source_factory)is MedicalReadSourceFactory else None
        path=self.transport.journal_path.parent/'.pipeline-writer.lock'
        fd=None;primary=None;locked=False
        try:
            fd=os.open(path,os.O_RDWR|os.O_NOFOLLOW)
            info=os.fstat(fd)
            if (not path.is_file() or path.is_symlink() or info.st_uid!=os.getuid()
                    or (path.stat().st_dev,path.stat().st_ino)!=(info.st_dev,info.st_ino)):
                raise PermissionError('Original private reader gate required')
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);locked=True
            self._query_read_gate=(fd,(info.st_dev,info.st_ino),path);self._query_read_generation=object();self.held=True
            yield
        except BaseException as error:primary=error
        finally:
            self.held=False;self._query_read_gate=None;self._query_read_generation=None
            finish(primary,([lambda:medical_factory.require_admission(self.source_admission)]if medical_factory is not None else [])+([lambda:fcntl.flock(fd,fcntl.LOCK_UN)] if locked else [])+([lambda:os.close(fd)]if fd is not None else []))

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
        if getattr(self,'query_source_factory',None) is not None:
            with self.query_read_interval(context):yield
            return
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
        return graph_sql_plan(prior, batch, tables, materialized_at=materialized_at, schema_policy=self.schema_admit, schema_transition_policy=self.schema_transition_admit,lookup_hash_profile=self.lookup_hash_profile)

    def _selected_publication_profile(self):
        """Bind manifest and receipt validation to this immutable carrier choice."""
        installed=getattr(self.transport,'installation_profile',None)
        if self.lookup_hash_profile=='spark-xxhash64-typed-key/0.1':
            if (type(installed)is not dict
                    or installed.get('physical_profile')!='ashlar-delta/0.4-xxhash64'
                    or installed.get('lookup_hash_profile')!=self.lookup_hash_profile):
                raise PermissionError('Exact installed local `.4` publication profile required')
            return installed['physical_profile']
        if self.lookup_hash_profile!='sha256-json-v1' or installed is not None:
            raise PermissionError('Legacy local `.3` publication profile required')
        return PROFILE

    def source_admit(self, request):
        self.admission_facts()
        binding = getattr(self, '_original_medical_publication_binding', None)
        if binding is not None:
            self.require(self.context)
            if not original_native_equal(request, json.loads(binding['view'])['request']):
                raise PermissionError('Original bound medical source request differs')
            self._medical_publication_source(binding, self.context)
            self.admission_facts()
            return
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

    def _medical_source_plan_reference(self, request, original):
        return {'profile': 'ashlar-original-medical-source-plan-reference/0.1',
            'request_digest': request['request_digest'], 'bytes': len(original.encode('utf-8')),
            'sha256': sha(original)}

    def _require_medical_artifact_budget(self, binding, request, artifact):
        """Bound the original complete phase cell before its first submission."""
        maximum = binding['bounds'].cell_bytes
        _medical_json_budget(artifact, maximum)
        text = encoded(artifact); row = artifact['manifest']; descriptor = encoded(row)
        for phase in ('applied', 'committing', 'committed'):
            body = {'request': request, 'result_json': text,
                'descriptor_json': descriptor if phase == 'committed' else None}
            _medical_json_budget(body, maximum, ascii_only=True)
            payload = json.dumps(body, sort_keys=True, separators=(',', ':'))
            # Same string/key/null JSON measurement family as the native carrier guard.
            # Actual Spark pretransfer measurement still independently applies on readback.
            _medical_json_budget({'payload_json': payload}, maximum)

    def _validate_medical_effect_proof(self, request, artifact, context):
        """Resolve retained original medical proofs; hashes never replace native admission."""
        from .lifecycle import finish
        binding = getattr(self, '_original_medical_publication_binding', None)
        self.require(context)
        if binding is None:
            raise PermissionError('Original bound medical effect proof required')
        # Keep original owner/plan/roles for independent closing even on current drift.
        transport = binding['transport']; value = json.loads(binding['view'])
        digest = value['request']['request_digest']; operation = 'effects:' + digest
        steps = value['selected_steps']; effects = LocalDeltaEffects(transport)
        original = None; effect_original = None; identities = (); proof = None
        versions = {}; prior = {}; primary = None
        if type(artifact) is dict and type(artifact.get('manifest')) is dict:
            try:
                candidate = json.loads(artifact['manifest']['table_versions_json'])
                if type(candidate) is dict and all(type(k) is str and type(v) is int and v >= 0
                        for k, v in candidate.items()):
                    versions = candidate
            except (KeyError, ValueError, TypeError):
                pass

        def source_row():
            capacity = operation_capacity(transport.operation_capacity)
            if capacity is None:raise PermissionError('Explicit original medical operation capacity required')
            maximum = capacity['max_intent_bytes']
            rows = transport.db.execute('SELECT length(CAST(original AS BLOB)),substr(CAST(original AS BLOB),1,?) FROM local_source_plan WHERE request_digest=?',
                (maximum+1, digest)).fetchall()
            if (len(rows) != 1 or type(rows[0][0]) is not int or not 0 < rows[0][0] <= maximum
                    or type(rows[0][1]) is not bytes or len(rows[0][1]) != rows[0][0]):
                raise PermissionError('Complete bounded original medical source plan missing')
            return rows[0][1].decode('utf-8')

        def require_source():
            nonlocal prior
            current = source_row()
            if original is not None and current != original:
                raise PermissionError('Original medical source plan closing differs')
            expected = {'request': value['request'], 'generated_steps': value['generated_steps'],
                'selected_steps': steps, 'zero_match_elisions': value['zero_match_elisions'],
                'complete_prior_oracle': value['previous_expected']}
            retained = json.loads(current)
            if encoded(retained) != current:
                raise PermissionError('Original canonical medical source plan required')
            if (type(retained) is not dict or set(retained) != set(expected) | {'observed_native_prior'}
                    or not original_native_equal({k: retained[k] for k in expected}, expected)):
                raise PermissionError('Complete original medical source plan differs')
            anchors = retained['observed_native_prior']
            tables = {value['tables'][r] for r in value['previous_expected']}
            if type(anchors) is not dict or set(anchors) != tables:
                raise PermissionError('Complete original medical prior anchors missing')
            for table, anchor in anchors.items():
                if (type(anchor) is not dict or set(anchor) != {'uuid', 'version', 'snapshot'}
                        or anchor['uuid'] != transport.targets[table].uuid
                        or type(anchor['version']) is not int or anchor['version'] < 0):
                    raise PermissionError('Original medical prior anchor differs')
            if not prior:
                prior = json.loads(encoded(anchors))
            if proof is not None and (not original_native_equal(proof['source_plan_reference'],
                    self._medical_source_plan_reference(value['request'], current))
                    or proof['original_source_plan_sha256'] != sha(current)):
                raise PermissionError('Original medical source reference differs')
            return current, anchors

        def require_transport_policy():
            if transport.policy is not binding['policy']:
                raise PermissionError('Original medical effect transport policy changed')

        def require_effects():
            nonlocal effect_original, identities
            require_transport_policy()
            current, current_sha, ids = effects.original_plan(operation, digest, steps)
            if effect_original is not None and (current != effect_original or ids != list(identities)):
                raise PermissionError('Original medical ordered effect plan changed')
            if effect_original is None:
                effect_original = current; identities = tuple(ids)
            observation = effects.observe(operation, digest, steps, context=context)
            if (observation['plan_sha256'] != current_sha or observation['intent_digest'] != digest
                    or observation['operation'] != operation or observation['operation_ids'] != ids
                    or observation['states'] != ['committed'] * len(ids)
                    or proof is not None and (proof['plan_sha256'] != current_sha or proof['intent_digest'] != digest)):
                raise PermissionError('Complete committed original medical ordinals required')
            return current, ids

        def inspect(index, step):
            require_transport_policy()
            if len(identities) != len(steps):
                raise PermissionError('Original medical ordinal identity unavailable')
            key = identities[index]
            native = transport.inspect_committed(key, step['statement'], step['parameters'],
                intent_digest=digest, context=context)
            compact = [{'version': str(native['version']), 'uuid': native['uuid'],
                'operation_sha256': native['operation_sha256'],
                'snapshot_sha256': native['snapshot']['row_sha256']}]
            if proof is not None and not original_native_equal(proof['original_native_receipts'][index], compact):
                raise PermissionError('Original medical compact native receipt differs')

        def priors():
            total = [0]
            def capture(role):
                table = value['tables'][role]; anchor = prior.get(table)
                if anchor is None:
                    raise PermissionError('Original medical prior native anchor unavailable')
                snapshot, _, size = self._medical_publication_capture(binding, role,
                    {table: anchor['version']}, binding['bounds'].total_bytes - total[0], previous=True)
                total[0] += size
                if not original_native_equal(snapshot, anchor['snapshot']):
                    raise PermissionError('Complete original medical prior native snapshot differs')
            finish(None, [lambda role=role: capture(role) for role in value['previous_expected']])

        try:
            self._require_medical_publication_binding(binding)
            if (getattr(transport, 'observation_bounds', None) is not binding['bounds']
                    or not original_native_equal(request, value['request'])
                    or type(artifact) is not dict or set(artifact) != {'effects', 'manifest'}):
                raise PermissionError('Exact bounded original medical proof interval required')
            self._require_medical_artifact_budget(binding, request, artifact)
            proof = artifact['effects']
            if (type(proof) is not dict or set(proof) != {'profile', 'plan_sha256', 'intent_digest',
                    'original_native_receipts', 'source_plan_reference', 'zero_match_elisions', 'original_source_plan_sha256'}
                    or proof['profile'] != 'ashlar-original-medical-effect-proof/0.1'
                    or not original_native_equal(proof['zero_match_elisions'], value['zero_match_elisions'])
                    or type(proof['original_native_receipts']) is not list
                    or len(proof['original_native_receipts']) != len(steps)):
                raise PermissionError('Closed original medical effect proof required')
            original, prior = require_source()
            effect_original, ids = require_effects(); identities = tuple(ids)
            priors()
            finish(None, [lambda i=i, step=step: inspect(i, step)
                for i, step in enumerate(steps)])
        except BaseException as error:
            primary = error
        # Fixed actual acquired owners all close, even if the first ordinal/current check failed.
        finish(primary, [require_source, require_effects, priors] +
            [lambda i=i, step=step: inspect(i, step)
                for i, step in enumerate(steps)] +
            self._medical_publication_closing_callbacks(binding, context, versions))

    def preflight_original_medical_recovery(self, plan, *, context, bounds, stages=None):
        """Validate all original partial custody before any remaining effect action."""
        from .medical_publication_plan import MedicalPublicationPlan
        from .evolution_verification_types import VerificationBounds
        from .lifecycle import finish
        from .medical_preflight_timing import MedicalPreflightTiming
        if stages is not None and type(stages)is not MedicalPreflightTiming:raise ValueError('Exact preflight timing owner required')
        def timed(stage,action,**labels):return action()if stages is None else stages.call(stage,action,**labels)
        self.require(context)
        binding = getattr(self, '_original_medical_publication_binding', None)
        if (type(plan) is not MedicalPublicationPlan or type(bounds) is not VerificationBounds
                or binding is None or plan is not binding['plan'] or bounds is not binding['bounds']
                or self.transport.observation_bounds is not bounds):
            raise PermissionError('Exact held bound medical recovery required')
        transport = binding['transport']; value = json.loads(binding['view'])
        request = value['request']; digest = request['request_digest']; operation = 'effects:' + digest
        steps = value['selected_steps']; effects = LocalDeltaEffects(transport)
        identities = (); states = (); versions = {}; captures = {}; primary = None
        source_original = None; whole_original = None
        from .local_physical_carriers import original_local_physical_carriers
        schemas = original_local_physical_carriers()['carriers']
        def guard():
            self.require(context); self._require_medical_publication_binding(binding, request=request)
        def source_plan():
            nonlocal source_original
            guard(); current = self._apply_source_plan(request, context, True, stages=stages)
            if source_original is None:source_original = current
            elif current != source_original:raise PermissionError('Original recovery source plan changed during preflight')
        def ordinal(index):
            guard()
            if index < len(states) and states[index] == 'committed':
                step = steps[index]
                transport.inspect_committed(identities[index], step['statement'], step['parameters'],
                    intent_digest=digest, context=context)
        def native(target, total, closing):
            # Select from the same fresh post-identity history used for custody.
            # Opening and closing each observe independently; no interval cache.
            history = self._medical_publication_native(binding, target, {})
            if not history or type(history[0].get('version')) is not int:
                raise PermissionError('Complete partial medical native history required')
            absent = {identities[index] for index, state in enumerate(states) if state == 'no-operation-record'}
            for record in history:
                text = record.get('userMetadata')
                if text is None:continue
                if type(text) is not str:raise PermissionError('Exact native recovery metadata carrier required')
                try:metadata = json.loads(text)
                except (ValueError,TypeError):continue
                if (type(metadata) is dict and metadata.get('profile') == 'ashlar-local-delta-commit/0.1'
                        and metadata.get('operation') in absent and metadata.get('request_digest') == digest):
                    raise PermissionError('Original native commit missing ordinal custody; no restoration')
            version = history[0]['version']
            snapshot, schema = transport.bounded_snapshot(target, version, remaining_bytes=bounds.total_bytes-total[0])
            role = target.table.split('.')[-1]
            if not original_native_equal(schema, json.loads(schemas[role]['schemaJson'])):
                raise PermissionError('Complete original partial medical schema differs')
            size = len(encoded({'snapshot':snapshot,'schema':schema}).encode()); total[0] += size
            if total[0] > bounds.total_bytes:raise PermissionError('Complete partial medical native capture exceeds budget')
            observed = encoded({'version':version,'snapshot':snapshot,'schema':schema})
            if closing and target.table in captures and captures[target.table] != observed:
                raise PermissionError('Partial medical native state changed during preflight')
            if not closing:captures[target.table] = observed; versions[target.table] = version
        def whole():
            nonlocal whole_original
            guard(); current = effects.observe(operation, digest, steps, context=context)
            if whole_original is None:whole_original = encoded(current)
            elif encoded(current) != whole_original:raise PermissionError('Original recovery ordinal state changed during preflight')
            return current
        try:
            guard(); timed('source-admission',lambda:self.source_admit(request)); timed('source-plan',source_plan)
            _, _, identities = effects.original_plan(operation, digest, steps)
            observed = timed('ordinal-state',whole); states = tuple(observed['states'])
            if len(states) != len(steps):raise PermissionError('Complete original recovery ordinal inventory required')
            pending_seen = False
            for state in states:
                if state == 'committed' and pending_seen:raise PermissionError('Original recovery committed prefix has a gap')
                if state != 'committed':pending_seen = True
            # Missing states remain native-absence concerns. Existing committed
            # receipts are all independently checked before shared recovery acts.
            total = [0]
            finish(None, [lambda target=target:timed('native-target',lambda:native(target,total,False),role=target.table.split('.')[-1]) for target in binding['targets']])
            finish(None, [lambda index=index:timed('committed-ordinal',lambda:ordinal(index),ordinal=index) for index in range(len(steps))])
        except BaseException as error:primary = error
        total = [0]
        finish(primary, [lambda:timed('source-plan',source_plan), lambda:timed('ordinal-state',whole), *(lambda index=index:timed('committed-ordinal',lambda:ordinal(index),ordinal=index) for index in range(len(steps))),
            *(lambda target=target:timed('native-target',lambda:native(target,total,True),role=target.table.split('.')[-1]) for target in binding['targets']),
            lambda:timed('source-closing',lambda:self._medical_publication_source(binding,context)),
            lambda:timed('ack-scope',lambda:self._medical_publication_ack_scope(binding,context)), lambda:timed('binding',guard)])
        return None

    def _apply_source_plan(self, request, context, recovery, *, stages=None):
        active = self.policy.active
        medical_binding = getattr(self, '_original_medical_publication_binding', None)
        source_plan = {'request': request, 'generated_steps': active['generated_steps'], 'selected_steps': active['steps'], 'zero_match_elisions': active['elisions'], 'complete_prior_oracle': active['previous_expected']}
        if medical_binding is None:
            retained_plan = self.transport.db.execute('SELECT original FROM local_source_plan WHERE request_digest=?', (request['request_digest'],)).fetchone()
        else:
            capacity = operation_capacity(self.transport.operation_capacity)
            if capacity is None:raise PermissionError('Explicit original medical operation capacity required')
            maximum = capacity['max_intent_bytes']
            row = self.transport.db.execute('SELECT length(CAST(original AS BLOB)),substr(CAST(original AS BLOB),1,?) FROM local_source_plan WHERE request_digest=?', (maximum+1, request['request_digest'])).fetchone()
            if row is not None and (type(row[0]) is not int or not 0 < row[0] <= maximum or type(row[1]) is not bytes or len(row[1]) != row[0]):
                raise LocalDeltaError('Complete bounded original source plan required')
            retained_plan = None if row is None else (row[1].decode('utf-8'),)
        truss_intent = None
        if self.admission_facts()['profile'] in (TRUSS_SCALAR_GRAPH_PROFILE,TRUSS_TYPED_GRAPH_PROFILE,TRUSS_CURRENT_GRAPH_PROFILE,TRUSS_CURRENT_045_GRAPH_PROFILE,TRUSS_CURRENT_048_GRAPH_PROFILE,TRUSS_CURRENT_049_GRAPH_PROFILE,TRUSS_CURRENT_050_GRAPH_PROFILE,TRUSS_EMPTY_INTERVAL_PROFILE):
            truss_intent = {'profile':'ashlar-truss-publication-intent/0.1',
                'publication_id':active['publication_id'], 'recorded_at':active['recorded_at'],
                'previous_progress':active['previous_progress'], 'progress':active['progress'],
                'source_admission':self.admission_facts(), 'source_oracle_sha256':sha(encoded(active['expected']))}
            # Older applied-only plans retain their original six-field bytes.
            if retained_plan is None or 'publication_intent' in json.loads(retained_plan[0]):
                source_plan['publication_intent'] = truss_intent
        if retained_plan is None:
            if recovery:
                raise LocalDeltaError('Original source plan custody absent; no replacement')
            anchors = {}
            prior_total = 0
            for (role, rows) in active['previous_expected'].items():
                target = self.transport.targets[self.tables[role]]
                version = int(self.transport.original_history(target)[0]['version'])
                if medical_binding is None:
                    snapshot = self.transport._snapshot(target, version)
                else:
                    snapshot, _, size = self._medical_publication_capture(medical_binding, role,
                        {target.table: version}, medical_binding['bounds'].total_bytes-prior_total, previous=True)
                    prior_total += size
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
            if medical_binding is None:
                retained = json.loads(original_plan)
            else:
                def pairs(items):
                    result = {}
                    for key, item in items:
                        if key in result:raise LocalDeltaError('Duplicate original medical source plan member')
                        result[key] = item
                    return result
                def nonfinite(value):raise LocalDeltaError('Nonfinite original medical source plan member')
                retained = json.loads(original_plan, object_pairs_hook=pairs, parse_constant=nonfinite)
                if type(retained) is not dict:raise LocalDeltaError('Exact original medical source plan object required')
            observed_source = {k: v for (k, v) in retained.items() if k != 'observed_native_prior'}
            if (not original_native_equal(observed_source, source_plan) if medical_binding is not None else observed_source != source_plan):
                raise LocalDeltaError('Original exact source plan differs')
            anchors = retained.get('observed_native_prior')
            expected_tables = {self.tables[role] for role in active['previous_expected']}
            if medical_binding is not None and (type(anchors) is not dict or set(anchors) != expected_tables):
                raise LocalDeltaError('Complete exact original prior anchor inventory required')
            prior_total = 0
            for (table, anchor) in anchors.items():
                if medical_binding is not None and (type(anchor) is not dict or set(anchor) != {'uuid','version','snapshot'}
                        or type(anchor['uuid']) is not str
                        or type(anchor['version']) is not int or anchor['version'] < 0
                        or type(anchor['snapshot']) is not dict
                        or set(anchor['snapshot']) != {'schema','rows','row_sha256'}):
                    raise LocalDeltaError('Exact original prior anchor carriers required')
                target = self.transport.targets[table]
                if medical_binding is None:
                    observed = self.transport._snapshot(target, anchor['version'])
                else:
                    role = next(role for role in active['previous_expected'] if self.tables[role] == table)
                    def capture():return self._medical_publication_capture(medical_binding, role,
                        {table: anchor['version']}, medical_binding['bounds'].total_bytes-prior_total, previous=True)
                    observed, _, size = capture()if stages is None else stages.call('prior-capture',capture,role=role)
                    prior_total += size
                if anchor['uuid'] != target.uuid or encoded(observed) != encoded(anchor['snapshot']):
                    raise LocalDeltaError('Original no-op prior native evidence differs')
        return original_plan

    def prepare_original_medical_apply(self, plan, *, context, bounds):
        """Retain original source/effect plans before any medical attempt write."""
        self.require(context)
        binding = getattr(self, '_original_medical_publication_binding', None)
        if (binding is None or plan is not binding['plan'] or bounds is not binding['bounds']
                or self.transport.observation_bounds is not bounds):
            raise PermissionError('Exact bound original medical preparation required')
        value = self._require_medical_publication_binding(binding)
        request = value['request']
        self.source_admit(request)
        effects = LocalDeltaEffects(self.transport)
        operation = 'effects:' + request['request_digest']
        _, _, identities = effects.original_plan(operation, request['request_digest'], value['selected_steps'])
        whole_exists = self.transport.db.execute('SELECT 1 FROM local_plan WHERE operation=?', (operation,)).fetchone() is not None
        source_exists = self.transport.db.execute('SELECT 1 FROM local_source_plan WHERE request_digest=?', (request['request_digest'],)).fetchone() is not None
        started = any(self.transport.db.execute('SELECT 1 FROM local_operation WHERE operation=?', (identity,)).fetchone() is not None for identity in identities)
        if started or whole_exists and not source_exists:
            raise LocalDeltaError('Original medical preparation custody missing or already started')
        original = json.loads(self._apply_source_plan(request, context, False))
        for table, anchor in original['observed_native_prior'].items():
            target = self.transport.targets[table]
            history = self._medical_publication_native(binding, target, {table: anchor['version']})
            if (target.uuid != anchor['uuid'] or not history
                    or type(history[0].get('version')) is not int or history[0]['version'] != anchor['version']):
                raise LocalDeltaError('Original prepared medical native prior changed')
        effects.prepare(operation, request['request_digest'], value['selected_steps'], context=context)
        observed = effects.observe(operation, request['request_digest'], value['selected_steps'], context=context)
        if any(state != 'no-operation-record' for state in observed['states']):
            raise LocalDeltaError('Original medical prepared effects already started')
        self._require_medical_publication_binding(binding)
        self.source_admit(request)

    def prepare_truss_apply(self, request, context):
        """Retain original Truss source and effect intent before prepared phase."""
        self.require(context)
        if self.admission_facts()['profile'] not in (TRUSS_SCALAR_GRAPH_PROFILE,TRUSS_TYPED_GRAPH_PROFILE,TRUSS_CURRENT_GRAPH_PROFILE,TRUSS_CURRENT_045_GRAPH_PROFILE,TRUSS_CURRENT_048_GRAPH_PROFILE,TRUSS_CURRENT_049_GRAPH_PROFILE,TRUSS_CURRENT_050_GRAPH_PROFILE,TRUSS_EMPTY_INTERVAL_PROFILE) or self.policy.active['request'] != request:
            raise PermissionError('Exact original Truss graph preparation required')
        self.source_admit(request)
        self._apply_source_plan(request, context, False)
        LocalDeltaEffects(self.transport).prepare('effects:'+request['request_digest'], request['request_digest'], self.policy.active['steps'], context=context)
        self.observe_prepared_truss_apply(request, context)

    def observe_prepared_truss_apply(self, request, context):
        """Require unchanged original native prior and no started effect slots."""
        self.require(context)
        if self.admission_facts()['profile'] not in (TRUSS_SCALAR_GRAPH_PROFILE,TRUSS_TYPED_GRAPH_PROFILE,TRUSS_CURRENT_GRAPH_PROFILE,TRUSS_CURRENT_045_GRAPH_PROFILE,TRUSS_CURRENT_048_GRAPH_PROFILE,TRUSS_CURRENT_049_GRAPH_PROFILE,TRUSS_CURRENT_050_GRAPH_PROFILE,TRUSS_EMPTY_INTERVAL_PROFILE) or self.policy.active['request'] != request:
            raise PermissionError('Exact original Truss graph preparation required')
        self.source_admit(request)
        plan = json.loads(self._apply_source_plan(request, context, True))
        anchors = plan['observed_native_prior']
        if set(anchors) != {self.tables[role] for role in self.policy.active['previous_expected']}:
            raise LocalDeltaError('Complete original prepared native prior required')
        for table, anchor in anchors.items():
            target = self.transport.targets[table]
            self.transport._detail(target)
            if target.uuid != anchor['uuid'] or int(self.transport.original_history(target)[0]['version']) != anchor['version']:
                raise LocalDeltaError('Original prepared native prior changed')
        observed = LocalDeltaEffects(self.transport).observe('effects:'+request['request_digest'], request['request_digest'], self.policy.active['steps'], context=context)
        if any(state != 'no-operation-record' for state in observed['states']):
            raise LocalDeltaError('Original prepared effects already started')
        self.source_admit(request)

    def _apply(self, request, context, recovery):
        if getattr(self,'query_source_factory',None) is not None:
            raise PermissionError('Registered query source cannot publish')
        self.require(context)
        medical_binding = getattr(self, '_original_medical_publication_binding', None)
        if medical_binding is not None:
            self._require_medical_publication_binding(medical_binding)
            if getattr(self.transport, 'observation_bounds', None) is not medical_binding['bounds']:
                raise PermissionError('Original medical apply requires its bound capture interval')
        self.source_admit(request)
        active = self.policy.active
        if active['request'] != request:
            raise PermissionError('Complete original applying request differs')
        effects = LocalDeltaEffects(self.transport)
        original_plan = self._apply_source_plan(request, context, recovery)
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
            if medical_binding is not None:
                self._validate_medical_effect_proof(request, artifact, context)
            return row[1]
        versions = {}
        parity = {}
        final_total = 0
        for role in ('object_current', 'edge_current', 'tombstone', 'whole_source_history'):
            target = self.transport.targets[self.tables[role]]
            version = int(self.transport.original_history(target)[0]['version'])
            if medical_binding is None:
                snapshot = self.transport._snapshot(target, version)
            else:
                snapshot, _, size = self._medical_publication_capture(medical_binding, role,
                    {target.table: version}, medical_binding['bounds'].total_bytes-final_total)
                final_total += size
            if snapshot['rows'] != sorted(active['expected'][role], key=encoded):
                raise ValueError('Independent complete source oracle differs: ' + role)
            versions[target.table] = version
            parity[role] = snapshot['row_sha256']
        if medical_binding is None:
            proof = {**proof, 'original_generated_steps': active['generated_steps'], 'zero_match_elisions': active['elisions'], 'original_source_plan_sha256': sha(original_plan), 'observed_native_prior': json.loads(original_plan)['observed_native_prior']}
        else:
            proof = {**proof, 'profile': 'ashlar-original-medical-effect-proof/0.1',
                'source_plan_reference': self._medical_source_plan_reference(request, original_plan),
                'zero_match_elisions': active['elisions'], 'original_source_plan_sha256': sha(original_plan)}
        report={'complete': True, 'request_digest': request['request_digest'], 'predecessor': request['predecessor'], 'effect_parity': parity, 'original_source_plan_sha256': sha(original_plan), 'zero_match_elisions': active['elisions'], 'source_oracle_sha256': sha(encoded(active['expected'])), 'source_admission': self.admission_facts(), 'scope': self.admission_facts()['qualification']}
        if self.lookup_hash_profile=='spark-xxhash64-typed-key/0.1':
            profile=getattr(self.transport,'installation_profile',None)
            if type(profile)is not dict or profile.get('physical_profile')!='ashlar-delta/0.4-xxhash64':
                raise PermissionError('Exact installed local `.4` profile required for manifest')
            report.update(physical_carrier_profile=profile['physical_profile'],
                lookup_hash_profile=profile['lookup_hash_profile'],
                generated_carriers_sha256=profile['generated_carriers_sha256'],
                umf_revision=profile['umf_revision'],
                installation_profile_sha256=sha(encoded(profile)))
        manifest = {'publication_id': active['publication_id'], 'profile_version': self._selected_publication_profile(), 'table_versions_json': encoded(versions), 'schema_revisions_json': request['schema_revisions_json'], 'source_progress_json': encoded(active['progress']), 'validation_report_json': encoded(report), 'recorded_at': active.get('recorded_at', '1791547200000000')}
        active['manifest'] = manifest
        artifact_value = {'effects': proof, 'manifest': manifest}
        if medical_binding is not None:
            self._require_medical_artifact_budget(medical_binding, request, artifact_value)
        artifact = encoded(artifact_value)
        if medical_binding is not None:
            self._validate_medical_effect_proof(request, json.loads(artifact), context)
        with self.transport.db:
            self.transport.db.execute('INSERT INTO local_publication_artifact VALUES(?,?,?)', (request['request_digest'], encoded(request), artifact))
        self.validate_manifest(manifest, context)
        return artifact

    def apply(self, request, context):
        return self._apply(request, context, False)

    def recover_apply(self, request, context):
        return self._apply(request, context, True)

    def validate_manifest(self, row, context):
        from .lifecycle import owned_context
        from contextlib import nullcontext
        binding = getattr(self, '_medical_publication_binding', None)
        if getattr(self,'_original_medical_read_binding',None)is not None:
            return self._validate_manifest_body(row,context)
        if (self.admission_facts()['profile'] != 'ashlar-original-medical-public-dataset-admission/0.1'
                or getattr(self, 'query_source_factory', None) is not None):
            return self._validate_manifest_body(row, context)
        if binding is None:
            raise PermissionError('Bound complete medical publication validation required')
        current = getattr(self.transport, 'observation_bounds', None)
        if current is not None and current is not binding['bounds']:
            raise PermissionError('Original medical manifest capture bounds differ')
        interval = nullcontext() if current is binding['bounds'] else self.transport.bounded_observations(binding['bounds'])
        with owned_context(interval):
            return self._validate_manifest_body(row, context)

    def _validate_manifest_body(self, row, context):
        self.require(context)
        active = self.policy.active
        if row != active['manifest']:
            raise PermissionError('Immutable original manifest differs')
        if row.get('profile_version')!=self._selected_publication_profile():
            raise PermissionError('Manifest selected publication profile differs from installation custody')
        report=json.loads(row['validation_report_json'])
        if report.get('source_admission') != self.admission_facts():
            raise PermissionError('Manifest original admitted source profile differs')
        if self.lookup_hash_profile=='spark-xxhash64-typed-key/0.1':
            profile=getattr(self.transport,'installation_profile',None)
            if (type(profile)is not dict or report.get('physical_carrier_profile')!='ashlar-delta/0.4-xxhash64'
                    or report.get('lookup_hash_profile')!=self.lookup_hash_profile
                    or report.get('generated_carriers_sha256')!=profile.get('generated_carriers_sha256')
                    or report.get('umf_revision')!=profile.get('umf_revision')
                    or report.get('installation_profile_sha256')!=sha(encoded(profile))):
                raise PermissionError('Manifest physical carrier profile differs from its installation custody')
        elif any(key in report for key in ('physical_carrier_profile','lookup_hash_profile','generated_carriers_sha256','umf_revision','installation_profile_sha256')):
            raise PermissionError('A legacy `.3` publication cannot claim another physical carrier profile')
        validate_union(active['previous_progress'], active['request'], row)
        self.source_admit(active['request'])
        binding = getattr(self, '_medical_publication_binding', None)
        if getattr(self,'_original_medical_read_binding',None)is not None:
            self._capture_original_medical_read(row,context)
        elif binding is not None:
            self._require_medical_publication_binding(binding)
            total = 0; versions = json.loads(row['table_versions_json'])
            for role in ('object_current', 'edge_current', 'tombstone', 'whole_source_history'):
                _, _, size = self._medical_publication_capture(binding, role, versions,
                    binding['bounds'].total_bytes-total)
                total += size
        else:
            profile=getattr(self.transport,'installation_profile',None)
            if self.lookup_hash_profile=='spark-xxhash64-typed-key/0.1':
                from .local_current_carrier_profile import (
                    XXHASH64_PROFILE,verify_local_current_profile,
                    verify_local_xxhash64_snapshot,verify_local_xxhash64_zstd_snapshot,
                )
                if type(profile)is not dict or profile.get('physical_profile')!=XXHASH64_PROFILE:
                    raise PermissionError('Exact installed local `.4` carrier profile required')
                verify_local_current_profile(self.transport.spark,profile,
                    tuple(self.transport.targets.values()),XXHASH64_PROFILE)
                versions=json.loads(row['table_versions_json'])
                expected_tables={self.tables[role] for role in
                    ('object_current','edge_current','tombstone','whole_source_history')}
                if set(versions)!=expected_tables:
                    raise PermissionError('Complete exact local `.4` four-role vector required')
            for (role, expected) in active['expected'].items():
                target = self.transport.targets[self.tables[role]]
                version = json.loads(row['table_versions_json'])[target.table]
                snapshot=self.transport._snapshot(target,version)
                if self.lookup_hash_profile=='spark-xxhash64-typed-key/0.1':
                    binding=profile['roles'][role]
                    canonical=[(name,{'STRING':'string','BIGINT':'bigint','TIMESTAMP':'timestamp'}[kind])
                               for name,kind,_nullable in binding['fields']]
                    if snapshot['schema']!=[list(field) for field in canonical]:
                        raise ValueError('Pinned local `.4` complete role schema differs: '+role)
                    if role in ('object_current','edge_current'):
                        verify_local_xxhash64_snapshot(self.transport.spark,target.path,
                            version,role)
                    if 'deletionVectors' in profile['roles'][role]['protocol']['features']:
                        raise PermissionError('`.4` footer row parity is not qualified for deletion-vector tables')
                    footer = verify_local_xxhash64_zstd_snapshot(
                        self.transport.spark,target,version,
                        expected_rows=len(snapshot['rows']))
                    if footer['row_groups_rows'] != len(snapshot['rows']):
                        raise ValueError('Pinned `.4` Parquet footer rows differ from the complete Delta snapshot: ' + role)
                if snapshot['rows'] != sorted(expected, key=encoded):
                    raise ValueError('Original exact publication snapshot differs: ' + role)
        for role in active['expected']:
            self.transport._detail(self.transport.targets[self.tables[role]])

    def validate(self, request, artifact, descriptor, context):
        if getattr(self, '_original_medical_publication_binding', None) is not None:
            self._validate_medical_effect_proof(request, json.loads(artifact), context)
        if json.loads(artifact)['manifest'] != dict(descriptor.raw):
            raise ValueError('Retained original applied manifest differs')
        self.validate_manifest(dict(descriptor.raw), context)

    def observe_original_commerce_applied(self, request, artifact_text, context):
        """Observe retained original commerce effects without replay or mutation."""
        self.require(context)
        from .commerce_admission import CommerceAdmission
        if type(self.source_admission)is not CommerceAdmission or self.policy.active['request']!=request:
            raise PermissionError('Actual original applied commerce owner required')
        self.source_admit(request)
        plan=self._apply_source_plan(request,context,True)
        artifact=json.loads(artifact_text);proof=artifact['effects'];effects=LocalDeltaEffects(self.transport)
        from ashlar.attempt_store import DeltaAttemptStore
        from .lifecycle import owned_context
        target=self.transport.targets[self.tables['attempts']]
        store=DeltaAttemptStore(AttemptExecutor(self),CarrierPolicy(self,'attempts'),target.table,target.uuid)
        with owned_context(store.session(context))as session:
            phases=session.read(request['stream'],request['batch_id'])
        if (not phases or phases[-1].phase!='committed'or phases[-1].request_digest!=request['request_digest']
                or json.loads(phases[-1].payload_json)['request']!=request
                or json.loads(json.loads(phases[-1].payload_json)['descriptor_json'])!=artifact['manifest']
                or self.descriptors(artifact['manifest']['publication_id'])!=[artifact['manifest']]):
            raise PermissionError('Original commerce native committed publication required')
        original,digest,identities=effects.original_plan('effects:'+request['request_digest'],request['request_digest'],self.policy.active['steps'])
        if (proof['plan_sha256']!=digest or proof['intent_digest']!=request['request_digest']
                or proof['original_source_plan_sha256']!=sha(plan)or len(proof['original_native_receipts'])!=len(identities)):
            raise PermissionError('Exact original commerce effect proof differs')
        observation=effects.observe('effects:'+request['request_digest'],request['request_digest'],self.policy.active['steps'],context=context)
        if any(state!='committed'for state in observation['states']):raise PermissionError('Original commerce effects must already be committed')
        for ordinal,(key,step)in enumerate(zip(identities,self.policy.active['steps'])):
            native=self.transport.inspect_committed(key,step['statement'],step['parameters'],intent_digest=request['request_digest'],context=context)
            compact=[{'version':str(native['version']),'uuid':native['uuid'],'operation_sha256':native['operation_sha256'],'snapshot_sha256':native['snapshot']['row_sha256']}]
            if compact!=proof['original_native_receipts'][ordinal]:raise PermissionError('Actual original commerce effect receipt differs')
        self.validate_manifest(artifact['manifest'],context)

    @contextmanager
    def hold(self, vector, *, context):
        from .lifecycle import owned_context
        from contextlib import nullcontext
        binding = getattr(self, '_original_medical_publication_binding', None)
        interval = nullcontext()
        if binding is not None:
            current = getattr(self.transport, 'observation_bounds', None)
            if current is not None and current is not binding['bounds']:
                raise PermissionError('Original medical pin capture bounds differ')
            if current is None:
                interval = self.transport.bounded_observations(binding['bounds'])
        with owned_context(interval):
            with owned_context(self._hold(vector, context=context)):
                yield

    @contextmanager
    def _hold(self, vector, *, context):
        self.require(context)
        if self.pin_held:
            raise PermissionError('No nested private pin interval')
        row = dict(self.policy.active['manifest'])
        original_versions = json.loads(row['table_versions_json'])
        expected = manifest_pin_vector(row, {t: self.transport.targets[t].uuid for t in original_versions}, authority='private-local-process')
        if vector != expected:
            raise PermissionError('Original complete held native vector differs')
        medical_closing=getattr(self,'_original_medical_read_closing',None)
        publication_binding=getattr(self,'_original_medical_publication_binding',None)
        self._held_original_vector = vector
        self.pin_held = True
        primary = None
        admitted = False
        try:
            self.validate_manifest(row, context)
            admitted = True
            yield
        except BaseException as error:
            primary = error
        finally:
            try:
                from .lifecycle import finish
                callbacks=[lambda: self.validate_manifest(row, context)] if admitted else []
                if publication_binding is not None:
                    callbacks.extend(self._medical_publication_closing_callbacks(publication_binding, context,
                        original_versions))
                if medical_closing is not None:
                    def close_medical():
                        if type(medical_closing)is not _OriginalGraphSourceRenewal or medical_closing.owner is not self:raise PermissionError('Actual medical closing owner required')
                        medical_closing.renew(vector,context)
                    callbacks.append(close_medical)
                finish(primary,callbacks)
            finally:
                self.pin_held = False
                self._held_original_vector = None

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
        if getattr(self,'_original_medical_read_binding',None)is not None:
            binding,_=self._require_original_medical_read_binding()
            if self.transport.observation_bounds is not binding['bounds']:raise PermissionError('Medical read snapshot bound interval required')
            versions=json.loads(self.policy.active['manifest']['table_versions_json'])
            if type(version)is not int or versions.get(table)!=version:raise PermissionError('Exact immutable medical read version required')
            self._original_medical_read_observation.snapshot(table,version)
        else:self.transport._snapshot(target, version)
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
        if self.admission_facts()['profile'] == 'ashlar-original-medical-public-dataset-admission/0.1':
            binding = getattr(self, '_medical_publication_binding', None)
            if binding is None or not self.pin_held:
                raise PermissionError('Bound original medical protected ACK observation required')
            self.observe_original_medical_publication(binding['plan'], resolved.descriptor,
                context=context, bounds=binding['bounds'])
        else:
            self.validate_manifest(dict(resolved.descriptor.raw), context)

    def acknowledge(self, request, descriptor, context):
        if getattr(self,'query_source_factory',None) is not None:
            raise PermissionError('Registered query source cannot submit ACK')
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
            ack = ProtectedOutboxAck(factory, self, self, self, port['scope'], supported_profiles=[self._selected_publication_profile()], supported_revisions={k: [v] for (k, v) in json.loads(request['schema_revisions_json']).items()})
            ack_ports = {}
        else:
            if encoded(self.ack_sessions.metadata()) != self.original_ack_sessions:
                raise PermissionError('Original ordinary ACK registrations changed')
            ack = self.ack_sessions
            ack_ports = {'pins': self, 'publication_backend': self, 'supported_profiles': [self._selected_publication_profile()],
                         'supported_revisions': {k: [v] for k, v in json.loads(request['schema_revisions_json']).items()}}

        from .truss_feed_sessions import TrussFeedSessions
        truss_reconcile = False
        if type(ack) is TrussFeedSessions:
            from .truss_ack_journal import retain_truss_ack_attempt
            checkpoint = ack.observe_checkpoint(context=context)
            if checkpoint == ack.page.prior.content:
                retain_truss_ack_attempt(self, request, row, vector)
            elif checkpoint == ack.page.next.content:
                ack.adopt_ack_attempt(self, request, row, vector)
                truss_reconcile = True
            else:
                raise ValueError('Original Truss ACK checkpoint differs')

        raw_request = encoded(request).encode()
        raw_manifest = encoded(row).encode()
        try:
            receipt = (ack.reconcile if truss_reconcile else ack.acknowledge)(raw_request, raw_manifest, vector, context=context, **ack_ports)
        except AckOutcomeUncertain as uncertain:
            from .ack_sessions import RegisteredOutboxAcks
            receipt = (ack.reconcile_uncertain(uncertain,vector,context=context,**ack_ports)
                if type(ack)is RegisteredOutboxAcks else ack.reconcile(raw_request, raw_manifest, vector, context=context, **ack_ports))
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

    def __init__(self, driver, *, phase_absence=None):
        self.driver = driver
        self.phase_absence = phase_absence

    def observe_phase_absence(self,read,*,context):
        if self.phase_absence is None:return None
        from .phase_absence import PhaseAbsenceOwner
        if type(self.phase_absence)is not PhaseAbsenceOwner or self.phase_absence.driver is not self.driver:raise PermissionError('Actual phase absence owner required')
        return self.phase_absence.observe(read,context=context)

    def query(self, sql, parameters):
        if sql.lstrip().split(None, 1)[0].upper() in ('SELECT', 'DESCRIBE', 'SHOW'):
            return self.driver.transport.query(sql, parameters)
        request = self.driver.policy.active['request']
        return LocalOperationExecutor(self.driver.transport, 'attempt:' + request['request_digest'], request['request_digest'], self.driver.context).query(sql, parameters)


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
