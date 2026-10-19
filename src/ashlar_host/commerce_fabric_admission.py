# SPDX-License-Identifier: MIT OR Apache-2.0
"""Private cooperative native Fabric admission, activation and query leases.

No Fabric generation token/security isolation is claimed. Every participating
mutator must use the same owner; independent source/ACK/native custody remains
with the held deployment or execute_commerce_fabric_gql's fresh source interval.
"""
from collections import Counter
from contextlib import contextmanager
from dataclasses import asdict
import base64
import hashlib
import json
import os
import threading
import time

from ashlar.schema import decode_retained_json
from .commerce_fabric_deployment import (CommerceFabricDeploymentConfig, HeldCommerceFabricAdmission,
                                         HeldCommerceFabricDeployment)
from .fabric_deployment_http import open_fabric_deployment_http
from .commerce_graph_release import HeldOriginalCommerceGraphRelease
from .fabric_graph_definition import fabric_carrier_queries
from .fabric_gql import FabricGraphBinding, execute_gql
from .fabric_ordinary_http import FabricOrdinaryHttp, FabricOrdinaryHttpConfig
from .fabric_release_journal import FabricReleaseJournal
from .lifecycle import finish, owned_context

_FORMAT = 'ashlar-private-fabric-admission/0.1'


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _bytes(value):
    return base64.b64encode(value).decode('ascii')


def _response(value):
    return {'status': value.status, 'body': _bytes(value.body), 'headers': list(value.headers)}


def _configuration(value):
    return (tuple(vars(value.upload.authentication).items()), value.upload.lakehouse_id,
            value.upload.maximum_file_bytes, value.upload.maximum_table_bytes, value.node_projection, value.edge_projection, value.node_table, value.edge_table,
            value.display_name, value.ownership_profile)


def _transport(transport, context, configuration, binding):
    if type(transport) is not FabricOrdinaryHttp or context is not transport.context:
        raise PermissionError('Actual selected ordinary GQL owner/context required')
    expected = FabricOrdinaryHttpConfig(**vars(configuration.upload.authentication), graph_model_id=binding.graph_model_id)
    if transport.selected_configuration() != expected:
        raise PermissionError('Original query account/workspace/item selection differs')


def _definition(http, binding, scope):
    response = http.graph_definition(binding.graph_model_id)
    evidence = [_response(response)]
    if response.status == 202:
        operation = dict(response.headers)['x-ms-operation-id']
        headers = dict(response.headers)
        expires = time.monotonic() + scope.configuration.upload.authentication.deadline_seconds
        for _ in range(16):
            scope.renew()
            wait = int(headers.get('retry-after', '0'))
            if wait >= expires - time.monotonic():
                raise TimeoutError('Original definition operation deadline refused')
            if wait:
                time.sleep(wait)
            scope.renew()
            state = http.creation_state(operation)  # shared public LRO-state envelope
            evidence.append(_response(state))
            phase = decode_retained_json(state.body)['status']
            if phase == 'Succeeded':
                response = http.operation_result(operation)
                evidence.append(_response(response))
                break
            if phase not in ('NotStarted', 'Running'):
                raise PermissionError('Original definition operation failed')
            headers = dict(state.headers)
        else:
            raise TimeoutError('Original definition operation capacity refused')
    value = decode_retained_json(response.body)
    actual = value.get('definition') if type(value) is dict else None
    expected = decode_retained_json(scope.intent.mapping_json.encode())['definition']
    def parts(definition):
        if type(definition) is not dict or set(definition) != {'format', 'parts'} or definition['format'] != 'json' or type(definition['parts']) is not list:
            raise PermissionError('Exact supported public definition required')
        result = {}
        for part in definition['parts']:
            if (type(part) is not dict or set(part) != {'path', 'payloadType', 'payload'}
                    or part['payloadType'] != 'InlineBase64' or type(part['path']) is not str
                    or part['path'] in result or type(part['payload']) is not str):
                raise PermissionError('Complete distinct original definition parts required')
            raw = base64.b64decode(part['payload'], validate=True)
            # Public retained JSON codec preserves duplicate rejection and scalar
            # types. Full structural JSON comparison never drops extension keys.
            result[part['path']] = _json(decode_retained_json(raw))
        return result
    if parts(actual) != parts(expected):
        raise PermissionError('Native definition/mapping/source locations differ')
    return evidence


def _controls(scope):
    scope.renew()
    records = {}
    with owned_context(open_fabric_deployment_http(scope.configuration.upload.authentication)) as http:
        if scope.creation.kind == 'asynchronous':
            state = http.creation_state(scope.creation.operation_handle)
            if decode_retained_json(state.body)['status'] != 'Succeeded':
                raise PermissionError('Original native creation no longer succeeded')
            result = http.creation_result(scope.creation.operation_handle)
            item = decode_retained_json(result.body)
            if item['id'] != scope.binding.graph_model_id or item.get('displayName') != scope.configuration.display_name:
                raise PermissionError('Original creation item differs')
            records['creation'] = [_response(state), _response(result)]
        elif scope.creation.kind != 'synchronous':
            raise PermissionError('Original real creation response required')
        listing = http.item_jobs(scope.binding.graph_model_id)
        value = decode_retained_json(listing.body)
        jobs = value['value']
        if (value.get('continuationToken') or value.get('continuationUri') or len(jobs) != 1
                or jobs[0]['id'] != scope.first_load or jobs[0]['jobType'] != 'Refresh'
                or jobs[0]['invokeType'] != 'Manual'):
            raise PermissionError('Sole original load required; unexpected refresh refuses')
        job = http.item_job(scope.binding.graph_model_id, scope.first_load)
        loaded = decode_retained_json(job.body)
        if (loaded['status'] != 'Completed' or loaded['jobType'] != 'Refresh'
                or loaded['invokeType'] != 'Manual' or loaded.get('failureReason') is not None):
            raise PermissionError('Original initial load must be terminal Completed before GQL')
        records['jobs'] = [_response(listing), _response(job)]
        records['definition'] = _definition(http, scope.binding, scope)
    scope.renew()
    return records


class _ProofPolicy:
    def __init__(self, scope):
        self.scope = scope

    @contextmanager
    def hold(self, binding, context):
        self.scope.same(binding, context)
        self.scope.renew()
        primary = None
        try:
            yield
        except BaseException as error:
            primary = error
        finish(primary, [self.scope.renew])

    def admit(self, binding, query, context):
        self.scope.same(binding, context)
        if query not in tuple(case[1] for case in fabric_carrier_queries()):
            raise PermissionError('Only original independent full-bag proof queries admitted')
        self.scope.renew()


def _prove(scope):
    # This substantive native gate occurs before any proof query can warm/cache
    # the inactive item. No receipt or caller Boolean can substitute for it.
    opening = _controls(scope)
    expected = decode_retained_json(scope.release_payload)
    evidence = {}
    primary = None
    try:
        for role, query, names in fabric_carrier_queries():
            result = execute_gql(scope.binding, query, transport=scope.transport, policy=_ProofPolicy(scope),
                context=scope.transport_context, maximum_rows=10000)
            rows = expected[role] if role != 'endpoints' else [
                {'source': edge['src'], 'relationship_id': edge['graph_id'], 'destination': edge['dst']}
                for edge in expected['edges']]
            wanted = Counter(tuple(row[name] for name in names) for row in rows)
            if (result.columns != tuple((name, 'STRING') for name in names)
                    and not (not wanted and result.columns == () and result.rows == ())):
                raise PermissionError('Exact full native carrier columns/types required')
            if Counter(result.rows) != wanted:
                raise PermissionError('Independent full native identity/value/multiplicity bag differs')
            evidence[role] = {'query': query, 'request_sha256': result.request_sha256,
                              'responses': [_bytes(raw) for raw in result.responses]}
    except BaseException as error:
        primary = error
    closing = {}
    def close_native():
        closing.update(_controls(scope))
    finish(primary, [scope.renew, close_native])
    return {'opening': opening, 'bags': evidence, 'closing': closing}


class _Scope:
    def same(self, binding, context):
        if binding != self.binding or context is not self.transport_context:
            raise PermissionError('Original native proof binding/context required')

    def renew(self):
        finish(None, [self.custody.renew, lambda: _transport(self.transport, self.transport_context, self.configuration, self.binding)])


class _DeploymentCustody:
    def __init__(self, interval):
        if type(interval) is not HeldCommerceFabricAdmission:
            raise PermissionError('Nominal actual deployment admission interval required')
        self.interval = interval
    def renew(self):
        self.interval.renew()


def admit_commerce_fabric_deployment(deployment, *, transport, transport_context, expected_revision):
    """Native definition/full-bag admission and one durable evidence/predecessor CAS.

    Caller must close its ordinary GQL owner and enclosing deployment before
    releasing a final workflow report. Private cooperative control is explicit;
    native content addressing/security isolation is not established.
    """
    if type(deployment) is not HeldCommerceFabricDeployment:
        raise ValueError('Actual held deployment owner required')
    if type(expected_revision) is not int or expected_revision < 0:
        raise ValueError('Exact nonnegative activation predecessor required')
    with owned_context(deployment.admission_interval()) as interval:
        scope = _Scope()
        scope.custody = _DeploymentCustody(interval)
        scope.configuration = interval.configuration
        scope.intent = interval.intent
        scope.release_payload = interval.release.payload
        mutation = interval.mutation
        scope.creation = mutation.creation(scope.intent.candidate_id)
        scope.first_load = mutation.first_load(scope.intent.candidate_id)
        if scope.creation.graph_model_id is None or scope.first_load is None:
            raise PermissionError('Original native creation and first-load handles required')
        scope.binding = FabricGraphBinding(scope.intent.workspace_id, scope.creation.graph_model_id, scope.intent.release_sha256)
        scope.transport, scope.transport_context = transport, transport_context
        native = _prove(scope)
        receipt = {'format': _FORMAT, 'ownership_profile': scope.configuration.ownership_profile,
            'intent': asdict(scope.intent), 'binding': asdict(scope.binding),
            'release_payload': _bytes(scope.release_payload), 'first_load': scope.first_load,
            'creation': {'kind': scope.creation.kind, 'operation_handle': scope.creation.operation_handle,
                         'synchronous_response': None if scope.creation.synchronous_response is None else _bytes(scope.creation.synchronous_response)},
            'expected_revision': expected_revision, 'native': native}
        raw = _json(receipt).encode()
        scope.renew()
        revision = mutation.record_admission_and_activate(scope.intent.candidate_id, expected_revision, raw)
        scope.renew()
        return scope.binding, revision


class _ReaderCustody:
    def __init__(self, policy, active, receipt):
        self.policy, self.active, self.receipt = policy, active, receipt
        self.pid, self.thread = os.getpid(), threading.get_ident()
        self.live = True

    def renew(self):
        if not self.live or self.pid != os.getpid() or self.thread != threading.get_ident():
            raise PermissionError('Original active reader interval expired')
        self.policy.current()
        self.policy._source.renew()
        with self.policy.journal.reader() as current:
            if current != self.active or self.policy.journal.admission(current.intent.candidate_id) != self.receipt:
                raise PermissionError('Original active binding/admission changed')


class CommerceFabricLoadedGraphPolicy:
    """Existing hold/admit port over actual active-item read exclusion/native proof.

    execute_commerce_fabric_gql independently acquires fresh source/ACK/native
    custody and binds the exact release digest. This owner never reacquires it.
    """
    def __init__(self, journal, configuration, transport, transport_context):
        if type(journal) is not FabricReleaseJournal or type(configuration) is not CommerceFabricDeploymentConfig:
            raise ValueError('Exact private deployment owner selection required')
        configuration.__post_init__()
        self.journal, self.configuration = journal, configuration
        self.transport, self.transport_context = transport, transport_context
        self.context = object()
        self._selected = (journal, configuration, _configuration(configuration), transport, transport_context, self.context)
        self._scope = None
        self._source = None
        self._query = None

    def __setattr__(self, name, value):
        if name in ('_selected', '_scope', '_source') and hasattr(self, name):
            raise AttributeError('Original cooperative policy interval is immutable')
        object.__setattr__(self, name, value)

    def __delattr__(self, name):
        if name in ('_selected', '_scope', '_source'):
            raise AttributeError('Original cooperative policy interval is immutable')
        object.__delattr__(self, name)

    @contextmanager
    def source_interval(self, held):
        if type(held) is not HeldOriginalCommerceGraphRelease or self._source is not None:
            raise PermissionError('Nominal fresh original source interval required')
        held.renew()
        object.__setattr__(self, '_source', held)
        primary = None
        try:
            yield
        except BaseException as error:
            primary = error
        try:
            finish(primary, [held.renew])
        finally:
            object.__setattr__(self, '_source', None)

    def current(self):
        journal, config, facts, transport, context, graph_context = self._selected
        if (self.journal is not journal or self.configuration is not config or _configuration(config) != facts
                or self.transport is not transport or self.transport_context is not context or self.context is not graph_context):
            raise PermissionError('Original cooperative policy owner selection changed')

    @contextmanager
    def hold(self, binding, context):
        self.current()
        if context is not self.context or self._scope is not None or self._source is None:
            raise PermissionError('Original source-held nonoverlapping graph policy interval required')
        with self.journal.reader() as active:
            if binding != FabricGraphBinding(active.intent.workspace_id, active.graph_model_id, active.intent.release_sha256):
                raise PermissionError('Exact active publication binding required')
            retained = self.journal.admission(active.intent.candidate_id)
            value = decode_retained_json(retained[1])
            if (value.get('format') != _FORMAT or value.get('ownership_profile') != self.configuration.ownership_profile
                    or value.get('binding') != asdict(binding) or value.get('intent') != decode_retained_json(_json(asdict(active.intent)).encode())
                    or value.get('expected_revision') != retained[0] or active.revision != retained[0] + 1):
                raise PermissionError('Original durable admission/predecessor correspondence differs')
            payload = base64.b64decode(value['release_payload'], validate=True)
            if (hashlib.sha256(payload).hexdigest() != binding.release_sha256
                    or self._source.release.payload != payload):
                raise PermissionError('Original complete release bytes differ')
            source = decode_retained_json(payload)
            vector = decode_retained_json(active.intent.publication_json.encode())
            if (source['snapshots'] != vector['snapshots'] or source['publication']['publication_id'] != vector['publication_id']
                    or hashlib.sha256(_json(source['publication']).encode()).hexdigest() != vector['manifest_sha256']):
                raise PermissionError('Original publication manifest/full UUID-version vector differs')
            custody = _ReaderCustody(self, active, retained)
            scope = _Scope()
            scope.custody, scope.configuration, scope.intent = custody, self.configuration, active.intent
            scope.release_payload, scope.binding = payload, binding
            scope.creation = self.journal.creation(active.intent.candidate_id)
            scope.first_load = self.journal.first_load(active.intent.candidate_id)
            creation = {'kind': scope.creation.kind, 'operation_handle': scope.creation.operation_handle,
                        'synchronous_response': None if scope.creation.synchronous_response is None else _bytes(scope.creation.synchronous_response)}
            if (scope.first_load != value['first_load'] or scope.creation.graph_model_id != binding.graph_model_id
                    or creation != value['creation']):
                raise PermissionError('Original creation/first-load identity differs')
            scope.transport, scope.transport_context = self.transport, self.transport_context
            primary = None
            try:
                _prove(scope)  # actual current native evidence, never receipt authority
                object.__setattr__(self, '_scope', scope)
                self._query = None
                yield
            except BaseException as error:
                primary = error
            finally:
                object.__setattr__(self, '_scope', None)
                self._query = None
            try:
                finish(primary, [scope.renew, lambda: _prove(scope)])
            finally:
                custody.live = False

    def admit(self, binding, query, context):
        self.current()
        if self._scope is None or context is not self.context or binding != self._scope.binding:
            raise PermissionError('Original held active graph policy required')
        if self._query is None:
            self._query = query
        if query != self._query:
            raise PermissionError('Original query text required throughout use')
        self._scope.renew()
