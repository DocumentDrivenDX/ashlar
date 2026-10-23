# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure, versioned per-slot binding for current-core R04 publication planning.

This module checks correspondence only. It has no Truss, Delta, source, ACK,
or native driver port and cannot perform an effect.
"""
from dataclasses import dataclass
import base64
import hashlib
import json
import re

from .r04_schedule import R04CustodyError, R04Schedule, canonical
from ashlar.source_checkpoint import truss_checkpoint
from ashlar.staging import batch_from_row
from .publication_planning import truss_request_for

PROFILE = 'ashlar-current-core-r04-slot-plan/0.2'
DATA_ROLES = ('object_current', 'edge_current', 'tombstone', 'whole_source_history')
NATIVE_ROLES = (*DATA_ROLES, 'attempts', 'manifest')
_HEX = re.compile(r'^[0-9a-f]{64}$')


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _native_encoded(value):
    """Match delta_custody.encoded without its platform-bound transport import."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False)


def _digest_request(request):
    if type(request) is not dict or set(request) != {
            'stream', 'batch_id', 'predecessor', 'schema_revisions_json',
            'source_batch_json', 'source_batch_digest', 'source_checkpoint_json',
            'request_digest'}:
        raise R04CustodyError('Closed Ashlar Truss publication request required')
    body = {key: value for key, value in request.items() if key != 'request_digest'}
    # Match publication_planning.truss_request_for's JSON encoding exactly.
    raw = json.dumps(body, sort_keys=True, separators=(',', ':')).encode('utf-8')
    digest = _sha(raw)
    if request['request_digest'] != digest:
        raise R04CustodyError('Derived Ashlar publication request digest differs')
    try:
        checkpoint = json.loads(request['source_checkpoint_json'])
    except (TypeError, ValueError) as error:
        raise R04CustodyError('Truss source checkpoint JSON required') from error
    if type(checkpoint) is not dict or not all(type(checkpoint.get(k)) is str for k in
            ('profile', 'feed', 'epoch', 'previous', 'position', 'payload_digest', 'batch_id')):
        raise R04CustodyError('Complete Truss source checkpoint required')
    return digest, checkpoint


def _validate_vector(value, roles, label):
    if type(value) is not dict or set(value) != set(roles):
        raise R04CustodyError(f'Complete {label} role vector required')
    for role, entry in value.items():
        if type(entry) is not dict or set(entry) != {'table', 'uuid', 'version'}:
            raise R04CustodyError(f'Closed {label} identity required for {role}')
        if (type(entry['table']) is not str or not entry['table']
                or type(entry['uuid']) is not str or not entry['uuid']
                or type(entry['version']) is not int or entry['version'] < 0):
            raise R04CustodyError(f'Valid {label} identity required for {role}')
    if len({entry['table'] for entry in value.values()}) != len(value):
        raise R04CustodyError(f'Role tables must be distinct in {label}')
    if len({entry['uuid'] for entry in value.values()}) != len(value):
        raise R04CustodyError(f'Role UUIDs must be distinct in {label}')


def _validate_anchors(value):
    if type(value) is not dict or set(value) != set(NATIVE_ROLES):
        raise R04CustodyError('Complete six-role native anchor inventory required')
    for role, entry in value.items():
        if type(entry) is not dict or set(entry) != {
                'table', 'uuid', 'version', 'schema_sha256', 'protocol', 'installation_id'}:
            raise R04CustodyError(f'Closed observed schema/protocol anchor required for {role}')
        if (type(entry['table']) is not str or not entry['table']
                or type(entry['uuid']) is not str or not entry['uuid']
                or type(entry['version']) is not int or entry['version'] < 0
                or type(entry['schema_sha256']) is not str or not _HEX.fullmatch(entry['schema_sha256'])
                or type(entry['protocol']) is not str or not entry['protocol']
                or type(entry['installation_id']) is not str or not entry['installation_id']):
            raise R04CustodyError(f'Valid observed schema/protocol anchor required for {role}')
    if (len({item['table'] for item in value.values()}) != len(value)
            or len({item['uuid'] for item in value.values()}) != len(value)
            or len({item['installation_id'] for item in value.values()}) != 1):
        raise R04CustodyError('Six native role tables and UUIDs must be distinct')


def _validate_effects(generated, selected, elisions):
    if any(type(value) is not list for value in (generated, selected, elisions)):
        raise R04CustodyError('Generated effects, selected effects and elisions must be lists')
    for step in generated + selected:
        if (type(step) is not dict or set(step) != {'statement', 'parameters'}
                or type(step['statement']) is not str or not step['statement']
                or type(step['parameters']) is not dict
                or any(type(key) is not str or type(value) is not str
                       for key, value in step['parameters'].items())):
            raise R04CustodyError('Closed native statement/parameter effect required')
    removed = {}
    for item in elisions:
        if set(item) != {'ordinal', 'original_step'} or type(item['ordinal']) is not int:
            raise R04CustodyError('Closed zero-match elision required')
        index = item['ordinal']
        if not 0 <= index < len(generated) or index in removed or item['original_step'] != generated[index]:
            raise R04CustodyError('Elision must identify its exact generated step once')
        removed[index] = True
    expected = [step for index, step in enumerate(generated) if index not in removed]
    if selected != expected:
        raise R04CustodyError('Selected effects must preserve generated order and exact content')


@dataclass(frozen=True)
class R04SlotPlan:
    """Immutable canonical envelope; use build and independent owner admission.

    Direct construction checks only the closed envelope and digest. The build
    path checks correspondence; the journal policy must re-admit before use.
    """
    raw: bytes

    def __post_init__(self):
        try:
            value = json.loads(self.raw.decode('utf-8'))
        except (UnicodeError, ValueError) as error:
            raise R04CustodyError('Canonical versioned R04 slot plan required') from error
        fields = {'profile', 'schedule_sha256', 'ordinal', 'slot', 'source',
                  'source_identity', 'source_identity_sha256', 'mutation_request_sha256',
                  'page', 'publication_request', 'publication_request_sha256',
                  'predecessor_manifest', 'predecessor_vector', 'predecessor_progress',
                  'next_progress', 'native_anchors', 'generated_effects',
                  'selected_effects', 'zero_match_elisions', 'independent_prefix_oracle',
                  'admitted_schema_revisions', 'accepted_model_pin', 'publication_id',
                  'operations',
                  'plan_sha256'}
        if (canonical(value) != self.raw or type(value) is not dict
                or set(value) != fields or value.get('profile') != PROFILE):
            raise R04CustodyError('Canonical versioned R04 slot plan required')
        if value.get('plan_sha256') != _sha(canonical({k: v for k, v in value.items() if k != 'plan_sha256'})):
            raise R04CustodyError('R04 slot plan digest differs')

    @classmethod
    def build(cls, schedule, ordinal, retained_slot, *, source_identity,
              publication_request, predecessor_manifest, predecessor_vector,
              predecessor_progress, native_anchors, generated_effects,
              selected_effects, elisions, independent_prefix_oracle,
              admitted_schema_revisions, publication_id):
        """Bind one retained journal slot to its exact publication inputs.

        `retained_slot` is the journal's state row. Values representing source,
        feed, native, and oracle observations must already have been admitted by
        their independent owners; this function only proves exact correspondence.
        """
        if type(schedule) is not R04Schedule or type(ordinal) is not int or not 0 <= ordinal < 8:
            raise R04CustodyError('Current-core schedule and valid slot ordinal required')
        rows = schedule.document()
        spec = rows['slots'][ordinal]
        if type(retained_slot) is not dict or any(retained_slot.get(k) is None for k in
                ('request', 'source_result', 'page')):
            raise R04CustodyError('Retained request, verified source result and actual page required')
        request_record, result_record, page_record = (retained_slot[k] for k in
                ('request', 'source_result', 'page'))
        if (request_record.get('schedule_sha256') != schedule.sha256
                or request_record.get('ordinal') != ordinal
                or result_record.get('request_sha256') != request_record.get('request_sha256')
                or page_record.get('request_sha256') != request_record.get('request_sha256')):
            raise R04CustodyError('Slot plan must bind one retained schedule/request lineage')
        if type(source_identity) is not dict or source_identity != rows['source_bindings'][spec['source']]:
            raise R04CustodyError('Selected source/consumer/ACK identity differs from frozen schedule')
        required_identity = {'feed', 'consumer_id', 'registration_id', 'ack_scope',
                             'ack_registration_id', 'installation_id', 'epoch'}
        if (not required_identity.issubset(source_identity)
                or any(type(source_identity[key]) is not str or not source_identity[key]
                       for key in required_identity)):
            raise R04CustodyError('Frozen independent source, consumer and ACK identity is incomplete')
        source_identity_sha = _sha(canonical(source_identity))
        page = page_record['page']
        event_sha = page.get('event_inventory_sha256')
        if type(event_sha) is not str or not _HEX.fullmatch(event_sha):
            raise R04CustodyError('Actual page event inventory digest required')
        page_facts = {key: page.get(key) for key in (
            'consumer_id', 'registration_id', 'writer_xid', 'outcome', 'sha256',
            'prior_base64', 'next_base64', 'event_inventory_sha256')}
        if any(type(value) is not str or not value for value in page_facts.values()):
            raise R04CustodyError('Actual page identity, opaque cursors and content digest required')
        if not _HEX.fullmatch(page_facts['sha256']) or not _HEX.fullmatch(event_sha):
            raise R04CustodyError('Actual page and event inventory SHA-256 required')
        if page_facts['outcome'] != 'transaction':
            raise R04CustodyError('A complete transaction page is required')
        if (page_facts['consumer_id'] != source_identity['consumer_id']
                or page_facts['registration_id'] != source_identity['registration_id']):
            raise R04CustodyError('Actual page consumer registration differs from frozen source identity')

        publication_sha, checkpoint = _digest_request(publication_request)
        manifest_fields = {'publication_id', 'profile_version', 'table_versions_json',
                           'source_progress_json', 'schema_revisions_json',
                           'validation_report_json', 'recorded_at'}
        if type(predecessor_manifest) is not dict or set(predecessor_manifest) != manifest_fields:
            raise R04CustodyError('Independently admitted predecessor manifest required')
        expected_feed = source_identity.get('feed')
        if type(expected_feed) is not str or checkpoint['feed'] != expected_feed:
            raise R04CustodyError('Publication checkpoint must bind the selected source feed')
        if checkpoint['batch_id'] != publication_request['batch_id']:
            raise R04CustodyError('Publication batch and source checkpoint identities differ')
        if (checkpoint['epoch'] != source_identity['epoch']
                or checkpoint['payload_digest'] != page_facts['sha256']
                or publication_request['predecessor'] != predecessor_manifest.get('publication_id')):
            raise R04CustodyError('Checkpoint epoch/page bytes or publication predecessor differs')
        try:
            batch_doc = json.loads(publication_request['source_batch_json'])
            source_row = {
                'source_profile': batch_doc['profile'], 'feed': batch_doc['feed'],
                'epoch': batch_doc['epoch'], 'batch_id': batch_doc['batch_id'],
                'cursor_before': batch_doc['cursor_before'], 'cursor_after': batch_doc['cursor_after'],
                'records_digest': batch_doc['records_sha256'],
                'batch_json': publication_request['source_batch_json'],
                'batch_digest': publication_request['source_batch_digest']}
            batch = batch_from_row(source_row)
            prior_cursor = json.loads(base64.b64decode(batch.cursor_before, validate=True))
            next_cursor = json.loads(base64.b64decode(batch.cursor_after, validate=True))
            expected_request = truss_request_for(publication_request['stream'], batch,
                publication_request['predecessor'], json.loads(publication_request['schema_revisions_json']))
        except (KeyError, TypeError, ValueError, UnicodeError) as error:
            raise R04CustodyError('Original Truss batch and publication request encoding required') from error
        if (expected_request != publication_request
                or truss_checkpoint(batch) != publication_request['source_checkpoint_json']
                or batch.begin != base64.b64decode(page['content_base64'], validate=True)
                or batch.feed != expected_feed or batch.epoch != source_identity['epoch']
                or batch.batch_id != page_facts['writer_xid']
                or any(cursor.get('installationId') != source_identity['installation_id']
                       or cursor.get('sourceEpoch') != source_identity['epoch']
                       or cursor.get('consumerId') != source_identity['consumer_id']
                       or cursor.get('registrationId') != source_identity['registration_id']
                       for cursor in (prior_cursor, next_cursor))
                or publication_request['predecessor'] != predecessor_manifest.get('publication_id')):
            raise R04CustodyError('Derived publication request does not reproduce the retained Truss page/source checkpoint')

        _validate_vector(predecessor_vector, DATA_ROLES, 'predecessor publication')
        _validate_anchors(native_anchors)
        if type(publication_id) is not str or not publication_id:
            raise R04CustodyError('Exact selected publication identity required before graph effects')
        for role in DATA_ROLES:
            if any(native_anchors[role][key] != predecessor_vector[role][key]
                   for key in ('table', 'uuid', 'version')):
                raise R04CustodyError('Native data anchor must equal the predecessor full vector')
        try:
            manifest_versions = json.loads(predecessor_manifest['table_versions_json'])
            manifest_progress = json.loads(predecessor_manifest['source_progress_json'])
        except (TypeError, ValueError) as error:
            raise R04CustodyError('Predecessor manifest vectors must be canonical JSON') from error
        if (type(manifest_versions) is not dict or type(manifest_progress) is not dict
                or set(manifest_versions) != {predecessor_vector[role]['table'] for role in DATA_ROLES}
                or any(manifest_versions.get(predecessor_vector[role]['table']) != predecessor_vector[role]['version']
                       for role in DATA_ROLES)):
            raise R04CustodyError('Four-role publication vector must match the predecessor manifest')
        if (type(predecessor_progress) is not dict or set(predecessor_progress) !=
                {item['feed'] for item in rows['source_bindings'].values()}
                or any(type(item) is not dict for item in predecessor_progress.values())):
            raise R04CustodyError('Both independent source progress entries required')
        if manifest_progress != predecessor_progress:
            raise R04CustodyError('Both retained source progress entries must match the predecessor manifest')
        try:
            current_revisions = json.loads(publication_request['schema_revisions_json'])
        except (TypeError, ValueError) as error:
            raise R04CustodyError('Current publication schema revisions required') from error
        if (not isinstance(predecessor_manifest['publication_id'], str)
                or not predecessor_manifest['publication_id']
                or type(current_revisions) is not dict or type(admitted_schema_revisions) is not dict
                or admitted_schema_revisions != current_revisions):
            raise R04CustodyError('Current schema revisions must match current admitted catalog evidence')
        next_progress = {key: dict(value) for key, value in predecessor_progress.items()}
        prior_source_progress = predecessor_progress[expected_feed]
        if (prior_source_progress.get('feed') != expected_feed
                or prior_source_progress.get('epoch') != source_identity['epoch']
                or prior_source_progress.get('profile') != checkpoint['profile']
                or checkpoint['previous'] != prior_source_progress.get('position')):
            raise R04CustodyError('Truss checkpoint must continue the selected source progress exactly')
        next_progress[expected_feed] = checkpoint
        other_feed = next(feed for feed in next_progress if feed != expected_feed)
        if (next_progress[expected_feed]['feed'] != expected_feed
                or next_progress[other_feed] != predecessor_progress[other_feed]):
            raise R04CustodyError('Current slot may advance only its selected source progress')

        oracle_before = rows['prefix_oracle'][ordinal]
        oracle_after = rows['prefix_oracle'][ordinal + 1]
        if independent_prefix_oracle != {'before': oracle_before, 'after': oracle_after}:
            raise R04CustodyError('Independent oracle must match the frozen adjacent prefix pair')
        _validate_effects(generated_effects, selected_effects, elisions)
        native_installation_id = native_anchors['manifest']['installation_id']
        effect_operation = 'effects:' + publication_sha
        native_intent = _native_encoded({'profile': 'ashlar-local-delta-effects/0.1',
            'installation_id': native_installation_id, 'operation': effect_operation,
            'intent_digest': publication_sha, 'steps': selected_effects})
        operation_digest = _sha(native_intent.encode('utf-8'))
        operations = [f'local-effect:{operation_digest}:{index}'
                      for index in range(len(selected_effects))]
        plan = {
            'profile': PROFILE,
            'schedule_sha256': schedule.sha256,
            'ordinal': ordinal,
            'slot': spec['slot'],
            'source': spec['source'],
            'source_identity': source_identity,
            'source_identity_sha256': source_identity_sha,
            'mutation_request_sha256': request_record['request_sha256'],
            'page': page_facts,
            'publication_request': publication_request,
            'publication_request_sha256': publication_sha,
            'predecessor_manifest': predecessor_manifest,
            'predecessor_vector': predecessor_vector,
            'predecessor_progress': predecessor_progress,
            'next_progress': next_progress,
            'native_anchors': native_anchors,
            'generated_effects': generated_effects,
            'selected_effects': selected_effects,
            'zero_match_elisions': elisions,
            'independent_prefix_oracle': independent_prefix_oracle,
            'admitted_schema_revisions': admitted_schema_revisions,
            'accepted_model_pin': rows['accepted_model_pins'][spec['source']],
            'publication_id': publication_id,
            'operations': operations,
        }
        plan['plan_sha256'] = _sha(canonical(plan))
        return cls(canonical(plan))

    def document(self):
        return json.loads(self.raw.decode('utf-8'))

    @property
    def sha256(self):
        return _sha(self.raw)


class R04SlotCoordinator:
    """Admission-only sequencer over a retained journal; no effect methods exist."""
    def __init__(self, journal, admissions):
        from .r04_schedule import R04Journal
        if type(journal) is not R04Journal or not callable(
                getattr(admissions, 'admit_source_identity', None)) or not callable(
                getattr(admissions, 'admit_current_slot', None)) or not callable(
                getattr(admissions, 'admit_completed_slot', None)):
            raise R04CustodyError('Retained journal and independent admission-only ports required')
        self.journal, self.admissions = journal, admissions

    def prepare(self, ordinal, **bindings):
        state = self.journal.state()
        schedule = self.journal.schedule
        if type(ordinal) is not int or not 0 <= ordinal < 8:
            raise R04CustodyError('Current R04 slot ordinal required')
        next_slot = next((index for index, row in enumerate(state) if row['ack'] is None), 8)
        if ordinal != next_slot:
            raise R04CustodyError('Global source/publication/ACK prefix must remain contiguous')
        document = schedule.document()
        selected = document['slots'][ordinal]['source']
        identity = bindings.get('source_identity')
        if identity != document['source_bindings'][selected] or self.admissions.admit_source_identity(
                selected, identity) is not None:
            raise R04CustodyError('Selected source and ACK session identity is not independently admitted')
        self.journal._revalidate_retained(state)
        current_revisions = self.admissions.admit_current_slot(schedule, ordinal,
            selected, identity, state[ordinal])
        if (type(current_revisions) is not dict
                or current_revisions != bindings.get('admitted_schema_revisions')):
            raise R04CustodyError('Current source result/page was not re-admitted before slot planning')
        if ordinal:
            prior_ordinal = ordinal - 1
            prior_source = document['slots'][prior_ordinal]['source']
            if state[prior_ordinal]['publication'] is None or state[prior_ordinal]['ack'] is None:
                raise R04CustodyError('Immediately prior slot must be re-admitted through its own source/ACK session')
            admitted_prior = self.admissions.admit_completed_slot(prior_ordinal, prior_source,
                document['source_bindings'][prior_source], state[prior_ordinal])
            expected_prior = {'manifest': bindings.get('predecessor_manifest'),
                              'vector': bindings.get('predecessor_vector'),
                              'progress': bindings.get('predecessor_progress')}
            if type(admitted_prior) is not dict or admitted_prior != expected_prior:
                raise R04CustodyError('Next slot predecessor must equal the immediately prior slot publication/vector/progress')
            if state[prior_ordinal]['slot_plan'] is None:
                raise R04CustodyError('Prior slot must retain its complete current-core publication plan')
            prior_plan_raw = base64.b64decode(state[prior_ordinal]['slot_plan']['plan_base64'], validate=True)
            prior_anchors = R04SlotPlan(prior_plan_raw).document()['native_anchors']
            current_anchors = bindings.get('native_anchors')
            if (type(current_anchors) is not dict
                    or any(type(current_anchors.get(role)) is not dict for role in NATIVE_ROLES)
                    or any(any(current_anchors.get(role, {}).get(key) != prior_anchors[role][key]
                               for key in ('table', 'uuid', 'schema_sha256', 'protocol', 'installation_id'))
                           for role in NATIVE_ROLES)):
                raise R04CustodyError('Current slots must retain the same six-role native installation identity')
        if state[ordinal]['slot_plan'] is not None:
            raw = base64.b64decode(state[ordinal]['slot_plan']['plan_base64'], validate=True)
            retained = R04SlotPlan(raw)
            value = retained.document()
            matched = (value['source_identity'] == identity
                and value['admitted_schema_revisions'] == current_revisions
                and value['publication_request'] == bindings.get('publication_request')
                and value['predecessor_manifest'] == bindings.get('predecessor_manifest')
                and value['predecessor_vector'] == bindings.get('predecessor_vector')
                and value['predecessor_progress'] == bindings.get('predecessor_progress')
                and value['native_anchors'] == bindings.get('native_anchors')
                and value['generated_effects'] == bindings.get('generated_effects')
                and value['selected_effects'] == bindings.get('selected_effects')
                and value['publication_id'] == bindings.get('publication_id')
                and value['zero_match_elisions'] == bindings.get('elisions')
                and value['independent_prefix_oracle'] == bindings.get('independent_prefix_oracle'))
            if (not matched or self.journal.policy.admit_retained_slot_plan(schedule,
                    ordinal, retained, self.journal.context) is not None):
                raise R04CustodyError('Retained slot plan differs from exact original plan inputs')
            return retained
        plan = R04SlotPlan.build(schedule, ordinal, state[ordinal], **bindings)
        self.journal.retain_slot_plan(ordinal, plan.raw)
        return plan
