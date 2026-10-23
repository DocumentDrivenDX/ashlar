"""Focused custody invariants; fake ports here are tests, not release evidence."""
import hashlib
import base64
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace

from ashlar_host.r04_schedule import (PROFILE, R04CustodyError, R04Journal,
    R04Schedule, R04SchedulePolicy, SLOT_ORDER, STAGES, canonical)
from ashlar_host.r04_slot_plan import (DATA_ROLES, NATIVE_ROLES,
    R04SlotCoordinator, R04SlotPlan)
from ashlar_host.r04_truss049_feed import (MAXIMUM_BYTES, MAXIMUM_FACTS,
    read_next_transaction)
from ashlar.truss_native_feed import native_feed_batch
from ashlar_host.publication_planning import truss_request_for
from ashlar_host.delta_custody import LocalDeltaEffects


def schedule():
    slots = []
    for ordinal, (name, stage) in enumerate(zip(SLOT_ORDER, STAGES)):
        source = name[0]
        slots.append({'ordinal': ordinal, 'slot': name, 'source': source,
            'stage': stage, 'entity_ref': f'entity:{source}' if ordinal < 6 else f'candidate:{source}',
            'predecessor_slot': ordinal - 2 if stage in ('update', 'delete') else None,
            'mutation': {'stage': stage, 'value': f'{name}-value'},
            'expected_transition': {'prefix': ordinal + 1, 'source': source}})
    return R04Schedule.from_document({'profile': PROFILE,
        'source_bindings': {
            'A': {'test_only': 'source-A', 'feed': 'feed-A', 'consumer_id': 'consumer-A',
                'registration_id': 'registration-A', 'ack_scope': 'ack-A',
                'ack_registration_id': 'ack-registration-A', 'installation_id': 'install-A', 'epoch': 'epoch-A'},
            'B': {'test_only': 'source-B', 'feed': 'feed-B', 'consumer_id': 'consumer-B',
                'registration_id': 'registration-B', 'ack_scope': 'ack-B',
                'ack_registration_id': 'ack-registration-B', 'installation_id': 'install-B', 'epoch': 'epoch-B'}},
        'accepted_model_pins': {'A': {'test_only': 'model-A'}, 'B': {'test_only': 'model-B'}},
        'slots': slots, 'prefix_oracle': [{'prefix': n, 'test_only': True} for n in range(9)]})


class TestOnlyPolicy(R04SchedulePolicy):
    reject_retained_page = False
    reject_retained_plan = False
    reject_prior = False
    result_id_override = None

    def admit_request(self, selected, ordinal, request_bytes, predecessor_ids, context):
        import json
        slot = selected.document()['slots'][ordinal]
        try:
            body = json.loads(request_bytes.decode())
        except (UnicodeError, ValueError):
            return 'invalid request'
        expected = {'slot': slot['slot'], 'stage': slot['stage'], 'value': slot['mutation']['value']}
        return None if body == expected else 'semantic mismatch'

    def admit_before_source_effect(self, selected, ordinal, request, prior_publication, prior_ack, context):
        if ordinal and (prior_publication is None or prior_ack is None):
            return 'prior slot missing'
        return 'current prior refused' if self.reject_prior else None

    def admit_schedule(self, selected, context):
        if selected is None or context != 'test':
            raise AssertionError('test seam only')

    def admit_source_result(self, selected, ordinal, request, result, readback, context):
        spec = selected.document()['slots'][ordinal]
        if ordinal < 6:
            identity = self.result_id_override or f'provider-id-{spec["source"]}'
            return {spec['entity_ref']: identity}
        return {'candidate': 'accepted'}

    def admit_page(self, selected, ordinal, request, result, page, context):
        return None

    def admit_retained_page(self, selected, ordinal, request, result, page_record, context):
        return 'stale page' if self.reject_retained_page else None

    def admit_before_graph_effect(self, selected, ordinal, request, result, page_record, slot_plan, context):
        return None

    def admit_slot_plan(self, selected, ordinal, plan, context):
        return None

    def admit_retained_slot_plan(self, selected, ordinal, plan, context):
        return 'stale plan' if self.reject_retained_plan else None

    def admit_before_ack_effect(self, selected, ordinal, publication, context):
        return None

    def admit_publication(self, selected, ordinal, page, publication, slot_plan, context):
        return None

    def admit_ack(self, selected, ordinal, publication, ack, context):
        return None


def page(content=None, source='A'):
    prior = canonical({'profile': 'truss-caller-asserted-feed/0.1', 'installationId': f'install-{source}',
        'sourceEpoch': f'epoch-{source}', 'consumerId': f'consumer-{source}',
        'registrationId': f'registration-{source}', 'nextXid': '1'})
    following = canonical({'profile': 'truss-caller-asserted-feed/0.1', 'installationId': f'install-{source}',
        'sourceEpoch': f'epoch-{source}', 'consumerId': f'consumer-{source}',
        'registrationId': f'registration-{source}', 'nextXid': '2'})
    if content is None:
        content = canonical({'interfaceVersion': 'truss-native-feed/0.1',
            'prior': json.loads(prior), 'next': json.loads(following), 'writerXid': '1',
            'catalogPrerequisites': [], 'revisions': [], 'changes': [], 'sources': [],
            'reservations': [], 'configuration': {}})
    return SimpleNamespace(content=content, sha256=hashlib.sha256(content).hexdigest(),
        prior=SimpleNamespace(content=prior), next=SimpleNamespace(content=following),
        consumer_id=f'consumer-{source}', registration_id=f'registration-{source}',
        writer_xid='1', outcome='transaction')


def request_for(selected):
    return canonical({'slot': selected['slot'], 'stage': selected['stage'],
                      'value': selected['mutation']['value']})


def publication_request_for(page_value, source='A', revisions=None):
    batch = native_feed_batch(page_value.content, sha256=page_value.sha256,
        prior=page_value.prior.content, next=page_value.next.content, feed=f'feed-{source}')
    return truss_request_for(f'r04-{source}', batch, 'genesis', revisions or {})


def prior_progress(page_value, source):
    return {'profile': 'truss-native-feed/0.1', 'feed': f'feed-{source}',
        'epoch': f'epoch-{source}', 'previous': 'genesis',
        'position': base64.b64encode(page_value.prior.content).decode('ascii'),
        'payload_digest': '0' * 64, 'batch_id': f'prior-{source}'}


def test_slot_plan(owner, ordinal):
    selected = owner.schedule.document()
    spec = selected['slots'][ordinal]
    source = spec['source']
    state = owner.state()[ordinal]
    vector = {role: {'table': f'local.graph.{role}', 'uuid': f'uuid-{role}', 'version': 0}
        for role in DATA_ROLES}
    anchors = {role: {'table': f'local.graph.{role}', 'uuid': f'uuid-{role}', 'version': 0,
        'schema_sha256': 'a' * 64, 'protocol': 'delta-protocol-v1',
        'installation_id': 'ashlar-install'} for role in NATIVE_ROLES}
    progress = {'feed-A': prior_progress(page(source='A'), 'A'),
                'feed-B': prior_progress(page(source='B'), 'B')}
    manifest = {'publication_id': 'genesis', 'profile_version': 'ashlar-delta/0.3',
        'table_versions_json': json.dumps({entry['table']: entry['version'] for entry in vector.values()}, sort_keys=True, separators=(',', ':')),
        'source_progress_json': json.dumps(progress, sort_keys=True, separators=(',', ':')),
        'schema_revisions_json': '{}', 'validation_report_json': '{}', 'recorded_at': '1'}
    return R04SlotPlan.build(owner.schedule, ordinal, state,
        source_identity=selected['source_bindings'][source],
        publication_request=publication_request_for(page(source=source), source),
        predecessor_manifest=manifest, predecessor_vector=vector,
        predecessor_progress=progress, native_anchors=anchors,
        generated_effects=[{'statement': 'MERGE', 'parameters': {'row': '{}'}}],
        selected_effects=[{'statement': 'MERGE', 'parameters': {'row': '{}'}}],
        elisions=[], independent_prefix_oracle={'before': selected['prefix_oracle'][ordinal],
            'after': selected['prefix_oracle'][ordinal + 1]}, admitted_schema_revisions={},
        publication_id=f'publication-{spec["slot"]}')


class R04ScheduleCustodyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name).resolve() / 'r04.sqlite'
        self.policy = TestOnlyPolicy()
        self.owner = R04Journal.create(self.path, schedule(), policy=self.policy, context='test')
        self.addCleanup(lambda: self.owner.connection.close())

    def complete(self, ordinal, *, predecessor_ids=None):
        selected = self.owner.schedule.document()['slots'][ordinal]
        entity = selected['entity_ref']
        ids = predecessor_ids or {}
        if self.owner.state()[ordinal]['request'] is None:
            self.owner.freeze_request(ordinal, request_for(selected), predecessor_ids=ids)
        self.owner.begin_source_effect(ordinal)
        self.owner.retain_source_result(ordinal, b'public-commit', b'public-readback')
        self.owner.bind_page(ordinal, page(source=selected['source']))
        self.owner.retain_slot_plan(ordinal, test_slot_plan(self.owner, ordinal).raw)
        self.owner.begin_graph_effect(ordinal)
        self.owner.retain_publication(ordinal, {'full_vector': f'vector-{ordinal}', 'progress': ordinal + 1})
        self.owner.begin_ack_effect(ordinal)
        self.owner.retain_ack(ordinal, {'source': selected['source'], 'durable': True})
        return {entity: f'provider-id-{selected["source"]}'}

    def test_truss049_feed_owner_reads_only_the_admitted_registered_consumer(self):
        class Ok:
            def __init__(self, value):
                self.value = value

        page_value = page(source='A')

        class Assembly:
            def __init__(self):
                self.calls = []

            def read_feed_in_transaction(self, transaction, **kwargs):
                self.calls.append((transaction, kwargs))
                return Ok(page_value)

        assembly = Assembly()
        binding = self.owner.schedule.document()['source_bindings']['A']
        result = read_next_transaction(assembly, object(), binding,
                                       ok_type=Ok, page_type=type(page_value))
        self.assertIs(result, page_value)
        self.assertEqual(assembly.calls[0][1], {
            'consumer_id': binding['consumer_id'],
            'maximum_facts': MAXIMUM_FACTS,
            'maximum_bytes': MAXIMUM_BYTES})
        self.assertNotIn('cursor', assembly.calls[0][1])
        self.assertNotIn('ack', assembly.calls[0][1])

        wrong_registration = dict(binding, registration_id='other-registration')
        with self.assertRaises(R04CustodyError):
            read_next_transaction(assembly, object(), wrong_registration,
                                  ok_type=Ok, page_type=type(page_value))

    def test_semantic_schedule_is_canonical_closed_and_eight_prefixes_exist(self):
        chosen = schedule()
        self.assertEqual(len(chosen.document()['slots']), 8)
        self.assertEqual(len(chosen.document()['prefix_oracle']), 9)
        changed = chosen.document()
        changed['slots'][2]['entity_ref'] = 'unrelated'
        with self.assertRaises(R04CustodyError):
            R04Schedule.from_document(changed)

    def test_dependent_request_needs_verified_provider_identity_and_page_precedes_graph(self):
        self.complete(0)
        self.complete(1)
        with self.assertRaises(R04CustodyError):
            self.owner.freeze_request(2, request_for(self.owner.schedule.document()['slots'][2]), predecessor_ids={'entity:A': 'guessed'})
        self.owner.freeze_request(2, request_for(self.owner.schedule.document()['slots'][2]), predecessor_ids={'entity:A': 'provider-id-A'})
        self.owner.begin_source_effect(2)
        self.owner.retain_source_result(2, b'commit', b'readback')
        with self.assertRaises(R04CustodyError):
            self.owner.begin_graph_effect(2)
        self.owner.bind_page(2, page(source='A'))
        with self.assertRaises(R04CustodyError):
            self.owner.begin_graph_effect(2)
        self.owner.retain_slot_plan(2, test_slot_plan(self.owner, 2).raw)
        self.owner.begin_graph_effect(2)

    def test_exact_request_must_match_frozen_semantic_slot(self):
        with self.assertRaises(R04CustodyError):
            self.owner.freeze_request(0, b'{"slot":"A1","stage":"delete"}')

    def test_r2_result_cannot_change_r1_identity(self):
        self.complete(0)
        self.complete(1)
        selected = self.owner.schedule.document()['slots'][2]
        self.owner.freeze_request(2, request_for(selected), predecessor_ids={'entity:A': 'provider-id-A'})
        self.owner.begin_source_effect(2)
        self.policy.result_id_override = 'different-provider-id'
        with self.assertRaises(R04CustodyError):
            self.owner.retain_source_result(2, b'commit', b'readback')

    def test_global_slot_order_requires_previous_durable_ack(self):
        self.owner.freeze_request(0, request_for(self.owner.schedule.document()['slots'][0]))
        self.owner.freeze_request(1, request_for(self.owner.schedule.document()['slots'][1]))
        with self.assertRaises(R04CustodyError):
            self.owner.begin_source_effect(1)
        self.complete(0)
        self.owner.begin_source_effect(1)

    def test_begin_source_effect_renews_prior_publication_and_ack_admission(self):
        self.complete(0)
        self.owner.freeze_request(1, request_for(self.owner.schedule.document()['slots'][1]))
        self.policy.reject_prior = True
        with self.assertRaises(R04CustodyError):
            self.owner.begin_source_effect(1)

    def test_uncertain_source_effect_is_resumed_by_observation_not_resubmitted(self):
        self.owner.freeze_request(0, request_for(self.owner.schedule.document()['slots'][0]))
        self.owner.begin_source_effect(0)
        self.owner.close()
        resumed = R04Journal.open_existing(self.path, policy=self.policy, context='test')
        self.addCleanup(resumed.close)
        with self.assertRaises(R04CustodyError):
            resumed.begin_source_effect(0)
        # Recovery supplies the original public result/readback to the verifier.
        resumed.retain_source_result(0, b'original-commit', b'original-readback')
        self.assertIsNotNone(resumed.state()[0]['source_result'])

    def test_restart_rechecks_retained_page_authority_before_resuming(self):
        self.complete(0)
        self.owner.close()
        retained = R04Journal.open_existing(self.path, policy=self.policy, context='test')
        retained.close()
        self.policy.reject_retained_page = True
        with self.assertRaises(R04CustodyError):
            R04Journal.open_existing(self.path, policy=self.policy, context='test')
        self.policy.reject_retained_page = False
        self.policy.reject_retained_plan = True
        with self.assertRaises(R04CustodyError):
            R04Journal.open_existing(self.path, policy=self.policy, context='test')

    def test_page_digest_mismatch_refuses_before_graph_or_ack(self):
        self.owner.freeze_request(0, request_for(self.owner.schedule.document()['slots'][0]))
        self.owner.begin_source_effect(0)
        self.owner.retain_source_result(0, b'commit', b'readback')
        bad = page(b'changed')
        bad.sha256 = '0' * 64
        with self.assertRaises(R04CustodyError):
            self.owner.bind_page(0, bad)
        with self.assertRaises(R04CustodyError):
            self.owner.begin_graph_effect(0)

    def test_current_slot_plan_binds_request_page_vectors_and_oracle(self):
        self.complete(0)
        selected = self.owner.schedule.document()
        publication_request = publication_request_for(page(), revisions={'urn:model': '2'})
        vector = {role: {'table': f'local.graph.{role}', 'uuid': f'uuid-{role}', 'version': 0}
            for role in DATA_ROLES}
        anchors = {role: {'table': f'local.graph.{role}', 'uuid': f'uuid-{role}', 'version': 0,
            'schema_sha256': 'a' * 64, 'protocol': 'delta-protocol-v1', 'installation_id': 'ashlar-install'}
            for role in NATIVE_ROLES}
        progress = {'feed-A': prior_progress(page(source='A'), 'A'),
                    'feed-B': prior_progress(page(source='B'), 'B')}
        manifest = {'publication_id': 'genesis', 'profile_version': 'ashlar-delta/0.3',
            'table_versions_json': json.dumps({entry['table']: entry['version'] for entry in vector.values()}, sort_keys=True, separators=(',', ':')),
            'source_progress_json': json.dumps(progress, sort_keys=True, separators=(',', ':')),
            'schema_revisions_json': '{}', 'validation_report_json': '{}', 'recorded_at': '1'}
        plan = R04SlotPlan.build(self.owner.schedule, 0, self.owner.state()[0],
            source_identity=selected['source_bindings']['A'],
            publication_request=publication_request,
            predecessor_manifest=manifest,
            predecessor_vector=vector, predecessor_progress=progress,
            native_anchors=anchors,
            generated_effects=[{'statement': 'MERGE', 'parameters': {'row': '{}'}}],
            selected_effects=[{'statement': 'MERGE', 'parameters': {'row': '{}'}}], elisions=[],
            independent_prefix_oracle={'before': selected['prefix_oracle'][0],
                'after': selected['prefix_oracle'][1]}, admitted_schema_revisions={'urn:model': '2'},
            publication_id='publication-A1')
        document = plan.document()
        self.assertEqual(document['next_progress']['feed-B'], progress['feed-B'])
        self.assertEqual(document['page']['event_inventory_sha256'],
            self.owner.state()[0]['page']['page']['event_inventory_sha256'])
        self.assertEqual(document['mutation_request_sha256'],
            self.owner.state()[0]['request']['request_sha256'])
        self.assertEqual(document['publication_request_sha256'], publication_request['request_digest'])
        class EffectsTransport:
            installation_id = 'ashlar-install'
            def _target(self, statement):
                return None
            def _parameters(self, parameters):
                return None
        _, _, operations = LocalDeltaEffects(EffectsTransport()).original_plan(
            'effects:' + publication_request['request_digest'],
            publication_request['request_digest'], document['selected_effects'])
        self.assertEqual(document['operations'], operations)

    def test_current_slot_plan_refuses_cross_source_or_incomplete_native_vector(self):
        self.complete(0)
        selected = self.owner.schedule.document()
        publication_request = publication_request_for(page())
        vector = {role: {'table': f'local.graph.{role}', 'uuid': f'uuid-{role}', 'version': 0}
            for role in DATA_ROLES}
        anchors = {role: {'table': f'local.graph.{role}', 'uuid': f'uuid-{role}', 'version': 0,
            'schema_sha256': 'a' * 64, 'protocol': 'delta-protocol-v1', 'installation_id': 'ashlar-install'}
            for role in NATIVE_ROLES}
        common = dict(publication_request=publication_request,
            predecessor_manifest={'publication_id': 'genesis'}, predecessor_vector=vector,
            predecessor_progress={'feed-A': prior_progress(page(source='A'), 'A'),
                'feed-B': prior_progress(page(source='B'), 'B')},
            native_anchors=anchors, generated_effects=[], selected_effects=[], elisions=[],
            independent_prefix_oracle={'before': selected['prefix_oracle'][0],
                'after': selected['prefix_oracle'][1]}, admitted_schema_revisions={},
            publication_id='publication-A1')
        with self.assertRaises(R04CustodyError):
            R04SlotPlan.build(self.owner.schedule, 0, self.owner.state()[0],
                source_identity=selected['source_bindings']['B'], **common)
        del anchors['manifest']
        with self.assertRaises(R04CustodyError):
            R04SlotPlan.build(self.owner.schedule, 0, self.owner.state()[0],
                source_identity=selected['source_bindings']['A'], **common)

    def test_coordinator_refuses_slot_switch_before_prior_source_ack(self):
        class Admissions:
            def admit_source_identity(self, source, identity):
                return None
            def admit_current_slot(self, schedule, ordinal, source, identity, state):
                return None
            def admit_completed_slot(self, ordinal, source, identity, state,
                                     predecessor_manifest, predecessor_vector,
                                     predecessor_progress, native_anchors):
                return None
        coordinator = R04SlotCoordinator(self.owner, Admissions())
        identity = self.owner.schedule.document()['source_bindings']['B']
        with self.assertRaises(R04CustodyError):
            coordinator.prepare(1, source_identity=identity)

    def test_coordinator_reuses_exact_retained_plan_after_restart(self):
        selected = self.owner.schedule.document()
        self.owner.freeze_request(0, request_for(selected['slots'][0]))
        self.owner.begin_source_effect(0)
        self.owner.retain_source_result(0, b'commit', b'readback')
        self.owner.bind_page(0, page(source='A'))
        original = test_slot_plan(self.owner, 0)
        self.owner.retain_slot_plan(0, original.raw)
        plan = original.document()
        bindings = {key: plan[key] for key in ('source_identity', 'publication_request',
            'predecessor_manifest', 'predecessor_vector', 'predecessor_progress',
            'native_anchors', 'generated_effects', 'selected_effects',
            'independent_prefix_oracle', 'admitted_schema_revisions', 'publication_id')}
        bindings['elisions'] = plan['zero_match_elisions']
        before = self.owner.connection.execute('SELECT COUNT(*) FROM event').fetchone()[0]
        self.owner.close()
        resumed = R04Journal.open_existing(self.path, policy=self.policy, context='test')
        self.addCleanup(resumed.close)

        class Admissions:
            def admit_source_identity(self, source, identity):
                return None
            def admit_current_slot(self, schedule, ordinal, source, identity, state):
                return {}
            def admit_completed_slot(self, *args):
                raise AssertionError('slot zero has no preceding slot')

        again = R04SlotCoordinator(resumed, Admissions()).prepare(0, **bindings)
        self.assertEqual(again.raw, original.raw)
        self.assertEqual(resumed.connection.execute('SELECT COUNT(*) FROM event').fetchone()[0], before)

    def test_coordinator_refuses_b1_with_crossed_predecessor_head(self):
        self.complete(0)
        selected = self.owner.schedule.document()
        vector = {role: {'table': f'local.graph.{role}', 'uuid': f'uuid-{role}', 'version': 1}
            for role in DATA_ROLES}
        anchors = {role: {'table': f'local.graph.{role}', 'uuid': f'uuid-{role}', 'version': 1,
            'schema_sha256': 'a' * 64, 'protocol': 'delta-protocol-v1',
            'installation_id': 'ashlar-install'} for role in NATIVE_ROLES}
        progress = {'feed-A': prior_progress(page(source='A'), 'A'),
                    'feed-B': prior_progress(page(source='B'), 'B')}
        manifest = {'publication_id': 'wrong-genesis', 'profile_version': 'ashlar-delta/0.3',
            'table_versions_json': json.dumps({entry['table']: entry['version'] for entry in vector.values()}, sort_keys=True, separators=(',', ':')),
            'source_progress_json': json.dumps(progress, sort_keys=True, separators=(',', ':')),
            'schema_revisions_json': '{}', 'validation_report_json': '{}', 'recorded_at': '2'}

        class Admissions:
            def admit_source_identity(self, source, identity):
                return None
            def admit_current_slot(self, schedule, ordinal, source, identity, state):
                return {}
            def admit_completed_slot(self, ordinal, source, identity, state):
                return {'manifest': {'publication_id': 'actual-A1'},
                    'vector': vector, 'progress': progress}

        with self.assertRaises(R04CustodyError):
            R04SlotCoordinator(self.owner, Admissions()).prepare(1,
                source_identity=selected['source_bindings']['B'],
                predecessor_manifest=manifest, predecessor_vector=vector,
                predecessor_progress=progress, native_anchors=anchors,
                admitted_schema_revisions={})


if __name__ == '__main__':
    unittest.main()
