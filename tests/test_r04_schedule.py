"""Focused custody invariants; fake ports here are tests, not release evidence."""
import hashlib
import base64
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace

from ashlar_host.r04_schedule import (PROFILE, R04CustodyError, R04Journal,
    R04Schedule, R04SchedulePolicy, SLOT_ORDER, STAGES, canonical)


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
        'source_bindings': {'A': {'test_only': 'source-A'}, 'B': {'test_only': 'source-B'}},
        'accepted_model_pins': {'A': {'test_only': 'model-A'}, 'B': {'test_only': 'model-B'}},
        'slots': slots, 'prefix_oracle': [{'prefix': n, 'test_only': True} for n in range(9)]})


class TestOnlyPolicy(R04SchedulePolicy):
    reject_retained_page = False
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

    def admit_before_graph_effect(self, selected, ordinal, request, result, page_record, context):
        return None

    def admit_before_ack_effect(self, selected, ordinal, publication, context):
        return None

    def admit_publication(self, selected, ordinal, page, publication, context):
        return None

    def admit_ack(self, selected, ordinal, publication, ack, context):
        return None


def page(content=None):
    event = b'event-bytes'
    prior = canonical({'consumerId': 'ordinary-consumer', 'registrationId': 'source-registration', 'nextXid': '1'})
    following = canonical({'consumerId': 'ordinary-consumer', 'registrationId': 'source-registration', 'nextXid': '2'})
    if content is None:
        content = canonical({'prior': __import__('json').loads(prior), 'next': __import__('json').loads(following),
            'writerXid': 'writer-1', 'changes': [{'new_value': {
            'component': 'event', 'bytesBase64': base64.b64encode(event).decode(),
            'bytesSha256': hashlib.sha256(event).hexdigest()}}]})
    return SimpleNamespace(content=content, sha256=hashlib.sha256(content).hexdigest(),
        prior=SimpleNamespace(content=prior), next=SimpleNamespace(content=following),
        consumer_id='ordinary-consumer', registration_id='source-registration',
        writer_xid='writer-1', outcome='transaction')


def request_for(selected):
    return canonical({'slot': selected['slot'], 'stage': selected['stage'],
                      'value': selected['mutation']['value']})


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
        self.owner.bind_page(ordinal, page())
        self.owner.begin_graph_effect(ordinal)
        self.owner.retain_publication(ordinal, {'full_vector': f'vector-{ordinal}', 'progress': ordinal + 1})
        self.owner.begin_ack_effect(ordinal)
        self.owner.retain_ack(ordinal, {'source': selected['source'], 'durable': True})
        return {entity: f'provider-id-{selected["source"]}'}

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
        self.owner.bind_page(2, page())
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


if __name__ == '__main__':
    unittest.main()
