# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fail-closed checks for managed A1 pure trial-plan composition."""
import unittest
import base64
import hashlib
import json
from dataclasses import replace

from types import SimpleNamespace
from unittest.mock import patch

from ashlar.truss_native_feed import native_feed_batch
from ashlar_host.managed_current_a1_trial_plan import (
    prepare_managed_current_a1_trial,
)
from ashlar_host.managed_current_genesis import ManagedCurrentGenesisPredecessor
from ashlar_host.managed_current_prefix_delivery import ManagedCurrentPrefixPage
from ashlar_host.managed_current_publication import _managed_current_a1_task_inputs
from ashlar_host.managed_current_spark_source_oracle import (
    build_managed_current_a1_source_oracle,
    managed_current_a1_lookup_keys,
)
from ashlar_host.managed_current_truss052_genesis_owner import (
    ManagedCurrentTruss052GenesisSelection,
)
from ashlar_host.managed_current_truss053_genesis_owner import (
    ManagedCurrentTruss053GenesisSelection,
)
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.r04_schedule import R04Schedule, PROFILE, SLOT_ORDER, STAGES


class TestManagedCurrentA1TrialPlan(unittest.TestCase):
    @staticmethod
    def _schedule():
        slots = []
        for n, (name, stage) in enumerate(zip(SLOT_ORDER, STAGES)):
            source = name[0]
            slots.append({'ordinal': n, 'slot': name, 'source': source,
                'stage': stage, 'entity_ref': f'entity:{source}',
                'predecessor_slot': n - 2 if stage in ('update', 'delete') else None,
                'mutation': {'stage': stage}, 'expected_transition': {'prefix': n + 1}})
        return R04Schedule.from_document({'profile': PROFILE,
            'source_bindings': {source: {'feed': f'feed-{source}', 'epoch': f'epoch-{source}',
                'consumer_id': f'consumer-{source}', 'registration_id': f'consumer-reg-{source}',
                'installation_id': f'install-{source}'} for source in ('A', 'B')},
            'accepted_model_pins': {source: {'documentRevision': f'model-{source}',
                'modelSha256': hashlib.sha256(f'model-{source}'.encode()).hexdigest()}
                for source in ('A', 'B')},
            'slots': slots, 'prefix_oracle': [{'prefix': n} for n in range(9)]})

    @staticmethod
    def _truss_batch():
        prior = json.dumps({'profile': 'truss-caller-asserted-feed/0.1',
            'nextXid': '0', 'installationId': 'install-A', 'sourceEpoch': 'epoch-A',
            'consumerId': 'consumer-A', 'registrationId': 'consumer-reg-A'},
            separators=(',', ':')).encode()
        after = json.dumps({'profile': 'truss-caller-asserted-feed/0.1',
            'nextXid': '2', 'installationId': 'install-A', 'sourceEpoch': 'epoch-A',
            'consumerId': 'consumer-A', 'registrationId': 'consumer-reg-A'},
            separators=(',', ':')).encode()
        payload = {'interfaceVersion': 'truss-native-feed/0.1',
            'prior': json.loads(prior), 'next': json.loads(after), 'writerXid': '1',
            'catalogPrerequisites': [], 'revisions': [], 'changes': [], 'sources': [],
            'reservations': [], 'configuration': {}}
        content = json.dumps(payload, separators=(',', ':')).encode()
        return native_feed_batch(content, sha256=hashlib.sha256(content).hexdigest(),
            prior=prior, next=after, feed='feed-A')

    def test_composes_real_native_feed_batch_and_original_public_page(self):
        schedule = self._schedule()
        batch = self._truss_batch()
        page = SimpleNamespace(content=batch.begin,
            prior=SimpleNamespace(content=base64.b64decode(batch.cursor_before)),
            next=SimpleNamespace(content=base64.b64decode(batch.cursor_after)))
        source_a = SimpleNamespace(batch=batch, page=page)
        source_b = SimpleNamespace(batch=None, page=None)
        admission = SimpleNamespace(batch=batch)
        manifest = {'publication_id': 'genesis-manifest'}
        predecessor = ManagedCurrentGenesisPredecessor(manifest, {}, {})
        for selection_type in (ManagedCurrentTruss052GenesisSelection,
                               ManagedCurrentTruss053GenesisSelection):
            with self.subTest(selection_type=selection_type.__name__):
                selection = selection_type(
                    schedule.sha256, schedule.raw, {}, {}, {},
                    {'install-A': '1', 'install-B': '1'},
                    {'A': source_a, 'B': source_b}, admission, batch)
                with patch('ashlar_host.managed_current_genesis.verify_managed_current_genesis_predecessor',
                           return_value=predecessor):
                    plan = prepare_managed_current_a1_trial(selection, manifest,
                        stream='source-stream', materialized_at='1762000000000000', context=object())
                self.assertEqual(plan.delivery_page.page_bytes, batch.begin)
                self.assertEqual(plan.delivery_page.prior_cursor_bytes,
                                 base64.b64decode(batch.cursor_before))
                self.assertEqual(plan.delivery_page.next_cursor_bytes,
                                 base64.b64decode(batch.cursor_after))
                self.assertIs(plan.driver_inputs['source_owner_admitted_batch'], batch)
                self.assertEqual(plan.request['batch_id'], batch.batch_id)
                self.assertEqual(plan.request['predecessor'], manifest['publication_id'])
                self.assertIs(plan.selection, selection)

    def test_delivery_accepts_reloaded_exact_page_and_refuses_changed_bytes(self):
        schedule = self._schedule()
        batch = self._truss_batch()
        page = SimpleNamespace(content=batch.begin,
            prior=SimpleNamespace(content=base64.b64decode(batch.cursor_before)),
            next=SimpleNamespace(content=base64.b64decode(batch.cursor_after)))
        selection = ManagedCurrentTruss052GenesisSelection(
            schedule.sha256, schedule.raw, {}, {}, {}, {},
            {'A': SimpleNamespace(batch=batch, page=page),
             'B': SimpleNamespace(batch=None)}, SimpleNamespace(batch=batch), batch)
        manifest = {'publication_id': 'genesis-manifest'}
        predecessor = ManagedCurrentGenesisPredecessor(manifest, {}, {})
        with patch('ashlar_host.managed_current_genesis.verify_managed_current_genesis_predecessor',
                   return_value=predecessor):
            plan = prepare_managed_current_a1_trial(selection, manifest,
                stream='source-stream', materialized_at='1', context=object())
        clone = ManagedCurrentPrefixPage(**plan.delivery_page.__dict__)
        with patch('ashlar_host.managed_current_prefix_delivery.validate_managed_current_prefix_delivery') as validate:
            marker = object()
            validate.return_value = marker
            self.assertIs(plan.verify_delivery(b'prefix', prefix_sha256='0' * 64,
                selected={}, schedule_bytes=schedule.raw, pages=(clone,)), marker)
            with self.assertRaisesRegex(ValueError, 'selected original A page'):
                changed = replace(clone, page_bytes=clone.page_bytes + b' ')
                plan.verify_delivery(b'prefix', prefix_sha256='0' * 64, selected={},
                                     schedule_bytes=schedule.raw, pages=(changed,))
            self.assertEqual(validate.call_count, 1)

    def test_requires_same_owner_admitted_batch_before_manifest_or_request(self):
        batch = object()
        selection = ManagedCurrentTruss052GenesisSelection(
            "0" * 64, b"{}", {}, {}, {}, {}, {},
            SimpleNamespace(batch=object()), batch)
        with self.assertRaisesRegex(ValueError, "exact source-owner admitted"):
            prepare_managed_current_a1_trial(
                selection, {}, stream="source-stream", materialized_at="1",
                context=object())

    def test_requires_both_original_observations_and_only_a_page_batch(self):
        batch = object()
        observations = {
            "A": SimpleNamespace(batch=batch),
            "B": SimpleNamespace(batch=batch),
        }
        selection = ManagedCurrentTruss052GenesisSelection(
            "0" * 64, b"{}", {}, {}, {}, {}, observations,
            SimpleNamespace(batch=batch), batch)
        with self.assertRaisesRegex(ValueError, "Complete original A/B observations"):
            prepare_managed_current_a1_trial(
                selection, {}, stream="source-stream", materialized_at="1",
                context=object())

    def test_managed_task_inputs_recompose_exact_owner_batch_and_genesis_plan(self):
        schedule = self._schedule()
        batch = self._truss_batch()
        page = SimpleNamespace(content=batch.begin,
            prior=SimpleNamespace(content=base64.b64decode(batch.cursor_before)),
            next=SimpleNamespace(content=base64.b64decode(batch.cursor_after)))
        admission = TrussGraphAdmission.__new__(TrussGraphAdmission)
        admission.batch = batch
        selection = ManagedCurrentTruss052GenesisSelection(
            schedule.sha256, schedule.raw, {}, {}, {}, {},
            {'A': SimpleNamespace(batch=batch, page=page),
             'B': SimpleNamespace(batch=None)}, admission, batch)
        manifest = {'publication_id': 'genesis-manifest'}
        predecessor = ManagedCurrentGenesisPredecessor(manifest, {}, {})
        context = object()
        with patch('ashlar_host.managed_current_genesis.verify_managed_current_genesis_predecessor',
                   return_value=predecessor):
            plan = prepare_managed_current_a1_trial(selection, manifest,
                stream='source-stream', materialized_at='1762000000000000', context=context)
            selected_admission, selected_predecessor, selected_batch, request = (
                _managed_current_a1_task_inputs(plan, stream='source-stream',
                    predecessor='genesis-manifest', schema_revisions_json='{}',
                    materialized_at='1762000000000000'))
            self.assertIs(selected_admission, admission)
            self.assertIs(selected_predecessor, predecessor)
            self.assertIs(selected_batch, batch)
            self.assertEqual(request, plan.request)
            with self.assertRaisesRegex(ValueError, 'request differs'):
                _managed_current_a1_task_inputs(plan, stream='other-stream',
                    predecessor='genesis-manifest', schema_revisions_json='{}',
                    materialized_at='1762000000000000')
            plan.driver_inputs['source_owner_admitted_batch'] = object()
            with self.assertRaisesRegex(ValueError, 'exact source-owner admission'):
                _managed_current_a1_task_inputs(plan, stream='source-stream',
                    predecessor='genesis-manifest', schema_revisions_json='{}',
                    materialized_at='1762000000000000')

    def test_a1_native_oracle_binds_the_owner_page_and_complete_progress(self):
        from ashlar_host.managed_current_core import (
            CARRIER_PROFILE_XXHASH64, ManagedCurrentCoreConfig,
        )

        schedule = self._schedule()
        batch = self._truss_batch()
        page = SimpleNamespace(content=batch.begin,
            prior=SimpleNamespace(content=base64.b64decode(batch.cursor_before)),
            next=SimpleNamespace(content=base64.b64decode(batch.cursor_after)))
        admission = TrussGraphAdmission.__new__(TrussGraphAdmission)
        admission.batch = batch
        admission.changes = ()
        admission.parsed = SimpleNamespace(event_bytes=(), edge_order_keys=())
        admission.admit = lambda _change: None
        admission.admit_transition = lambda _previous, _change: None
        selection = ManagedCurrentTruss052GenesisSelection(
            schedule.sha256, schedule.raw, {}, {}, {}, {'catalog-rev': '1'},
            {'A': SimpleNamespace(batch=batch, page=page),
             'B': SimpleNamespace(batch=None)}, admission, batch)
        genesis = {'publication_id': 'genesis-manifest'}
        predecessor = ManagedCurrentGenesisPredecessor(genesis, {}, {})
        with patch('ashlar_host.managed_current_genesis.verify_managed_current_genesis_predecessor',
                   return_value=predecessor):
            plan = prepare_managed_current_a1_trial(selection, genesis,
                stream='source-stream', materialized_at='1762000000000000', context=object())

        config = ManagedCurrentCoreConfig('client_dev', 'trial_schema', None, None, 0, 100,
            carrier_profile=CARRIER_PROFILE_XXHASH64)
        progress = {'feed-A': {'profile': 'page-checkpoint'},
                    'feed-B': {'profile': 'genesis-marker'}}
        manifest = {'publication_id': 'a1-manifest',
            'profile_version': CARRIER_PROFILE_XXHASH64,
            'source_progress_json': json.dumps(progress, sort_keys=True, separators=(',', ':')),
            'schema_revisions_json': json.dumps(selection.schema_revisions, sort_keys=True,
                                                separators=(',', ':'))}
        prefix = {'entries': [{'requestJson': json.dumps(plan.request, sort_keys=True,
                            separators=(',', ':')),
                    'publicationManifestJson': json.dumps(manifest, sort_keys=True,
                            separators=(',', ':')),
                    'materializationClockUs': '1762000000000000'}]}
        prefix_bytes = json.dumps(prefix, sort_keys=True, separators=(',', ':')).encode()
        prefix_sha = hashlib.sha256(prefix_bytes).hexdigest()
        selected = {'destination': {'catalog': 'client_dev', 'schema': 'trial_schema'}}
        correspondence = SimpleNamespace(prefix_sha256=prefix_sha,
                                         schedule_sha256=schedule.sha256)
        with patch('ashlar_host.managed_current_prefix_delivery.validate_managed_current_prefix_delivery',
                   return_value=correspondence), \
             patch('ashlar.manifest.validate_manifest_row', return_value=None), \
             patch('ashlar_host.managed_current_genesis.transition_managed_current_first_a_page',
                   return_value=progress):
            keys = managed_current_a1_lookup_keys(plan, config=config)
            oracle = build_managed_current_a1_source_oracle(
                plan, config=config, prefix_bytes=prefix_bytes, prefix_sha256=prefix_sha,
                selected=selected, manifest=manifest, lookup_hashes={})

        self.assertEqual(keys, ())
        self.assertEqual(oracle.profile, 'ashlar-managed-current-a1-source-oracle/0.1')
        self.assertEqual(oracle.prefix_sha256, prefix_sha)
        self.assertEqual(oracle.final_request, plan.request)
        self.assertEqual(oracle.progress, progress)
        self.assertEqual(oracle.expected_rows,
                         {role: [] for role in ('object_current', 'edge_current',
                             'tombstone', 'whole_source_history')})

    def test_truss052_reader_refuses_missing_or_partial_a1_pins_and_oracle_bypass(self):
        from ashlar_host.managed_current_core import CARRIER_PROFILE_XXHASH64
        from ashlar_host.managed_current_spark_receipt_read import (
            ManagedCurrentSparkReceiptReader,
        )

        reader = ManagedCurrentSparkReceiptReader.__new__(ManagedCurrentSparkReceiptReader)
        reader.config = SimpleNamespace(source_truss_version='0.0.52',
                                        carrier_profile=CARRIER_PROFILE_XXHASH64)
        cases = (
            {},
            {'a1_plan': object()},
            {'a1_prefix_selected': {}},
            {'source_prefix_bytes': b'prefix', 'source_prefix_sha256': '0' * 64,
             'source_core_config': object()},
            {'source_oracle': object()},
        )
        for pins in cases:
            with self.subTest(pins=tuple(pins)), self.assertRaisesRegex(
                    ValueError, 'Truss .52 A1 reads require'):
                reader.reopen(request_bytes=b'{}', manifest_bytes=b'{}',
                              expected_receipt={}, **pins)


if __name__ == "__main__":
    unittest.main()
