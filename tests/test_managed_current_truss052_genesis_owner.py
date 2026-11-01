# SPDX-License-Identifier: MIT OR Apache-2.0
"""Focused tests for bounded managed Truss .52 source-owner selection."""
import hashlib
import unittest

from ashlar_host.managed_current_genesis import admit_truss_source_genesis_progress
from ashlar_host.managed_current_truss052_genesis_owner import (
    ManagedCurrentTruss052GenesisError,
    ManagedCurrentTruss052GenesisSelection,
    ManagedCurrentTruss052GenesisSourceOwner,
    ManagedCurrentTruss052SourceSelection,
)
from ashlar_host.r04_schedule import R04Schedule, PROFILE, SLOT_ORDER, STAGES


def _schedule():
    slots = []
    for n, (name, stage) in enumerate(zip(SLOT_ORDER, STAGES)):
        source = name[0]
        slots.append({'ordinal': n, 'slot': name, 'source': source, 'stage': stage,
            'entity_ref': f'entity:{source}',
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


def _selection(source, factory):
    return ManagedCurrentTruss052SourceSelection(
        source=source, feed=f'feed-{source}', epoch=f'epoch-{source}',
        installation_id=f'install-{source}', consumer_id=f'consumer-{source}',
        consumer_registration_id=f'consumer-reg-{source}',
        catalog_registration_id=f'catalog-reg-{source}', catalog_revision='1',
        connection_factory=factory)


class ManagedTruss052GenesisOwnerTests(unittest.TestCase):
    def test_binding_mismatch_refuses_before_connecting(self):
        calls = []
        factory = lambda: calls.append('connected')
        owner = ManagedCurrentTruss052GenesisSourceOwner(_schedule(), {
            'A': ManagedCurrentTruss052SourceSelection(
                source='A', feed='wrong-feed', epoch='epoch-A',
                installation_id='install-A', consumer_id='consumer-A',
                consumer_registration_id='consumer-reg-A',
                catalog_registration_id='catalog-reg-A', catalog_revision='1',
                connection_factory=factory),
            'B': _selection('B', factory),
        })
        with self.assertRaisesRegex(ManagedCurrentTruss052GenesisError,
                                    'frozen R04 binding'):
            owner.admit_a1()
        self.assertEqual(calls, [])

    def test_manifest_arguments_preserve_original_schedule_bytes(self):
        schedule = _schedule()
        progress = {source: admit_truss_source_genesis_progress(
            feed=f'feed-{source}', epoch=f'epoch-{source}',
            cursor_bytes=(f'{{"profile":"truss-caller-asserted-feed/0.1",'
                f'"nextXid":"0","installationId":"install-{source}",'
                f'"sourceEpoch":"epoch-{source}","consumerId":"consumer-{source}",'
                f'"registrationId":"consumer-reg-{source}"}}').encode(),
            cursor_sha256=hashlib.sha256((f'{{"profile":"truss-caller-asserted-feed/0.1",'
                f'"nextXid":"0","installationId":"install-{source}",'
                f'"sourceEpoch":"epoch-{source}","consumerId":"consumer-{source}",'
                f'"registrationId":"consumer-reg-{source}"}}').encode()).hexdigest(),
            catalog_registration_id=f'catalog-{source}',
            catalog_registration_bytes=f'registration-{source}'.encode(),
            consumer_id=f'consumer-{source}',
            consumer_registration_id=f'consumer-reg-{source}',
            installation_id=f'install-{source}') for source in ('A', 'B')}
        selected = ManagedCurrentTruss052GenesisSelection(
            schedule.sha256, schedule.raw, progress, {}, {}, {}, {}, object(), object())
        args = ManagedCurrentTruss052GenesisSourceOwner.manifest_owner_arguments(
            selected, recorded_at='1762000000000000', context=object())
        self.assertIs(args['schedule_bytes'], schedule.raw)
        self.assertIs(args['source_progress'], progress)


if __name__ == '__main__':
    unittest.main()
