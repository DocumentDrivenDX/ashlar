"""Original semantic union/state controls; no public producer or native authority claim."""
from dataclasses import asdict, replace
import base64
import hashlib
import json
import unittest
from unittest.mock import patch
from ashlar.apply import Change, EntityKey, EntityState, empty_state
from ashlar.commerce_evolution import prepare, qualify
from ashlar.whole_entity import changes_from_batch
from ashlar_host.config import HostError
from ashlar_host.evolution_admission import (EvolutionAdmission, EvolutionSourceSet,
    packaged_inputs, expand_receipt, original_transitions, digest, REVISION)

CLOCK_A = ('2026-10-09T12:00:00+00:00', '2026-10-09T12:01:00+00:00',
           '2026-10-09T12:02:00+00:00', '2026-10-09T12:03:00+00:00')
CLOCK_B = ('2026-10-10T13:00:00+00:00', '2026-10-10T13:01:00+00:00',
           '2026-10-10T13:02:00+00:00', '2026-10-10T13:03:00+00:00')


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw = dict(packaged_inputs()); cls.proof = expand_receipt(cls.raw['public-presence.json.gz'], 4194304)
        closure = json.loads(cls.raw['source-closure.json'])
        cls.custody = json.dumps({'revision': REVISION,
            'source_closure_sha256': digest(cls.raw['source-closure.json']),
            'files': len(closure['files']), 'bytes': sum(item['bytes'] for item in closure['files']),
            'runtime_scope': closure['scope']}, sort_keys=True, separators=(',', ':')).encode()
        cls.a = cls.admission('A', 'shared-epoch'); cls.b = cls.admission('B', 'shared-epoch')
        cls.sources = EvolutionSourceSet((cls.a, cls.b))

    @classmethod
    def admission(cls, source, epoch):
        original = prepare(cls.raw['candidate.json'], cls.proof, cls.raw['original-model.json'], source_system=source, epoch=epoch)
        prepared = qualify(original)
        return EvolutionAdmission(prepared, cls.proof, cls.custody,
            tuple(change for batch in prepared.batches for change in changes_from_batch(batch)),
            original_transitions(cls.raw, original, prepared))

    def test_overlapping_local_entities_deliveries_and_epochs_remain_independent(self):
        state = self.sources.state_at({'A': 4, 'B': 4})
        self.assertEqual((len(state.current), len(state.history), len(state.tombstones)), (42, 90, 4))
        self.assertEqual({key.source for key in state.current}, {'A', 'B'})
        local_a = {(key.kind, key.type_id, key.id) for key in state.current if key.source == 'A'}
        local_b = {(key.kind, key.type_id, key.id) for key in state.current if key.source == 'B'}
        self.assertEqual(local_a, local_b)
        self.assertEqual({key[0] for key in state.deliveries}, {'A', 'B'})
        self.sources.admit(self.a.changes[0]); self.sources.admit_transition(*self.a.transitions[0])
        wrong = replace(self.a.changes[0], epoch='substituted')
        with self.assertRaises(HostError): self.sources.admit(wrong)
        previous, change = self.a.transitions[0]
        with self.assertRaises(HostError): self.sources.admit_transition(replace(previous, version=999), change)

    def test_publicly_constructible_admission_rechecked_against_originals(self):
        for admitted in (replace(self.a, receipt_bytes=b'{}'), replace(self.a, custody_bytes=b'{}'),
                         replace(self.a, changes=self.a.changes[:-1]), replace(self.a, transitions=()),
                         replace(self.a, prepared=replace(self.a.prepared, registry_json='{}')),
                         replace(self.a, prepared=replace(self.a.prepared, batches=self.a.prepared.batches[::-1]))):
            with self.subTest(admitted=type(admitted)):
                with self.assertRaises(HostError): EvolutionSourceSet((admitted,))
        with self.assertRaises(HostError): EvolutionSourceSet((object(),))
        with self.assertRaises(HostError): EvolutionSourceSet([self.a])
        with self.assertRaises(HostError): EvolutionSourceSet((self.a, self.a))
        other_epoch = self.admission('A', 'other-epoch')
        with self.assertRaises(HostError): EvolutionSourceSet((self.a, other_epoch))

    def test_zero_prefix_has_no_rows_or_progress_and_still_validates_clocks(self):
        with patch('ashlar_host.evolution_admission.scoped_oracle', side_effect=AssertionError('zero prefix oracle')):
            rows = self.sources.oracle({'A': 0, 'B': 0}, materialized_at={'A': CLOCK_A, 'B': CLOCK_B})
        self.assertEqual(set(rows), {'object_current', 'edge_current', 'tombstone', 'whole_source_history'})
        self.assertTrue(all(value == [] for value in rows.values()))
        self.assertEqual(self.sources.state_at({'A': 0, 'B': 0}), empty_state())
        for clocks in ({'A': CLOCK_A}, {'A': CLOCK_A, 'B': ('invalid', *CLOCK_B[1:])}, {'A': CLOCK_A, 'B': ('2026-10-10T13:00:00', *CLOCK_B[1:])}):
            with self.assertRaises(HostError): self.sources.oracle({'A': 0, 'B': 0}, materialized_at=clocks)

    def test_source_set_owns_reconstructed_admissions_and_rejects_executable_equality(self):
        borrowed = self.admission('isolated', 'epoch')
        owner = EvolutionSourceSet((borrowed,))
        self.assertIsNot(owner.admissions[0], borrowed)
        self.assertIsNot(owner.admissions[0].prepared, borrowed.prepared)
        object.__setattr__(borrowed.prepared, 'batches', ())
        self.assertEqual(len(owner.admissions[0].prepared.batches), 4)
        self.assertEqual(len(owner.state_at({'isolated': 4}).history), 45)
        class PretendingTuple(tuple):
            def __eq__(self, other): return True
        forged = replace(self.a, prepared=replace(self.a.prepared, batches=PretendingTuple(())))
        with self.assertRaises(HostError): EvolutionSourceSet((forged,))
        forged = replace(self.a, changes=PretendingTuple(()))
        with self.assertRaises(HostError): EvolutionSourceSet((forged,))

    def test_custody_requires_original_canonical_bytes(self):
        altered = json.loads(self.custody); altered['files'] = float(altered['files'])
        alternate = json.dumps(altered, sort_keys=True, separators=(',', ':')).encode()
        duplicate = self.custody[:-1] + b',"revision":"' + REVISION.encode() + b'"}'
        for raw in (alternate, duplicate, self.custody + b' '):
            with self.assertRaises(HostError): EvolutionSourceSet((replace(self.a, custody_bytes=raw),))

    def test_public_admission_does_not_invoke_caller_equality(self):
        class Forged:
            def __eq__(self, other): raise AssertionError('caller equality invoked')
        class ChangeSubclass(Change): pass
        class StateSubclass(EntityState): pass
        change = self.a.changes[0]
        supplied = ChangeSubclass(change.feed, change.epoch, change.delivery_id, change.raw_digest,
            change.operation, change.state)
        for value in (Forged(), supplied):
            with self.assertRaises(HostError): self.sources.admit(value)
        previous, change = self.a.transitions[0]
        supplied = StateSubclass(previous.key, previous.version, previous.schema_revision,
            previous.props_json, previous.retained_json, previous.endpoints)
        for value in (Forged(), supplied):
            with self.assertRaises(HostError): self.sources.admit_transition(value, change)

    def test_complete_prefix_and_clock_inventories_and_bounds(self):
        for prefixes in ({'A': 1}, {'A': 1, 'B': 1, 'C': 0}, {'A': True, 'B': 0}, {'A': -1, 'B': 0}, {'A': 5, 'B': 0}):
            with self.assertRaises(HostError): self.sources.state_at(prefixes)
        for clock in (False, CLOCK_A[0] + 'x' * 65, '2026-10-09T12:00:00+01:00'):
            with self.assertRaises(HostError): self.sources.oracle({'A': 1, 'B': 1}, materialized_at={'A': (clock, *CLOCK_A[1:]), 'B': CLOCK_B})
        for sequence in (list(CLOCK_A), CLOCK_A[:3], (*CLOCK_A, CLOCK_A[0]), (*CLOCK_A[:3], 'invalid')):
            with self.assertRaises(HostError): self.sources.oracle({'A': 0, 'B': 0}, materialized_at={'A': sequence, 'B': CLOCK_B})

    def test_unchanged_rows_preserve_first_clock_through_update_and_delete(self):
        rows = self.sources.oracle({'A': 1, 'B': 0}, materialized_at={'A': CLOCK_A, 'B': CLOCK_B})
        def inventory(rows):
            return {(row['source_system'], role, row.get('type_id', row.get('rel_type_id')), row['id']): row
                for role in ('object_current', 'edge_current') for row in rows[role]}
        first = inventory(rows)
        second = inventory(self.sources.oracle({'A': 2, 'B': 0}, materialized_at={'A': CLOCK_A, 'B': CLOCK_B}))
        third = inventory(self.sources.oracle({'A': 3, 'B': 0}, materialized_at={'A': CLOCK_A, 'B': CLOCK_B}))
        unchanged = set()
        changed = set()
        for key, row in second.items():
            if row['source_delivery_id'] == first[key]['source_delivery_id']:
                unchanged.add(key); self.assertEqual(row['published_at'], '1791547200000000')
            else:
                changed.add(key); self.assertEqual(row['published_at'], '1791547260000000')
        self.assertEqual((len(unchanged), len(changed)), (20, 1))
        self.assertEqual(len(third), 19)
        for key, row in third.items():
            self.assertEqual(row['published_at'], second[key]['published_at'])

    def test_source_inventory_keys_never_accept_caller_equality(self):
        class FakeKey:
            def __hash__(self): return hash('A')
            def __eq__(self, other): raise AssertionError('caller source equality invoked')
        wrong_prefixes = {FakeKey(): 0, 'B': 0}
        wrong_clocks = {FakeKey(): CLOCK_A, 'B': CLOCK_B}
        with self.assertRaises(HostError): self.sources.state_at(wrong_prefixes)
        with self.assertRaises(HostError): self.sources.oracle({'A': 0, 'B': 0}, materialized_at=wrong_clocks)

    def test_independent_full_union_uses_original_transaction_clock_witnesses(self):
        rows = self.sources.oracle({'A': 4, 'B': 3}, materialized_at={'A': CLOCK_A, 'B': CLOCK_B})
        # Original R1 creates21, R2 replaces1, R3 deletes2, R4 evolves19/new2.
        self.assertEqual(tuple(len(batch.records) for batch in self.a.prepared.batches), (21, 1, 2, 21))
        self.assertEqual(len(rows['whole_source_history']), (21 + 1 + 2 + 21) + (21 + 1 + 2))
        witnesses = {record.delivery_id: str(1791547200000000 + index * 60000000)
            for index, batch in enumerate(self.a.prepared.batches) for record in batch.records}
        witnesses_b = {record.delivery_id: str(1791637200000000 + index * 60000000)
            for index, batch in enumerate(self.b.prepared.batches) for record in batch.records}
        for role in ('object_current', 'edge_current'):
            for row in rows[role]:
                stamps = witnesses if row['source_system'] == 'A' else witnesses_b
                self.assertEqual(row['published_at'], stamps[row['source_delivery_id']])
                self.assertEqual(set(row), {name for name, family in self.sources.columns[role]})
        self.assertEqual({row['feed'] for row in rows['whole_source_history']}, {'A', 'B'})
        self.assertEqual({row['source_system'] for row in rows['tombstone']}, {'A', 'B'})
        self.assertNotIn('progress', rows)

    def test_returned_rows_columns_and_metadata_do_not_mutate_owned_sources(self):
        rows = self.sources.oracle({'A': 1, 'B': 0}, materialized_at={'A': CLOCK_A, 'B': CLOCK_B})
        original = json.dumps(rows, sort_keys=True)
        rows['object_current'][0]['id'] = 'changed'; rows['edge_current'].clear()
        columns = self.sources.columns; columns.clear()
        facts = self.sources.metadata(); facts['sources'][0]['epoch'] = 'changed'
        self.assertEqual(json.dumps(self.sources.oracle({'A': 1, 'B': 0}, materialized_at={'A': CLOCK_A, 'B': CLOCK_B}), sort_keys=True), original)
        self.assertEqual(self.sources.metadata()['sources'][0]['epoch'], 'shared-epoch')
        self.assertEqual(len(self.sources.columns), 4)
        self.assertIn('no fresh producer', self.sources.metadata()['qualification'])

    def test_full_original_oracle_matches_independent_core_replay(self):
        for prefixes in ({'A': 4, 'B': 3}, {'A': 1, 'B': 0}):
            with self.subTest(prefixes=prefixes):
                state = self.sources.state_at(prefixes)
                rows = self.sources.oracle(prefixes, materialized_at={'A': CLOCK_A, 'B': CLOCK_B})
                actual_current = {}
                records = {}
                for admitted in self.sources.admissions:
                    for batch in admitted.prepared.batches[:prefixes[admitted.prepared.source_system]]:
                        for record in batch.records:
                            records[(batch.feed, batch.epoch, record.delivery_id)] = (batch, record)
                for role, kind, type_name in (('object_current', 'object', 'type_id'), ('edge_current', 'edge', 'rel_type_id')):
                    for row in rows[role]:
                        key = EntityKey(row['source_system'], kind, int(row[type_name]), int(row['id']))
                        endpoints = tuple(EntityKey(key.source, 'object', int(row[end + '_type']), int(row[end + '_id']))
                            for end in ('source', 'target')) if kind == 'edge' else None
                        value = EntityState(key, int(row['entity_version']), row['schema_revision'],
                            row['props_json'], row['retained_json'], endpoints)
                        self.assertNotIn(key, actual_current); actual_current[key] = value
                        change = state.history[(key, value.version)]
                        self.assertEqual((row['source_feed'], row['source_epoch'], row['source_delivery_id']),
                            (change.feed, change.epoch, change.delivery_id))
                        batch, record = records[(change.feed, change.epoch, change.delivery_id)]
                        self.assertEqual(row['apply_batch_id'], batch.batch_id)
                        index = next(index for admitted in self.sources.admissions if admitted.prepared.source_system == change.feed
                            for index, original in enumerate(admitted.prepared.batches) if original.batch_id == batch.batch_id)
                        base_stamp = 1791547200000000 if change.feed == 'A' else 1791637200000000
                        self.assertEqual(row['published_at'], str(base_stamp + index * 60000000))
                        self.assertEqual(json.loads(row['source_cursor_json']), {'profile': batch.profile, 'offset': batch.cursor_after})
                        identity = {'source_system': key.source, type_name: key.type_id, 'id': key.id}
                        expected_hash = hashlib.sha256(json.dumps(identity, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()
                        self.assertEqual(row['lookup_hash'], expected_hash)
                        if kind == 'object': self.assertEqual(row['logical_key_json'], '[]')
                self.assertEqual(actual_current, dict(state.current))
                actual_history = {}
                for row in rows['whole_source_history']:
                    identity = (row['feed'], row['epoch'], row['delivery_id'])
                    self.assertNotIn(identity, actual_history)
                    actual_history[identity] = json.loads(row['change_json'])
                    batch, record = records[identity]
                    self.assertEqual(base64.b64decode(row['raw_base64']), record.raw)
                    self.assertEqual(row['digest'], record.sha256)
                expected_history = {(change.feed, change.epoch, change.delivery_id): json.loads(json.dumps(asdict(change)))
                    for change in state.history.values()}
                self.assertEqual(actual_history, expected_history)
                actual_tombstones = {}
                for row in rows['tombstone']:
                    key = EntityKey(row['source_system'], row['entity_kind'], int(row['type_id']), int(row['id']))
                    self.assertNotIn(key, actual_tombstones)
                    actual_tombstones[key] = (int(row['entity_version']), row['source_feed'], row['source_epoch'], row['source_delivery_id'])
                    batch, record = records[(row['source_feed'], row['source_epoch'], row['source_delivery_id'])]
                    self.assertEqual(json.loads(row['source_cursor_json']), {'profile': batch.profile, 'offset': batch.cursor_after})
                self.assertEqual(actual_tombstones, {key: (change.state.version, change.feed, change.epoch, change.delivery_id)
                    for key, change in state.tombstones.items()})


if __name__ == '__main__': unittest.main()
