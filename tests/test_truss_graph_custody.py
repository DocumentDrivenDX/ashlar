# SPDX-License-Identifier: MIT OR Apache-2.0
"""Structural immutable Truss capture correspondence; no native execution."""
import hashlib
import json
from pathlib import Path
import unittest
import test_finite_graph_export
from ashlar.graph_release import GraphRelease
from ashlar_host.delta_custody import encoded
from ashlar.pins import PinVector
from ashlar_host.truss_graph_admission import PROFILE as SOURCE_PROFILE
from ashlar_host.truss_graph_admission import CURRENT_052_CANDIDATE_PROFILE, CURRENT_053_FRESH_PROFILE
from ashlar_host.truss_current_catalog import (
    CURRENT_TRUSS_053_FRESH_WHEEL_SHA256,
    CURRENT_TRUSS_053_FRESH_RUNTIME_MANIFEST_SHA256,
    CURRENT_TRUSS_054_FRESH_WHEEL_SHA256,
    CURRENT_TRUSS_054_FRESH_RUNTIME_MANIFEST_SHA256,
    CURRENT_TRUSS_055_FRESH_WHEEL_SHA256,
    CURRENT_TRUSS_055_FRESH_RUNTIME_MANIFEST_SHA256)
from ashlar_host.truss_graph_custody import (
    PROFILE, admit_truss_graph_custody, build_truss_graph_custody,
)
from ashlar_host.private_graph_custody import load_private_graph_release


class Tests(unittest.TestCase):
    def test_roundtrip_and_checkpoint_mismatch_refusal(self):
        original, _ = test_finite_graph_export.Tests().pair()
        value = json.loads(original.payload)
        checkpoint = {'profile': 'truss-native-feed/0.1', 'feed': 'truss-fixture', 'epoch': 'epoch',
            'previous': 'cHJldmlvdXM=', 'position': 'bmV4dA==', 'payload_digest': 'a'*64, 'batch_id': '123'}
        source = {'profile': SOURCE_PROFILE, 'source_profile': checkpoint['profile'], 'feed': checkpoint['feed'],
            'epoch': checkpoint['epoch'], 'source_transaction_sha256': checkpoint['payload_digest'],
            'qualification': 'Structural fixture only'}
        value['publication']['source_progress_json'] = encoded({source['feed']: checkpoint})
        value['publication']['validation_report_json'] = encoded({'complete': True, 'source_admission': source})
        raw = (encoded(value)+'\n').encode()
        release = GraphRelease(raw, hashlib.sha256(raw).hexdigest())
        vector = PinVector('test-host', 'manifest', value['publication']['publication_id'],
                           'a' * 64, {table: (snapshot['uuid'], snapshot['version'])
                                      for table, snapshot in value['snapshots'].items()})
        receipt = build_truss_graph_custody(release, source=source, vector=vector, checkpoint=checkpoint)
        loaded = load_private_graph_release(raw, release.sha256, receipt, hashlib.sha256(receipt).hexdigest())
        self.assertEqual(loaded, value)
        self.assertEqual(json.loads(receipt)['format'], PROFILE)
        self.assertNotIn('ack', json.loads(receipt))
        changed = json.loads(receipt); changed['checkpoint']['position'] = 'Y2hhbmdlZA=='
        receipt = (encoded(changed)+'\n').encode()
        with self.assertRaises(ValueError):
            load_private_graph_release(raw, release.sha256, receipt, hashlib.sha256(receipt).hexdigest())

    def r59_pair(self):
        path = (Path(__file__).parents[1] / 'tests/fixtures/golden'
                / 'current-upstream-release-20261102/r59-native-query-export'
                / 'r59-graph-release.json')
        raw = path.read_bytes()
        value = json.loads(raw)
        manifest = value['publication']
        source = json.loads(manifest['validation_report_json'])['source_admission']
        checkpoint = json.loads(manifest['source_progress_json'])[source['feed']]
        targets = {table: (snapshot['uuid'], snapshot['version'])
                   for table, snapshot in value['snapshots'].items()}
        vector = PinVector('test-host', 'release', manifest['publication_id'],
                           hashlib.sha256(encoded(manifest).encode()).hexdigest(), targets)
        return GraphRelease(raw, hashlib.sha256(raw).hexdigest()), source, checkpoint, vector

    def test_r59_current_052_xxhash64_roundtrip_is_structural_only(self):
        release, source, checkpoint, vector = self.r59_pair()
        receipt = build_truss_graph_custody(
            release, source=source, vector=vector, checkpoint=checkpoint)
        loaded = load_private_graph_release(
            release.payload, release.sha256, receipt,
            hashlib.sha256(receipt).hexdigest())
        admitted = admit_truss_graph_custody(
            loaded, receipt, hashlib.sha256(receipt).hexdigest(), release.sha256)
        self.assertEqual((len(loaded['nodes']), len(loaded['edges'])), (6, 3))
        self.assertEqual(source['profile'], CURRENT_052_CANDIDATE_PROFILE)
        self.assertEqual(loaded['mapping']['lookupHashProfile'],
                         'spark-xxhash64-typed-key/0.1')
        self.assertEqual(admitted['checkpoint'], checkpoint)
        self.assertNotIn('ack', admitted)
        self.assertIn('no protected ACK receipt', admitted['qualification'])

    def test_r61_current_053_fresh_baseline_graph_export_custody(self):
        release, source, checkpoint, vector = self.r59_pair()
        source = dict(source, profile=CURRENT_053_FRESH_PROFILE,
                      runtime_wheel_sha256=CURRENT_TRUSS_053_FRESH_WHEEL_SHA256,
                      runtime_manifest_sha256=CURRENT_TRUSS_053_FRESH_RUNTIME_MANIFEST_SHA256)
        value = json.loads(release.payload)
        report = json.loads(value['publication']['validation_report_json'])
        report['source_admission'] = source
        value['publication']['validation_report_json'] = encoded(report)
        raw = (encoded(value)+'\n').encode()
        fresh_release = GraphRelease(raw, hashlib.sha256(raw).hexdigest())
        vector = PinVector(vector.authority, vector.scope_kind, vector.scope_id,
            hashlib.sha256(encoded(value['publication']).encode()).hexdigest(),
            {table: (snapshot['uuid'], snapshot['version'])
             for table, snapshot in value['snapshots'].items()})
        receipt = build_truss_graph_custody(
            fresh_release, source=source, vector=vector, checkpoint=checkpoint)
        loaded = load_private_graph_release(
            raw, fresh_release.sha256, receipt, hashlib.sha256(receipt).hexdigest())
        admitted = admit_truss_graph_custody(
            loaded, receipt, hashlib.sha256(receipt).hexdigest(), fresh_release.sha256)
        self.assertEqual(loaded['publication'], value['publication'])
        self.assertEqual(admitted['source'], source)
        self.assertEqual(admitted['checkpoint'], checkpoint)

        wrong = dict(source, runtime_wheel_sha256='0' * 64)
        bad_value = json.loads(raw)
        bad_report = json.loads(bad_value['publication']['validation_report_json'])
        bad_report['source_admission'] = wrong
        bad_value['publication']['validation_report_json'] = encoded(bad_report)
        bad_raw = (encoded(bad_value)+'\n').encode()
        bad_release = GraphRelease(bad_raw, hashlib.sha256(bad_raw).hexdigest())
        bad_vector = PinVector(vector.authority, vector.scope_kind, vector.scope_id,
            hashlib.sha256(encoded(bad_value['publication']).encode()).hexdigest(), dict(vector.targets))
        with self.assertRaisesRegex(ValueError, 'pins required'):
            build_truss_graph_custody(
                bad_release, source=wrong, vector=bad_vector, checkpoint=checkpoint)

    def test_fresh_054_source_custody_requires_its_exact_wheel_pin(self):
        from ashlar_host.truss_graph_custody import _validate_current_053_source
        _, source, _, _ = self.r59_pair()
        source = dict(source, profile=CURRENT_053_FRESH_PROFILE,
            runtime_wheel_sha256=CURRENT_TRUSS_054_FRESH_WHEEL_SHA256,
            runtime_manifest_sha256=CURRENT_TRUSS_054_FRESH_RUNTIME_MANIFEST_SHA256)
        _validate_current_053_source(source)
        source['runtime_wheel_sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'fresh-baseline source metadata'):
            _validate_current_053_source(source)

    def test_fresh_055_source_custody_requires_its_exact_wheel_pin(self):
        from ashlar_host.truss_graph_custody import _validate_current_053_source
        _, source, _, _ = self.r59_pair()
        source = dict(source, profile=CURRENT_053_FRESH_PROFILE,
            runtime_wheel_sha256=CURRENT_TRUSS_055_FRESH_WHEEL_SHA256,
            runtime_manifest_sha256=CURRENT_TRUSS_055_FRESH_RUNTIME_MANIFEST_SHA256)
        _validate_current_053_source(source)
        source['runtime_manifest_sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'fresh-baseline source metadata'):
            _validate_current_053_source(source)

    def test_r59_current_052_refuses_changed_source_checkpoint_vector_and_digests(self):
        release, source, checkpoint, vector = self.r59_pair()
        changed_source = dict(source, feed='substituted-feed')
        with self.assertRaises(ValueError):
            build_truss_graph_custody(
                release, source=changed_source, vector=vector, checkpoint=checkpoint)
        changed_checkpoint = dict(checkpoint, previous='Y2hhbmdlZA==')
        with self.assertRaises(ValueError):
            build_truss_graph_custody(
                release, source=source, vector=vector, checkpoint=changed_checkpoint)
        changed_targets = dict(vector.targets)
        table = next(iter(changed_targets))
        uuid, version = changed_targets[table]
        changed_targets[table] = (uuid, version + 1)
        changed_vector = PinVector(vector.authority, vector.scope_kind, vector.scope_id,
                                   vector.custody_digest, changed_targets)
        with self.assertRaises(ValueError):
            build_truss_graph_custody(
                release, source=source, vector=changed_vector, checkpoint=checkpoint)
        receipt = build_truss_graph_custody(
            release, source=source, vector=vector, checkpoint=checkpoint)
        with self.assertRaises(ValueError):
            admit_truss_graph_custody(
                json.loads(release.payload), receipt, '0' * 64, release.sha256)
        with self.assertRaises(ValueError):
            admit_truss_graph_custody(
                json.loads(release.payload), receipt,
                hashlib.sha256(receipt).hexdigest(), '0' * 64)

    def test_r59_current_052_refuses_wrong_layout_hash_and_carrier(self):
        release, source, checkpoint, vector = self.r59_pair()
        original = json.loads(release.payload)
        cases = []
        wrong_hash_profile = json.loads(release.payload)
        wrong_hash_profile['mapping']['lookupHashProfile'] = 'sha256-json-v1'
        cases.append(wrong_hash_profile)
        missing_hash_profile = json.loads(release.payload)
        del missing_hash_profile['mapping']['lookupHashProfile']
        cases.append(missing_hash_profile)
        unknown_layout = json.loads(release.payload)
        unknown_layout['publication']['profile_version'] = 'ashlar-delta/0.9'
        cases.append(unknown_layout)
        invalid_signed_hash = json.loads(release.payload)
        invalid_signed_hash['nodes'][0]['lookup_hash'] = '9223372036854775808'
        cases.append(invalid_signed_hash)
        for value in cases:
            with self.subTest(value=value['publication']['profile_version'],
                              lookup=value['mapping'].get('lookupHashProfile')):
                raw = (encoded(value) + '\n').encode()
                altered = GraphRelease(raw, hashlib.sha256(raw).hexdigest())
                with self.assertRaises(ValueError):
                    build_truss_graph_custody(
                        altered, source=source, vector=vector, checkpoint=checkpoint)
