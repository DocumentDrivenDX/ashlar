# SPDX-License-Identifier: MIT OR Apache-2.0
"""Structural immutable Truss capture correspondence; no native execution."""
import hashlib
import json
from types import SimpleNamespace
import unittest
import test_finite_graph_export
from ashlar.graph_release import GraphRelease
from ashlar_host.delta_custody import encoded
from ashlar_host.truss_graph_admission import PROFILE as SOURCE_PROFILE
from ashlar_host.truss_graph_custody import PROFILE, build_truss_graph_custody
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
        vector = SimpleNamespace(targets={table: (snapshot['uuid'], snapshot['version']) for table, snapshot in value['snapshots'].items()})
        receipt = build_truss_graph_custody(release, source=source, vector=vector, checkpoint=checkpoint)
        loaded = load_private_graph_release(raw, release.sha256, receipt, hashlib.sha256(receipt).hexdigest())
        self.assertEqual(loaded, value)
        self.assertEqual(json.loads(receipt)['format'], PROFILE)
        self.assertNotIn('ack', json.loads(receipt))
        changed = json.loads(receipt); changed['checkpoint']['position'] = 'Y2hhbmdlZA=='
        receipt = (encoded(changed)+'\n').encode()
        with self.assertRaises(ValueError):
            load_private_graph_release(raw, release.sha256, receipt, hashlib.sha256(receipt).hexdigest())
