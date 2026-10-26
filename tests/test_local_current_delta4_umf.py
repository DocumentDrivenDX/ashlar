# SPDX-License-Identifier: MIT OR Apache-2.0
import copy
import unittest

from ashlar_host.local_current_carrier_profile import _admit_zstd_footer_inventory
from ashlar_host.local_current_delta4_umf import (
    BUILD_INPUTS_SHA256, BRIDGE_SHA256, LOCAL_TARGET_PROFILE,
    LocalUmfDelta4Bridge, pinned_resources, admit_proposal,
)
from ashlar_host.managed_current_carriers import load_current_managed_carriers


class LocalCurrentDelta4UmfTests(unittest.TestCase):
    def test_all_six_original_sources_realize_to_typed_local_delta_targets(self):
        carriers = load_current_managed_carriers('ashlar-delta/0.4-xxhash64')
        sources = dict(carriers.inputs)
        manifest, _bundle, _sources, _semantics = pinned_resources()
        self.assertEqual(BRIDGE_SHA256, manifest['bridge']['assetSha256'])
        self.assertEqual(BUILD_INPUTS_SHA256, manifest['bridge']['buildInputsSha256'])
        aliases = {'publication_attempt_phase': 'attempts',
                   'publication_manifest': 'manifest'}
        with LocalUmfDelta4Bridge.open() as bridge:
            for carrier_role in manifest['roles']:
                local_role = aliases.get(carrier_role, carrier_role)
                source = sources[carrier_role + '.umf.json']
                path = '/private/tmp/ashlar-local-delta4-test/' + local_role
                admitted = bridge.realize(source, path, carrier_role)
                proposal = admitted['proposal']
                self.assertEqual(proposal['targetProfile'], LOCAL_TARGET_PROFILE)
                self.assertEqual(proposal['target']['kind'], 'delta-path')
                self.assertEqual(proposal['target']['path'], path)
                self.assertEqual(admitted['fields'], carriers.ordered_fields(carrier_role))
                self.assertTrue(proposal['sql'].startswith(
                    'CREATE TABLE delta.`' + path + '`'))
                self.assertIn(' USING DELTA', proposal['sql'])
                self.assertNotIn(' LOCATION ', proposal['sql'])
                if carrier_role in ('object_current', 'edge_current'):
                    self.assertEqual(proposal['writerSettings'], [{
                        'key': 'spark.sql.parquet.compression.codec', 'value': 'zstd'}])
                else:
                    self.assertEqual(proposal['writerSettings'], [])

    def test_local_target_and_original_dispositions_are_closed(self):
        _manifest, _bundle, sources, semantics = pinned_resources()
        source = sources['object_current.umf.json']
        with LocalUmfDelta4Bridge.open() as bridge:
            first = bridge.realize(source, '/private/tmp/ashlar-local-delta4-test/one', 'object_current')
            second = bridge.realize(source, '/private/tmp/ashlar-local-delta4-test/two', 'object_current')
            self.assertNotEqual(first['proposal']['target']['identitySha256'],
                                second['proposal']['target']['identitySha256'])
            self.assertEqual(first['proposal']['definition']['properties']['delta.targetFileSize'],
                             '67108864')
            outcomes = {item['sourcePointer']: item['outcome']
                        for item in first['proposal']['dispositions']}
            self.assertEqual(outcomes['/extensions/umf.delta.definition/properties/delta.targetFileSize'],
                             'unrealized-performance-hint')
            changed = copy.deepcopy(first['proposal'])
            changed['dispositions'][0]['outcome'] = 'silently-dropped'
            with self.assertRaises(ValueError):
                admit_proposal(changed, source, '/private/tmp/ashlar-local-delta4-test/one',
                               'object_current', semantics['object_current'])
            changed = copy.deepcopy(first['proposal'])
            changed['writerSettings'] = [{'key': 'spark.sql.parquet.compression.codec',
                                          'value': 'snappy'}]
            with self.assertRaises(ValueError):
                admit_proposal(changed, source, '/private/tmp/ashlar-local-delta4-test/one',
                               'object_current', semantics['object_current'])
            for field, value in (('path', '/private/tmp/crossed-target'),
                                 ('identitySha256', '0' * 64), ('kind', 'relation')):
                changed = copy.deepcopy(first['proposal'])
                changed['target'][field] = value
                with self.assertRaises(ValueError):
                    admit_proposal(changed, source, '/private/tmp/ashlar-local-delta4-test/one',
                                   'object_current', semantics['object_current'])
            for field, value in (('sourceSha256', '0' * 64),
                                 ('definition', {'profile': 'databricks-managed-delta/0.1',
                                                 'name': ['object_current'], 'clusterBy': [],
                                                 'partitionBy': [], 'properties': {}}),
                                 ('dispositions', [])):
                changed = copy.deepcopy(first['proposal'])
                changed[field] = value
                with self.assertRaises(ValueError):
                    admit_proposal(changed, source, '/private/tmp/ashlar-local-delta4-test/one',
                                   'object_current', semantics['object_current'])
            for pointer in ('/extensions/umf.delta.definition/properties/delta.parquet.compression.codec',
                            '/extensions/umf.delta.definition/properties/delta.targetFileSize'):
                changed = copy.deepcopy(first['proposal'])
                item = next(item for item in changed['dispositions'] if item['sourcePointer'] == pointer)
                item['outcome'] = 'sql'
                with self.assertRaises(ValueError):
                    admit_proposal(changed, source, '/private/tmp/ashlar-local-delta4-test/one',
                                   'object_current', semantics['object_current'])

    def test_zstd_footer_admission_requires_complete_nonempty_pinned_chunks(self):
        report = [{'path_sha256': 'a' * 64,
                   'row_groups': [{'rows': 3, 'codecs': ['ZSTD', 'ZSTD']}]}]
        self.assertEqual(_admit_zstd_footer_inventory(report, expected_rows=3), {
            'active_files': 1, 'nonempty_files': 1, 'row_groups_rows': 3,
            'zstd_column_chunks': 2,
        })
        self.assertEqual(_admit_zstd_footer_inventory([], expected_rows=0), {
            'active_files': 0, 'nonempty_files': 0, 'row_groups_rows': 0,
            'zstd_column_chunks': 0,
        })
        self.assertEqual(_admit_zstd_footer_inventory(report, expected_rows=3)['nonempty_files'],1)
        with self.assertRaises(ValueError):
            _admit_zstd_footer_inventory([{'path_sha256': 'b' * 64,
                'row_groups': [{'rows': 1, 'codecs': ['SNAPPY']}]}], expected_rows=1)
        with self.assertRaises(ValueError):
            _admit_zstd_footer_inventory([{'path_sha256': 'c' * 64,
                'row_groups': [{'rows': 2, 'codecs': []}]}], expected_rows=2)
        with self.assertRaises(ValueError):
            _admit_zstd_footer_inventory([{'path_sha256': 'd' * 64,
                'row_groups': [{'rows': 0, 'codecs': ['SNAPPY']}]}], expected_rows=0)
        with self.assertRaises(ValueError):
            _admit_zstd_footer_inventory(report, expected_rows=4)


if __name__ == '__main__':
    unittest.main()
