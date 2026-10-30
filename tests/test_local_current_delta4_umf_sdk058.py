# SPDX-License-Identifier: MIT OR Apache-2.0
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest

from ashlar_host.local_current_delta4_umf_sdk058 import (
    BRIDGE_SHA256, EXPECTED_ROLES, LocalUmfDelta4Sdk058Bridge,
    pinned_sdk058_resources,
)


ROOT = Path(__file__).resolve().parents[1]
PACKET = ROOT / 'docs/helix/04-build/evidence/current-upstream-release-20261029/g03-sdk058-delta4-realization-candidate-20261030'
RELEASE_PROPOSALS = PACKET / 'outputs/carrier-proposals.json'
RELEASE_PROPOSALS_SHA256 = 'd00c4a9d0c6c1fd0946007a5ae2495ee95960ba094d1a46dbed0dc6d966e68ea'
SOURCE_CORRESPONDENCE = ROOT / 'docs/helix/04-build/evidence/current-upstream-release-20261029/g03-sdk058-source-correspondence-20261030/outputs/source-correspondence-qualification.json'
SOURCE_CORRESPONDENCE_SHA256 = '1513d0b58774bcf1861f75f71595a5f3dc8d2ff20ac8821fa63a02cb2aa77da0'


class LocalCurrentDelta4UmfSdk058Tests(unittest.TestCase):
    def test_source_backed_sdk058_custody_and_frozen_baselines(self):
        manifest, bundle, sources, semantics = pinned_sdk058_resources()
        self.assertEqual(hashlib.sha256(bundle).hexdigest(), BRIDGE_SHA256)
        self.assertEqual(manifest['selectedRelease']['version'], '0.5.8')
        self.assertEqual(manifest['producerSource']['commit'],
                         '855f6c48649496fc60b3303798790d0ddb55d38f')
        self.assertEqual(manifest['qualificationBaselines']['sixRoleReleaseEquality']['revision'],
                         'db91104ad029887754dde5327e96d53a0fc885bd')
        self.assertEqual(manifest['qualificationBaselines']['sourceCorrespondence']['revision'],
                         '7b5a5aa3fc6a9dd5754ccc41d35ff7d2f51d8dec')
        self.assertEqual(tuple(manifest['roles']), EXPECTED_ROLES)
        self.assertEqual(set(semantics), set(EXPECTED_ROLES))
        self.assertTrue(all(role + '.umf.json' in sources for role in EXPECTED_ROLES))
        self.assertEqual(hashlib.sha256(RELEASE_PROPOSALS.read_bytes()).hexdigest(),
                         RELEASE_PROPOSALS_SHA256)
        self.assertEqual(hashlib.sha256(SOURCE_CORRESPONDENCE.read_bytes()).hexdigest(),
                         SOURCE_CORRESPONDENCE_SHA256)
        frozen = json.loads(RELEASE_PROPOSALS.read_text())
        self.assertEqual(tuple(item['role'] for item in frozen['roles']), EXPECTED_ROLES)
        self.assertTrue(all(item['release']['target']['kind'] == 'delta-path'
                            for item in frozen['roles']))

    @unittest.skipUnless(importlib.util.find_spec('py_mini_racer'),
                         'py_mini_racer is optional and absent in this environment')
    def test_source_backed_sdk058_matches_all_six_frozen_release_proposals(self):
        _manifest, _bundle, sources, _semantics = pinned_sdk058_resources()
        frozen = json.loads(RELEASE_PROPOSALS.read_text())
        with LocalUmfDelta4Sdk058Bridge.open() as bridge:
            for item in frozen['roles']:
                role = item['role']
                location = '/virtual/ashlar-g03-umf058/' + role
                admitted = bridge.realize(sources[role + '.umf.json'], location, role)
                self.assertEqual(admitted['proposal'], item['release'], role)


if __name__ == '__main__':
    unittest.main()
