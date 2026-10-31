# SPDX-License-Identifier: MIT OR Apache-2.0
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest

from ashlar_host.local_current_delta4_umf_sdk010 import (
    BRIDGE_SHA256, EXPECTED_ROLES, LocalUmfDelta4Sdk010Bridge,
    pinned_sdk010_resources,
)

ROOT = Path(__file__).resolve().parents[1]
PACKET = ROOT / 'docs/helix/04-build/evidence/current-upstream-release-20261031/sdk010-delta4-bridge-candidate'
RELEASE_PROPOSALS = ROOT / 'docs/helix/04-build/evidence/current-upstream-release-20261029/g03-sdk058-delta4-realization-candidate-20261030/outputs/carrier-proposals.json'
RELEASE_PROPOSALS_SHA256 = 'd00c4a9d0c6c1fd0946007a5ae2495ee95960ba094d1a46dbed0dc6d966e68ea'


class LocalCurrentDelta4UmfSdk010Tests(unittest.TestCase):
    def test_sdk010_source_policy_custody_and_frozen_baselines(self):
        manifest, bundle, sources, semantics = pinned_sdk010_resources()
        self.assertEqual(hashlib.sha256(bundle).hexdigest(), BRIDGE_SHA256)
        self.assertEqual(manifest['selectedRelease']['version'], '0.10.0')
        self.assertEqual(manifest['selectedRelease']['semanticCore'], '0.10.0')
        self.assertEqual(manifest['selectedRelease']['pythonDistribution'], '0.10.4')
        self.assertEqual(tuple(manifest['roles']), EXPECTED_ROLES)
        self.assertEqual(set(semantics), set(EXPECTED_ROLES))
        self.assertTrue(all(role + '.umf.json' in sources for role in EXPECTED_ROLES))
        self.assertEqual(hashlib.sha256(RELEASE_PROPOSALS.read_bytes()).hexdigest(), RELEASE_PROPOSALS_SHA256)
        qualification = json.loads((PACKET / 'outputs/qualification.json').read_bytes())
        self.assertTrue(qualification['allPublicPackageEqual'])
        self.assertTrue(qualification['allCandidateBridgeEqual'])
        self.assertTrue(all(item['publicPackageEqualsOldReceipt'] and item['candidateBridgeEqualsOldReceipt']
                            for item in qualification['roles']))

    @unittest.skipUnless(importlib.util.find_spec('py_mini_racer'),
                         'py_mini_racer is optional and absent in this environment')
    def test_sdk010_mini_racer_matches_all_six_frozen_release_proposals(self):
        _manifest, _bundle, sources, _semantics = pinned_sdk010_resources()
        frozen = json.loads(RELEASE_PROPOSALS.read_text())
        with LocalUmfDelta4Sdk010Bridge.open() as bridge:
            for item in frozen['roles']:
                role = item['role']
                location = item['release']['target']['path']
                admitted = bridge.realize(sources[role + '.umf.json'], location, role)
                self.assertEqual(admitted['proposal'], item['release'], role)


if __name__ == '__main__':
    unittest.main()
