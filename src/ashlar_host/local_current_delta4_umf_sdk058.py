# SPDX-License-Identifier: MIT OR Apache-2.0
"""Versioned UMF SDK 0.5.8 source-backed Delta 4 proposal bridge.

This IIFE is built from the selected release-lineage source modules rather
than the package-root entry, whose top-level await is not MiniRacer-compatible.
The historical 0.5.6 bridge and its receipts remain untouched.
"""
import hashlib
import json
import re
from importlib.resources import files

from .local_current_delta4_umf import admit_proposal
from .managed_current_carriers import load_current_managed_carriers

UMF_REALIZATION_PROFILE = 'ashlar-local-current-delta4-umf-source-sdk058/0.1'
LOCAL_TARGET_PROFILE = 'spark-delta4/0.1'
MANIFEST_SHA256 = '10b76f0359c62f0ff33734ba58ac5a8878ccbbf8d2fa2eba83413783c3f9fab1'
BUILD_INPUTS_SHA256 = '3c021d364d43a9b7c6bfbf95f4ede74d97eac49d79085131c140b06c19ae2677'
BRIDGE_SHA256 = '8ca2c7ec908c80b78d4bac66144511330f56ad722b9149175c6d5fb0495bcb93'
ENTRY_SHA256 = 'b06f90574e25bae2a774900b3024eb8882f7b3cc8746138e209d9cb0fb894fd7'
ROLE_SEMANTICS_SHA256 = '148a8f471b572e5dcb2cd19c8f170404c26c37f6ebb2c670ddbe3c74a224f33d'
NOTICES_SHA256 = '8177eaded1ef5e38152c2ab4d88df6029b90d032ca8a7cddedee61def508bca9'
EXPECTED_ROLES = (
    'object_current', 'edge_current', 'tombstone', 'whole_source_history',
    'publication_manifest', 'publication_attempt_phase',
)
ROLE_SOURCE_SHA256 = {
    'object_current': '6457b4b1a56fa5530732ef098b21b4a6ed5179db6d99a000545bf5c300caf771',
    'edge_current': '18732a5991f77d4b74a3ef2a77d4512b2b550e9de43a4085f342dfc326d237bb',
    'tombstone': 'dabf700f674c95d84b8bf2ee5f40e7fb6d989ec8052c3790744cd24352afebf0',
    'whole_source_history': '381b5f2bd566d39ea5583d3f4764c6b8f64c049c15544b4e1c5cb3cb6fd3f934',
    'publication_manifest': 'fa4a5bdf9493eca17f563030f20cfc5a3d36938fb7998c9839b85923ae9cc0d8',
    'publication_attempt_phase': 'd6ca4b087f36e2a14d03d363a67a49059016ef1cd96b8792771dddec0dee99dc',
}


def _require(value, message='Pinned UMF SDK 0.5.8 Delta 4 realization required'):
    if not value:
        raise ValueError(message)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _read(root, name, maximum):
    with root.joinpath(name).open('rb') as stream:
        raw = stream.read(maximum + 1)
    _require(0 < len(raw) <= maximum, 'Bounded SDK 0.5.8 bridge resource required')
    return raw


def pinned_sdk058_resources():
    root = files('ashlar_host').joinpath('resources', 'local-current-delta4-sdk058')
    manifest_bytes = _read(root, 'realization.json', 65536)
    inputs_bytes = _read(root, 'build-inputs.json', 262144)
    semantic_bytes = _read(root, 'role-semantics.json', 32768)
    bridge_bytes = _read(root, 'umf-spark-delta4.iife.js', 2097152)
    entry_bytes = _read(root, 'bridge-entry.ts', 8192)
    notices = _read(root, 'THIRD_PARTY_NOTICES.txt', 65536)
    _require(_sha(manifest_bytes) == MANIFEST_SHA256
             and _sha(inputs_bytes) == BUILD_INPUTS_SHA256
             and _sha(bridge_bytes) == BRIDGE_SHA256
             and _sha(entry_bytes) == ENTRY_SHA256
             and _sha(semantic_bytes) == ROLE_SEMANTICS_SHA256
             and _sha(notices) == NOTICES_SHA256,
             'Pinned source-backed SDK 0.5.8 bridge custody differs')
    manifest = json.loads(manifest_bytes)
    build_inputs = json.loads(inputs_bytes)
    semantics = json.loads(semantic_bytes)
    _require(manifest.get('profile') == UMF_REALIZATION_PROFILE
             and manifest.get('carrierProfile') == 'ashlar-delta/0.4-xxhash64'
             and manifest.get('selectedRelease') == {
                 'version': '0.5.8', 'tag': 'umf-v0.5.8',
                 'tagCommit': 'efb4042719866888ad80c01cb2da694972eb2738',
                 'semanticCore': '0.10.0',
             }
             and manifest.get('roles') == list(EXPECTED_ROLES)
             and manifest.get('roleSemanticsSha256') == ROLE_SEMANTICS_SHA256
             and manifest.get('thirdPartyNoticesSha256') == NOTICES_SHA256,
             'Selected SDK 0.5.8 bridge profile and role binding required')
    bridge = manifest.get('bridge')
    _require(type(bridge) is dict and bridge.get('asset') == 'umf-spark-delta4.iife.js'
             and bridge.get('assetSha256') == BRIDGE_SHA256
             and bridge.get('assetBytes') == len(bridge_bytes)
             and bridge.get('entry') == 'bridge-entry.ts'
             and bridge.get('entrySha256') == ENTRY_SHA256 == _sha(entry_bytes)
             and bridge.get('builder') == 'Bun 1.4.3'
             and bridge.get('buildInputsSha256') == BUILD_INPUTS_SHA256,
             'Exact source-backed bridge build receipt required')
    inventory = build_inputs.get('inputs')
    _require(type(build_inputs) is dict and set(build_inputs) == {'profile', 'inputs'}
             and build_inputs.get('profile') == 'ashlar.umf058-source-backed-bridge-inputs/1'
             and type(inventory) is list and len(inventory) == 195
             and bridge.get('inputCount') == len(inventory)
             and all(type(item) is dict and set(item) == {'path', 'bytes', 'sha256'}
                     and type(item['path']) is str and '..' not in item['path'].split('/')
                     and type(item['bytes']) is int and item['bytes'] > 0
                     and re.fullmatch('[0-9a-f]{64}', item['sha256']) is not None
                     for item in inventory)
             and len({item['path'] for item in inventory}) == len(inventory),
             'Complete, unique source-backed bridge input inventory required')
    indexed = {item['path']: item for item in inventory}
    _require(indexed.get('umf-source/bridge-entry-sdk058.ts', {}).get('sha256') == _sha(entry_bytes)
             and all(indexed.get('umf-source/' + path, {}).get('sha256') == digest
                     for path, digest in manifest['producerSource']['sourceModules'].items())
             and type(semantics) is dict and type(semantics.get('roles')) is dict
             and set(semantics['roles']) == set(EXPECTED_ROLES),
             'Exact SDK source entry, public API modules, and role semantics are required')
    carriers = load_current_managed_carriers('ashlar-delta/0.4-xxhash64')
    sources = dict(carriers.inputs)
    _require(all(role + '.umf.json' in sources
                 and _sha(sources[role + '.umf.json']) == ROLE_SOURCE_SHA256[role]
                 and semantics['roles'][role].get('sourceBytesSha256') == ROLE_SOURCE_SHA256[role]
                 for role in EXPECTED_ROLES),
             'Exact `.4` six-role source bytes are required')
    return manifest, bridge_bytes, sources, semantics['roles']


class LocalUmfDelta4Sdk058Bridge:
    """Bounded MiniRacer instance for the source-backed SDK 0.5.8 IIFE."""
    def __init__(self, runtime, manifest, role_semantics):
        self.runtime = runtime
        self.manifest = manifest
        self.role_semantics = role_semantics

    @classmethod
    def open(cls):
        manifest, bundle, _sources, semantics = pinned_sdk058_resources()
        from py_mini_racer import MiniRacer
        runtime = MiniRacer()
        try:
            runtime.eval(bundle.decode('utf-8'))
        except BaseException:
            runtime.close()
            raise
        return cls(runtime, manifest, semantics)

    def close(self):
        self.runtime.close()

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        self.close()
        return False

    def realize(self, source_bytes, location, carrier_role):
        _require(type(source_bytes) is bytes and 0 < len(source_bytes) <= 1048576
                 and carrier_role in EXPECTED_ROLES
                 and type(location) is str and location.startswith('/')
                 and len(location.encode('utf-8')) <= 4096,
                 'Bounded exact `.4` source and local Delta target required')
        try:
            proposal = self.runtime.call('__ashlarUmfSparkDelta4Sdk058.realize',
                source_bytes.decode('utf-8'), {'kind': 'delta-path', 'path': location})
        except UnicodeDecodeError:
            raise ValueError('Exact UTF-8 UMF source bytes required') from None
        return admit_proposal(proposal, source_bytes, location, carrier_role,
                              self.role_semantics[carrier_role])
