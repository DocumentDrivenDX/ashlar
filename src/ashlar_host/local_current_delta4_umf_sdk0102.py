# SPDX-License-Identifier: MIT OR Apache-2.0
"""UMF SDK 0.10.2 / Python Core 0.10.6 Rust-backed Delta 4 realization."""
import hashlib
import importlib.metadata
import json
import platform
import sys
from importlib.resources import files

from .local_current_delta4_umf import admit_proposal

UMF_REALIZATION_PROFILE = 'ashlar-local-current-delta4-umf-sdk0102/0.1'
LOCAL_TARGET_PROFILE = 'spark-delta4/0.1'
SDK_RELEASE = 'umf-rust-v0.1.0'
SDK_VERSION = '0.10.2'
CORE_VERSION = '0.10.6'
MODEL_VERSION = '0.10.0'
SDK_TAG_COMMIT = 'e35cd92b45e16816be2bdbf28e83ef9c53ac99a2'
DELTA_DDL_PROFILE = 'umf-r8-delta-ddl/0.1.0'
DELTA_DDL_OPERATION = 'delta.ddl.realizeSparkDelta4'
SDK_ARCHIVE_SHA256 = {
    ('darwin', 'arm64'): '785de2c4db18b030c9462c7f55f52d012db50a4d4b3c608a4dc3aa6f5fa0d39b',
    ('linux', 'x86_64'): '0e210969dc6d585d73b0e70d35660421b40c141746389118f7181c54bfcff943',
}
CORE_WHEEL_SHA256 = {
    ('darwin', 'arm64'): 'e6219539dabcdfe9657bc09485ad991b3380ae306c658d0a51a35caac0196e36',
    ('linux', 'x86_64'): '9a4fc6e739fb9c0c80e22916490484fb41b6f00985afc53fb81dcca2d72029b9',
}
RESOURCE_ROOT = 'local-current-delta4-sdk0102'
MANIFEST_SHA256 = '4db01b6a3bef4ce2cf898910ff2316ab130e38a1634e9e9f5138c2b47ccf51c5'
ROLE_SEMANTICS_SHA256 = '148a8f471b572e5dcb2cd19c8f170404c26c37f6ebb2c670ddbe3c74a224f33d'
SOURCE_POLICY_SHA256 = '0c53f26b9a75fde90a269d0cc6fd7d5166e31fd320f86d6484630e709c2e64ea'


def _require(value, message='Pinned UMF SDK 0.10.2 Delta 4 realization required'):
    if not value:
        raise ValueError(message)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _read(root, name, maximum):
    with root.joinpath(name).open('rb') as stream:
        raw = stream.read(maximum + 1)
    _require(0 < len(raw) <= maximum, 'Bounded SDK 0.10.2 realization resource required')
    return raw


def pinned_sdk0102_resources():
    root = files('ashlar_host').joinpath('resources', RESOURCE_ROOT)
    manifest_bytes = _read(root, 'realization.json', 32768)
    semantics_bytes = _read(root, 'role-semantics.json', 65536)
    policy_bytes = _read(root, 'source-policy.json', 8192)
    manifest = json.loads(manifest_bytes)
    semantics = json.loads(semantics_bytes)
    policy = json.loads(policy_bytes)
    _require(_sha(manifest_bytes) == '4db01b6a3bef4ce2cf898910ff2316ab130e38a1634e9e9f5138c2b47ccf51c5'
             and _sha(semantics_bytes) == '148a8f471b572e5dcb2cd19c8f170404c26c37f6ebb2c670ddbe3c74a224f33d'
             and _sha(policy_bytes) == '0c53f26b9a75fde90a269d0cc6fd7d5166e31fd320f86d6484630e709c2e64ea',
             'Pinned SDK 0.10.2 profile resource custody differs')
    _require(manifest == {
        'profile': UMF_REALIZATION_PROFILE,
        'carrierProfile': 'ashlar-delta/0.4-xxhash64',
        'release': SDK_RELEASE,
        'sdkVersion': SDK_VERSION,
        'coreVersion': CORE_VERSION,
        'modelVersion': MODEL_VERSION,
        'tagCommit': SDK_TAG_COMMIT,
        'adapter': {'profile': DELTA_DDL_PROFILE,
                    'operation': 'realizeSparkDelta4',
                    'wireOperation': DELTA_DDL_OPERATION},
        'platformArtifacts': {
            'darwin-arm64': {'sdkArchiveSha256': SDK_ARCHIVE_SHA256['darwin', 'arm64'],
                             'coreWheelSha256': CORE_WHEEL_SHA256['darwin', 'arm64']},
            'linux-x86_64': {'sdkArchiveSha256': SDK_ARCHIVE_SHA256['linux', 'x86_64'],
                             'coreWheelSha256': CORE_WHEEL_SHA256['linux', 'x86_64']},
        },
        'roleSemanticsSha256': _sha(semantics_bytes),
    }, 'SDK 0.10.2 release profile differs')
    _require(type(semantics) is dict and set(semantics) == {
        'candidateCommit', 'profile', 'realizationProfile', 'roles'
    } and semantics.get('profile') == 'ashlar-umf-local-delta4-role-semantics/0.1'
        and semantics.get('realizationProfile') == 'umf.delta.spark-realization/0.1.0'
        and type(semantics.get('roles')) is dict
        and set(semantics['roles']) == {
            'object_current', 'edge_current', 'tombstone', 'whole_source_history',
            'publication_manifest', 'publication_attempt_phase'
        }, 'Exact six-role semantic descriptor required')
    _require(policy == {
        'profile': 'ashlar-local-current-delta4-sdk0102-source-policy/0.1',
        'selectedSourceProfile': 'ashlar-delta/0.4-xxhash64',
        'sourceRoles': [
            'object_current', 'edge_current', 'tombstone', 'whole_source_history',
            'publication_manifest', 'publication_attempt_phase'
        ],
        'sdkRelease': SDK_RELEASE,
        'sdkVersion': SDK_VERSION,
        'coreVersion': CORE_VERSION,
        'modelVersion': MODEL_VERSION,
        'platformArtifacts': {
            'darwin-arm64': {'sdkArchiveSha256': SDK_ARCHIVE_SHA256['darwin', 'arm64'],
                             'coreWheelSha256': CORE_WHEEL_SHA256['darwin', 'arm64']},
            'linux-x86_64': {'sdkArchiveSha256': SDK_ARCHIVE_SHA256['linux', 'x86_64'],
                             'coreWheelSha256': CORE_WHEEL_SHA256['linux', 'x86_64']},
        },
        'qualification': 'Rust-backed Python Core Delta DDL proposal only; no Spark execution or source/ACK authority.'
    }, 'Exact SDK 0.10.2 source-policy manifest required')
    return manifest, semantics['roles']


def _selected_core_wheel_sha256():
    key = (sys.platform, platform.machine())
    expected = CORE_WHEEL_SHA256.get(key)
    _require(expected is not None, 'Supported UMF Python Core 0.10.6 platform required')
    distribution = importlib.metadata.distribution('umf-core')
    _require(distribution.version == CORE_VERSION, 'UMF Python Core 0.10.6 required')
    raw = distribution.read_text('direct_url.json')
    _require(type(raw) is str, 'UMF Core wheel provenance required')
    try:
        direct_url = json.loads(raw)
        actual = direct_url['archive_info']['hashes']['sha256']
    except (ValueError, TypeError, KeyError):
        raise ValueError('UMF Core wheel provenance required') from None
    _require(actual == expected, 'UMF Python Core wheel differs from the selected release')
    return expected


def selected_platform_artifacts():
    key = (sys.platform, platform.machine())
    _require(key in CORE_WHEEL_SHA256 and key in SDK_ARCHIVE_SHA256,
             'Supported UMF SDK 0.10.2 platform required')
    return {'sdk_archive_sha256': SDK_ARCHIVE_SHA256[key],
            'core_wheel_sha256': _selected_core_wheel_sha256()}


class LocalUmfDelta4Sdk0102Bridge:
    """Pure Python adapter over the pinned Rust-backed UMF Core Delta DDL operation."""

    def __init__(self, manifest, role_semantics, execute):
        self.manifest = manifest
        self.role_semantics = role_semantics
        self._execute = execute

    @classmethod
    def open(cls):
        manifest, roles = pinned_sdk0102_resources()
        _selected_core_wheel_sha256()
        import umf
        _require(getattr(umf, '__version__', None) == CORE_VERSION,
                 'UMF Python Core runtime version differs')
        info = umf.adapter_info('delta-ddl')
        _require(type(info) is dict and info.get('profile') == DELTA_DDL_PROFILE
                 and info.get('operations', {}).get('realizeSparkDelta4') == DELTA_DDL_OPERATION,
                 'UMF Rust Delta DDL operation profile differs')
        return cls(manifest, roles, umf.execute_adapter)

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        return False

    def realize(self, source_bytes, location, carrier_role):
        _require(type(source_bytes) is bytes and 0 < len(source_bytes) <= 1048576
                 and carrier_role in self.role_semantics
                 and type(location) is str and location.startswith('/')
                 and len(location.encode('utf-8')) <= 4096,
                 'Bounded exact `.4` source and local Delta target required')
        try:
            document = json.loads(source_bytes.decode('utf-8'))
        except (UnicodeDecodeError, ValueError):
            raise ValueError('Exact UTF-8 UMF source document required') from None
        proposal = self._execute('delta-ddl', 'realizeSparkDelta4',
            document=document, target={'kind': 'delta-path', 'path': location})
        return admit_proposal(proposal, source_bytes, location, carrier_role,
                              self.role_semantics[carrier_role])


def realize_current_six_role_proposals(output):
    """Bind Rust-backed DDL proposals to all six exact `.4` carrier sources."""
    from pathlib import Path
    from .managed_current_carriers import load_current_managed_carriers
    from .schema_rows import LOCAL_CURRENT_ROLE_TO_UMF
    output = Path(output)
    _require(output.is_absolute() and output.resolve() == output,
             'Canonical absolute six-role proposal root required')
    carriers = load_current_managed_carriers('ashlar-delta/0.4-xxhash64')
    sources = dict(carriers.inputs)
    result = {}
    with LocalUmfDelta4Sdk0102Bridge.open() as bridge:
        for local_role, carrier_role in LOCAL_CURRENT_ROLE_TO_UMF.items():
            source = sources.get(carrier_role + '.umf.json')
            _require(type(source) is bytes, 'Exact original `.4` carrier source required')
            result[local_role] = bridge.realize(source, str(output / local_role), carrier_role)
    _require(set(result) == set(LOCAL_CURRENT_ROLE_TO_UMF),
             'Complete six-role SDK 0.10.2 output binding required')
    return result
