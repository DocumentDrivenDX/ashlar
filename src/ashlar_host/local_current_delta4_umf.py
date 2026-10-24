# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pinned local UMF Delta 4 realization bridge and proposal admission."""
from dataclasses import dataclass
import hashlib
import json
import re
from importlib.resources import files

from .managed_current_carriers import load_current_managed_carriers

UMF_BRIDGE_RESOURCE = 'local-current-delta4'
UMF_REALIZATION_PROFILE = 'ashlar-local-current-delta4-umf-realization/0.1'
LOCAL_TARGET_PROFILE = 'spark-delta4/0.1'
UMF_CANDIDATE_COMMIT = '259bf1ed91466ff7e9782d7517c14f2112282026'
REALIZATION_MANIFEST_SHA256 = 'da7d4b2a50ca949632520e64a616674646ecc3f829c3b9574dadced42ee84089'
BUILD_INPUTS_SHA256 = 'b314d067d88ede4117f37c0b25604f53fac5753b5c5c2d05500762530950a26a'
BRIDGE_SHA256 = '34deb5c79b5dcb1ace8093e66a4b1213a98cbcc1fb50bfd24fdc4a21721c76b5'
ROLE_SEMANTICS_SHA256 = '148a8f471b572e5dcb2cd19c8f170404c26c37f6ebb2c670ddbe3c74a224f33d'
ROLE_SOURCE_SHA256 = {
    'object_current': '6457b4b1a56fa5530732ef098b21b4a6ed5179db6d99a000545bf5c300caf771',
    'edge_current': '18732a5991f77d4b74a3ef2a77d4512b2b550e9de43a4085f342dfc326d237bb',
    'tombstone': 'dabf700f674c95d84b8bf2ee5f40e7fb6d989ec8052c3790744cd24352afebf0',
    'whole_source_history': '381b5f2bd566d39ea5583d3f4764c6b8f64c049c15544b4e1c5cb3cb6fd3f934',
    'publication_manifest': 'fa4a5bdf9493eca17f563030f20cfc5a3d36938fb7998c9839b85923ae9cc0d8',
    'publication_attempt_phase': 'd6ca4b087f36e2a14d03d363a67a49059016ef1cd96b8792771dddec0dee99dc',
}
ROLES = tuple(ROLE_SOURCE_SHA256)
_TYPE_NAMES = {'string': 'STRING', 'long': 'BIGINT', 'timestamp': 'TIMESTAMP'}


def _require(value, reason='Pinned local UMF Delta 4 realization required'):
    if not value:
        raise ValueError(reason)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _read(resource, name, maximum):
    with resource.joinpath(name).open('rb') as stream:
        raw = stream.read(maximum + 1)
    _require(0 < len(raw) <= maximum, 'Bounded pinned UMF realization resource required')
    return raw


def _pinned_resources():
    root = files('ashlar_host').joinpath('resources', UMF_BRIDGE_RESOURCE)
    manifest_bytes = _read(root, 'realization.json', 65536)
    semantic_bytes = _read(root, 'role-semantics.json', 32768)
    inputs_bytes = _read(root, 'build-inputs.json', 131072)
    bridge_bytes = _read(root, 'umf-spark-delta4.iife.js', 1048576)
    entry_bytes = _read(root, 'bridge-entry.ts', 8192)
    notices = _read(root, 'THIRD_PARTY_NOTICES.txt', 65536)
    _require(_sha(manifest_bytes) == REALIZATION_MANIFEST_SHA256
             and _sha(semantic_bytes) == ROLE_SEMANTICS_SHA256
             and _sha(inputs_bytes) == BUILD_INPUTS_SHA256
             and _sha(bridge_bytes) == BRIDGE_SHA256,
             'Pinned UMF Delta 4 bridge custody differs')
    manifest = json.loads(manifest_bytes)
    build_inputs = json.loads(inputs_bytes)
    role_semantics = json.loads(semantic_bytes)
    _require(manifest == {
        'bridge': {
            'arguments': ['bun', '.cache/sdk-distribution/ashlar-bridge-0.5.6/build-bridge.ts'],
            'asset': 'umf-spark-delta4.iife.js',
            'assetSha256': BRIDGE_SHA256,
            'buildInputsSha256': BUILD_INPUTS_SHA256,
            'builder': 'Bun 1.4.3',
            'builderScriptSha256': '2c03ed1d4ef593bdad98e235bd18f5c5d8be078deded42fde2ef0ff2cce71124',
            'entry': 'bridge-entry.ts',
            'entrySha256': 'd05088842f4c00d21151b1707e74e8477bda54f860940f7996540705140b11c8',
            'metafileSha256': '19d4a797180a4ac55d0afa46cb2174884193cab1494f0add09e7666e437c4b08',
            'thirdPartyNoticesSha256': _sha(notices),
            'workingDirectory': '/private/tmp/umf-merge-259-source-rebuild-20261024',
        },
        'carrierProfile': 'ashlar-delta/0.4-xxhash64',
        'inputs': {
            'managedGeneratedSha256': 'cb6c75c3cb3805011245ca460a763f15509a7d9627009301cc815e6983413faa',
            'roleSourceSha256': ROLE_SOURCE_SHA256,
        },
        'localTargetProfile': LOCAL_TARGET_PROFILE,
        'originalSourceProfile': 'ashlar-managed-current-carrier-layout/0.4-xxhash64',
        'profile': UMF_REALIZATION_PROFILE,
        'roles': list(ROLES),
        'targetSemantics': {
            'codec': 'spark.sql.parquet.compression.codec=zstd',
            'targetFileSize': 'unrealized-performance-hint',
            'unknownMeaning': 'refuse',
        },
        'targetBinding': {
            'relation': 'named relation with LOCATION',
            'deltaPath': 'direct Delta path relation without metastore LOCATION',
        },
        'roleSemanticsSha256': ROLE_SEMANTICS_SHA256,
        'thirdParty': [
            'ajv@8.20.0 (MIT)', 'fast-deep-equal@3.1.3 (MIT)',
            'fast-uri@3.1.8 (BSD-3-Clause)',
            'json-schema-traverse@1.0.0 (MIT)', 'yaml@2.9.1 (ISC)',
        ],
        'umf': {
            'apiExports': ['readDocument', 'realizeSparkDelta4DDL'],
            'bunLockSha256': '5fb58795dfe9ef5df89597c44372cf2a0739e1855f322f7622ae0c9d0ac71ce4',
            'candidateCommit': UMF_CANDIDATE_COMMIT,
            'coreVersion': '0.10.0',
            'release': 'umf-v0.5.6 (published)',
            'sourceModules': {
                'src/adapters/delta/ddl.ts': '8e4f694b7e3ecbdc0548f06b4db0a67339ea9319d3b70939003d4a0d18856051',
                'src/model/document.ts': '6ea8f43e4bcd3b5d7259329e6768dd563e066d7169a23bee75e88f054f348ddb',
            },
            'managedOutputPreserved': 'Not requalified by this Ashlar local-target bridge receipt.',
        },
    }, 'UMF realization manifest does not match this Ashlar implementation')
    _require(_sha(entry_bytes) == manifest['bridge']['entrySha256'],
             'Exact retained UMF bridge build entry required')
    inventory = build_inputs.get('inputs')
    _require(set(build_inputs) == {'sdkVersion', 'sdkCommit', 'command', 'builder', 'inputs'}
             and build_inputs.get('sdkVersion') == '0.5.6'
             and build_inputs.get('sdkCommit') == UMF_CANDIDATE_COMMIT
             and build_inputs.get('command') == 'bun .cache/sdk-distribution/ashlar-bridge-0.5.6/build-bridge.ts'
             and build_inputs.get('builder') == "Bun.build({entrypoints:[out+'/bridge-entry.ts'],target:'browser',format:'iife',minify:true,write:false,metafile:true})"
             and type(inventory) is list and len(inventory) == 195
             and all(type(item) is dict and set(item) == {'path', 'bytes', 'sha256'}
                     and type(item['path']) is str and type(item['bytes']) is int and item['bytes'] > 0
                     and re.fullmatch('[0-9a-f]{64}', item['sha256']) is not None
                     for item in inventory),
             'Pinned UMF bridge build-input inventory required')
    indexed_inputs = {item['path']: item for item in inventory}
    source_modules = manifest['umf']['sourceModules']
    _require(len(indexed_inputs) == len(inventory)
             and all(path in indexed_inputs
                     and indexed_inputs[path]['sha256'] == digest
                     for path, digest in source_modules.items())
             and '.cache/sdk-distribution/ashlar-bridge-0.5.6/bridge-entry.ts' in indexed_inputs
             and indexed_inputs['.cache/sdk-distribution/ashlar-bridge-0.5.6/bridge-entry.ts']['sha256']
             == manifest['bridge']['entrySha256'],
             'UMF source modules and exact bridge entry must be present in the build inventory')
    _require(type(role_semantics) is dict and set(role_semantics) == {
        'candidateCommit', 'profile', 'realizationProfile', 'roles'
    } and role_semantics.get('candidateCommit') == UMF_CANDIDATE_COMMIT
        and role_semantics.get('profile') == 'ashlar-umf-local-delta4-role-semantics/0.1'
        and role_semantics.get('realizationProfile') == 'umf.delta.spark-realization/0.1.0'
        and type(role_semantics.get('roles')) is dict
        and set(role_semantics['roles']) == set(ROLES)
        and all(type(item) is dict and set(item) == {
            'definition', 'dispositions', 'schemaSha256', 'sourceBytesSha256',
            'sourceSha256', 'writerSettings', 'layout'
        } for item in role_semantics['roles'].values()),
        'Exact UMF-owned six-role semantic descriptor required')
    carriers = load_current_managed_carriers('ashlar-delta/0.4-xxhash64')
    source_bytes = dict(carriers.inputs)
    _require(_sha(carriers.generated_bytes) == manifest['inputs']['managedGeneratedSha256'],
             'Exact `.4` managed source compiler output required')
    for role, digest in ROLE_SOURCE_SHA256.items():
        _require(role + '.umf.json' in source_bytes
                 and _sha(source_bytes[role + '.umf.json']) == digest,
                 'Original `.4` source bytes changed: ' + role)
    return manifest, bridge_bytes, source_bytes, role_semantics['roles']


@dataclass
class LocalUmfDelta4Bridge:
    """One bounded V8 instance over the pinned UMF proposal-only bridge."""
    runtime: object
    manifest: dict
    role_semantics: dict
    bridge_sha256: str = BRIDGE_SHA256

    @classmethod
    def open(cls):
        _manifest, bundle, _sources, semantics = _pinned_resources()
        from py_mini_racer import MiniRacer
        runtime = MiniRacer()
        try:
            runtime.eval(bundle.decode('utf-8'))
        except BaseException:
            runtime.close()
            raise
        return cls(runtime, _manifest, semantics)

    def close(self):
        self.runtime.close()

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        self.close()
        return False

    def realize(self, source_bytes, location, carrier_role):
        _require(type(source_bytes) is bytes and 0 < len(source_bytes) <= 1048576
                 and carrier_role in ROLES
                 and type(location) is str and location.startswith('/')
                 and len(location.encode('utf-8')) <= 4096,
                 'Bounded exact local UMF source and target required')
        try:
            proposal = self.runtime.call('__ashlarUmfSparkDelta4.realize',
                source_bytes.decode('utf-8'), {'kind': 'delta-path', 'path': location})
        except UnicodeDecodeError:
            raise ValueError('Exact UTF-8 UMF source bytes required') from None
        return admit_proposal(proposal, source_bytes, location, carrier_role,
                              self.role_semantics[carrier_role])


def admit_proposal(proposal, source_bytes, location, carrier_role, expected_semantics):
    """Check complete closed UMF output before a caller may execute any DDL."""
    _require(type(expected_semantics) is dict and set(expected_semantics) == {
        'definition', 'dispositions', 'schemaSha256', 'sourceBytesSha256',
        'sourceSha256', 'writerSettings', 'layout'
    } and expected_semantics.get('sourceBytesSha256') == _sha(source_bytes),
        'Exact pinned UMF-owned role semantic descriptor required')
    _require(type(proposal) is dict and set(proposal) == {
        'definition', 'dispositions', 'qualification', 'realizationProfile', 'schemaJson',
        'schemaSha256', 'sourceSha256', 'sql', 'target', 'targetProfile', 'writerSettings',
    }, 'Complete UMF Delta 4 realization output required')
    _require(proposal.get('targetProfile') == LOCAL_TARGET_PROFILE
             and proposal.get('realizationProfile') == 'umf.delta.spark-realization/0.1.0'
             and re.fullmatch('[0-9a-f]{64}', proposal.get('sourceSha256', '')) is not None
             and proposal.get('sourceSha256') == expected_semantics['sourceSha256']
             and type(proposal.get('schemaJson')) is str
             and proposal.get('schemaSha256') == expected_semantics['schemaSha256']
             and proposal.get('schemaSha256') == _sha(proposal['schemaJson'].encode('utf-8'))
             and proposal.get('definition') == expected_semantics['definition']
             and proposal.get('dispositions') == expected_semantics['dispositions']
             and proposal.get('writerSettings') == expected_semantics['writerSettings']
             and type(proposal.get('qualification')) is str
             and type(proposal.get('sql')) is str and proposal['sql'].endswith(';\n')
             and proposal['sql'].startswith('CREATE TABLE delta.`' + location.replace('`', '``') + '` (')
             and ' USING DELTA' in proposal['sql'] and ' LOCATION ' not in proposal['sql'],
             'Exact UMF source/schema/DDL identity required')
    target = proposal.get('target')
    target_identity = {'profile': LOCAL_TARGET_PROFILE, 'kind': 'delta-path', 'path': location}
    _require(type(target) is dict and set(target) == {'identitySha256', 'kind', 'path', 'profile'}
             and target.get('profile') == LOCAL_TARGET_PROFILE
             and target.get('kind') == 'delta-path' and target.get('path') == location
             and target.get('identitySha256') == _sha(json.dumps(
                 target_identity, sort_keys=True, separators=(',', ':')).encode('utf-8')),
             'Typed UMF local target identity differs')
    definition = expected_semantics['definition']
    layout = expected_semantics['layout']
    _require(type(definition) is dict and definition.get('name') == [carrier_role]
             and type(layout) is dict and set(layout) == {'clusterBy', 'partitionBy', 'properties'},
             'Original managed Delta source identity must remain exact')
    settings = proposal.get('writerSettings')
    _require(type(settings) is list and len(settings) <= 8,
             'Bounded UMF local writer-setting inventory required')
    schema = json.loads(proposal['schemaJson'])
    _require(type(schema) is dict and schema.get('type') == 'struct'
             and type(schema.get('fields')) is list and bool(schema['fields']),
             'Complete UMF Delta schema realization required')
    fields = []
    field_metadata = []
    for field in schema['fields']:
        _require(type(field) is dict and set(field) == {'name', 'type', 'nullable', 'metadata'}
                 and type(field.get('name')) is str and type(field.get('nullable')) is bool
                 and type(field.get('metadata')) is dict
                 and set(field['metadata']) <= {'comment'}
                 and all(type(value) is str and 0 < len(value.encode('utf-8')) <= 4096
                         for value in field['metadata'].values())
                 and field.get('type') in _TYPE_NAMES,
                 'Supported exact local Delta schema fields required')
        fields.append((field['name'], _TYPE_NAMES[field['type']], field['nullable']))
        field_metadata.append(dict(field['metadata']))
    _require(len({field[0] for field in fields}) == len(fields), 'Unique local Delta field names required')
    return {
        'proposal': proposal,
        'source_bytes_sha256': _sha(source_bytes),
        'proposal_sha256': _sha(json.dumps(proposal, ensure_ascii=False, sort_keys=True,
                                           separators=(',', ':')).encode('utf-8')),
        'fields': tuple(fields),
        'field_metadata': tuple(field_metadata),
        'layout': layout,
    }
