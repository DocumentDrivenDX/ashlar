# SPDX-License-Identifier: MIT OR Apache-2.0
"""Explicit UMF SDK 0.5.8 typed-graph admission, separate from historical 2ede.

The source configuration remains pinned to its historical source producer. A
separate versioned admission manifest binds that source packet to this exact
SDK distribution. This module establishes public validation outputs only.
"""
import hashlib
import json
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path

from ashlar.schema import decode_retained_json
from ashlar.typed_graph_source import GRAPH_CHECK_PIN, build_typed_graph_request, verify_typed_graph_receipt
from .config import HostError
from .evolution_producer import capture, snapshot
from .lifecycle import finish, owned_context
from .typed_graph_admission import _KEYS, _source_bytes

SDK058_PRODUCER_REVISION = (
    'umf-sdk-0.5.8/071544fcc83c5abcf1e280890e1516613f679a58cf245b31a7bf16f0b6dd24fd'
)
SDK058_ARCHIVE_SHA256 = '2aaf24741aee1c135fade5b7631db90d710e6211977fb9ca7fcc080ea7fdf58d'
SDK058_CLOSURE_SHA256 = 'e90c44cb79cc5ce39921274cc155c8634232635fc9ba02565d43846f6ee1152f'
SDK058_PRODUCER_RECEIPT_SHA256 = '1ddd2b37623ec1daaffbb592200adafc9f8bb27522c65f1b5584f0a9aec945c4'
SDK058_INSTALL_CUSTODY_SHA256 = 'a1ba0b4346da1304562b3c2bd48d3fcd35c35fef779e38e959980b877c5cdea4'
SDK058_ENTRY_SHA256 = '071544fcc83c5abcf1e280890e1516613f679a58cf245b31a7bf16f0b6dd24fd'
SDK058_PACKAGE_SHA256 = '4533bf7e5ed7bb0aba4e3bdf395818423281ae464b96a73fe492c57291e81500'
SDK058_MANIFEST_SHA256 = '0683f83ffef546c7c48d0b267528fafe83c16006a2cef5fdb299260ebb444309'
SDK058_TREE_SHA256 = '7227738ae23966239b67d3e9df72f848c101761719ef4b39477fe86de6ef519c'
SDK058_NODE_SHA256 = '9af7e24478e538455cf4af53a2607f1b79304bd5e25458b041fb855c3c167991'
SDK058_SOURCE_COMMIT = '855f6c48649496fc60b3303798790d0ddb55d38f'
SDK058_SOURCE_TREE = '53f2bfffbc0c5780e79d76d8fd3d202239d088b2'
SDK058_TAG_COMMIT = 'efb4042719866888ad80c01cb2da694972eb2738'
SDK058_ADMISSION_PROFILE = 'ashlar-typed-graph-sdk058-source-admission/0.1'
_CHECKER = Path(__file__).parent / 'resources' / 'typed_graph_values_sdk058.mjs'
_CHECKER_SHA256 = '4c73b838d48ea7738b43f74622438a31dcf3156dbe51b8eb53fccf60247186f0'
_MANIFEST_KEYS = {
    'profile', 'admissionProducerRevision', 'sourceProducerRevision', 'sourceConfigurationSha256',
    'sourceSha256', 'bindingSha256', 'modelSha256', 'sdkVersion', 'sdkTagCommit', 'sdkSourceCommit',
    'sdkSourceTree', 'sdkArchiveSha256', 'sdkEntrySha256',
}


def _require(value):
    if not value:
        raise HostError('typed-graph-sdk058-admission-refused')


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _identity(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _pinned_file(path, maximum, expected_sha256, *, executable=False):
    _require(isinstance(path, Path) and path.is_absolute())
    before = path.lstat()
    _require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= maximum
             and (not executable or before.st_mode & 0o111))
    raw = snapshot(path, maximum)
    after = path.lstat()
    _require(_identity(before) == _identity(after) and _sha(raw) == expected_sha256)
    return raw, (str(path), _identity(after))


@dataclass(frozen=True)
class TypedGraphSdk058AdmissionConfig:
    """All SDK, runtime and versioned source-admission inputs are explicit paths."""
    node: Path
    sdk_package_root: Path
    sdk_archive: Path
    producer_closure: Path
    producer_receipt: Path
    install_custody: Path
    source_admission_manifest: Path
    timeout_seconds: int = 30
    maximum_output_bytes: int = 1048576

    def __post_init__(self):
        try:
            paths = (self.node, self.sdk_package_root, self.sdk_archive, self.producer_closure,
                     self.producer_receipt, self.install_custody, self.source_admission_manifest)
            _require(all(isinstance(path, Path) and path.is_absolute() for path in paths))
            _require(type(self.timeout_seconds) is int and 0 < self.timeout_seconds <= 60)
            _require(type(self.maximum_output_bytes) is int and 65536 <= self.maximum_output_bytes <= 1048576)
        except (HostError, OSError, TypeError, AttributeError):
            raise HostError('typed-graph-sdk058-admission-configuration-refused') from None


def _custody(config):
    """Bind actual executable, archive producer closure and installed package bytes."""
    root = config.sdk_package_root
    entry = root / 'dist' / 'umf.js'
    package = root / 'package.json'
    manifest = root / 'manifest.json'
    files = [
        _pinned_file(config.node, 2 * 1024 * 1024, SDK058_NODE_SHA256, executable=True),
        _pinned_file(config.sdk_archive, 40 * 1024 * 1024, SDK058_ARCHIVE_SHA256),
        _pinned_file(config.producer_closure, 32768, SDK058_CLOSURE_SHA256),
        _pinned_file(config.producer_receipt, 1024 * 1024, SDK058_PRODUCER_RECEIPT_SHA256),
        _pinned_file(config.install_custody, 32768, SDK058_INSTALL_CUSTODY_SHA256),
        _pinned_file(entry, 20 * 1024 * 1024, SDK058_ENTRY_SHA256),
        _pinned_file(package, 32768, SDK058_PACKAGE_SHA256),
        _pinned_file(manifest, 512 * 1024, SDK058_MANIFEST_SHA256),
        _pinned_file(_CHECKER, 65536, _CHECKER_SHA256),
        _pinned_file(config.source_admission_manifest, 32768,
                     'd8b00a1819937beb68dc8442a88176e5a19ffaf6b61afc00e36082ae84828970'),
    ]
    closure = decode_retained_json(files[2][0])
    package_value = decode_retained_json(files[6][0])
    install = decode_retained_json(files[4][0])
    source_manifest = decode_retained_json(files[9][0])
    _require(type(closure) is dict and closure.get('status') == 'passed'
             and closure.get('sourceCommit') == SDK058_SOURCE_COMMIT
             and closure.get('sourceTree') == SDK058_SOURCE_TREE
             and closure.get('selectedSoftware', {}).get('sdkArchive', {}).get('sha256') == SDK058_ARCHIVE_SHA256
             and closure.get('selectedSoftware', {}).get('installedSdkPackage', {}).get('treeSha256') == SDK058_TREE_SHA256)
    _require(type(package_value) is dict and package_value.get('name') == '@umf/core'
             and package_value.get('version') == '0.5.8')
    _require(type(install) is dict and install.get('archiveSha256') == SDK058_ARCHIVE_SHA256
             and install.get('exactArchiveMembers', {}).get('package/dist/umf.js', {}).get('sha256') == SDK058_ENTRY_SHA256)
    _require(type(source_manifest) is dict and set(source_manifest) == _MANIFEST_KEYS
             and source_manifest.get('profile') == SDK058_ADMISSION_PROFILE
             and source_manifest.get('admissionProducerRevision') == SDK058_PRODUCER_REVISION
             and source_manifest.get('sourceProducerRevision') == GRAPH_CHECK_PIN
             and source_manifest.get('sdkVersion') == '0.5.8'
             and source_manifest.get('sdkTagCommit') == SDK058_TAG_COMMIT
             and source_manifest.get('sdkSourceCommit') == SDK058_SOURCE_COMMIT
             and source_manifest.get('sdkSourceTree') == SDK058_SOURCE_TREE
             and source_manifest.get('sdkArchiveSha256') == SDK058_ARCHIVE_SHA256
             and source_manifest.get('sdkEntrySha256') == SDK058_ENTRY_SHA256)
    return tuple(files), source_manifest


def admit_typed_graph_sdk058(config, source_configuration, model, request=None):
    """Run full request/source/Core correspondence against the pinned SDK 0.5.8.

    The source configuration retains its historical producer identity. Its
    explicit adjacent SDK058 manifest grants only a release-specific checker
    route; the old config loader and receipt default remain unchanged.
    """
    _require(type(config) is TypedGraphSdk058AdmissionConfig
             and type(model) is bytes and 0 < len(model) <= 65536)
    config.__post_init__()
    from ashlar.typed_graph_source_config import TypedGraphJsonlConfiguration
    _require(type(source_configuration) is TypedGraphJsonlConfiguration)
    source_configuration.verify()
    _require(source_configuration.producer_revision == GRAPH_CHECK_PIN
             and source_configuration.model_source == model)
    baseline, source_manifest = _custody(config)
    if request is None:
        request = build_typed_graph_request(source_configuration)
    _require(type(request) is bytes and 0 < len(request) <= 1048576)
    value = decode_retained_json(request)
    _require(type(value) is dict and set(value) == _KEYS
             and value['profile'] == 'ashlar-typed-graph-check-request/0.1'
             and value['configurationSha256'] == source_manifest['sourceConfigurationSha256']
             and value['sourceSha256'] == source_manifest['sourceSha256']
             and value['bindingSha256'] == source_manifest['bindingSha256']
             and value['modelSha256'] == source_manifest['modelSha256'] == _sha(model)
             and type(value['groups']) is list and 1 <= len(value['groups']) <= 4)
    _require(_sha(source_configuration.original) == source_manifest['sourceConfigurationSha256']
             and _sha(source_configuration.source_bytes) == source_manifest['sourceSha256']
             and _sha(source_configuration.binding_source) == source_manifest['bindingSha256'])
    _require(all(type(group) is dict and set(group) == {'batch', 'events', 'records', 'dataset'}
                 and type(group['events']) is list and type(group['records']) is list for group in value['groups']))
    _require(sum(len(group['events']) for group in value['groups']) <= 1000
             and 0 < sum(len(group['records']) for group in value['groups']) <= 1000)
    _source_bytes(value)

    primary = None
    response = None
    try:
        with owned_context(tempfile.TemporaryDirectory(prefix='ashlar-sdk058-typed-graph-')) as directory:
            root = Path(directory)
            root.chmod(0o700)
            schema, query = root / 'model.json', root / 'request.json'
            _require(schema.write_bytes(model) == len(model) and query.write_bytes(request) == len(request))
            schema.chmod(0o600); query.chmod(0o600)
            inner = None
            try:
                node, sdk_entry = config.node, config.sdk_package_root / 'dist' / 'umf.js'
                argv = [str(node), '--no-experimental-webstorage', str(_CHECKER), str(schema), str(query), str(sdk_entry)]
                env = {'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8', 'HOME': '/nonexistent',
                       'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': '/dev/null',
                       'GIT_TERMINAL_PROMPT': '0', 'GIT_OPTIONAL_LOCKS': '0'}
                response, stderr = capture(argv, cwd=root, environment=env,
                    timeout_seconds=config.timeout_seconds, maximum_output_bytes=config.maximum_output_bytes)
                _require(type(response) is bytes and 0 < len(response) <= min(4194304, config.maximum_output_bytes)
                         and stderr == b'')
                verify_typed_graph_receipt(model, request, response,
                    producer_revision=SDK058_PRODUCER_REVISION)
            except BaseException as error:
                inner = error
            finish(inner, [lambda: _require(snapshot(schema, 65536) == model),
                           lambda: _require(snapshot(query, 1048576) == request)])
    except BaseException as error:
        primary = error
    finish(primary, [lambda: _require(_custody(config)[0] == baseline)])
    return response


def admit_typed_graph_sdk058_policy(config, source_configuration, model, request=None):
    """Mint the explicit SDK 0.5.8 source policy after release-specific admission."""
    receipt = admit_typed_graph_sdk058(config, source_configuration, model, request)
    from ashlar.typed_graph_source import TypedGraphSourcePolicy
    return TypedGraphSourcePolicy(source_configuration, receipt, producer_revision=SDK058_PRODUCER_REVISION)
