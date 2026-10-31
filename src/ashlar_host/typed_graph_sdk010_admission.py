# SPDX-License-Identifier: MIT OR Apache-2.0
"""Explicit UMF SDK 0.10.0 typed-graph admission, separate from SDK058."""
import hashlib
import io
import json
import os
import stat
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path

from ashlar.schema import decode_retained_json
from ashlar.typed_graph_source import GRAPH_CHECK_PIN, build_typed_graph_request, verify_typed_graph_receipt
from .config import HostError
from .evolution_producer import capture, snapshot
from .lifecycle import finish, owned_context
from .typed_graph_admission import _KEYS, _source_bytes
from .local_current_delta4_umf_sdk010 import pinned_sdk010_resources

SDK010_PRODUCER_REVISION = 'umf-sdk-0.10.0/2fb63dea5ca85b32c6bad823703de526875ac79490234dbfea4a8863c7d5fe6c/b32bce2fdcf928bc5b9fd8376b4c7cab9beb1b63d90cb1d20b9ad0ec94595951'
SDK010_ARCHIVE_SHA256 = '2fb63dea5ca85b32c6bad823703de526875ac79490234dbfea4a8863c7d5fe6c'
SDK010_ENTRY_SHA256 = 'b32bce2fdcf928bc5b9fd8376b4c7cab9beb1b63d90cb1d20b9ad0ec94595951'
SDK010_NODE_SHA256 = '9af7e24478e538455cf4af53a2607f1b79304bd5e25458b041fb855c3c167991'
SDK010_TAG_COMMIT = '96230e0eef44b255d33575ba496402555b76800e'
SDK010_SOURCE_COMMIT = '96230e0eef44b255d33575ba496402555b76800e'
SDK010_SOURCE_TREE = '96230e0eef44b255d33575ba496402555b76800e'
SDK010_ADMISSION_PROFILE = 'ashlar-typed-graph-sdk010-source-admission/0.1'
_CHECKER = Path(__file__).parent / 'resources' / 'typed_graph_values_sdk010.mjs'
_MANIFEST = Path(__file__).parent / 'resources' / 'typed_graph_sdk010_source_admission.json'
SDK010_CHECKER_PATH = _CHECKER
SDK010_ADMISSION_MANIFEST_PATH = _MANIFEST
_MANIFEST_SHA256 = '19874c2e7d62a70fd226d5667176e203ed1e41438581aba9302437f71821ce87'
_CHECKER_SHA256 = '570c238d3fb870284b1de0d3664e4a138a2990b1ce337e9e4548abbb73dee8d5'
_MANIFEST_KEYS = {
    'profile', 'admissionProducerRevision', 'sourceProducerRevision', 'sourceConfigurationSha256',
    'sourceSha256', 'bindingSha256', 'modelSha256', 'sdkVersion', 'sdkTagCommit', 'sdkSourceCommit',
    'sdkSourceTree', 'sdkArchiveSha256', 'sdkEntrySha256',
}

def _require(value):
    if not value: raise HostError('typed-graph-sdk010-admission-refused')

def _sha(raw): return hashlib.sha256(raw).hexdigest()
def _identity(info): return (info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, info.st_ctime_ns)

def _pinned_file(path, maximum, expected_sha256, *, executable=False):
    _require(isinstance(path, Path) and path.is_absolute() and path.resolve() == path
             and not any(parent.is_symlink() for parent in (path, *path.parents)))
    before = path.lstat()
    _require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= maximum
             and (not executable or before.st_mode & 0o111))
    raw = snapshot(path, maximum)
    after = path.lstat()
    _require(_identity(before) == _identity(after) and _sha(raw) == expected_sha256)
    return raw, (str(path), _identity(after))

@dataclass(frozen=True)
class TypedGraphSdk010AdmissionConfig:
    """All SDK, runtime and typed-source admission inputs are explicit paths."""
    node: Path
    sdk_package_root: Path
    sdk_archive: Path
    timeout_seconds: int = 30
    maximum_output_bytes: int = 1048576
    def __post_init__(self):
        try:
            paths = (self.node, self.sdk_package_root, self.sdk_archive)
            _require(all(isinstance(path, Path) and path.is_absolute() and path.resolve() == path
                         and not any(parent.is_symlink() for parent in (path, *path.parents))
                         for path in paths))
            _require(type(self.timeout_seconds) is int and 0 < self.timeout_seconds <= 60)
            _require(type(self.maximum_output_bytes) is int and 65536 <= self.maximum_output_bytes <= 1048576)
        except (HostError, OSError, TypeError, AttributeError):
            raise HostError('typed-graph-sdk010-admission-configuration-refused') from None

def _archive_member(archive_bytes, name, maximum):
    with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode='r:gz') as archive:
        try: member = archive.getmember(name)
        except KeyError: _require(False)
        _require(member.isfile() and 0 < member.size <= maximum)
        stream = archive.extractfile(member); _require(stream is not None)
        raw = stream.read(maximum + 1)
        _require(len(raw) == member.size)
        return raw

def _custody(config):
    pinned_sdk010_resources()
    entry = config.sdk_package_root / 'dist' / 'umf.js'
    package = config.sdk_package_root / 'package.json'
    archive_file = _pinned_file(config.sdk_archive, 40 * 1024 * 1024, SDK010_ARCHIVE_SHA256)
    archive_entry = _archive_member(archive_file[0], 'package/dist/umf.js', 20 * 1024 * 1024)
    archive_package = _archive_member(archive_file[0], 'package/package.json', 32768)
    _require(_sha(archive_entry) == SDK010_ENTRY_SHA256)
    files = [
        _pinned_file(config.node, 2 * 1024 * 1024, SDK010_NODE_SHA256, executable=True),
        archive_file,
        _pinned_file(entry, 20 * 1024 * 1024, SDK010_ENTRY_SHA256),
        _pinned_file(package, 32768, _sha(archive_package)),
        _pinned_file(_CHECKER, 65536, _CHECKER_SHA256),
        _pinned_file(_MANIFEST, 32768, _MANIFEST_SHA256),
    ]
    package_value = decode_retained_json(files[3][0]); source_manifest = decode_retained_json(files[5][0])
    _require(package_value.get('name') == '@umf/core' and package_value.get('version') == '0.10.0'
             and archive_entry == files[2][0])
    _require(type(source_manifest) is dict and set(source_manifest) == _MANIFEST_KEYS
             and source_manifest.get('profile') == SDK010_ADMISSION_PROFILE
             and source_manifest.get('admissionProducerRevision') == SDK010_PRODUCER_REVISION
             and source_manifest.get('sourceProducerRevision') == GRAPH_CHECK_PIN
             and source_manifest.get('sdkVersion') == '0.10.0'
             and source_manifest.get('sdkTagCommit') == SDK010_TAG_COMMIT
             and source_manifest.get('sdkSourceCommit') == SDK010_SOURCE_COMMIT
             and source_manifest.get('sdkSourceTree') == SDK010_SOURCE_TREE
             and source_manifest.get('sdkArchiveSha256') == SDK010_ARCHIVE_SHA256
             and source_manifest.get('sdkEntrySha256') == SDK010_ENTRY_SHA256)
    return tuple(files), source_manifest

def admit_typed_graph_sdk010(config, source_configuration, model, request=None):
    """Run complete typed-source/Core correspondence using the pinned SDK 0.10.0."""
    _require(type(config) is TypedGraphSdk010AdmissionConfig
             and type(model) is bytes and 0 < len(model) <= 65536)
    config.__post_init__()
    from ashlar.typed_graph_source_config import TypedGraphJsonlConfiguration
    _require(type(source_configuration) is TypedGraphJsonlConfiguration)
    source_configuration.verify()
    _require(source_configuration.producer_revision == GRAPH_CHECK_PIN
             and source_configuration.model_source == model)
    baseline, source_manifest = _custody(config)
    if request is None: request = build_typed_graph_request(source_configuration)
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
    primary = None; response = None
    try:
        with owned_context(tempfile.TemporaryDirectory(prefix='ashlar-sdk010-typed-graph-')) as directory:
            from pathlib import Path
            root = Path(directory); root.chmod(0o700)
            schema, query = root / 'model.json', root / 'request.json'
            _require(schema.write_bytes(model) == len(model) and query.write_bytes(request) == len(request))
            schema.chmod(0o600); query.chmod(0o600)
            inner = None
            try:
                argv = [str(config.node), '--no-experimental-webstorage', str(_CHECKER), str(schema), str(query), str(config.sdk_package_root / 'dist' / 'umf.js')]
                env = {'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8', 'HOME': '/nonexistent',
                       'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': '/dev/null',
                       'GIT_TERMINAL_PROMPT': '0', 'GIT_OPTIONAL_LOCKS': '0'}
                response, stderr = capture(argv, cwd=root, environment=env,
                    timeout_seconds=config.timeout_seconds, maximum_output_bytes=config.maximum_output_bytes)
                _require(type(response) is bytes and 0 < len(response) <= min(4194304, config.maximum_output_bytes)
                         and stderr == b'')
                verify_typed_graph_receipt(model, request, response, producer_revision=SDK010_PRODUCER_REVISION)
            except BaseException as error: inner = error
            finish(inner, [lambda: _require(snapshot(schema, 65536) == model),
                           lambda: _require(snapshot(query, 1048576) == request)])
    except BaseException as error: primary = error
    finish(primary, [lambda: _require(_custody(config)[0] == baseline)])
    return response

def admit_typed_graph_sdk010_policy(config, source_configuration, model, request=None):
    receipt = admit_typed_graph_sdk010(config, source_configuration, model, request)
    from ashlar.typed_graph_source import TypedGraphSourcePolicy
    return TypedGraphSourcePolicy(source_configuration, receipt, producer_revision=SDK010_PRODUCER_REVISION)
