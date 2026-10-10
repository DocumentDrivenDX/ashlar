"""Explicit local installation of an independently indexed Weft realization.

Index trust is injected by application composition. Inert manifests, passing
checks and executable bytes cannot register themselves. This module never opens
native engines, downloads artifacts or discharges compiler host obligations.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tempfile
import threading
import zlib
from typing import Optional

JSON_LIMIT = 4 * 1024 * 1024
FILE_LIMIT = 32 * 1024 * 1024
TOTAL_LIMIT = 96 * 1024 * 1024
DECODED_LIMIT = 20 * 1024 * 1024
PROTOCOL_LIMIT = 16 * 1024 * 1024
SAFE_INTEGER = 9007199254740991
CONTROL_IDS = ('candidate-opt-out', 'unknown-backend', 'wrong-backend-version', 'wrong-profile',
               'unknown-envelope-member', 'mismatched-interface-dialect', 'binding-digest-mismatch',
               'model-digest-mismatch', 'sql-byte-limit', 'binding-byte-limit', 'duplicate-envelope-member',
               'missing-publication', 'negative-table-version', 'duplicate-table-uuid', 'missing-table-mapping',
               'inconsistent-model-pin', 'fresh-binding-0', 'fresh-binding-1', 'fresh-binding-2')
SCOPES = ('columns-native', 'application-native', 'key-refusal', 'unsigned-columns', 'optional-native',
          'relationship-native', 'compound-native', 'compound-boundaries-native', 'compound-application-native')


class InstallationError(ValueError):
    """Bounded installation refusal; callers must not select a fallback."""


def _refuse():
    raise InstallationError('ASHLAR-WEFT-INSTALLATION-REFUSED')


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            _refuse()
        result[key] = value
    return result


def _json(raw):
    try:
        return json.loads(raw, object_pairs_hook=_pairs, parse_constant=lambda _: _refuse())
    except (ValueError, UnicodeError, RecursionError):
        _refuse()


def _closed(value, keys):
    if type(value) is not dict or set(value) != set(keys):
        _refuse()


def _text(value, limit=4096):
    if type(value) is not str or not 1 <= len(value) <= limit or any(ord(c) < 32 or ord(c) == 127 for c in value):
        _refuse()


def _number(value, minimum=0, maximum=SAFE_INTEGER):
    if type(value) is not int or not minimum <= value <= maximum:
        _refuse()


def _digest(value, digits=64):
    if type(value) is not str or re.fullmatch('[0-9a-f]{' + str(digits) + '}', value) is None:
        _refuse()


def _relative(path):
    _text(path)
    if path.startswith('/') or '\\' in path or ':' in path or any(p in ('', '.', '..') for p in path.split('/')):
        _refuse()
    return path


def _no_symlink(path):
    if any(p.is_symlink() for p in (path, *path.parents)):
        _refuse()


def _read(path, limit=FILE_LIMIT):
    _no_symlink(path)
    if not path.is_file():
        _refuse()
    with path.open('rb') as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        _refuse()
    return raw


def _descriptor(value):
    _closed(value, ('path', 'sha256', 'bytes'))
    _relative(value['path'])
    _digest(value['sha256'])
    _number(value['bytes'], maximum=FILE_LIMIT)
    return value


def _inflate(raw, limit=DECODED_LIMIT):
    try:
        decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
        result = decoder.decompress(raw, limit + 1)
        if len(result) > limit or decoder.unconsumed_tail or not decoder.eof or decoder.unused_data:
            _refuse()
        return result
    except zlib.error:
        _refuse()


def _rows(raw):
    return [_json(line) for line in raw.splitlines() if line]


@dataclass(frozen=True)
class InstallationConfig:
    """All authority and host observations come from trusted composition.

    observed_target/observed_os are actual independently obtained host settings,
    not user-selected overrides or guessed defaults. Initial admission is limited
    to the exact observed platform recorded by this qualified realization.
    """
    index_path: Path
    index_revision: str
    index_sha256: str
    package: Optional[Path]
    realization_id: str
    output: Path
    observed_target: str
    observed_os: str

    def __post_init__(self):
        for key in ('index_path', 'package', 'output'):
            value = getattr(self, key)
            if key == 'package' and value is None:
                continue
            if not isinstance(value, Path):
                _refuse()
            object.__setattr__(self, key, value.absolute())
        _digest(self.index_revision, 40)
        _digest(self.index_sha256)
        _text(self.realization_id, 256)
        if re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]*', self.realization_id) is None:
            _refuse()
        _text(self.observed_target, 256)
        _text(self.observed_os, 256)


class _Package:
    def __init__(self, root):
        self.root = root
        self.raw = {}
        self.total = 0

    def artifact(self, desc, limit=FILE_LIMIT):
        desc = _descriptor(desc)
        if desc['bytes'] > limit: _refuse()
        if desc['path'] not in self.raw and self.total + desc['bytes'] > TOTAL_LIMIT:
            _refuse()
        path = self.root / desc['path']
        _no_symlink(path)
        path.absolute().relative_to(self.root)
        raw = _read(path, limit)
        if len(raw) != desc['bytes'] or _sha(raw) != desc['sha256']:
            _refuse()
        if desc['path'] in self.raw and self.raw[desc['path']] != raw:
            _refuse()
        if desc['path'] not in self.raw:
            self.total += len(raw)
            if self.total > TOTAL_LIMIT:
                _refuse()
        self.raw[desc['path']] = raw
        return raw

    def close(self):
        for path, raw in self.raw.items():
            if _read(self.root / path) != raw:
                _refuse()
        _exact_tree(self.root, self.raw)


def _exact_tree(root, paths):
    """Visit only the admitted trie; extras refuse before descent or file reads."""
    trie = {}
    nodes = 0
    for path in paths:
        parts = _relative(path).split('/')
        if len(parts) > 64: _refuse()
        branch = trie
        for part in parts[:-1]:
            if part in branch and branch[part] is None: _refuse()
            if part not in branch:
                nodes += 1
                if nodes > 8192: _refuse()
                branch[part] = {}
            branch = branch[part]
        if parts[-1] in branch: _refuse()
        branch[parts[-1]] = None
        nodes += 1
        if nodes > 8192: _refuse()
    pending = [(root, trie)]
    while pending:
        directory, expected = pending.pop()
        _no_symlink(directory)
        seen = set()
        with os.scandir(directory) as entries:
            for entry in entries:
                # Enumeration stops at the very first extra, rather than
                # collecting or recursing through an untrusted directory.
                if entry.name not in expected or entry.name in seen: _refuse()
                seen.add(entry.name)
                mode = entry.stat(follow_symlinks=False).st_mode
                child = expected[entry.name]
                if child is None:
                    if not stat.S_ISREG(mode): _refuse()
                else:
                    if not stat.S_ISDIR(mode): _refuse()
                    pending.append((directory / entry.name, child))
        if seen != set(expected): _refuse()


def _index(config):
    # No caller manifest/package is inspected until the trusted pin matches.
    raw = _read(config.index_path, JSON_LIMIT)
    if _sha(raw) != config.index_sha256:
        _refuse()
    value = _json(raw)
    _closed(value, ('format', 'entries'))
    if value['format'] != 'weft-distribution-index/0.1' or type(value['entries']) is not list or not 1 <= len(value['entries']) <= 1000:
        _refuse()
    identities = []
    selected = None
    for entry in value['entries']:
        _closed(entry, ('realizationId', 'manifest', 'assemblyCustody', 'executable', 'target'))
        _text(entry['realizationId'], 256)
        _descriptor(entry['manifest']); _descriptor(entry['assemblyCustody']); _descriptor(entry['executable'])
        if entry['assemblyCustody']['path'] != 'assembly-custody.json': _refuse()
        _text(entry['target'], 256)
        identities.append(entry['realizationId'])
        if entry['realizationId'] == config.realization_id:
            selected = entry
    if len(set(identities)) != len(identities) or selected is None:
        _refuse()
    if selected['target'] != config.observed_target or selected['target'] != 'aarch64-apple-darwin':
        _refuse()
    return raw, selected


def _manifest(value):
    _closed(value, ('format', 'realizationId', 'source', 'build', 'executable', 'backendManifests', 'publicSchemas', 'conformance'))
    if value['format'] != 'weft-distribution/0.1':
        _refuse()
    _text(value['realizationId'], 256)
    _closed(value['source'], ('commit', 'inventory'))
    _digest(value['source']['commit'], 40)
    inventory = value['source']['inventory']
    _closed(inventory, ('artifact', 'decodedSha256', 'decodedBytes', 'trackedFiles'))
    _descriptor(inventory['artifact']); _digest(inventory['decodedSha256'])
    _number(inventory['decodedBytes'], 1, JSON_LIMIT); _number(inventory['trackedFiles'], 1, 20000)
    build = value['build']
    _closed(build, ('release', 'target', 'features', 'command', 'tools', 'lockfiles', 'toolchain', 'effectiveEnvironment', 'platform'))
    if build['release'] is not True or build['target'] != 'aarch64-apple-darwin' or build['features'] != ['ashlar-databricks-candidate']:
        _refuse()
    if build['command'] != ['cargo', 'build', '-j1', '-p', 'weft-runtime', '--bin', 'weft-runtime', '--release', '--no-default-features', '--features', 'ashlar-databricks-candidate', '--offline', '--locked']:
        _refuse()
    for field in ('tools', 'toolchain'): _descriptor(build[field])
    for field in ('lockfiles',):
        if type(build[field]) is not list or not build[field]: _refuse()
        for item in build[field]: _descriptor(item)
        if [d['path'] for d in build[field]] != sorted(set(d['path'] for d in build[field])): _refuse()
    environment = build['effectiveEnvironment']
    _closed(environment, ('observed', 'unknowns'))
    _closed(environment['observed'], ('CARGO_HOME', 'RUSTUP_HOME', 'CARGO_TARGET_DIR', 'PATHPrefix'))
    for text in environment['observed'].values(): _text(text)
    if type(environment['unknowns']) is not list or not environment['unknowns'] or len(set(environment['unknowns'])) != len(environment['unknowns']): _refuse()
    for text in environment['unknowns']: _text(text)
    _closed(build['platform'], ('binaryFormat', 'machine', 'minimumOS', 'sdk', 'observedOS'))
    if build['platform'] != {'binaryFormat': 'mach-o', 'machine': 'arm64', 'minimumOS': '11.0', 'sdk': '27.0', 'observedOS': '27.0.1'}: _refuse()
    _descriptor(value['executable'])
    for field in ('backendManifests', 'publicSchemas'):
        if type(value[field]) is not list or not value[field]: _refuse()
        for desc in value[field]: _descriptor(desc)
        if [d['path'] for d in value[field]] != sorted(set(d['path'] for d in value[field])): _refuse()
    if len(value['backendManifests']) != 1: _refuse()
    _closed(value['conformance'], ('corpus', 'controls', 'transport'))
    for key in ('corpus', 'controls'):
        required = ('cases', 'responses', 'summary', 'custody') + (('compiled', 'blocked') if key == 'corpus' else ())
        _closed(value['conformance'][key], required)
        item = value['conformance'][key]
        _number(item['cases'], 1)
        for field in ('responses', 'summary', 'custody'): _descriptor(item[field])
    corpus = value['conformance']['corpus']
    _number(corpus['compiled']); _number(corpus['blocked'])
    if (corpus['cases'], corpus['compiled'], corpus['blocked']) != (463, 462, 1) or corpus['cases'] != corpus['compiled'] + corpus['blocked'] or value['conformance']['controls']['cases'] != 19: _refuse()
    _descriptor(value['conformance']['transport'])


def _source_and_proof(package, manifest, manifest_raw, entry):
    proof_path = 'assembly-custody.json'
    proof_raw = package.artifact(entry['assemblyCustody'], JSON_LIMIT)
    if len(proof_raw) > JSON_LIMIT: _refuse()
    # The independently indexed exact manifest references the receipts/source;
    # this auxiliary closure is checked against every corresponding descriptor.
    proof = _json(proof_raw)
    _closed(proof, ('format', 'sourceCommit', 'qualification', 'producerSha256', 'artifacts', 'sourceSubset'))
    if proof['format'] != 'weft-distribution-assembly-custody/0.1' or proof['sourceCommit'] != manifest['source']['commit']: _refuse()
    _text(proof['qualification']); _digest(proof['producerSha256'])
    if type(proof['artifacts']) is not list or not 1 <= len(proof['artifacts']) <= 4096: _refuse()
    descriptors = {}
    for desc in proof['artifacts']:
        _descriptor(desc)
        if desc['path'] in descriptors or desc['path'] == proof_path: _refuse()
        descriptors[desc['path']] = desc
        package.artifact(desc)
    if list(descriptors) != sorted(descriptors): _refuse()
    package.raw[proof_path] = proof_raw
    if manifest_raw != package.raw.get('manifest.json'): _refuse()
    inventory = manifest['source']['inventory']
    decoded = _inflate(package.artifact(inventory['artifact']), JSON_LIMIT)
    if len(decoded) != inventory['decodedBytes'] or _sha(decoded) != inventory['decodedSha256']: _refuse()
    entries = _json(decoded)
    if type(entries) is not list or len(entries) != inventory['trackedFiles']: _refuse()
    source = {}
    for item in entries:
        _closed(item, ('path', 'mode', 'gitBlob', 'sha256', 'bytes'))
        _relative(item['path']); _digest(item['sha256']); _digest(item['gitBlob'], 40)
        _number(item['bytes'], maximum=FILE_LIMIT)
        if item['mode'] not in ('100644', '100755') or item['path'] in source: _refuse()
        source[item['path']] = item
    if list(source) != sorted(source): _refuse()
    actual_subset = sorted(path[len('source-subset/'):] for path in package.raw if path.startswith('source-subset/'))
    if proof['sourceSubset'] != actual_subset: _refuse()
    for path in actual_subset:
        if path not in source: _refuse()
        raw = package.raw['source-subset/' + path]
        expected = source[path]
        if len(raw) != expected['bytes'] or _sha(raw) != expected['sha256'] or hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest() != expected['gitBlob']: _refuse()
    schemas = ['source-subset/' + p for p in source if p.startswith('docs/helix/02-design/contracts/') and p.endswith('.schema.json')]
    if [d['path'] for d in manifest['publicSchemas']] != schemas or len(schemas) != 13: _refuse()
    for desc in manifest['publicSchemas'] + manifest['backendManifests'] + manifest['build']['lockfiles'] + [manifest['build']['tools'], manifest['build']['toolchain'], manifest['executable']]:
        if descriptors.get(desc['path']) != desc: _refuse()
        package.artifact(desc)
    if manifest['build']['lockfiles'][0]['path'] != 'source-subset/Cargo.lock' or len(manifest['build']['lockfiles']) != 1 or manifest['build']['toolchain']['path'] != 'source-subset/rust-toolchain.toml': _refuse()
    for scope in ('cli-produced-corpus-20261009', 'cli-candidate-controls-20261009'):
        custody = _json(package.raw['evidence/' + scope + '/custody.json'])
        if custody.get('sourceCommit', custody.get('compilerSourceCommit')) != manifest['source']['commit'] or custody['binarySha256'] != manifest['executable']['sha256']: _refuse()
        for desc in custody['reports']:
            _descriptor(desc)
            referenced = {**desc, 'path': 'evidence/' + scope + '/' + desc['path']}
            if descriptors.get(referenced['path']) != referenced: _refuse()
    metadata = _json(package.raw['evidence/source-backend-metadata-20261009/custody.json'])
    backend_raw = package.artifact(manifest['backendManifests'][0])
    if metadata['sourceCommit'] != manifest['source']['commit'] or metadata['metadataSha256'] != _sha(backend_raw) or metadata['metadataBytes'] != len(backend_raw) or metadata['sourceInventoryDecodedSha256'] != _sha(decoded) or metadata['sourceInventoryDecodedBytes'] != len(decoded) or metadata['sourceTrackedFiles'] != len(source) or metadata['sourceInventoryCompressedSha256'] != inventory['artifact']['sha256']: _refuse()
    if metadata['exporterSha256'] != _sha(package.raw['producer/scripts/distribution/backend-metadata.rs']) or metadata['harnessSha256'] != _sha(package.raw['producer/scripts/distribution/describe-backend.py']) or proof['producerSha256'] != _sha(package.raw['producer/scripts/distribution/assemble-distribution.py']): _refuse()
    backend = _json(backend_raw)
    if backend['backendId'] != 'ashlar.databricks' or backend['backendVersion'] != '0.1.0-candidate' or backend['interfaceVersion'] != 'weft-backend/0.2.0': _refuse()
    return descriptors


def _conformance(package, manifest, descriptors):
    scopes = manifest['conformance']
    for item in (scopes['corpus'], scopes['controls']):
        for key in ('responses', 'summary', 'custody'):
            if descriptors.get(item[key]['path']) != item[key]: _refuse()
    if descriptors.get(scopes['transport']['path']) != scopes['transport']: _refuse()
    corpus = scopes['corpus']
    summary = _json(package.artifact(corpus['summary']))
    custody = _json(package.artifact(corpus['custody']))
    raw = _inflate(package.artifact(corpus['responses']))
    if _sha(raw) != custody['caseReportDecodedSha256'] or len(raw) != custody['caseReportDecodedBytes']: _refuse()
    records = _rows(raw)
    originals = []
    base = 'source-subset/docs/helix/04-build/evidence/'
    paths = []
    for scope in SCOPES:
        path = base + 'B-006-' + scope + '/compile-artifacts.jsonl'
        paths.append(path[len('source-subset/'):])
        originals.extend((scope + ':' + str(item['id']), item) for item in _rows(package.raw[path]))
    for scope in ('scalar-native', 'global-native'):
        for path in sorted(p for p in package.raw if p.startswith(base + 'B-006-' + scope + '/') and p.endswith('-compile.json')):
            paths.append(path[len('source-subset/'):]); originals.append((scope + ':' + Path(path).stem, _json(package.raw[path])))
    path = base + 'B-006-cross-module-native/compile.json'
    paths.append(path[len('source-subset/'):]); originals.append(('cross-module', _json(package.raw[path])))
    if paths != [item['path'] for item in summary['sourceFiles']]: _refuse()
    for item in summary['sourceFiles']:
        if _sha(package.raw['source-subset/' + item['path']]) != item['sha256']: _refuse()
    ids = [row['id'] for row in records]
    if len(records) != corpus['cases'] or ids != [identity for identity, _ in originals] or len(set(ids)) != len(ids): _refuse()
    counts = {'compiled': 0, 'blocked': 0}
    for record, (_, original) in zip(records, originals):
        _closed(record, ('id', 'requestSha256', 'exit', 'stdoutSha256', 'stderrSha256', 'stdout', 'stderr'))
        if record['requestSha256'] != _sha(_encode(original['request'])) or type(record['exit']) is not int or record['exit'] != 0 or record['stderr'] or _sha(record['stdout'].encode()) != record['stdoutSha256'] or _sha(record['stderr'].encode()) != record['stderrSha256'] or record['stdout'].count('\n') != 1 or not record['stdout'].endswith('\n'): _refuse()
        status = _json(record['stdout'])['status']
        if status not in counts: _refuse()
        counts[status] += 1
    if counts != {'compiled': corpus['compiled'], 'blocked': corpus['blocked']} or any(summary.get(k) is not True for k in ('caseIdsUnique', 'sourceFilesUnchanged', 'fullResponseParity', 'deterministic')) or summary['cases'] != corpus['cases']: _refuse()
    binary = manifest['executable']['sha256']
    if any(summary[k] != binary for k in ('openingBinarySha256', 'closingBinarySha256', 'binarySha256')): _refuse()
    if _json(package.raw['evidence/cli-produced-corpus-20261009/mismatches.json']) != []: _refuse()
    if custody['build']['command'] != manifest['build']['command'] or custody['build']['target'] != manifest['build']['target'] or custody['build']['effectiveEnvironment'] != manifest['build']['effectiveEnvironment']['observed'] or custody['build']['unknowns'] != manifest['build']['effectiveEnvironment']['unknowns']: _refuse()
    controls = scopes['controls']
    c_custody = _json(package.artifact(controls['custody']))
    c_summary = _json(package.artifact(controls['summary']))
    c_raw = _inflate(package.artifact(controls['responses']))
    if len(c_raw) != c_custody['decodedCaseBytes'] or _sha(c_raw) != c_custody['decodedCaseSha256']: _refuse()
    rows = _rows(c_raw)
    if [r['id'] for r in rows] != list(CONTROL_IDS) or len(rows) != controls['cases'] or c_summary['cases'] != controls['cases'] or c_summary['blockedCases'] != 16 or c_summary['compiledCases'] != 3 or c_summary['custodyClosed'] is not True or c_summary['binarySha256'] != binary: _refuse()
    for row in rows:
        _closed(row, ('id', 'requestSha256', 'requestJson', 'stdout', 'stderr', 'exit', 'expectedCode'))
        if _sha(row['requestJson'].encode()) != row['requestSha256'] or type(row['exit']) is not int or row['exit'] != 0 or row['stderr'] or row['stdout'].count('\n') != 1 or not row['stdout'].endswith('\n'): _refuse()
        response = _json(row['stdout'])
        if row['expectedCode']:
            if response['status'] != 'blocked' or len(response['diagnostics']) != 1 or response['diagnostics'][0]['code'] != row['expectedCode']: _refuse()
        elif response['status'] != 'compiled': _refuse()
    for receipt in (custody, c_custody):
        harness = receipt['harness']
        if _sha(package.raw['producer/' + harness['path']]) != harness['sha256'] or len(package.raw['producer/' + harness['path']]) != harness['bytes']: _refuse()
    if _sha(package.raw['source-subset/' + c_custody['publicResponseChecker']['path']]) != c_custody['publicResponseChecker']['sha256'] or _sha(package.raw[base + 'B-006-cross-module-native/compile.json']) != c_summary['sourceSha256']: _refuse()
    transport = _json(package.artifact(scopes['transport']))
    _closed(transport, ('binarySha256', 'controls', 'qualification'))
    originals = summary['controls'] + _json(package.raw['evidence/cli-produced-corpus-20261009/io-controls.json'])['controls']
    if transport['controls'] != originals or transport['binarySha256'] != binary or [r['id'] for r in originals] != ['oversize', 'invalid-utf8', 'split-utf8-at-limit', 'exact-limit', 'exact-limit-multibyte', 'malformed-json', 'directory-input', 'closed-output']: _refuse()


@dataclass(frozen=True)
class VerifiedPackage:
    """Opaque byte snapshot returned only after full package admission."""
    manifest_bytes: bytes
    executable_bytes: bytes
    provenance_bytes: bytes


def inspect_package(config: InstallationConfig) -> VerifiedPackage:
    try:
        if not isinstance(config.package, Path): _refuse()
        index_raw, entry = _index(config)
        _no_symlink(config.package)
        package = _Package(config.package)
        manifest_raw = package.artifact(entry['manifest'], JSON_LIMIT)
        if len(manifest_raw) > JSON_LIMIT: _refuse()
        manifest = _json(manifest_raw); _manifest(manifest)
        if manifest['realizationId'] != config.realization_id or manifest['executable'] != entry['executable'] or manifest['build']['target'] != entry['target'] or manifest['build']['platform']['observedOS'] != config.observed_os: _refuse()
        descriptors = _source_and_proof(package, manifest, manifest_raw, entry)
        _conformance(package, manifest, descriptors)
        executable = package.artifact(manifest['executable'])
        # Thin little-endian 64-bit Mach-O arm64 executable, not a script/fat/x86
        # artifact. OS qualification is the explicit observed composition above.
        import struct
        if len(executable) < 32 or struct.unpack('<IIIIIIII', executable[:32])[:4] != (0xfeedfacf, 0x100000c, 0, 2): _refuse()
        package.close()
        if _read(config.index_path, JSON_LIMIT) != index_raw: _refuse()
        provenance = {'format': 'ashlar-weft-installation-provenance/0.1', 'indexRevision': config.index_revision, 'indexSha256': config.index_sha256,
                      'realizationId': config.realization_id, 'target': config.observed_target, 'observedOS': config.observed_os,
                      'manifest': entry['manifest'], 'assemblyCustody': entry['assemblyCustody'], 'executable': entry['executable'],
                      'manifestJson': manifest, 'manifestHex': manifest_raw.hex(),
                      'assemblyCustodyJson': _json(package.raw['assembly-custody.json']), 'assemblyCustodyHex': package.raw['assembly-custody.json'].hex(),
                      'verifiedArtifacts': [descriptors[p] for p in sorted(descriptors)],
                      'qualification': 'Trusted indexed byte realization only; no source/authorization/publication/stored-value host obligation discharged. Complete source rebuild requires separately checked original Git commit.'}
        provenance_raw = _encode(provenance) + b'\n'
        if len(provenance_raw) > JSON_LIMIT: _refuse()
        return VerifiedPackage(manifest_raw, executable, provenance_raw)
    except (KeyError, TypeError, OSError, ValueError, RecursionError):
        _refuse()


@dataclass(frozen=True)
class Installation:
    config: InstallationConfig
    executable_sha256: str
    executable_bytes: int
    provenance_sha256: str
    ready_bytes: bytes
    cleanup_pending: bool = field(default=False, compare=False)


def _cleanup_owned(path, identity):
    if identity is None or path.is_symlink() or not path.is_dir():
        return
    current = path.stat()
    if (current.st_dev, current.st_ino) == identity:
        # Invalidate availability even if later directory cleanup is denied.
        try:
            (path / 'ready.json').unlink(missing_ok=True)
            shutil.rmtree(path)
        except OSError:
            return


def install(config: InstallationConfig) -> Installation:
    verified = inspect_package(config)
    _no_symlink(config.output)
    if config.output.exists(): _refuse()
    parent = config.output.parent
    if not parent.is_dir(): _refuse()
    staging = Path(tempfile.mkdtemp(prefix='.ashlar-weft-', dir=parent))
    owned = None
    try:
        exe = staging / 'weft-runtime'; provenance = staging / 'provenance.json'
        for path, raw, mode in ((exe, verified.executable_bytes, 0o555), (provenance, verified.provenance_bytes, 0o444)):
            with path.open('xb') as stream: stream.write(raw)
            path.chmod(mode)
            if _read(path) != raw: _refuse()
        ready = {'format': 'ashlar-weft-installation-ready/0.1', 'indexRevision': config.index_revision, 'indexSha256': config.index_sha256,
                 'realizationId': config.realization_id, 'target': config.observed_target, 'observedOS': config.observed_os,
                 'executable': {'path': 'weft-runtime', 'sha256': _sha(verified.executable_bytes), 'bytes': len(verified.executable_bytes)},
                 'provenance': {'path': 'provenance.json', 'sha256': _sha(verified.provenance_bytes), 'bytes': len(verified.provenance_bytes)}}
        ready_raw = _encode(ready) + b'\n'
        (staging / 'ready.json').write_bytes(ready_raw); (staging / 'ready.json').chmod(0o444)
        config.output.mkdir(exist_ok=False)
        stat = config.output.stat(); owned = (stat.st_dev, stat.st_ino)
        for name in ('weft-runtime', 'provenance.json'):
            shutil.copyfile(staging / name, config.output / name)
            (config.output / name).chmod(0o555 if name == 'weft-runtime' else 0o444)
            if _read(config.output / name) != _read(staging / name): _refuse()
        # Every mandatory check precedes the final availability commit.
        if _read(staging / 'ready.json', JSON_LIMIT) != ready_raw: _refuse()
        result = _verify_installation(config, ready_raw, ('weft-runtime', 'provenance.json'))
        os.link(staging / 'ready.json', config.output / 'ready.json')
    except (OSError, ValueError, KeyError, TypeError):
        _cleanup_owned(config.output, owned)
        try:
            shutil.rmtree(staging)
        except OSError:
            pass  # An incomplete owned directory is never available.
        _refuse()
    # Ready publication is the commit. Post-commit staging housekeeping is
    # best effort, never rollback or a false installation failure. The returned
    # maintenance flag is not availability/identity and need not survive restart.
    try:
        shutil.rmtree(staging)
    except OSError:
        return replace(result, cleanup_pending=True)
    return result


def open_installation(config: InstallationConfig) -> Installation:
    try:
        _index(config)  # Trusted pin gate precedes caller-controlled ready bytes.
        return _verify_installation(config, _read(config.output / 'ready.json', JSON_LIMIT), ('weft-runtime', 'provenance.json', 'ready.json'))
    except (OSError, ValueError, KeyError, TypeError, RecursionError):
        _refuse()


def _verify_installation(config, ready_raw, names):
    try:
        index_raw, entry = _index(config)
        _no_symlink(config.output)
        ready = _json(ready_raw)
        _closed(ready, ('format', 'indexRevision', 'indexSha256', 'realizationId', 'target', 'observedOS', 'executable', 'provenance'))
        if ready['format'] != 'ashlar-weft-installation-ready/0.1' or ready['indexRevision'] != config.index_revision or ready['indexSha256'] != config.index_sha256 or ready['realizationId'] != config.realization_id or ready['target'] != config.observed_target or ready['observedOS'] != config.observed_os or ready['executable'] != {**entry['executable'], 'path': 'weft-runtime'}: _refuse()
        _descriptor(ready['provenance'])
        if ready['provenance']['path'] != 'provenance.json': _refuse()
        exe = _read(config.output / 'weft-runtime')
        provenance_raw = _read(config.output / 'provenance.json', JSON_LIMIT)
        if len(exe) != ready['executable']['bytes'] or _sha(exe) != ready['executable']['sha256'] or len(provenance_raw) != ready['provenance']['bytes'] or _sha(provenance_raw) != ready['provenance']['sha256'] or (config.output / 'weft-runtime').stat().st_mode & 0o777 != 0o555: _refuse()
        provenance = _json(provenance_raw)
        _closed(provenance, ('format', 'indexRevision', 'indexSha256', 'realizationId', 'target', 'observedOS', 'manifest', 'assemblyCustody', 'executable', 'manifestJson', 'manifestHex', 'assemblyCustodyJson', 'assemblyCustodyHex', 'verifiedArtifacts', 'qualification'))
        if provenance['format'] != 'ashlar-weft-installation-provenance/0.1': _refuse()
        manifest_original = bytes.fromhex(provenance['manifestHex'])
        assembly_original = bytes.fromhex(provenance['assemblyCustodyHex'])
        if _json(manifest_original) != provenance['manifestJson'] or _json(assembly_original) != provenance['assemblyCustodyJson']: _refuse()
        if provenance['indexRevision'] != config.index_revision or provenance['indexSha256'] != config.index_sha256 or provenance['realizationId'] != config.realization_id or provenance['target'] != config.observed_target or provenance['observedOS'] != config.observed_os or provenance['manifest'] != entry['manifest'] or provenance['assemblyCustody'] != entry['assemblyCustody'] or provenance['executable'] != entry['executable'] or _sha(manifest_original) != entry['manifest']['sha256'] or len(manifest_original) != entry['manifest']['bytes']: _refuse()
        _manifest(provenance['manifestJson'])
        if provenance['manifestJson']['build']['platform']['observedOS'] != config.observed_os or _sha(assembly_original) != entry['assemblyCustody']['sha256'] or len(assembly_original) != entry['assemblyCustody']['bytes'] or provenance['verifiedArtifacts'] != provenance['assemblyCustodyJson']['artifacts']: _refuse()
        _exact_tree(config.output, names)
        if _read(config.index_path, JSON_LIMIT) != index_raw: _refuse()
        return Installation(config, ready['executable']['sha256'], len(exe), ready['provenance']['sha256'], ready_raw)
    except (OSError, ValueError, KeyError, TypeError, RecursionError):
        _refuse()


def compile_request(installation: Installation, request: bytes) -> bytes:
    """Return original stdout bytes; execution does not discharge host guards."""
    if not isinstance(installation, Installation) or type(request) is not bytes or len(request) > PROTOCOL_LIMIT:
        _refuse()
    opening = open_installation(installation.config)
    if opening != installation: _refuse()
    try:
        process = subprocess.Popen([str(installation.config.output / 'weft-runtime')], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError:
        _refuse()
    stdout, stderr, failure = bytearray(), bytearray(), []
    def read(stream, target, maximum):
        try:
            while True:
                chunk = stream.read(min(65536, maximum + 1 - len(target)))
                if not chunk: break
                target.extend(chunk)
                if len(target) > maximum:
                    failure.append(True); process.kill(); break
        except (OSError, ValueError): failure.append(True)
    def write():
        try:
            process.stdin.write(request); process.stdin.close()
        except (OSError, ValueError): failure.append(True)
    threads = [threading.Thread(target=read, args=(process.stdout, stdout, PROTOCOL_LIMIT)), threading.Thread(target=read, args=(process.stderr, stderr, 4096)), threading.Thread(target=write)]
    try:
        for thread in threads: thread.start()
        code = process.wait(timeout=30)
        for thread in threads: thread.join(timeout=2)
        if any(thread.is_alive() for thread in threads) or failure or code != 0 or stderr or not stdout.endswith(b'\n') or stdout.count(b'\n') != 1: _refuse()
        try:
            stdout.decode('utf-8')
        except UnicodeError:
            _refuse()
        _json(bytes(stdout))
        if open_installation(installation.config) != opening: _refuse()
        return bytes(stdout)
    except (OSError, subprocess.SubprocessError):
        _refuse()
    finally:
        if process.poll() is None: process.kill(); process.wait(timeout=5)
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None and not stream.closed:
                try: stream.close()
                except (OSError, ValueError): _refuse()
