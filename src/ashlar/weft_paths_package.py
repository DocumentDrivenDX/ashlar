"""Separate Paths530 package verification against an injected trusted index.

Index trust is injected by application composition. Inert manifests, passing
checks and executable bytes cannot register themselves. This module never opens
native engines, downloads artifacts or discharges compiler host obligations.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import zlib
from typing import Optional

JSON_LIMIT = 4 * 1024 * 1024
FILE_LIMIT = 32 * 1024 * 1024
TOTAL_LIMIT = 96 * 1024 * 1024
DECODED_LIMIT = 64 * 1024 * 1024
PROTOCOL_LIMIT = 16 * 1024 * 1024
SAFE_INTEGER = 9007199254740991
CONTROL_IDS = ('candidate-opt-out', 'unknown-backend', 'wrong-backend-version', 'wrong-profile',
               'unknown-envelope-member', 'mismatched-interface-dialect', 'binding-digest-mismatch',
               'model-digest-mismatch', 'sql-byte-limit', 'binding-byte-limit', 'duplicate-envelope-member',
               'missing-publication', 'negative-table-version', 'duplicate-table-uuid', 'missing-table-mapping',
               'inconsistent-model-pin', 'fresh-binding-0', 'fresh-binding-1', 'fresh-binding-2')
SCOPES = ('columns-native', 'application-native', 'key-refusal', 'unsigned-columns', 'optional-native',
          'relationship-native', 'compound-native', 'compound-boundaries-native', 'compound-application-native')


class PathsInstallationError(ValueError):
    """Bounded installation refusal; callers must not select a fallback."""


def _refuse():
    raise PathsInstallationError('ASHLAR-WEFT-PATHS-REFUSED')


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encode_document(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            _refuse()
        result[key] = value
    return result


def _integer(token):
    if len(token.lstrip('-')) > 20: _refuse()
    return int(token)


def decode_document(raw):
    try:
        return json.loads(raw.decode('utf-8') if type(raw) is bytes else raw, object_pairs_hook=_pairs, parse_int=_integer, parse_float=lambda _: _refuse(), parse_constant=lambda _: _refuse())
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


def read_snapshot(path: Path, limit: int = FILE_LIMIT) -> bytes:
    """Bounded regular-file descriptor snapshot; cooperating directory writers."""
    _no_symlink(path)
    fd = None; primary = None; raw = None
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        if not stat.S_ISREG(os.fstat(fd).st_mode): _refuse()
        chunks = []; remaining = limit + 1
        while remaining:
            chunk = os.read(fd, min(65536, remaining))
            if not chunk: break
            chunks.append(chunk); remaining -= len(chunk)
        raw = b''.join(chunks)
        if len(raw) > limit: _refuse()
    except BaseException as exc: primary = exc
    finally:
        if fd is not None:
            try: os.close(fd)
            except BaseException as exc:
                if primary is None: primary = exc
                else:
                    try: setattr(primary, 'cleanup_failed', True)
                    except BaseException: pass
    if primary is not None: raise primary
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
    return [decode_document(line) for line in raw.splitlines() if line]


@dataclass(frozen=True)
class PathsInstallationConfig:
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
            if not isinstance(value, Path) or not value.is_absolute():
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
        raw = read_snapshot(path, limit)
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
            if read_snapshot(self.root / path) != raw:
                _refuse()
        verify_exact_tree(self.root, self.raw)


def verify_exact_tree(root, paths):
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


def verify_trusted_index(config):
    # No caller manifest/package is inspected until the trusted pin matches.
    raw = read_snapshot(config.index_path, JSON_LIMIT)
    if _sha(raw) != config.index_sha256:
        _refuse()
    value = decode_document(raw)
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



SOURCE = '530ae3511a4a50364d3d7e26195d3883952601df'
BINARY = '71ba88a07c27661413830d5a42e2ea5c4a8b071df965bb98dbbe8f3615a6c911'
BACKEND = 'fc36ddbb6ca41309d25dadf1969bb27efb72debe0c6669b76a8a6a425ca35ec5'
RECEIPT = '7f80a87ac41d868a32d220fd5a5ec7f8998c264ec0eecc4455029ce3ba7caf68'
CASES = '1cf69c0dfbde7cb8f82e6653c67637157342374bd1ab4114050d93246f21000d'
INVENTORY = '2a1eed46946a4d623f2f8abc7e7592e6d74e3964585ce1c500600a7ea62d6896'
SCHEMA_BASE = 'source-subset/docs/helix/02-design/contracts/'
INSTALLED_SCHEMAS = ('compile-request-v0.4.schema.json', 'compile-response-v0.4.schema.json',
                     'logical-plan-v0.4.schema.json', 'application-result-v0.4.schema.json',
                     'backend-manifest-v0.3.schema.json')


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()


def validate_manifest(manifest):
    _closed(manifest, ('format', 'realizationId', 'source', 'build', 'executable', 'backendManifests', 'publicSchemas', 'conformance'))
    if manifest['format'] != 'weft-distribution/0.1' or manifest['source']['commit'] != SOURCE: _refuse()
    _closed(manifest['source'], ('commit', 'inventory'))
    inventory = manifest['source']['inventory']
    _closed(inventory, ('artifact', 'decodedSha256', 'decodedBytes', 'trackedFiles'))
    _descriptor(inventory['artifact']); _number(inventory['decodedBytes'], 1, JSON_LIMIT)
    if inventory['decodedSha256'] != INVENTORY or inventory['trackedFiles'] != 1842: _refuse()
    build = manifest['build']
    _closed(build, ('release', 'target', 'features', 'command', 'tools', 'lockfiles', 'toolchain', 'effectiveEnvironment', 'platform'))
    if build['release'] is not True or build['target'] != 'aarch64-apple-darwin' or build['features'] != ['ashlar-databricks-paths']: _refuse()
    if type(build['command']) is not list or len(build['command']) != 15 or build['command'][1:] != ['build', '--offline', '--locked', '--release', '-j1', '--target', 'aarch64-apple-darwin', '-p', 'weft-runtime', '--no-default-features', '--features', 'ashlar-databricks-paths', '--bin', 'weft-paths']: _refuse()
    _text(build['command'][0])
    if build['platform'] != {'binaryFormat': 'mach-o', 'machine': 'arm64', 'minimumOS': '11.0', 'sdk': '27.0', 'observedOS': '27.0.1'}: _refuse()
    _closed(build['effectiveEnvironment'], ('observed', 'unknowns'))
    _closed(build['effectiveEnvironment']['observed'], ('CARGO_HOME', 'RUSTUP_HOME', 'CARGO_TARGET_DIR', 'PATHPrefix'))
    for item in build['effectiveEnvironment']['observed'].values(): _text(item)
    unknowns = build['effectiveEnvironment']['unknowns']
    if type(unknowns) is not list or not 1 <= len(unknowns) <= 32 or len(set(unknowns)) != len(unknowns): _refuse()
    for item in unknowns: _text(item)
    for field in ('tools', 'toolchain'): _descriptor(build[field])
    if type(build['lockfiles']) is not list or len(build['lockfiles']) != 1 or build['lockfiles'][0]['path'] != 'source-subset/Cargo.lock' or build['toolchain']['path'] != 'source-subset/rust-toolchain.toml': _refuse()
    _descriptor(build['lockfiles'][0]); _descriptor(manifest['executable'])
    if manifest['executable']['path'] != 'bin/weft-paths' or manifest['executable']['sha256'] != BINARY or manifest['executable']['bytes'] != 8273840: _refuse()
    for field, count in (('backendManifests', 1), ('publicSchemas', 20)):
        if type(manifest[field]) is not list or len(manifest[field]) != count: _refuse()
        for item in manifest[field]: _descriptor(item)
        if [d['path'] for d in manifest[field]] != sorted(set(d['path'] for d in manifest[field])): _refuse()
    if manifest['backendManifests'][0]['sha256'] != BACKEND: _refuse()
    conformance = manifest['conformance']; _closed(conformance, ('corpus', 'controls', 'transport'))
    for kind in ('corpus', 'controls'):
        item = conformance[kind]
        _closed(item, ('cases', 'responses', 'summary', 'custody') + (('compiled', 'blocked') if kind == 'corpus' else ()))
        for field in ('responses', 'summary', 'custody'): _descriptor(item[field])
    if (conformance['corpus']['cases'], conformance['corpus']['compiled'], conformance['corpus']['blocked'], conformance['controls']['cases']) != (493, 26, 467, 19): _refuse()
    _descriptor(conformance['transport'])


def _proof(package, manifest, entry):
    proof_raw = package.artifact(entry['assemblyCustody'], JSON_LIMIT); proof = decode_document(proof_raw)
    _closed(proof, ('format', 'sourceCommit', 'qualification', 'producerSha256', 'artifacts', 'sourceSubset'))
    if proof['format'] != 'weft-distribution-assembly-custody/0.1' or proof['sourceCommit'] != SOURCE: _refuse()
    _text(proof['qualification']); _digest(proof['producerSha256'])
    if type(proof['artifacts']) is not list or len(proof['artifacts']) != 69: _refuse()
    descriptors = {}
    for item in proof['artifacts']:
        _descriptor(item)
        if item['path'] in descriptors or item['path'] == 'assembly-custody.json': _refuse()
        descriptors[item['path']] = item; package.artifact(item)
    if list(descriptors) != sorted(descriptors): _refuse()
    if descriptors.get('manifest.json') != entry['manifest'] or descriptors.get('bin/weft-paths') != entry['executable']: _refuse()
    inventory = manifest['source']['inventory']; raw = _inflate(package.artifact(inventory['artifact']), JSON_LIMIT)
    if _sha(raw) != INVENTORY or len(raw) != inventory['decodedBytes']: _refuse()
    value = decode_document(raw); _closed(value, ('sourceCommit', 'files'))
    if value['sourceCommit'] != SOURCE or type(value['files']) is not list or len(value['files']) != 1842: _refuse()
    sources = {}
    for item in value['files']:
        _closed(item, ('path', 'mode', 'gitBlob', 'sha256', 'bytes')); _relative(item['path']); _digest(item['sha256']); _digest(item['gitBlob'], 40); _number(item['bytes'], maximum=FILE_LIMIT)
        if item['mode'] not in ('100644', '100755') or item['path'] in sources: _refuse()
        sources[item['path']] = item
    if list(sources) != sorted(sources): _refuse()
    subset = sorted(p[len('source-subset/'):] for p in package.raw if p.startswith('source-subset/'))
    if proof['sourceSubset'] != subset: _refuse()
    for name in subset:
        raw = package.raw['source-subset/' + name]; item = sources[name]
        if len(raw) != item['bytes'] or _sha(raw) != item['sha256'] or hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest() != item['gitBlob']: _refuse()
    schemas = ['source-subset/' + name for name in sources if name.startswith('docs/helix/02-design/contracts/') and name.endswith('.schema.json')]
    if schemas != [d['path'] for d in manifest['publicSchemas']]: _refuse()
    for desc in manifest['publicSchemas'] + manifest['backendManifests'] + manifest['build']['lockfiles'] + [manifest['build']['tools'], manifest['build']['toolchain'], inventory['artifact']]:
        if descriptors.get(desc['path']) != desc: _refuse()
    if proof['producerSha256'] != _sha(package.raw['producer/assemble-paths-distribution.py']): _refuse()
    backend = decode_document(package.artifact(manifest['backendManifests'][0]))
    if backend['backendId'] != 'ashlar.databricks.paths' or backend['backendVersion'] != '0.4.0-paths-candidate' or backend['interfaceVersion'] != 'weft-backend/0.3.0' or backend['languageProfiles'] != [{'dialectProfile': 'weft-sql/0.4.0', 'irVersion': 'weft-ir/0.4.0'}] or len(backend['capabilities']) != 47: _refuse()
    command_raw = package.raw['evidence/build-command.json']
    if _sha(command_raw) != 'd7bec02e48c77842db160d3c34081a432e9a00b867d8547df11dd0de0dc45fa0': _refuse()
    for phase, name in (('cli-build','cli-build-outcome.json'), ('source-metadata','metadata-build-outcome.json')):
        outcome = decode_document(package.raw['evidence/' + name])
        if outcome['commandSha256'] != _sha(command_raw) or outcome['phase'] != phase or outcome['exitCode'] != 0 or outcome['openingClosingCustody'] is not True or outcome['environmentInherited'] is not False: _refuse()
    return proof_raw, descriptors, backend


def _conformance(package, manifest, descriptors, backend):
    receipt_raw = _inflate(package.raw['evidence/full-receipt.json.gz'])
    bundle_raw = package.raw['evidence/expected-cases.json']
    if _sha(receipt_raw) != RECEIPT or _sha(bundle_raw) != CASES: _refuse()
    receipt = decode_document(receipt_raw); bundle = decode_document(bundle_raw)
    if receipt['sourceCommit'] != SOURCE or receipt['binarySha256'] != BINARY or receipt['sourceInventorySha256'] != INVENTORY or receipt['declaredCapabilityCount'] != 47: _refuse()
    if receipt['casesInput'] != {'sha256': CASES, 'bytes': len(bundle_raw)} or receipt['backendInput'] != {'sha256': BACKEND, 'bytes': len(package.raw[manifest['backendManifests'][0]['path']])}: _refuse()
    _closed(bundle, ('sourceCommit', 'paths', 'controls', 'coverage', 'sources', 'legacyNamespaceRefusal'))
    cases = receipt['cases']
    if len(cases) != 512 or len({r['id'] for r in cases}) != 512 or len(bundle['paths']) != 30 or [c['id'] for c in bundle['controls']] != list(CONTROL_IDS): _refuse()
    fence = bytes.fromhex(bundle['legacyNamespaceRefusal'])
    if decode_document(fence) != {'interfaceVersion':'weft-compile/0.4.0','status':'blocked','diagnostics':[{'code':'WFT-VERSION','severity':'error','message':'This entrypoint requires the exact 0.4 compile and dialect pair','phase':'input','recoverability':'correct-input'}]}: _refuse()
    originals = []; base = 'source-subset/docs/helix/04-build/evidence/'
    for scope in SCOPES:
        originals.extend((scope + ':' + str(item['id']), item) for item in _rows(package.raw[base + 'B-006-' + scope + '/compile-artifacts.jsonl']))
    for scope in ('scalar-native', 'global-native'):
        for path in sorted(p for p in package.raw if p.startswith(base + 'B-006-' + scope + '/') and p.endswith('-compile.json')):
            originals.append((scope + ':' + Path(path).stem, decode_document(package.raw[path])))
    originals.append(('cross-module', decode_document(package.raw[base + 'B-006-cross-module-native/compile.json'])))
    if len(originals) != 463: _refuse()
    for row, (identity, original) in zip(cases[:463], originals):
        if row['id'] != 'legacy:' + identity or row['scope'] != 'legacy' or row['role'] != 'namespace' or bytes.fromhex(row['requestHex']) != _canonical(original['request']) or bytes.fromhex(row['originalExpectedHex']) != _canonical(original['response']) + b'\n' or bytes.fromhex(row['responseHex']) != fence: _refuse()
    expected = [('paths:' + c['id'], 'paths', c) for c in bundle['paths']] + [('controls:' + c['id'], 'controls', c) for c in bundle['controls']]
    for row, (identity, scope, case) in zip(cases[463:], expected):
        if (row['id'], row['scope'], row['role'], row['requestHex'], row['responseHex']) != (identity, scope, case['role'], case['requestHex'], case['responseHex']): _refuse()
    for row in cases:
        if type(row['exit']) is not int or row['exit'] != 0 or row['stderrHex'] or row['migrations']: _refuse()
        response = decode_document(bytes.fromhex(row['responseHex']))
        if response['interfaceVersion'] != 'weft-compile/0.4.0' or response['status'] not in ('compiled', 'blocked'): _refuse()
    for kind, rows, compiled in (('corpus', cases[:493], 26), ('controls', cases[493:], 3)):
        item = manifest['conformance'][kind]
        for field in ('responses', 'summary', 'custody'):
            if descriptors.get(item[field]['path']) != item[field]: _refuse()
        raw = _inflate(package.artifact(item['responses'])); summary = decode_document(package.artifact(item['summary'])); custody = decode_document(package.artifact(item['custody']))
        if raw != b''.join(_canonical(row) + b'\n' for row in rows) or len(raw) != summary['decodedBytes'] or _sha(raw) != summary['decodedSha256'] or summary['cases'] != len(rows) or summary['compiled'] != compiled or summary['blocked'] != len(rows) - compiled: _refuse()
        if custody != {'sourceCommit':SOURCE,'binarySha256':BINARY,'fullReceiptSha256':RECEIPT,'expectedCasesSha256':CASES,'decodedSha256':_sha(raw),'decodedBytes':len(raw)}: _refuse()
        if sum(decode_document(bytes.fromhex(row['responseHex']))['status'] == 'compiled' for row in rows) != compiled: _refuse()
    if receipt['coverage'] != bundle['coverage'] or set(bundle['coverage']) != {c['id'] for c in backend['capabilities']}: _refuse()
    by_id = {r['id']: decode_document(bytes.fromhex(r['responseHex'])) for r in cases}
    for capability, item in bundle['coverage'].items():
        _closed(item, ('accepted', 'refused', 'scope'))
        if not item['scope'] or not (item['accepted'] or item['refused']): _refuse()
        for identity in item['accepted']:
            if by_id[identity]['status'] != 'compiled' or capability not in by_id[identity]['logicalPlan']['requiredCapabilities']: _refuse()
        for identity in item['refused']:
            if by_id[identity]['status'] != 'blocked': _refuse()
    transport = decode_document(package.artifact(manifest['conformance']['transport']))
    if transport != {'binarySha256':BINARY,'controls':receipt['transport']} or [r['id'] for r in receipt['transport']] != ['oversize','invalid-utf8','split-utf8-at-limit','exact-limit','exact-limit-multibyte','malformed-json','directory-input','closed-output']: _refuse()
    if receipt['harnessSha256'] != _sha(package.raw['source-subset/scripts/distribution/check-paths-cli.py']): _refuse()
    for desc in receipt['sources']:
        raw = package.raw['source-subset/' + desc['path']]
        if _sha(raw) != desc['sha256'] or len(raw) != desc['bytes']: _refuse()


@dataclass(frozen=True)
class VerifiedPathsPackage:
    """Immutable bytes, not caller-authorized claims; install always re-verifies."""
    manifest_bytes: bytes
    custody_bytes: bytes
    executable_bytes: bytes
    provenance_bytes: bytes
    resources: tuple[tuple[str, bytes], ...]


def inspect_package(config: PathsInstallationConfig) -> VerifiedPathsPackage:
    """Verify trusted selection and all package bytes without executing code."""
    try:
        index_raw, entry = verify_trusted_index(config)
        if not isinstance(config.package, Path): _refuse()
        package = _Package(config.package)
        raw = package.artifact(entry['manifest'], JSON_LIMIT); manifest = decode_document(raw); validate_manifest(manifest)
        if manifest['realizationId'] != config.realization_id or manifest['executable'] != entry['executable'] or manifest['build']['platform']['observedOS'] != config.observed_os: _refuse()
        proof_raw, descriptors, backend = _proof(package, manifest, entry)
        _conformance(package, manifest, descriptors, backend)
        resources = [('backend-manifest.json', package.artifact(manifest['backendManifests'][0]))]
        for name in INSTALLED_SCHEMAS:
            resources.append(('schemas/' + name, package.raw[SCHEMA_BASE + name]))
        provenance = {'format':'ashlar-weft-paths-provenance/0.1','indexRevision':config.index_revision,'indexSha256':config.index_sha256,'realizationId':config.realization_id,'manifestHex':raw.hex(),'custodyHex':proof_raw.hex(),'resources':[{'path':name,'sha256':_sha(data),'bytes':len(data)} for name,data in resources],'qualification':'Indexed compiler bytes only; no native/source/publication/ACK obligation discharged.'}
        provenance_raw = encode_document(provenance) + b'\n'
        if len(provenance_raw) > JSON_LIMIT: _refuse()
        executable = package.artifact(manifest['executable'])
        package.close()
        if read_snapshot(config.index_path, JSON_LIMIT) != index_raw: _refuse()
        return VerifiedPathsPackage(raw, proof_raw, executable, provenance_raw, tuple(resources))
    except (KeyError, TypeError, OSError, ValueError, RecursionError): _refuse()
