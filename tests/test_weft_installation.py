"""Fixture index authority only; no public realization is registered by tests."""
from dataclasses import replace
import hashlib
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from ashlar import weft_installation as w

def fixture_package(root):
    """Synthetic 463-row byte-custody fixture, never compiler qualification."""
    import gzip
    import struct
    raw = {}
    def put(path, value): raw[path] = value if isinstance(value, bytes) else w._encode(value) + b'\n'
    def desc(path): return {'path': path, 'sha256': w._sha(raw[path]), 'bytes': len(raw[path])}
    source_commit = 'a' * 40
    build = {'command': ['cargo', 'build', '-j1', '-p', 'weft-runtime', '--bin', 'weft-runtime', '--release', '--no-default-features', '--features', 'ashlar-databricks-candidate', '--offline', '--locked'],
             'target': 'aarch64-apple-darwin', 'effectiveEnvironment': {k: 'fixture-observation' for k in ('CARGO_HOME', 'RUSTUP_HOME', 'CARGO_TARGET_DIR', 'PATHPrefix')}, 'unknowns': ['fixture, no actual build']}
    put('bin/weft-runtime', struct.pack('<IIIIIIII', 0xfeedfacf, 0x100000c, 0, 2, 0, 0, 0, 0))
    binary = desc('bin/weft-runtime')['sha256']
    source_base = 'docs/helix/04-build/evidence/'
    originals, sources = [], []
    for scope in w.SCOPES:
        path = source_base + 'B-006-' + scope + '/compile-artifacts.jsonl'
        values = [{'id': i, 'request': {'original': scope + ':' + str(i)}, 'response': {'status': 'compiled'}} for i in range(51)]
        put('source-subset/' + path, b''.join(w._encode(row) + b'\n' for row in values))
        sources.append({'path': path, 'sha256': w._sha(raw['source-subset/' + path])})
        originals.extend((scope + ':' + str(row['id']), row) for row in values)
    for scope, stem in [('scalar-native', 'one'), ('scalar-native', 'two'), ('global-native', 'one'), ('cross-module-native', 'compile')]:
        path = source_base + 'B-006-' + scope + '/' + (stem + '-compile.json' if scope != 'cross-module-native' else 'compile.json')
        identity = scope + ':' + (stem + '-compile') if scope != 'cross-module-native' else 'cross-module'
        value = {'request': {'original': identity}, 'response': {'status': 'blocked' if identity == 'cross-module' else 'compiled'}}
        put('source-subset/' + path, value)
        sources.append({'path': path, 'sha256': w._sha(raw['source-subset/' + path])}); originals.append((identity, value))
    rows = []
    for identity, original in originals:
        stdout = w._encode(original['response']).decode() + '\n'
        rows.append({'id': identity, 'requestSha256': w._sha(w._encode(original['request'])), 'exit': 0,
                     'stdoutSha256': w._sha(stdout.encode()), 'stderrSha256': w._sha(b''), 'stdout': stdout, 'stderr': ''})
    cbase = 'evidence/cli-produced-corpus-20261009/'
    rows_raw = b''.join(w._encode(row) + b'\n' for row in rows)
    put(cbase + 'cases.jsonl.gz', gzip.compress(rows_raw, mtime=0))
    transport_ids = ['oversize', 'invalid-utf8', 'split-utf8-at-limit', 'exact-limit', 'exact-limit-multibyte', 'malformed-json', 'directory-input', 'closed-output']
    controls = [{'id': identity, 'scope': 'synthetic byte fixture'} for identity in transport_ids]
    summary = {'sourceFiles': sources, 'controls': controls[:6], 'cases': 463, 'caseIdsUnique': True, 'sourceFilesUnchanged': True, 'fullResponseParity': True, 'deterministic': True,
               'openingBinarySha256': binary, 'closingBinarySha256': binary, 'binarySha256': binary}
    put(cbase + 'summary.json', summary); put(cbase + 'mismatches.json', []); put(cbase + 'io-controls.json', {'controls': controls[6:]})
    put(cbase + 'tool-versions.json', {'scope': 'fixture only'})
    put('producer/scripts/distribution/check-cli.py', b'# fixture no execution\n')
    put('producer/scripts/distribution/check-cli-controls.py', b'# fixture no execution\n')
    put('source-subset/tests/compile/schema-check.ts', b'// fixture no validation\n')
    for name in ('Cargo.lock', 'rust-toolchain.toml'): put('source-subset/' + name, b'# fixture pin\n')
    schemas = []
    for i in range(13):
        path = 'source-subset/docs/helix/02-design/contracts/profile-%02d.schema.json' % i
        put(path, {'$comment': 'synthetic schema fixture'}); schemas.append(desc(path))
    entries = []
    for path in sorted(p for p in raw if p.startswith('source-subset/')):
        data = raw[path]
        entries.append({'path': path[len('source-subset/'):], 'mode': '100644', 'gitBlob': hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest(), 'sha256': w._sha(data), 'bytes': len(data)})
    inventory = w._encode(entries)
    put(cbase + 'source-inventory.json.gz', gzip.compress(inventory, mtime=0))
    corpus_custody = {'sourceCommit': source_commit, 'binarySha256': binary, 'caseReportDecodedSha256': w._sha(rows_raw), 'caseReportDecodedBytes': len(rows_raw),
                     'harness': {'path': 'scripts/distribution/check-cli.py', 'sha256': w._sha(raw['producer/scripts/distribution/check-cli.py']), 'bytes': len(raw['producer/scripts/distribution/check-cli.py'])},
                     'build': build, 'reports': [{**desc(p), 'path': p[len(cbase):]} for p in sorted(raw) if p.startswith(cbase)]}
    put(cbase + 'custody.json', corpus_custody)
    kbase = 'evidence/cli-candidate-controls-20261009/'
    krows = []
    for i, identity in enumerate(w.CONTROL_IDS):
        request = '{"fixture":true}'
        response = {'status': 'blocked', 'diagnostics': [{'code': 'fixture-refusal'}]} if i < 16 else {'status': 'compiled'}
        krows.append({'id': identity, 'requestSha256': w._sha(request.encode()), 'requestJson': request, 'stdout': w._encode(response).decode() + '\n', 'stderr': '', 'exit': 0, 'expectedCode': 'fixture-refusal' if i < 16 else None})
    kraw = b''.join(w._encode(row) + b'\n' for row in krows); put(kbase + 'cases.jsonl.gz', gzip.compress(kraw, mtime=0))
    put(kbase + 'summary.json', {'cases': 19, 'blockedCases': 16, 'compiledCases': 3, 'custodyClosed': True, 'binarySha256': binary, 'sourceSha256': w._sha(raw['source-subset/' + source_base + 'B-006-cross-module-native/compile.json'])})
    put(kbase + 'custody.json', {'compilerSourceCommit': source_commit, 'binarySha256': binary, 'decodedCaseBytes': len(kraw), 'decodedCaseSha256': w._sha(kraw),
                               'harness': {'path': 'scripts/distribution/check-cli-controls.py', 'sha256': w._sha(raw['producer/scripts/distribution/check-cli-controls.py']), 'bytes': len(raw['producer/scripts/distribution/check-cli-controls.py'])},
                               'publicResponseChecker': {'path': 'tests/compile/schema-check.ts', 'sha256': w._sha(raw['source-subset/tests/compile/schema-check.ts'])},
                               'reports': [{**desc(p), 'path': p[len(kbase):]} for p in sorted(raw) if p.startswith(kbase)]})
    put('backend/backend-manifest.json', {'backendId': 'ashlar.databricks', 'backendVersion': '0.1.0-candidate', 'interfaceVersion': 'weft-backend/0.2.0'})
    for name in ('backend-metadata.rs', 'describe-backend.py', 'assemble-distribution.py'): put('producer/scripts/distribution/' + name, b'# fixture no execution\n')
    put('evidence/source-backend-metadata-20261009/custody.json', {'sourceCommit': source_commit, 'metadataSha256': w._sha(raw['backend/backend-manifest.json']), 'metadataBytes': len(raw['backend/backend-manifest.json']), 'sourceInventoryDecodedSha256': w._sha(inventory), 'sourceInventoryDecodedBytes': len(inventory), 'sourceTrackedFiles': len(entries), 'sourceInventoryCompressedSha256': w._sha(raw[cbase + 'source-inventory.json.gz']),
                                                              'exporterSha256': w._sha(raw['producer/scripts/distribution/backend-metadata.rs']), 'harnessSha256': w._sha(raw['producer/scripts/distribution/describe-backend.py'])})
    put('evidence/transport-controls.json', {'binarySha256': binary, 'controls': controls, 'qualification': 'fixture only'})
    manifest = {'format': 'weft-distribution/0.1', 'realizationId': 'fixture-never-public', 'source': {'commit': source_commit, 'inventory': {'artifact': desc(cbase + 'source-inventory.json.gz'), 'decodedSha256': w._sha(inventory), 'decodedBytes': len(inventory), 'trackedFiles': len(entries)}},
                'build': {'release': True, 'target': build['target'], 'features': ['ashlar-databricks-candidate'], 'command': build['command'], 'tools': desc(cbase + 'tool-versions.json'), 'lockfiles': [desc('source-subset/Cargo.lock')], 'toolchain': desc('source-subset/rust-toolchain.toml'), 'effectiveEnvironment': {'observed': build['effectiveEnvironment'], 'unknowns': build['unknowns']}, 'platform': {'binaryFormat': 'mach-o', 'machine': 'arm64', 'minimumOS': '11.0', 'sdk': '27.0', 'observedOS': '27.0.1'}},
                'executable': desc('bin/weft-runtime'), 'backendManifests': [desc('backend/backend-manifest.json')], 'publicSchemas': schemas,
                'conformance': {'corpus': {'cases': 463, 'compiled': 462, 'blocked': 1, **{k: desc(cbase + v) for k, v in [('responses', 'cases.jsonl.gz'), ('summary', 'summary.json'), ('custody', 'custody.json')]}},
                                'controls': {'cases': 19, **{k: desc(kbase + v) for k, v in [('responses', 'cases.jsonl.gz'), ('summary', 'summary.json'), ('custody', 'custody.json')]}}, 'transport': desc('evidence/transport-controls.json')}}
    put('manifest.json', manifest)
    put('assembly-custody.json', {'format': 'weft-distribution-assembly-custody/0.1', 'sourceCommit': source_commit, 'qualification': 'synthetic installer fixture, never runtime/index qualification', 'producerSha256': w._sha(raw['producer/scripts/distribution/assemble-distribution.py']), 'artifacts': [desc(p) for p in sorted(raw)], 'sourceSubset': sorted(p[len('source-subset/'):] for p in raw if p.startswith('source-subset/'))})
    for path, value in raw.items():
        destination = root / path; destination.parent.mkdir(parents=True, exist_ok=True); destination.write_bytes(value)
    return root



class InstallerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.package = self.root / 'package'
        fixture_package(self.package)
        self.manifest_raw = (self.package / 'manifest.json').read_bytes()
        self.manifest = json.loads(self.manifest_raw)
        entry = {'realizationId': self.manifest['realizationId'],
                 'manifest': {'path': 'manifest.json', 'sha256': w._sha(self.manifest_raw), 'bytes': len(self.manifest_raw)},
                 'assemblyCustody': {'path': 'assembly-custody.json', 'sha256': w._sha((self.package / 'assembly-custody.json').read_bytes()), 'bytes': (self.package / 'assembly-custody.json').stat().st_size},
                 'executable': self.manifest['executable'], 'target': 'aarch64-apple-darwin'}
        self.index_raw = w._encode({'format': 'weft-distribution-index/0.1', 'entries': [entry]})
        self.index = self.root / 'fixture-index.json'; self.index.write_bytes(self.index_raw)
        self.config = w.InstallationConfig(self.index, 'f' * 40, w._sha(self.index_raw), self.package,
                                          self.manifest['realizationId'], self.root / 'installed',
                                          'aarch64-apple-darwin', '27.0.1')

    def tearDown(self): self.temporary.cleanup()

    def test_fixture_install_and_restart_exact_binary(self):
        with patch.object(w.subprocess, 'Popen', side_effect=AssertionError('executable invoked during install')):
            installed = w.install(self.config)
            self.assertEqual(w.open_installation(self.config), installed)
        self.assertEqual((self.config.output / 'weft-runtime').read_bytes(), (self.package / 'bin/weft-runtime').read_bytes())
        self.assertFalse(any(self.root.glob('.ashlar-weft-*')))
        with self.assertRaises(w.InstallationError): w.install(self.config)

    def test_wrong_index_refuses_before_caller_package(self):
        wrong = replace(self.config, index_sha256='0' * 64)
        with patch.object(w._Package, 'artifact', side_effect=AssertionError('package consulted before pin')):
            with self.assertRaises(w.InstallationError): w.install(wrong)
        self.assertFalse(self.config.output.exists())

    def test_unknown_identity_and_platform_refuse_without_callback(self):
        for config in (replace(self.config, realization_id='unknown'), replace(self.config, observed_target='x86_64-apple-darwin'), replace(self.config, observed_os='27.0.2')):
            with self.subTest(config=config), patch.object(w.subprocess, 'Popen', side_effect=AssertionError('invoked')):
                with self.assertRaises(w.InstallationError): w.install(config)
        self.assertFalse(self.config.output.exists())

    def test_changed_manifest_binary_schema_and_corpus_refuse(self):
        for name in ('manifest.json', 'bin/weft-runtime', 'source-subset/docs/helix/02-design/contracts/profile-00.schema.json', 'evidence/cli-produced-corpus-20261009/cases.jsonl.gz'):
            path = self.package / name
            raw = path.read_bytes(); path.chmod(0o644); path.write_bytes(raw + b'changed')
            with self.subTest(name=name), self.assertRaises(w.InstallationError): w.install(self.config)
            path.write_bytes(raw)
            self.assertFalse(self.config.output.exists())

    def test_missing_and_extra_artifacts_or_symlink_refuse(self):
        path = self.package / 'backend/backend-manifest.json'
        raw = path.read_bytes(); path.unlink()
        with self.assertRaises(w.InstallationError): w.install(self.config)
        path.write_bytes(raw)
        extra = self.package / 'unindexed'; extra.write_bytes(b'not in custody')
        with self.assertRaises(w.InstallationError): w.install(self.config)
        extra.unlink()
        path.unlink(); path.symlink_to(self.root / 'other')
        with self.assertRaises(w.InstallationError): w.install(self.config)

    def test_fifo_and_extra_directory_refuse(self):
        fifo = self.package / 'unindexed-fifo'
        w.os.mkfifo(fifo)
        with self.assertRaises(w.InstallationError): w.inspect_package(self.config)
        fifo.unlink()
        directory = self.package / 'unindexed-directory'; directory.mkdir()
        with self.assertRaises(w.InstallationError): w.inspect_package(self.config)

    def test_extra_directory_refuses_before_descent_or_unbounded_enumeration(self):
        unindexed = self.package / 'unindexed-directory'; unindexed.mkdir()
        (unindexed / 'never-visit').mkdir()
        original = w.os.scandir
        visited = []
        def enumerate_directory(path):
            visited.append(Path(path))
            if Path(path) == unindexed: raise AssertionError('descended into unindexed tree')
            return original(path)
        with patch.object(w.os, 'scandir', side_effect=enumerate_directory):
            with self.assertRaises(w.InstallationError): w.inspect_package(self.config)
        self.assertNotIn(unindexed, visited)
        # A stream of arbitrary extra names is stopped at the first item.
        class Entry:
            name = 'unknown'
        class Infinite:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def __iter__(self): return self
            def __next__(self):
                visited.append('yield')
                if visited.count('yield') > 1: raise AssertionError('unbounded enumeration')
                return Entry()
        with patch.object(w.os, 'scandir', return_value=Infinite()):
            with self.assertRaises(w.InstallationError): w._exact_tree(self.package, ['manifest.json'])
        self.assertEqual(visited.count('yield'), 1)

    def test_duplicate_index_and_path_escapes_refuse(self):
        raw = b'{"format":"weft-distribution-index/0.1","format":"weft-distribution-index/0.1","entries":[]}'
        self.index.write_bytes(raw)
        with self.assertRaises(w.InstallationError): w.install(replace(self.config, index_sha256=w._sha(raw)))
        for path in ('../outside', '/outside', 'a//b', 'a/./b', 'a\\b', 'C:outside'):
            with self.assertRaises(w.InstallationError): w._descriptor({'path': path, 'sha256': 'a' * 64, 'bytes': 0})

    def test_restart_without_package_and_install_missing_package(self):
        installed = w.install(self.config)
        retained = replace(self.config, package=None)
        self.assertEqual(w.open_installation(retained).ready_bytes, installed.ready_bytes)
        fresh = replace(retained, output=self.config.output.parent / 'missing-package-output')
        with patch.object(w, '_read', side_effect=AssertionError('read before missing package refusal')):
            with self.assertRaises(w.InstallationError): w.install(fresh)
        self.assertFalse(fresh.output.exists())

    def test_restart_wrong_index_refuses_before_ready_read(self):
        w.install(self.config)
        original = w._read
        def guarded(path, limit):
            if Path(path).name == 'ready.json': raise AssertionError('read ready before trust gate')
            return original(path, limit)
        with patch.object(w, '_read', side_effect=guarded):
            with self.assertRaises(w.InstallationError):
                w.open_installation(replace(self.config, index_sha256='0' * 64))

    def test_aggregate_budget_refuses_before_next_read(self):
        package = w._Package(self.package)
        package.total = w.TOTAL_LIMIT
        descriptor = {'path': 'manifest.json', 'sha256': 'a' * 64, 'bytes': 1}
        with patch.object(w, '_read', side_effect=AssertionError('read beyond aggregate budget')):
            with self.assertRaises(w.InstallationError): package.artifact(descriptor)

    def test_metadata_descriptor_limit_refuses_before_read(self):
        package = w._Package(self.package)
        descriptor = {'path': 'manifest.json', 'sha256': 'a' * 64, 'bytes': w.JSON_LIMIT + 1}
        with patch.object(w, '_read', side_effect=AssertionError('oversized metadata read')):
            with self.assertRaises(w.InstallationError): package.artifact(descriptor, w.JSON_LIMIT)

    def test_bounded_json_gzip_and_bool_counts_refuse(self):
        import gzip
        with self.assertRaises(w.InstallationError): w._inflate(gzip.compress(b'1234'), 3)
        with self.assertRaises(w.InstallationError): w._inflate(gzip.compress(b'1') + gzip.compress(b'2'), 3)
        with self.assertRaises(w.InstallationError): w._json(b'{"a":1,"a":2}')
        with self.assertRaises(w.InstallationError): w._number(True)
        huge = self.root / 'large'; huge.write_bytes(b'a' * 12)
        with self.assertRaises(w.InstallationError): w._read(huge, 10)

    def test_copy_and_final_marker_failures_leave_unavailable(self):
        for target in ('copyfile', 'link'):
            owner = w.shutil if target == 'copyfile' else w.os
            with self.subTest(target=target), patch.object(owner, target, side_effect=OSError('injected')):
                with self.assertRaises(w.InstallationError): w.install(self.config)
            self.assertFalse(self.config.output.exists())
            self.assertFalse(any(self.root.glob('.ashlar-weft-*')))

    def test_postcommit_cleanup_is_pending_success_without_rollback(self):
        original = w.shutil.rmtree
        def failure(path, *args, **kwargs):
            if Path(path).name.startswith('.ashlar-weft-'):
                raise OSError('injected staging cleanup failure')
            return original(path, *args, **kwargs)
        original_unlink = Path.unlink
        def unlink(path, *args, **kwargs):
            if path.name == 'ready.json': raise OSError('injected rollback denial')
            return original_unlink(path, *args, **kwargs)
        with patch.object(w.shutil, 'rmtree', side_effect=failure), patch.object(Path, 'unlink', unlink):
            installed = w.install(self.config)
        self.assertTrue(installed.cleanup_pending)
        self.assertEqual(w.open_installation(self.config), installed)
        self.assertTrue((self.config.output / 'ready.json').exists())

    def test_cleanup_never_removes_replaced_foreign_output(self):
        def failure(*args, **kwargs):
            self.config.output.rename(self.root / 'original-owned-incomplete')
            self.config.output.mkdir()
            (self.config.output / 'foreign').write_bytes(b'preserve')
            raise OSError('output replaced by noncooperating test writer')
        with patch.object(w.shutil, 'copyfile', side_effect=failure):
            with self.assertRaises(w.InstallationError): w.install(self.config)
        self.assertEqual((self.config.output / 'foreign').read_bytes(), b'preserve')

    def test_untrusted_assembly_closure_tampering_refuses(self):
        path = self.package / 'assembly-custody.json'
        value = json.loads(path.read_bytes())
        value['artifacts'] = value['artifacts'][:-1]
        path.write_bytes(w._encode(value) + b'\n')
        with self.assertRaises(w.InstallationError): w.install(self.config)
        self.assertFalse(self.config.output.exists())

    def test_closed_record_counts_features_and_schema_inventory_refuse(self):
        import copy
        for mutation in ('count', 'feature', 'unknown', 'schema-order'):
            value = copy.deepcopy(self.manifest)
            if mutation == 'count': value['conformance']['corpus']['compiled'] = 461
            if mutation == 'feature': value['build']['features'] = ['other']
            if mutation == 'unknown': value['claimsRegistration'] = True
            if mutation == 'schema-order': value['publicSchemas'].reverse()
            with self.subTest(mutation=mutation), self.assertRaises(w.InstallationError): w._manifest(value)

    def test_incomplete_directory_never_available(self):
        self.config.output.mkdir()
        (self.config.output / 'weft-runtime').write_bytes(b'incomplete')
        with self.assertRaises(w.InstallationError): w.open_installation(self.config)
        with self.assertRaises(w.InstallationError): w.install(self.config)
        self.assertEqual((self.config.output / 'weft-runtime').read_bytes(), b'incomplete')

    def test_protocol_request_and_stdout_are_unchanged_and_closing_checked(self):
        installed = w.install(self.config)
        request = b'{"interfaceVersion":"weft-compile/0.1.0","original":"\xc3\xa9"}'
        stdout = b'{"status":"blocked","interfaceVersion":"weft-compile/0.1.0"}\n'
        class Writable(io.BytesIO):
            def close(self):
                if not self.closed: self.retained = self.getvalue()
                super().close()
        class Process:
            def __init__(self):
                self.stdin = Writable(); self.stdout = io.BytesIO(stdout); self.stderr = io.BytesIO(b'')
            def wait(self, timeout=None): return 0
            def poll(self): return 0
            def kill(self): pass
        process = Process()
        with patch.object(w.subprocess, 'Popen', return_value=process) as callback:
            self.assertEqual(w.compile_request(installed, request), stdout)
        self.assertEqual(process.stdin.retained, request)
        self.assertEqual(callback.call_args[0][0], [str(self.config.output / 'weft-runtime')])
        process = Process()
        def completion(timeout=None):
            binary = self.config.output / 'weft-runtime'
            binary.chmod(0o755); binary.write_bytes(b'during-execution-drift')
            return 0
        process.wait = completion
        with patch.object(w.subprocess, 'Popen', return_value=process):
            with self.assertRaises(w.InstallationError): w.compile_request(installed, request)
        binary = self.config.output / 'weft-runtime'; binary.chmod(0o755); binary.write_bytes(b'changed')
        with patch.object(w.subprocess, 'Popen', side_effect=AssertionError('tampered binary executed')):
            with self.assertRaises(w.InstallationError): w.compile_request(installed, request)

    def test_oversized_request_refuses_before_execution(self):
        installed = w.install(self.config)
        with patch.object(w.subprocess, 'Popen', side_effect=AssertionError('invoked')):
            with self.assertRaises(w.InstallationError): w.compile_request(installed, b'x' * (w.PROTOCOL_LIMIT + 1))


if __name__ == '__main__': unittest.main()
