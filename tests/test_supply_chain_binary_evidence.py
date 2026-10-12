"""Real file ownership and inert original query orchestration; no SDK execution."""
import contextlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ashlar_host import supply_chain_binary_evidence as owner
from ashlar_host import supply_chain_native as native, supply_chain_native_cli as cli
from ashlar_host.lifecycle import finish
from test_supply_chain_native_integration import configured


class BinaryEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.sink = owner.SupplyChainBinaryEvidence(self.root)
        self.addCleanup(self.dispose)

    def dispose(self):
        if not self.sink.closed:
            try:
                self.sink.close()
            except ValueError:
                pass  # An incomplete/error fixture is deliberately unqualified.

    def complete(self):
        for name, iteration in owner.INVENTORY:
            self.sink.retain(name, iteration, b'\x00\xff original request ' + name.encode(),
                             b'\x00 original response ' + name.encode())

    def test_complete_original_bytes_index_identity_and_aggregate(self):
        self.complete()
        ref = self.sink.reference()
        rows = [json.loads(raw) for raw in (self.sink.root / owner.INDEX_NAME).read_bytes().splitlines()]
        self.assertEqual([(r['case'], r['iteration']) for r in rows], list(owner.INVENTORY))
        self.assertEqual(len(list(self.sink.root.iterdir())), 69)
        self.assertEqual(ref['records'], 34)
        self.assertEqual(ref['files'], 68)
        self.assertEqual([r['ordinal'] for r in rows[:10]], [i for i in range(5) for _ in (0, 1)])
        self.assertEqual([r['ordinal'] for r in rows[10:]], [i for i in range(12) for _ in (0, 1)])
        self.assertEqual({r['kind'] for r in rows}, {'authored', 'two-hop'})
        total = 0
        for row in rows:
            for side in ('request', 'response'):
                item = row[side]
                raw = (self.sink.root / item['name']).read_bytes()
                self.assertIn(b'\x00', raw)
                if side == 'request':
                    self.assertIn(b'\xff', raw)
                self.assertEqual(len(raw), item['bytes'])
                self.assertEqual(list(owner._identity((self.sink.root / item['name']).stat())), item['identity'])
                total += len(raw)
        self.assertEqual(ref['aggregate_bytes'], total + (self.sink.root / owner.INDEX_NAME).stat().st_size)
        self.assertLessEqual(ref['aggregate_bytes'], 8388608)
        fds = [e['fd'] for e in self.sink.files] + [self.sink.index['fd'], self.sink.directory_fd]
        self.sink.close()
        for fd in fds:
            with self.assertRaises(OSError):
                os.fstat(fd)

    def test_preflight_pair_and_complete_identity_record_before_any_file(self):
        with patch.object(owner, 'AGGREGATE_BYTES', 20), patch.object(owner.os, 'open') as opened:
            with self.assertRaises(ValueError):
                self.sink.retain(owner.CASE_IDS[0], 0, b'original', b'original')
            opened.assert_not_called()
        self.assertEqual(list(self.sink.root.iterdir()), [])
        with patch.object(owner.os, 'open') as opened:
            with self.assertRaises(ValueError):
                self.sink.retain(owner.CASE_IDS[0], 0, b'x' * (1048576 + 1), b'r')
            opened.assert_not_called()

    def test_known_positions_only_no_duplicate_rotation_or_extra_files(self):
        for name, iteration in (('../escape', 0), ('two-hop-12', 0), (owner.CASE_IDS[0], True), (owner.CASE_IDS[0], 1)):
            with self.assertRaises(ValueError):
                self.sink.retain(name, iteration, b'a', b'b')
        self.sink.retain(owner.CASE_IDS[0], 0, b'a', b'b')
        with self.assertRaises(ValueError):
            self.sink.retain(owner.CASE_IDS[0], 0, b'a', b'b')
        (self.sink.root / 'extra').write_bytes(b'foreign')
        with self.assertRaises(ValueError):
            self.sink.retain(owner.CASE_IDS[0], 1, b'a', b'b')
        (self.sink.root / 'extra').unlink()

    def test_short_then_zero_write_withholds_index_and_closes_all_acquired(self):
        actual = os.write
        calls = []
        def partial(fd, raw):
            calls.append(fd)
            return actual(fd, raw[:1]) if len(calls) == 1 else 0
        with patch.object(owner.os, 'write', side_effect=partial), self.assertRaises(ValueError):
            self.sink.retain(owner.CASE_IDS[0], 0, b'original', b'response')
        self.assertEqual(self.sink.records, [])
        self.assertFalse((self.sink.root / owner.INDEX_NAME).exists())
        fd = self.sink.files[0]['fd']
        primary = KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt) as caught:
            finish(primary, [self.sink.close])
        self.assertIs(caught.exception, primary)
        with self.assertRaises(OSError):
            os.fstat(fd)

    def test_index_failure_preserves_exact_cancel_and_independent_cleanup(self):
        primary = GeneratorExit()
        actual = self.sink._write
        def fail_index(entry, raw):
            if entry['name'] == owner.INDEX_NAME:
                raise primary
            actual(entry, raw)
        with patch.object(self.sink, '_write', side_effect=fail_index), self.assertRaises(GeneratorExit) as caught:
            self.sink.retain(owner.CASE_IDS[0], 0, b'original', b'response')
        self.assertIs(caught.exception, primary)
        fds = [e['fd'] for e in self.sink.files] + [self.sink.index['fd'], self.sink.directory_fd]
        with self.assertRaises(GeneratorExit) as caught:
            finish(primary, [self.sink.close])
        self.assertIs(caught.exception, primary)
        for fd in fds:
            with self.assertRaises(OSError):
                os.fstat(fd)

    def test_byte_correspondence_not_metadata_only_and_all_closing_attempts(self):
        self.complete()
        original = os.pread
        first = self.sink.files[0]['fd']
        def changed(fd, count, offset):
            raw = original(fd, count, offset)
            return b'X' + raw[1:] if fd == first and offset == 0 and raw else raw
        fds = [e['fd'] for e in self.sink.files] + [self.sink.index['fd'], self.sink.directory_fd]
        with patch.object(owner.os, 'pread', side_effect=changed), self.assertRaises(ValueError):
            self.sink.close()
        for fd in fds:
            with self.assertRaises(OSError):
                os.fstat(fd)

    def test_independent_checks_and_first_closing_cancellation(self):
        self.complete()
        actual = self.sink._verify
        attempts = []
        cancel = KeyboardInterrupt()
        def verify(entry):
            attempts.append(entry['fd'])
            if len(attempts) == 1: raise ValueError('ordinary closing')
            if len(attempts) == 2: raise cancel
            actual(entry)
        with patch.object(self.sink, '_verify', side_effect=verify), self.assertRaises(KeyboardInterrupt) as caught:
            self.sink.close()
        self.assertIs(caught.exception, cancel)
        self.assertEqual(len(attempts), 69)
        for fd in attempts:
            with self.assertRaises(OSError): os.fstat(fd)

    def test_named_replacement_and_symlink_refuse_even_same_bytes(self):
        self.complete()
        entry = self.sink.files[0]
        p = self.sink.root / entry['name']
        raw = p.read_bytes()
        p.unlink()
        p.write_bytes(raw)
        with self.assertRaises(ValueError):
            self.sink.reference()
        p.unlink()
        p.symlink_to(self.sink.root / self.sink.files[1]['name'])
        with self.assertRaises(ValueError):
            self.sink.reference()

    def test_bounded_inode_refusal_precedes_actual_record_encoding_and_releases_fd(self):
        actual = os.fstat
        def oversized(fd):
            info = actual(fd)
            if fd == self.sink.directory_fd:
                return info
            fields = {key: getattr(info, key) for key in ('st_dev', 'st_uid', 'st_mode',
                'st_nlink', 'st_size', 'st_mtime_ns', 'st_ctime_ns')}
            return SimpleNamespace(st_ino=2**10000, **fields)
        with patch.object(owner.os, 'fstat', side_effect=oversized), patch.object(owner, '_encode', wraps=owner._encode) as encode:
            with self.assertRaises(ValueError):
                self.sink.retain(owner.CASE_IDS[0], 0, b'original', b'response')
            self.assertEqual(encode.call_count, 1)  # Fixed bounded reservation only.
        fd = self.sink.files[0]['fd']
        with self.assertRaises(ValueError):
            self.sink.close()
        with self.assertRaises(OSError):
            os.fstat(fd)

    def test_private_directory_parent_alias_drift_refuses_and_closes_held_descriptors(self):
        self.complete()
        moved = self.root.parent / (self.root.name + '-moved')
        self.root.rename(moved)
        self.root.symlink_to(moved, target_is_directory=True)
        try:
            with self.assertRaises(ValueError):
                self.sink.close()
            with self.assertRaises(OSError):
                os.fstat(self.sink.directory_fd)
        finally:
            self.root.unlink()
            moved.rename(self.root)


class BinaryProfileTests(unittest.TestCase):
    def config(self, root):
        path, value = configured(root)
        value.update(profile=cli.PROFILE_BINARY, compiler_evidence_format=owner.FORMAT)
        path.write_text(json.dumps(value))
        return path, value

    def test_closed_three_gate_and_old_profiles_refuse_new_field_before_effects(self):
        with tempfile.TemporaryDirectory() as tmp:
            path, value = self.config(Path(tmp).resolve())
            selected = cli.parse_supply_chain_native_invocation(path.read_bytes())
            self.assertEqual(selected.native.compiler_evidence_format, owner.FORMAT)
            for change in ({'profile': cli.PROFILE_TWO_HOP}, {'profile': cli.PROFILE},
                           {'compiler_evidence_format': None}, {'compiler_evidence_format': True},
                           {'compiler_evidence_format': 'hex'}, {'two_hop': None}):
                path.write_text(json.dumps({**value, **change}))
                with patch.object(cli, 'publish_query_supply_chain_native') as effect, self.assertRaises(ValueError):
                    cli.run_supply_chain_native_command(path)
                effect.assert_not_called()
            value.pop('compiler_evidence_format')
            path.write_text(json.dumps(value))
            with self.assertRaises(ValueError):
                cli.parse_supply_chain_native_invocation(path.read_bytes())

    def test_real_binary_sink_same_driver_five_plus_twelve_no_report_hex(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            path, _ = self.config(root)
            config = cli.parse_supply_chain_native_invocation(path.read_bytes()).native
            config.output.mkdir(mode=0o700)
            sink = owner.SupplyChainBinaryEvidence(config.output)
            driver = SimpleNamespace(source=None)
            pack, aliases = b'original pack', {'original': 'alias'}
            calls = []
            def five(actual, p, a, *args, observe, observe_native):
                self.assertIs(actual, driver); self.assertIs(p, pack); self.assertIs(a, aliases)
                calls.append('five')
                for name in owner.CASE_IDS:
                    for iteration in (0, 1):
                        observe(name, iteration, b'count request', b'count response')
                    observe_native(name, {'rows': [['same', None], ['same', None]]})
                return {'cases': [{'id': name} for name in owner.CASE_IDS]}
            def twelve(actual, p, a, q, *, observe):
                self.assertIs(actual, driver); self.assertIs(p, pack); self.assertIs(a, aliases)
                self.assertIs(q, config.two_hop); calls.append('twelve')
                for ordinal in range(12):
                    for iteration in (0, 1):
                        observe(ordinal, iteration, b'path request', b'path response')
                return {'queries': [{'ordinal': i, 'requestBytes': b'path request',
                    'responseBytes': b'path response', 'evidence': {'rows': [['same', None], ['same', None]]}}
                    for i in range(12)]}
            import ashlar_host.supply_chain_two_hop_query as paths
            patches = [patch.object(native, '_schemas', return_value=None),
                       patch.object(native, 'CountStarAdmissionConfig', return_value=None),
                       patch.object(native, 'CountStarExecutionConfig', return_value=None),
                       patch.object(native, 'finite_public_source', return_value=None),
                       patch.object(native, 'query_supply_chain_cases', side_effect=five),
                       patch.object(paths, 'query_supply_chain_two_hop', side_effect=twelve)]
            primary = None
            try:
                with contextlib.ExitStack() as stack:
                    for port in patches: stack.enter_context(port)
                    result = native._queries(config, driver, pack, aliases, binary_sink=sink)
                self.assertEqual(calls, ['five', 'twelve'])
                self.assertEqual(result['compiler_evidence']['records'], 34)
                self.assertFalse((config.output / 'compiler-observations.jsonl').exists())
                self.assertEqual(len((config.output / 'native-observations.jsonl').read_bytes().splitlines()), 17)
                for q in result['two_hop']['queries']:
                    self.assertNotIn('requestBytes', q); self.assertNotIn('responseBytes', q)
                    self.assertEqual(q['protocol_reference']['iterations'], [0, 1])
                    self.assertEqual(q['evidence']['rows'], [['same', None], ['same', None]])
                self.assertNotIn(b'"hex"', native.report_body(result, 67108864))
            except BaseException as error:
                primary = error
            finish(primary, [sink.close])

    def test_actual_outer_closing_rechecks_binary_after_native_source_cleanup(self):
        import ashlar.weft_paths_distribution as paths
        for failure in (None, 'closing', 'cancel'):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp).resolve()
                path, _ = self.config(root)
                config = cli.parse_supply_chain_native_invocation(path.read_bytes()).native
                events, targets, sinks = [], [], []
                primary = KeyboardInterrupt()
                def source_close():
                    events.append('source-close')
                    if failure:
                        (sinks[0].root / sinks[0].files[0]['name']).write_bytes(b'changed')
                source = SimpleNamespace(batch=SimpleNamespace(batch_id='original'),
                    renew=source_close, metadata=lambda: {})
                class Spark:
                    def sql(self, text):
                        if text.startswith('CREATE TABLE delta.'):
                            Path(text.split('`')[1]).mkdir()
                        return SimpleNamespace(first=lambda: {'id': 'original'})
                    def stop(self): events.append('spark-close')
                class Transport:
                    def __init__(self, selected): self.targets = {t.table: t for t in selected}
                    def original_history(self, target): return []
                    def close(self): events.append('transport-close')
                class Lease:
                    def __init__(self, *a, **k): pass
                    def renew(self): events.append('lease-current')
                    def close(self): events.append('lease-close')
                class Driver:
                    def __init__(self, *a, **k): self.source, self.publication_id = a[5], a[6]
                    def prepare_original(self, request): pass
                    def backend(self): return object()
                    def restore_committed(self, request): self.restored = True
                def initialize(*a, **k): targets.extend(a[3]); return Transport(a[3])
                def query(c, driver, pack, aliases, *, binary_sink):
                    self.assertTrue(driver.restored); sinks.append(binary_sink)
                    for name, iteration in owner.INVENTORY:
                        binary_sink.retain(name, iteration, b'original request', b'original response')
                    if failure == 'cancel': raise primary
                    return {'cases': [{} for _ in range(5)],
                        'two_hop': {'queries': [{'ordinal': i} for i in range(12)]},
                        'compiler_evidence': binary_sink.reference()}
                patches = [patch.object(native, '_runtime', return_value=[]),
                    patch.object(native, '_spark', return_value=Spark()),
                    patch.object(native, 'snapshot', return_value=b'original'),
                    patch.object(native, 'supply_chain_cases'), patch.object(native, 'PrivateFiniteLease', Lease),
                    patch.object(native, 'HeldSupplyChainDataset', return_value=SimpleNamespace(metadata=lambda: {})),
                    patch.object(native, 'FiniteSupplyChainSource', return_value=source),
                    patch.object(native, 'fixture_columns', return_value={r: (('id', 'STRING'),) for r in native.GRAPH_ROLES}),
                    patch.object(native, '_request', return_value={'stream': 'original', 'predecessor': 'origin', 'schema_revisions_json': '{}'}),
                    patch.object(native.LocalDeltaTransport, 'initialize', side_effect=initialize),
                    patch.object(native.ReadOnlyTransport, 'open', side_effect=lambda *a, **k: Transport(targets)),
                    patch.object(native, 'FiniteSupplyChainDriver', Driver),
                    patch.object(native, 'publish_batch', return_value=SimpleNamespace(versions={}, raw={})),
                    patch.object(native, '_queries', side_effect=query),
                    patch.object(native, 'installed_count_star_schema_bundle', return_value={'schema': b'original'}),
                    patch.object(paths, 'installed_paths_schema_bundle', return_value={'schema': b'original'})]
                with contextlib.ExitStack() as stack:
                    for port in patches: stack.enter_context(port)
                    if failure:
                        with self.assertRaises(KeyboardInterrupt if failure == 'cancel' else ValueError) as caught:
                            native.publish_query_supply_chain_native(config)
                        if failure == 'cancel': self.assertIs(caught.exception, primary)
                        self.assertFalse((config.output / 'report.json').exists())
                    else:
                        result = native.publish_query_supply_chain_native(config)
                        report = json.loads(Path(result['report']).read_bytes())
                        self.assertEqual(report['compiler_evidence']['records'], 34)
                self.assertTrue(sinks[0].closed)
                self.assertIn('transport-close', events); self.assertIn('spark-close', events)
                self.assertIn('source-close', events); self.assertIn('lease-close', events)
                for entry in sinks[0].files:
                    with self.assertRaises(OSError): os.fstat(entry['fd'])


if __name__ == '__main__':
    unittest.main()
