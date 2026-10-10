"""Explicit synthetic file admission; no SDK, native or credential discovery."""
from dataclasses import asdict
from pathlib import Path
import copy
import json
import os
import tempfile
import unittest
from unittest.mock import patch

from ashlar_host.config import DiagnosticsLimits, HostError, load_diagnostics_config


class ConfigFileTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.path = self.root / 'settings.json'
        self.values = dict(profile='ashlar-host-otel-http/0.1',
            capture_root=str(self.root / 'capture'), endpoint='http://127.0.0.1:4318/',
            headers=[['Authorization', 'synthetic-private-sentinel']], tls='loopback-test',
            ca_file=None, environment='test',
            limits=asdict(DiagnosticsLimits(1024,16,4096,4096,16384,100,8,100,1000,60)))

    def write(self, values=None):
        self.path.write_text(json.dumps(self.values if values is None else values))

    def refused(self):
        with self.assertRaisesRegex(HostError, '^diagnostics-configuration$') as caught:
            load_diagnostics_config(self.path)
        self.assertIsNone(caught.exception.__cause__)

    def test_exact_explicit_values_secret_wrapping_and_no_ambient_discovery(self):
        self.write()
        with patch.dict(os.environ, {'OTEL_EXPORTER_OTLP_ENDPOINT':'private-poison', 'HTTPS_PROXY':'private-poison'}):
            result = load_diagnostics_config(self.path)
        self.assertEqual(len(result.origins),17)
        self.assertEqual(set(result.origins.values()), {'explicit'})
        self.assertNotIn('synthetic-private-sentinel', repr(result))
        self.assertEqual(result.capture_root, self.root / 'capture')
        self.assertFalse(result.capture_root.exists())

    def test_closed_structural_shape_duplicate_keys_nonfinite_utf8(self):
        self.write();good=self.path.read_bytes()
        for raw in (b'\xff', b'{"endpoint":"private","endpoint":"other"}',
                    good.replace(b'"test"', b'NaN'), good.replace(b'"test"', b'1e999'),
                    good.replace(b'"max_event_bytes": 1024', b'"max_event_bytes": 1024,"max_event_bytes": 1024')):
            with self.subTest(raw=raw[:10]):self.path.write_bytes(raw);self.refused()
        for key in self.values:
            values=copy.deepcopy(self.values);del values[key];self.write(values);self.refused()
        values=copy.deepcopy(self.values);values['extra']='private';self.write(values);self.refused()
        for change in ({}, {'unknown':1}, {**self.values['limits'],'max_event_bytes':True}):
            values=copy.deepcopy(self.values);values['limits']=change;self.write(values);self.refused()
        for change in ([['Authorization']], [['Authorization',7]], {'Authorization':'private'}, ['private']):
            values=copy.deepcopy(self.values);values['headers']=change;self.write(values);self.refused()

    def test_depth_number_guards_reject_before_unbounded_conversion(self):
        self.write();good=self.path.read_bytes()
        self.path.write_bytes(b'['*13+b'0'+b']'*13)
        with patch('ashlar_host.config.json.loads',side_effect=AssertionError('decoder must not run')) as decoder:
            self.refused();decoder.assert_not_called()
        for number in (b'1'*21,b'-'+b'1'*21,b'1.0',b'1e0'):
            self.path.write_bytes(good.replace(b'"max_event_bytes": 1024',b'"max_event_bytes": '+number))
            with patch('ashlar_host.config.int',side_effect=AssertionError('integer conversion must not run'),create=True) as conversion:
                self.refused();conversion.assert_not_called()
        values=copy.deepcopy(self.values)
        values['headers']=[['Authorization','synthetic\\"'+('['*20)+(']'*20)]]
        self.write(values)
        self.assertEqual(load_diagnostics_config(self.path).environment,'test')

    def test_regular_absolute_nonsymlink_bounded_file(self):
        with self.assertRaises(HostError):load_diagnostics_config(Path('relative.json'))
        self.refused()
        self.path.mkdir();self.refused();self.path.rmdir()
        other=self.root/'other';other.write_text('{}');self.path.symlink_to(other);self.refused();self.path.unlink()
        for raw in (b'', b' '*32769):self.path.write_bytes(raw);self.refused()
        self.write();raw=self.path.read_bytes();self.path.write_bytes(raw+b' '*(32768-len(raw)))
        self.assertEqual(load_diagnostics_config(self.path).environment,'test')

    def test_replacement_between_inspection_and_open_refused_and_closed(self):
        self.write();actual=os.open;opened=[]
        def replace(path, flags):
            replacement=self.root/'replacement';replacement.write_text(self.path.read_text())
            replacement.replace(self.path)
            descriptor=actual(path,flags);opened.append(descriptor);return descriptor
        with patch('ashlar_host.config.os.open',side_effect=replace):self.refused()
        with self.assertRaises(OSError):os.fstat(opened[0])

    def test_growth_and_path_replacement_during_read_refused(self):
        actual=os.read
        for operation in ('grow','replace'):
            self.write();calls=[]
            def read(descriptor,size):
                raw=actual(descriptor,size)
                if not calls:
                    calls.append(True)
                    if operation=='grow':
                        with self.path.open('ab') as out:out.write(b' ')
                    else:
                        replacement=self.root/'replacement';replacement.write_text(self.path.read_text());replacement.replace(self.path)
                return raw
            with patch('ashlar_host.config.os.read',side_effect=read):self.refused()

    def test_cancellation_preserved_descriptor_closed_and_cleanup_attempted(self):
        self.write();actual_open=os.open;actual_close=os.close;opened=[]
        def opening(*args):
            descriptor=actual_open(*args);opened.append(descriptor);return descriptor
        for primary in (KeyboardInterrupt(),SystemExit(),GeneratorExit()):
            def closing(descriptor):actual_close(descriptor);raise OSError('private-cleanup')
            with patch('ashlar_host.config.os.open',side_effect=opening),patch('ashlar_host.config.os.read',side_effect=primary),patch('ashlar_host.config.os.close',side_effect=closing):
                with self.assertRaises(BaseException) as caught:load_diagnostics_config(self.path)
            self.assertIs(caught.exception,primary);self.assertTrue(primary.cleanup_failed)
            with self.assertRaises(OSError):os.fstat(opened[-1])
        for cancellation in (KeyboardInterrupt(),SystemExit(),GeneratorExit()):
            def closing(descriptor):actual_close(descriptor);raise cancellation
            with patch('ashlar_host.config.os.read',side_effect=OSError('private-read')),patch('ashlar_host.config.os.close',side_effect=closing):
                with self.assertRaises(BaseException) as caught:load_diagnostics_config(self.path)
            self.assertIs(caught.exception,cancellation)


if __name__ == '__main__':unittest.main()
