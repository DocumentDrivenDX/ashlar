"""Independent finite loader controls; synthetic files only, no SDK or network."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from dataclasses import asdict

from ashlar_host import config as c


class IndependentLoaderControls(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.path = self.root / 'synthetic.json'
        self.values = dict(profile='ashlar-host-otel-http/0.1', capture_root=str(self.root/'capture'),
            endpoint='http://127.0.0.1:4318/base', headers=[['Authorization','synthetic-only']],
            tls='loopback-test', ca_file=None, environment='test',
            limits=asdict(c.DiagnosticsLimits(1024,16,4096,4096,16384,100,8,100,1000,60)))
        self.raw = json.dumps(self.values).encode('utf8')
        self.path.write_bytes(self.raw)

    def refused(self):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            with self.assertRaises(c.HostError) as seen:
                c.load_diagnostics_config(self.path)
        self.assertEqual(str(seen.exception), 'diagnostics-configuration')
        self.assertIsNone(seen.exception.__cause__)
        self.assertTrue(seen.exception.__suppress_context__)
        self.assertEqual(out.getvalue()+err.getvalue(), '')
        return seen.exception

    def test_exact_depth_boundary_and_quote_escapes(self):
        loads = c.json.loads
        for depth in (12,13):
            self.path.write_bytes(b'['*depth+b'0'+b']'*depth)
            with patch.object(c.json,'loads',wraps=loads) as decoder:
                self.refused()
                self.assertEqual(decoder.call_count, int(depth == 12))
        for secret in ('['*100+']'*100, '\\"'+('{'*100)+'\\\\'+('}'*100)):
            self.values['headers']=[['X-Fixture',secret]]
            self.path.write_text(json.dumps(self.values))
            self.assertEqual(c.load_diagnostics_config(self.path).environment,'test')

    def test_integer_and_fraction_lexical_boundaries(self):
        for token, conversions in ((b'1'*20,1),(b'-'+b'1'*20,1),(b'1'*21,0),
                                   (b'-'+b'1'*21,0),(b'1.0',0),(b'1e-100000',0)):
            self.path.write_bytes(self.raw.replace(b'"max_event_bytes": 1024',b'"max_event_bytes": '+token))
            calls=[]
            def convert(raw):
                calls.append(raw)
                return int(raw)
            with patch.object(c,'int',side_effect=convert,create=True): self.refused()
            self.assertEqual(calls.count(token.decode('ascii')),conversions)
            if not conversions:self.assertEqual(calls,[])

    def test_unicode_duplicate_and_caller_origins_refuse(self):
        self.path.write_bytes(self.raw.replace(b'"max_event_bytes": 1024',
            b'"max_event_bytes": 1024,"max_event_\\u0062ytes": 1024'))
        self.refused()
        self.values['origins']={'endpoint':'explicit'}
        self.path.write_text(json.dumps(self.values));self.refused()

    def test_fifo_replacement_at_open_is_nonblocking_and_closed(self):
        actual_open=os.open; opened=[]
        def replacement(path,flags):
            self.assertTrue(flags & os.O_NONBLOCK)
            self.assertTrue(flags & os.O_NOFOLLOW)
            self.path.unlink();os.mkfifo(self.path)
            fd=actual_open(path,flags);opened.append(fd);return fd
        with patch.object(c.os,'open',side_effect=replacement):self.refused()
        self.assertEqual(len(opened),1)
        with self.assertRaises(OSError):os.fstat(opened[0])

    def test_same_size_overwrite_is_detected(self):
        actual_read=os.read;done=[]
        def changed(fd,size):
            value=actual_read(fd,size)
            if not done:
                done.append(True)
                info=self.path.stat()
                self.path.write_bytes(self.raw.replace(b'"test"',b'"prod"'))
                os.utime(self.path,ns=(info.st_atime_ns,info.st_mtime_ns+1000000000))
            return value
        with patch.object(c.os,'read',side_effect=changed):self.refused()

    def test_readonly_primary_and_cleanup_priority(self):
        class Cancel(BaseException):
            def __setattr__(self,name,value):
                if name=='cleanup_failed':raise RuntimeError('synthetic-marker')
                super().__setattr__(name,value)
        actual_close=os.close
        for primary in (Cancel(),KeyboardInterrupt(),SystemExit(),GeneratorExit()):
            def close(fd):actual_close(fd);raise OSError('synthetic-close')
            with patch.object(c.os,'read',side_effect=primary),patch.object(c.os,'close',side_effect=close):
                with self.assertRaises(BaseException) as seen:c.load_diagnostics_config(self.path)
            self.assertIs(seen.exception,primary)
        for cancellation in (KeyboardInterrupt(),SystemExit(),GeneratorExit()):
            def close(fd):actual_close(fd);raise cancellation
            with patch.object(c.os,'close',side_effect=close):
                with self.assertRaises(BaseException) as seen:c.load_diagnostics_config(self.path)
            self.assertIs(seen.exception,cancellation)

    def test_result_snapshot_redaction_and_no_effects(self):
        with patch.dict(os.environ,{'OTEL_EXPORTER_OTLP_ENDPOINT':'synthetic-poison','HTTPS_PROXY':'synthetic-poison'}):
            result=c.load_diagnostics_config(self.path)
        self.path.write_bytes(b'{}')
        self.assertEqual(result.environment,'test')
        self.assertEqual(len(result.origins),17)
        self.assertEqual(set(result.origins.values()),{'explicit'})
        self.assertFalse((self.root/'capture').exists())
        with self.assertRaises(TypeError):result.origins['endpoint']='field-default'
        self.assertNotIn('synthetic-only',repr(result))
        self.assertNotIn('synthetic-only',str(result.headers))


if __name__=='__main__':unittest.main(verbosity=2)
