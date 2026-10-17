"""Installed command wiring and real bounded diagnostics; no native services."""
from contextlib import contextmanager
import hashlib,json,os,sys,unittest
from pathlib import Path
from unittest.mock import patch
import test_managed_singleton_configuration as fixture
from ashlar_host import managed_singleton_cli as command
from ashlar_host.managed_singleton_configuration import parse_managed_singleton
from ashlar_host.process_capture import capture_process

class Tests(unittest.TestCase):
    def setUp(self):
        self.template=fixture.Tests();self.template.setUp();self.addCleanup(self.template.doCleanups)
        self.value=self.template.value;self.root=Path(self.template.temp.name).resolve()
        self.file=self.root/'command.json';self.raw=json.dumps(self.value).encode();self.file.write_bytes(self.raw)
        self.config=parse_managed_singleton(self.raw)
    def test_parent_closes_capture_and_retains_only_matching_complete_report(self):
        report=command._report(self.config,{'id':'1'})
        with patch.object(command,'capture_process',return_value=(command._encoded(report),b'bounded diagnostic'))as capture:
            self.assertEqual(command.run_managed_singleton_command(self.file),report)
        args=capture.call_args;self.assertIn('ashlar_host.managed_singleton_cli',args.args[0])
        self.assertEqual(args.kwargs['timeout_seconds'],300)
        self.assertEqual(json.loads((self.config.output/'report.json').read_bytes()),report)
        self.assertEqual((self.config.output/'diagnostics.log').read_bytes(),b'bounded diagnostic')
        self.assertEqual((self.config.output/'report.json').stat().st_mode&0o777,0o600)
    def test_configuration_drift_withholds_report_and_keeps_safe_failure(self):
        def captured(*args,**kwargs):
            self.file.write_bytes(self.raw+b'\n')
            return command._encoded(command._report(self.config,None)),b''
        with patch.object(command,'capture_process',side_effect=captured):
            with self.assertRaises(Exception):command.run_managed_singleton_command(self.file)
        self.assertFalse((self.config.output/'report.json').exists())
        self.assertEqual(json.loads((self.config.output/'failure.json').read_bytes())['status'],'refused')
    def test_worker_returns_after_both_actual_owner_contexts_close(self):
        self.config.output.mkdir(mode=0o700);events=[];selected=object();reader=object()
        @contextmanager
        def selection(_):
            events.append('selection-open');yield selected;events.append('selection-close')
        @contextmanager
        def sql(*args,**kwargs):
            events.append('sql-open');yield reader;events.append('sql-close')
        def read(actual,transport,pins,**kwargs):
            self.assertIs(actual,selected);self.assertIs(transport,reader)
            self.assertIs(pins.context,kwargs['context']);return None
        with patch.object(command,'open_managed_publication_selection',selection),patch.object(command,'open_managed_sql_read',sql),patch.object(command,'managed_sql_child_context',return_value=object()),patch.object(command,'read_managed_singleton',side_effect=read):
            raw=command._worker(self.file,hashlib.sha256(self.raw).hexdigest())
        self.assertEqual(events,['selection-open','sql-open','sql-close','selection-close'])
        self.assertIsNone(json.loads(raw)['row'])
    def test_python_and_descriptor_one_diagnostics_leave_clean_protocol(self):
        source=Path(__file__).resolve().parents[1]/'src'
        code='import sys,os;sys.path.insert(0,'+repr(str(source))+');from ashlar_host.managed_singleton_cli import _diagnostics\nwith _diagnostics():\n print("python diagnostic",flush=True)\n os.write(1,b"descriptor diagnostic\\n")\nprint("protocol",flush=True)'
        out,err=capture_process([sys.executable,'-I','-B','-c',code],cwd=self.root,
            environment={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C'},timeout_seconds=5,maximum_output_bytes=4096)
        self.assertEqual(out,b'protocol\n');self.assertIn(b'python diagnostic',err);self.assertIn(b'descriptor diagnostic',err)
    def test_public_cli_dispatch_uses_closed_installed_command(self):
        from ashlar.cli import main
        with patch.object(sys,'argv',['ashlar','read-managed-singleton','--configuration',str(self.file)]),patch.object(command,'run_managed_singleton_command',return_value=command._report(self.config,None))as run,patch('builtins.print'):
            main()
        run.assert_called_once_with(self.file)
