"""Installed worker dispatch controls; runtime/native delegates remain inert."""
from contextlib import contextmanager
import hashlib,json,os
from pathlib import Path
import subprocess,sys,unittest
from unittest.mock import patch
import test_original_medical_local_configuration as fixture
from ashlar_host.medical_process_supervision import OriginalMedicalNativePhase
from ashlar_host.original_medical_local_cli import (
    execute_original_medical_initialization,run_original_medical_initialization_command)
from ashlar_host.original_medical_local_configuration import OriginalMedicalInitializationConfig

class Tests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixture.OriginalMedicalConfigurationTests();self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.path=self.fixture.root/'configuration.json'
        self.raw=json.dumps(self.fixture.configuration()).encode();self.path.write_bytes(self.raw)
        read,write=os.pipe();self.addCleanup(os.close,read);self.addCleanup(os.close,write)
        os.set_blocking(write,False);self.phase=OriginalMedicalNativePhase(write)
        self.digest=hashlib.sha256(self.raw).hexdigest()
    def execute(self):
        return execute_original_medical_initialization(self.path,expected_sha256=self.digest,phase=self.phase)
    def test_exact_worker_digest_phase_and_typed_initialization(self):
        with patch('ashlar_host.original_medical_local_cli.initialize_original_medical_local',return_value={'ready':True})as initialize:
            self.assertEqual(self.execute(),{'ready':True})
            self.assertIs(type(initialize.call_args.args[0]),OriginalMedicalInitializationConfig)
            self.assertIs(initialize.call_args.kwargs['supervision'],self.phase)
    def test_worker_config_drift_and_cancellation_withhold_success(self):
        def drift(config,**kwargs):self.path.write_bytes(self.raw+b' ');return {'ready':True}
        with patch('ashlar_host.original_medical_local_cli.initialize_original_medical_local',side_effect=drift):
            with self.assertRaises(Exception):self.execute()
        self.path.write_bytes(self.raw);cancel=KeyboardInterrupt()
        with patch('ashlar_host.original_medical_local_cli.initialize_original_medical_local',side_effect=cancel):
            with self.assertRaises(KeyboardInterrupt)as caught:self.execute()
        self.assertIs(caught.exception,cancel)
    def test_wrong_digest_phase_and_operation_refuse_before_initialization(self):
        with patch('ashlar_host.original_medical_local_cli.initialize_original_medical_local')as initialize:
            for digest,phase in (('b'*64,self.phase),(True,self.phase),(self.digest,object())):
                with self.assertRaises(Exception):
                    execute_original_medical_initialization(self.path,expected_sha256=digest,phase=phase)
            self.path.write_bytes(json.dumps(self.fixture.configuration('publication')).encode())
            with self.assertRaises(ValueError):run_original_medical_initialization_command(self.path)
            initialize.assert_not_called()
    @contextmanager
    def custody(self,*args):
        class Held:
            def renew(inner):self.events.append('renew')
            def native_environment(inner):return {'PATH':'/usr/bin:/bin','LANG':'C'}
        self.events.append('open')
        try:yield Held()
        finally:self.events.append('close')
    def parent(self,capture):
        self.events=[]
        with patch('ashlar_host.original_medical_local_cli.OriginalMedicalRuntimeCustody.open',side_effect=self.custody),patch('ashlar_host.original_medical_local_cli.capture_original_medical_worker',side_effect=capture):
            return run_original_medical_initialization_command(self.path)
    def test_parent_uses_fixed_isolated_selected_argv_environment_bounds_and_custody(self):
        def capture(argv,**kwargs):
            self.assertEqual(argv[:5],[str(self.fixture.root/'python'),'-I','-B','-m','ashlar_host.original_medical_local_cli'])
            self.assertEqual(argv[-1],self.digest)
            self.assertEqual(kwargs['environment'],{'PATH':'/usr/bin:/bin','LANG':'C'})
            self.assertEqual(kwargs['bounds'].native_seconds,300)
            self.assertEqual(kwargs['cwd'],self.path.parent)
            kwargs['renew']();return b'{"ready":true}\n',b''
        with patch.dict(os.environ,{'PYTHONPATH':'foreign','PGHOST':'foreign'},clear=False):
            self.assertEqual(self.parent(capture),{'ready':True})
        self.assertEqual(self.events,['open','renew','close'])
    def test_parent_cancellation_and_config_drift_close_custody(self):
        cancel=KeyboardInterrupt()
        def capture(*args,**kwargs):raise cancel
        with self.assertRaises(KeyboardInterrupt)as caught:self.parent(capture)
        self.assertIs(caught.exception,cancel);self.assertEqual(self.events,['open','close'])
        def drift(*args,**kwargs):self.path.write_bytes(self.raw+b' ');return b'{"ready":true}',b''
        with self.assertRaises(Exception):self.parent(drift)
        self.assertEqual(self.events,['open','close'])
    def test_parent_result_bounds_duplicate_nonfinite_and_shape(self):
        for raw in (b'',b'[]',b'{"x":1,"x":2}',b'{"x":NaN}',b'x'*1048577):
            with self.subTest(raw=raw[:30]),self.assertRaises(ValueError):
                self.parent(lambda *args,**kwargs:(raw,b''))

    def test_public_dispatch_and_help_do_not_import_native_host(self):
        from ashlar.cli import main
        with patch.object(sys,'argv',['ashlar','initialize-original-medical','--configuration',str(self.path)]),patch('ashlar_host.original_medical_local_cli.run_original_medical_initialization_command',return_value={'ready':True})as run,patch('builtins.print'):
            main();self.assertEqual(run.call_args.args,(self.path,))
        root=Path(__file__).resolve().parents[1]
        code="import sys;sys.path.insert(0,"+repr(str(root/'src'))+");from ashlar.cli import main;sys.argv=['ashlar','initialize-original-medical','--help'];\ntry:main()\nexcept SystemExit as e:assert e.code==0\nassert not any(n.startswith('ashlar_host')or n.startswith('pyspark')for n in sys.modules)"
        result=subprocess.run([sys.executable,'-I','-B','-S','-c',code],stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=5)
        self.assertEqual(result.returncode,0,result.stderr)
