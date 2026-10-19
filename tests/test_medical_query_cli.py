# SPDX-License-Identifier: MIT OR Apache-2.0
"""Public batch CLI composition with inert runtime/compiler/native ports."""
from contextlib import contextmanager
import hashlib,json,os,sys,subprocess,unittest
from types import SimpleNamespace
from unittest.mock import patch
import test_original_medical_local_configuration as fixture
import ashlar_host.original_medical_local_cli as owner
from ashlar_host.medical_query_configuration import parse_original_medical_query,retain_medical_query_selection
from ashlar_host.medical_process_supervision import OriginalMedicalNativePhase


class Tests(unittest.TestCase):
    def setUp(self):
        f=fixture.OriginalMedicalConfigurationTests();f.setUp();self.addCleanup(f.doCleanups)
        self.f=f;self.path=f.root/'query.json'
        self.value={'profile':'ashlar-original-medical-query-batch/0.1','read':f.configuration('read'),
            'compiler':{'index':str(f.root/'original-index.json'),'installation':str(f.root/'compiler')},'cases':['singleton','two-hop']}
        self.raw=json.dumps(self.value).encode();self.path.write_bytes(self.raw)
        read,write=os.pipe();self.addCleanup(os.close,read);self.addCleanup(os.close,write);os.set_blocking(write,False);self.phase=OriginalMedicalNativePhase(write)

    def test_closed_config_no_ambient_defaults_or_native_effects(self):
        config=parse_original_medical_query(self.raw)
        self.assertEqual(config.cases,('singleton','two-hop'));self.assertIs(config.runtime,config.read.runtime)
        for change in ('extra','missing','cases','read','compiler'):
            value=json.loads(self.raw)
            if change=='extra':value['grant']=True
            elif change=='missing':del value['cases']
            elif change=='cases':value['cases']=['foreign']
            elif change=='read':value['read']=self.f.configuration('publication')
            else:value['compiler']['package']='untrusted'
            with self.subTest(change=change),self.assertRaises(ValueError):parse_original_medical_query(json.dumps(value).encode())

    def test_distribution_entry_closing_and_selection_drift(self):
        config=parse_original_medical_query(self.raw)
        with patch('ashlar_host.medical_query_configuration.open_implicit_arithmetic_distribution',return_value=SimpleNamespace(ready_bytes=b'original'))as opened:
            renew=retain_medical_query_selection(config);renew();self.assertEqual(opened.call_count,2)
            opened.return_value=SimpleNamespace(ready_bytes=b'changed')
            with self.assertRaises(PermissionError):renew()
            object.__setattr__(config,'cases',('singleton',))
            with self.assertRaises(PermissionError):renew()

    def test_worker_typed_public_dispatch_and_primary_preserved(self):
        primary=KeyboardInterrupt()
        for fail in (False,True):
            with patch('ashlar_host.medical_query_configuration.open_implicit_arithmetic_distribution',return_value=SimpleNamespace(ready_bytes=b'original'))as opened,patch('ashlar_host.medical_query_local.query_original_medical_cases',side_effect=primary if fail else None,return_value={'closed':True})as run:
                if fail:
                    with self.assertRaises(KeyboardInterrupt)as caught:owner.execute_original_medical_query(self.path,expected_sha256=hashlib.sha256(self.raw).hexdigest(),phase=self.phase)
                    self.assertIs(caught.exception,primary)
                else:self.assertEqual(owner.execute_original_medical_query(self.path,expected_sha256=hashlib.sha256(self.raw).hexdigest(),phase=self.phase),{'closed':True})
                self.assertEqual(opened.call_count,2);self.assertEqual(run.call_args.kwargs['cases'],('singleton','two-hop'));self.assertIs(run.call_args.kwargs['supervision'],self.phase)

    def test_parent_whitelisted_query_capture_and_closing(self):
        events=[]
        @contextmanager
        def custody(*args):
            events.append('open')
            try:yield SimpleNamespace(native_environment=lambda:{'CLOSED':'yes'},renew=lambda:events.append('renew'))
            finally:events.append('close')
        def capture(argv,**kw):
            self.assertEqual(argv[5:7],['--operation','query']);self.assertEqual(kw['environment'],{'CLOSED':'yes'});kw['renew']();return b'{"closed":true}',b''
        with patch('ashlar_host.medical_query_configuration.open_implicit_arithmetic_distribution',return_value=SimpleNamespace(ready_bytes=b'original'))as opened,patch.object(owner.OriginalMedicalRuntimeCustody,'open',side_effect=custody),patch.object(owner,'capture_original_medical_worker',side_effect=capture):
            self.assertEqual(owner.run_original_medical_query_command(self.path),{'closed':True});self.assertEqual(opened.call_count,2)
        self.assertEqual(events,['open','renew','close'])

    def test_public_command_dispatch(self):
        from ashlar.cli import main
        with patch.object(sys,'argv',['ashlar','query-original-medical','--configuration',str(self.path)]),patch.object(owner,'run_original_medical_query_command',return_value={'closed':True})as run,patch('builtins.print'):
            main();run.assert_called_once_with(self.path)

    def test_closing_compiler_failure_withholds_worker_result(self):
        with patch('ashlar_host.medical_query_configuration.open_implicit_arithmetic_distribution',side_effect=[SimpleNamespace(ready_bytes=b'original'),PermissionError('closing')]),patch('ashlar_host.medical_query_local.query_original_medical_cases',return_value={'closed':True}):
            with self.assertRaises(PermissionError):owner.execute_original_medical_query(self.path,expected_sha256=hashlib.sha256(self.raw).hexdigest(),phase=self.phase)

    def test_help_is_lazy_and_worker_config_digest_is_required(self):
        root=__import__('pathlib').Path(__file__).resolve().parents[1]
        code="import sys;sys.path.insert(0,"+repr(str(root/'src'))+");from ashlar.cli import main;sys.argv=['ashlar','query-original-medical','--help'];\ntry:main()\nexcept SystemExit as e:assert e.code==0\nassert not any(n.startswith('ashlar_host')or n.startswith('pyspark')for n in sys.modules)"
        result=subprocess.run([sys.executable,'-I','-B','-S','-c',code],stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=5)
        self.assertEqual(result.returncode,0,result.stderr)
        with patch('ashlar_host.medical_query_configuration.open_implicit_arithmetic_distribution')as opened:
            with self.assertRaises(PermissionError):owner.execute_original_medical_query(self.path,expected_sha256='0'*64,phase=self.phase)
            opened.assert_not_called()
