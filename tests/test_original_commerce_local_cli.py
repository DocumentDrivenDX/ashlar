# SPDX-License-Identifier: MIT OR Apache-2.0
"""Installed dispatch/configuration controls, without source/provider/native effects."""
import json,sys,tempfile,subprocess,hashlib
from pathlib import Path
from unittest import TestCase,mock
from ashlar_host.original_commerce_local_cli import parse_original_commerce_local,run_original_commerce_local_command,_execute_original_commerce_local
from ashlar_host.original_commerce_local_configuration import OriginalCommerceLocalInitializationConfig

class Tests(TestCase):
    def value(self):
        return {'profile':'ashlar-original-commerce-local-initialization/0.1','root':'/private/tmp/explicit-native','output':'/private/tmp/explicit-output','installation_id':'operator-original',
            'model':'/model','graph':'/graph','jars':'/jars','capacity':None,
            'producer':{'source':'/producer','bun':'/bun','git':'/git','timeout_seconds':30,'maximum_output_bytes':1024,'maximum_receipt_bytes':4096},
            'runtime':{'python':'/python','python_sha256':'1'*64,'java_home':'/java','java_sha256':'2'*64,'jvm_config_sha256':'3'*64}}
    def test_closed_parse_and_duplicate_unknown_bool_float_refusal(self):
        value=self.value();self.assertIs(type(parse_original_commerce_local(json.dumps(value).encode(),initialize=True)),OriginalCommerceLocalInitializationConfig)
        for change in ({'installation_id':True},{'extra':'refused'},{'capacity':{'profile':'ashlar-private-local-operation-capacity/0.1','max_intent_bytes':8388608.0}}):
            altered=dict(value);altered.update(change)
            with self.assertRaises(ValueError):parse_original_commerce_local(json.dumps(altered).encode(),initialize=True)
        with self.assertRaises(ValueError):parse_original_commerce_local(b'{"profile":1,"profile":2}',initialize=True)
    def test_actual_dispatch_receives_typed_config_and_drift_withholds_success(self):
        with tempfile.TemporaryDirectory()as directory:
            path=Path(directory).resolve()/'config.json';raw=json.dumps(self.value()).encode();path.write_bytes(raw)
            captured=[]
            def initializer(config):
                captured.append(config);self.assertIsNotNone(config.custody);return {'ready':True}
            with mock.patch('ashlar_host.original_commerce_local_cli.initialize_original_commerce_local',side_effect=initializer):
                self.assertEqual(_execute_original_commerce_local(path,initialize=True,expected_sha256=hashlib.sha256(raw).hexdigest()),{'ready':True})
            self.assertIs(type(captured[0]),OriginalCommerceLocalInitializationConfig)
            def drift(config):path.write_bytes(raw+b' ');return {'ready':True}
            with mock.patch('ashlar_host.original_commerce_local_cli.initialize_original_commerce_local',side_effect=drift):
                with self.assertRaises(Exception):_execute_original_commerce_local(path,initialize=True,expected_sha256=hashlib.sha256(raw).hexdigest())
    def test_public_cli_dispatch_and_help_imports_no_native_host(self):
        from ashlar.cli import main
        with mock.patch.object(sys,'argv',['ashlar','initialize-original-commerce','--configuration','/explicit/config']),mock.patch('ashlar_host.original_commerce_local_cli.run_original_commerce_local_command',return_value={'ready':True})as run,mock.patch('builtins.print'):
            main();self.assertEqual(run.call_args.kwargs,{'initialize':True,'provider':None,'provider_sha256':None})
        root=Path(__file__).resolve().parents[1]
        code="import sys;sys.path.insert(0,"+repr(str(root/'src'))+");from ashlar.cli import main;sys.argv=['ashlar','initialize-original-commerce','--help'];\ntry:main()\nexcept SystemExit as e:assert e.code==0\nassert not any(n.startswith('ashlar_host')or n.startswith('pyspark')for n in sys.modules)"
        result=subprocess.run([sys.executable,'-I','-B','-S','-c',code],stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=5)
        self.assertEqual(result.returncode,0,result.stderr)

    def test_supervisor_actual_argv_environment_and_closing_on_cancellation(self):
        import os
        from ashlar_host.original_commerce_local_configuration import OriginalCommerceLocalRuntime
        with tempfile.TemporaryDirectory()as directory:
            path=Path(directory).resolve()/'config.json';path.write_bytes(json.dumps(self.value()).encode())
            events=[]
            def capture(argv,**kwargs):
                self.assertEqual(argv[:5],['/python','-I','-B','-m','ashlar_host.original_commerce_local_cli'])
                self.assertEqual(kwargs['timeout_seconds'],300)
                self.assertEqual(kwargs['environment']['JAVA_HOME'],'/java')
                self.assertEqual(kwargs['environment']['PYTHONDONTWRITEBYTECODE'],'1')
                self.assertNotIn('PGHOSTADDR',kwargs['environment'])
                self.assertEqual(argv[argv.index('--sha256')+1],hashlib.sha256(path.read_bytes()).hexdigest())
                kwargs['renew']();return b'{"ready":true}',b''
            with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(OriginalCommerceLocalRuntime,'renew',side_effect=lambda:events.append('renew')),mock.patch('ashlar_host.graphframes_release.capture_graphframes_worker',side_effect=capture):
                self.assertEqual(run_original_commerce_local_command(path,initialize=True),{'ready':True})
            self.assertEqual(len(events),3)
            cancel=KeyboardInterrupt();events.clear()
            with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(OriginalCommerceLocalRuntime,'renew',side_effect=lambda:events.append('renew')),mock.patch('ashlar_host.graphframes_release.capture_graphframes_worker',side_effect=cancel):
                with self.assertRaises(KeyboardInterrupt)as caught:run_original_commerce_local_command(path,initialize=True)
            self.assertIs(caught.exception,cancel);self.assertEqual(len(events),2)

    def test_real_owned_transport_timeout_still_renews_invocation(self):
        import os
        from ashlar_host.graphframes_release import capture_graphframes_worker
        from ashlar_host.original_commerce_local_configuration import OriginalCommerceLocalRuntime
        with tempfile.TemporaryDirectory()as directory:
            path=Path(directory).resolve()/'config.json';path.write_bytes(json.dumps(self.value()).encode())
            events=[]
            def transport(argv,**kwargs):
                self.assertEqual(kwargs['timeout_seconds'],300)
                return capture_graphframes_worker([sys.executable,'-I','-B','-c','import time;time.sleep(10)'],
                    cwd=kwargs['cwd'],environment=kwargs['environment'],renew=kwargs['renew'],
                    timeout_seconds=1,maximum_output_bytes=1024)
            with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(OriginalCommerceLocalRuntime,'renew',side_effect=lambda:events.append('renew')),mock.patch('ashlar_host.graphframes_release.capture_graphframes_worker',side_effect=transport):
                with self.assertRaises(ValueError):run_original_commerce_local_command(path,initialize=True)
            self.assertGreaterEqual(len(events),3)
