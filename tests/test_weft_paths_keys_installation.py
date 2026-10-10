"""Finite installation/transport controls; fake bytes and fixture authority only."""
from dataclasses import replace
from pathlib import Path
import os
import sys
import tempfile
import unittest
from unittest.mock import patch
from ashlar import weft_paths_keys_installation as i
from ashlar.weft_paths_keys_package import PathsKeysInstallationConfig,VerifiedPathsKeysPackage,PathsKeysInstallationError

class PathsKeysInstallationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name).resolve()
        self.config=PathsKeysInstallationConfig(self.root/'index','a'*40,'b'*64,None,'fixture-paths',self.root/'output','aarch64-apple-darwin','27.0.1')
        self.verified=VerifiedPathsKeysPackage(b'{}',b'{}',b'fixture binary',b'{}',(('backend-manifest.json',b'{}'),)+tuple(('schemas/'+name,b'{}')for name in i.INSTALLED_SCHEMAS))
    def tearDown(self):self.temp.cleanup()
    def test_install_none_refuses_before_effects(self):
        with patch.object(i,'inspect_package',side_effect=PathsKeysInstallationError('refuse')):
            with self.assertRaises(PathsKeysInstallationError):i.install(self.config)
        self.assertFalse(self.config.output.exists())
    def fixture_install(self,**kwargs):
        def verify(config,raw,**unused):return i.PathsKeysInstallation(config,raw)
        return patch.multiple(i,inspect_package=lambda _:self.verified,_verify=verify,**kwargs)
    def test_final_ready_commit_retains_schema_resources(self):
        with self.fixture_install():result=i.install(self.config)
        self.assertTrue((self.config.output/'ready.json').is_file());self.assertFalse(result.cleanup_pending)
        self.assertEqual((self.config.output/'weft-paths-keys').read_bytes(),b'fixture binary')
        self.assertTrue(all((self.config.output/'schemas'/n).exists()for n in i.INSTALLED_SCHEMAS))
    def test_precommit_verification_failure_never_available(self):
        with patch.object(i,'inspect_package',return_value=self.verified),patch.object(i,'_verify',side_effect=PathsKeysInstallationError('refuse')):
            with self.assertRaises(PathsKeysInstallationError):i.install(self.config)
        self.assertFalse((self.config.output/'ready.json').exists())
    def test_postcommit_cleanup_is_maintenance_success(self):
        original=i.shutil.rmtree
        def cleanup(path):
            if Path(path).name.startswith('.ashlar-paths-'):raise OSError('maintenance')
            return original(path)
        with self.fixture_install(),patch.object(i.shutil,'rmtree',side_effect=cleanup):result=i.install(self.config)
        self.assertTrue(result.cleanup_pending);self.assertTrue((self.config.output/'ready.json').exists())
    def test_postcommit_cancellation_propagates_with_ready_retained(self):
        primary=KeyboardInterrupt()
        with self.fixture_install(),patch.object(i.shutil,'rmtree',side_effect=primary):
            with self.assertRaises(KeyboardInterrupt)as caught:i.install(self.config)
        self.assertIs(caught.exception,primary);self.assertTrue((self.config.output/'ready.json').is_file())

    def test_cancellation_and_cleanup_preserve_exact_primary(self):
        primary=KeyboardInterrupt()
        with patch.object(i,'inspect_package',return_value=self.verified),patch.object(i,'_write_owned',side_effect=primary),patch.object(i.shutil,'rmtree',side_effect=OSError('cleanup')):
            with self.assertRaises(KeyboardInterrupt)as caught:i.install(self.config)
        self.assertIs(caught.exception,primary);self.assertFalse((self.config.output/'ready.json').exists())
    def test_restart_trust_gate_precedes_ready_read(self):
        with patch.object(i,'verify_trusted_index',side_effect=PathsKeysInstallationError('pin')),patch.object(i,'read_snapshot',side_effect=AssertionError('ready read before pin')):
            with self.assertRaises(PathsKeysInstallationError):i.open_installation(self.config)
    def script_installation(self,body):
        self.config.output.mkdir();script=self.config.output/'weft-paths-keys';script.write_text('#!'+sys.executable+'\n'+body);script.chmod(0o555)
        return i.PathsKeysInstallation(self.config,b'fixture-ready')
    def test_bounded_fake_transport_original_stdout_and_closing_gate(self):
        raw=b'{"interfaceVersion":"weft-compile/0.4.0","status":"blocked"}\n'
        installed=self.script_installation('import sys\nsys.stdin.buffer.read()\nsys.stdout.buffer.write('+repr(raw)+')\n')
        with patch.object(i,'open_installation',return_value=installed):self.assertEqual(i.compile_request(installed,b'original request'),raw)
        changed=replace(installed,ready_bytes=b'changed')
        with patch.object(i,'open_installation',side_effect=[installed,changed]):
            with self.assertRaises(PathsKeysInstallationError):i.compile_request(installed,b'original request')
    def test_transport_profile_and_request_bounds_refuse(self):
        installed=self.script_installation('print(\'{"interfaceVersion":"weft-compile/0.2.0","status":"blocked"}\')\n')
        with patch.object(i,'open_installation',return_value=installed):
            with self.assertRaises(PathsKeysInstallationError):i.compile_request(installed,b'original request')
        with patch.object(i,'open_installation',side_effect=AssertionError('must refuse first')):
            with self.assertRaises(PathsKeysInstallationError):i.compile_request(installed,b'x'*(i.PROTOCOL_LIMIT+1))
    def test_successful_leader_cleanup_targets_stdio_closed_descendant(self):
        pidfile=self.root/'descendant-pid'
        body="import os,time,sys\npid=os.fork()\nif pid==0:\n os.close(0);os.close(1);os.close(2);time.sleep(10);os._exit(0)\nopen("+repr(str(pidfile))+",'w').write(str(pid))\nprint('{\"interfaceVersion\":\"weft-compile/0.4.0\",\"status\":\"blocked\"}')\n"
        installed=self.script_installation(body);original=os.killpg
        with patch.object(i,'open_installation',return_value=installed),patch.object(i.os,'killpg',wraps=original)as kill:
            i.compile_request(installed,b'original')
        self.assertEqual(kill.call_count,1);self.assertEqual(kill.call_args.args[1],i.signal.SIGKILL)
        self.assertTrue(pidfile.exists())

    def test_early_setup_cancellation_closes_all_real_owned_pipes(self):
        installed=self.script_installation('import time\ntime.sleep(10)\n')
        original=i.subprocess.Popen
        for boundary in ('blocking','register'):
            processes=[];primary=KeyboardInterrupt()
            def spawn(*args,**kwargs):
                child=original(*args,**kwargs);processes.append(child);return child
            selector=i.selectors.DefaultSelector()
            failing=patch.object(i.os,'set_blocking',side_effect=primary) if boundary=='blocking' else patch.object(selector,'register',side_effect=primary)
            with patch.object(i,'open_installation',return_value=installed),patch.object(i.subprocess,'Popen',side_effect=spawn),patch.object(i.selectors,'DefaultSelector',return_value=selector),failing:
                with self.assertRaises(KeyboardInterrupt)as caught:i.compile_request(installed,b'original')
            self.assertIs(caught.exception,primary);self.assertEqual(len(processes),1)
            child=processes[0];self.assertIsNotNone(child.poll())
            self.assertTrue(all(stream.closed for stream in (child.stdin,child.stdout,child.stderr)))

    def test_transport_cancellation_not_masked_by_selector_cleanup(self):
        installed=self.script_installation('import time\ntime.sleep(10)\n');primary=KeyboardInterrupt();real=i.selectors.DefaultSelector()
        with patch.object(i,'open_installation',return_value=installed),patch.object(i.selectors,'DefaultSelector',return_value=real),patch.object(real,'select',side_effect=primary),patch.object(real,'close',side_effect=OSError('close')):
            with self.assertRaises(KeyboardInterrupt)as caught:i.compile_request(installed,b'original')
        self.assertIs(caught.exception,primary);real.close()

if __name__=='__main__':unittest.main()
