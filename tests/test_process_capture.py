"""Real tiny local transport children only; no source/engine/remote execution."""
import os,sys,tempfile,unittest,subprocess
from pathlib import Path
from unittest.mock import patch
from ashlar_host.config import HostError
from ashlar_host.process_capture import capture_process
from ashlar_host.evolution_producer import capture

class Tests(unittest.TestCase):
    def test_retained_leader_and_distinct_finite_public_profiles(self):
        with tempfile.TemporaryDirectory()as temp:
            root=Path(temp)
            self.assertEqual(capture_process((sys.executable,'-c',"import os;assert os.getppid()==os.getsid(0);print('owned')"),cwd=root,environment={},timeout_seconds=600,maximum_output_bytes=100),(b'owned\n',b''))
            for timeout in (True,0,601):
                with self.assertRaises(HostError):capture_process((sys.executable,'-c','pass'),cwd=root,environment={},timeout_seconds=timeout,maximum_output_bytes=100)
            with patch('ashlar_host.evolution_producer.capture_process')as delegated:
                with self.assertRaises(HostError):capture((sys.executable,'-c','pass'),cwd=root,environment={},timeout_seconds=61,maximum_output_bytes=100)
                delegated.assert_not_called()
    def test_timeout_and_cancellation_reap_original_owned_group(self):
        original=subprocess.Popen;created=[]
        def launch(*args,**kwargs):
            process=original(*args,**kwargs);created.append(process);return process
        with tempfile.TemporaryDirectory()as temp,patch('ashlar_host.process_capture.subprocess.Popen',side_effect=launch):
            root=Path(temp)
            with self.assertRaises(HostError):capture_process((sys.executable,'-c','import time;time.sleep(10)'),cwd=root,environment={},timeout_seconds=1,maximum_output_bytes=100)
            primary=KeyboardInterrupt()
            with patch('ashlar_host.process_capture.os.set_blocking',side_effect=primary):
                with self.assertRaises(KeyboardInterrupt)as caught:capture_process((sys.executable,'-c','import time;time.sleep(10)'),cwd=root,environment={},timeout_seconds=2,maximum_output_bytes=100)
            self.assertIs(caught.exception,primary)
            for process in created:
                self.assertIsNotNone(process.returncode);self.assertTrue(process.stdout.closed and process.stderr.closed)
                with self.assertRaises(ProcessLookupError):os.killpg(process.pid,0)

if __name__=='__main__':unittest.main()
