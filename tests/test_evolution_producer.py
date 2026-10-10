import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from ashlar_host.evolution_producer import capture, snapshot
from ashlar_host.config import HostError


class EvolutionProducerTests(unittest.TestCase):
    def test_real_inert_child_closed_environment_and_stream_caps(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.assertEqual(capture((sys.executable, '-c', "import os;assert 'SECRET' not in os.environ;print('ok')"), cwd=root,
                                     environment={}, timeout_seconds=2, maximum_output_bytes=100), (b'ok\n', b''))
            with self.assertRaises(HostError):
                capture((sys.executable, '-c', "print('x'*101)"), cwd=root,
                        environment={}, timeout_seconds=2, maximum_output_bytes=100)

    def test_timeout_when_descendant_holds_pipe(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(HostError):
                capture((sys.executable, '-c', 'import os,time\nif os.fork()==0:time.sleep(10)\n'),
                        cwd=Path(temporary), environment={}, timeout_seconds=1, maximum_output_bytes=100)

    def test_setup_cancel_closes_real_owned_streams_and_preserves_identity(self):
        import subprocess
        created = []
        actual = subprocess.Popen
        def launch(*args, **kwargs):
            process = actual(*args, **kwargs)
            created.append(process)
            return process
        primary = KeyboardInterrupt()
        with tempfile.TemporaryDirectory() as temporary, patch('ashlar_host.evolution_producer.subprocess.Popen', side_effect=launch), patch('ashlar_host.evolution_producer.os.set_blocking', side_effect=primary):
            with self.assertRaises(KeyboardInterrupt) as caught:
                capture((sys.executable, '-c', 'import time;time.sleep(10)'), cwd=Path(temporary),
                        environment={}, timeout_seconds=2, maximum_output_bytes=100)
        self.assertIs(caught.exception, primary)
        self.assertTrue(created[0].stdout.closed and created[0].stderr.closed)
        self.assertIsNotNone(created[0].returncode)

    def test_descriptor_refuses_symlink_fifo_and_over_limit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); (root/'file').write_bytes(b'123')
            (root/'link').symlink_to(root/'file'); os.mkfifo(root/'fifo')
            for path, bound in ((root/'file', 2), (root/'link', 10), (root/'fifo', 10)):
                with self.assertRaises(HostError): snapshot(path, bound)
            self.assertEqual(snapshot(root/'file', 3), b'123')

    def test_success_kills_stdio_closed_descendant(self):
        import time
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); pidfile=root/'pid'
            code="import os,time\npid=os.fork()\nif pid==0:\n os.close(1);os.close(2)\n while True:\n  with open("+repr(str(pidfile))+",'ab') as f:f.write(b'x')\n  time.sleep(.01)\nelse:\n while not os.path.exists("+repr(str(pidfile))+"):time.sleep(.01)\n"
            capture((sys.executable,'-c',code),cwd=root,environment={},timeout_seconds=2,maximum_output_bytes=100)
            size=pidfile.stat().st_size
            time.sleep(.05)
            self.assertEqual(pidfile.stat().st_size,size)

    def test_body_cancel_survives_selector_cleanup_failure(self):
        primary=KeyboardInterrupt()
        with tempfile.TemporaryDirectory() as temporary, patch('ashlar_host.evolution_producer.os.set_blocking',side_effect=primary), patch.object(__import__('selectors').DefaultSelector, 'close',side_effect=OSError('private')):
            with self.assertRaises(KeyboardInterrupt) as caught:
                capture((sys.executable,'-c','pass'),cwd=Path(temporary),environment={},timeout_seconds=2,maximum_output_bytes=100)
        self.assertIs(caught.exception,primary)
        self.assertTrue(primary.cleanup_failed)

    def test_real_status_shell_free_argv_and_group_kill_precedes_reap(self):
        import subprocess
        actual_launch = subprocess.Popen
        actual_kill = os.killpg
        events = []
        def launch(*args, **kwargs):
            process = actual_launch(*args, **kwargs)
            actual_wait = process.wait
            def wait(*args, **kwargs):
                events.append('reap')
                return actual_wait(*args, **kwargs)
            process.wait = wait
            return process
        def kill(*args):
            events.append('kill')
            return actual_kill(*args)
        with tempfile.TemporaryDirectory() as temporary, patch('ashlar_host.evolution_producer.subprocess.Popen', side_effect=launch), patch('ashlar_host.evolution_producer.os.killpg', side_effect=kill):
            root = Path(temporary)
            token = '$(touch forbidden);`touch forbidden`; spaced'
            self.assertEqual(capture((sys.executable, '-c', 'import sys;print(sys.argv[1])', token), cwd=root, environment={}, timeout_seconds=2, maximum_output_bytes=100), (token.encode()+b'\n', b''))
            self.assertFalse((root/'forbidden').exists())
            self.assertEqual(events, ['kill', 'reap'])
            with self.assertRaises(HostError):
                capture((sys.executable, '-c', 'import sys;sys.exit(7)'), cwd=root, environment={}, timeout_seconds=2, maximum_output_bytes=100)
            self.assertEqual(events, ['kill', 'reap', 'kill', 'reap'])

    def test_early_supervisor_loss_cannot_forge_success(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(HostError):
                capture((sys.executable, '-c', 'import os,signal;os.kill(os.getppid(),signal.SIGKILL)'), cwd=Path(temporary), environment={}, timeout_seconds=2, maximum_output_bytes=100)

    def test_snapshot_fresh_cleanup_cancellation_wins_over_ordinary_failure(self):
        actual_close = os.close
        for cancellation in (KeyboardInterrupt(), SystemExit(), GeneratorExit()):
            def close(fd):
                actual_close(fd)
                raise cancellation
            with tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary)/'data'; path.write_bytes(b'value')
                with patch('ashlar_host.evolution_producer.os.read', side_effect=OSError('private')), patch('ashlar_host.evolution_producer.os.close', side_effect=close):
                    with self.assertRaises(type(cancellation)) as caught:
                        snapshot(path, 100)
                self.assertIs(caught.exception, cancellation)

    def test_post_launch_setup_failures_close_all_owned_descriptors(self):
        import subprocess
        actual_launch = subprocess.Popen
        actual_close = os.close
        actual_pipe = os.pipe
        for seam in ('close', 'fdopen'):
            for failure in (OSError('private'), KeyboardInterrupt()):
                processes = []
                descriptors = []
                failed = [False]
                def pipe():
                    pair = actual_pipe()
                    descriptors.extend(pair)
                    return pair
                def launch(*args, **kwargs):
                    process = actual_launch(*args, **kwargs)
                    processes.append(process)
                    return process
                def close(fd):
                    if processes and fd == descriptors[1] and not failed[0]:
                        failed[0] = True
                        raise failure
                    return actual_close(fd)
                with tempfile.TemporaryDirectory() as temporary, patch('ashlar_host.evolution_producer.os.pipe', side_effect=pipe), patch('ashlar_host.evolution_producer.subprocess.Popen', side_effect=launch):
                    fault = patch('ashlar_host.evolution_producer.os.close', side_effect=close) if seam == 'close' else patch('ashlar_host.evolution_producer.os.fdopen', side_effect=failure)
                    with fault:
                        with self.assertRaises(HostError if isinstance(failure, Exception) else type(failure)) as caught:
                            capture((sys.executable, '-c', 'import time;time.sleep(10)'), cwd=Path(temporary), environment={}, timeout_seconds=2, maximum_output_bytes=100)
                if not isinstance(failure, Exception):
                    self.assertIs(caught.exception, failure)
                self.assertEqual(len(processes), 1)
                self.assertTrue(processes[0].stdout.closed)
                self.assertTrue(processes[0].stderr.closed)
                self.assertIsNotNone(processes[0].returncode)
                for fd in descriptors[:2]:
                    with self.assertRaises(OSError):
                        os.fstat(fd)
