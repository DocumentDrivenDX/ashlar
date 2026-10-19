# SPDX-License-Identifier: MIT OR Apache-2.0
"""Real tiny child/pipe/group capture controls, without Spark or data writes."""
from dataclasses import replace
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from ashlar_host.medical_worker_capture import capture_original_medical_worker
from ashlar_host.original_medical_local_configuration import OriginalMedicalBounds


class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.bounds = OriginalMedicalBounds(4194304, 10000, 1048576, 16777216,
            1048576, 1048576, 8388608, 300, 540, 'local[1]', '512m')
        self.environment = {'PATH': '/usr/bin:/bin', 'PYTHONDONTWRITEBYTECODE': '1'}

    def run_child(self, body, *, renew=lambda: None, bounds=None):
        source = "import os,sys,time\nfd=int(sys.argv[-1])\n" + body
        return capture_original_medical_worker(
            [sys.executable, '-I', '-B', '-c', source], cwd=self.root,
            environment=self.environment, renew=renew, bounds=bounds or self.bounds)

    def test_exact_phase_and_two_independent_bounded_streams(self):
        self.assertEqual(self.run_child(
            "os.write(fd,b'native.begin\\n');os.write(1,b'ok');os.write(2,b'diagnostic')\n"
            "os.write(fd,b'native.end\\n')\n"), (b'ok', b'diagnostic'))

    def test_missing_duplicate_reversed_unknown_and_truncated_phase_refuse(self):
        for signals in (b'', b'native.end\n', b'native.begin\nnative.begin\n',
                b'native.begin\nnative.end\nnative.end\n', b'foreign\n', b'native.begin'):
            with self.subTest(signals=signals), self.assertRaises(ValueError):
                self.run_child('os.write(fd,' + repr(signals) + ')\n')

    def test_each_stream_bound_refuses_independently(self):
        for descriptor in (1, 2):
            with self.subTest(descriptor=descriptor), self.assertRaises(ValueError) as caught:
                self.run_child("os.write(fd,b'native.begin\\n')\n"
                    + f"os.write({descriptor},b'12345')\n"
                    + "os.write(fd,b'native.end\\n')\n",
                    bounds=replace(self.bounds, stream_bytes=4))
            stream = 'stdout' if descriptor == 1 else 'stderr'
            self.assertEqual(caught.exception.medical_capture[stream], b'1234')

    def test_nonzero_exit_remains_failure_with_complete_phase(self):
        import subprocess
        with self.assertRaises(subprocess.CalledProcessError) as caught:
            self.run_child("os.write(fd,b'native.begin\\nnative.end\\n');sys.exit(7)\n")
        self.assertEqual(caught.exception.returncode, 7)
        self.assertEqual(caught.exception.medical_capture['phase'], b'native.begin\nnative.end\n')
        self.assertFalse(caught.exception.medical_capture['complete'])

    def test_cancellation_kills_owned_worker_and_preserves_exact_primary(self):
        marker = self.root / 'pid'
        class Cancellation(KeyboardInterrupt):
            def __setattr__(self, name, value): raise AttributeError('No attribute hook')
        primary = Cancellation('owned cancellation')
        closings = []
        def renew():
            if marker.exists():
                closings.append(True)
                raise primary
        with self.assertRaises(KeyboardInterrupt) as caught:
            self.run_child("os.write(fd,b'native.begin\\n')\n"
                "with open('pid','w') as f: f.write(str(os.getpid()))\n"
                "time.sleep(60)\n", renew=renew)
        self.assertIs(caught.exception, primary)
        self.assertEqual(len(closings), 2)
        self.assertEqual(primary.medical_capture['phase'], b'native.begin\n')
        pid = int(marker.read_text())
        # The retained group leader is reaped by capture. A killed child may
        # briefly remain a system-owned zombie, but cannot remain executing.
        for _ in range(50):
            try: os.kill(pid, 0)
            except ProcessLookupError: break
            time.sleep(.01)
        else: self.fail('Owned child survived cancellation')

    def test_final_closing_cannot_grant_success_past_whole_deadline(self):
        real_clock = time.monotonic
        marker = self.root / 'final'
        renewals = []; offset = [0]
        def renew():
            if marker.exists():
                renewals.append(True)
                if len(renewals) == 2: offset[0] = 541
        def clock(): return real_clock() + offset[0]
        with patch('ashlar_host.medical_worker_capture.time.monotonic', side_effect=clock), self.assertRaises(TimeoutError) as caught:
            self.run_child("os.write(fd,b'native.begin\\nnative.end\\n')\n"
                "with open('final','w') as f: f.write('done')\n", renew=renew)
        self.assertEqual(len(renewals), 2)
        self.assertFalse(caught.exception.medical_capture['complete'])

    def test_success_kills_descendant_that_outlives_worker(self):
        self.run_child("os.write(fd,b'native.begin\\n')\n"
            "pid=os.fork()\n"
            "if pid == 0:\n"
            " os.close(fd);os.close(1);os.close(2)\n"
            " with open('descendant','w') as f: f.write(str(os.getpid()))\n"
            " time.sleep(60);os._exit(0)\n"
            "while not os.path.exists('descendant'): time.sleep(.01)\n"
            "os.write(fd,b'native.end\\n')\n")
        pid = int((self.root / 'descendant').read_text())
        for _ in range(50):
            try: os.kill(pid, 0)
            except ProcessLookupError: break
            time.sleep(.01)
        else: self.fail('Owned descendant survived successful worker exit')

    def test_native_and_whole_deadlines_are_independent(self):
        real_clock = time.monotonic
        for phase,offset in ((True,301),(True,541),(False,541)):
            marker = self.root / 'clock'; marker.unlink(missing_ok=True)
            def clock(): return real_clock() + (offset if marker.exists() else 0)
            with self.subTest(phase=phase), patch(
                    'ashlar_host.medical_worker_capture.time.monotonic', side_effect=clock), self.assertRaises(TimeoutError)as caught:
                self.run_child(("os.write(fd,b'native.begin\\n');time.sleep(.1)\n" if phase else '')
                    + "with open('clock','w') as f: f.write('ready')\ntime.sleep(60)\n")
            self.assertEqual(caught.exception.medical_capture['failure']['cause'],'native-deadline'if phase else'whole-deadline')
            self.assertEqual(caught.exception.medical_capture['failure']['native_phase'],'begun'if phase else'ready')
