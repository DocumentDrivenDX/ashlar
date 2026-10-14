"""Actual local POSIX lock custody only; no Spark or publication authority."""
import os
from pathlib import Path
import subprocess
import select
import sys
import tempfile
import unittest
import ashlar_host.delta_custody as owner
from ashlar_host.delta_custody import LocalDeltaTransport, LocalDeltaError


class Tests(unittest.TestCase):
    def test_real_competing_process_refuses_then_releases(self):
        with tempfile.TemporaryDirectory() as directory:
            transport = object.__new__(LocalDeltaTransport)
            transport.journal_path = Path(directory)/'journal.sqlite'
            path = str(transport.journal_path)+'.lock'
            code = "import fcntl,sys; f=open(sys.argv[1],'a'); fcntl.flock(f,fcntl.LOCK_EX); print('held',flush=True); sys.stdin.read(1); f.close()"
            holder = subprocess.Popen([sys.executable,'-c',code,path], stdin=subprocess.PIPE,
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                self.assertTrue(select.select([holder.stdout],[],[],5)[0], 'Lock holder readiness deadline')
                self.assertEqual(holder.stdout.readline().strip(),'held')
                contender = """import sys
from pathlib import Path
sys.path.insert(0,sys.argv[2])
from ashlar_host.delta_custody import LocalDeltaTransport,LocalDeltaError
t=object.__new__(LocalDeltaTransport);t.journal_path=Path(sys.argv[1])
try:
    with t._lock():raise AssertionError('Contended journal must not yield')
except LocalDeltaError as error:
    assert 'already held' in str(error)
    print('refused')
"""
                refusal = subprocess.run([sys.executable,'-c',contender,str(transport.journal_path),
                    str(Path(owner.__file__).resolve().parents[1])],capture_output=True,text=True,timeout=5)
                self.assertEqual(refusal.returncode,0,refusal.stderr)
                self.assertEqual(refusal.stdout.strip(),'refused')
                holder.stdin.write('x');holder.stdin.flush()
                holder.communicate(timeout=5)
                self.assertEqual(holder.returncode,0)
                primary = RuntimeError('original action')
                with self.assertRaises(RuntimeError) as caught:
                    with transport._lock():
                        raise primary
                self.assertIs(caught.exception,primary)
                with transport._lock():
                    self.assertTrue(os.path.isfile(path))
            finally:
                if holder.poll() is None:
                    holder.kill();holder.communicate(timeout=5)
