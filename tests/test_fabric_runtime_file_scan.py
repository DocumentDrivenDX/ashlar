# SPDX-License-Identifier: MIT OR Apache-2.0
import hashlib
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from ashlar_host.fabric_runtime_file_scan import file_facts,scan_runtime_files,_read_file


class Controls(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory();self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name).resolve();self.interpreter=Path(sys.executable).resolve()
        self.paths=[]
        for n,data in enumerate((b'',b'abc',b'x'*140000,b'\x00\xff',b'five')):
            p=self.root/str(n);p.write_bytes(data);self.paths.append(p)
        self.tasks=tuple((p,file_facts(p.lstat()))for p in self.paths)
    def scan(self,tasks=None,deadline=None):
        return scan_runtime_files(self.tasks if tasks is None else tasks,interpreter=self.interpreter,
                                  deadline=time.monotonic()+5 if deadline is None else deadline,maximum_bytes=1048576)
    def capture_spawn(self,script=None,pass_fds=()):
        original=subprocess.Popen;owned=[]
        def spawn(argv,**kwargs):
            if script is not None:argv=(str(self.interpreter),'-I','-S','-B','-c',script)
            if pass_fds:kwargs['pass_fds']=pass_fds
            process=original(argv,**kwargs);owned.append(process);return process
        return owned,spawn
    def closed(self,owned):
        self.assertEqual(len(owned),4)
        for p in owned:
            self.assertIsNotNone(p.returncode)
            self.assertTrue(all(pipe.closed for pipe in (p.stdin,p.stdout,p.stderr)))
    def test_full_bytes_order_and_four_leaders_reaped(self):
        owned,spawn=self.capture_spawn()
        with patch('ashlar_host.fabric_runtime_file_scan.subprocess.Popen',side_effect=spawn):rows=self.scan()
        self.assertEqual(rows,tuple(('file',str(p),facts,hashlib.sha256(p.read_bytes()).hexdigest())for p,facts in self.tasks))
        self.closed(owned)
    def test_fifo_replacement_between_named_check_and_open_refuses(self):
        path=self.paths[1];expected=file_facts(path.lstat());original=os.open;opened=[]
        def replace_then_open(selected,flags,*args,**kwargs):
            if Path(selected)==path:
                self.assertTrue(flags & os.O_NONBLOCK)
                path.unlink();os.mkfifo(path)
            fd=original(selected,flags,*args,**kwargs);opened.append(fd);return fd
        with patch('ashlar_host.fabric_runtime_file_scan.os.open',side_effect=replace_then_open):
            with self.assertRaises(PermissionError):
                _read_file(path,expected,time.monotonic()+1)
        self.assertEqual(len(opened),1)
        with self.assertRaises(OSError):os.fstat(opened[0])
    def test_changed_bytes_identity_and_links_refuse(self):
        self.paths[1].write_bytes(b'changed')
        with self.assertRaises((PermissionError,RuntimeError)):self.scan()
        target=self.root/'alias';target.symlink_to(self.paths[0])
        with self.assertRaises((PermissionError,RuntimeError)):
            self.scan(((target,file_facts(target.lstat())),))
    def test_expired_original_deadline_prevents_spawn(self):
        with patch('ashlar_host.fabric_runtime_file_scan.subprocess.Popen')as spawn:
            with self.assertRaises(TimeoutError):self.scan(deadline=time.monotonic()-1)
            spawn.assert_not_called()
    def test_blocked_workers_and_inherited_descendant_pipe_closed(self):
        read_fd,write_fd=os.pipe();self.addCleanup(os.close,read_fd)
        code="import os,time\npid=os.fork()\nif pid==0:\n time.sleep(30)\nelse:\n time.sleep(30)\n"
        owned,spawn=self.capture_spawn(code,(write_fd,))
        try:
            with patch('ashlar_host.fabric_runtime_file_scan.subprocess.Popen',side_effect=spawn):
                with self.assertRaises(TimeoutError):self.scan(deadline=time.monotonic()+.4)
        finally:os.close(write_fd)
        self.closed(owned)
        ready,_,_=select.select([read_fd],[],[],1)
        self.assertTrue(ready);self.assertEqual(os.read(read_fd,1),b'')
        # EOF proves no owned worker/descendant retains the inherited writer;
        # leaders are reaped, no claim of waitpid authority for nonchild zombies.
    def test_cancellation_closes_all_actual_leaders(self):
        owned,spawn=self.capture_spawn('import time;time.sleep(30)')
        with patch('ashlar_host.fabric_runtime_file_scan.subprocess.Popen',side_effect=spawn),patch('selectors.KqueueSelector.select'if sys.platform=='darwin'else'selectors.EpollSelector.select',side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):self.scan()
        self.closed(owned)
    def test_foreign_reply_refuses_and_cleanup_preserves_primary(self):
        code="import sys,time;sys.stdout.write('{\"ordinal\":999,\"facts\":[],\"count\":0,\"sha256\":\"x\"}\\n');sys.stdout.flush();time.sleep(30)"
        owned,spawn=self.capture_spawn(code)
        with patch('ashlar_host.fabric_runtime_file_scan.subprocess.Popen',side_effect=spawn):
            with self.assertRaises(PermissionError):self.scan()
        self.closed(owned)
    def test_setup_consumed_original_deadline_forwards_no_tasks(self):
        owned,spawn=self.capture_spawn('import time;time.sleep(30)')
        def delayed(*args,**kwargs):
            process=spawn(*args,**kwargs);time.sleep(.12);return process
        with patch('ashlar_host.fabric_runtime_file_scan.subprocess.Popen',side_effect=delayed),patch('ashlar_host.fabric_runtime_file_scan.os.write')as write:
            with self.assertRaises(TimeoutError):self.scan(deadline=time.monotonic()+.06)
            write.assert_not_called()
        self.assertEqual(len(owned),1)
        self.assertIsNotNone(owned[0].returncode)
        self.assertTrue(all(p.closed for p in (owned[0].stdin,owned[0].stdout,owned[0].stderr)))
    def test_worker_primary_survives_independent_cleanup_failure(self):
        owned,spawn=self.capture_spawn("import sys,time;sys.stderr.write('private inert failure');sys.stderr.flush();time.sleep(30)")
        def failing_wait(*args,**kwargs):
            process=spawn(*args,**kwargs);original=process.wait
            def wait(*args,**kwargs):
                original(*args,**kwargs);raise OSError('inert cleanup report failure after actual reap')
            process.wait=wait;return process
        with patch('ashlar_host.fabric_runtime_file_scan.subprocess.Popen',side_effect=failing_wait):
            with self.assertRaisesRegex(RuntimeError,'scanner failure')as raised:self.scan()
        self.assertTrue(raised.exception.cleanup_failed);self.closed(owned)
    def test_selected_capacity_and_duplicate_tasks_refuse_without_spawn(self):
        with patch('ashlar_host.fabric_runtime_file_scan.subprocess.Popen')as spawn:
            with self.assertRaises(ValueError):self.scan((self.tasks[0],self.tasks[0]))
            with self.assertRaises(PermissionError):
                scan_runtime_files(self.tasks,interpreter=self.interpreter,deadline=time.monotonic()+1,maximum_bytes=1)
            spawn.assert_not_called()

class RuntimeIntegration(unittest.TestCase):
    def setUp(self):
        from ashlar_host import fabric_cli_runtime as runtime
        self.runtime=runtime
        self.temporary=tempfile.TemporaryDirectory();self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name).resolve();(self.root/'content').write_bytes(b'complete bytes')
        self.owner=runtime.FabricRuntimeCustody.__new__(runtime.FabricRuntimeCustody)
        self.owner.roots=(self.root,);self.owner.extra=();self.owner.links=();self.owner.directory_pins=()
        self.owner.maximum_files=128;self.owner.maximum_bytes=1048576;self.owner.scan_seconds=5;self.owner.deadline=None
        self.owner.selection=runtime.FabricHttpRuntimeSelection('stdlib-python-3.14.8-fabric-http',128,1048576,5,runtime.CA_BUNDLE)
        self.owner.configuration=None;self.owner.host_identity=None
    def test_parent_full_rows_and_postnamespace_checks(self):
        with patch.object(self.runtime,'HTTP_PYTHON',Path(sys.executable).resolve()),patch.object(self.runtime,'NATIVE_PINS',()):
            rows=self.owner._scan()
        files=[row for row in rows if row[0]=='file']
        self.assertEqual(files[0][-1],hashlib.sha256(b'complete bytes').hexdigest())
        original=scan_runtime_files
        def changed(*args,**kwargs):
            value=original(*args,**kwargs);(self.root/'foreign').write_bytes(b'new');return value
        with patch.object(self.runtime,'HTTP_PYTHON',Path(sys.executable).resolve()),patch.object(self.runtime,'NATIVE_PINS',()),patch.object(self.runtime,'scan_runtime_files',side_effect=changed):
            with self.assertRaisesRegex(PermissionError,'namespace changed'):self.owner._scan()
    def test_original_worker_bytes_gate_before_any_worker_launch(self):
        from types import SimpleNamespace
        worker=Path(self.runtime.__file__).with_name('fabric_runtime_file_scan.py').resolve()
        original=SimpleNamespace(roots=self.owner.roots,extra=(worker,),links=(),directories=(),
            limits=(128,1048576,5,None),kind=self.runtime.FabricHttpRuntimeSelection,configuration=None,
            extension_directory=None,host_identity=None,
            original=(('file',str(worker),file_facts(worker.lstat()),'0'*64),))
        with patch.object(self.runtime,'NATIVE_PINS',()),patch.object(self.runtime,'scan_runtime_files')as scan:
            with self.assertRaisesRegex(PermissionError,'resource bytes changed'):self.owner._scan(original)
            scan.assert_not_called()


if __name__=='__main__':unittest.main()
