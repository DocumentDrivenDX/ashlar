# SPDX-License-Identifier: MIT OR Apache-2.0
import hashlib,os,subprocess,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from contextlib import ExitStack
from ashlar_host.weft_compatible_cli import CompatibleCompiler,compile_compatible,CompatibleCompilerError

class Tests(unittest.TestCase):
    def platform(self,stack,raw):
        stack.enter_context(patch('ashlar_host.weft_compatible_cli.sys.platform','darwin'));stack.enter_context(patch('ashlar_host.weft_compatible_cli.platform.machine',return_value='arm64'));stack.enter_context(patch('ashlar_host.weft_compatible_cli.platform.mac_ver',return_value=('27.0.1','','')))
        stack.enter_context(patch('ashlar_host.weft_compatible_cli.BINARY_BYTES',len(raw)));stack.enter_context(patch('ashlar_host.weft_compatible_cli.BINARY_SHA256',hashlib.sha256(raw).hexdigest()))
    def test_private_pinned_execution_original_response_and_cleanup(self):
        with tempfile.TemporaryDirectory()as directory,ExitStack()as stack:
            root=Path(directory).resolve();original=root/'original';raw=b'reviewed test realization';original.write_bytes(raw);self.platform(stack,raw)
            response=b'{"status":"compiled"}\n';request=b'{"original":"request"}\n';seen=[]
            class Process:
                def __init__(self,args,**kwargs):
                    selected=Path(args[0]);seen.append(selected);self_test.assertNotEqual(selected,original);self_test.assertEqual(selected.read_bytes(),raw);self_test.assertEqual(selected.stat().st_mode&0o777,0o700);self_test.assertEqual(selected.parent.stat().st_mode&0o777,0o700)
                    ri,wi=os.pipe();self.input_reader=os.fdopen(ri,'rb');self.stdin=os.fdopen(wi,'wb');ro,wo=os.pipe();os.write(wo,response);os.close(wo);self.stdout=os.fdopen(ro,'rb');re,we=os.pipe();os.close(we);self.stderr=os.fdopen(re,'rb')
                def wait(self,timeout):self_test.assertEqual(self.input_reader.read(),request);self.input_reader.close();return 0
                def poll(self):return 0
            self_test=self
            with patch('ashlar_host.weft_compatible_cli.subprocess.Popen',side_effect=Process):self.assertEqual(compile_compatible(CompatibleCompiler(original),request),response)
            self.assertFalse(seen[0].parent.exists());self.assertEqual(original.read_bytes(),raw)
    def test_bad_realization_before_process_and_timeout_reaped(self):
        with tempfile.TemporaryDirectory()as directory,ExitStack()as stack:
            root=Path(directory).resolve();binary=root/'original';raw=b'reviewed test realization';binary.write_bytes(b'x'*len(raw));self.platform(stack,raw)
            with patch('ashlar_host.weft_compatible_cli.subprocess.Popen')as process:
                with self.assertRaises(CompatibleCompilerError):compile_compatible(CompatibleCompiler(binary),b'{}')
                process.assert_not_called()
            binary.write_bytes(raw);processes=[]
            class Process:
                def __init__(self,args,**kwargs):
                    self.killed=False;self.selected=Path(args[0]);processes.append(self)
                    ri,wi=os.pipe();self.reader=os.fdopen(ri,'rb');self.stdin=os.fdopen(wi,'wb');ro,wo=os.pipe();os.write(wo,b'{"status":"compiled"}\n');os.close(wo);self.stdout=os.fdopen(ro,'rb');re,we=os.pipe();os.close(we);self.stderr=os.fdopen(re,'rb')
                def wait(self,timeout):
                    if not self.killed:raise subprocess.TimeoutExpired('payload-free-test',timeout)
                    self.reader.close();return -9
                def poll(self):return -9 if self.killed else None
                def kill(self):self.killed=True
            with patch('ashlar_host.weft_compatible_cli.subprocess.Popen',side_effect=Process):
                with self.assertRaises(CompatibleCompilerError):compile_compatible(CompatibleCompiler(binary),b'{}')
            process=processes[0];self.assertTrue(process.killed);self.assertTrue(all(stream.closed for stream in (process.stdin,process.stdout,process.stderr)));self.assertFalse(process.selected.parent.exists())
