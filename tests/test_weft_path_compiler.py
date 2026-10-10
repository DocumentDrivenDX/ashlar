from pathlib import Path
import json
import os
import selectors
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from weft_path_compiler import PathCompilerConfig, PathCompilerError, compile_path_request


class Compiler(unittest.TestCase):
    def config(self, **changes):
        settings = dict(binary=Path(sys.executable).absolute(), maximum_request_bytes=1024,
                        maximum_response_bytes=1024, timeout_seconds=2)
        settings.update(changes)
        return PathCompilerConfig(**settings)

    def test_binary_custody_precedes_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / 'wrong'
            binary.write_bytes(b'not the compiler')
            with patch('weft_path_compiler.subprocess.Popen') as spawn:
                with self.assertRaises(PathCompilerError):
                    compile_path_request(b'{}', config=self.config(binary=binary))
                spawn.assert_not_called()

    def test_configuration_and_early_input_limits(self):
        for settings in [dict(maximum_request_bytes=True), dict(maximum_response_bytes=0),
                         dict(timeout_seconds=61), dict(binary=Path('relative'))]:
            with self.assertRaises(PathCompilerError): self.config(**settings)
        with patch('weft_path_compiler.subprocess.Popen') as spawn:
            with self.assertRaises(PathCompilerError):
                compile_path_request(b'x'*1025, config=self.config())
            spawn.assert_not_called()

    def test_actual_process_transport_without_claiming_compiler_identity(self):
        # Only the identity port is mocked. Real child processes test capture,
        # deadline and cleanup; they are not admitted compiler artifacts.
        with patch('weft_path_compiler._identity', return_value=(1, 2, 3, 4)):
            self.assertEqual(compile_path_request(b'print("{}")', config=self.config()), b'{}\n')
            for request in [b'print("x"*2048)', b'import sys; print("private",file=sys.stderr)',
                            b'print("one"); print("two")', b'import time; time.sleep(10)']:
                with self.assertRaises(PathCompilerError) as error:
                    compile_path_request(request, config=self.config(timeout_seconds=1))
                self.assertNotIn('private', str(error.exception))

    def test_closing_identity_drift_refuses_complete_output(self):
        with patch('weft_path_compiler._identity', side_effect=[(1,2,3,4), (1,5,3,4)]):
            with self.assertRaises(PathCompilerError):
                compile_path_request(b'print("{}")', config=self.config())

    def test_input_cleanup_does_not_replace_cancellation(self):
        class Input:
            def write(self, request): raise KeyboardInterrupt()
            def close(self): raise OSError('private cleanup payload')
        with patch('weft_path_compiler._identity', return_value=(1,2,3,4)), \
             patch('tempfile.TemporaryFile', return_value=Input()), \
             patch('weft_path_compiler.subprocess.Popen') as spawn:
            with self.assertRaises(KeyboardInterrupt):
                compile_path_request(b'{}', config=self.config())
            spawn.assert_not_called()

    def test_input_cleanup_failure_refuses_successful_output(self):
        stream = tempfile.TemporaryFile()
        class Input:
            def write(self, value): return stream.write(value)
            def seek(self, position): return stream.seek(position)
            def fileno(self): return stream.fileno()
            def close(self):
                stream.close()
                raise OSError('private cleanup payload')
        with patch('weft_path_compiler._identity', return_value=(1,2,3,4)), \
             patch('tempfile.TemporaryFile', return_value=Input()):
            with self.assertRaises(PathCompilerError) as error:
                compile_path_request(b'print("{}")', config=self.config())
            self.assertEqual(str(error.exception), 'Compiler cleanup refused')
            self.assertTrue(error.exception.cleanup_failed)

    def test_inherited_pipe_deadline_and_owned_group_cleanup(self):
        # A real direct child exits while its child keeps both output pipes.
        # No compiler identity or engine behavior is claimed by this process probe.
        actual_spawn, actual_killpg = subprocess.Popen, os.killpg
        spawned = []
        def spawn(*args, **kwargs):
            child = actual_spawn(*args, **kwargs)
            spawned.append(child)
            return child
        with tempfile.TemporaryDirectory() as directory:
            pid_file = Path(directory) / 'pipe-holder.json'
            code = ('import json,os,subprocess,sys\nfrom pathlib import Path\n'
                    'p=subprocess.Popen([sys.executable,"-c","import time; time.sleep(8)"])\n'
                    'Path(' + repr(str(pid_file)) + ').write_text(json.dumps([p.pid,os.getpgrp()]))\n'
                    'print("{}")\n').encode()
            try:
                with patch('weft_path_compiler._identity', return_value=(1,2,3,4)), \
                     patch('weft_path_compiler.subprocess.Popen', side_effect=spawn), \
                     patch('weft_path_compiler.os.killpg', wraps=actual_killpg) as kill_group:
                    start = time.monotonic()
                    with self.assertRaises(PathCompilerError) as error:
                        compile_path_request(code, config=self.config(timeout_seconds=1))
                    self.assertLess(time.monotonic() - start, 3)
                    self.assertEqual(str(error.exception), 'Compiler execution refused')
                    self.assertFalse(error.exception.cleanup_failed)
                    descendant, group = json.loads(pid_file.read_text())
                    self.assertEqual(group, spawned[0].pid)
                    kill_group.assert_called_once_with(group, signal.SIGKILL)
                    self.assertIsNotNone(spawned[0].poll())
                    self.assertTrue(spawned[0].stdout.closed and spawned[0].stderr.closed)
            finally:
                # Clean up even if a future regression fails before the helper's
                # group cleanup. Never signal an unrelated process group.
                for child in spawned:
                    try: actual_killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError: pass
                    child.wait(timeout=3)

    def test_utf8_output_byte_sentinel_preserves_exact_response(self):
        code = b'import sys; sys.stdout.buffer.write(b"\\xc3\\xa9\\n")'
        with patch('weft_path_compiler._identity', return_value=(1,2,3,4)):
            self.assertEqual(compile_path_request(code,
                config=self.config(maximum_response_bytes=3)), b'\xc3\xa9\n')
            for program, maximum in [(code, 2),
                    (b'import sys; sys.stdout.buffer.write(b"\\xff\\n")', 3),
                    (b'import sys; sys.stderr.buffer.write(b"x"*5000)', 1024)]:
                with self.assertRaises(PathCompilerError):
                    compile_path_request(program, config=self.config(maximum_response_bytes=maximum))

    def test_cancellation_during_pipe_wait_reaps_child_and_closes_descriptors(self):
        actual_spawn = subprocess.Popen
        selector = selectors.DefaultSelector()
        spawned = []
        cancellation = KeyboardInterrupt('controlled cancellation')
        def spawn(*args, **kwargs):
            child = actual_spawn(*args, **kwargs)
            spawned.append(child)
            return child
        with patch('weft_path_compiler._identity', return_value=(1,2,3,4)), \
             patch('weft_path_compiler.subprocess.Popen', side_effect=spawn), \
             patch('weft_path_compiler.selectors.DefaultSelector', return_value=selector), \
             patch.object(selector, 'select', side_effect=cancellation):
            with self.assertRaises(KeyboardInterrupt) as error:
                compile_path_request(b'import time; time.sleep(8)', config=self.config())
            self.assertIs(error.exception, cancellation)
        self.assertIsNotNone(spawned[0].poll())
        self.assertTrue(spawned[0].stdout.closed and spawned[0].stderr.closed)


if __name__ == '__main__': unittest.main()
