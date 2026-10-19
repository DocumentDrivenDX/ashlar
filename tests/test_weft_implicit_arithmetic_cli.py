# SPDX-License-Identifier: MIT OR Apache-2.0
import io
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from contextlib import ExitStack
from ashlar.cli import main
from ashlar import weft_implicit_arithmetic_distribution as d


class ImplicitArithmeticCliTests(unittest.TestCase):
    def invoke(self, args, raw=b'{}\n', compiler=None, installer=None):
        stdin = io.TextIOWrapper(io.BytesIO(raw))
        stdout = io.TextIOWrapper(io.BytesIO(), write_through=True)
        stderr = io.StringIO()
        self.addCleanup(stdin.close)
        self.addCleanup(stdout.close)
        with ExitStack() as stack:
            for name, value in [('argv', ['ashlar'] + args), ('stdin', stdin), ('stdout', stdout), ('stderr', stderr)]:
                stack.enter_context(patch('sys.' + name, value))
            if compiler is not None:
                stack.enter_context(patch.object(d, 'compile_implicit_arithmetic_distribution', **compiler))
            if installer is not None:
                stack.enter_context(patch.object(d, 'install_implicit_arithmetic_distribution', **installer))
            try:
                main()
                error = None
            except BaseException as caught:
                error = caught
        return stdout.buffer.getvalue(), stderr.getvalue(), error

    def test_install_exact_paths_and_status(self):
        def install(paths):
            self.assertEqual(paths, d.ImplicitArithmeticDistributionPaths(Path('/i'), Path('/o'), Path('/p')))
            return SimpleNamespace(cleanup_pending=False)
        self.assertEqual(self.invoke(['install-weft-implicit-arithmetic', '--index', '/i', '--package', '/p', '--output', '/o'], installer={'side_effect': install}), (b'', 'ashlar-weft-implicit-arithmetic: installed\n', None))

    def test_cleanup_pending_is_not_protocol_output(self):
        out, err, error = self.invoke(['install-weft-implicit-arithmetic', '--index', '/i', '--package', '/p', '--output', '/o'], installer={'return_value': SimpleNamespace(cleanup_pending=True)})
        self.assertEqual((out, err, error), (b'', 'ashlar-weft-implicit-arithmetic: cleanup-pending\n', None))

    def test_compile_preserves_original_bytes_and_blocked_response(self):
        raw = b'{"original":"SELECT unchanged"}\n'
        response = b'{"status":"blocked"}\n'
        def compile(paths, request):
            self.assertEqual(request, raw)
            self.assertEqual(paths, d.ImplicitArithmeticDistributionPaths(Path('/i'), Path('/o')))
            return response
        self.assertEqual(self.invoke(['compile-weft-implicit-arithmetic', '--index', '/i', '--installation', '/o'], raw, compiler={'side_effect': compile}), (response, '', None))

    def test_oversize_refuses_before_compiler(self):
        out, err, error = self.invoke(['compile-weft-implicit-arithmetic', '--index', '/i', '--installation', '/o'], b'x' * (d.REQUEST_LIMIT + 1), compiler={'side_effect': AssertionError('effect')})
        self.assertEqual(out, b'')
        self.assertEqual(error.code, 2)
        self.assertEqual(err, 'ashlar-weft-implicit-arithmetic: refused\n')

    def test_refusal_redacts_and_cancellation_preserves_identity(self):
        args = ['compile-weft-implicit-arithmetic', '--index', '/i', '--installation', '/o']
        for failure in (OSError('private detail'), d.ImplicitArithmeticDistributionError('private detail'), KeyboardInterrupt('original')):
            with self.subTest(failure=type(failure).__name__):
                out, err, error = self.invoke(args, compiler={'side_effect': failure})
                self.assertEqual(out, b'')
                if isinstance(failure, KeyboardInterrupt):
                    self.assertIs(error, failure)
                    self.assertEqual(err, '')
                else:
                    self.assertEqual(error.code, 2)
                    self.assertEqual(err, 'ashlar-weft-implicit-arithmetic: refused\n')

    def test_profile_override_is_not_accepted(self):
        out, err, error = self.invoke(['compile-weft-implicit-arithmetic', '--index', '/i', '--installation', '/o', '--profile', 'count-star'], compiler={'side_effect': AssertionError('effect')})
        self.assertEqual(error.code, 2)
        self.assertIn('unrecognized arguments', err)
        self.assertEqual(out, b'')

    def test_install_refusal_and_cancellation(self):
        args = ['install-weft-implicit-arithmetic', '--index', '/i', '--package', '/p', '--output', '/o']
        for failure in (OSError('private'), SystemExit('cancel')):
            out, err, error = self.invoke(args, installer={'side_effect': failure})
            self.assertEqual(out, b'')
            if isinstance(failure, SystemExit):
                self.assertIs(error, failure)
                self.assertEqual(err, '')
            else:
                self.assertEqual(error.code, 2)
                self.assertEqual(err, 'ashlar-weft-implicit-arithmetic: refused\n')

    def test_output_write_and_flush_refusals(self):
        args = ['ashlar', 'compile-weft-implicit-arithmetic', '--index', '/i', '--installation', '/o']
        for written, failure in ((0, None), (True, None), (2, OSError('private flush'))):
            buffer = SimpleNamespace(write=lambda raw: written, flush=lambda: None)
            if failure is not None:
                def flush(): raise failure
                buffer.flush = flush
            stderr = io.StringIO()
            with patch('sys.argv', args), patch('sys.stdin', SimpleNamespace(buffer=io.BytesIO(b'{}'))), patch('sys.stdout', SimpleNamespace(buffer=buffer)), patch('sys.stderr', stderr), patch.object(d, 'compile_implicit_arithmetic_distribution', return_value=b'{}'):
                with self.assertRaises(SystemExit) as error: main()
            self.assertEqual(error.exception.code, 2)
            self.assertEqual(stderr.getvalue(), 'ashlar-weft-implicit-arithmetic: refused\n')
