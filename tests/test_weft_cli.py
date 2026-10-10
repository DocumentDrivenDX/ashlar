import io
import unittest
from unittest.mock import patch
from ashlar.cli import main
from ashlar import weft_distribution as d


class WeftCliTests(unittest.TestCase):
    def invoke(self, args, raw=b'{}\n'):
        stdin = io.TextIOWrapper(io.BytesIO(raw))
        stdout = io.TextIOWrapper(io.BytesIO(), write_through=True)
        stderr = io.StringIO()
        return stdin, stdout, stderr, patch('sys.argv', ['ashlar'] + args)

    def test_original_compile_stdout_and_package_free_reopen(self):
        raw = b'{"source":"original SQL"}\n'
        response = b'{"status":"blocked","original":true}\n'
        stdin, stdout, stderr, argv = self.invoke(
            ['compile-weft', '--index', 'i', '--installation', 'o'], raw)
        with argv, patch('sys.stdin', stdin), patch('sys.stdout', stdout), patch('sys.stderr', stderr), patch.object(d, 'compile_distribution', return_value=response) as compile:
            main()
        self.assertEqual(stdout.buffer.getvalue(), response)
        self.assertEqual(stderr.getvalue(), '')
        self.assertEqual(compile.call_args.args[1], raw)
        self.assertIsNone(compile.call_args.args[0].package)

    def test_install_outcome_stays_off_protocol_stdout(self):
        class Installed: cleanup_pending = True
        stdin, stdout, stderr, argv = self.invoke(
            ['install-weft', '--index', 'i', '--package', 'p', '--output', 'o'])
        with argv, patch('sys.stdout', stdout), patch('sys.stderr', stderr), patch.object(d, 'install_distribution', return_value=Installed()):
            main()
        self.assertEqual(stdout.buffer.getvalue(), b'')
        self.assertEqual(stderr.getvalue(), 'ashlar-weft: cleanup-pending\n')

    def test_oversize_stdin_refuses_before_opening_or_output(self):
        stdin, stdout, stderr, argv = self.invoke(
            ['compile-weft', '--index', 'i', '--installation', 'o'], b'x' * (d.REQUEST_LIMIT + 1))
        with argv, patch('sys.stdin', stdin), patch('sys.stdout', stdout), patch('sys.stderr', stderr), patch.object(d, 'compile_distribution', side_effect=AssertionError('opened')):
            with self.assertRaises(SystemExit) as error: main()
        self.assertEqual(error.exception.code, 2)
        self.assertEqual(stdout.buffer.getvalue(), b'')
        self.assertEqual(stderr.getvalue(), 'ashlar-weft: request-too-large\n')

    def test_io_error_redacts_paths_and_contents(self):
        stdin, stdout, stderr, argv = self.invoke(
            ['install-weft', '--index', 'private-index', '--package', 'private-package', '--output', 'private-output'])
        with argv, patch('sys.stdout', stdout), patch('sys.stderr', stderr), patch.object(d, 'install_distribution', side_effect=OSError('secret /raw/path')):
            with self.assertRaises(SystemExit) as error: main()
        self.assertEqual(error.exception.code, 2)
        self.assertEqual(stdout.buffer.getvalue(), b'')
        self.assertEqual(stderr.getvalue(), 'ashlar-weft: io-refused\n')
