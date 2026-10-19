# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual CLI dispatch and protocol failures through an inert host port."""
import io
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from ashlar.cli import main


class Output:
    def __init__(self, buffer=None):
        self.buffer = io.BytesIO() if buffer is None else buffer


class TrussPreparationCliTests(unittest.TestCase):
    def invoke(self, operation, output=None, argv=None):
        module = types.ModuleType('ashlar_host.truss_preparation')
        module.run_truss_preparation_command = operation
        output = Output() if output is None else output
        stderr = io.StringIO()
        self.stderr = stderr
        arguments = ['ashlar', 'prepare-truss-catalog', '--config', '/private/tmp/input.json']
        with patch.dict(sys.modules, {'ashlar_host.truss_preparation': module}), \
                patch.object(sys, 'argv', arguments if argv is None else argv), \
                patch.object(sys, 'stdout', output), patch.object(sys, 'stderr', stderr):
            main()
        return output, stderr

    def test_explicit_host_dispatch_and_complete_unicode_summary(self):
        answer = {'outcome': 'prepared', 'document': '雪', 'accepted': False}
        operation = Mock(return_value=answer)
        output, stderr = self.invoke(operation)
        operation.assert_called_once_with(Path('/private/tmp/input.json'))
        payload = output.buffer.getvalue()
        self.assertTrue(payload.endswith(b'\n'))
        self.assertEqual(json.loads(payload), answer)
        self.assertEqual(stderr.getvalue(), '')

    def test_required_config_refuses_before_host(self):
        operation = Mock()
        with self.assertRaises(SystemExit) as caught:
            self.invoke(operation, argv=['ashlar', 'prepare-truss-catalog'])
        self.assertEqual(caught.exception.code, 2)
        operation.assert_not_called()

    def test_host_refusal_withholds_summary_and_raw_error(self):
        operation = Mock(side_effect=ValueError('private source and configuration'))
        output = Output()
        with self.assertRaises(SystemExit) as caught:
            self.invoke(operation, output)
        self.assertEqual(caught.exception.code, 2)
        self.assertEqual(output.buffer.getvalue(), b'')
        self.assertEqual(self.stderr.getvalue(), 'ashlar Truss preparation command failed\n')

    def test_short_write_and_flush_failure_are_command_failures(self):
        class ShortWrite(io.BytesIO):
            def write(self, value):
                return super().write(value[:-1])

        class BadFlush(io.BytesIO):
            def flush(self):
                raise OSError('private output failure')

        for stream in (ShortWrite(), BadFlush()):
            with self.subTest(stream=type(stream).__name__):
                operation = Mock(return_value={'outcome': 'prepared'})
                with self.assertRaises(SystemExit) as caught:
                    self.invoke(operation, Output(stream))
                self.assertEqual(caught.exception.code, 2)
                operation.assert_called_once()
                self.assertEqual(self.stderr.getvalue(), 'ashlar Truss preparation command failed\n')

    def test_nonfinite_summary_refuses_before_output(self):
        output = Output()
        with self.assertRaises(SystemExit) as caught:
            self.invoke(Mock(return_value={'invalid': float('nan')}), output)
        self.assertEqual(caught.exception.code, 2)
        self.assertEqual(output.buffer.getvalue(), b'')

    def test_primary_cancellation_identity_survives_dispatch(self):
        primary = KeyboardInterrupt()
        output = Output()
        with self.assertRaises(KeyboardInterrupt) as caught:
            self.invoke(Mock(side_effect=primary), output)
        self.assertIs(caught.exception, primary)
        self.assertEqual(output.buffer.getvalue(), b'')
        self.assertEqual(self.stderr.getvalue(), '')
