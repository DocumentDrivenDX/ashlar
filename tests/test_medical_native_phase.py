# SPDX-License-Identifier: MIT OR Apache-2.0
"""Real pipe and selected runtime custody controls; no native execution."""
from contextlib import ExitStack
import os
import unittest
from unittest.mock import patch

from ashlar_host.medical_process_supervision import OriginalMedicalNativePhase
import test_original_medical_runtime as runtime_fixture


class Tests(unittest.TestCase):
    def fixture(self, stack):
        fixture = runtime_fixture.Tests()
        fixture.setUp()
        stack.callback(fixture.doCleanups)
        owner = stack.enter_context(fixture.opened())
        read, write = os.pipe()
        stack.callback(os.close, read)
        stack.callback(os.close, write)
        os.set_blocking(write, False)
        return owner, read, write

    def test_exact_signals_once_and_borrowed_descriptor_remains_open(self):
        with ExitStack() as stack:
            owner, read, write = self.fixture(stack)
            phase = OriginalMedicalNativePhase(write)
            phase.begin(owner)
            phase.end(owner)
            self.assertEqual(os.read(read, 32), b'native.begin\nnative.end\n')
            os.fstat(write)
            for operation in (phase.begin, phase.end):
                with self.assertRaises(ValueError):
                    operation(owner)

    def test_wrong_pipe_custody_and_order_refuse(self):
        with ExitStack() as stack:
            owner, read, write = self.fixture(stack)
            with self.assertRaises(PermissionError):
                OriginalMedicalNativePhase(read)
            os.set_blocking(write, True)
            with self.assertRaises(PermissionError):
                OriginalMedicalNativePhase(write)
            os.set_blocking(write, False)
            phase = OriginalMedicalNativePhase(write)
            with self.assertRaises(ValueError):
                phase.end(owner)
            with self.assertRaises(ValueError):
                phase.begin(object())
            phase.begin(owner)
            with self.assertRaises(ValueError):
                phase.end(object())
            phase.end(owner)

    def test_descriptor_substitution_and_uncertain_send_refuse(self):
        with ExitStack() as stack:
            owner, read, write = self.fixture(stack)
            phase = OriginalMedicalNativePhase(write)
            phase.begin(owner)
            other_read, other_write = os.pipe()
            stack.callback(os.close, other_read)
            stack.callback(os.close, other_write)
            os.set_blocking(other_write, False)
            os.dup2(other_write, write)
            with self.assertRaises(PermissionError):
                phase.end(owner)
        with ExitStack() as stack:
            owner, read, write = self.fixture(stack)
            phase = OriginalMedicalNativePhase(write)
            with patch('ashlar_host.medical_process_supervision.os.write', return_value=1):
                with self.assertRaises(OSError):
                    phase.begin(owner)
            with self.assertRaises(ValueError):
                phase.begin(owner)
