"""Effect-free checks for retained bounded native failure diagnostics."""
import importlib.util
from pathlib import Path
import unittest

SCRIPT = Path(__file__).with_name('prepare.py')
SPEC = importlib.util.spec_from_file_location('g03_native_prepare', SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

class BoundedFailureRecordTests(unittest.TestCase):
    def test_module_name_and_traceback_are_retained(self):
        try:
            raise ModuleNotFoundError("No module named 'ashlar_worker_probe'")
        except ModuleNotFoundError as error:
            kind, message, trace, truncated = MODULE.bounded_exception_details(error)
        self.assertEqual(kind, 'ModuleNotFoundError')
        self.assertIn('ashlar_worker_probe', message)
        self.assertIn('ModuleNotFoundError', trace)
        self.assertFalse(truncated)

    def test_traceback_is_bounded(self):
        try:
            raise RuntimeError('x' * 20000)
        except RuntimeError as error:
            _, _, trace, truncated = MODULE.bounded_exception_details(error)
        self.assertTrue(truncated)
        self.assertLessEqual(len(trace.encode('utf-8')), 16384)

if __name__ == '__main__':
    unittest.main()
