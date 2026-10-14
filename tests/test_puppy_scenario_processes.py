"""Inert owned-process failure diagnostics; no graph engine or source authority."""
import subprocess
import sys
import unittest
from unittest.mock import patch
from ashlar_host import puppy_scenario_processes as owner


class OwnedProcessDiagnosticsTest(unittest.TestCase):
    def test_inventory_failure_records_only_owned_identity_and_still_reaps(self):
        for primary_type, observer_type in ((OSError, None), (OSError, RuntimeError),
                (OSError, KeyboardInterrupt), (KeyboardInterrupt, RuntimeError),
                (KeyboardInterrupt, KeyboardInterrupt)):
            with self.subTest(primary_type=primary_type, observer_type=observer_type):
                process = subprocess.Popen([sys.executable, '-I', '-B', '-c',
                    'import time; time.sleep(30)'], stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    start_new_session=True)
                primary = primary_type('private inventory failure')
                observer_error = None if observer_type is None else observer_type('private observer failure')
                selected = observer_error if primary_type is OSError and observer_type is KeyboardInterrupt else primary
                observed = []
                def observe(record):
                    observed.append(dict(record))
                    if observer_error is not None:
                        raise observer_error
                try:
                    with patch.object(owner, 'capture', side_effect=primary):
                        with self.assertRaises(type(selected)) as raised:
                            owner.terminate_scenario_processes(process, failure_observer=observe)
                    self.assertIs(raised.exception, selected)
                    self.assertEqual(len(observed), 1)
                    self.assertEqual(observed[0], {
                        'session_leader_pid': process.pid,
                        'process_groups': (process.pid,), 'phase': 'inventory',
                        'error_class': primary_type.__name__, 'session_leader_reaped': True,
                        'remaining_groups': None})
                    self.assertIsNotNone(process.returncode)
                    if observer_error is not None:
                        self.assertTrue(selected.cleanup_failed)
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.wait(timeout=2)


if __name__ == '__main__':
    unittest.main()
