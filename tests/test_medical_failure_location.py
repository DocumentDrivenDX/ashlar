import json
from types import SimpleNamespace
import unittest
from ashlar_host.lifecycle import finish
from ashlar_host.medical_failure_location import medical_failure_location, retain_medical_failure_location


class Tests(unittest.TestCase):
    def test_owned_location_only_no_exception_payload(self):
        scope = {'__name__': 'ashlar_host.safe_owner'}
        exec(compile('def refuse():\n raise ValueError("credential secret SQL /private/path")\n', '/private/secret.py', 'exec'), scope)
        try: scope['refuse']()
        except ValueError as error: raw = medical_failure_location(error)
        value = json.loads(raw)
        self.assertEqual(value['exception'], 'ValueError')
        self.assertEqual(value['frames'], [{'module': 'ashlar_host.safe_owner', 'function': 'refuse', 'line': 2}])
        self.assertNotIn(b'secret', raw)
        self.assertNotIn(b'private', raw)
        self.assertLessEqual(len(raw), 8192)

    def test_bounded_frames_and_builtin_exception_category(self):
        scope = {'__name__': 'ashlar_host.' + 'a' * 127}
        exec('def recur(n):\n if n: return recur(n-1)\n raise ValueError("never render")\n', scope)
        try: scope['recur'](100)
        except ValueError as error: raw = medical_failure_location(error)
        self.assertLessEqual(len(raw), 8192)
        self.assertLessEqual(len(json.loads(raw)['frames']), 64)

    def test_not_acquired_has_no_write(self):
        output = SimpleNamespace(fd=None, write=lambda *args: self.fail('unacquired write'))
        retain_medical_failure_location(output, ValueError('private'))

    def test_writer_failure_preserves_primary_and_closing(self):
        error = ValueError('original')
        def refuse(*args): raise KeyboardInterrupt()
        output = SimpleNamespace(fd=5, write=refuse)
        closed = []
        with self.assertRaises(ValueError) as raised:
            finish(error, [lambda: retain_medical_failure_location(output, error), lambda: closed.append(True)])
        self.assertIs(raised.exception, error)
        self.assertEqual(closed, [True])
        self.assertTrue(error.cleanup_failed)

    def test_fresh_private_output_callback(self):
        writes = []
        output = SimpleNamespace(fd=5, write=lambda *args: writes.append(args))
        retain_medical_failure_location(output, SystemExit(2))
        self.assertEqual(writes[0][0], 'failure-location.json')
        self.assertEqual(writes[0][2], 8192)
        self.assertEqual(json.loads(writes[0][1])['exception'], 'SystemExit')
