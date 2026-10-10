"""Owned lifecycle exception identity and cleanup/cancellation ordering."""
from contextlib import contextmanager
import unittest
from ashlar_host.lifecycle import finish, owned_context, owned_connection


class Tests(unittest.TestCase):
    def test_all_cleanup_attempted_and_later_cancellation_wins_over_ordinary(self):
        primary = ValueError('body'); cleanup = OSError('close'); cancellation = KeyboardInterrupt('stop')
        calls = []
        def ordinary(): calls.append('ordinary'); raise cleanup
        def cancel(): calls.append('cancel'); raise cancellation
        def final(): calls.append('final')
        with self.assertRaises(KeyboardInterrupt) as observed: finish(primary, [ordinary, cancel, final])
        self.assertIs(observed.exception, cancellation); self.assertEqual(calls, ['ordinary', 'cancel', 'final'])

    def test_original_cancellation_outranks_every_cleanup(self):
        primary = KeyboardInterrupt('body'); later = SystemExit('close'); calls = []
        def cancel(): calls.append('close'); raise later
        def last(): calls.append('last')
        with self.assertRaises(KeyboardInterrupt) as observed: finish(primary, [cancel, last])
        self.assertIs(observed.exception, primary); self.assertEqual(calls, ['close', 'last'])

    def test_primary_ordinary_failure_outranks_ordinary_cleanup(self):
        primary = ValueError('body')
        def close(): raise OSError('close')
        with self.assertRaises(ValueError) as observed: finish(primary, [close])
        self.assertIs(observed.exception, primary); self.assertTrue(primary.cleanup_failed)

    def test_first_cleanup_cancellation_wins_without_body_failure(self):
        first = KeyboardInterrupt('first'); second = SystemExit('second'); calls = []
        def cancel(error):
            def callback(): calls.append(error); raise error
            return callback
        with self.assertRaises(KeyboardInterrupt) as observed: finish(None, [cancel(first), cancel(second)])
        self.assertIs(observed.exception, first); self.assertEqual(calls, [first, second])

    def test_context_cleanup_cancellation_outranks_ordinary_body(self):
        primary = ValueError('body'); cancellation = KeyboardInterrupt('close')
        @contextmanager
        def owner():
            try: yield
            finally: raise cancellation
        with self.assertRaises(KeyboardInterrupt) as observed:
            with owned_context(owner()): raise primary
        self.assertIs(observed.exception, cancellation)

    def test_context_preserves_original_cancellation_and_suppression(self):
        primary = KeyboardInterrupt('body')
        @contextmanager
        def suppress():
            try: yield
            except BaseException: pass
        with self.assertRaises(KeyboardInterrupt) as observed:
            with owned_context(suppress()): raise primary
        self.assertIs(observed.exception, primary)
        @contextmanager
        def replace():
            try: yield
            finally: raise SystemExit('close')
        with self.assertRaises(KeyboardInterrupt) as observed:
            with owned_context(replace()): raise primary
        self.assertIs(observed.exception, primary)

    def test_owned_connection_applies_same_cancellation_precedence(self):
        cancellation = KeyboardInterrupt('close')
        class Connection:
            def close(self): raise cancellation
        with self.assertRaises(KeyboardInterrupt) as observed:
            with owned_connection(Connection()): raise ValueError('body')
        self.assertIs(observed.exception, cancellation)


if __name__ == '__main__': unittest.main()
