"""Held execution cancellation controls; mocked ports confer no native support."""
import unittest
from contextlib import contextmanager

import test_run_commerce_path_weft as fixtures


class IntervalCleanupTests(unittest.TestCase):
    def setup_execution(self):
        helper = fixtures.PathExecutionTests()
        case = helper.fixture()
        opened, config, oracle = helper.setup(case, [])
        return helper, case, opened, config, oracle

    def test_guard_and_query_cancellation_survive_interval_cleanup_failure(self):
        for phase in ('guard', 'query'):
            with self.subTest(phase=phase):
                helper, case, opened, config, oracle = self.setup_execution()
                primary = KeyboardInterrupt('original execution cancellation')
                cleanup = OSError('private cleanup detail')
                original_sql = opened.provider.sql
                queries = []

                def sql(text, args):
                    queries.append(text)
                    is_query = text == case['response']['sql']
                    if is_query == (phase == 'query'):
                        raise primary
                    return original_sql(text, args)

                @contextmanager
                def interval(context):
                    opened.provider.active = True
                    try:
                        yield
                    finally:
                        opened.provider.active = False
                        raise cleanup

                opened.provider.interval = interval
                opened.provider.driver.transport.spark.sql = sql
                with self.assertRaises(KeyboardInterrupt) as caught:
                    helper.execute(case, opened, config, oracle)
                self.assertIs(caught.exception, primary)
                self.assertTrue(primary.interval_cleanup_failed)
                self.assertFalse(opened.provider.active)
                self.assertEqual(case['response']['sql'] in queries, phase == 'query')

    def test_suppressing_interval_cannot_swallow_original_cancellation(self):
        helper, case, opened, config, oracle = self.setup_execution()
        primary = KeyboardInterrupt('original execution cancellation')

        @contextmanager
        def interval(context):
            opened.provider.active = True
            try:
                yield
            except BaseException:
                pass
            finally:
                opened.provider.active = False

        def sql(*args, **kwargs):
            raise primary

        opened.provider.interval = interval
        opened.provider.driver.transport.spark.sql = sql
        with self.assertRaises(KeyboardInterrupt) as caught:
            helper.execute(case, opened, config, oracle)
        self.assertIs(caught.exception, primary)
        self.assertFalse(hasattr(primary, 'interval_cleanup_failed'))
        self.assertFalse(opened.provider.active)

    def test_successful_body_with_cleanup_failure_withholds_result(self):
        helper, case, opened, config, oracle = self.setup_execution()
        cleanup = OSError('private cleanup detail')

        @contextmanager
        def interval(context):
            opened.provider.active = True
            try:
                yield
            finally:
                opened.provider.active = False
                raise cleanup

        opened.provider.interval = interval
        with self.assertRaises(OSError) as caught:
            helper.execute(case, opened, config, oracle)
        self.assertIs(caught.exception, cleanup)
        self.assertFalse(opened.provider.active)
        self.assertTrue(any(isinstance(call, tuple) and call[1] == case['response']['sql']
                            for call in opened.provider.calls))

    def test_interval_entry_failure_precedes_native_callbacks(self):
        helper, case, opened, config, oracle = self.setup_execution()
        entry = OSError('entry refused')

        @contextmanager
        def interval(context):
            raise entry
            yield

        opened.provider.interval = interval
        with self.assertRaises(OSError) as caught:
            helper.execute(case, opened, config, oracle)
        self.assertIs(caught.exception, entry)
        self.assertEqual(opened.provider.calls, [])


if __name__ == '__main__':
    unittest.main()
