"""Existing operator example preserves cancellation through acquired cleanup."""
from contextlib import contextmanager
import os
from pathlib import Path
import runpy
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch


class Tests(unittest.TestCase):
    def setUp(self):
        source = Path(__file__).resolve().parents[1] / 'examples/existing-evolution-operator.py'
        self.provider = runpy.run_path(str(source))

    def test_separate_public_routes_preserve_primary_when_operator_cleanup_fails(self):
        for name in ('open_evolution', 'open_verification'):
            with self.subTest(route=name):
                events = []; selected = object(); primary = KeyboardInterrupt('body')
                @contextmanager
                def opening(invocation):
                    self.assertIs(invocation, selected)
                    try:
                        yield selected
                    finally:
                        events.append('closed')
                        raise OSError('operator cleanup')
                operator = SimpleNamespace(**{name: opening})
                self.provider[name].__wrapped__.__globals__['existing_operator'] = lambda: operator
                with self.assertRaises(KeyboardInterrupt) as caught:
                    with self.provider[name](selected) as answer:
                        self.assertIs(answer, selected)
                        raise primary
                self.assertIs(caught.exception, primary)
                self.assertEqual(events, ['closed'])

    def test_acquired_spark_stops_without_replacing_body_cancellation(self):
        events = []; primary = GeneratorExit('body')
        def stop():
            events.append('spark-stop')
            raise OSError('spark cleanup')
        spark = SimpleNamespace(stop=stop)
        class Builder:
            def master(self, value): return self
            def appName(self, value): return self
            def config(self, *args): return self
            def getOrCreate(self): return spark
        session = SimpleNamespace(getActiveSession=lambda: None, builder=Builder())
        with patch.dict(sys.modules, {'pyspark.sql': SimpleNamespace(SparkSession=session)}), \
                patch.dict(os.environ, {'ASHLAR_DELTA_JARS': '/explicit/existing/cached.jar'}):
            with self.assertRaises(GeneratorExit) as caught:
                with self.provider['native_session']() as acquired:
                    self.assertIs(acquired, spark)
                    raise primary
        self.assertIs(caught.exception, primary)
        self.assertEqual(events, ['spark-stop'])
