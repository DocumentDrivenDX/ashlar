"""Fresh-context and primary-safe cleanup controls using inert SDK doubles."""
from contextlib import ExitStack
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from ashlar_host.original_medical_runtime import OriginalMedicalRuntimeCustody
from ashlar_host.original_medical_native_session import original_medical_native_session
from ashlar_host.medical_process_supervision import OriginalMedicalNativePhase
from ashlar_host.original_medical_local_configuration import OriginalMedicalBounds


class Configuration:
    def __init__(self, *, loadDefaults):
        if loadDefaults is not False: raise AssertionError('Ambient Spark defaults')
        self.values = {}
    def setAll(self, values): self.values = dict(values); return self
    def get(self, key): return self.values[key]


class Tests(unittest.TestCase):
    def phase(self, stack):
        self.custody.bounds = OriginalMedicalBounds(
            4194304, 10000, 1048576, 16777216, 1048576, 1048576,
            8388608, 300, 540, 'local[1]', '512m')
        read, write = os.pipe()
        stack.callback(os.close, read); stack.callback(os.close, write)
        os.set_blocking(write, False)
        return OriginalMedicalNativePhase(write), read

    def test_phase_begins_before_imports_and_ends_after_both_stops(self):
        with ExitStack() as stack:
            phase, read = self.phase(stack)
            observed = []
            def imports():
                if not observed:
                    observed.append(os.read(read, 32))
            self.custody.verify_imports.side_effect = imports
            with self.sdk(), original_medical_native_session(self.custody, phase=phase):
                self.assertEqual(observed, [b'native.begin\n'])
            self.session.stop.assert_called_once(); self.context.stop.assert_called_once()
            self.assertEqual(os.read(read, 32), b'native.end\n')

    def test_failed_cleanup_leaves_native_interval_unfinished(self):
        with ExitStack() as stack:
            phase, read = self.phase(stack)
            self.session.stop.side_effect = OSError('stop failed')
            with self.sdk(), self.assertRaises(OSError):
                with original_medical_native_session(self.custody, phase=phase): pass
            self.context.stop.assert_called_once()
            self.assertEqual(os.read(read, 32), b'native.begin\n')
            os.set_blocking(read, False)
            with self.assertRaises(BlockingIOError): os.read(read, 32)

    def setUp(self):
        self.environment = {'JAVA_HOME':'/selected/java', 'PYTHONDONTWRITEBYTECODE':'1'}
        self.custody = object.__new__(OriginalMedicalRuntimeCustody)
        self.custody.closed = False; self.custody.plan = {}
        self.custody.runtime = SimpleNamespace(spark_version='4.0.1', jars=(SimpleNamespace(path=Path('/selected/a.jar')), SimpleNamespace(path=Path('/selected/b.jar'))))
        self.custody.bounds = SimpleNamespace(master='local[1]', driver_memory='512m')
        self.custody.renew = Mock(); self.custody.verify_imports = Mock()
        self.custody.native_environment = Mock(return_value=self.environment)
        self.context = SimpleNamespace(master='local[1]', stop=Mock())
        self.session = SimpleNamespace(version='4.0.1', sparkContext=self.context, stop=Mock())
        self.session_error = None; self.context_error = None
        self.constructed = 0
    def context_factory(self, *, conf):
        if self.context_error: raise self.context_error
        self.constructed += 1; self.context.getConf = lambda: conf
        self.session.conf = conf
        return self.context
    def session_factory(self, context):
        self.assertIs(context, self.context)
        if self.session_error: raise self.session_error
        return self.session
    def sdk(self):
        stack = ExitStack()
        stack.enter_context(patch.dict(os.environ, self.environment, clear=True))
        stack.enter_context(patch.dict(sys.modules, {
            'pyspark':SimpleNamespace(SparkConf=Configuration, SparkContext=self.context_factory),
            'pyspark.sql':SimpleNamespace(SparkSession=self.session_factory)}))
        return stack
    def test_fresh_public_construction_exact_settings_and_independent_stop(self):
        with self.sdk(), original_medical_native_session(self.custody) as session:
            self.assertIs(session, self.session); self.assertEqual(self.constructed, 1)
            settings = self.context.getConf().values
            self.assertEqual(settings['spark.jars'], '/selected/a.jar,/selected/b.jar')
            self.assertEqual(settings['spark.master'], 'local[1]')
            self.assertEqual(settings['spark.driver.memory'], '512m')
        self.session.stop.assert_called_once(); self.context.stop.assert_called_once()
        self.assertGreaterEqual(self.custody.renew.call_count, 3)
        self.assertEqual(self.custody.verify_imports.call_count, 2)
    def test_exact_owner_and_closed_environment_before_context(self):
        with self.assertRaises(ValueError):
            with original_medical_native_session(SimpleNamespace()): pass
        with self.sdk(), patch.dict(os.environ, {'SPARK_REMOTE':'foreign'}), self.assertRaises(PermissionError):
            with original_medical_native_session(self.custody): pass
        self.assertEqual(self.constructed, 0)
    def test_session_construction_failure_stops_acquired_context(self):
        self.session_error = OSError('session construction')
        with self.sdk(), self.assertRaises(OSError) as caught:
            with original_medical_native_session(self.custody): pass
        self.assertIs(caught.exception, self.session_error)
        self.context.stop.assert_called_once(); self.session.stop.assert_not_called()
        self.assertEqual(self.custody.verify_imports.call_count, 2)
    def test_existing_context_refusal_does_not_stop_foreign_owner(self):
        self.context_error = ValueError('existing context')
        with self.sdk(), self.assertRaises(ValueError) as caught:
            with original_medical_native_session(self.custody): pass
        self.assertIs(caught.exception, self.context_error)
        self.context.stop.assert_not_called(); self.session.stop.assert_not_called()
    def test_wrong_version_and_effective_configuration_refuse_with_cleanup(self):
        for change in ('version', 'master', 'jars', 'sql'):
            self.setUp()
            original = self.session_factory
            def changed(context):
                session = original(context)
                if change == 'version': session.version = '3.5.3'
                if change == 'master': context.master = 'local[8]'
                if change == 'jars': context.getConf().values['spark.jars'] = '/foreign.jar'
                if change == 'sql': session.conf = SimpleNamespace(get=lambda key:'foreign')
                return session
            self.session_factory = changed
            with self.subTest(change=change), self.sdk(), self.assertRaises(PermissionError):
                with original_medical_native_session(self.custody): pass
            self.session_factory = original
            self.session.stop.assert_called_once(); self.context.stop.assert_called_once()
    def test_closing_drift_refuses_success_and_stops_both_owners(self):
        with self.sdk(), self.assertRaises(PermissionError):
            with original_medical_native_session(self.custody):
                self.context.getConf().values['spark.driver.memory'] = '8g'
        self.session.stop.assert_called_once(); self.context.stop.assert_called_once()
    def test_primary_cancellation_survives_independent_closing_failures(self):
        primary = KeyboardInterrupt('cancel')
        self.session.stop.side_effect = OSError('session stop')
        self.context.stop.side_effect = OSError('context stop')
        with self.sdk(), self.assertRaises(KeyboardInterrupt) as caught:
            with original_medical_native_session(self.custody):
                self.custody.renew.side_effect = OSError('custody closing')
                raise primary
        self.assertIs(caught.exception, primary)
        self.session.stop.assert_called_once(); self.context.stop.assert_called_once()
        self.assertEqual(self.custody.verify_imports.call_count, 2)
