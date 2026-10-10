"""Synthetic composition ownership controls; no installed SDK/native claim."""
from dataclasses import replace
from pathlib import Path
import json
import sys
import tempfile
import unittest
from unittest.mock import patch

from ashlar_host import diagnostic_composition as module
from ashlar_host.diagnostics import DiagnosticSignalSink, DiagnosticsError, read_diagnostics
from ashlar_host.config import HostError
import test_diagnostics_configuration as fixtures


class DisabledCompositionTests(unittest.TestCase):
    def test_disabled_avoids_discovery_construction_and_preserves_body_primary(self):
        with patch.object(module.metadata,'version',side_effect=AssertionError),patch.object(module,'DiagnosticRun',side_effect=AssertionError),patch.object(module,'OtelRun',side_effect=AssertionError):
            with module.diagnostic_run() as run:self.assertIsNone(run)
            primary=KeyboardInterrupt()
            with self.assertRaises(BaseException)as caught:
                with module.diagnostic_run():raise primary
            self.assertIs(caught.exception,primary)


@unittest.skipUnless(sys.version_info[:2] == (3,11), 'selected composition requires Python 3.11')
class CompositionTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name).resolve()
        self.config=replace(fixtures.DiagnosticsConfigurationTests().configuration(),capture_root=self.root)
        def version(name):
            return '0.1.0.dev0' if name=='ashlar-graph-toolkit' else module.SDK_VERSIONS[name]
        self.metadata=patch('ashlar_host.diagnostic_composition.metadata.version',side_effect=version)
        self.metadata.start();self.addCleanup(self.metadata.stop)

    def owner(self,context_error=None,shutdown_error=None,emit_error=None):
        self.calls=[]
        def context(*args):
            self.calls.append('context')
            if context_error is not None:raise context_error
            return None
        def emit(raw):
            self.calls.append('emit')
            if emit_error is not None:raise emit_error
        def shutdown(deadline):
            self.calls.append('shutdown')
            if shutdown_error is not None:raise shutdown_error
            return {name:{'submitted':0,'handed_off':0,'dropped':0,'unknown':False,'flush':'complete'}for name in ('logs','spans','metrics')}
        class Owner:pass
        owner=Owner();owner.sink=DiagnosticSignalSink(emit,context,shutdown)
        return owner

    def test_all_sixteen_profile_pins_admitted_before_capture_or_worker(self):
        for missing in module.SDK_VERSIONS:
            def version(name):
                if name==missing:return 'wrong'
                return '0.1.0.dev0' if name=='ashlar-graph-toolkit' else module.SDK_VERSIONS[name]
            with patch.object(module.metadata,'version',side_effect=version),patch.object(module,'DiagnosticRun')as capture,patch.object(module,'OtelRun')as worker:
                with self.assertRaisesRegex(HostError,'^diagnostics-configuration$'):
                    with module.diagnostic_run(self.config):pass
                capture.assert_not_called();worker.assert_not_called()

    def test_local_capture_precedes_worker_and_startup_failure_remains_untraced(self):
        def startup(config):
            directories=list(self.root.iterdir());self.assertEqual(len(directories),1)
            self.assertEqual(json.loads((directories[0]/'run.json').read_bytes())['state'],'open')
            error=module.OtelStartupError(cleanup_complete=True,dependency_admitted=True)
            raise error
        with patch.object(module,'OtelRun',side_effect=startup):
            with module.diagnostic_run(self.config)as run:
                attempt=run.begin_attempt('held-read');run.finish_attempt(attempt,'succeeded')
        result=read_diagnostics(run.directory)
        self.assertEqual(result['matched'],2)
        self.assertTrue(result['capture_complete'])
        self.assertTrue(all('trace'not in record['event']for record in result['records']))
        self.assertTrue(all(loss['unknown']and loss['flush']=='failed'for name,loss in result['loss'].items()if name in ('logs','spans','metrics')))

    def test_ordinary_context_failure_keeps_capture_and_still_closes_owner_once(self):
        owner=self.owner(context_error=OSError('private-context'))
        with patch.object(module,'OtelRun',return_value=owner):
            with module.diagnostic_run(self.config)as run:
                attempt=run.begin_attempt('held-read');run.phase(attempt,'guard','completed');run.finish_attempt(attempt,'succeeded')
        self.assertEqual(self.calls,['context','shutdown'])
        result=read_diagnostics(run.directory);self.assertEqual(result['matched'],3)
        self.assertTrue(result['loss']['logs']['unknown'])

    def test_ordinary_emit_failure_retains_owner_for_cleanup_and_local_capture(self):
        owner=self.owner(emit_error=OSError('private-emit'))
        with patch.object(module,'OtelRun',return_value=owner):
            with module.diagnostic_run(self.config)as run:
                attempt=run.begin_attempt('held-read');run.finish_attempt(attempt,'succeeded')
        self.assertEqual(self.calls,['context','emit','shutdown'])
        result=read_diagnostics(run.directory)
        self.assertEqual(result['matched'],2);self.assertTrue(result['loss']['logs']['unknown'])

    def test_close_failure_before_sink_still_closes_owner_with_one_deadline(self):
        owner=self.owner();actual=module.DiagnosticRun
        class InterruptedClose:
            def __init__(self,config,sink):self.run=actual(config,sink)
            def close(self):raise OSError('private-close')
        with patch.object(module,'DiagnosticRun',InterruptedClose),patch.object(module,'OtelRun',return_value=owner):
            with module.diagnostic_run(self.config):pass
        self.assertEqual(self.calls,['shutdown'])

    def test_diagnostic_cleanup_does_not_mark_business_primary_and_cancellation_priority(self):
        for business,cleanup in ((OSError('private-business'),OSError('private-cleanup')),
                                 (OSError('private-business'),KeyboardInterrupt()),
                                 (KeyboardInterrupt(),SystemExit()),(SystemExit(),GeneratorExit())):
            self.config=replace(self.config,capture_root=self.root)
            owner=self.owner(shutdown_error=cleanup)
            with patch.object(module,'OtelRun',return_value=owner):
                with self.assertRaises(BaseException)as caught:
                    with module.diagnostic_run(self.config):raise business
            self.assertIs(caught.exception,cleanup if isinstance(business,Exception)and not isinstance(cleanup,Exception)else business)
            self.assertFalse(hasattr(business,'cleanup_failed'))
            self.assertEqual(self.calls,['shutdown'])

    def test_partial_capture_refuses_before_worker_and_startup_cancellation_closes_capture(self):
        with patch.object(module,'DiagnosticRun',side_effect=OSError('private-capture')),patch.object(module,'OtelRun')as worker:
            with self.assertRaisesRegex(HostError,'^diagnostics-configuration$'):
                with module.diagnostic_run(self.config):pass
            worker.assert_not_called()
        primary=KeyboardInterrupt()
        with patch.object(module,'OtelRun',side_effect=primary):
            with self.assertRaises(BaseException)as caught:
                with module.diagnostic_run(self.config):pass
        self.assertIs(caught.exception,primary)
        directory=next(self.root.iterdir())
        self.assertEqual(json.loads((directory/'run.json').read_bytes())['state'],'closed')

    def test_unqualified_startup_failure_never_enters_business_body(self):
        class UntrustedSubclass(module.OtelStartupError):pass
        spoof=DiagnosticsError('diagnostics-configuration');spoof.cleanup_complete=spoof.dependency_admitted=True
        for failure in (OSError('private-startup'),DiagnosticsError('diagnostics-configuration'),
                        module.OtelStartupError(cleanup_complete=False,dependency_admitted=True),
                        module.OtelStartupError(cleanup_complete=True,dependency_admitted=False),
                        UntrustedSubclass(cleanup_complete=True,dependency_admitted=True),spoof):
            with patch.object(module,'OtelRun',side_effect=failure):
                entered=[]
                with self.assertRaisesRegex(HostError,'^diagnostics-configuration$'):
                    with module.diagnostic_run(self.config):entered.append(True)
                self.assertEqual(entered,[])


if __name__=='__main__':unittest.main()
