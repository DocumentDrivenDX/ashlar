"""Independent finite parent protocol and cleanup controls; no SDK worker/network."""
from unittest import TestCase,main
from unittest.mock import patch,Mock
import threading
import subprocess
from ashlar_host import otel
from ashlar_host.otel import OtelStartupError,DiagnosticsError
from test_otel_supervision import CHILD,SupervisionTests
import test_diagnostics_configuration as fixtures


class OwnerSeamControls(TestCase):
    def test_real_fake_child_init_protocol_matrix(self):
        corrupt=[
            "write({'dependency_admitted':True})",
            "write({'dependency_admitted':False})",
            "write({'unexpected':True})",
            "write('not-ready')",
            "write('private-unrecognized-refusal',False)",
        ]
        def frame(raw):
            return 'sys.stdout.buffer.write(struct.pack(\'>I\',len('+repr(raw)+'))+'+repr(raw)+');sys.stdout.buffer.flush()'
        corrupt += [frame(b'{"ok":1,"value":"ready"}'),
                    frame(b'{"ok":true,"value":"ready","ok":true}'),
                    frame(b'{"ok":true,"value":NaN}'),
                    frame(b'not-json'),
                    "sys.stdout.buffer.write(struct.pack('>I',65537));sys.stdout.buffer.flush()"]
        for addition in corrupt:
            case=SupervisionTests()
            child=CHILD.replace("write('ready')",addition+';time.sleep(20)')
            with self.assertRaises(OtelStartupError) as seen:case.create(child)
            self.assertIs(seen.exception.dependency_admitted,False,addition)
            self.assertIs(seen.exception.cleanup_complete,True,addition)
            self.assertIsNone(seen.exception.__context__)
            self.assertIsNone(seen.exception.__cause__)
            self.assertTrue(all(p.returncode is not None and p.stdin.closed and p.stdout.closed for p in case.children))
        for admitted in (False,True):
            case=SupervisionTests()
            child=CHILD.replace("write('ready')","write('diagnostics-configuration',False);time.sleep(20)")
            if not admitted:child=child.replace("write({'dependency_admitted':True})",'')
            with self.assertRaises(OtelStartupError) as seen:case.create(child)
            self.assertIs(seen.exception.dependency_admitted,admitted)
            self.assertIs(seen.exception.cleanup_complete,True)

    def test_public_disposition_closed_bool_properties(self):
        for cleanup in (False,True):
            for admitted in (False,True):
                error=OtelStartupError(cleanup_complete=cleanup,dependency_admitted=admitted)
                self.assertIs(error.cleanup_complete,cleanup);self.assertIs(error.dependency_admitted,admitted)
                for field in ('cleanup_complete','dependency_admitted','_cleanup_complete','_dependency_admitted'):
                    with self.assertRaises(AttributeError):setattr(error,field,True)
                    with self.assertRaises(AttributeError):delattr(error,field)
        for value in (None,0,1,'true',[],object()):
            with self.assertRaises(DiagnosticsError):OtelStartupError(cleanup_complete=value,dependency_admitted=True)
            with self.assertRaises(DiagnosticsError):OtelStartupError(cleanup_complete=True,dependency_admitted=value)

    def test_constructor_owned_cleanup_receipt_and_context_sanitization(self):
        config=fixtures.DiagnosticsConfigurationTests().configuration()
        for fault in (None,'group','stdin','stdout','wait'):
            process=Mock(pid=1234567,returncode=None)
            process.stdin.fileno.return_value=1234567;process.stdout.fileno.return_value=1234568
            primary=OSError('synthetic-private-constructor')
            if fault=='stdin':process.stdin.close.side_effect=OSError('synthetic-close')
            if fault=='stdout':process.stdout.close.side_effect=OSError('synthetic-close')
            if fault=='wait':process.wait.side_effect=subprocess.TimeoutExpired('synthetic',0)
            def startup(owner,*args):owner._dependency_admitted=True;raise primary
            with patch.object(otel.metadata,'version',return_value='0.1.0.dev0'),patch.object(otel.subprocess,'Popen',return_value=process),patch.object(otel.os,'set_blocking'),patch.object(otel.os,'killpg',side_effect=OSError('synthetic-kill') if fault=='group' else None),patch.object(otel.OtelRun,'_exchange',startup):
                with self.assertRaises(OtelStartupError) as seen:otel.OtelRun(config)
            self.assertIs(seen.exception.dependency_admitted,True)
            self.assertIs(seen.exception.cleanup_complete,fault is None)
            self.assertIsNone(seen.exception.__context__);self.assertIsNone(seen.exception.__cause__)
            process.stdin.close.assert_called_once();process.stdout.close.assert_called_once();process.wait.assert_called_once()

    def test_cleanup_first_nonexception_matrix(self):
        for primary in (OSError('ordinary'),KeyboardInterrupt(),SystemExit(),GeneratorExit()):
            for cleanup in (KeyboardInterrupt(),SystemExit(),GeneratorExit()):
                run=otel.OtelRun.__new__(otel.OtelRun)
                run._process=Mock(pid=1234567)
                run._cleanup_lock=threading.Lock()
                run._group_termination_attempted=False;run._group_termination_complete=False
                run._process.stdin.close.side_effect=cleanup
                with patch.object(otel.os,'killpg'):
                    if isinstance(primary,Exception):
                        with self.assertRaises(BaseException) as seen:run._dispose(primary=primary)
                        self.assertIs(seen.exception,cleanup)
                    else:self.assertIs(run._dispose(primary=primary),False)
                run._process.stdout.close.assert_called_once();run._process.wait.assert_called_once()

    def test_proven_absent_group_and_uncertain_group_are_distinct(self):
        for failure,expected in ((ProcessLookupError(),True),(PermissionError(),False)):
            run=otel.OtelRun.__new__(otel.OtelRun);run._process=Mock(pid=1234567)
            run._cleanup_lock=threading.Lock()
            run._group_termination_attempted=False;run._group_termination_complete=False
            with patch.object(otel.os,'killpg',side_effect=failure) as kill:
                self.assertIs(run._dispose(primary=OSError('primary')),expected)
                self.assertIs(run._dispose(primary=OSError('primary')),expected)
                kill.assert_called_once()


if __name__=='__main__':main(verbosity=2)
