import io,json,tempfile,unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from ashlar_host.truss_local_server import _serve,selected_directory

class Tests(unittest.TestCase):
    def runtime(self,events,cleanup_error=None):
        class Runtime:
            info=SimpleNamespace(connection_uri='postgresql://actual-live-local-server')
            def __enter__(self):events.append('started');return self
            def __exit__(self,*args):
                events.append('stop')
                if cleanup_error is not None:raise cleanup_error
        return Runtime()
    def test_existing_initialized_directory_and_live_uri_then_normal_stop(self):
        with tempfile.TemporaryDirectory()as directory:
            path=Path(directory).resolve()
            with self.assertRaises(ValueError):selected_directory(path)
            (path/'PG_VERSION').write_text('18\n');self.assertEqual(selected_directory(path),path)
            (path/'postmaster.pid').write_text('123\n')
            with self.assertRaises(ValueError):selected_directory(path)
            (path/'postmaster.pid').unlink()
            events=[];output=io.StringIO();runtime=self.runtime(events)
            def wait():
                self.assertEqual(events,['started'])
                self.assertEqual(json.loads(output.getvalue()),{'connection_uri':runtime.info.connection_uri})
                events.append('user-stop')
            with redirect_stdout(output):_serve(path,lambda selected:runtime,wait)
            self.assertEqual(events,['started','user-stop','stop'])
    def test_primary_failure_is_preserved_when_owned_stop_also_fails(self):
        events=[];primary=LookupError('foreground failure');runtime=self.runtime(events,RuntimeError('stop failed'))
        def wait():raise primary
        with redirect_stdout(io.StringIO()),self.assertRaises(LookupError)as caught:_serve(Path('/private/existing'),lambda selected:runtime,wait)
        self.assertIs(caught.exception,primary);self.assertTrue(primary.cleanup_failed)
        self.assertEqual(events,['started','stop'])
    def test_startup_interruption_is_not_reported_as_normal_shutdown(self):
        primary=KeyboardInterrupt();events=[];output=io.StringIO()
        class Runtime:
            def __enter__(self):events.append('start-attempt');raise primary
            def __exit__(self,*args):events.append('stop')
        with redirect_stdout(output),self.assertRaises(KeyboardInterrupt)as caught:
            _serve(Path('/private/existing'),lambda selected:Runtime(),lambda:None)
        self.assertIs(caught.exception,primary);self.assertEqual(output.getvalue(),'')
        self.assertEqual(events,['start-attempt'])
