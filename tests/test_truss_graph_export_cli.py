"""Basic existing-publication export orchestration; no native engine claim."""
import hashlib,json,os,tempfile,unittest
from pathlib import Path
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.truss_graph_export_cli import run_export_truss_graph_command

class Tests(unittest.TestCase):
    def run_case(self,root,*,failure=False):
        output=root/'result.json';events=[]
        driver=SimpleNamespace(context=object(),source_sessions=SimpleNamespace(batch=lambda:SimpleNamespace(feed='feed')))
        @contextmanager
        def writer(*args):
            events.append('writer-open')
            try:yield
            finally:events.append('writer-close')
        driver.writer=writer
        raw=b'{"publication":{"publication_id":"original"},"nodes":[{"id":"1"}],"edges":[]}\n';custody=b'{"original":"custody"}\n'
        pair=(SimpleNamespace(payload=raw),custody)
        def selected(configuration,destination,operation,*,report_writer):
            captured=operation(driver,{'request_digest':'original'})
            events.append('all-owners-closed')
            return report_writer(captured,Path(destination))
        def captured(*args,**kwargs):
            self.assertEqual(events[-1],'writer-close');return pair
        from ashlar_host import truss_graph_export_cli as module
        original=module._write_original
        def write(path,payload):
            self.assertIn('all-owners-closed',events)
            if failure and str(path).endswith('-custody.json'):raise OSError('injected pair write failure')
            original(path,payload)
        with patch.object(module,'parse_resume_truss_configuration',return_value=None),patch.object(module,'run_selected_truss_command',side_effect=selected),patch.object(module,'restore_applied_truss_publication',side_effect=lambda *args:events.append('restore')),patch.object(module,'read_original_truss_graph_export',side_effect=captured),patch.object(module,'_write_original',side_effect=write):
            if failure:
                with self.assertRaisesRegex(OSError,'pair write failure'):run_export_truss_graph_command('/private/config',str(output))
                self.assertFalse(output.exists());self.assertEqual(Path(str(output)+'-release.json').read_bytes(),raw)
            else:
                result=run_export_truss_graph_command('/private/config',str(output))
                self.assertEqual(result['publication_id'],'original');self.assertEqual(result['nodes'],1)
                for key,payload in (('release',raw),('custody',custody)):
                    path=Path(result[key]['path']);self.assertEqual(path.read_bytes(),payload);self.assertEqual(path.stat().st_mode&0o777,0o600)
                    self.assertEqual(result[key]['sha256'],hashlib.sha256(payload).hexdigest())
                self.assertEqual(json.loads(output.read_bytes()),result)
    def test_pair_written_only_after_close(self):
        with tempfile.TemporaryDirectory()as directory:self.run_case(Path(directory))
    def test_partial_pair_retained_without_success_report(self):
        with tempfile.TemporaryDirectory()as directory:self.run_case(Path(directory),failure=True)
