"""Real finalizer tests without importing or starting a native runtime."""
import json,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from run_commerce_outbox_publication import finalize_publication

class CommercePublicationLifecycleTests(unittest.TestCase):
    def test_report_preserves_bytes_and_follows_both_cleanup_steps(self):
        with tempfile.TemporaryDirectory()as directory:
            output=Path(directory);calls=[];report={'format':'original','rows':[{'exact':'12.5000'}]}
            def cleanup(name):
                self.assertFalse((output/'report.json').exists());calls.append(name)
            finalize_publication(SimpleNamespace(close=lambda:cleanup('transport')),SimpleNamespace(stop=lambda:cleanup('spark')),report,output)
            self.assertEqual(calls,['transport','spark'])
            self.assertEqual((output/'report.json').read_text(),'{"format":"original","rows":[{"exact":"12.5000"}]}\n')
            self.assertEqual(json.loads((output/'report.json').read_bytes()),report)

    def test_each_and_both_cleanup_failures_withhold_success_and_attempt_both(self):
        for fail in ({'transport'},{'spark'},{'transport','spark'}):
            with self.subTest(fail=fail),tempfile.TemporaryDirectory()as directory:
                output=Path(directory);calls=[]
                def cleanup(name):
                    calls.append(name)
                    if name in fail:raise RuntimeError(name+' failed')
                with self.assertRaises(RuntimeError):
                    finalize_publication(SimpleNamespace(close=lambda:cleanup('transport')),SimpleNamespace(stop=lambda:cleanup('spark')),{'success':True},output)
                self.assertEqual(calls,['transport','spark']);self.assertFalse((output/'report.json').exists())

    def test_incomplete_operation_and_absent_transport_stop_without_success(self):
        with tempfile.TemporaryDirectory()as directory:
            output=Path(directory);calls=[]
            finalize_publication(None,SimpleNamespace(stop=lambda:calls.append('spark')),None,output)
            self.assertEqual(calls,['spark']);self.assertFalse((output/'report.json').exists())

if __name__=='__main__':unittest.main()
