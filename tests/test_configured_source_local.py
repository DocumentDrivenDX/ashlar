"""Portable configured source lifecycle only; no Delta or protected ACK claims."""
import tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import test_configured_source_admission as fixtures
from ashlar_host.configured_source_local import open_configured_local_source
from ashlar.source_checkpoint import jsonl_checkpoint
from ashlar.attempt_store import verify_request_digest

class Tests(unittest.TestCase):
    def test_complete_prefix_restart_replay_and_detached_states(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700);config=fixtures.Tests().fixture(root);context=object()
            with patch('ashlar_host.configured_source_admission.admit_managed_records',return_value=(b'{}',)):
                with open_configured_local_source(config,stream='original-雪',context=context)as source:
                    snapshots=[source.state_at(n,context)for n in range(len(source.batches)+1)]
                    self.assertEqual(len(snapshots[0].current),0)
                    self.assertGreater(len(snapshots[-1].history),len(snapshots[-1].current))
                    self.assertGreater(len(snapshots[-1].tombstones),0)
                    self.assertEqual(len(snapshots[1].current),2)
                    changed=next(v for k,v in snapshots[2].current.items()if k.id==1)
                    self.assertEqual(changed.version,2);self.assertEqual(changed.props_json,'{"1023":"updated","1024":"雪"}')
                    self.assertEqual(changed.retained_json,'{"future":18446744073709551615}')
                    self.assertEqual(len(snapshots[3].current),1)
                    specs=[source.spec(n,context)for n in range(1,len(source.batches)+1)]
                    for n,spec in enumerate(specs):
                        request=spec.request();verify_request_digest(request);source.admit_request(request,context)
                        self.assertEqual(request['source_checkpoint_json'],jsonl_checkpoint(source.batches[n]))
                        if n:self.assertEqual(request['predecessor'],specs[n-1].publication_id)
                    with self.assertRaises(TypeError):snapshots[-1].current['foreign']=object()
                    self.assertGreater(len(source.state_at(len(source.batches),context).current),0)
                with self.assertRaises(ValueError):source.metadata(context)
                # New source owner and receipt directory; same original inputs,
                # no publication or local checkpoint is invented on restart.
                from dataclasses import replace
                restarted=replace(config,output=root/'restart-receipts')
                with open_configured_local_source(restarted,stream='original-雪',context=object())as reopened:
                    self.assertEqual([reopened.spec(n,reopened.context)for n in range(1,len(reopened.batches)+1)],specs)
    def test_drift_and_cancellation_close_source_and_refuse_requests(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700);config=fixtures.Tests().fixture(root);context=object();cancel=KeyboardInterrupt()
            with patch('ashlar_host.configured_source_admission.admit_managed_records',return_value=(b'{}',)):
                with self.assertRaises(KeyboardInterrupt)as raised:
                    with open_configured_local_source(config,stream='original-雪',context=context)as source:
                        request=source.spec(1,context).request();request['predecessor']='foreign'
                        with self.assertRaises(ValueError):source.admit_request(request,context)
                        (root/'source.input').write_bytes(b'changed')
                        with self.assertRaises(ValueError):source.state_at(1,context)
                        raise cancel
                self.assertIs(raised.exception,cancel);self.assertTrue(cancel.cleanup_failed)
                with self.assertRaises(ValueError):source.spec(1,context)
