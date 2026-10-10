import io, json, hashlib, unittest
from pathlib import Path
from unittest.mock import patch
from ashlar_host import _otel_worker as w
import test_otel_worker as wt
import test_otel_supervision as st

class HoldIndependent(unittest.TestCase):
    def request(self):
        value = io.BytesIO(); w.write_frame(value, {'op':'init','settings':wt.settings_wire()}); value.seek(0); return value
    def test_constructor_cancellation_never_holds_or_replies(self):
        for primary in (KeyboardInterrupt(), SystemExit(), GeneratorExit()):
            request=self.request(); out=io.BytesIO()
            with patch.object(request, 'read', wraps=request.read) as read, patch.object(w, 'SDKWorker', side_effect=primary):
                with self.assertRaises(BaseException) as raised: w.serve(request,out)
            self.assertIs(raised.exception,primary); self.assertEqual(read.call_count,2); self.assertEqual(out.getvalue(),b'')
    def test_hold_reads_exactly_one_byte_after_fixed_reply(self):
        request=self.request(); request.seek(0,2); request.write(b'XY'); request.seek(0); out=io.BytesIO()
        with patch.object(request,'read', wraps=request.read) as read, patch.object(w, 'SDKWorker', side_effect=OSError('private')):
            w.serve(request,out)
        self.assertEqual(read.call_args.args,(1,)); self.assertEqual(request.read(),b'Y')
        self.assertEqual(w.read_frame(io.BytesIO(out.getvalue())),{'ok':False,'value':'diagnostics-configuration'})
    def test_ready_worker_refusal_does_not_use_init_hold(self):
        request=self.request(); request.seek(0,2); w.write_frame(request, {'op':'bad'}); request.write(b'XY'); request.seek(0)
        closed=[]
        class Provider:
            def shutdown(self,**kw): closed.append(True)
        class Worker:
            def __init__(self,*a,**kw): self.closed=False; self.logs=self.traces=self.metrics=Provider()
        out=io.BytesIO()
        with patch.object(request,'read',wraps=request.read) as read, patch.object(w,'SDKWorker',Worker): w.serve(request,out)
        self.assertEqual(read.call_count,4); self.assertEqual(request.read(),b'XY'); self.assertEqual(len(closed),3)
        out.seek(0); self.assertEqual(w.read_frame(out),{'ok':True,'value':'ready'});self.assertEqual(w.read_frame(out),{'ok':False,'value':'diagnostics-configuration'})

if __name__=='__main__':
    s=unittest.TestSuite()
    s.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(HoldIndependent))
    for name in ('test_init_refusal_hold_follows_reply_and_preserves_read_cancellation','test_failed_refusal_write_does_not_wait_for_parent','test_actual_pipe_eof_releases_refusing_worker'):
        s.addTest(wt.WorkerProtocolTests(name))
    s.addTest(st.SupervisionTests('test_actual_refusal_hold_keeps_session_live_for_parent_cleanup'))
    result=unittest.TextTestRunner(verbosity=2).run(s)
    raise SystemExit(not result.wasSuccessful())
