import contextlib, importlib.util, io, json, pathlib, tempfile, unittest
from types import SimpleNamespace
from unittest.mock import patch

HERE=pathlib.Path(__file__).resolve().parent
SUPERVISOR=HERE/'r59_truss052_supervisor.py'
WORKER=HERE/'r59-ack-only-worker.py'
SCOPE=HERE/'r59-host-scope.json'
spec=importlib.util.spec_from_file_location('r59_supervisor_test',SUPERVISOR)
supervisor=importlib.util.module_from_spec(spec);spec.loader.exec_module(supervisor)

class R59AckSupervisorTests(unittest.TestCase):
    def test_reviewed_dispatch_handshake_runs_preflight_then_mock_dispatch_only(self):
        original=json.loads(SCOPE.read_text())
        with tempfile.TemporaryDirectory(prefix='ashlar-r59-mock-dispatch-',dir='/private/tmp') as td:
            root=pathlib.Path(td);scope=original.copy();scope['paths']=dict(original['paths'])
            scope['paths']['output_root']=str(root/'attempt');scope['paths']['supervisor_root']=str(root/'supervisor')
            scope['status']='sol_reviewed_and_dispatchable';scope['native_dispatch']=True
            scope_path=root/'scope.json';scope_path.write_text(json.dumps(scope,sort_keys=True,indent=2)+'\n')
            raw=scope_path.read_bytes()
            receipt={'profile':'ashlar-r59-ack-only-dispatch/0.1','status':'sol_reviewed_and_dispatchable',
                'scope_sha256':__import__('hashlib').sha256(raw).hexdigest(),
                'worker_sha256':__import__('hashlib').sha256(WORKER.read_bytes()).hexdigest(),
                'supervisor_sha256':__import__('hashlib').sha256(SUPERVISOR.read_bytes()).hexdigest(),
                'pending_receipt_sha256':scope['r58']['pending_receipt_sha256'],
                'native_readback_sha256':scope['r58']['native_readback_sha256'],
                'publication_id':scope['r58']['publication_id'],'native_dispatch':True,
                'native_run_performed':False,'source_ack_attempted':False,'group_resubmitted':False,
                'delta_republished':False,
                'mandatory_pre_ack_gate':'one exact read-only SQL request_receipt and consumer checkpoint; public FeedPage and protected custody reread before one ACK'}
            dispatch=root/'dispatch.json';dispatch.write_text(json.dumps(receipt,sort_keys=True,separators=(',',':')))
            calls=[]
            base=SimpleNamespace(_selected_python=lambda value:pathlib.Path(value),_tree_bytes=lambda _path:0,
                _dispatch=lambda *args:calls.append(args) or 0)
            output=io.StringIO()
            with patch.object(supervisor,'load_base',return_value=base),contextlib.redirect_stdout(output):
                with patch('sys.argv',[str(SUPERVISOR),'--scope',str(scope_path),'--worker',str(WORKER),'--dispatch-receipt',str(dispatch)]):
                    code=supervisor.main()
            self.assertEqual(code,0,output.getvalue())
            self.assertEqual(len(calls),1)
            self.assertEqual(calls[0][0]['r58']['publication_id'],scope['r58']['publication_id'])
            self.assertFalse((root/'attempt').exists())
            self.assertFalse((root/'supervisor').exists())

if __name__=='__main__': unittest.main()
