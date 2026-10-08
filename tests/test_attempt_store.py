from contextlib import contextmanager
import hashlib,json
import unittest
from ashlar.attempt_store import AttemptStoreError,DeltaAttemptStore
from ashlar.native import SQLResult

REQUEST={'stream':'s','batch_id':'b','predecessor':'old','schema_revisions_json':'{"f":"1"}','source_batch_json':'{"source":"fixture"}','source_batch_digest':'a'*64}
REQUEST['request_digest']=hashlib.sha256(json.dumps(REQUEST,sort_keys=True,separators=(',',':')).encode()).hexdigest()
class Policy:
    @contextmanager
    def writer(self,*args):yield
class MemoryExecutor:
    def __init__(self):self.rows=[];self.calls=[]
    def query(self,sql,parameters):
        self.calls.append(sql)
        if sql.startswith('DESCRIBE'):return SQLResult([{'id':'uuid'}])
        if sql.startswith('MERGE'):
            value=json.loads(parameters['payload']);self.rows.append(value);return SQLResult([])
        return SQLResult([{k:r[k] for k in ['phase','request_digest','payload_json','payload_digest']} for r in self.rows])
class AttemptStoreTests(unittest.TestCase):
    def test_complete_phase_replay_and_session_lifetime(self):
        ex=MemoryExecutor();store=DeltaAttemptStore(ex,Policy(),'c.s.t','uuid')
        with store.session('context') as session:
            self.assertEqual(session.read('s','b'),())
            for phase in ['prepared','applying','applied','committing','committed']:
                kwargs={} if phase in ['prepared','applying'] else {'result_json':'{"pin":1}'}
                if phase=='committed':kwargs['descriptor_json']='{"id":"original"}'
                phase_record=session.append(REQUEST,phase,**kwargs)
                self.assertEqual(session.append(REQUEST,phase,**kwargs),phase_record)
            self.assertEqual(len(session.read('s','b')),5)
        with self.assertRaises(AttemptStoreError):session.read('s','b')
        self.assertEqual(sum(x.startswith('MERGE') for x in ex.calls),5)
    def test_skip_conflict_and_missing_result_refuse_before_write(self):
        ex=MemoryExecutor();store=DeltaAttemptStore(ex,Policy(),'c.s.t','uuid')
        with store.session('ctx') as session:
            with self.assertRaises(AttemptStoreError):session.append(REQUEST,'applying')
            session.append(REQUEST,'prepared');session.append(REQUEST,'applying')
            with self.assertRaises(AttemptStoreError):session.append(REQUEST,'applied')
            session.append(REQUEST,'applied',result_json='{"pin":1}')
            with self.assertRaises(AttemptStoreError):session.append(REQUEST,'applied',result_json='{"pin":2}')
            with self.assertRaises(AttemptStoreError):session.append(REQUEST,'committing',result_json='{"pin":2}')
        self.assertEqual(len(ex.rows),3)
    def test_corrupt_duplicate_and_missing_custody_refuse(self):
        for mutation in ['digest','duplicate','missing']:
            ex=MemoryExecutor();store=DeltaAttemptStore(ex,Policy(),'c.s.t','uuid')
            with store.session('ctx') as session:
                session.append(REQUEST,'prepared');session.append(REQUEST,'applying')
                if mutation=='digest':ex.rows[0]['payload_digest']='0'*64
                if mutation=='duplicate':ex.rows.append(dict(ex.rows[-1]))
                if mutation=='missing':ex.rows.pop(0)
                with self.assertRaises(AttemptStoreError):session.read('s','b')
if __name__=='__main__':unittest.main()
