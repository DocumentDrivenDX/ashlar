"""Source-qualified original phase controls with inert native SQL transport."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from ashlar.attempt_store import DeltaAttemptStore, AttemptStoreError
from ashlar.native import SQLResult
from ashlar.outbox import PostgresOutbox
from ashlar.publisher import publish_batch
from ashlar.source_checkpoint import outbox_checkpoint
from ashlar.stored_publisher import StoredPublisherBackend
from ashlar_host.driver import request_for
import test_stored_publisher as publisher_harness


class Policy:
    @contextmanager
    def writer(self,*args): yield


class Executor:
    def __init__(self): self.rows=[];self.calls=[]
    def query(self,sql,parameters):
        self.calls.append((sql,dict(parameters)))
        if sql.startswith('DESCRIBE'): return SQLResult([{'id':'attempt-uuid'}])
        if sql.startswith('MERGE'):
            self.rows.append(json.loads(parameters['payload'])); return SQLResult([])
        rows=[row for row in self.rows if row['stream']==parameters['stream'] and row['batch_id']==parameters['batch']]
        order=('prepared','applying','applied','committing','committed')
        rows.sort(key=lambda row: order.index(row['phase']))
        return SQLResult([{k:row[k] for k in ('phase','request_digest','payload_json','payload_digest')} for row in rows])


class Tests(unittest.TestCase):
    def original(self,feed,epoch='original',predecessor='prior'):
        raw=(Path(__file__).resolve().parents[1]/'examples/end-to-end/source.jsonl').read_bytes()
        class Reader:
            def query(self,sql,parameters):
                return SQLResult([{'position':'1'}] if '.head ' in sql else [{
                    'position':'1','batch_id':'fixture-tx-1','payload':raw.decode(),
                    'digest':hashlib.sha256(raw).hexdigest()}])
        transaction=PostgresOutbox(Reader(),feed=feed,epoch=epoch).read('0')[0]
        request=request_for('shared-stream',transaction,predecessor,{feed:'1'})
        return transaction,request

    def test_two_feeds_same_local_id_publish_and_resume_originals(self):
        executor=Executor(); manifests={}; drivers={}; originals=[]
        class Manifest:
            def commit(self,row,*,context):
                manifests[row['publication_id']]=dict(row); return dict(row)
            def recover(self,row,*,context):
                if manifests.get(row['publication_id'])!=row: raise AssertionError('changed manifest')
                return dict(row)
        for feed in ('A','B'):
            transaction,request=self.original(feed); driver=publisher_harness.Driver(); drivers[feed]=driver
            backend=StoredPublisherBackend(DeltaAttemptStore(executor,Policy(),'c.s.attempt','attempt-uuid',
                original_request=request),driver,lambda request,context:Manifest())
            descriptor=publish_batch(backend,'shared-stream',transaction.batch,predecessor='prior',
                schema_revisions_json=request['schema_revisions_json'],source_checkpoint_json=outbox_checkpoint(transaction),context=object())
            originals.append((transaction,request,descriptor))
        self.assertEqual(len(executor.rows),10)
        for transaction,request,descriptor in originals:
            driver=drivers[transaction.feed]; backend=StoredPublisherBackend(DeltaAttemptStore(executor,Policy(),
                'c.s.attempt','attempt-uuid',original_request=request),driver,lambda request,context:Manifest())
            replay=publish_batch(backend,'shared-stream',transaction.batch,predecessor='prior',
                schema_revisions_json=request['schema_revisions_json'],source_checkpoint_json=outbox_checkpoint(transaction),context=object())
            self.assertEqual(replay,descriptor); self.assertEqual(driver.calls.count('apply'),1)
        self.assertEqual(len(executor.rows),10)
        merges=[sql for sql,_ in executor.calls if sql.startswith('MERGE')]
        self.assertEqual(len(merges),10)
        self.assertTrue(all("'$.feed'" in sql and "'$.epoch'" in sql for sql in merges))

    def test_same_source_changed_original_and_unknown_custody_refuse(self):
        executor=Executor();_,request=self.original('A')
        store=DeltaAttemptStore(executor,Policy(),'c.s.attempt','attempt-uuid',original_request=request)
        with store.session(object()) as session: session.append(request,'prepared')
        _,changed=self.original('A',predecessor='replacement')
        with store.session(object()) as session:
            with self.assertRaises(AttemptStoreError): session.append(changed,'prepared')
        changed_store=DeltaAttemptStore(executor,Policy(),'c.s.attempt','attempt-uuid',original_request=changed)
        with changed_store.session(object()) as session:
            with self.assertRaises(AttemptStoreError):session.read(request['stream'],request['batch_id'])
        executor.rows.append(dict(executor.rows[0]))
        with store.session(object()) as session:
            with self.assertRaises(AttemptStoreError):session.read(request['stream'],request['batch_id'])
        executor.rows.pop(); executor.rows[0]['payload_json']='{}'
        with store.session(object()) as session:
            with self.assertRaises(AttemptStoreError):session.read(request['stream'],request['batch_id'])
        self.assertEqual(sum(sql.startswith('MERGE') for sql,_ in executor.calls),1)

    def test_payload_budget_before_decode_and_caller_request_snapshot(self):
        executor=Executor();_,request=self.original('A')
        store=DeltaAttemptStore(executor,Policy(),'c.s.attempt','attempt-uuid',original_request=request)
        original=dict(request);request['predecessor']='mutated'
        with store.session(object()) as session:
            with self.assertRaises(AttemptStoreError):session.append(request,'prepared')
            session.append(original,'prepared')
        executor.rows[0]['payload_json']='x'*(4*1024*1024+1)
        with store.session(object()) as session:
            # read decodes the owned constructor snapshot, then must refuse the
            # foreign row's byte bound before attempting its JSON decode.
            from ashlar.attempt_store import _json
            calls=[]
            def decode(raw):
                calls.append(len(raw));return _json(raw)
            with patch('ashlar.attempt_store._json',side_effect=decode):
                with self.assertRaises(AttemptStoreError):session.read(original['stream'],original['batch_id'])
            self.assertEqual(len(calls),1)
        executor.rows=[]
        _,next_epoch=self.original('A',epoch='next-epoch')
        second=DeltaAttemptStore(executor,Policy(),'c.s.attempt','attempt-uuid',original_request=next_epoch)
        with store.session(object()) as session:session.append(original,'prepared')
        with second.session(object()) as session:session.append(next_epoch,'prepared')
        self.assertEqual(len(executor.rows),2)

if __name__=='__main__': unittest.main()
