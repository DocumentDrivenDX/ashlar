# SPDX-License-Identifier: MIT OR Apache-2.0
"""Source-qualified original phase controls with inert native SQL transport."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from ashlar.attempt_store import (DeltaAttemptStore, AttemptStoreError,
    validate_source_qualified_phase_inventory)
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

    def test_current_truss_checkpoint_gets_profile_feed_epoch_phase_scope(self):
        import test_truss_feed_sessions as truss_harness
        _,request,_,_,_,_,_=truss_harness.Tests().setup_case()
        executor=Executor()
        store=DeltaAttemptStore(executor,Policy(),'c.s.attempt','attempt-uuid',original_request=request)
        checkpoint=json.loads(request['source_checkpoint_json'])
        self.assertEqual(store.source_scope,('truss-native-feed/0.1',checkpoint['feed'],checkpoint['epoch']))
        with store.session(object()) as session:
            row=session.append(request,'prepared')
            self.assertEqual(session.read(request['stream'],request['batch_id']),(row,))
        merge=next(sql for sql,_ in executor.calls if sql.startswith('MERGE'))
        self.assertTrue(all("'$.%s'"%field in merge for field in ('profile','feed','epoch')))

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
            # The original scoped checkpoint is validated independently; the
            # oversized untrusted phase payload must never be decoded.
            self.assertEqual(len(calls),3)
            self.assertTrue(all(size<16384 for size in calls))
        executor.rows=[]
        _,next_epoch=self.original('A',epoch='next-epoch')
        second=DeltaAttemptStore(executor,Policy(),'c.s.attempt','attempt-uuid',original_request=next_epoch)
        with store.session(object()) as session:session.append(original,'prepared')
        with second.session(object()) as session:session.append(next_epoch,'prepared')
        self.assertEqual(len(executor.rows),2)

    def test_pure_complete_phase_inventory_preserves_scope_and_prefix_rules(self):
        executor=Executor();_,request=self.original('A')
        store=DeltaAttemptStore(executor,Policy(),'c.s.attempt','attempt-uuid',original_request=request)
        with store.session(object()) as session:
            prepared=session.append(request,'prepared',plan_json='{}')
            applying=session.append(request,'applying')
            applied=session.append(request,'applied',result_json='{}')
        rows=[{k:row[k] for k in ('phase','request_digest','payload_json','payload_digest')}
              for row in executor.rows]
        self.assertEqual(validate_source_qualified_phase_inventory(
            request['stream'],request['batch_id'],rows,request),(prepared,applying,applied))
        with self.assertRaises(AttemptStoreError):
            validate_source_qualified_phase_inventory(request['stream'],request['batch_id'],[rows[0],rows[2]],request)
        with self.assertRaises(AttemptStoreError):
            validate_source_qualified_phase_inventory(request['stream'],request['batch_id'],rows+[rows[-1]],request)
        _,foreign_request=self.original('B')
        other=Executor();foreign_store=DeltaAttemptStore(other,Policy(),'c.s.attempt','attempt-uuid',
            original_request=foreign_request)
        with foreign_store.session(object()) as session: session.append(foreign_request,'prepared')
        foreign_rows=[{k:row[k] for k in ('phase','request_digest','payload_json','payload_digest')}
                      for row in other.rows]
        self.assertEqual(validate_source_qualified_phase_inventory(request['stream'],request['batch_id'],
            rows+foreign_rows,request),(prepared,applying,applied))
        malformed=[dict(row) for row in rows+foreign_rows]
        foreign_payload=json.loads(malformed[-1]['payload_json'])
        foreign_payload['request']['source_checkpoint_json']='{}'
        malformed[-1]['payload_json']=json.dumps(foreign_payload,sort_keys=True,separators=(',',':'))
        malformed[-1]['payload_digest']=hashlib.sha256(malformed[-1]['payload_json'].encode()).hexdigest()
        with self.assertRaises(AttemptStoreError):
            validate_source_qualified_phase_inventory(request['stream'],request['batch_id'],malformed,request)
        changed=[dict(row) for row in rows]
        row=changed[-1]
        payload=json.loads(row['payload_json']);payload['plan_json']='{"changed":true}'
        row['payload_json']=json.dumps(payload,sort_keys=True,separators=(',',':'))
        row['payload_digest']=hashlib.sha256(row['payload_json'].encode()).hexdigest()
        with self.assertRaises(AttemptStoreError):
            validate_source_qualified_phase_inventory(request['stream'],request['batch_id'],changed,request)
        with self.assertRaises(AttemptStoreError):
            validate_source_qualified_phase_inventory(request['stream'],request['batch_id'],rows,request,maximum_rows=1)

if __name__=='__main__': unittest.main()
