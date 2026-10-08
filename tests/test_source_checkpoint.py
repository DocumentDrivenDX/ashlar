from dataclasses import replace
import hashlib
import json
from pathlib import Path
import unittest
from ashlar.outbox import OutboxTransaction,publish_outbox_transaction
from ashlar.source import jsonl_batches
from ashlar.source_checkpoint import outbox_checkpoint,CheckpointError
from ashlar.attempt_store import _request_digest,AttemptStoreError
from test_publisher import Backend

ROOT=Path(__file__).resolve().parents[1]

class CheckpointTests(unittest.TestCase):
    def transaction(self):
        raw=(ROOT/'examples/end-to-end/source.jsonl').read_bytes()
        batch=tuple(jsonl_batches(raw.splitlines(keepends=True),feed='native',epoch='one'))[0]
        return OutboxTransaction('ashlar-postgresql-outbox/0.1','native','one','10','11',hashlib.sha256(raw).hexdigest(),batch)
    def publish(self,backend,transaction):
        return publish_outbox_transaction(backend,'stream',transaction,predecessor='old',
            schema_revisions_json='{"native":"1"}',context='authorized')
    def test_native_checkpoint_retained_and_exact_replay_uses_same_attempt(self):
        class Capture(Backend):
            def prepare(self,stream,request):
                self.original=dict(request)
                return super().prepare(stream,request)
        transaction=self.transaction();backend=Capture()
        self.publish(backend,transaction)
        self.assertEqual(backend.original['source_checkpoint_json'],outbox_checkpoint(transaction))
        self.assertEqual(_request_digest(backend.original),backend.original['request_digest'])
        self.publish(backend,transaction)
        self.assertEqual(backend.calls.count('apply'),1)
        # Identical inner bytes at a different native source position are a
        # different durable intent, not a successful committed replay.
        with self.assertRaises(ValueError):
            self.publish(backend,replace(transaction,previous='11',position='12'))
    def test_invalid_outer_binding_refuses_before_writer(self):
        for fields in [dict(payload_digest='0'*64),dict(position='12'),dict(previous='010'),
                       dict(feed='other'),dict(profile='unknown')]:
            backend=Backend()
            with self.assertRaises(CheckpointError):self.publish(backend,replace(self.transaction(),**fields))
            self.assertEqual(backend.calls,[])
    def test_rehashed_phase_request_cannot_hide_changed_native_source_binding(self):
        class Capture(Backend):
            def prepare(self,stream,request):
                self.original=dict(request)
                return super().prepare(stream,request)
        backend=Capture();self.publish(backend,self.transaction())
        for fields in [dict(feed='other'),dict(payload_digest='0'*64),dict(batch_id='other'),dict(extra='x')]:
            request=dict(backend.original);value=json.loads(request['source_checkpoint_json']);value.update(fields)
            request['source_checkpoint_json']=json.dumps(value)
            content={k:v for k,v in request.items() if k!='request_digest'}
            request['request_digest']=hashlib.sha256(json.dumps(content,sort_keys=True,separators=(',',':')).encode()).hexdigest()
            with self.assertRaises(AttemptStoreError):_request_digest(request)
