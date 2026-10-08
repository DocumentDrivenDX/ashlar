from dataclasses import replace
import hashlib
import json
from pathlib import Path
import unittest
from ashlar.publication import Descriptor
from ashlar.source import jsonl_batches
from ashlar.outbox import OutboxTransaction
from ashlar.staging import batch_row
from ashlar.source_checkpoint import outbox_checkpoint,bind_outbox_descriptor,CheckpointError
ROOT=Path(__file__).resolve().parents[1]

class DescriptorCheckpointTests(unittest.TestCase):
    def pair(self):
        raw=(ROOT/'examples/end-to-end/source.jsonl').read_bytes()
        batch=tuple(jsonl_batches(raw.splitlines(keepends=True),feed='native',epoch='one'))[0]
        transaction=OutboxTransaction('ashlar-postgresql-outbox/0.1','native','one','10','11',hashlib.sha256(raw).hexdigest(),batch)
        row=batch_row(batch)
        request={'stream':'stream','batch_id':batch.batch_id,'predecessor':'prior',
                 'schema_revisions_json':'{"source":"1"}','source_batch_json':row['batch_json'],
                 'source_batch_digest':row['batch_digest'],'source_checkpoint_json':outbox_checkpoint(transaction)}
        request['request_digest']=hashlib.sha256(json.dumps(request,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        progress={'native':json.loads(request['source_checkpoint_json']),'other':{'position':'unmodified'}}
        descriptor=Descriptor('published','selected',{}, {'source':'1'},progress,
                              {'request_digest':request['request_digest']},{})
        return request,descriptor
    def test_exact_original_source_binding_preserves_other_progress(self):
        request,descriptor=self.pair()
        self.assertIsNone(bind_outbox_descriptor(request,descriptor,expected_publication_id='published'))
        self.assertEqual(descriptor.source_progress['other'],{'position':'unmodified'})
    def test_wrong_epoch_position_payload_schema_identity_or_intent_refuses(self):
        request,descriptor=self.pair()
        for fields in [dict(position=11),dict(position='999'),dict(epoch='other'),dict(payload_digest='0'*64),dict(batch_id='other')]:
            progress=dict(descriptor.source_progress);progress['native']=dict(progress['native'],**fields)
            with self.assertRaises(CheckpointError):bind_outbox_descriptor(request,replace(descriptor,source_progress=progress),expected_publication_id='published')
        for changed in [replace(descriptor,publication_id='other'),replace(descriptor,revisions={'source':'3'}),replace(descriptor,validation_report={'request_digest':'0'*64})]:
            with self.assertRaises(CheckpointError):bind_outbox_descriptor(request,changed,expected_publication_id='published')
