from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
import base64
import hashlib
import json
import unittest
from ashlar.native import SQLResult
from ashlar.source import jsonl_batches
from ashlar.staging import DeltaBatchStage,StagingError,batch_row

ROOT=Path(__file__).resolve().parents[1]
TABLE='catalog.schema.stage'
class Policy:
    @contextmanager
    def writer(self,table,uuid,context):
        if context!='authorized':raise PermissionError('Writer denied')
        yield
class Executor:
    def __init__(self):self.calls=[];self.row=None;self.uuid='stage-uuid'
    def query(self,sql,parameters):
        self.calls.append((sql,parameters))
        if sql.startswith('DESCRIBE'):return SQLResult([{'id':self.uuid}])
        if sql.startswith('MERGE'):
            self.row=json.loads(parameters['payload']);return SQLResult([])
        return SQLResult([self.row])
class StageTests(unittest.TestCase):
    def batch(self):
        raw=(ROOT/'examples/end-to-end/source.jsonl').read_bytes()
        return list(jsonl_batches(raw.splitlines(keepends=True),feed='f',epoch='e'))[0]
    def test_exact_byte_roundtrip_and_bound_values(self):
        batch=self.batch();row=batch_row(batch);payload=json.loads(row['batch_json'])
        self.assertEqual(base64.b64decode(payload['records'][0]['raw_base64']),batch.records[0].raw)
        self.assertEqual(row['batch_digest'],hashlib.sha256(row['batch_json'].encode()).hexdigest())
        ex=Executor();stage=DeltaBatchStage(ex,Policy(),TABLE,'stage-uuid')
        receipt=stage.stage(batch,context='authorized')
        self.assertEqual(receipt.batch_digest,row['batch_digest'])
        self.assertEqual(len(ex.calls),4)
        self.assertNotIn('item-1-create',ex.calls[1][0])
        self.assertIn('item-1-create',ex.calls[1][1]['payload'])
    def test_denial_before_native_effects(self):
        ex=Executor()
        with self.assertRaises(PermissionError):DeltaBatchStage(ex,Policy(),TABLE,'stage-uuid').stage(self.batch(),context='denied')
        self.assertFalse(ex.calls)
    def test_nonconforming_policy_refuses_before_native(self):
        class BadPolicy:
            @contextmanager
            def writer(self,*args):yield False
        ex=Executor()
        with self.assertRaises(StagingError):DeltaBatchStage(ex,BadPolicy(),TABLE,'stage-uuid').stage(self.batch(),context='authorized')
        self.assertFalse(ex.calls)
    def test_table_replacement_before_write_refuses(self):
        ex=Executor();ex.uuid='replacement'
        with self.assertRaises(StagingError):DeltaBatchStage(ex,Policy(),TABLE,'stage-uuid').stage(self.batch(),context='authorized')
        self.assertEqual(len(ex.calls),1)
    def test_forged_batch_metadata_refuses(self):
        batch=self.batch()
        for bad in [replace(batch,cursor_after='999'),replace(batch,records_sha256='0'*64),replace(batch,batch_id='different')]:
            with self.assertRaises(StagingError):batch_row(bad)
    def test_mismatched_readback_refuses(self):
        class BadExecutor(Executor):
            def query(self,sql,p):
                result=super().query(sql,p)
                if sql.startswith('SELECT'):return SQLResult([])
                return result
        with self.assertRaises(StagingError):DeltaBatchStage(BadExecutor(),Policy(),TABLE,'stage-uuid').stage(self.batch(),context='authorized')
if __name__=='__main__':unittest.main()
