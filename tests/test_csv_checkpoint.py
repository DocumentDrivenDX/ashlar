from dataclasses import replace
import hashlib,json,unittest
from ashlar.csv_source import csv_batches
from ashlar.source_checkpoint import csv_checkpoint,validate_source_checkpoint,bind_source_descriptor,bind_outbox_descriptor,CheckpointError
from ashlar.publisher import publish_batch
from ashlar.attempt_store import _request_digest,AttemptStoreError
from ashlar.publication import Descriptor
from test_publisher import Backend

class Capture(Backend):
    def prepare(self,stream,request):
        self.original=dict(request)
        return super().prepare(stream,request)
class CSVCheckpointTests(unittest.TestCase):
    def batch(self):return next(csv_batches([b'id,entity_version,operation,label\n',b'1,1,create,x\n'],feed='csv',epoch='immutable',source_system='s',schema_revision='r',type_id='17',properties={'label':'23'}))
    def publish(self,b,batch,text):return publish_batch(b,'csv-stream',batch,predecessor='prior',schema_revisions_json='{"csv":"r"}',context='authorized',source_checkpoint_json=text)
    def test_original_csv_outer_progress_survives_durable_intent_and_replay(self):
        batch=self.batch();text=csv_checkpoint(batch);b=Capture()
        self.publish(b,batch,text);self.publish(b,batch,text)
        self.assertEqual(b.calls.count('apply'),1)
        self.assertEqual(_request_digest(b.original),b.original['request_digest'])
        progress=json.loads(text);self.assertEqual((progress['previous'],progress['position']),('0','1'))
        self.assertNotEqual(batch.cursor_after,'1')
        d=Descriptor('p','selected',{}, {'csv':'r'},{'csv':progress},{'request_digest':b.original['request_digest']},{})
        self.assertIsNone(bind_source_descriptor(b.original,d,expected_publication_id='p'))
        with self.assertRaises(CheckpointError):bind_outbox_descriptor(b.original,d,expected_publication_id='p')
    def test_changed_csv_position_epoch_digest_or_profile_refuses_before_writer(self):
        batch=self.batch();original=json.loads(csv_checkpoint(batch))
        for delta in [{'position':'2'},{'epoch':'other'},{'payload_digest':'0'*64},{'profile':'unknown'},{'extra':'x'},{'position':1}]:
            b=Capture()
            with self.assertRaises(CheckpointError):self.publish(b,batch,json.dumps(dict(original,**delta)))
            self.assertEqual(b.calls,[])
    def test_rehashed_retained_request_cannot_hide_source_checkpoint_change(self):
        batch=self.batch();b=Capture();self.publish(b,batch,csv_checkpoint(batch))
        request=dict(b.original);value=json.loads(request['source_checkpoint_json']);value['previous']='1'
        request['source_checkpoint_json']=json.dumps(value)
        request['request_digest']=hashlib.sha256(json.dumps({k:v for k,v in request.items() if k!='request_digest'},sort_keys=True,separators=(',',':')).encode()).hexdigest()
        with self.assertRaises(AttemptStoreError):_request_digest(request)
if __name__=='__main__':unittest.main()
