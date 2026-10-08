from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
import unittest
from ashlar.publisher import Attempt,PublicationError,publish_batch
from ashlar.source import jsonl_batches

ROOT=Path(__file__).resolve().parents[1]
class Backend:
    def __init__(self):self.attempt=None;self.calls=[];self.visible='old';self.progress='0';self.fail=None;self.native_descriptor=None
    @contextmanager
    def writer(self,stream,context):
        if context!='authorized':raise PermissionError('Denied')
        yield
    def observe(self,*args):self.calls.append('observe');return self.attempt
    def prepare(self,stream,r):self.calls.append('prepare');self.attempt=Attempt(r['request_digest'],'prepared');return self.attempt
    def start_apply(self,*args):self.calls.append('start');self.attempt=replace(self.attempt,phase='applying')
    def apply(self,*args):
        self.calls.append('apply')
        if self.fail=='apply':raise RuntimeError('Native outcome unknown; original handle retained')
        return {'actual_pin':'original'}
    def recover_apply(self,*args):self.calls.append('recover');return {'actual_pin':'original'}
    def retain_applied(self,s,b,d,result):self.calls.append('retain');self.attempt=Attempt(d,'applied',result);return self.attempt
    def validate(self,*args):
        self.calls.append('validate')
        if self.fail=='validate':raise RuntimeError('Incomplete parity')
    def start_commit(self,*args):self.calls.append('start-commit');self.attempt=replace(self.attempt,phase='committing')
    def recover_commit(self,s,r,result,context):
        self.calls.append('recover-commit')
        if self.native_descriptor is None:raise RuntimeError('Original native handle still unavailable')
        self.attempt=Attempt(r['request_digest'],'committed',result,self.native_descriptor);return self.attempt
    def commit(self,s,r,result):
        self.calls.append('commit');self.visible='new';self.native_descriptor='new'
        if self.fail=='commit':raise RuntimeError('Committed native outcome unknown')
        self.attempt=Attempt(r['request_digest'],'committed',result,'new');return self.attempt
    def acknowledge(self,*args):
        self.calls.append('ack')
        if self.fail=='ack':raise RuntimeError('Acknowledgement lost')
        self.progress='complete'

class PublisherTests(unittest.TestCase):
    def batch(self):return list(jsonl_batches((ROOT/'examples/end-to-end/source.jsonl').read_bytes().splitlines(keepends=True),feed='f',epoch='e'))[0]
    def publish(self,b,**kw):return publish_batch(b,'stream',self.batch(),predecessor=kw.get('predecessor','old'),schema_revisions_json='{"f":"1"}',context=kw.get('context','authorized'))
    def test_order_and_exact_replay(self):
        b=Backend();self.assertEqual(self.publish(b),'new')
        self.assertEqual(b.calls,['observe','prepare','start','apply','retain','validate','start-commit','commit','ack'])
        self.publish(b);self.assertEqual(b.calls[-2:],['observe','ack'])
        self.assertEqual(b.calls.count('apply'),1)
    def test_unknown_apply_recovers_original_without_reexecution(self):
        b=Backend();b.fail='apply'
        with self.assertRaises(RuntimeError):self.publish(b)
        self.assertEqual((b.visible,b.progress),('old','0'))
        b.fail=None;self.publish(b)
        self.assertEqual(b.calls.count('apply'),1);self.assertEqual(b.calls.count('recover'),1)
    def test_incomplete_validation_keeps_prior_publication_and_progress(self):
        b=Backend();b.fail='validate'
        with self.assertRaises(RuntimeError):self.publish(b)
        self.assertEqual((b.visible,b.progress),('old','0'))
        b.fail=None;self.publish(b)
        self.assertEqual(b.calls.count('apply'),1)
    def test_lost_commit_or_ack_reuses_original_descriptor(self):
        for failure in ['commit','ack']:
            b=Backend();b.fail=failure
            with self.assertRaises(RuntimeError):self.publish(b)
            self.assertEqual(b.progress,'0')
            b.fail=None;self.publish(b)
            self.assertEqual(b.calls.count('commit'),1)
            self.assertEqual(b.calls.count('apply'),1)
    def test_missing_result_and_malformed_revision_refuse(self):
        b=Backend();self.publish(b)
        b.attempt=replace(b.attempt,phase='applied',apply_result=None)
        with self.assertRaises(PublicationError):self.publish(b)
        for text in ['{}','{"f":1}','{"f":"1","f":"2"}']:
            with self.assertRaises(PublicationError):publish_batch(Backend(),'stream',self.batch(),predecessor='old',schema_revisions_json=text,context='authorized')
    def test_changed_intent_and_denied_writer_refuse(self):
        b=Backend();self.publish(b)
        with self.assertRaises(PublicationError):self.publish(b,predecessor='different')
        b=Backend()
        with self.assertRaises(PermissionError):self.publish(b,context='denied')
        self.assertFalse(b.calls)
if __name__=='__main__':unittest.main()
