import base64,hashlib,json,sqlite3,unittest
from contextlib import contextmanager,closing
from dataclasses import dataclass,replace
from types import SimpleNamespace,MappingProxyType
from unittest.mock import patch
import test_truss_graph_sql as graph_cases
from ashlar.staging import batch_row
from ashlar.source_checkpoint import truss_checkpoint
from ashlar_host.truss_feed_sessions import TrussFeedSessions
from ashlar_host.ack import AckOutcomeUncertain

@dataclass(frozen=True)
class Cursor:content:bytes

@dataclass(frozen=True)
class Page:
    content:bytes;sha256:str;prior:Cursor;next:Cursor
    consumer_id:str='consumer';registration_id:str='registration';writer_xid:str='42';outcome:str='transaction';safe_watermark_xid:str='43'

@dataclass(frozen=True)
class Acknowledged:
    checkpoint:Cursor;outcome:str='advanced'

class Tests(unittest.TestCase):
    def setup_case(self):
        batch,_=graph_cases.Tests().batch();row=batch_row(batch)
        page=Page(batch.begin,hashlib.sha256(batch.begin).hexdigest(),Cursor(base64.b64decode(batch.cursor_before)),Cursor(base64.b64decode(batch.cursor_after)))
        request={'stream':'notes','batch_id':batch.batch_id,'predecessor':'origin','schema_revisions_json':'{"notes":"1"}',
            'source_batch_json':row['batch_json'],'source_batch_digest':row['batch_digest'],'source_checkpoint_json':truss_checkpoint(batch)}
        request['request_digest']=hashlib.sha256(json.dumps(request,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        context=object();events=[]
        class Connection:
            def commit(self):events.append('commit')
            def rollback(self):events.append('rollback')
            def close(self):events.append('close')
        connection=Connection()
        def factory(supplied):
            self.assertIs(supplied,context);events.append('connect');return connection
        with patch('ashlar_host.truss_feed_sessions._sdk',return_value=(None,None,None,None,Page,Acknowledged)):
            owner=TrussFeedSessions(page,batch.feed,factory,context)
        class Pins:
            @contextmanager
            def hold(self,vector,*,context):
                events.append('pins-open')
                try:yield
                finally:events.append('pins-close')
        @contextmanager
        def assembly(connection,readonly):
            self.assertFalse(readonly);events.append('assembly-open')
            def acknowledge(tx,selected):
                self.assertIs(selected,page);events.append('ack');return Acknowledged(page.next)
            try:yield SimpleNamespace(acknowledge_feed_in_transaction=acknowledge),object()
            finally:events.append('assembly-close')
        return owner,request,context,events,connection,Pins(),assembly

    def test_destination_failure_prevents_source_ack(self):
        owner,request,context,events,_,pins,assembly=self.setup_case()
        with patch.object(TrussFeedSessions,'_resolve',side_effect=ValueError('publication unavailable')):
            with self.assertRaisesRegex(ValueError,'publication unavailable'):
                owner.acknowledge(json.dumps(request).encode(),b'{"publication_id":"p"}',object(),context=context,pins=pins,
                    publication_backend=object(),supported_profiles=['profile'],supported_revisions={'notes':['1']})
        self.assertEqual(events,['pins-open','pins-close'])

    def test_driver_requires_matching_truss_source_and_ack_owner(self):
        from ashlar_host.driver import NativeDriver,progress_union
        owner,_,context,_,_,_,_=self.setup_case()
        facts={'profile':'ashlar-truss-note-semantic-admission/0.1','qualification':'Unit semantic port',
            'feed':owner.feed,'epoch':owner.batch().epoch,'source_transaction_sha256':owner.page.sha256}
        semantic=SimpleNamespace(metadata=lambda:facts,admit=lambda change:None)
        with closing(sqlite3.connect(':memory:'))as db:
            db.execute('CREATE TABLE local_plan (value TEXT)')
            transport=SimpleNamespace(db=db)
            with self.assertRaises(PermissionError):
                NativeDriver(transport,object(),context,{}, {},(),{},source_admission=semantic,source_sessions=owner)
            driver=NativeDriver(transport,object(),context,{}, {},(),{},source_admission=semantic,source_sessions=owner,ack_sessions=owner)
            self.assertIs(driver.source_sessions,owner);self.assertIs(driver.ack_sessions,owner)
        checkpoint=json.loads(truss_checkpoint(owner.batch()))
        self.assertEqual(progress_union({},checkpoint),{owner.feed:checkpoint})
        checkpoint['previous']=checkpoint['position']
        with self.assertRaises(ValueError):progress_union({},checkpoint)

    def test_same_transaction_can_have_a_new_safe_frontier_observation(self):
        owner,request,context,events,_,_,_=self.setup_case()
        with patch.object(TrussFeedSessions,'_read',return_value=replace(owner.page,safe_watermark_xid='90')):
            owner.admit(request,context=context)
        self.assertEqual(events,['connect','rollback','close'])

    def test_public_ack_commit_follows_both_destination_gates(self):
        owner,request,context,events,_,pins,assembly=self.setup_case()
        owner._request(MappingProxyType(request),context)
        with patch.object(TrussFeedSessions,'_resolve',side_effect=lambda *args:events.append('resolve')),\
             patch('ashlar_host.truss_feed_sessions._assembly',assembly),\
             patch('ashlar_host.truss_feed_sessions._sdk',return_value=(None,None,None,None,Page,Acknowledged)),\
             patch('ashlar_host.truss_feed_sessions._value',side_effect=lambda value:value):
            answer=owner.acknowledge(json.dumps(request).encode(),b'{"publication_id":"p"}',object(),context=context,pins=pins,
                publication_backend=object(),supported_profiles=['profile'],supported_revisions={'notes':['1']})
        self.assertTrue(answer['caller_commit_confirmed'])
        self.assertEqual(events,['pins-open','resolve','connect','assembly-open','ack','resolve','assembly-close','commit','pins-close','rollback','close'])
        # Actual observed commit allows the same owner's pin-release admission;
        # the source is still authenticated through a fresh public read.
        with patch.object(TrussFeedSessions,'_read',return_value=SimpleNamespace(prior=owner.page.next)):
            owner.admit(request,context=context)
        self.assertEqual(events[-3:],['connect','rollback','close'])

    def test_fresh_owner_cannot_claim_an_advanced_checkpoint_as_its_ack(self):
        owner,request,context,events,_,pins,_=self.setup_case()
        with self.assertRaisesRegex(ValueError,'Original actual ACK attempt required'):
            owner.reconcile(json.dumps(request).encode(),b'{"publication_id":"p"}',object(),context=context,pins=pins,
                publication_backend=object(),supported_profiles=['profile'],supported_revisions={'notes':['1']})
        self.assertEqual(events,[])

    def test_commit_response_loss_reconciles_without_another_write(self):
        owner,request,context,events,connection,pins,assembly=self.setup_case()
        def lost():events.append('commit');raise OSError('lost response')
        connection.commit=lost
        ports={'context':context,'pins':pins,'publication_backend':object(),'supported_profiles':['profile'],'supported_revisions':{'notes':['1']}}
        with patch.object(TrussFeedSessions,'_resolve'),patch('ashlar_host.truss_feed_sessions._assembly',assembly),\
             patch('ashlar_host.truss_feed_sessions._sdk',return_value=(None,None,None,None,Page,Acknowledged)),\
             patch('ashlar_host.truss_feed_sessions._value',side_effect=lambda value:value):
            with self.assertRaises(AckOutcomeUncertain):owner.acknowledge(json.dumps(request).encode(),b'{"publication_id":"p"}',object(),**ports)
        events.clear()
        with patch.object(TrussFeedSessions,'_resolve'),patch.object(TrussFeedSessions,'_read',return_value=SimpleNamespace(prior=owner.page.next)):
            answer=owner.reconcile(json.dumps(request).encode(),b'{"publication_id":"p"}',object(),**ports)
        self.assertTrue(answer['checkpoint_observed']);self.assertNotIn('ack',events);self.assertNotIn('commit',events)
        self.assertEqual(events[-2:],['rollback','close'])
