"""Public receipt ACK controls; no SDK installation or native engine."""
import json,hashlib,unittest
from contextlib import contextmanager
from dataclasses import make_dataclass
from types import SimpleNamespace
from unittest.mock import patch
import test_truss_feed_sessions as fixtures
from ashlar_host.truss_feed_sessions import TrussFeedSessions,TrussSessionError
from ashlar_host.ack import AckOutcomeUncertain
Receipt=make_dataclass('Receipt',[(k,object)for k in ('installation_id','source_epoch','consumer_id','registration_id','prior','next','feed_sha256','destination_id','request_sha256','publication_id','receipt_bytes','receipt_sha256')])
Acknowledged=make_dataclass('Acknowledged',[('receipt',object),('checkpoint',object),('outcome',str)])
Reconciled=make_dataclass('Reconciled',[('receipt',object),('checkpoint',object),('outcome',str)])

class Tests(unittest.TestCase):
    def fixture(self,loss=False,mismatch=False):
        old,request,context,events,connection,pins,_=fixtures.Tests().setup_case()
        with patch('ashlar_host.truss_feed_sessions._sdk',return_value=(None,None,None,None,fixtures.Page,fixtures.Acknowledged)),patch('ashlar_host.truss_feed_sessions.require_truss_receipt_sdk',return_value=(Receipt,Acknowledged,Reconciled)):
            owner=TrussFeedSessions(old.page,old.feed,old.connection_factory,context,destination_receipts=True,destination_id='actual-delta-destination')
        retained=[];cursor=json.loads(owner.page.prior.content)
        @contextmanager
        def assembly(connection,readonly):
            def acknowledge(tx,page,**kwargs):
                events.append('receipt-ack')
                receipt=Receipt(cursor['installationId'],cursor['sourceEpoch'],page.consumer_id,page.registration_id,page.prior,page.next,page.sha256,kwargs['destination_id'],kwargs['request_sha256'],kwargs['publication_id'],kwargs['receipt_bytes'],hashlib.sha256(kwargs['receipt_bytes']).hexdigest())
                retained.append(receipt);return Acknowledged(receipt,page.next,'advanced')
            def reconcile(tx,page,**kwargs):
                events.append('receipt-observe');receipt=retained[0]
                if mismatch:receipt=Receipt(**{**receipt.__dict__,'publication_id':'foreign'})
                later=json.loads(page.next.content);later['nextXid']=str(int(later['nextXid'])+1)
                return Reconciled(receipt,fixtures.Cursor(json.dumps(later).encode()),'matched')
            yield SimpleNamespace(acknowledge_feed_with_receipt_in_transaction=acknowledge,reconcile_feed_ack_in_transaction=reconcile),object()
        if loss:
            def commit():events.append('commit');raise OSError('actual commit response lost')
            connection.commit=commit
        vector=SimpleNamespace(authority='actual-private-owner',scope_kind='installation',scope_id='actual-scope',custody_digest='original-digest',targets={'table':('uuid',1)})
        ports={'context':context,'pins':pins,'publication_backend':object(),'supported_profiles':[],'supported_revisions':{}}
        raw=json.dumps(request).encode();manifest=b'{"publication_id":"original-publication"}'
        return owner,raw,manifest,vector,ports,events,assembly,retained
    def invoke(self,owner,raw,manifest,vector,ports,assembly,reconcile=False):
        with patch('ashlar_host.truss_feed_sessions.require_truss_receipt_sdk',return_value=(Receipt,Acknowledged,Reconciled)),patch('ashlar_host.truss_feed_sessions._assembly',side_effect=assembly),patch('ashlar_host.truss_feed_sessions._value',side_effect=lambda x:x),patch.object(TrussFeedSessions,'_resolve')as resolve:
            result=(owner.reconcile if reconcile else owner.acknowledge)(raw,manifest,vector,**ports)
            self.assertEqual(resolve.call_count,2);return result
    def test_original_receipt_retained_after_destination_gates(self):
        owner,raw,manifest,vector,ports,events,assembly,retained=self.fixture()
        result=self.invoke(owner,raw,manifest,vector,ports,assembly)
        self.assertTrue(result['caller_commit_confirmed']);self.assertTrue(result['source_receipt_retained'])
        self.assertEqual(retained[0].request_sha256,hashlib.sha256(raw).hexdigest())
        self.assertEqual(events.count('receipt-ack'),1)
    def test_commit_loss_fresh_receipt_observation_has_no_second_write(self):
        owner,raw,manifest,vector,ports,events,assembly,retained=self.fixture(loss=True)
        with self.assertRaises(AckOutcomeUncertain):self.invoke(owner,raw,manifest,vector,ports,assembly)
        result=self.invoke(owner,raw,manifest,vector,ports,assembly,reconcile=True)
        self.assertTrue(result['receipt_observed']);self.assertEqual(events.count('receipt-ack'),1);self.assertEqual(events.count('receipt-observe'),1);self.assertEqual(events.count('connect'),2)
    def test_foreign_retained_receipt_cannot_be_reconciled_as_success(self):
        owner,raw,manifest,vector,ports,events,assembly,retained=self.fixture(loss=True,mismatch=True)
        with self.assertRaises(AckOutcomeUncertain):self.invoke(owner,raw,manifest,vector,ports,assembly)
        with self.assertRaisesRegex(TrussSessionError,'receipt differs'):self.invoke(owner,raw,manifest,vector,ports,assembly,reconcile=True)
        self.assertEqual(events.count('receipt-ack'),1)
