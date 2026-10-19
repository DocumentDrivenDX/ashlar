# SPDX-License-Identifier: MIT OR Apache-2.0
"""Original uncertain protected readback; inert PG/native ports, no native claim."""
import unittest
from unittest.mock import patch
from ashlar_host.ack import AckError,AckScope,AckOutcomeUncertain,receipt_bytes
from ashlar_host.ack_sessions import OutboxAckRegistration,RegisteredOutboxAcks
from ashlar_host.source_sessions import OutboxSourceRegistration
from test_protected_outbox_ack import Harness,FakeSession


class Tests(unittest.TestCase):
    def test_original_response_loss_fresh_observe_without_second_write_and_foreign_scope_refuses(self):
        h=Harness()
        h.scope=AckScope(h.scope.service_schema,h.scope.scope_id,h.scope.installation_id,h.scope.consumer,h.scope.feed,h.scope.epoch)
        source=OutboxSourceRegistration(h.scope,'ashlar_ack_source_test','a'*64,h.factory)
        owner=RegisteredOutboxAcks((OutboxAckRegistration(source,h.factory,h.port.policy),))
        ports={'context':h.context,'pins':h.port.pins,'publication_backend':h.backend,
            'supported_profiles':['ashlar-delta/0.3'],'supported_revisions':{'source':['r1']}}
        with patch('ashlar_host.ack.Session',FakeSession):
            h.unknown_commit=True
            with self.assertRaises(AckOutcomeUncertain)as result:
                owner.acknowledge(h.request_bytes,h.manifest_bytes,h.vector,**ports)
            uncertain=result.exception;h.unknown_commit=False
            self.assertEqual(owner.reconcile_uncertain(uncertain,h.vector,**ports),receipt_bytes(h.scope,h.request_bytes,h.manifest_bytes)[3])
            self.assertEqual([connection.writes for connection in h.connections],[1,0])
            self.assertTrue(all(connection.closed for connection in h.connections))
            foreign=AckScope(h.scope.service_schema,h.scope.scope_id,h.scope.installation_id,'different-consumer',h.scope.feed,h.scope.epoch)
            wrong=AckOutcomeUncertain(h.request_bytes,h.manifest_bytes,foreign,commit_attempted=True,commit_observed=False)
            with self.assertRaisesRegex(AckError,'registered ACK scope'):
                owner.reconcile_uncertain(wrong,h.vector,**ports)
            self.assertEqual(len(h.connections),2)
            second_scope=AckScope(h.scope.service_schema,h.scope.scope_id,h.scope.installation_id,h.scope.consumer,'other-feed',h.scope.epoch)
            second_source=OutboxSourceRegistration(second_scope,'ashlar_ack_source_test','a'*64,h.factory)
            two=RegisteredOutboxAcks((*owner.registrations,OutboxAckRegistration(second_source,h.factory,h.port.policy)))
            wrong=AckOutcomeUncertain(h.request_bytes,h.manifest_bytes,second_scope,commit_attempted=True,commit_observed=False)
            with self.assertRaisesRegex(AckError,'outbox scope'):
                two.reconcile_uncertain(wrong,h.vector,**ports)
            self.assertEqual(len(h.connections),2)
            h.stored=None
            with self.assertRaises(AckError):owner.reconcile_uncertain(uncertain,h.vector,**ports)
            self.assertEqual(h.connections[-1].writes,0)
