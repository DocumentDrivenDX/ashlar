# SPDX-License-Identifier: MIT OR Apache-2.0
"""Real SQLite/memo with inert native owner; remove only duplicate identity."""
from contextlib import nullcontext
import unittest
from unittest.mock import patch
import test_local_delta_effect_plan as harness
from ashlar_host.delta_custody import LocalDeltaError
from ashlar_host.immutable_snapshot_memo import immutable_snapshot_memo

class Tests(unittest.TestCase):
    def fixture(self):
        host=harness.Tests();host.setUp();self.addCleanup(host.doCleanups)
        owner=host.transport;owner.mutation('original',harness.SQL,{'id':'1'},intent_digest='b'*64,context='held')
        return host,owner
    def inspect(self,owner):return owner.inspect_committed('original',harness.SQL,{'id':'1'},intent_digest='b'*64,context='held')
    def test_exact_memo_delegates_identity_no_memo_original_order_unchanged(self):
        host,owner=self.fixture();before=owner.db.execute('SELECT * FROM local_operation').fetchall();writes=host.engine.writes
        for enabled in (False,True):
            events=[];queries=[];detail=owner._detail;history=owner.original_history;snapshot=owner._snapshot
            def observe(label,method):
                def call(*args,**kwargs):events.append(label);return method(*args,**kwargs)
                return call
            owner.db.set_trace_callback(queries.append)
            with patch.object(owner,'_detail',side_effect=observe('detail',detail)),patch.object(owner,'original_history',side_effect=observe('history',history)),patch.object(owner,'_snapshot',side_effect=observe('snapshot',snapshot)),patch.object(owner.policy,'admit',wraps=owner.policy.admit)as admit,patch.object(owner,'_profile',wraps=owner._profile)as profile:
                with immutable_snapshot_memo(owner)if enabled else nullcontext():
                    self.inspect(owner);self.inspect(owner)
                self.assertEqual((admit.call_count,profile.call_count),(4,4))
            owner.db.set_trace_callback(None)
            if not enabled:self.assertEqual(events,['detail','history','snapshot','history','snapshot']*2)
            else:self.assertEqual(events,['detail','history','detail','snapshot','detail','detail','snapshot','detail','detail','snapshot','detail','detail','snapshot','detail','history','detail'])
            self.assertEqual(sum(q.startswith('SELECT intent,intent_sha,before_version,state,receipt FROM local_operation WHERE operation=')for q in queries),4)
        self.assertEqual(before,owner.db.execute('SELECT * FROM local_operation').fetchall());self.assertEqual(host.engine.writes,writes)
    def test_memo_opening_closing_uuid_and_registration_drift_refuse(self):
        for identity in ('uuid','registration'):
            for at in (1,2):
                with self.subTest(identity=identity,at=at):
                    host,owner=self.fixture();detail=owner._detail;native=host.engine.sql;calls=[];drift=[False]
                    def sql(statement,args=None):
                        if drift[0]and identity=='uuid'and statement.startswith('DESCRIBE DETAIL'):return harness.SimpleNamespace(first=lambda:harness.Row(id='replacement',properties=host.engine.properties))
                        return native(statement,args)
                    def checked(target):
                        calls.append(target)
                        if len(calls)==at:
                            drift[0]=True
                            if identity=='registration':host.engine.properties['ashlar.local.registry.sha256']='changed'
                        return detail(target)
                    with self.assertRaises(LocalDeltaError):
                        with patch.object(host.engine,'sql',side_effect=sql),patch.object(owner,'_detail',side_effect=checked):
                            with immutable_snapshot_memo(owner):self.inspect(owner)
                    self.assertGreaterEqual(len(calls),at);self.assertIsNone(owner._immutable_snapshot_memo)
    def test_capacity_fallback_and_cancellation_retained_history_close(self):
        host,owner=self.fixture()
        with patch.object(owner,'original_history',wraps=owner.original_history)as history:
            with immutable_snapshot_memo(owner)as memo,patch.object(memo,'_fits',return_value=False):self.inspect(owner)
            self.assertEqual(history.call_count,4)
        primary=KeyboardInterrupt('original cancellation')
        with patch.object(owner,'original_history',wraps=owner.original_history)as history,patch.object(owner,'_detail',wraps=owner._detail)as detail:
            with self.assertRaises(KeyboardInterrupt)as caught:
                with immutable_snapshot_memo(owner):
                    self.inspect(owner);host.engine.history.append({'version':99,'operation':'foreign'});raise primary
            self.assertIs(caught.exception,primary);self.assertEqual(history.call_count,2)
            self.assertEqual(detail.call_count,6)
        self.assertIsNone(owner._immutable_snapshot_memo)
