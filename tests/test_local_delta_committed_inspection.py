"""Read-only native receipt controls with actual owned SQLite and inert engine."""
import unittest
from unittest.mock import patch
from contextlib import contextmanager
import test_local_delta_effect_plan as harness
from ashlar_host.delta_custody import LocalDeltaError, LocalDeltaUncertain

class Tests(unittest.TestCase):
    def setUp(self):
        self.host = harness.Tests(); self.host.setUp(); self.addCleanup(self.host.doCleanups)
        self.transport = self.host.transport
        self.sql = harness.SQL; self.params = {'id': '1'}; self.digest = 'b' * 64

    def inspect(self):
        return self.transport.inspect_committed('original', self.sql, self.params,
            intent_digest=self.digest, context='held')

    def commit(self):
        return self.transport.mutation('original', self.sql, self.params,
            intent_digest=self.digest, context='held')

    def test_exact_receipt_inspection_does_not_write_or_change_journal(self):
        self.commit(); before = self.transport.db.execute('SELECT * FROM local_operation').fetchall()
        writes = self.host.engine.writes
        proof = self.inspect()
        self.assertEqual(proof['operation'], 'original')
        self.assertEqual(self.host.engine.writes, writes)
        self.assertEqual(before, self.transport.db.execute('SELECT * FROM local_operation').fetchall())
        proof['snapshot']['rows'].append({'borrowed': True})
        self.assertEqual(self.inspect()['snapshot']['rows'], [])

    def test_inspection_independently_reobserves_and_closing_uuid_drift_refuses(self):
        self.commit()
        before = self.transport.db.execute('SELECT * FROM local_operation').fetchall()
        writes = self.host.engine.writes
        snapshot = self.transport._snapshot
        events = []
        original_sql = self.host.engine.sql
        drift = False
        def sql(statement, args=None):
            if statement.startswith('DESCRIBE DETAIL'):
                events.append('detail')
                if drift and events.count('detail') == 5:
                    return harness.SimpleNamespace(first=lambda: harness.Row(id='replacement-uuid'))
            if statement.startswith('DESCRIBE HISTORY'):
                events.append('history')
            return original_sql(statement, args)
        def held_snapshot(target, version):
            # Model the production snapshot's own fresh opening/closing checks;
            # inspect_committed must not replace either independent capture.
            self.transport._detail(target)
            try:
                events.append('snapshot')
                return snapshot(target, version)
            finally:
                self.transport._detail(target)
        admission = self.transport.policy.admit
        def admit(intent, context):
            events.append('admit')
            return admission(intent, context)
        with patch.object(self.host.engine, 'sql', side_effect=sql), patch.object(
                self.transport, '_snapshot', side_effect=held_snapshot), patch.object(
                self.transport.policy, 'admit', side_effect=admit):
            self.inspect()
            self.assertEqual(events, ['admit', 'detail', 'history', 'detail',
                'snapshot', 'detail', 'admit', 'history', 'detail', 'snapshot', 'detail'])
            events.clear(); drift = True
            with self.assertRaisesRegex(LocalDeltaError, 'UUID changed'):
                self.inspect()
            self.assertEqual(events.count('history'), 2)
            self.assertEqual(events.count('snapshot'), 2)
            self.assertEqual(events.count('admit'), 2)
            self.assertEqual(events.count('detail'), 5)
        self.assertEqual(self.host.engine.writes, writes)
        self.assertEqual(before, self.transport.db.execute('SELECT * FROM local_operation').fetchall())

    def test_missing_submitted_ambiguous_and_changed_receipt_refuse_without_write(self):
        with self.assertRaises(LocalDeltaUncertain): self.inspect()
        self.commit(); writes = self.host.engine.writes
        for control in ('submitted', 'ambiguous', 'receipt'):
            original = self.transport.db.execute('SELECT state,receipt FROM local_operation').fetchone()
            history = list(self.host.engine.history)
            if control == 'submitted': self.transport.db.execute("UPDATE local_operation SET state='submitted',receipt=NULL")
            elif control == 'ambiguous': self.host.engine.history.insert(0, dict(history[0]))
            else: self.transport.db.execute("UPDATE local_operation SET receipt='{}'")
            with self.assertRaises(LocalDeltaError): self.inspect()
            self.assertEqual(self.host.engine.writes, writes)
            self.transport.db.execute('UPDATE local_operation SET state=?,receipt=?', original)
            self.host.engine.history[:] = history

    def test_suppressed_policy_failure_withholds_observation(self):
        self.commit()
        class Policy:
            @contextmanager
            def writer(self, *args):
                try: yield
                except PermissionError: pass
            def admit(self, *args): raise PermissionError('current native policy')
        self.transport.policy = Policy()
        with self.assertRaises(PermissionError): self.inspect()

    def test_policy_cancellation_identity_survives_suppressing_writer(self):
        self.commit()
        for cancellation in (KeyboardInterrupt, SystemExit, GeneratorExit):
            primary = cancellation('original current policy cancellation')
            class Policy:
                @contextmanager
                def writer(self, *args):
                    try: yield
                    except BaseException: pass
                def admit(self, *args): raise primary
            self.transport.policy = Policy()
            with self.assertRaises(cancellation) as observed: self.inspect()
            self.assertIs(observed.exception, primary)


class HistoryMemoTests(unittest.TestCase):
    def fixture(self):
        host=harness.Tests();host.setUp();self.addCleanup(host.doCleanups)
        transport=host.transport
        transport.mutation('original',harness.SQL,{'id':'1'},intent_digest='b'*64,context='held')
        return host,transport

    def inspect(self,transport):
        return transport.inspect_committed('original',harness.SQL,{'id':'1'},intent_digest='b'*64,context='held')

    def test_twenty_real_sqlite_handle_checks_reuse_history_only(self):
        from ashlar_host.immutable_snapshot_memo import immutable_snapshot_memo
        host,transport=self.fixture();before=transport.db.execute('SELECT * FROM local_operation').fetchall();writes=host.engine.writes
        counts={}
        for mode in ('raw','memo'):
            from contextlib import nullcontext
            sql_reads=[];transport.db.set_trace_callback(sql_reads.append)
            with patch.object(transport,'original_history',wraps=transport.original_history)as history,patch.object(transport.policy,'admit',wraps=transport.policy.admit)as policy,patch.object(transport,'_profile',wraps=transport._profile)as profile,patch.object(transport,'_snapshot',wraps=transport._snapshot)as snapshot,patch.object(transport,'_detail',wraps=transport._detail)as detail:
                with immutable_snapshot_memo(transport)if mode=='memo'else nullcontext():
                    for _ in range(20):self.inspect(transport)
                counts[mode]=(history.call_count,policy.call_count,profile.call_count,snapshot.call_count,detail.call_count)
            transport.db.set_trace_callback(None)
            self.assertEqual(sum(statement.startswith('SELECT intent,intent_sha,before_version,state,receipt FROM local_operation WHERE operation=')for statement in sql_reads),40)
        self.assertEqual(counts['raw'][:4],(40,40,40,40))
        self.assertEqual(counts['memo'][:4],(2,40,40,40))
        self.assertGreaterEqual(counts['memo'][4],counts['raw'][4])
        self.assertEqual(before,transport.db.execute('SELECT * FROM local_operation').fetchall());self.assertEqual(writes,host.engine.writes)
        self.assertIsNone(transport._immutable_snapshot_memo)

    def test_changed_history_uuid_receipt_and_cancel_withhold(self):
        from ashlar_host.immutable_snapshot_memo import immutable_snapshot_memo
        for control in ('history','uuid','receipt','cancel'):
            host,transport=self.fixture();primary=KeyboardInterrupt()
            with self.assertRaises((ValueError,LocalDeltaError,KeyboardInterrupt))as caught:
                with immutable_snapshot_memo(transport):
                    self.inspect(transport)
                    if control=='history':host.engine.history.append({'version':99,'operation':'foreign'})
                    elif control=='uuid':
                        with patch.object(transport,'_detail',side_effect=LocalDeltaError('UUID changed')):self.inspect(transport)
                    elif control=='receipt':transport.db.execute("UPDATE local_operation SET receipt='{}'");self.inspect(transport)
                    else:host.engine.history.append({'version':99});raise primary
            if control=='cancel':self.assertIs(caught.exception,primary)
            self.assertIsNone(transport._immutable_snapshot_memo)

    def test_history_capacity_fallback_and_copy_isolation(self):
        from ashlar_host.immutable_snapshot_memo import immutable_snapshot_memo
        host,transport=self.fixture();target=host.target
        with immutable_snapshot_memo(transport)as memo:
            first=memo.observe_history(transport,target);first.clear()
            self.assertTrue(memo.observe_history(transport,target))
        with immutable_snapshot_memo(transport)as memo,patch.object(memo,'_fits',return_value=False),patch.object(transport,'original_history',wraps=transport.original_history)as history:
            memo.observe_history(transport,target);memo.observe_history(transport,target)
            self.assertEqual(history.call_count,4);self.assertEqual(memo.histories,{})

if __name__ == '__main__': unittest.main()
