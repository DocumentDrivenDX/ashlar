"""Read-only native receipt controls with actual owned SQLite and inert engine."""
import unittest
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

if __name__ == '__main__': unittest.main()
