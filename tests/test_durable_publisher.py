from contextlib import contextmanager
from pathlib import Path
import unittest
from test_attempt_store import MemoryExecutor, Policy
from ashlar.attempt_store import DeltaAttemptStore
from ashlar.durable_publisher import DurablePublisher
from ashlar.publisher import publish_batch, PublicationError
from ashlar.source import jsonl_batches

ROOT = Path(__file__).resolve().parents[1]
RESULT = '{"pin":"original","unknown":18446744073709551615}'
DESCRIPTOR = '{"publication":"original","retained":"  bytes  "}'

class Effects:
    def __init__(self, fail=None):
        self.calls = []
        self.fail = fail
    @contextmanager
    def writer(self, stream, context):
        if context != 'authorized': raise PermissionError('Denied')
        yield
    def apply(self, *args):
        self.calls.append('apply')
        if self.fail == 'apply': raise RuntimeError('Uncertain original submission')
        return RESULT
    def recover_apply(self, *args):
        self.calls.append('recover_apply'); return RESULT
    def validate(self, *args):
        self.calls.append('validate')
        if self.fail == 'validate': raise RuntimeError('Missing pin proof')
    def commit(self, *args):
        self.calls.append('commit')
        if self.fail == 'commit': raise RuntimeError('Uncertain original commit')
        return DESCRIPTOR
    def recover_commit(self, *args):
        self.calls.append('recover_commit'); return DESCRIPTOR
    def acknowledge(self, stream, request, descriptor, context):
        self.calls.append('ack')
        assert descriptor == DESCRIPTOR

class DurablePublisherTests(unittest.TestCase):
    def publish(self, ex, effects, predecessor='old'):
        # New coordinator/store instances retain no prior attempt state.
        backend = DurablePublisher(DeltaAttemptStore(ex, Policy(), 'c.s.t', 'uuid'), effects)
        batch = next(jsonl_batches((ROOT/'examples/end-to-end/source.jsonl').read_bytes().splitlines(keepends=True),feed='f',epoch='e'))
        return publish_batch(backend, 's', batch, predecessor=predecessor,
                             schema_revisions_json='{"f":"1"}', context='authorized')
    def test_full_phase_custody_and_fresh_instance_replay(self):
        ex=MemoryExecutor();effects=Effects()
        self.assertEqual(self.publish(ex,effects), DESCRIPTOR)
        self.assertEqual(len(ex.rows),5)
        self.assertEqual(self.publish(ex,effects), DESCRIPTOR)
        self.assertEqual(effects.calls,['apply','validate','commit','ack','ack'])
        with self.assertRaises(PublicationError): self.publish(ex,effects,'changed')
        self.assertEqual(len(ex.rows),5)
    def test_uncertain_effect_and_commit_reload_original_phase(self):
        for failure, phase, recovery in [('apply','applying','recover_apply'),('commit','committing','recover_commit')]:
            ex=MemoryExecutor();effects=Effects(failure)
            with self.assertRaises(RuntimeError): self.publish(ex,effects)
            self.assertEqual(ex.rows[-1]['phase'],phase)
            self.assertNotIn('ack',effects.calls)
            effects.fail=None
            self.assertEqual(self.publish(ex,effects),DESCRIPTOR)
            self.assertEqual(effects.calls.count(failure),1)
            self.assertEqual(effects.calls.count(recovery),1)
    def test_validation_refusal_retains_applied_without_commit(self):
        ex=MemoryExecutor();effects=Effects('validate')
        with self.assertRaises(RuntimeError): self.publish(ex,effects)
        self.assertEqual(ex.rows[-1]['phase'],'applied')
        self.assertEqual(effects.calls,['apply','validate'])
        effects.fail=None;self.publish(ex,effects)
        self.assertEqual(effects.calls.count('apply'),1)

if __name__ == '__main__': unittest.main()
