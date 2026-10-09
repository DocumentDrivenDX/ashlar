import unittest
from contextlib import nullcontext
from types import SimpleNamespace
from ashlar.publisher import Attempt,PublicationError
from committed_replay import CommittedReplayBackend


class ReplayTests(unittest.TestCase):
    def test_missing_incomplete_or_conflicting_attempt_cannot_be_recreated(self):
        for attempt in [None,Attempt('digest','prepared'),Attempt('digest','applying'),Attempt('digest','applied'),Attempt('digest','committing'),Attempt('other','committed',descriptor='original'),Attempt('digest','committed',descriptor='other')]:
            backend=SimpleNamespace(observe=lambda *args:attempt)
            replay=CommittedReplayBackend(backend,'stream','batch','digest','original')
            with self.assertRaises(PublicationError):replay.observe('stream','batch')
            self.assertFalse(hasattr(replay,'prepare'))
            self.assertFalse(hasattr(replay,'apply'))
            self.assertFalse(hasattr(replay,'commit'))

    def test_exact_replay_still_uses_original_writer_and_current_ack(self):
        calls=[];original=Attempt('digest','committed',descriptor='original')
        backend=SimpleNamespace(writer=lambda stream,context:nullcontext(),observe=lambda *args:original,
            acknowledge=lambda *args:calls.append(args))
        replay=CommittedReplayBackend(backend,'stream','batch','digest','original')
        with replay.writer('stream','context'):self.assertIs(replay.observe('stream','batch'),original)
        request={'batch_id':'batch','request_digest':'digest'}
        replay.acknowledge('stream',request,'original','context')
        self.assertEqual(calls,[('stream',request,'original','context')])
        with self.assertRaises(PublicationError):replay.acknowledge('stream',request,'other','context')
        self.assertEqual(len(calls),1)


if __name__=='__main__':unittest.main()
