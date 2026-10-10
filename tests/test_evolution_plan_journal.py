"""Local original-intent custody, not native publication or ACK qualification."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
from ashlar_host.commerce_evolution import (
    EvolutionAttemptPlan, EvolutionPlanJournal, EvolutionPlanError, PROFILE, owned_fd)


def original():
    request = dict(stream='commerce', batch_id='A:1', predecessor='initial',
        schema_revisions_json='{"A":"R1"}', source_batch_json='{"original":"batch"}',
        source_batch_digest='0' * 64, source_checkpoint_json='{"feed":"A"}')
    request['request_digest'] = hashlib.sha256(json.dumps(request, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    step = {'statement': 'MERGE original', 'parameters': {'rows': '["exact"]'}}
    return dict(profile=PROFILE, request=request, generated_steps=[step], selected_steps=[step],
        zero_match_elisions=[], observed_native_prior={'original.table': {'uuid': 'u', 'version': 0}},
        publication_id='pub-A-1', materialized_at='original-clock', recorded_at='123',
        previous_expected={}, expected={'object_current': []}, previous_progress={},
        progress={'A': '1'}, schema_state={'A': 'R1'},
        resource_registry={'original.table': {'uuid': 'u', 'path': '/original'}},
        source_admission={'profile': 'selected/1'}, operations=['effects:original:0'])


def plan(value=None):
    return EvolutionAttemptPlan(json.dumps(original() if value is None else value, separators=(',', ':')).encode())


class Policy:
    def __init__(self, expected):
        self.expected = expected
        self.calls = []
    def admit(self, supplied, context):
        self.calls.append(supplied.raw)
        if supplied.raw != self.expected or context != 'held':
            raise PermissionError('Current original authority required')


class JournalTests(unittest.TestCase):
    def test_original_is_immutable_and_reconstructed_in_new_interpreter(self):
        value = plan()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'original.json'
            owner = EvolutionPlanJournal(path, Policy(value.raw))
            self.assertEqual(owner.retain(value, context='held'), value)
            changed = value.document()
            changed['publication_id'] = 'borrowed'
            self.assertNotEqual(changed, value.document())
            script = '''import json, sys
from pathlib import Path
from ashlar_host.commerce_evolution import EvolutionPlanJournal
class Policy:
 def admit(self, plan, context):
  assert context == "held"
p = EvolutionPlanJournal(Path(sys.argv[1]), Policy()).load(expected_sha256=sys.argv[2], context="held")
print(json.dumps({"sha256": p.sha256, "operations": p.document()["operations"]}))
'''
            result = subprocess.run([sys.executable, '-S', '-c', script, str(path), value.sha256],
                check=True, capture_output=True, text=True, timeout=10,
                env={'PYTHONPATH': str(Path(__file__).resolve().parents[1] / 'src')})
            self.assertEqual(json.loads(result.stdout), {'sha256': value.sha256, 'operations': ['effects:original:0']})
            self.assertEqual(result.stderr, '')
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_existing_original_cannot_be_replaced_even_with_same_bytes(self):
        value = plan()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'original.json'
            owner = EvolutionPlanJournal(path, Policy(value.raw))
            owner.retain(value, context='held')
            with self.assertRaises(FileExistsError):
                owner.retain(value, context='held')
            self.assertEqual(path.read_bytes(), value.raw)

    def test_concurrent_retain_has_one_original_winner(self):
        value = plan()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'original.json'
            gate = threading.Barrier(2)
            def retain():
                owner = EvolutionPlanJournal(path, Policy(value.raw))
                gate.wait(timeout=5)
                try:
                    owner.retain(value, context='held')
                    return 'retained'
                except FileExistsError:
                    return 'refused'
            with ThreadPoolExecutor(max_workers=2) as pool:
                handles = [pool.submit(retain) for _ in range(2)]
                self.assertEqual(sorted(handle.result(timeout=10) for handle in handles), ['refused', 'retained'])
            self.assertEqual(path.read_bytes(), value.raw)

    def test_missing_partial_tampered_and_symlink_resume_refuse(self):
        value = plan()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'original.json'
            owner = EvolutionPlanJournal(path, Policy(value.raw))
            with self.assertRaises(FileNotFoundError):
                owner.load(expected_sha256=value.sha256, context='held')
            self.assertFalse(path.exists())
            path.write_bytes(b'{')
            with self.assertRaises(EvolutionPlanError):
                owner.load(expected_sha256=value.sha256, context='held')
            modified = original(); modified['publication_id'] = 'different'
            path.write_bytes(plan(modified).raw)
            with self.assertRaises(EvolutionPlanError):
                owner.load(expected_sha256=value.sha256, context='held')
            path.unlink(); path.symlink_to('absent')
            with self.assertRaises(OSError):
                owner.load(expected_sha256=value.sha256, context='held')

    def test_policy_refusal_precedes_creation(self):
        value = plan()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'original.json'
            with self.assertRaises(PermissionError):
                EvolutionPlanJournal(path, Policy(value.raw)).retain(value, context='wrong')
            self.assertFalse(path.exists())

    def test_partial_write_is_retained_and_cannot_be_reinitialized(self):
        value = plan()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'original.json'
            owner = EvolutionPlanJournal(path, Policy(value.raw))
            actual_write = os.write
            calls = []
            def interrupted(fd, raw):
                if calls: raise OSError('interrupted')
                calls.append(fd)
                return actual_write(fd, raw[:7])
            with patch('ashlar_host.commerce_evolution.os.write', interrupted):
                with self.assertRaises(OSError): owner.retain(value, context='held')
            self.assertEqual(path.read_bytes(), value.raw[:7])
            with self.assertRaises(FileExistsError): owner.retain(value, context='held')
            with self.assertRaises(EvolutionPlanError): owner.load(expected_sha256=value.sha256, context='held')

    def test_sync_failure_retains_original_and_refuses_reinitialization(self):
        value = plan()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'original.json'
            owner = EvolutionPlanJournal(path, Policy(value.raw))
            with patch('ashlar_host.commerce_evolution.os.fsync', side_effect=OSError('sync')):
                with self.assertRaises(OSError): owner.retain(value, context='held')
            self.assertEqual(path.read_bytes(), value.raw)
            with self.assertRaises(FileExistsError): owner.retain(value, context='held')

    def test_matching_caller_hash_does_not_replace_original_policy(self):
        value = plan(); modified = original(); modified['publication_id'] = 'replacement'
        replacement = plan(modified)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'original.json'; path.write_bytes(replacement.raw)
            with self.assertRaises(PermissionError):
                EvolutionPlanJournal(path, Policy(value.raw)).load(
                    expected_sha256=replacement.sha256, context='held')
            self.assertEqual(path.read_bytes(), replacement.raw)

    def test_closed_original_plan_and_order_refusals(self):
        for change in (lambda x: x.pop('resource_registry'),
                       lambda x: x['request'].update(predecessor='changed'),
                       lambda x: x.update(operations=['same', 'same']),
                       lambda x: x.update(selected_steps=[]),
                       lambda x: x.update(zero_match_elisions=[{'ordinal': True}])):
            value = original(); change(value)
            with self.assertRaises(EvolutionPlanError): plan(value)
        with self.assertRaises(EvolutionPlanError):
            EvolutionAttemptPlan(b'{"profile":1,"profile":2}')
        with self.assertRaises(EvolutionPlanError): EvolutionAttemptPlan(bytearray(plan().raw))

    def test_cleanup_cancellation_priority_and_original_cancellation_identity(self):
        ordinary = ValueError('body'); cancellation = KeyboardInterrupt('cleanup')
        with patch('ashlar_host.commerce_evolution.os.close', side_effect=cancellation):
            with self.assertRaises(KeyboardInterrupt) as observed:
                with owned_fd(1): raise ordinary
        self.assertIs(observed.exception, cancellation)
        original_cancel = KeyboardInterrupt('body')
        with patch('ashlar_host.commerce_evolution.os.close', side_effect=OSError('close')):
            with self.assertRaises(KeyboardInterrupt) as observed:
                with owned_fd(1): raise original_cancel
        self.assertIs(observed.exception, original_cancel)
        self.assertTrue(original_cancel.cleanup_failed)


if __name__ == '__main__': unittest.main()
