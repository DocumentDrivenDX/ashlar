from dataclasses import replace
import hashlib
from pathlib import Path
import sys
import unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'))
from run_schema_evolution import inputs
from ashlar.apply import empty_state
from ashlar.outbox import OutboxTransaction,OutboxError,apply_outbox_transactions
from ashlar.source import jsonl_batches

class OutboxApplyTests(unittest.TestCase):
    def inputs(self):
        _,policies,transition,_=inputs()
        # Each committed PG group contains one independent JSONL transaction.
        lines=(ROOT/'examples/end-to-end/schema-evolution-source.jsonl').read_bytes().splitlines(keepends=True)
        groups=[];group=[]
        for line in lines:
            group.append(line)
            if b'"kind":"commit"' in line:
                raw=b''.join(group);group=[]
                batch=tuple(jsonl_batches(raw.splitlines(keepends=True),feed='outbox',epoch='one'))[0]
                position=len(groups)+1
                groups.append(OutboxTransaction('ashlar-postgresql-outbox/0.1','outbox','one',str(position-1),str(position),hashlib.sha256(raw).hexdigest(),batch))
        args=dict(prior=empty_state(),feed='outbox',epoch='one',after='0',expected_position='3',schema_policy=policies,schema_transition_policy=transition)
        return groups,args
    def test_native_positions_drive_schema_evolution_across_independent_byte_cursors(self):
        groups,args=self.inputs();result=apply_outbox_transactions(groups,**args)
        self.assertEqual((result.position,result.transactions),('3',3))
        self.assertEqual((len(result.state.current),len(result.state.history),len(result.state.tombstones)),(1,4,1))
        self.assertTrue(all(group.batch.cursor_before=='0' for group in groups))
        first=apply_outbox_transactions(groups[:1],**dict(args,expected_position='1'))
        resumed=apply_outbox_transactions(groups[1:],**dict(args,prior=first.state,after='1'))
        self.assertEqual(resumed.state,result.state)
    def test_gap_scope_payload_and_byte_offset_checkpoint_refuse(self):
        groups,args=self.inputs()
        for bad in [groups[::-1],groups[:-1],[groups[0],groups[0]],
                    [replace(groups[0],payload_digest='0'*64)],
                    [replace(groups[0],epoch='different')]]:
            with self.assertRaises(OutboxError):apply_outbox_transactions(bad,**args)
        with self.assertRaises(OutboxError):
            apply_outbox_transactions(groups,**dict(args,expected_position=groups[-1].batch.cursor_after))
