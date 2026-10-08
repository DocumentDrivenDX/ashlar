from dataclasses import replace
from pathlib import Path
import json
import unittest
from ashlar.apply import ApplyError,empty_state,plan_apply
from ashlar.source import jsonl_batches,records_digest
from ashlar.whole_entity import changes_from_batch
ROOT=Path(__file__).resolve().parents[1]
class WholeEntityTests(unittest.TestCase):
    def batches(self):
        return list(jsonl_batches((ROOT/'examples/end-to-end/graph-source.jsonl').read_bytes().splitlines(keepends=True),feed='fixture',epoch='one'))
    def rewrite(self,batch,change):
        lines=[batch.begin]+[record.raw for record in batch.records]
        value=json.loads(lines[1]);change(value);lines[1]=(json.dumps(value)+'\n').encode()
        commit=json.loads(batch.commit);commit['records_sha256']=records_digest(lines[1:]);lines.append((json.dumps(commit)+'\n').encode())
        return list(jsonl_batches(lines,feed=batch.feed,epoch=batch.epoch,cursor_before=batch.cursor_before))[0]
    def test_real_transaction_adapter_and_complete_apply(self):
        state=empty_state()
        for batch in self.batches():state=plan_apply(state,changes_from_batch(batch),schema_policy=lambda _:None)
        self.assertEqual(len(state.current),2)
        self.assertEqual(len(state.history),9)
        self.assertEqual(len(state.tombstones),3)
        updated=next(x for x in state.current.values() if x.key.id==1)
        self.assertEqual(updated.props_json,'{"23":"updated","24":null}')
        self.assertEqual(updated.retained_json,'{"future":18446744073709551615}')
    def test_no_version_guess_or_profile_alias(self):
        for update in [lambda v:v.pop('entity_version'),lambda v:v.update(entity_version=1),lambda v:v.update(entity_version='01'),lambda v:v.update(source_profile='truss-native')]:
            with self.assertRaises(ApplyError):changes_from_batch(self.rewrite(self.batches()[0],update))
    def test_unknown_executable_assertion_blocks(self):
        with self.assertRaises(ApplyError):changes_from_batch(self.rewrite(self.batches()[0],lambda v:v.update(unknown_execution='must-run')))
if __name__=='__main__':unittest.main()
