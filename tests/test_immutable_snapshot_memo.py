import unittest
from pathlib import Path
from types import SimpleNamespace
from ashlar_host.immutable_snapshot_memo import immutable_snapshot_memo
from ashlar_host.delta_custody import LocalDeltaTransport

class Tests(unittest.TestCase):
    def owner(self):
        owner=object.__new__(LocalDeltaTransport);owner.reads=0;owner.details=0;owner.histories=0;owner.versions=[0,1];owner.identity='native-uuid'
        def detail(target):
            owner.details+=1
            if target.uuid!=owner.identity:raise ValueError('UUID changed')
        def history(target):owner.histories+=1;return [{'version':v}for v in owner.versions]
        def snapshot(target,version):owner.reads+=1;return {'schema':[['value','string']],'rows':[{'value':str(version)}]}
        owner._detail=detail;owner.original_history=history;owner._snapshot_uncached=snapshot
        return owner,SimpleNamespace(table='local.x.object_current',uuid='native-uuid',path=Path('/private/tmp/original-table'))
    def test_hit_miss_deepcopies_and_new_interval_reobserves(self):
        owner,target=self.owner()
        with immutable_snapshot_memo(owner)as memo:
            first=owner._snapshot(target,0);first['rows'][0]['value']='caller-edit'
            self.assertEqual(owner._snapshot(target,0)['rows'],[{'value':'0'}]);self.assertEqual(owner.reads,1)
            owner._snapshot(target,1);self.assertEqual(owner.reads,2);self.assertEqual((owner.details,owner.histories),(6,6));self.assertGreater(memo.bytes,0)
        self.assertIsNone(owner._immutable_snapshot_memo);self.assertEqual(memo.values,{})
        with immutable_snapshot_memo(owner):owner._snapshot(target,0)
        self.assertEqual(owner.reads,3)
    def test_live_uuid_version_loss_failure_and_capacity_clear(self):
        owner,target=self.owner()
        with self.assertRaisesRegex(ValueError,'UUID changed'):
            with immutable_snapshot_memo(owner):
                owner._snapshot(target,0);owner.identity='replaced';owner._snapshot(target,0)
        self.assertIsNone(owner._immutable_snapshot_memo);owner.identity=target.uuid
        with self.assertRaisesRegex(ValueError,'version absent'):
            with immutable_snapshot_memo(owner):
                owner._snapshot(target,0);owner.versions=[1];owner._snapshot(target,0)
        self.assertIsNone(owner._immutable_snapshot_memo);owner.versions=[0,1]
        owner._snapshot_uncached=lambda target,version:{'rows':[{'large':'x'*1048576}]}
        with immutable_snapshot_memo(owner)as memo:
            owner._snapshot(target,0);self.assertEqual(memo.values,{});self.assertEqual(memo.bytes,0)
