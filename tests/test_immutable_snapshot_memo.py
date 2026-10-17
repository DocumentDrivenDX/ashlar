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
            owner._snapshot(target,1);self.assertEqual(owner.reads,2);self.assertEqual((owner.details,owner.histories),(6,1));self.assertGreater(memo.bytes,0)
        self.assertEqual((owner.details,owner.histories),(8,2));self.assertIsNone(owner._immutable_snapshot_memo);self.assertEqual(memo.values,{})
        with immutable_snapshot_memo(owner):owner._snapshot(target,0)
        self.assertEqual(owner.reads,3)
    def test_live_uuid_version_loss_failure_and_capacity_clear(self):
        owner,target=self.owner()
        with self.assertRaisesRegex(ValueError,'UUID changed'):
            with immutable_snapshot_memo(owner):
                owner._snapshot(target,0);owner.identity='replaced';owner._snapshot(target,0)
        self.assertIsNone(owner._immutable_snapshot_memo);owner.identity=target.uuid
        with self.assertRaisesRegex(ValueError,'history changed'):
            with immutable_snapshot_memo(owner):
                owner._snapshot(target,0);owner.versions=[1];owner._snapshot(target,0)
        self.assertIsNone(owner._immutable_snapshot_memo);owner.versions=[0,1]
        uncached=[]
        def oversized(target,version):uncached.append(version);return {'rows':[{'large':'x'*(4*1048576)}]}
        owner._snapshot_uncached=oversized
        with immutable_snapshot_memo(owner)as memo:
            owner._snapshot(target,0);owner._snapshot(target,0)
            self.assertEqual(uncached,[0,0]);self.assertEqual(memo.values,{});self.assertGreater(memo.bytes,0)
        owner,target=self.owner();primary=RuntimeError('original failure')
        with self.assertRaises(RuntimeError)as caught:
            with immutable_snapshot_memo(owner):
                owner._snapshot(target,0);owner.versions=[1];raise primary
        self.assertIs(caught.exception,primary);self.assertEqual(owner.histories,2);self.assertIsNone(owner._immutable_snapshot_memo)

    def test_late_three_megabyte_value_hits_and_copies_with_fresh_identity(self):
        from ashlar_host.immutable_snapshot_memo import _size
        owner,target=self.owner();owner.versions=list(range(17))
        def snapshot(target,version):
            owner.reads+=1
            return {'rows':[{'payload':str(version)+('x'*3420000)}]}
        owner._snapshot_uncached=snapshot
        with immutable_snapshot_memo(owner)as memo:
            first=owner._snapshot(target,16);self.assertGreater(_size(first),3*1048576)
            first['rows'].clear()
            self.assertEqual(len(owner._snapshot(target,16)['rows']),1)
            self.assertEqual((owner.reads,owner.details),(1,4));self.assertEqual(len(memo.values),1)
            self.assertLessEqual(memo.bytes+16384,8*1048576)
        self.assertEqual((owner.histories,owner.details),(2,6));self.assertEqual(memo.value_capacities,{})

    def test_value_lru_exact_accounting_protects_history_and_phase_witness(self):
        owner,target=self.owner();owner.versions=[0,1,2]
        def snapshot(target,version):owner.reads+=1;return {'rows':[{'payload':str(version)+('x'*2800000)}]}
        owner._snapshot_uncached=snapshot
        with immutable_snapshot_memo(owner)as memo:
            witness=memo.phase_witness(owner,target,(('stream','batch','original-request'),))
            protected=memo.bytes;histories=dict(memo.histories);witnesses=dict(memo.phase_witnesses)
            owner._snapshot(target,0);owner._snapshot(target,1);owner._snapshot(target,0)
            owner._snapshot(target,2)
            self.assertEqual([key[-1]for key in memo.values],[0,2])
            self.assertEqual(memo.bytes,protected+sum(memo.value_capacities.values()))
            self.assertLessEqual(memo.bytes+16384,8*1048576)
            self.assertEqual(memo.histories,histories);self.assertEqual(memo.phase_witnesses,witnesses)
            memo.admit_phase_witness(owner,target,witness)
            owner._snapshot(target,1);self.assertEqual(owner.reads,4)
            self.assertEqual([key[-1]for key in memo.values],[2,1])
            self.assertEqual(memo.bytes,protected+sum(memo.value_capacities.values()))
        self.assertEqual(owner.histories,2);self.assertEqual(memo.bytes,0)

    def test_protected_capacity_fallback_and_history_cancel_after_value_eviction(self):
        for cancellation in (False,True):
            owner,target=self.owner();owner.versions=[0,1,2]
            def snapshot(target,version):owner.reads+=1;return {'rows':[{'payload':str(version)+('x'*2800000)}]}
            owner._snapshot_uncached=snapshot;primary=KeyboardInterrupt('original')
            with self.assertRaises(KeyboardInterrupt if cancellation else ValueError)as caught:
                with immutable_snapshot_memo(owner)as memo:
                    for version in owner.versions:owner._snapshot(target,version)
                    self.assertEqual(len(memo.values),2)
                    owner.versions=[0,1]
                    if cancellation:raise primary
            if cancellation:self.assertIs(caught.exception,primary)
            self.assertEqual(owner.histories,2);self.assertIsNone(owner._immutable_snapshot_memo)
            self.assertEqual(memo.value_capacities,{})
        owner,target=self.owner();owner.versions=[0]
        owner.original_history=lambda target:[{'version':0,'opaque':'h'*800000}]
        with immutable_snapshot_memo(owner)as memo:
            # Protected debt is never evicted to accommodate a snapshot. Keep
            # its accounting real by retaining separate original table histories.
            targets=[SimpleNamespace(table='local.x.t'+str(i),uuid=target.uuid,path=Path('/private/tmp/table'+str(i)))for i in range(8)]
            for other in targets:memo.observe_history(owner,other)
            debt=memo.bytes;history_keys=set(memo.histories)
            owner._snapshot_uncached=lambda target,version:{'rows':[{'payload':'x'*2800000}]}
            owner._snapshot(targets[0],0);owner._snapshot(targets[0],0)
            self.assertEqual(memo.values,{});self.assertEqual(memo.bytes,debt);self.assertEqual(set(memo.histories),history_keys)
