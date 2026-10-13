"""Read-only restore memo scope with inert native observations; no timeout diagnosis."""
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import unittest
from ashlar_host.finite_publication import FiniteFileDriver
from ashlar_host.delta_custody import LocalDeltaTransport


class FiniteRestoreMemoTests(unittest.TestCase):
    def fixture(self):
        owner=object.__new__(LocalDeltaTransport)
        owner.reads=0; owner.history_reads=0; owner.versions=[0]; owner.held=False
        target=SimpleNamespace(table='local.original.object_current',uuid='original-uuid',path=Path('/original'))
        def detail(selected):
            self.assertTrue(owner.held)
            self.assertIs(selected,target)
        def history(selected):
            self.assertTrue(owner.held);owner.history_reads+=1
            return [{'version':v} for v in owner.versions]
        def snapshot(selected,version):
            self.assertTrue(owner.held);owner.reads+=1
            return {'rows':[{'value':'original'}]}
        owner._detail=detail;owner.original_history=history;owner._snapshot_uncached=snapshot
        closes=[]
        def driver():
            d=object.__new__(FiniteFileDriver)
            d.request=None;d.manifest=None;d.context=object();d.transport=owner
            d.source=SimpleNamespace(admit_request=lambda request:self.assertFalse(owner.held))
            @contextmanager
            def interval(stream,context,*,restore):
                self.assertEqual(stream,'original');self.assertIs(context,d.context);self.assertTrue(restore)
                self.assertIsNone(getattr(owner,'_immutable_snapshot_memo',None))
                owner.held=True
                try:yield 'original-phase-closing'
                finally:
                    self.assertIsNone(owner._immutable_snapshot_memo)
                    closes.append('complete-phase-closing');owner.held=False
            d._existing_read_interval=interval
            return d
        return owner,target,driver,closes

    def test_scope_reuses_defensive_snapshots_then_reobserves_next_restore(self):
        owner,target,new_driver,closes=self.fixture()
        def records(request,*,select_manifest):
            self.assertEqual(select_manifest,'original-phase-closing')
            value=owner._snapshot(target,0);value['rows'][0]['value']='caller-edit'
            return owner._snapshot(target,0)
        for expected in (1,2):
            d=new_driver();d._restore_committed_records=records
            self.assertEqual(d.restore_committed_read_only({'stream':'original'}),{'rows':[{'value':'original'}]})
            self.assertEqual(owner.reads,expected)
            self.assertEqual(owner.history_reads,2*expected)
        self.assertEqual(closes,['complete-phase-closing']*2)

    def test_closing_drift_refuses_and_body_cancellation_survives_drift(self):
        for cancel in (False,True):
            owner,target,new_driver,closes=self.fixture();primary=KeyboardInterrupt()
            def records(request,*,select_manifest):
                owner._snapshot(target,0);owner.versions=[]
                if cancel:raise primary
                return 'provisional'
            d=new_driver();d._restore_committed_records=records
            with self.assertRaises(KeyboardInterrupt if cancel else ValueError) as caught:
                d.restore_committed_read_only({'stream':'original'})
            if cancel:self.assertIs(caught.exception,primary)
            self.assertFalse(owner.held);self.assertIsNone(owner._immutable_snapshot_memo)
            self.assertEqual(closes,['complete-phase-closing'])
