"""One fresh native prefix audit supplies only the ordinal preflight."""
import unittest
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock,patch
from ashlar_host.commerce_evolution import _run_commerce_evolution
from ashlar_host.evolution_composition import NativeEvolutionRunPolicy
from ashlar_host.evolution_run import EvolutionRunLedger,EvolutionRunLedgerView
from ashlar_host.evolution_plan import EvolutionPlanError

class Tests(unittest.TestCase):
    def test_native_audit_reuse_refusals_and_fallback_preserve_closing(self):
        for case in ('native','fallback','missing','short','changed','ordinal','later-started'):
            with self.subTest(case=case):
                slots=[(0,'retained',b'original','digest',None)]+[(i,'unprepared',None,None,None)for i in range(1,8)]
                if case=='later-started':slots[1]=(1,'publication-started',b'later','later-digest',None)
                view=EvolutionRunLedgerView(tuple(slots));observed=None if case=='missing'else EvolutionRunLedgerView(tuple(slots[:1]))if case=='short'else view
                changed=EvolutionRunLedgerView(tuple([(0,'unprepared',None,None,None),*slots[1:]]))
                definition={'installation':{'source_admission':{'sources':[]}},'request':{},'schedule':[{'publication_id':'publication','materialized_at':'clock','recorded_at':'1'}]}
                original=SimpleNamespace(raw=b'original-run',document=lambda:definition)
                driver=SimpleNamespace(writer=lambda *a:nullcontext(),validate_evolution_run=Mock(),verify_evolution_sources=Mock(return_value=None),observe_completed_evolution_prefix=Mock(return_value=(observed,())),publish_evolution_attempt_held=Mock(side_effect=RuntimeError('first submission reached')))
                policy=object.__new__(NativeEvolutionRunPolicy)if case!='fallback'else SimpleNamespace(admit_run=Mock(return_value=None),admit_ledger=Mock(return_value=None))
                if case!='fallback':
                    object.__setattr__(policy,'driver',driver);object.__setattr__(policy,'authority',SimpleNamespace(admit_run=Mock(return_value=None)))
                ledger=SimpleNamespace(original=original,policy=policy,_inventory=Mock(return_value=changed if case=='changed'else view),slot=Mock(return_value=slots[0][1:]),attempt=Mock(return_value='journal'),admit=Mock(),close=Mock())
                config=SimpleNamespace(__post_init__=lambda:None,driver=driver,context=object(),ledger_path='/original/ledger',policy=policy,expected_sha256='original',producer=object())
                plan=SimpleNamespace(document=lambda:{'previous_progress':{},'publication_id':'publication','request':{'stream':'original','predecessor':'origin'},'materialized_at':'clock','recorded_at':'1'})
                def inspect_view(path,selected,*,context,custody):
                    self.assertEqual((path,selected,context),(config.ledger_path,original,config.context));self.assertIsNone(custody(path,selected,context))
                    return changed if case=='changed'else view
                wanted=RuntimeError if case in ('native','fallback')else EvolutionPlanError
                with patch('ashlar_host.evolution_run.EvolutionRunLedger.open',return_value=ledger),patch('ashlar_host.evolution_run.request_from_document',return_value=SimpleNamespace(stream='original',predecessor='origin')),patch('ashlar_host.evolution_admission.EvolutionSourceSet',return_value=object()),patch('ashlar_host.commerce_evolution.EvolutionAttemptPlan',return_value=plan),patch.object(EvolutionRunLedger,'inspect',side_effect=inspect_view)as inspect:
                    with self.assertRaises(wanted):_run_commerce_evolution(config,False,through_ordinal=2 if case=='ordinal'else 0)
                ledger.admit.assert_called_once();ledger.close.assert_called_once();self.assertGreaterEqual(driver.verify_evolution_sources.call_count,2)
                if case=='fallback':
                    driver.observe_completed_evolution_prefix.assert_not_called();self.assertEqual(inspect.call_count,2);policy.admit_ledger.assert_called_once()
                else:
                    driver.observe_completed_evolution_prefix.assert_called_once();self.assertEqual(inspect.call_count,0 if case in ('missing','short')else 1)
                self.assertEqual(driver.publish_evolution_attempt_held.call_count,1 if case in ('native','fallback')else 0)
