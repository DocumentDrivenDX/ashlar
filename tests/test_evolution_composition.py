"""Pure correspondence controls; no installed native authority qualification."""
import unittest
import test_evolution_run as run_harness
from ashlar_host.evolution_composition import OriginalRunCorrespondence
from ashlar_host.evolution_plan import EvolutionAttemptPlan, EvolutionPlanError
from ashlar_host.evolution_run import encoded


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): run_harness.Tests.setUpClass()

    def setUp(self):
        self.host = run_harness.Tests(); self.host.setUp(); self.addCleanup(self.host.doCleanups)
        driver = self.host.driver; driver.held = True
        self.original = driver.capture_evolution_run(self.host.request, context=driver.context)
        self.owner = OriginalRunCorrespondence(self.original, driver.source_admission)

    def test_all_original_source_qualified_positions(self):
        for ordinal in range(8):
            value = self.owner.at(ordinal)
            checkpoint = __import__('json').loads(value['request']['source_checkpoint_json'])
            self.assertEqual(checkpoint['feed'], ('A', 'B')[ordinal % 2])
            self.assertEqual(checkpoint['position'], str(ordinal // 2 + 1))
            self.assertEqual(set(value['progress']), {'A'} if ordinal == 0 else {'A', 'B'})
        first = self.owner.at(0); first['progress'].clear()
        self.assertEqual(set(self.owner.at(0)['progress']), {'A'})

    def test_exact_original_attempt_and_foreign_progress_refusal(self):
        driver = self.host.driver
        plan = driver.plan_evolution_transaction(self.original, 0, {}, context=driver.context)
        self.owner.admit_attempt(0, plan)
        value = plan.document(); value['previous_progress'] = {'foreign': {}}
        with self.assertRaises(EvolutionPlanError):
            self.owner.admit_attempt(0, EvolutionAttemptPlan(encoded(value)))
        value = plan.document(); value['schema_state']['prefixes']['A'] = True
        with self.assertRaises(EvolutionPlanError):
            self.owner.admit_attempt(0, EvolutionAttemptPlan(encoded(value)))
        with self.assertRaises(EvolutionPlanError): self.owner.at(True)

if __name__ == '__main__': unittest.main()
