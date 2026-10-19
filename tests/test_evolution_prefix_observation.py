# SPDX-License-Identifier: MIT OR Apache-2.0
"""Owner-scoped completed-prefix reuse; no native runtime."""
import unittest
from unittest.mock import patch
from ashlar_host.driver import NativeDriver
from ashlar_host.evolution_run import EvolutionRunLedger, EvolutionRunLedgerView
from ashlar_host.evolution_composition import NativeEvolutionRunPolicy


class Tests(unittest.TestCase):
    def fixture(self):
        driver = object.__new__(NativeDriver)
        ledger = object.__new__(EvolutionRunLedger)
        policy = object.__new__(NativeEvolutionRunPolicy)
        context = object(); original = object()
        object.__setattr__(policy, 'driver', driver)
        object.__setattr__(policy, 'authority', type('Authority', (), {'admit_ledger': lambda *args: None})())
        ledger.policy = policy; ledger.context = context; ledger.original = original
        inventory = EvolutionRunLedgerView(((0, 'completed', b'original', 'digest', b'descriptor'),))
        return driver, ledger, context, inventory

    def test_one_native_pass_original_per_ordinal_observation(self):
        driver, ledger, context, inventory = self.fixture()
        descriptor = object(); journal = object()
        with patch.object(NativeDriver, 'require'), patch.object(NativeEvolutionRunPolicy, 'admit_run'), \
             patch.object(NativeEvolutionRunPolicy, 'original_attempt_admission', return_value=object()), \
             patch.object(EvolutionRunLedger, '_inventory', return_value=inventory), \
             patch.object(EvolutionRunLedger, 'attempt', return_value=journal), \
             patch.object(NativeDriver, '_validate_evolution_run_ledger', return_value=(descriptor,)) as full, \
             patch.object(NativeDriver, '_observe_completed_evolution_attempt', return_value=descriptor) as stored:
            self.assertEqual(driver.observe_completed_evolution_prefix(ledger, context=context),
                             (inventory, (descriptor,)))
            self.assertEqual(full.call_count, 1)
            stored.assert_called_once_with(journal, context=context, expected_sha256='digest',
                                           prefix=(inventory, (descriptor,)))

    def test_closing_inventory_drift_withholds_result(self):
        driver, ledger, context, inventory = self.fixture()
        with patch.object(NativeDriver, 'require'), patch.object(NativeEvolutionRunPolicy, 'admit_run'), \
             patch.object(NativeEvolutionRunPolicy, 'original_attempt_admission', return_value=object()), \
             patch.object(EvolutionRunLedger, '_inventory', side_effect=[inventory, inventory, object()]), \
             patch.object(EvolutionRunLedger, 'attempt', return_value=object()), \
             patch.object(NativeDriver, '_validate_evolution_run_ledger', return_value=(object(),)), \
             patch.object(NativeDriver, '_observe_completed_evolution_attempt', return_value=object()):
            with self.assertRaisesRegex(PermissionError, 'prefix changed'):
                driver.observe_completed_evolution_prefix(ledger, context=context)
