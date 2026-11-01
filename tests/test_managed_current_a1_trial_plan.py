# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fail-closed checks for managed A1 pure trial-plan composition."""
import unittest

from types import SimpleNamespace

from ashlar_host.managed_current_a1_trial_plan import (
    prepare_managed_current_a1_trial,
)
from ashlar_host.managed_current_truss052_genesis_owner import (
    ManagedCurrentTruss052GenesisSelection,
)


class TestManagedCurrentA1TrialPlan(unittest.TestCase):
    def test_requires_same_owner_admitted_batch_before_manifest_or_request(self):
        batch = object()
        selection = ManagedCurrentTruss052GenesisSelection(
            "0" * 64, b"{}", {}, {}, {}, {}, {},
            SimpleNamespace(batch=object()), batch)
        with self.assertRaisesRegex(ValueError, "exact source-owner admitted"):
            prepare_managed_current_a1_trial(
                selection, {}, stream="source-stream", materialized_at="1",
                context=object())

    def test_requires_both_original_observations_and_only_a_page_batch(self):
        batch = object()
        observations = {
            "A": SimpleNamespace(batch=batch),
            "B": SimpleNamespace(batch=batch),
        }
        selection = ManagedCurrentTruss052GenesisSelection(
            "0" * 64, b"{}", {}, {}, {}, {}, observations,
            SimpleNamespace(batch=batch), batch)
        with self.assertRaisesRegex(ValueError, "Complete original A/B observations"):
            prepare_managed_current_a1_trial(
                selection, {}, stream="source-stream", materialized_at="1",
                context=object())


if __name__ == "__main__":
    unittest.main()
