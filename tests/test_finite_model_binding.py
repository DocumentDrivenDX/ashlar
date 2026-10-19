# SPDX-License-Identifier: MIT OR Apache-2.0
"""Graph metadata correspondence does not depend on a query scenario."""
import unittest
from unittest.mock import patch
from ashlar_host.finite_pack import FinitePackDefinition
from ashlar_host.pack_count_star_request import original_finite_pack_binding
import test_payments_count_query as fixtures

class Tests(unittest.TestCase):
    def test_complete_binding_without_scenario_selection(self):
        values=fixtures.Tests().inputs()
        with patch.object(FinitePackDefinition,'cases',side_effect=AssertionError('No scenario SQL permitted')):
            binding=original_finite_pack_binding(FinitePackDefinition('payments'),*values)
        self.assertEqual(binding['publication']['id'],'inert-payments')
        self.assertEqual(len(binding['publication']['tables']),4)
        self.assertEqual(binding['modelPins'][0]['umfVersion'],'0.8.0')
        self.assertNotIn('sql',binding)
