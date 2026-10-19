# SPDX-License-Identifier: MIT OR Apache-2.0
"""Closed inspection dispatch refuses unknown owners before any acquisition."""
import unittest
from unittest.mock import patch
from ashlar_host.medical_inspection_selection import parse_medical_inspection,create_medical_inspection,medical_inspection_selection_bytes
from ashlar_host.medical_prospective_manifest import original_inspection_facts

class InspectionSelectionTests(unittest.TestCase):
 def test_unknown_profile_and_unknown_nominal_owner_refuse_without_callbacks(self):
  with self.assertRaises(ValueError):parse_medical_inspection(b'{"profile":"unqualified"}')
  class Unknown:
   def __post_init__(self):raise AssertionError('Unowned callback reached')
  for function in (create_medical_inspection,medical_inspection_selection_bytes):
   with self.assertRaises(ValueError):function(Unknown())
 def test_journal_facts_have_one_independent_observation_owner(self):
  with patch('ashlar_host.medical_prospective_manifest.validate_medical_journal_observations',return_value=None)as validate:
   self.assertEqual(original_inspection_facts({'classification':'observed-absent-at-anchor','journal_observations':[{'retained':'facts'}]}),{'classification':'observed-absent-at-anchor'})
   validate.assert_called_once_with([{'retained':'facts'}])
  with self.assertRaises(ValueError):original_inspection_facts({'journal_observations':[],'process_observations':[]})
