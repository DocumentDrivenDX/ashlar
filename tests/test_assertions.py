import base64
import copy
import hashlib
import json
from pathlib import Path
import unittest
from dataclasses import replace
from ashlar.assertions import selected_assertion_inventory, AssertionInventoryError
from ashlar.schema import SchemaIntake

ROOT=Path(__file__).resolve().parents[1]
PIN='16c35e8d943769ccfa7bb57d16785aa7159abe65'

class AssertionInventoryTests(unittest.TestCase):
    def intake(self,v):
        return SchemaIntake.read((ROOT/f'examples/end-to-end/schema-v{v}.intake.json').read_bytes(),str(v),trusted_validator_revision=PIN)
    def interpretation(self,v):return (ROOT/f'examples/end-to-end/schema-v{v}.interpretation.json').read_bytes()
    def test_independent_selected_membership_and_honest_enforcement(self):
        result=selected_assertion_inventory(self.intake(3),self.interpretation(3))
        expected={'/modules/0/elements/0/kind','/modules/0/elements/0/members/0','/modules/0/elements/0/members/1'}
        expected.update('/modules/0/elements/'+str(i)+'/'+field for i in [1,2] for field in ['kind','scalarType','nullability','cardinality'])
        self.assertEqual({row['assertion']['sourcePointer'] for row in result['entries']},expected)
        self.assertEqual(len(result['entries']),11)
        self.assertFalse(result['completeInterpretation'])
        for row in result['entries']:
            self.assertEqual((row['enforcement'],row['reason']),('none','unqualified'))
            self.assertEqual(row['assertion']['definitionPin'],self.intake(3).source_sha256)
            self.assertEqual(base64.b64decode(row['source']['bytesBase64']),self.intake(3).source)
            self.assertNotIn('qualificationReceiptSha256',row)
        raw=base64.b64decode(result['inventoryArtifact']['bytesBase64'])
        self.assertEqual(hashlib.sha256(raw).hexdigest(),result['assertionInventorySha256'])
        self.assertEqual(base64.b64decode(result['originalInterpretation']['bytesBase64']),self.interpretation(3))
        self.assertEqual(base64.b64decode(result['originalIntake']['bytesBase64']),self.intake(3).artifact)
    def test_wrong_source_and_unsupported_assertions_refuse_completeness(self):
        with self.assertRaises(AssertionInventoryError):selected_assertion_inventory(self.intake(1),self.interpretation(3))
        with self.assertRaises(AssertionInventoryError):selected_assertion_inventory(self.intake(2),self.interpretation(2))
    def test_resource_refusal_returns_no_truncated_inventory(self):
        with self.assertRaises(AssertionInventoryError):
            selected_assertion_inventory(self.intake(3),b' '*1048577)
        with self.assertRaises(AssertionInventoryError):
            selected_assertion_inventory(replace(self.intake(3),source=b' '*65537),self.interpretation(3))

    def test_result_does_not_share_mutable_profile_pin_state(self):
        first=selected_assertion_inventory(self.intake(3),self.interpretation(3))
        first['entries'][0]['assertion']['sourceIdentityProfile']['identity']='altered'
        second=selected_assertion_inventory(self.intake(3),self.interpretation(3))
        self.assertNotEqual(first['entries'][0]['assertion']['sourceIdentityProfile'],second['entries'][0]['assertion']['sourceIdentityProfile'])

if __name__=='__main__':unittest.main()
