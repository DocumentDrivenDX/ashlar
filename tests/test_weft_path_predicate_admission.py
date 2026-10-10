"""Actual compiler fixture; schema port mocked only for owning inventory controls."""
import base64
import copy
import hashlib
import json
import unittest
import zlib
from test_weft_path_plan import SCHEMAS, FIXTURES
from weft_path_plan import PathAdmissionConfig, PathSchemaValidation, PathPlanError, admit_path_artifact

FIXTURE_B64 = (
    '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'
)
RAW = zlib.decompress(base64.b64decode(FIXTURE_B64))
assert hashlib.sha256(RAW).hexdigest() == 'acc7cc222dbee4324464cce8e00b2518eb5a0855ab16d23a8ccf8488c5a9580b'
FIXTURE = json.loads(RAW)

class NumericPredicateAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.config = PathAdmissionConfig(2097152, PathSchemaValidation(SCHEMAS, lambda *_: None))

    def admit(self, fixture):
        return admit_path_artifact(fixture['request'], fixture['response'], copy.deepcopy(fixture['response']), config=self.config)

    def test_original_numeric_equality_requires_and_retains_owning_guards(self):
        result = self.admit(FIXTURE)
        self.assertFalse(any(c.startswith('arithmetic.') for c in result.artifact['logicalPlan']['requiredCapabilities']))
        self.assertIn('ashlar.arithmetic.exact', [o['id'] for o in result.obligations])
        guard = next(o for o in result.obligations if o['id'] == 'ashlar.arithmetic.exact')
        self.assertEqual(guard['parameters']['checks'][0]['phase'], 'where-candidates')

    def test_missing_numeric_predicate_guard_refuses(self):
        x = copy.deepcopy(FIXTURE)
        x['response']['obligations'] = [o for o in x['response']['obligations'] if o['id'] != 'ashlar.arithmetic.exact']
        with self.assertRaises(PathPlanError): self.admit(x)

    def test_string_path_does_not_accept_unselected_numeric_guard(self):
        x = copy.deepcopy(next(f for f in FIXTURES if f['test'] == 'original_collection_retains_pins_keys_wide_order_and_closed_inventory'))
        self.admit(x)
        guard = next(o for o in FIXTURE['response']['obligations'] if o['id'] == 'ashlar.arithmetic.exact')
        x['response']['obligations'].append(copy.deepcopy(guard))
        with self.assertRaises(PathPlanError): self.admit(x)
