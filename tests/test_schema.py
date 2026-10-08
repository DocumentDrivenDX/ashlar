import json
from pathlib import Path
import unittest
from ashlar.schema import SchemaIntake, SchemaIntakeError

ROOT = Path(__file__).resolve().parents[1]
PIN = '16c35e8d943769ccfa7bb57d16785aa7159abe65'

class SchemaCustodyTests(unittest.TestCase):
    def setUp(self):
        self.raw = (ROOT/'examples/end-to-end/schema-v1.intake.json').read_bytes()
        self.value = json.loads(self.raw)

    def read(self, raw):
        return SchemaIntake.read(raw,'revision-1',trusted_validator_revision=PIN)

    def test_exact_bytes(self):
        item = self.read(self.raw)
        self.assertEqual(item.artifact,self.raw)
        self.assertEqual(item.source,(ROOT/'examples/end-to-end/schema-v1.umf.json').read_bytes())
        self.assertFalse(item.complete_interpretation)

    def test_refuses_corrupted_custody(self):
        for key,value in [('sourceSha256','0'*64),('sourceBytes',True),('documentId','other'),('validatorRevision','0'*40),('completeInterpretation',True),('validatedStructure',False)]:
            with self.subTest(key=key):
                bad = dict(self.value,**{key:value})
                with self.assertRaises(SchemaIntakeError):
                    self.read(json.dumps(bad).encode())

    def test_duplicate_and_nonfinite_refused(self):
        for raw in [b'{"format":"a","format":"b"}',b'{"x":NaN}',b'\xff']:
            with self.assertRaises(SchemaIntakeError):
                self.read(raw)

    def test_explicit_revision(self):
        with self.assertRaises(SchemaIntakeError):
            SchemaIntake.read(self.raw,'',trusted_validator_revision=PIN)

    def test_unknown_source_retained(self):
        raw = (ROOT/'examples/end-to-end/schema-unknown.intake.json').read_bytes()
        item = self.read(raw)
        self.assertEqual(item.source,(ROOT/'examples/end-to-end/schema-unknown.umf.json').read_bytes())
        self.assertFalse(item.complete_interpretation)

if __name__=='__main__': unittest.main()
