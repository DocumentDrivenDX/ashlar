import importlib.util
from pathlib import Path
import unittest
from ashlar.schema import SchemaIntake
from ashlar.semantic_policy import SemanticPolicyError

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('truss_probe',ROOT/'tools/probe_truss_catalog.py')
probe=importlib.util.module_from_spec(spec);spec.loader.exec_module(probe)
PIN='16c35e8d943769ccfa7bb57d16785aa7159abe65'

class TrussCatalogProbeTests(unittest.TestCase):
    def intake(self,version):
        return SchemaIntake.read((ROOT/f'examples/end-to-end/schema-v{version}.intake.json').read_bytes(),str(version),trusted_validator_revision=PIN)
    def interpretation(self,version):
        return (ROOT/f'examples/end-to-end/schema-v{version}.interpretation.json').read_bytes()
    def test_refuses_unsupported_or_wrong_source_before_native_submission(self):
        with self.assertRaises(SemanticPolicyError):probe.candidate_sql(self.intake(2),self.interpretation(2))
        with self.assertRaises(SemanticPolicyError):probe.candidate_sql(self.intake(1),self.interpretation(3))
    def test_sql_text_preserves_opaque_bytes_without_executable_interpolation(self):
        authored="quote'; COMMIT; --\n$guard$é\\"
        expression=probe.text_sql(authored)
        self.assertNotIn('COMMIT',expression)
        self.assertEqual(bytes.fromhex(expression.split("'")[1]).decode('utf-8'),authored)
        for bad in ['x\x00y','\ud800']:
            with self.assertRaises((ValueError,UnicodeError)):probe.text_sql(bad)

if __name__=='__main__':unittest.main()
