from pathlib import Path
from dataclasses import replace
import unittest
from ashlar.semantic_policy import StringRecordPolicy,SemanticPolicyError
from ashlar.catalog import Identity,MappingEntry
from ashlar.schema import SchemaIntake
from ashlar.apply import Change,EntityKey,EntityState,empty_state,plan_apply
ROOT=Path(__file__).resolve().parents[1];PIN='16c35e8d943769ccfa7bb57d16785aa7159abe65'
ENTRIES=[MappingEntry(Identity('type',('truss-weft-source-review-fixture','fixture','item')),17,True),MappingEntry(Identity('property',('truss-weft-source-review-fixture','fixture','item','fixture','label')),23,True),MappingEntry(Identity('property',('truss-weft-source-review-fixture','fixture','item','fixture','caption')),24,True)]
class SemanticTests(unittest.TestCase):
    def policy(self,version='v3',entries=ENTRIES):return StringRecordPolicy((ROOT/('examples/end-to-end/schema-'+version+'.interpretation.json')).read_bytes(),entries,validator_revision=PIN,source_system='source',schema_revision='3')
    def change(self,props):return Change('feed','epoch','delivery','a'*64,'create',EntityState(EntityKey('source','object',17,1),1,'3',props,'{"future":18446744073709551615}'))
    def test_required_optional_and_retained_text_admitted(self):
        policy=self.policy()
        for props in ['{"23":"hello"}','{"23":"hello","24":"雪"}']:
            state=plan_apply(empty_state(),[self.change(props)],schema_policy=policy)
            self.assertEqual(next(iter(state.current.values())).retained_json,'{"future":18446744073709551615}')
    def test_missing_null_unknown_and_array_refuse(self):
        for props in ['{}','{"23":null}','{"23":"x","24":null}','{"23":["x"]}','{"23":"x","025":"unknown"}']:
            with self.assertRaises(SemanticPolicyError):self.policy()(self.change(props))
    def test_unknown_availability_and_incomplete_binding_refuse(self):
        with self.assertRaises(SemanticPolicyError):self.policy('v2')
        with self.assertRaises(SemanticPolicyError):self.policy(entries=ENTRIES[:-1])
    def test_original_intake_barrier_and_wrong_source_refusal(self):
        interpretation=(ROOT/'examples/end-to-end/schema-v3.interpretation.json').read_bytes()
        v3=SchemaIntake.read((ROOT/'examples/end-to-end/schema-v3.intake.json').read_bytes(),'3',trusted_validator_revision=PIN)
        policy=StringRecordPolicy.from_intake(v3,interpretation,ENTRIES,source_system='source')
        policy(self.change('{"23":"accepted"}'))
        v1=SchemaIntake.read((ROOT/'examples/end-to-end/schema-v1.intake.json').read_bytes(),'1',trusted_validator_revision=PIN)
        with self.assertRaises(SemanticPolicyError):StringRecordPolicy.from_intake(v1,interpretation,ENTRIES,source_system='source')
        with self.assertRaises(SemanticPolicyError):StringRecordPolicy.from_intake(replace(v3,source=b'changed'),interpretation,ENTRIES,source_system='source')
    def test_schema_or_source_drift_refuses(self):
        c=self.change('{"23":"x"}')
        for state in [replace(c.state,schema_revision='2'),replace(c.state,key=EntityKey('other','object',17,1))]:
            with self.assertRaises(SemanticPolicyError):self.policy()(replace(c,state=state))
if __name__=='__main__':unittest.main()
