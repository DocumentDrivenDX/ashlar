import tempfile,unittest
from pathlib import Path
from dataclasses import replace
from unittest.mock import patch
from ashlar.apply import empty_state,plan_apply
from ashlar.semantic_policy import StringRecordPolicy
from run_schema_evolution import inputs
from run_local_example import PIN,ROOT
from test_semantic_policy import ENTRIES
from check_bound_umf_records import check_bound_existing_records
from ashlar.whole_entity import changes_from_batch

class BoundPrestateTests(unittest.TestCase):
    def test_prestate_uses_original_ids_when_target_ids_change(self):
        intakes,policies,transition,batches=inputs()
        state=plan_apply(empty_state(),changes_from_batch(batches[0]),schema_policy=policies)
        original=repr(state)
        target=StringRecordPolicy.from_intake(intakes[1],(ROOT/'examples/end-to-end/schema-v3.interpretation.json').read_bytes(),
            [replace(entry,catalog_id=entry.catalog_id+1000) for entry in ENTRIES],source_system='local-example')
        captured=[]
        def checker(source,intake,records,output,schema_path,*,identity):
            captured.extend(records);self.assertEqual(identity,{'module':'fixture','element':'item'});return {'valid':True}
        with tempfile.TemporaryDirectory() as tmp,patch('check_bound_umf_records.check_value_request',checker):
            result=check_bound_existing_records(None,intakes[1],target,state,policies.policies,schema_path='schema',output_dir=Path(tmp)/'receipts')
        self.assertEqual(result,({'valid':True},));self.assertEqual(len(captured),2)
        self.assertEqual(captured[0]['values'][0]['field'],{'module':'fixture','element':'label'})
        self.assertTrue(all(len(row['values'])==1 for row in captured));self.assertEqual(repr(state),original)
    def test_missing_original_binding_or_history_refuses_before_upstream(self):
        intakes,policies,transition,batches=inputs()
        state=plan_apply(empty_state(),changes_from_batch(batches[0]),schema_policy=policies)
        for original_policies,candidate in [({},state),(policies.policies,replace(state,history={}))]:
            with tempfile.TemporaryDirectory() as tmp,patch('check_bound_umf_records.check_value_request') as checker:
                with self.assertRaises(ValueError):check_bound_existing_records(None,intakes[1],policies.policies[('local-example','3')],candidate,original_policies,schema_path='schema',output_dir=Path(tmp)/'receipts')
                checker.assert_not_called()
