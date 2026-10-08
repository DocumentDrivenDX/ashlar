import copy
import unittest
from pathlib import Path
from native_schema_inventory import bind_intake_proofs
from run_schema_evolution import inputs
from fixture_oracle import fixture_batches,fixture_columns,fixture_inventory
from whole_graph_sql import graph_sql_plan
from ashlar.apply import empty_state
from ashlar.publisher import PublicationError
from ashlar.source_checkpoint import jsonl_checkpoint
import json

ROOT=Path(__file__).resolve().parents[1]

class NativeSchemaInventoryTests(unittest.TestCase):
    def proofs(self):
        intakes,_,_,_=inputs()
        return intakes,[dict(source_sha256=i.source_sha256,artifact_sha256=i.artifact_sha256,
            revision=i.document_revision,validator_revision=i.validator_revision,
            table='catalog.schema.schema_intake',table_uuid='00000000-0000-0000-0000-000000000001') for i in intakes]

    def test_exact_two_revision_custody_and_original_single_proof_compatibility(self):
        intakes,proofs=self.proofs()
        self.assertEqual(bind_intake_proofs(intakes,proofs,'catalog.schema'),tuple(zip(intakes,proofs)))
        legacy=copy.deepcopy(proofs[1]);legacy['table']='catalog.old.schema_intake'
        self.assertEqual(bind_intake_proofs(intakes[1:],[legacy],'catalog.schema'),((intakes[1],legacy),))

    def test_missing_substituted_duplicate_or_foreign_registry_refuses(self):
        intakes,proofs=self.proofs()
        for key,value in [('revision','2'),('source_sha256','0'*64),('artifact_sha256','0'*64),('table','catalog.other.schema_intake'),('table_uuid','different')]:
            changed=copy.deepcopy(proofs);changed[0][key]=value
            with self.assertRaises(PublicationError):bind_intake_proofs(intakes,changed,'catalog.schema')
        with self.assertRaises(PublicationError):bind_intake_proofs(intakes,proofs[:1],'catalog.schema')
        with self.assertRaises(PublicationError):bind_intake_proofs((intakes[0],intakes[0]),[proofs[0],proofs[0]],'catalog.schema')

    def test_evolution_original_bytes_sql_state_and_independent_oracle(self):
        _,policies,transition,batches=inputs()
        _,oracle=fixture_batches(ROOT,'evolution');self.assertEqual(tuple(oracle),batches)
        tables={r:'catalog.schema.'+r for r in fixture_columns(ROOT)}
        state=empty_state();previous='0'
        for ordinal,batch in enumerate(batches,1):
            checkpoint=json.loads(jsonl_checkpoint(batch));self.assertEqual(checkpoint['previous'],previous)
            previous=checkpoint['position']
            state,steps=graph_sql_plan(state,batch,tables,materialized_at='2026-10-08T17:00:00.000001+00:00',schema_policy=policies,schema_transition_policy=transition)
            expected,_=fixture_inventory(oracle[:ordinal],fixture_columns(ROOT),materialized_at='2026-10-08T17:00:00.000001+00:00')
            self.assertTrue(steps)
            self.assertEqual(len(state.current),len(expected['object_current']))
            self.assertEqual(len(state.history),len(expected['whole_source_history']))
            self.assertEqual(len(state.tombstones),len(expected['tombstone']))
            self.assertEqual({e.schema_revision for e in state.current.values()},{r['schema_revision'] for r in expected['object_current']})
        self.assertEqual(len(state.current),1)
        self.assertEqual(next(iter(state.current.values())).props_json,'{"23":"updated","24":"雪"}')
