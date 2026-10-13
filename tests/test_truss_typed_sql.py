import json,unittest
from ashlar.apply import empty_state
from ashlar.truss_native_feed import NativeFeedError
from ashlar_host.graph_sql import graph_sql_plan,TABLES
from ashlar_host.truss_graph_admission import TrussGraphAdmission
import test_truss_typed_admission

class Tests(unittest.TestCase):
    def test_explicit_typed_plan_preserves_tokens_and_invokes_actual_admission(self):
        batch,catalogs=test_truss_typed_admission.Tests().fixture('decimal','required',{'kind':'decimal','text':'1.2300'})
        admission=TrussGraphAdmission(batch,catalogs);seen=[]
        def admit(change):seen.append(change);admission.admit(change)
        tables={role:'local.runtime.'+role for role in TABLES}
        after,steps=graph_sql_plan(empty_state(),batch,tables,materialized_at='2026-10-13T00:00:00+00:00',schema_policy=admit,native_scalar_profile='typed')
        self.assertEqual(len(seen),3)
        rows=[row for step in steps if 'rows'in step['parameters']for row in json.loads(step['parameters']['rows'])]
        self.assertTrue(any('"99":1.2300'in row.get('props_json','')for row in rows));self.assertEqual(len(after.current),3)
        with self.assertRaises(NativeFeedError):graph_sql_plan(empty_state(),batch,tables,materialized_at='2026-10-13T00:00:00+00:00',schema_policy=admit)
        previous=len(seen)
        with self.assertRaises(ValueError):graph_sql_plan(empty_state(),batch,tables,materialized_at='2026-10-13T00:00:00+00:00',schema_policy=admit,native_scalar_profile='unknown')
        self.assertEqual(len(seen),previous)
