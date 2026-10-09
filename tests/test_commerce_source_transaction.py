import json
import unittest
from decimal import Decimal
from commerce_source_transaction import ROOT,build_transaction
from ashlar.whole_entity import changes_from_batch

class Tests(unittest.TestCase):
    def setUp(self):
        self.source=(ROOT/'ontology.json').read_bytes();self.graph=(ROOT/'graph/fixture.json').read_bytes()
    def test_original_rows_endpoints_and_exact_values(self):
        batch,binding=build_transaction(self.source,self.graph,source_system='commerce-fixture')
        changes=changes_from_batch(batch);self.assertEqual(len(changes),21)
        original=json.loads(self.graph);objects=original['objects'];edges=original['edges']
        by_property={b['property_id']:b['identity'][2] for b in binding['properties']}
        self.assertEqual(len(by_property),len(binding['properties']))
        self.assertTrue(all(str(int(key))==key and 0<int(key)<2**63 for key in by_property))
        by_original={b['originalKey']:c.state.key for b,c in zip(binding['entities'],changes)}
        for row,change in zip(objects+edges,changes):
            self.assertEqual(json.loads(change.state.retained_json)['original'],row)
            if 'values' in row:
                props=json.loads(change.state.props_json,parse_float=Decimal)
                for key,value in props.items():
                    self.assertEqual(str(value),row['values'][by_property[key]])
            else:self.assertEqual(change.state.endpoints,tuple(by_original[row[k]] for k in ('source','target')))
    def test_deterministic_complete_transaction_and_source_isolation(self):
        a,ab=build_transaction(self.source,self.graph,source_system='A')
        again,bb=build_transaction(self.source,self.graph,source_system='A')
        b,_=build_transaction(self.source,self.graph,source_system='B')
        self.assertEqual(a,again);self.assertEqual(ab,bb)
        self.assertNotEqual(a.records_sha256,b.records_sha256)
        self.assertTrue(set(c.state.key for c in changes_from_batch(a)).isdisjoint(c.state.key for c in changes_from_batch(b)))
    def test_modified_originals_and_missing_namespace_refused(self):
        for s,g,n in [(self.source+b' ',self.graph,'A'),(self.source,self.graph+b' ','A'),(self.source,self.graph,'')]:
            with self.assertRaises(ValueError):build_transaction(s,g,source_system=n)

    def test_fresh_installation_plan_and_exact_delivery_replay(self):
        from ashlar.apply import empty_state
        from whole_graph_sql import graph_sql_plan
        batch,_=build_transaction(self.source,self.graph,source_system='commerce-fixture')
        exact=changes_from_batch(batch)
        def fixture_admit(change):
            self.assertIn(change,exact);self.assertEqual(change.operation,'create')
        tables={k:'c.s.'+k for k in ['object_current','edge_current','tombstone','whole_source_history']}
        state,steps=graph_sql_plan(empty_state(),batch,tables,materialized_at='2026-10-09T00:00:00+00:00',schema_policy=fixture_admit)
        self.assertEqual(len(state.current),21);self.assertEqual(len(state.history),21)
        self.assertEqual(len(state.tombstones),0)
        self.assertEqual(set(state.current),{c.state.key for c in exact})
        for change in exact:self.assertEqual(state.current[change.state.key],change.state)
        again,replay=graph_sql_plan(state,batch,tables,materialized_at='2026-10-09T00:00:00+00:00',schema_policy=fixture_admit)
        self.assertEqual(state,again);self.assertEqual(replay,[])
        history=json.loads(next(step['parameters']['rows'] for step in steps if 'whole_source_history' in step['statement']))
        self.assertEqual(len(history),21)

    def test_legacy_named_property_fixture_is_explicit_and_original_rows_remain_equal(self):
        current,binding=build_transaction(self.source,self.graph,source_system='A')
        old,legacy=build_transaction(self.source,self.graph,source_system='A',binding_profile='ashlar-commerce-development-bindings/0.1')
        self.assertNotIn('properties',legacy)
        self.assertEqual([json.loads(r.raw)['retained_json'].split('"original":',1)[1] for r in current.records],
                         [json.loads(r.raw)['retained_json'].split('"original":',1)[1] for r in old.records])
        with self.assertRaises(ValueError):build_transaction(self.source,self.graph,source_system='A',binding_profile='unknown')
