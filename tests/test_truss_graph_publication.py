import hashlib,json,unittest
from ashlar.apply import empty_state
from ashlar_host.graph_sql import graph_sql_plan,TABLES
from ashlar_host.schema_rows import fixture_columns
from ashlar_host.resources import RESOURCE_ROOT
from ashlar_host.truss_note_publication import graph_expected_rows
import test_truss_edge_feed

class Tests(unittest.TestCase):
    def project(self,prior,rows,delete=False):
        batch,parsed,_=test_truss_edge_feed.Tests().batch(delete=delete)
        clock='2026-10-12T00:00:00+00:00';columns=fixture_columns(RESOURCE_ROOT)
        state,_=graph_sql_plan(prior,batch,{role:'local.graph.'+role for role in TABLES},materialized_at=clock,schema_policy=lambda change:None)
        return state,graph_expected_rows(rows,state,parsed.changes,batch,columns,clock,parsed.event_bytes,parsed.edge_order_keys)
    def test_independent_graph_projection_preserves_typed_edge_order_and_identity(self):
        columns=fixture_columns(RESOURCE_ROOT);state,rows=self.project(empty_state(),{role:[]for role in columns})
        self.assertEqual(len(rows['object_current']),2);self.assertEqual(len(rows['edge_current']),1)
        edge=rows['edge_current'][0]
        self.assertEqual((edge['rel_type_id'],edge['source_type'],edge['source_id'],edge['target_type'],edge['target_id']),('9','3','7','4','8'))
        self.assertEqual(edge['order_key'],'ordered first')
        identity={'source_system':'installation','rel_type_id':9,'id':9}
        self.assertEqual(edge['lookup_hash'],hashlib.sha256(json.dumps(identity,separators=(',',':')).encode()).hexdigest())
        self.assertEqual(len(rows['whole_source_history']),3)
    def test_edge_first_atomic_delete_and_replay_preserve_complete_prior_history(self):
        columns=fixture_columns(RESOURCE_ROOT);created,rows=self.project(empty_state(),{role:[]for role in columns})
        deleted,following=self.project(created,rows,delete=True)
        self.assertEqual(following['object_current'],[]);self.assertEqual(following['edge_current'],[])
        self.assertEqual(len(following['whole_source_history']),6);self.assertEqual(len(following['tombstone']),3)
        self.assertEqual(following['tombstone'][0]['entity_kind'],'edge')
        self.assertEqual(set(following['tombstone'][0]),{name for name,_ in columns['tombstone']})
        self.assertIsNone(following['tombstone'][0]['source_position'])
        self.assertTrue(all(row['entity_version']=='2'for row in following['tombstone']))
        replay,repeated=self.project(deleted,following,delete=True)
        self.assertEqual(repeated,following);self.assertEqual(replay,deleted)

    def test_wrong_published_prior_edge_order_refuses_without_mutating_rows(self):
        from ashlar_host.truss_note_publication import validate_graph_edge_orders
        columns=fixture_columns(RESOURCE_ROOT);created,rows=self.project(empty_state(),{role:[]for role in columns})
        rows['edge_current'][0]['order_key']=None
        original=json.dumps(rows,sort_keys=True)
        _,parsed,_=test_truss_edge_feed.Tests().batch(delete=True)
        with self.assertRaises(ValueError):validate_graph_edge_orders(rows,parsed.changes,parsed.event_bytes)
        self.assertEqual(json.dumps(rows,sort_keys=True),original)
