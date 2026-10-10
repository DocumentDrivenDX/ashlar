import copy,dataclasses,json,unittest
from pathlib import Path
from weft_path_fixture import fixture_inputs
from test_weft_path_fixture_request import RECEIPT
from weft_path_fixture_source import build_path_transaction,AuthoredPathAdmission,independent_states,independent_rows
from ashlar.whole_entity import changes_from_batch
from ashlar.apply import empty_state,plan_apply
from fixture_oracle import fixture_columns

class AuthoredSourceTests(unittest.TestCase):
    def setUp(self):
        self.m,self.g,self.r=fixture_inputs();self.batch=build_path_transaction(self.m,self.g,self.r)
        self.port=AuthoredPathAdmission(self.batch,model_bytes=self.m,graph_bytes=self.g,registry_bytes=self.r,public_receipt_bytes=RECEIPT,verify_public_receipt=lambda *args:None)
    def test_full_states_history_and_signed_ids(self):
        changes=changes_from_batch(self.batch);expected=independent_states(self.m,self.g,self.r)
        self.assertEqual([dataclasses.asdict(c.state) for c in changes], [dict(s,endpoints=None if s['endpoints'] is None else tuple(s['endpoints'])) for s in expected])
        for c in changes:self.port.admit(c)
        edge=[s['key']['id'] for s in expected if s['key']['kind']=='edge'];self.assertIn(-(2**63),edge);self.assertIn(2**63-1,edge);self.assertEqual(edge.count(0),3)
        rows=independent_rows(self.m,self.g,self.r,batch=self.batch,columns=fixture_columns(Path(__file__).resolve().parents[1]),published_at_micros='1')
        self.assertEqual([len(rows[r]) for r in ['object_current','edge_current','whole_source_history','tombstone']],[10,10,20,0])
        self.assertEqual([json.loads(r['change_json'])['state'] for r in rows['whole_source_history']],expected)
    def test_actual_apply_sql_full_rows_and_replay(self):
        from whole_graph_sql import graph_sql_plan
        columns=fixture_columns(Path(__file__).resolve().parents[1]);tables={r:'local.authored.'+r for r in columns}
        state,steps=graph_sql_plan(empty_state(),self.batch,tables,materialized_at='1970-01-01T00:00:00.000001+00:00',schema_policy=self.port.admit)
        self.assertEqual((len(state.current),len(state.history),len(state.tombstones)),(20,20,0))
        expected=independent_rows(self.m,self.g,self.r,batch=self.batch,columns=columns,published_at_micros='1')
        def bag(rows):return sorted(json.dumps(r,sort_keys=True) for r in rows)
        for role in ('object_current','edge_current','whole_source_history'):
            insert=next(s for s in steps if s['statement'].startswith('INSERT INTO `local`.`authored`.`'+role+'`'))
            rows=[{**{name:None for name,_ in columns[role]},**r} for r in json.loads(insert['parameters']['rows'])]
            for r in rows:
                if 'published_at' in r:r['published_at']='1'
            self.assertEqual(bag(rows),bag(expected[role]))
        replay,none=graph_sql_plan(state,self.batch,tables,materialized_at='1970-01-01T00:00:00.000001+00:00',schema_policy=self.port.admit)
        self.assertEqual(replay,state);self.assertEqual(none,[])
    def test_wrong_change_and_receipt_refuse(self):
        c=changes_from_batch(self.batch)[0]
        with self.assertRaises(PermissionError):self.port.admit(dataclasses.replace(c,epoch='wrong'))
        with self.assertRaises(PermissionError):self.port.admit(dataclasses.replace(c,raw_digest='0'*64))
        receipt=json.loads(RECEIPT);receipt['receipt']['relationships'][0]['targetInstanceId']='wrong'
        with self.assertRaises(PermissionError):AuthoredPathAdmission(self.batch,model_bytes=self.m,graph_bytes=self.g,registry_bytes=self.r,public_receipt_bytes=json.dumps(receipt).encode(),verify_public_receipt=lambda *args:None)
    def test_owning_verifier_and_snapshot(self):
        def refused(*args):raise PermissionError('Public verifier refused')
        with self.assertRaises(PermissionError):AuthoredPathAdmission(self.batch,model_bytes=self.m,graph_bytes=self.g,registry_bytes=self.r,public_receipt_bytes=RECEIPT,verify_public_receipt=refused)
        facts=self.port.metadata();facts['publicDatasetValidation']['valid']=False;self.assertTrue(self.port.metadata()['publicDatasetValidation']['valid'])

    def test_verifier_non_none_is_refusal(self):
        for value in (False, {'error':'refused'}):
            with self.subTest(value=value),self.assertRaises(PermissionError):
                AuthoredPathAdmission(self.batch,model_bytes=self.m,graph_bytes=self.g,registry_bytes=self.r,public_receipt_bytes=RECEIPT,verify_public_receipt=lambda *args:value)

    def test_receipt_numeric_work_refuses_before_callback(self):
        for token in (b'9'*1000000,b'1.5',b'NaN'):
            calls=[]
            with self.subTest(token=token[:5]),self.assertRaises(ValueError):
                AuthoredPathAdmission(self.batch,model_bytes=self.m,graph_bytes=self.g,registry_bytes=self.r,public_receipt_bytes=b'{"untrusted":'+token+b'}',verify_public_receipt=lambda *args:calls.append(args))
            self.assertEqual(calls,[])
