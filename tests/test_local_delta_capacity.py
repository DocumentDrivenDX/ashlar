import json,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
sys.path[:0]=[str(Path(__file__).resolve().parents[1]/'tools'),str(Path(__file__).resolve().parent)]
from test_local_delta_custody import FakeSpark,Policy,SQL,PARAMS,TABLE
from local_delta_custody import DeltaTarget,LocalDeltaTransport,LOCAL_OPERATION_CAPACITY_8M,LocalDeltaError,original_operation_intent,bounded_local_json_bytes,encoded
class CapacityTests(unittest.TestCase):
    def setup(self,capacity=None):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);root=Path(temp.name);path=root/'table';path.mkdir();spark=FakeSpark();target=DeltaTarget(TABLE,path,'original-uuid');t=LocalDeltaTransport.initialize(spark,root/'journal.sqlite','installation',(target,),Policy(),context='host',capacity=capacity);self.addCleanup(t.close);t._history=lambda _:list(spark.history);t._snapshot=lambda _,v:{'schema':[['id','bigint'],['props','string']],'rows':[spark.executed[-1]],'row_sha256':'a'*64};return root,spark,t,target
    def params_at(self,target,limit,capacity=None):
        parameters={'id':'7','props':''};size=len(original_operation_intent('installation','operation','b'*64,SQL,parameters,target,capacity).encode());parameters['props']='x'*(limit-size);return parameters
    def test_default_old_bytes_and_exact_four_mib_boundary(self):
        root,spark,t,target=self.setup();old=encoded({'profile':'ashlar-local-delta-operation/0.1','installation_id':'installation','operation':'operation','request_digest':'b'*64,'statement':SQL,'parameters':PARAMS,'table':target.table,'path':str(target.path),'uuid':target.uuid});self.assertEqual(original_operation_intent('installation','operation','b'*64,SQL,PARAMS,target),old);self.assertNotIn('operation_capacity',json.loads(t.db.execute('SELECT original FROM local_installation').fetchone()[0]))
        p=self.params_at(target,4194304);t.mutation('operation',SQL,p,intent_digest='b'*64,context='host');self.assertEqual(spark.writes,1);self.assertEqual(len(t.db.execute('SELECT intent FROM local_operation').fetchone()[0].encode()),4194304)
    def test_default_one_byte_over_refuses_before_journal_and_native_dispatch(self):
        root,spark,t,target=self.setup();p=self.params_at(target,4194304);p['props']+='x'
        with self.assertRaises(LocalDeltaError):t.mutation('operation',SQL,p,intent_digest='b'*64,context='host')
        self.assertEqual(spark.writes,0);self.assertEqual(t.db.execute('SELECT count(*) FROM local_operation').fetchone()[0],0)
    def test_explicit_eight_mib_boundary_binds_every_custody_surface_and_missing_reopen_refuses(self):
        root,spark,t,target=self.setup(LOCAL_OPERATION_CAPACITY_8M);registry=json.loads(t.db.execute('SELECT original FROM local_installation').fetchone()[0]);self.assertEqual(registry['operation_capacity'],dict(LOCAL_OPERATION_CAPACITY_8M));reservation=json.loads((target.path/'.ashlar-local-installation-reservation.json').read_bytes());self.assertEqual(reservation['registry'],registry)
        p=self.params_at(target,8388608,LOCAL_OPERATION_CAPACITY_8M);t.mutation('operation',SQL,p,intent_digest='b'*64,context='host');intent=json.loads(t.db.execute('SELECT intent FROM local_operation').fetchone()[0]);self.assertEqual(intent['operation_capacity'],dict(LOCAL_OPERATION_CAPACITY_8M));self.assertEqual(intent['parameters'],p);self.assertEqual(spark.writes,1)
        p['props']+='x'
        with self.assertRaises(LocalDeltaError):t.mutation('operation',SQL,p,intent_digest='b'*64,context='host')
        self.assertEqual(spark.writes,1)
        with self.assertRaises(LocalDeltaError):LocalDeltaTransport(spark,root/'journal.sqlite','installation',(target,),Policy())
        self.assertEqual(spark.writes,1)
    def test_same_selected_profile_reopens_original_operation_and_recovers_without_second_write(self):
        root,spark,t,target=self.setup(LOCAL_OPERATION_CAPACITY_8M);original=t.mutation('operation',SQL,PARAMS,intent_digest='b'*64,context='host')
        reopened=LocalDeltaTransport(spark,root/'journal.sqlite','installation',(target,),Policy(),capacity=LOCAL_OPERATION_CAPACITY_8M)
        try:
            reopened._history=t._history;reopened._snapshot=t._snapshot
            self.assertEqual(reopened.recover('operation',SQL,PARAMS,intent_digest='b'*64,context='host'),original);self.assertEqual(spark.writes,1)
        finally:reopened.close()
    def test_unknown_open_profiles_refuse_before_any_native_or_journal_installation(self):
        for capacity in [False,{}, {'profile':'future','max_intent_bytes':8388608},{'profile':LOCAL_OPERATION_CAPACITY_8M['profile'],'max_intent_bytes':True},{'profile':LOCAL_OPERATION_CAPACITY_8M['profile'],'max_intent_bytes':16777216},{**LOCAL_OPERATION_CAPACITY_8M,'extra':True}]:
            with self.subTest(capacity=capacity),tempfile.TemporaryDirectory()as directory:
                root=Path(directory);path=root/'table';path.mkdir();spark=FakeSpark();target=DeltaTarget(TABLE,path,'original-uuid')
                with self.assertRaises(LocalDeltaError):LocalDeltaTransport.initialize(spark,root/'journal.sqlite','installation',(target,),Policy(),context='host',capacity=capacity)
                self.assertEqual(len(spark.history),1);self.assertFalse((root/'journal.sqlite').exists());self.assertFalse((path/'.ashlar-local-installation-reservation.json').exists())
    def test_exact_utf8_escape_count_and_early_refusal_without_whole_serialization(self):
        for value in [None,True,False,42,{'雪🙂"\\':['\b\t\n\f\r\u0000','é']},['a',{}]]:
            length=len(encoded(value).encode());self.assertEqual(bounded_local_json_bytes(value,length),length)
            with self.assertRaises(LocalDeltaError):bounded_local_json_bytes(value,length-1)
        root,spark,t,target=self.setup();calls=[]
        def refuse(*args,**kwargs):calls.append(True);raise AssertionError('Whole JSON serialization prohibited')
        with patch('local_delta_custody.encoded',refuse):
            with self.assertRaises(LocalDeltaError):original_operation_intent('installation','operation','b'*64,SQL,{'props':'x'*8_000_000},target)
        self.assertEqual(calls,[])
        with self.assertRaises(LocalDeltaError):bounded_local_json_bytes('\ud800',4194304)
if __name__=='__main__':unittest.main()
