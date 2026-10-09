import json,tempfile,unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from local_delta_custody import DeltaTarget,LocalDeltaTransport,LocalDeltaEffects,LocalDeltaError,LocalDeltaUncertain

TABLE='local.runtime.items'
SQL='INSERT INTO `local`.`runtime`.`items` VALUES (cast(:id AS BIGINT),:props)'
PARAMS={'id':'9007199254740993','props':' {"decimal":-0.00,"opaque":18446744073709551615} '}
class Conf:
    def __init__(self):self.values={'spark.sql.session.timeZone':'UTC','spark.sql.ansi.enabled':'true'}
    def get(self,k,default=None):return self.values.get(k,default)
    def set(self,k,v):self.values[k]=v
    def unset(self,k):self.values.pop(k,None)
class Policy:
    @contextmanager
    def writer(self,operation,context):yield
    def admit(self,intent,context):pass
class Row(dict):
    def asDict(self):return dict(self)
class FakeSpark:
    def __init__(self):
        self.conf=Conf();self.history=[{'version':0,'operation':'WRITE','userMetadata':None}];self.writes=0;self.fail=False;self.drop_commit=False;self.properties={};self.uuid='original-uuid';self.executed=[]
        self.read=SimpleNamespace(format=lambda x:SimpleNamespace(option=lambda *x:SimpleNamespace(load=lambda x:SimpleNamespace(count=lambda:0))))
    def sql(self,statement,args=None):
        if statement.startswith('DESCRIBE DETAIL'):return SimpleNamespace(first=lambda:Row(id=self.uuid,properties=self.properties))
        if statement.startswith('DESCRIBE HISTORY'):return SimpleNamespace(limit=lambda x:SimpleNamespace(collect=lambda:[Row(r) for r in self.history]))
        if statement.startswith('ALTER TABLE'):
            import re
            self.properties=dict(re.findall("'([^']+)'='([^']+)'",statement))
        else:
            self.writes+=1;self.executed.append(dict(args or {}))
            if self.fail:raise OSError('Lost original submission before known commit')
        if not self.drop_commit:self.history.insert(0,{'version':self.history[0]['version']+1,'operation':'WRITE','userMetadata':self.conf.get('spark.databricks.delta.commitInfo.userMetadata')})
        return SimpleNamespace(collect=lambda:[])
class Tests(unittest.TestCase):
    def setup(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);root=Path(temp.name);path=root/'table';path.mkdir()
        spark=FakeSpark();target=DeltaTarget(TABLE,path,'original-uuid');transport=LocalDeltaTransport.initialize(spark,root/'journal.sqlite','installation',(target,),Policy(),context='host');self.addCleanup(transport.close)
        transport._history=lambda t:list(spark.history)
        transport._snapshot=lambda t,v:{'schema':[['id','bigint'],['props','string']],'rows':[PARAMS],'row_sha256':'a'*64}
        return root,spark,transport
    def mutate(self,t):return t.mutation('operation',SQL,PARAMS,intent_digest='b'*64,context='host')
    def test_original_native_metadata_and_exact_receipt_replay_without_write(self):
        root,spark,t=self.setup();result=self.mutate(t);self.assertEqual(result.rows[0]['version'],'2')
        self.assertEqual(self.mutate(t),result);self.assertEqual(spark.writes,1)
        stored=t.db.execute('SELECT intent,state,receipt FROM local_operation').fetchone();self.assertEqual(stored[1],'committed')
        self.assertEqual(json.loads(stored[0])['parameters'],PARAMS)
        self.assertEqual(json.loads(stored[2])['snapshot']['rows'],[PARAMS])
    def test_submitted_absent_is_uncertain_and_never_replaced(self):
        root,spark,t=self.setup();spark.fail=True
        with self.assertRaises(OSError):self.mutate(t)
        self.assertEqual(t.db.execute('SELECT state FROM local_operation').fetchone()[0],'submitted')
        spark.fail=False
        for function in (t.mutation,t.recover):
            with self.assertRaises(LocalDeltaUncertain):function('operation',SQL,PARAMS,intent_digest='b'*64,context='host')
        self.assertEqual(spark.writes,1)
    def test_original_intent_and_uncertain_commit_duplicates_refuse(self):
        root,spark,t=self.setup();self.mutate(t)
        with self.assertRaises(LocalDeltaError):t.mutation('operation',SQL,{**PARAMS,'props':'changed'},intent_digest='b'*64,context='host')
        spark.history.insert(0,{**spark.history[0],'version':3})
        with self.assertRaises(LocalDeltaUncertain):self.mutate(t)
        self.assertEqual(spark.writes,1)
    def test_wrong_original_native_commit_version_and_uuid_refuse(self):
        root,spark,t=self.setup();self.mutate(t);spark.history[0]['version']=3
        with self.assertRaises(LocalDeltaError):self.mutate(t)
        t._detail=lambda t:(_ for _ in ()).throw(LocalDeltaError('UUID replaced'))
        with self.assertRaises(LocalDeltaError):self.mutate(t)
        self.assertEqual(spark.writes,1)
    def test_recovery_requires_original_plan_and_ordered_steps(self):
        root,spark,t=self.setup();effects=LocalDeltaEffects(t);steps=[{'statement':SQL,'parameters':PARAMS}]
        with self.assertRaises(LocalDeltaError):effects.recover('plan','b'*64,steps,context='host')
        first=effects.run('plan','b'*64,steps,context='host');self.assertEqual(effects.recover('plan','b'*64,steps,context='host'),first)
        with self.assertRaises(LocalDeltaError):effects.recover('plan','b'*64,steps+[steps[0]],context='host')
        self.assertEqual(spark.writes,1)
    def test_closed_target_and_parameter_profiles_and_journal_installation(self):
        root,spark,t=self.setup()
        for sql,params in [('DELETE FROM `local`.`runtime`.`items`',PARAMS),(SQL+'; SELECT 1',PARAMS),(SQL,{'id':1}),(SQL.replace('items','unknown'),PARAMS)]:
            with self.assertRaises(LocalDeltaError):t.mutation('operation',sql,params,intent_digest='b'*64,context='host')
        with self.assertRaises(LocalDeltaError):LocalDeltaTransport(spark,root/'journal.sqlite','other',tuple(t.targets.values()),Policy())
        self.assertEqual(spark.writes,0)

    def test_lost_journal_never_reinitializes(self):
        root,spark,t=self.setup();spark.fail=True
        with self.assertRaises(OSError):self.mutate(t)
        t.close();(root/'journal.sqlite').unlink();writes=spark.writes
        with self.assertRaises(LocalDeltaError):LocalDeltaTransport(spark,root/'journal.sqlite','installation',tuple(t.targets.values()),Policy())
        with self.assertRaises(LocalDeltaError):LocalDeltaTransport.initialize(spark,root/'journal.sqlite','installation',tuple(t.targets.values()),Policy(),context='host')
        self.assertEqual(spark.writes,writes)
    def test_original_parameters_frozen_before_callbacks(self):
        root,spark,t=self.setup();params=dict(PARAMS)
        class Mutating(Policy):
            def admit(self,intent,context):params['id']='2'
        t.policy=Mutating();t.mutation('freeze',SQL,params,intent_digest='b'*64,context='host')
        self.assertEqual(spark.executed[0],PARAMS)
    def test_closing_callback_uuid_and_runtime_refuse(self):
        for change in ('uuid','ansi'):
            root,spark,t=self.setup()
            class Closing(Policy):
                calls=0
                def admit(self,intent,context):
                    self.calls+=1
                    if self.calls==2:
                        if change=='uuid':spark.uuid='replacement'
                        else:spark.conf.set('spark.sql.ansi.enabled','false')
            t.policy=Closing()
            with self.assertRaises(LocalDeltaError):self.mutate(t)
            self.assertEqual(t.db.execute('SELECT state FROM local_operation').fetchone()[0],'submitted')
    def test_missing_or_changed_registration_refuses(self):
        root,spark,t=self.setup();t._reservation_path(next(iter(t.targets.values()))).unlink()
        with self.assertRaises(LocalDeltaError):self.mutate(t)
        self.assertEqual(spark.writes,0)
    def test_deleted_operation_named_history_refuses_changed_parameters(self):
        root,spark,t=self.setup();self.mutate(t)
        with t.db:t.db.execute('DELETE FROM local_operation')
        with self.assertRaises(LocalDeltaError):t.mutation('operation',SQL,{**PARAMS,'id':'2'},intent_digest='b'*64,context='host')
        self.assertEqual(spark.writes,1)
    def test_reservation_survives_uncertain_initialization_at_empty_version_zero(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);root=Path(temp.name);path=root/'table';path.mkdir()
        target=DeltaTarget(TABLE,path,'original-uuid');spark=FakeSpark();spark.drop_commit=True
        with self.assertRaises(LocalDeltaUncertain):LocalDeltaTransport.initialize(spark,root/'journal.sqlite','installation',(target,),Policy(),context='host')
        self.assertEqual(spark.history[0]['version'],0)
        self.assertFalse((root/'journal.sqlite').exists())
        self.assertTrue((path/'.ashlar-local-installation-reservation.json').is_file())
        with self.assertRaises(LocalDeltaError):LocalDeltaTransport.initialize(spark,root/'journal.sqlite','installation',(target,),Policy(),context='host')
        self.assertEqual(spark.history[0]['version'],0)
