import sys
from contextlib import contextmanager
from pathlib import Path
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from durable_effects import DurableEffects,EffectPlanError
from durable_sql import DurableSQL,SQLPending

class Policy:
    @contextmanager
    def writer(self,op,context):
        if context!='admitted':raise PermissionError('Denied')
        yield
    def admit(self,plan,context):
        if plan['steps'][0]['parameters'].get('deny'):raise PermissionError('Unsupported plan')
class API:
    def __init__(self):self.calls=[];self.pending=True
    def do(self,method,path,**kwargs):
        self.calls.append((method,path))
        handle='h'+str(sum(m=='POST' for m,p in self.calls)) if method=='POST' else path.rsplit('/',1)[-1]
        return {'statement_id':handle,'status':{'state':'RUNNING' if handle=='h2' and self.pending else 'SUCCEEDED'},'manifest':{},'result':{}}
STEPS=[{'statement':'INSERT first','parameters':{}},{'statement':'INSERT second','parameters':{}}]
class DurableEffectTests(unittest.TestCase):
    def test_interrupted_plan_reloads_and_recovers_only_original_second_handle(self):
        with tempfile.TemporaryDirectory() as d:
            path=str(Path(d)/'journal');api=API();c=DurableSQL(path,api,'2439e1f2e37ac563','principal')
            with self.assertRaises(SQLPending):DurableEffects(c,Policy()).run('original','a'*64,STEPS,context='admitted')
            c.close();api.pending=False;c=DurableSQL(path,api,'2439e1f2e37ac563','principal')
            result=DurableEffects(c,Policy()).recover('original','a'*64,STEPS,context='admitted')
            self.assertEqual(len(result['responses']),2)
            self.assertEqual([m for m,p in api.calls],['POST','POST','GET'])
            before=len(api.calls)
            with self.assertRaises(EffectPlanError):DurableEffects(c,Policy()).run('original','a'*64,STEPS[::-1],context='admitted')
            self.assertEqual(len(api.calls),before);c.close()
    def test_denied_plan_retains_no_intent_or_effects(self):
        with tempfile.TemporaryDirectory() as d:
            api=API();c=DurableSQL(str(Path(d)/'journal'),api,'2439e1f2e37ac563','principal');runner=DurableEffects(c,Policy())
            with self.assertRaises(PermissionError):runner.run('op','a'*64,STEPS,context='denied')
            self.assertEqual(c.db.execute('SELECT count(*) FROM effect_plan').fetchone()[0],0)
            self.assertEqual(api.calls,[]);c.close()
    def test_recovery_of_missing_or_changed_plan_refuses_without_effects(self):
        with tempfile.TemporaryDirectory() as d:
            api=API();api.pending=False
            c=DurableSQL(str(Path(d)/'journal'),api,'2439e1f2e37ac563','principal')
            runner=DurableEffects(c,Policy())
            with self.assertRaises(EffectPlanError):runner.recover('missing','a'*64,STEPS,context='admitted')
            self.assertEqual(api.calls,[])
            self.assertEqual(c.db.execute('SELECT count(*) FROM effect_plan').fetchone()[0],0)
            runner.run('original','a'*64,STEPS,context='admitted')
            before=list(api.calls)
            for digest,steps in [('b'*64,STEPS),('a'*64,STEPS[::-1])]:
                with self.assertRaises(EffectPlanError):runner.recover('original',digest,steps,context='admitted')
            with c.db:c.db.execute("UPDATE effect_plan SET plan_digest=? WHERE operation='original'",('0'*64,))
            with self.assertRaises(EffectPlanError):runner.recover('original','a'*64,STEPS,context='admitted')
            with c.db:c.db.execute("DELETE FROM effect_plan WHERE operation='original'")
            with self.assertRaises(EffectPlanError):runner.recover('original','a'*64,STEPS,context='admitted')
            self.assertEqual(api.calls,before)
            self.assertEqual(c.db.execute('SELECT count(*) FROM effect_plan').fetchone()[0],0)
            c.close()
if __name__=='__main__':unittest.main()
