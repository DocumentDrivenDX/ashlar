from contextlib import contextmanager
import unittest
from ashlar.pins import PinVector,PostgresPins,PinError
from ashlar.native import SQLResult
class Policy:
    def admit_registration(self,v,c):
        if c!='admitted':raise PermissionError('Denied')
    authorize_read=admit_registration
class Executor:
    def __init__(self):self.rows=[];self.open=False;self.calls=[]
    @contextmanager
    def transaction(self,c):
        prior=list(self.rows);self.open=True
        try:yield self
        except BaseException:self.rows=prior;raise
        finally:self.open=False
    def query(self,sql,p):
        assert self.open;self.calls.append(sql)
        if 'ashlar_pins.register(' in sql:
            self.rows.append({'table_name':p['table'],'table_uuid':p['uuid'],'version':p['version'],'digest':p['digest'],'released':False})
        elif 'assert_active(' in sql:
            if not any(r['table_name']==p['table'] and not r['released'] for r in self.rows):raise PinError('Missing pin')
        else:return SQLResult(self.rows)
        return SQLResult([])
class PinTests(unittest.TestCase):
    def vector(self):return PinVector('authority','manifest','p','a'*64,{'c.s.a':('ua',1),'c.s.b':('ub',2)})
    def test_complete_register_and_read_scope_holds_transaction(self):
        e=Executor();p=PostgresPins(e,Policy());v=self.vector();p.register(v,context='admitted')
        with p.hold(v,context='admitted') as held:self.assertTrue(e.open);self.assertIs(held,v)
        self.assertFalse(e.open)
    def test_partial_released_and_changed_version_refuse(self):
        for change in ['missing','released','version']:
            e=Executor();p=PostgresPins(e,Policy());v=self.vector();p.register(v,context='admitted')
            if change=='missing':e.rows.pop()
            elif change=='released':e.rows[0]['released']=True
            else:e.rows[0]['version']='3'
            with self.assertRaises(PinError):
                with p.hold(v,context='admitted'):self.fail('Incomplete pin admitted')
    def test_invalid_vector_and_denial(self):
        with self.assertRaises(PinError):PinVector('a','manifest','p','a'*64,{'c.s.t':('u',True)})
        e=Executor();p=PostgresPins(e,Policy())
        with self.assertRaises(PermissionError):p.register(self.vector(),context='denied')
        self.assertEqual(e.calls,[])
if __name__=='__main__':unittest.main()
