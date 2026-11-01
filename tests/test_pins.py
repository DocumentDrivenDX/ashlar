# SPDX-License-Identifier: MIT OR Apache-2.0
from contextlib import contextmanager
import unittest
from ashlar.pins import PinVector,PostgresPins,PinError
from ashlar.native import SQLResult
class Policy:
    def admit_registration(self,v,c):
        if c!='admitted':raise PermissionError('Denied')
    authorize_read=admit_registration
class Executor:
    def __init__(self):self.rows=[];self.open=False;self.calls=[];self.quarantined=False
    @contextmanager
    def transaction(self,c):
        prior=list(self.rows);self.open=True
        try:yield self
        except BaseException:self.rows=prior;raise
        finally:self.open=False
    def query(self,sql,p):
        assert self.open;self.calls.append(sql)
        if 'assert_uuid_available(' in sql:
            if self.quarantined:raise PinError('Unresolved cleanup')
            return SQLResult([])
        if 'ashlar_pins.register(' in sql:
            self.rows.append({'table_name':p['table'],'table_uuid':p['uuid'],'version':p['version'],'digest':p['digest'],'released':False})
        elif 'assert_active(' in sql:
            if not any(r['table_name']==p['table'] and not r['released'] for r in self.rows):raise PinError('Missing pin')
        else:return SQLResult(self.rows)
        return SQLResult([])
class PinTests(unittest.TestCase):
    def test_pending_cleanup_refuses_held_read_before_yield(self):
        e=Executor();p=PostgresPins(e,Policy());v=self.vector();p.register(v,context='admitted');e.quarantined=True
        with self.assertRaises(PinError):
            with p.hold(v,context='admitted'):self.fail('Quarantined read admitted')
        self.assertFalse(e.open)
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
    def test_live_source_bound_handle_rechecks_complete_vector_in_same_transaction(self):
        e=Executor();v=self.vector();source=object();context='admitted';events=[]
        class SourcePolicy(Policy):
            def authorize_source_bound_read(self,actual,selection,supplied):
                events.append((actual,selection,supplied))
                if actual is not v or selection is not source or supplied is not context:
                    raise PermissionError('Source selection denied')
        p=PostgresPins(e,SourcePolicy());p.register(v,context=context)
        events.clear();e.calls.clear();handle=p.open_live(v,source,context=context)
        self.assertTrue(e.open)
        self.assertEqual(len(events),1)
        self.assertEqual(e.calls.count('SELECT ashlar_pins.assert_uuid_available(:uuid)'),2)
        handle.renew()
        self.assertTrue(e.open)
        self.assertEqual(len(events),2)
        self.assertEqual(e.calls.count('SELECT ashlar_pins.assert_uuid_available(:uuid)'),4)
        handle.close()
        self.assertFalse(e.open)
        self.assertEqual(len(events),3)
        self.assertEqual(e.calls.count('SELECT ashlar_pins.assert_uuid_available(:uuid)'),6)
        with self.assertRaises(PinError):handle.renew()

    def test_live_handle_refuses_missing_source_authority_and_vector_drift(self):
        e=Executor();v=self.vector();source=object();context='admitted'
        p=PostgresPins(e,Policy());p.register(v,context=context)
        with self.assertRaisesRegex(PinError,'Source-bound pin authorization unavailable'):
            p.open_live(v,source,context=context)
        self.assertFalse(e.open)
        class SourcePolicy(Policy):
            def authorize_source_bound_read(self,actual,selection,supplied):return None
        p=PostgresPins(e,SourcePolicy());handle=p.open_live(v,source,context=context)
        e.rows.pop()
        with self.assertRaises(PinError):handle.renew()
        with self.assertRaises(PinError):handle.close()
        self.assertFalse(e.open)

    def test_live_handle_final_failure_preserves_body_primary(self):
        e=Executor();v=self.vector();source=object();context='admitted'
        class SourcePolicy(Policy):
            def authorize_source_bound_read(self,actual,selection,supplied):return None
        p=PostgresPins(e,SourcePolicy());p.register(v,context=context)
        primary=KeyboardInterrupt('body cancellation')
        with self.assertRaises(KeyboardInterrupt) as caught:
            with p.open_live(v,source,context=context):
                e.rows.pop()
                raise primary
        self.assertIs(caught.exception,primary)
        self.assertFalse(e.open)

    def test_caught_renewal_failure_poison_closes_live_interval(self):
        e=Executor();v=self.vector();source=object();context='admitted'
        class SourcePolicy(Policy):
            def authorize_source_bound_read(self,actual,selection,supplied):return None
        p=PostgresPins(e,SourcePolicy());p.register(v,context=context)
        handle=p.open_live(v,source,context=context)
        e.quarantined=True
        with self.assertRaises(PinError):handle.renew()
        self.assertFalse(e.open)
        e.quarantined=False
        with self.assertRaisesRegex(PinError,'closed'):handle.renew()
        with self.assertRaisesRegex(PinError,'closed'):handle.close()

if __name__=='__main__':unittest.main()
