from dataclasses import replace
import unittest
from ashlar.apply import *

A=EntityKey('source','object',1,1)
B=EntityKey('source','object',1,2)
I=EntityKey('source','object',1,3)
E=EntityKey('source','edge',2,1)
F=EntityKey('source','edge',2,2)

def change(key,version,op='create',props='{}',endpoints=None):
    return Change('feed','epoch',key.kind+str(key.id)+'-'+str(version),'a'*64,op,EntityState(key,version,'accepted-1',props,'{"unknown":null}',endpoints))
def admit(change):
    if change.state.schema_revision!='accepted-1':raise PermissionError('Unsupported schema revision')

class ApplyTests(unittest.TestCase):
    def apply(self,old,changes):return plan_apply(old,changes,schema_policy=admit)
    def test_create_parallel_edges_isolated_node_and_late_endpoint(self):
        changes=[change(E,1,endpoints=(A,B)),change(A,1),change(B,1),change(I,1),change(F,1,endpoints=(A,B))]
        state=self.apply(empty_state(),changes)
        self.assertEqual(len(state.current),5)
        self.assertEqual(len(state.history),5)
        self.assertEqual(state.current[E].endpoints,state.current[F].endpoints)
        replay=self.apply(state,changes)
        self.assertEqual(replay,state)
    def test_replace_delete_replay_retains_exact_history_and_tombstone(self):
        initial=change(A,1,props='{"23":null}')
        updated=change(A,2,'replace',props='{"23":"new","unknown":18446744073709551615}')
        deleted=replace(updated,delivery_id='delete',operation='delete',state=replace(updated.state,version=3))
        state=self.apply(empty_state(),[initial,updated,deleted])
        self.assertFalse(state.current)
        self.assertEqual(state.tombstones[A],deleted)
        self.assertEqual(len(state.history),3)
        self.assertEqual(self.apply(state,[initial,updated,deleted]),state)
        with self.assertRaises(TypeError):state.current[A]=initial.state
    def test_conflict_stale_resurrection_refuse_without_mutating_prior(self):
        first=change(A,1);prior=self.apply(empty_state(),[first])
        for bad in [replace(first,raw_digest='b'*64),replace(first,delivery_id='different'),change(A,0,'replace'),change(A,2,'create')]:
            with self.assertRaises(ApplyError):self.apply(prior,[bad])
        self.assertEqual(prior.current[A],first.state)
        self.assertEqual(len(prior.history),1)
    def test_missing_typed_endpoint_and_incident_node_delete_refuse(self):
        with self.assertRaises(ApplyError):self.apply(empty_state(),[change(E,1,endpoints=(A,B))])
        prior=self.apply(empty_state(),[change(A,1),change(B,1),change(E,1,endpoints=(A,B))])
        with self.assertRaises(ApplyError):self.apply(prior,[change(A,2,'delete')])
        self.assertIn(A,prior.current)
        final=self.apply(prior,[change(A,2,'delete'),change(E,2,'delete',endpoints=(A,B))])
        self.assertEqual(set(final.current),{B})
    def test_policy_required_and_false_is_not_success(self):
        with self.assertRaises(ApplyError):plan_apply(empty_state(),[change(A,1)],schema_policy=lambda _:False)
        with self.assertRaises(PermissionError):self.apply(empty_state(),[replace(change(A,1),state=replace(change(A,1).state,schema_revision='unknown'))])
if __name__=='__main__':unittest.main()
