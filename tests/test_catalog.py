import unittest
from ashlar.catalog import Identity, MappingEntry, CatalogPlanError, MAX_ID, plan_catalog_ids

A=Identity('type',('doc','module','a'))
B=Identity('type',('doc','module','b'))
P=Identity('property',('doc','module','a','module','label'))

class CatalogIdentityTests(unittest.TestCase):
    def plan(self,old,new,water=None):
        return plan_catalog_ids(old,new,highwater=water or {'type':17,'property':23,'relationship':0},expected_head='7')

    def test_preserves_authoritative_ids_and_reservations(self):
        old=[MappingEntry(A,17,True),MappingEntry(P,23,True)]
        plan=self.plan(old,[P,B,A])
        self.assertEqual([(e.identity,e.catalog_id) for e in plan.allocated],[(B,18)])
        self.assertEqual({e.identity:e.catalog_id for e in plan.entries},{A:17,P:23,B:18})
        self.assertEqual(dict(plan.highwater),{'type':18,'property':23,'relationship':0})
        with self.assertRaises(TypeError): plan.highwater['type']=1
        self.assertEqual(old[0],MappingEntry(A,17,True))

    def test_retirement_reactivation_and_distinct_incarnation(self):
        retired=self.plan([MappingEntry(A,17,True)],[B])
        self.assertEqual(retired.retired,(MappingEntry(A,17,False),))
        revived=self.plan(retired.entries,[A,B],dict(retired.highwater))
        self.assertEqual(revived.reactivated,(MappingEntry(A,17,True),))
        self.assertFalse(revived.allocated)
        self.assertEqual(dict(revived.highwater)['type'],18)

    def test_qualification_and_owner_do_not_collapse(self):
        otherdoc=Identity('type',('other','module','a'))
        otherowner=Identity('property',('doc','module','b','module','label'))
        plan=self.plan([MappingEntry(A,17,True),MappingEntry(P,23,True)],[A,B,P,otherdoc,otherowner])
        self.assertEqual({e.identity:e.catalog_id for e in plan.allocated},{B:18,otherdoc:19,otherowner:24})

    def test_never_reuses_highwater_gaps(self):
        plan=self.plan([], [A],{'type':999,'property':0,'relationship':0})
        self.assertEqual(plan.allocated[0].catalog_id,1000)

    def test_refuses_duplicate_and_invalid_state(self):
        for old,new,water in [([MappingEntry(A,17,True),MappingEntry(B,17,True)],[A],None),([], [P],None),([], [A,A],None),([MappingEntry(A,18,True)],[A],None),([], [Identity('type',('doc','m'))],None),([], [A],{'type':True,'property':0,'relationship':0})]:
            with self.subTest(old=old,new=new):
                with self.assertRaises(CatalogPlanError): self.plan(old,new,water)

    def test_exhaustion_does_not_prevent_existing_identity(self):
        water={'type':MAX_ID,'property':0,'relationship':0}
        self.assertFalse(self.plan([MappingEntry(A,17,False)],[A],water).allocated)
        with self.assertRaises(CatalogPlanError):self.plan([MappingEntry(A,17,True)],[A,B],water)

if __name__=='__main__':unittest.main()
