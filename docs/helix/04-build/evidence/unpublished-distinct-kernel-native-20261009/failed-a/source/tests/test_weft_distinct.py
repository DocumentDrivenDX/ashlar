"""Closed DISTINCT host admission; actual compiler fixture replay is explicit."""
import copy,json,os,unittest
from pathlib import Path
from weft_field_plan import admit_field_plan
from run_commerce_arithmetic_weft import execute_guarded

FIXTURES=os.environ.get('ASHLAR_DISTINCT_COMPILER_FIXTURES')

@unittest.skipUnless(FIXTURES,'Explicit actual compiler request/artifact directory required')
class DistinctAdmission(unittest.TestCase):
    def case(self,name='original'):
        root=Path(FIXTURES);request=json.loads((root/(name+'-request.json')).read_bytes());artifact=json.loads((root/(name+'-artifact.json')).read_bytes());return request,artifact,json.loads(request['target']['bindingJson'])

    def test_original_repeated_positions_and_projected_order_limit(self):
        for name in ('original','repeated-position','projected-order-limit'):
            request,artifact,binding=self.case(name)
            admit_field_plan(artifact,binding,request['modules'],distinct=True)
            with self.assertRaises(ValueError):admit_field_plan(artifact,binding,request['modules'])

    def test_unknown_flag_missing_capability_and_hidden_order_refuse(self):
        request,original,binding=self.case('projected-order-limit');mutations=[]
        for flag in (1,False,None,'true'):
            a=copy.deepcopy(original);a['logicalPlan']['distinct']=flag;mutations.append(a)
        a=copy.deepcopy(original);a['logicalPlan']['requiredCapabilities'].remove('project.distinct');mutations.append(a)
        for capability in ('limit','order.asc','type.string'):
            a=copy.deepcopy(original);a['logicalPlan']['requiredCapabilities'].remove(capability);mutations.append(a)
        a=copy.deepcopy(original);a['logicalPlan']['order'][0]['scan']='unknown';mutations.append(a)
        a=copy.deepcopy(original);a['logicalPlan']['outputs'][0]['expression']['op']='arithmetic';mutations.append(a)
        a=copy.deepcopy(original);a['columns'][0]['representation']['logicalType']['family']='decimal';mutations.append(a)
        for a in mutations:
            with self.assertRaises(ValueError):admit_field_plan(a,binding,request['modules'],distinct=True)

    def test_default_route_and_bad_proof_refuse_before_provider_callback(self):
        class Never:
            def __getattr__(self,name):raise AssertionError('Provider callback before DISTINCT admission: '+name)
        request,artifact,binding=self.case()
        for optin,changed in ((False,False),(True,True)):
            a=copy.deepcopy(artifact)
            if changed:a['logicalPlan']['distinct']=1
            with self.assertRaises(ValueError):execute_guarded(Never(),request,a,context=None,distinct=optin)

    def test_real_operator_free_distinct_reaches_provider_interval(self):
        class Reached(Exception):pass
        class Provider:
            def interval(self,context):raise Reached('Actual admitted DISTINCT reached held provider interval')
        request,artifact,binding=self.case('operator-free')
        self.assertEqual(artifact['obligations'][next(i for i,o in enumerate(artifact['obligations'])if o['id']=='ashlar.arithmetic.exact')]['parameters']['checks'],[])
        with self.assertRaises(Reached):execute_guarded(Provider(),request,artifact,context=None,distinct=True)

    def test_limit_boolean_and_combined_null_optin_refuse(self):
        request,artifact,binding=self.case('projected-order-limit');artifact['logicalPlan']['limit']=True
        with self.assertRaises(ValueError):admit_field_plan(artifact,binding,request['modules'],distinct=True)
        request,artifact,binding=self.case()
        with self.assertRaises(ValueError):admit_field_plan(artifact,binding,request['modules'],distinct=True,native_null=True)


class IndependentDistinctFixture(unittest.TestCase):
    def test_source_lexicals_join_bag_and_order_oracle_are_independent(self):
        from prepare_native_distinct_controls import rows,controls,fixture_model
        original=rows();self.assertEqual(len(original),6)
        self.assertEqual([r['value']for r in original],['A','A','A ','é','e\u0301',''])
        expected=controls();self.assertEqual(expected[0]['expected'],[[''],['A'],['A '],['e\u0301'],['é']])
        self.assertEqual(expected[1]['expected'],[[r[0],r[0]]for r in expected[0]['expected']])
        self.assertEqual(expected[2]['expected'],[]);self.assertEqual(expected[3]['expected'],[[''],['A']])
        model=fixture_model();self.assertTrue(all(e['nullability']=='required'and e['scalarType']=='string'for e in model['modules'][0]['elements']if e['kind']=='field'))

if __name__=='__main__':unittest.main()
