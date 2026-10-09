import copy,hashlib,json,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from weft_field_plan import admit_field_plan
from run_commerce_arithmetic_weft import execute_guarded
import test_commerce_arithmetic_weft as fixtures

ROOT=Path(__file__).resolve().parents[1]
INVENTORY=ROOT/'docs/helix/04-build/evidence/original-pack-weft-unqualified-compilation-20261009.json'
FIXTURES=ROOT/'tests/fixtures/weft-field-plans'


class FieldPlanAdmissionTests(unittest.TestCase):
    def fixture(self, case='specialists'):
        entry=next(c for c in json.loads(INVENTORY.read_bytes())['cases'] if c['case']==case)
        artifact=json.loads((FIXTURES/(entry['pack']+'-'+case+'-stdout.json')).read_bytes())
        request=json.loads((FIXTURES/(entry['pack']+'-'+case+'-request.json')).read_bytes())
        return request,artifact,json.loads(request['target']['bindingJson'])

    def refuse_before_sql(self, request,artifact):
        provider=fixtures.ArithmeticPublicationHostTests().provider()
        with self.assertRaises(ValueError):
            execute_guarded(provider,request,artifact,context={})
        self.assertEqual(provider.calls,[])

    def test_all_six_original_compiled_field_plans_admit(self):
        frozen=json.loads((FIXTURES/'manifest.json').read_bytes())
        self.assertEqual(hashlib.sha256(INVENTORY.read_bytes()).hexdigest(),frozen['inventory_sha256'])
        for entry in frozen['files']:
            self.assertEqual(hashlib.sha256((FIXTURES/entry['file']).read_bytes()).hexdigest(),entry['sha256'])
        cases=[c['case']for c in json.loads(INVENTORY.read_bytes())['cases']if c['status']=='compiled']
        self.assertEqual(len(cases),6)
        for case in cases:
            request,artifact,binding=self.fixture(case)
            admit_field_plan(artifact,binding,request['modules'])

    def test_unknown_arithmetic_scope_and_model_mutations_refuse_before_sql(self):
        request,original,_=self.fixture();mutations=[]
        a=copy.deepcopy(original);a['logicalPlan']['joins'][0]['on'][0]['op']='arithmeticCompare';mutations.append(a)
        a=copy.deepcopy(original);a['logicalPlan']['joins'][0]['on'][0]['predicate']['left']['scan']='s5';mutations.append(a)
        a=copy.deepcopy(original);a['logicalPlan']['joins'][0]['right']['occurrence']='s0';mutations.append(a)
        a=copy.deepcopy(original);a['logicalPlan']['joins'][0]['right']['pin']['revision']='changed';mutations.append(a)
        a=copy.deepcopy(original);a['logicalPlan']['outputs'][0]['expression']['scan']='s1';mutations.append(a)
        a=copy.deepcopy(original);a['logicalPlan']['joins'][0]['on'][0]['predicate']['left']['type']['family']='integer';mutations.append(a)
        a=copy.deepcopy(original);a['logicalPlan']['joins'][0]['on'][0]['predicate']['left']['type']['nullable']=True;mutations.append(a)
        a=copy.deepcopy(original);a['columns'][0]['sourceIdentities']=[];mutations.append(a)
        a=copy.deepcopy(original);a['columns'][0]['representation']['decoder']='float';mutations.append(a)
        a=copy.deepcopy(original);a['columns'][0]['position']=True;mutations.append(a)
        a=copy.deepcopy(original);a['logicalPlan']['typeGraph'].append({'future':'meaning'});mutations.append(a)
        a=copy.deepcopy(original);a['logicalPlan']['typeGraph'][0]['type']['facets']={'shadow':True};a['columns'][0]['representation']['logicalType']['facets']={'shadow':True};mutations.append(a)
        for artifact in mutations:self.refuse_before_sql(request,artifact)
        changed=copy.deepcopy(request);changed['modules'][0]['documentJson']+=' '
        self.refuse_before_sql(changed,original)

    def test_every_consumed_field_retains_schema_public_and_capacity_guards(self):
        request,original,_=self.fixture()
        for flag in (None,'publicSourceOnly','representabilityOnly'):
            artifact=copy.deepcopy(original)
            obligation=next(o for o in artifact['obligations']if o['id']=='ashlar.candidate.scalarIntegrity')
            guards=obligation['parameters']['checks']
            chosen=next(c for c in guards if c['field']['element']=='fauna_results.nisp'and (c.get(flag)is True if flag else not c.get('publicSourceOnly')and not c.get('representabilityOnly')))
            guards.remove(chosen)
            self.refuse_before_sql(request,artifact)
        for key,value in (('bits',64.0),('signed',1)):
            artifact=copy.deepcopy(original)
            slot=next(p for p in artifact['parameters']if p['origin'].get('kind')=='typeDiscriminator')
            slot['logicalType']['facets']['integerWidth'][key]=value
            self.refuse_before_sql(request,artifact)

    def test_string_literal_slot_custody_and_numeric_predicate_refuse(self):
        request,original,_=self.fixture('missing-media')
        artifact=copy.deepcopy(original);artifact['parameters'][-1]['value']='changed'
        self.refuse_before_sql(request,artifact)
        artifact=copy.deepcopy(original);artifact['logicalPlan']['filters'][0]['predicate']['right']['type']['family']='decimal'
        self.refuse_before_sql(request,artifact)

    def test_named_string_parameter_retains_exact_occurrence_and_slot(self):
        request=json.loads((FIXTURES/'named-string-request.json').read_bytes())
        artifact=json.loads((FIXTURES/'named-string-stdout.json').read_bytes())
        binding=json.loads(request['target']['bindingJson'])
        admit_field_plan(artifact,binding,request['modules'])
        for mutation in ('name','value','logicalType','facets','span','empty-name'):
            bad=copy.deepcopy(artifact)
            slot=next(p for p in bad['parameters']if p['origin'].get('kind')=='namedParameter')
            if mutation=='name':slot['origin']['name']='other'
            elif mutation=='value':slot['value']='changed'
            elif mutation=='logicalType':slot['logicalType']['family']='boolean'
            elif mutation=='facets':
                slot['logicalType']['facets']={'shadow':True}
                bad['logicalPlan']['filters'][0]['predicate']['right']['type']['facets']={'shadow':True}
            elif mutation=='span':
                slot['origin']['sourceSpan']={'start':True,'end':0}
                bad['logicalPlan']['filters'][0]['predicate']['right']['span']={'start':True,'end':0}
            else:
                slot['origin']['name']=''
                bad['logicalPlan']['filters'][0]['predicate']['right']['name']=''
            self.refuse_before_sql(request,bad)

    def test_actual_compiled_conjunction_and_order_keep_field_custody(self):
        request=json.loads((FIXTURES/'named-string-and-order-request.json').read_bytes())
        artifact=json.loads((FIXTURES/'named-string-and-order-stdout.json').read_bytes())
        self.assertIn('and',artifact['logicalPlan']['requiredCapabilities'])
        self.assertIn('order.asc',artifact['logicalPlan']['requiredCapabilities'])
        admit_field_plan(artifact,json.loads(request['target']['bindingJson']),request['modules'])
        bad=copy.deepcopy(artifact);bad['logicalPlan']['order'][0]['scan']='unknown'
        self.refuse_before_sql(request,bad)
        bad=copy.deepcopy(artifact);bad['logicalPlan']['filters'][1]['predicate']['left']['type']['nullable']=True
        self.refuse_before_sql(request,bad)

    def test_guards_and_successful_cleanup_still_precede_rows(self):
        request,artifact,_=self.fixture('missing-media')
        p=fixtures.ArithmeticPublicationHostTests().provider()
        def sql(text,parameters):
            p.calls.append(text)
            return [{'result':'buffered'}] if text==artifact['sql'] else [{'violations':'0'}]
        p.sql=sql
        result=execute_guarded(p,request,artifact,context={})
        self.assertEqual(result['rows'],[{'result':'buffered'}])
        self.assertEqual(p.calls[-1],artifact['sql'])
        self.assertEqual(len(p.calls),len(next(o for o in artifact['obligations']if o['id']=='ashlar.candidate.scalarIntegrity')['parameters']['checks'])+1)
        p=fixtures.ArithmeticPublicationHostTests().provider(fail_close=True)
        p.sql=sql
        with self.assertRaisesRegex(ValueError,'closing'):
            execute_guarded(p,request,artifact,context={})


class NativeFieldRunBoundaryTests(unittest.TestCase):
    def test_independent_expected_integer_text_never_uses_float(self):
        from run_pack_field_weft import exact_expected
        self.assertEqual(exact_expected([[9007199254740993,'雪']]),[['9007199254740993','雪']])
        for value in (9007199254740992.0,True,None):
            with self.assertRaises(ValueError):exact_expected([[value]])


if __name__=='__main__':unittest.main()
