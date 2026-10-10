"""Explicit optional COUNT/HAVING proof against actual compiler output; no native claim."""
import copy,json,os,unittest
from pathlib import Path
from weft_field_plan import admit_field_plan
from run_commerce_arithmetic_weft import execute_guarded,admit_count_result_rows,NativeGuardRefusal
FIXTURES=os.environ.get('ASHLAR_HAVING_COMPILER_FIXTURES')
class OriginalMediaOracle(unittest.TestCase):
    def test_original_optional_null_group_and_exact_seeded_ids(self):
        from run_pack_count_having_weft import control_oracles
        root=Path(__file__).resolve().parents[1]
        raw=(root/'examples/domain-packs/archaeology/upstream/graph/fixture.json').read_bytes()
        groups,controls=control_oracles(raw);expected={name:rows for name,sql,rows in controls}
        self.assertIn(['[42,0,"AS4"]','2'],groups)
        self.assertEqual(expected['global'],[['2']]);self.assertEqual(expected['global-empty'],[['0']])
        self.assertEqual(expected['all-null-groups'],[['[42,0,"AS5"]','0']])
        graph=json.loads(raw);obj=next(o for o in graph['objects']if o['type']['element']=='asset_subjects');del obj['values']['asset_subjects.context_id']
        with self.assertRaises(ValueError):control_oracles(json.dumps(graph).encode())
    def test_opt_in_retains_original_model_sql_and_only_selected_home(self):
        from run_pack_count_having_weft import opt_in
        p=Path(FIXTURES or '/private/tmp/ashlar-weft-media-source-20261009-a')
        if not p.exists():self.skipTest('Actual compiler fixture needed')
        r=json.loads((p/'original-request.json').read_bytes());before=copy.deepcopy(r)
        selected=opt_in(r);self.assertEqual(r,before);self.assertEqual(selected['modules'],r['modules']);self.assertEqual(selected['sql'],r['sql'])
        b=json.loads(r['target']['bindingJson']);after=json.loads(selected['target']['bindingJson'])
        for record in b['records']:
            for prop in record['properties']:
                if prop['logical']['element']=='asset_subjects.context_id':prop['home']['encoding']='ashlar-weft-json-native-null/0.1-candidate'
        self.assertEqual(after,b)
@unittest.skipUnless(FIXTURES,'Actual public compiler fixtures required')
class HavingAdmission(unittest.TestCase):
    def case(self,name='original'):
        p=Path(FIXTURES);r=json.loads((p/(name+'-request.json')).read_bytes());a=json.loads((p/(name+'-artifact.json')).read_bytes());return r,a,json.loads(r['target']['bindingJson'])
    def test_actual_compiled_optional_route_reaches_interval(self):
        class Reached(Exception):pass
        class Provider:
            def interval(self,context):raise Reached()
            def sql_ordered(self,*args):raise AssertionError("No SQL before interval")
        for name in ('original','global','global-empty','all-null-groups','having-excludes-all','global-having-empty'):
            r,a,b=self.case(name);admit_field_plan(a,b,r['modules'],count_having=True)
            with self.assertRaises(ValueError):admit_field_plan(a,b,r['modules'])
            with self.assertRaises(ValueError):admit_field_plan(a,b,r['modules'],count_distinct=True)
            with self.assertRaises(Reached):execute_guarded(Provider(),r,a,context=None,count_having=True)
    def test_exact_empty_having_and_valid_joined_plan_scope_refusal(self):
        r,a,b=self.case('global-empty');self.assertNotIn('having',a['logicalPlan'])
        a['logicalPlan']['having']=[]
        with self.assertRaisesRegex(ValueError,'Single original Record and omitted-or-single HAVING'):
            admit_field_plan(a,b,r['modules'],count_having=True)
        r,a,b=self.case('joined-required');self.assertTrue(a['logicalPlan']['joins'])
        with self.assertRaisesRegex(ValueError,'Single original Record and omitted-or-single HAVING'):
            admit_field_plan(a,b,r['modules'],count_having=True)
    def test_custody_mutations_refuse_before_callbacks(self):
        class Never:
            def __getattr__(self,name):raise AssertionError('Premature callback '+name)
        r,a,b=self.case();mutations=[]
        for cap in ('aggregate','group','aggregate.countDistinct','aggregate.countDistinct.optional','aggregate.havingCountDistinctGreater','value.nativeNull'):
            m=copy.deepcopy(a);m['logicalPlan']['requiredCapabilities'].remove(cap);mutations.append(m)
        m=copy.deepcopy(a);m['logicalPlan']['having'][0]['threshold']['value']='9223372036854775808';mutations.append(m)
        m=copy.deepcopy(a);m['logicalPlan']['having']=[];mutations.append(m)
        m=copy.deepcopy(a);m['logicalPlan']['joins']=[{}];mutations.append(m)
        m=copy.deepcopy(a);t=m['logicalPlan']['having'][0]['threshold'];t['value']='0'*1025+'1'
        next(q for q in m['parameters']if q['origin'].get('sourceSpan')==t['span'])['value']=t['value'];mutations.append(m)
        m=copy.deepcopy(a);m['logicalPlan']['having'][0]['threshold']['kind']='parameter';mutations.append(m)
        m=copy.deepcopy(a);m['logicalPlan']['having'][0]['count']['argument']['identity']['revision']='wrong';mutations.append(m)
        m=copy.deepcopy(a);m['logicalPlan']['typeGraph'][1]['availability']='required';mutations.append(m)
        m=copy.deepcopy(a);next(o for o in m['obligations']if o['id']=='ashlar.arithmetic.exact')['parameters']['checks']=[];mutations.append(m)
        for m in mutations:
            with self.assertRaises(ValueError):execute_guarded(Never(),r,m,context=None,count_having=True)
        with self.assertRaises(ValueError):execute_guarded(Never(),r,a,context=None)
    def test_pre_having_capacity_failure_withholds_user_query(self):
        from contextlib import contextmanager
        from types import SimpleNamespace
        r,a,b=self.case();guard=next(o for o in a['obligations']if o['id']=='ashlar.arithmetic.exact')['parameters']['checks'][0]['sql']
        class Provider:
            def __init__(self):self.calls=[]
            @contextmanager
            def interval(self,context):yield
            def resolve(self,context):return SimpleNamespace(descriptor=SimpleNamespace(raw={}),snapshots=())
            def admit_binding(self,*args):pass
            def runtime(self,*args):pass
            def sql_ordered(self,*args):raise AssertionError("Capacity failure must withhold user SQL")
            def sql(self,sql,params):
                self.calls.append(sql);return [{'violations':'1' if sql==guard else '0'}]
        p=Provider()
        with self.assertRaises(NativeGuardRefusal):execute_guarded(p,r,a,context=None,count_having=True)
        self.assertNotIn(a['sql'],p.calls)
    def test_missing_ordered_transport_refuses_before_interval_callback(self):
        class Never:
            sql_ordered=None
            def __getattr__(self,name):raise AssertionError('Premature callback '+name)
        r,a,b=self.case()
        with self.assertRaisesRegex(ValueError,'requires exact native schema/ordered-cell transport'):
            execute_guarded(Never(),r,a,context=None,count_having=True)
    def test_native_actual_schema_and_ordered_cells_are_mandatory(self):
        from run_commerce_arithmetic_weft import admit_count_ordered_cells
        r,a,b=self.case();names=[c['outputName']for c in a['columns']]
        actual={'schema':[[n,'STRING']for n in names],'rows':[['asset','2']]}
        self.assertIs(admit_count_ordered_cells(a,actual),actual)
        for change in ('schema-order','schema-type','width','numeric','null'):
            m=copy.deepcopy(actual)
            if change=='schema-order':m['schema'].reverse()
            elif change=='schema-type':m['schema'][1][1]='BIGINT'
            elif change=='width':m['rows'][0].pop()
            elif change=='numeric':m['rows'][0][1]=2
            else:m['rows'][0][0]=None
            with self.assertRaises(ValueError):admit_count_ordered_cells(a,m)
    def test_global_having_zero_or_one_and_grouped_complete_tuple(self):
        r,a,b=self.case('global-having-empty');admit_field_plan(a,b,r['modules'],count_having=True)
        name=a['columns'][0]['outputName'];admit_count_result_rows(a,[],count_having=True)
        admit_count_result_rows(a,[{name:'0'}],count_having=True)
        for rows in ([{name:'0'},{name:'1'}],[{name:False}],[{name:'00'}]):
            with self.assertRaises(ValueError):admit_count_result_rows(a,rows,count_having=True)
        with self.assertRaises(ValueError):admit_count_result_rows(a,[],count_having=False)
        r,a,b=self.case();names=[c['outputName']for c in a['columns']];row={names[0]:'x',names[1]:'0'}
        with self.assertRaises(ValueError):admit_count_result_rows(a,[row,row],count_having=True)
if __name__=='__main__':unittest.main()
