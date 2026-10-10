"""Explicit count/IN host route against actual public compiler artifacts."""
import copy,json,os,unittest
from pathlib import Path
from weft_field_plan import admit_field_plan
from run_commerce_arithmetic_weft import execute_guarded,admit_count_result_rows
FIXTURES=os.environ.get('ASHLAR_COUNT_COMPILER_FIXTURES')
class ControlOracle(unittest.TestCase):
    def test_original_authored_order_is_observable(self):
        from run_pack_count_distinct_weft import admit_ordered_rows
        expected=[['dissolved-oxygen','2'],['temperature','2']]
        admit_ordered_rows(expected,expected)
        with self.assertRaises(ValueError):admit_ordered_rows(expected[::-1],expected)

    def test_actual_retained_graph_bytes(self):
        from ecology_graph_oracle import ROOT
        from run_pack_count_distinct_weft import control_oracles
        names,controls=control_oracles((ROOT/'graph/fixture.json').read_bytes())
        self.assertEqual(dict((n,e)for n,sql,e in controls),{'global':[['3']],'global-empty':[['0']],'grouped-empty':[],'literal-duplicates':[['dissolved-oxygen'],['temperature']]})
        with self.assertRaises(ValueError):control_oracles({'objects':[]})

@unittest.skipUnless(FIXTURES,'Actual compiler fixture directory required')
class CountAdmission(unittest.TestCase):
    def case(self,name='original'):
        p=Path(FIXTURES);r=json.loads((p/(name+'-request.json')).read_bytes());a=json.loads((p/(name+'-artifact.json')).read_bytes());return r,a,json.loads(r['target']['bindingJson'])
    def test_actual_five_admitted_only_explicitly(self):
        for name in ('original','global','global-empty','grouped-empty','literal-duplicates'):
            r,a,b=self.case(name);admit_field_plan(a,b,r['modules'],count_distinct=True)
            with self.assertRaises(ValueError):admit_field_plan(a,b,r['modules'])
    def test_mutations_refuse_before_callback(self):
        class Never:
            def __getattr__(self,name):raise AssertionError('Premature callback '+name)
        r,a,b=self.case();mutations=[]
        for cap in ('aggregate','group','aggregate.countDistinct','predicate.stringIn','equal','and'):
            m=copy.deepcopy(a);m['logicalPlan']['requiredCapabilities'].remove(cap);mutations.append(m)
        m=copy.deepcopy(a);m['columns'][1]['representation']['logicalType']['nullable']=0;mutations.append(m)
        m=copy.deepcopy(a);m['logicalPlan']['outputs'][1]['expression']['argument']['identity']['revision']='wrong';mutations.append(m)
        m=copy.deepcopy(a);next(o for o in m['obligations']if o['id']=='ashlar.arithmetic.exact')['parameters']['checks']=[];mutations.append(m)
        m=copy.deepcopy(a)
        operand=m['logicalPlan']['filters'][1]['predicate']['right']
        operand.update(kind='parameter',name='injected')
        slot=next(q for q in m['parameters']if q['origin'].get('sourceSpan')==operand['span'])
        slot['origin']={'kind':'namedParameter','name':'injected','sourceSpan':operand['span']}
        mutations.append(m)
        for m in mutations:
            with self.assertRaises(ValueError):execute_guarded(Never(),r,m,context=None,count_distinct=True)
        with self.assertRaises(ValueError):execute_guarded(Never(),r,a,context=None)
    def test_actual_global_reaches_interval(self):
        class Reached(Exception):pass
        class Provider:
            def interval(self,context):raise Reached()
        r,a,b=self.case('global')
        with self.assertRaises(Reached):execute_guarded(Provider(),r,a,context=None,count_distinct=True)
    def test_native_capacity_refusal_withholds_user_sql(self):
        from contextlib import contextmanager
        from types import SimpleNamespace
        from run_commerce_arithmetic_weft import NativeGuardRefusal
        r,a,b=self.case('global');guard=next(o for o in a['obligations']if o['id']=='ashlar.arithmetic.exact')['parameters']['checks'][0]['sql']
        class Provider:
            calls=[]
            @contextmanager
            def interval(self,context):yield
            def resolve(self,context):return SimpleNamespace(descriptor=SimpleNamespace(raw={}),snapshots=())
            def admit_binding(self,*args):pass
            def runtime(self,*args):pass
            def sql(self,sql,params):
                self.calls.append(sql)
                return [{'violations':'1' if sql==guard else '0'}]
        p=Provider()
        with self.assertRaises(NativeGuardRefusal) as raised:execute_guarded(p,r,a,context=None,count_distinct=True)
        self.assertNotIn(a['sql'],p.calls)
        self.assertIs(raised.exception.evidence['userSqlExecuted'],False)

    def test_global_zero_cardinality_and_exact_carrier(self):
        r,a,b=self.case('global-empty');admit_count_result_rows(a,[{'n':'0'}])
        for rows in ([],[{'n':'0'},{'n':'1'}],[{'n':False}],[{'n':'00'}],[{'n':'-1'}],[{'n':'9223372036854775808'}]):
            with self.assertRaises(ValueError):admit_count_result_rows(a,rows)
    def test_grouped_empty_and_duplicate_group_refusal(self):
        r,a,b=self.case('grouped-empty');admit_count_result_rows(a,[])
        names=[c['outputName']for c in a['columns']];row={names[0]:'a',names[1]:'0'}
        with self.assertRaises(ValueError):admit_count_result_rows(a,[row,row])
if __name__=='__main__':unittest.main()
