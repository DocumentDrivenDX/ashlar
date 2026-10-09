import copy,hashlib,json,unittest
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
from weft_field_plan import admit_field_plan,admit_positioned_outputs,admit_positioned_cells
from run_commerce_arithmetic_weft import execute_guarded,decode_rows
from run_commerce_publication_weft import PublicationProvider

FIXTURES=Path(__file__).parent/'fixtures/weft-positioned-field-plans'

class PositionedHostTests(unittest.TestCase):
    def fixture(self,case='archaeology-cycle'):
        request=json.loads((FIXTURES/(case+'-request.json')).read_bytes())
        artifact=json.loads((FIXTURES/(case+'-stdout.json')).read_bytes())
        return request,artifact,json.loads(request['target']['bindingJson'])
    def result(self,artifact):
        return {'schema':[[c['carrierName'],'STRING']for c in artifact['columns']], 'rows':[['first','second']]}
    def provider(self,artifact,*,result=None,fail_close=False):
        class Provider:
            def __init__(s):s.calls=[];s.active=False
            @contextmanager
            def interval(s,c):
                s.active=True
                try:yield
                finally:s.active=False
                if fail_close:raise ValueError('closing held publication failure')
            def resolve(s,c):return SimpleNamespace(descriptor=SimpleNamespace(raw={'original':1}),snapshots=('fullvector',))
            def admit_binding(s,*a):pass
            def runtime(s,*a):pass
            def sql(s,sql,params):
                assert s.active;s.calls.append(('dict',sql));return [{'violations':'0'}]
            def sql_ordered(s,sql,params):
                assert s.active;s.calls.append(('ordered',sql));return copy.deepcopy(result if result is not None else self.result(artifact))
        return Provider()
    def test_all_nine_original_artifacts_and_custody(self):
        manifest=json.loads((FIXTURES/'manifest.json').read_bytes())
        self.assertEqual(len(manifest['files']),18)
        for entry in manifest['files']:self.assertEqual(hashlib.sha256((FIXTURES/entry['file']).read_bytes()).hexdigest(),entry['sha256'])
        for path in FIXTURES.glob('*-stdout.json'):
            request,artifact,binding=self.fixture(path.name[:-12])
            positioned=any(o['id']=='weft.output.positioned'for o in artifact['obligations'])
            arithmetic=next(o for o in artifact['obligations']if o['id']=='ashlar.arithmetic.exact')
            admit_field_plan(artifact,binding,request['modules'],positioned_output_only=positioned and bool(arithmetic['parameters']['checks']))
    def test_positioned_native_arrays_preserve_duplicate_labels_and_scan_occurrences(self):
        request,artifact,_=self.fixture();provider=self.provider(artifact)
        result=execute_guarded(provider,request,artifact,context={},public_source=lambda *a:{'original':'public receipt'},positioned_outputs=True)
        self.assertEqual(result['rows'],[['first','second']]);self.assertEqual([c['outputName']for c in result['positioned']['columns']],['id','id'])
        self.assertEqual([c['scan']for c in result['positioned']['lineage']],['s0','s1'])
        self.assertEqual(provider.calls[-1],('ordered',artifact['sql']))
        self.assertFalse(any(kind=='dict'and sql==artifact['sql']for kind,sql in provider.calls))
        refused=self.provider(artifact)
        with self.assertRaisesRegex(ValueError,'explicit host opt-in'):execute_guarded(refused,request,artifact,context={})
        self.assertEqual(refused.calls,[])
        with self.assertRaisesRegex(ValueError,'closing held'):execute_guarded(self.provider(artifact,fail_close=True),request,artifact,context={},public_source=lambda *a:{},positioned_outputs=True)
    def test_native_schema_cells_and_dict_collapse_refuse(self):
        _,artifact,_=self.fixture();baseline=self.result(artifact)
        mutations=[]
        for schema in [baseline['schema'][:1],list(reversed(baseline['schema'])),[['id','STRING'],['id','STRING']],[[c[0],'DOUBLE']for c in baseline['schema']]]:
            mutations.append({**copy.deepcopy(baseline),'schema':schema})
        for rows in [[{'id':'collapsed'}],[['one']],[['one','two','extra']],[[1,'two']],[[None,'two']]]:
            mutations.append({**copy.deepcopy(baseline),'rows':rows})
        for result in mutations:
            with self.assertRaises(ValueError):admit_positioned_cells(artifact,result)
    def test_changed_obligation_column_and_scan_refuse_before_sql(self):
        request,original,_=self.fixture()
        for mutation in ['map','scan','lineage','duplicate','position','missing']:
            artifact=copy.deepcopy(original)
            obligation=next(o for o in artifact['obligations']if o['id']=='weft.output.positioned')
            if mutation=='map':obligation['parameters']['columns'][0]['carrierName']='changed'
            elif mutation=='scan':artifact['logicalPlan']['outputs'][0]['expression']['scan']='unknown'
            elif mutation=='lineage':artifact['columns'][0]['sourceIdentities']=[]
            elif mutation=='duplicate':artifact['columns'][1]['carrierName']=artifact['columns'][0]['carrierName']
            elif mutation=='position':artifact['columns'][0]['position']=True
            else:artifact['obligations'].remove(obligation)
            provider=self.provider(artifact)
            with self.assertRaises(ValueError):execute_guarded(provider,request,artifact,context={},public_source=lambda *a:{},positioned_outputs=True)
            self.assertEqual(provider.calls,[])
    def test_closed_string_comparison_and_capability_correspondence(self):
        request,original,binding=self.fixture('ecology-effort-event');admit_field_plan(original,binding,request['modules'])
        for mutation in ['numeric','nullable','operator','capability','on-policy']:
            artifact=copy.deepcopy(original);p=artifact['logicalPlan']['filters'][0]
            if mutation=='numeric':p['left']['type']['family']='integer'
            elif mutation=='nullable':p['left']['type']['nullable']=True
            elif mutation=='operator':p['operator']='greater'
            elif mutation=='capability':artifact['logicalPlan']['requiredCapabilities'].remove('compare.notEqual')
            else:
                artifact['logicalPlan']['filters']=[];artifact['logicalPlan']['joins'][-1]['on'].append(p)
            with self.assertRaises(ValueError):admit_field_plan(artifact,binding,request['modules'])
    def test_actual_provider_captures_schema_and_cells_without_asdict(self):
        class Row(tuple):
            def asDict(self):raise AssertionError('Dictionary conversion prohibited')
        fields=[SimpleNamespace(name='_weft_output_1',dataType=SimpleNamespace(simpleString=lambda:'string')),SimpleNamespace(name='_weft_output_2',dataType=SimpleNamespace(simpleString=lambda:'string'))]
        frame=SimpleNamespace(schema=SimpleNamespace(fields=fields),collect=lambda:[Row(('first','second'))])
        provider=object.__new__(PublicationProvider);provider.active=True
        provider.driver=SimpleNamespace(transport=SimpleNamespace(spark=SimpleNamespace(sql=lambda *a,**k:frame)))
        self.assertEqual(provider.sql_ordered('original',{}),{'schema':[[f.name,'STRING']for f in fields],'rows':[['first','second']]})
        provider.active=False
        with self.assertRaises(PermissionError):provider.sql_ordered('original',{})

    def test_explicit_positional_decoder_preserves_cells_and_rejects_wrong_native_shape(self):
        _,artifact,_=self.fixture();native=self.result(artifact)
        self.assertEqual(decode_rows(artifact,native['rows'],positioned=True,native_schema=native['schema'],ordered_rows=native['rows']),[['first','second']])
        with self.assertRaises(ValueError):decode_rows(artifact,native['rows'])
        for schema in [native['schema'][:1],list(reversed(native['schema'])),[['id','STRING'],['id','STRING']],[[name,'DOUBLE']for name,_ in native['schema']]]:
            with self.assertRaises(ValueError):decode_rows(artifact,native['rows'],positioned=True,native_schema=schema,ordered_rows=native['rows'])
        for rows in [[['one']],[{'id':'collapsed'}],[[1,'two']],[['one','two','three']]]:
            with self.assertRaises(ValueError):decode_rows(artifact,rows,positioned=True,native_schema=native['schema'],ordered_rows=rows)
        with self.assertRaises(ValueError):decode_rows(artifact,[['changed','two']],positioned=True,native_schema=native['schema'],ordered_rows=native['rows'])
        bad=copy.deepcopy(artifact);bad['obligations'].append(copy.deepcopy(next(o for o in bad['obligations']if o['id']=='weft.output.positioned')))
        with self.assertRaises(ValueError):decode_rows(bad,native['rows'],positioned=True,native_schema=native['schema'],ordered_rows=native['rows'])
        bad=copy.deepcopy(artifact);bad['obligations'][0]['id']='unknown'
        with self.assertRaises(ValueError):decode_rows(bad,native['rows'],positioned=True,native_schema=native['schema'],ordered_rows=native['rows'])

    def test_capacity_forwarding_retains_explicit_original_reader_profile(self):
        from unittest.mock import patch
        from run_commerce_publication_weft import ReadOnlyTransport
        captured=[]
        class Base:
            def __init__(self,*args,**kwargs):captured.append((args,kwargs))
        with patch('local_delta_custody.LocalDeltaTransport',Base):
            ReadOnlyTransport.open('spark','journal','installation','targets','policy')
            profile={'profile':'ashlar-private-local-operation-capacity/0.1','max_intent_bytes':8388608}
            ReadOnlyTransport.open('spark','journal','installation','targets','policy',capacity=profile)
        self.assertEqual(captured[0][1],{'capacity':None})
        self.assertEqual(captured[1][1],{'capacity':profile})

if __name__=='__main__':unittest.main()
