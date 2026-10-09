import copy,json,sys,unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from run_commerce_arithmetic_weft import compiler_request,execute_guarded,scenarios
from run_commerce_publication_weft import encoded

class ArithmeticPublicationHostTests(unittest.TestCase):
    def request(self):
        root=Path(__file__).resolve().parents[1]
        pack=root/'examples/domain-packs/commerce/upstream'
        model=(pack/'ontology.json').read_bytes();graph=(pack/'graph/fixture.json').read_bytes()
        from commerce_source_transaction import build_transaction
        _,bindings=build_transaction(model,graph,source_system='private-original-commerce-fixture',binding_profile='ashlar-commerce-development-bindings/0.2')
        report=json.loads((root/'docs/helix/04-build/evidence/commerce-canonical-publication-20261009.json').read_bytes())
        manifest=report['native_manifest']
        aliases={t:'spark_catalog.commerce.'+t.split('.')[-1] for t in json.loads(manifest['table_versions_json'])}
        return compiler_request(scenarios()[0][0]['sql'],model,bindings,manifest,report['table_registry'],aliases)
    def test_original_all_record_and_field_homes_and_equivalent_query(self):
        req=self.request();binding=json.loads(req['target']['bindingJson'])
        self.assertEqual(len(binding['records']),10);self.assertEqual(sum(len(r['properties'])for r in binding['records']),34)
        self.assertEqual(len(binding['publication']['tables']),4)
        cases,_=scenarios();self.assertEqual([c['expected']for c in cases],[[[6,2,6]],[['F2']],[['PAY1']],[['RF1']]])
        self.assertIn('remaining_quantity',cases[0]['sql']);self.assertIn('l.quantity>f.quantity',cases[0]['sql'])
        self.assertIsNotNone(cases[0]['equivalentQueryQualification'])
    def artifact(self,request):
        binding=json.loads(request['target']['bindingJson'])
        publication={k:binding[k] for k in ['publication','modelPins','layoutRevision','layoutSha256']}
        guards={'phase':'before-user-query','samePublicationRequired':True,'noPartialPublication':True,'checks':[{'sql':'original guard'}]}
        return {'status':'compiled','bindingSha256':request['target']['bindingSha256'],'modelPins':binding['modelPins'],'parameters':[],'sql':'original query','obligations':[{'id':'ashlar.candidate.publication','parameters':publication},{'id':'ashlar.candidate.scalarIntegrity','parameters':copy.deepcopy(guards)},{'id':'ashlar.arithmetic.exact','parameters':{**copy.deepcopy(guards),'nativeRepresentation':'DECIMAL(38,0) coefficients','maxScale':18,'checks':[{'phase':'projection-survivors','sql':'original guard'}]}}]}
    def provider(self,*,fail_close=False,bad_guard=False,changed=False):
        class Provider:
            calls=[]
            @contextmanager
            def interval(s,c):
                yield
                if fail_close:raise ValueError('closing custody failed')
            def resolve(s,c):
                return SimpleNamespace(descriptor=SimpleNamespace(raw={'original':len(s.calls) if changed else 1}),snapshots=('full4vector',))
            def admit_binding(s,*a):pass
            def runtime(s,*a):pass
            def sql(s,sql,params):
                s.calls.append(sql)
                return [{'violations':'1' if bad_guard else '0'}] if sql=='original guard' else [{'result':'buffered'}]
        return Provider()
    def test_checks_unchanged_before_query_and_buffer_until_close(self):
        req=self.request();art=self.artifact(req);p=self.provider()
        self.assertEqual(execute_guarded(p,req,art,context={})['rows'],[{'result':'buffered'}]);self.assertEqual(p.calls,['original guard','original guard','original query'])
        for options in [{'fail_close':True},{'bad_guard':True},{'changed':True}]:
            with self.assertRaises(ValueError):execute_guarded(self.provider(**options),req,art,context={})
    def test_missing_unknown_lifecycle_or_publication_obligations_refuse(self):
        req=self.request();original=self.artifact(req)
        mutations=[]
        a=copy.deepcopy(original);a['obligations'].pop();mutations.append(a)
        a=copy.deepcopy(original);a['obligations'][2]['id']='unknown';mutations.append(a)
        a=copy.deepcopy(original);a['obligations'][2]['parameters']['samePublicationRequired']=1;mutations.append(a)
        a=copy.deepcopy(original);a['obligations'][2]['parameters']['checks']=[];mutations.append(a)
        a=copy.deepcopy(original);a['obligations'][0]['parameters']['publication']['tables'].pop();mutations.append(a)
        for a in mutations:
            p=self.provider()
            with self.assertRaises(ValueError):execute_guarded(p,req,a,context={})
            self.assertEqual(p.calls,[])

    def test_public_source_rows_require_actual_admission_before_capacity_or_query(self):
        req=self.request();art=self.artifact(req)
        art['obligations'][1]['parameters']['checks'][0]['publicSourceOnly']=True
        provider=self.provider()
        with self.assertRaisesRegex(ValueError,'public source'):execute_guarded(provider,req,art,context={})
        self.assertEqual(provider.calls,['original guard'])
        observed=[]
        def public(r,a,checks):
            observed.append(checks)
            return {'originalRequestText':'retained actual public receipt request'}
        provider=self.provider();result=execute_guarded(provider,req,art,context={},public_source=public)
        self.assertEqual(len(observed),1);self.assertEqual(result['checks'][0]['publicSourceReceipt']['originalRequestText'],'retained actual public receipt request')
        self.assertEqual(provider.calls,['original guard','original guard','original query'])

    def test_exact_operator_free_plan_allows_empty_arithmetic_but_enforces_schema_guard(self):
        from run_commerce_arithmetic_weft import NativeGuardRefusal
        req=self.request();binding=json.loads(req['target']['bindingJson']);record=next(r for r in binding['records']if r['logical']['element']=='order_lines');field=next(p['logical']for p in record['properties']if p['logical']['element']=='order_lines.quantity')
        artifact=self.artifact(req);artifact['obligations'][2]['parameters']['checks']=[];artifact['columns']=[{'outputName':'quantity'}]
        artifact['logicalPlan']={'irVersion':'weft-ir/0.3.0','aggregate':False,'filters':[],'groups':[],'joins':[],'order':[],'limit':None,'pageKey':None,'readProfile':None,'modulePins':binding['modelPins'],'requiredCapabilities':['project','scan','type.integer','type.integer.unbounded'],'source':{'occurrence':'s0','pin':binding['modelPins'][0],'record':record['logical']},'outputs':[{'name':'quantity','expression':{'op':'field','scan':'s0','identity':field}}],'typeGraph':[]}
        self.assertEqual(execute_guarded(self.provider(),req,artifact,context={})['rows'],[{'result':'buffered'}])
        p=self.provider(bad_guard=True)
        with self.assertRaises(NativeGuardRefusal):execute_guarded(p,req,artifact,context={})
        self.assertEqual(p.calls,['original guard'])
        for key,value in [('filters',[{'unknown':'meaning'}]),('requiredCapabilities',['arithmetic.exact']),('aggregate',True)]:
            bad=copy.deepcopy(artifact);bad['logicalPlan'][key]=value;p=self.provider()
            with self.assertRaises(ValueError):execute_guarded(p,req,bad,context={})
            self.assertEqual(p.calls,[])
        bad=copy.deepcopy(artifact);bad['logicalPlan']['outputs'][0]['expression']['op']='add'
        with self.assertRaises(ValueError):execute_guarded(self.provider(),req,bad,context={})

class OriginalReplayIdentityTests(unittest.TestCase):
    def test_graph_original_identity_compares_without_rewriting_native_text(self):
        from run_commerce_arithmetic_weft import original_graph_expected
        root=Path(__file__).resolve().parents[1];graph=json.loads((root/'examples/domain-packs/commerce/upstream/graph/fixture.json').read_bytes())
        cases,_=scenarios()
        for case in cases:
            native,witnesses=original_graph_expected(case,graph)
            if case['id']=='partial-return':self.assertEqual(native,[['6','2','6']]);self.assertEqual(witnesses,[])
            else:
                self.assertEqual(json.loads(native[0][0]),[42,0,case['expected'][0][0]])
                self.assertEqual(witnesses[0]['originalGraphValue'],native[0][0])
        changed=copy.deepcopy(graph);changed['run']['seed']=43
        with self.assertRaises(ValueError):original_graph_expected(cases[1],changed)
        duplicate=copy.deepcopy(graph);duplicate['objects'].append(next(o for o in graph['objects']if o['values'].get('fulfillments.id')=='[42,0,"F2"]'))
        with self.assertRaises(ValueError):original_graph_expected(cases[1],duplicate)

class CleanupEvidenceTests(unittest.TestCase):
    def test_spark_stop_failure_withholds_all_evidence(self):
        import tempfile
        from unittest.mock import Mock
        from run_commerce_arithmetic_weft import persist_after_stop
        with tempfile.TemporaryDirectory() as path:
            output=Path(path);spark=Mock();spark.stop.side_effect=ValueError('cleanup failed')
            with self.assertRaises(ValueError):persist_after_stop(spark,{'report.json':'success','request.json':'original'},output)
            self.assertEqual(list(output.iterdir()),[])
            spark.stop.side_effect=None;persist_after_stop(spark,{'report.json':'success'},output)
            self.assertEqual((output/'report.json').read_text(),'success')

class PublicScalarTextTests(unittest.TestCase):
    def artifact(self,family,facets):
        return {'columns':[{'position':1,'outputName':'v','nullable':False,'sourceIdentities':[],'representation':{'kind':'scalar','logicalType':{'family':family,'facets':facets,'nullable':False},'carrier':'text','decoder':{'string':'text','integer':'exact-integer','decimal':'exact-decimal','boolean':'boolean'}[family]}}]}
    def test_exact_original_text_no_float_or_zero_normalization(self):
        from run_commerce_arithmetic_weft import decode_rows
        for family,facets,value in [('integer',{},'9007199254740993'),('integer',{},'-0'),('decimal',{'scale':2},'-0.00'),('decimal',{'precision':18,'scale':2},'12.50'),('boolean',{},'false'),('string',{},'雪')]:
            self.assertEqual(decode_rows(self.artifact(family,facets),[{'v':value}]),[[value]])
    def test_unknown_lossy_or_missing_results_refuse(self):
        from run_commerce_arithmetic_weft import decode_rows
        for family,facets,value in [('integer',{},1.0),('integer',{},'1e3'),('integer',{'width':64},'1'),('decimal',{'scale':2},'0.001'),('decimal',{'precision':3,'scale':2},'10.00'),('boolean',{},'1')]:
            with self.assertRaises(ValueError):decode_rows(self.artifact(family,facets),[{'v':value}])
        with self.assertRaises(ValueError):decode_rows(self.artifact('integer',{}),[{}])

class RetainedNativeArithmeticTests(unittest.TestCase):
    def test_actual_bags_guard_phases_and_original_public_receipts(self):
        import hashlib
        root=Path(__file__).resolve().parents[1];base=root/'docs/helix/04-build/evidence/commerce-arithmetic-weft-20261009'
        report=json.loads((base/'report.json').read_bytes());graph=json.loads((root/'examples/domain-packs/commerce/upstream/graph/fixture.json').read_bytes())
        from run_commerce_arithmetic_weft import original_graph_expected
        self.assertEqual(len(report['original_native_files']),66)
        for query in report['queries']:
            expected,witnesses=original_graph_expected(query['case'],graph)
            self.assertEqual(query['exact_text_rows'],expected);self.assertEqual(query['original_replay_identity_witnesses'],witnesses)
            for check in query['result']['checks']:
                receipt=check['publicSourceReceipt']
                if receipt is None:self.assertEqual(check['rows'],[{'violations':'0'}])
                else:
                    self.assertEqual(hashlib.sha256(receipt['originalReceiptText'].encode()).hexdigest(),receipt['receiptSha256'])
                    original=json.loads(receipt['originalReceiptText']);self.assertIs(original['admitted'],True);self.assertEqual(original['originalRequestText'],receipt['originalRequestText'])
        controls={c['name']:c for c in report['guardControls']}
        for name,phase in [('projection-cancellation','projection-survivors'),('where-prefilter','where-candidates'),('join-prepredicate','join-candidates')]:
            actual=controls[name]['actual'];self.assertEqual(actual['check']['phase'],phase);self.assertEqual(actual['rows'],[{'violations':'1'}]);self.assertIs(actual['userSqlExecuted'],False);self.assertIs(actual['resultReleased'],False)
        self.assertEqual(controls['projection-empty-survivors']['actual']['rows'],[])
        self.assertEqual(controls['wrong-schema-revision']['actual']['obligation'],'ashlar.candidate.scalarIntegrity')
        self.assertEqual(controls['wrong-schema-revision']['actual']['rows'],[{'violations':'1'}])

if __name__=='__main__':unittest.main()
