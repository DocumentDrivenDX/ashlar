import json,unittest
from pathlib import Path
from run_local_example import fixture_inputs
from ashlar.weft_decode import decode_string_column
from ashlar.publication import ResolutionError
ROOT=Path(__file__).resolve().parents[1]
class DecodeTests(unittest.TestCase):
    def test_selected_string_presence_and_refusals(self):
        intake,policy,_=fixture_inputs();columns=json.loads((ROOT/'docs/helix/04-build/evidence/weft-pinned-compiler-20261008.json').read_text())['compiled']['response']['columns']
        decode=lambda c,v:decode_string_column(c,v,policy,intake)
        self.assertEqual(decode(columns[0],'updated'),'updated')
        self.assertEqual(dict(decode(columns[1],'{"state":"absent"}')),{'state':'absent'})
        self.assertEqual(dict(decode(columns[1],'{"state":"value","value":"雪"}')),{'state':'value','value':'雪'})
        for raw in [None,'{"state":"null"}','{"state":"value","value":null}','{"state":"value","value":1}','{"state":"absent","state":"value"}','{"state":"absent","extra":true}']:
            with self.assertRaises(ValueError):decode(columns[1],raw)
        for raw in [None,'bad\x00text','\ud800']:
            with self.assertRaises(ResolutionError):decode(columns[0],raw)


from decimal import Decimal, localcontext
from ashlar.weft_decode import decode_exact_scalar


def scalar(family,facets=None,carrier='text',decoder=None):
    return {'kind':'scalar','logicalType':{'family':family,'facets':{} if facets is None else facets,'nullable':False},
            'carrier':carrier,'decoder':decoder or {'integer':'exact-integer','decimal':'exact-decimal','string':'text','boolean':'boolean'}[family]}


class ExactScalarTests(unittest.TestCase):
    def test_int64_safe_integer_and_original_spelling(self):
        r=scalar('integer',{'integerWidth':{'bits':64,'signed':True}})
        for text in ['9007199254740993',str(-2**63),str(2**63-1),'-0','00012']:
            result=decode_exact_scalar(r,text);self.assertEqual(result.value,int(text));self.assertEqual(result.original,text)
        for value in [str(-2**63-1),str(2**63),1,True,1.0,None,'1e2','+1','1.0',' 1','١']:
            with self.subTest(value=value),self.assertRaises(ResolutionError):decode_exact_scalar(r,value)

    def test_unsigned_and_small_width_bounds(self):
        for bits,signed,minimum,maximum in [(8,True,-128,127),(8,False,0,255),(64,False,0,2**64-1),(1,True,-1,0)]:
            r=scalar('integer',{'integerWidth':{'bits':bits,'signed':signed}})
            for n in [minimum,maximum]:self.assertEqual(decode_exact_scalar(r,str(n)).value,n)
            for n in [minimum-1,maximum+1]:
                with self.assertRaises(ResolutionError):decode_exact_scalar(r,str(n))

    def test_count_requires_explicit_admission(self):
        r=scalar('integer')
        with self.assertRaises(ResolutionError):decode_exact_scalar(r,'1')
        self.assertEqual(decode_exact_scalar(r,str(2**63-1),count=True).value,2**63-1)
        for value in ['-1',str(2**63)]:
            with self.assertRaises(ResolutionError):decode_exact_scalar(r,value,count=True)
        with self.assertRaises(ResolutionError):decode_exact_scalar(scalar('integer',{'integerWidth':{'bits':64,'signed':True}}),'1',count=True)

    def test_decimal_preserves_zero_scale_and_never_rounds(self):
        r=scalar('decimal',{'precision':3,'scale':1})
        for text in ['99.9','099.9000','-0.000','0.10']:
            result=decode_exact_scalar(r,text);self.assertEqual(result.value,Decimal(text));self.assertEqual(result.original,text)
        self.assertTrue(decode_exact_scalar(r,'-0.000').value.is_signed())
        for text in ['100','99.99','1e1','1E+1','NaN','Infinity','.1','1.','+1.0',' 1.0']:
            with self.subTest(text=text),self.assertRaises(ResolutionError):decode_exact_scalar(r,text)
        # Decimal construction and lexical domain proof ignore ambient rounding.
        with localcontext() as context:
            context.prec=2
            result=decode_exact_scalar(scalar('decimal',{'precision':28,'scale':2}),'12345678901234567890123456.78')
        self.assertEqual(result.value.as_tuple().digits,tuple(int(v) for v in '1234567890123456789012345678'))

    def test_boolean_text_and_native_are_distinct(self):
        r=scalar('boolean')
        for text,value in [('true',True),('false',False)]:self.assertIs(decode_exact_scalar(r,text).value,value)
        for text in ['TRUE','False','t','f','1','0',True,None]:
            with self.assertRaises(ResolutionError):decode_exact_scalar(r,text)
        r=scalar('boolean',carrier='boolean')
        self.assertIs(decode_exact_scalar(r,False).value,False)
        for value in [0,1,'true',None]:
            with self.assertRaises(ResolutionError):decode_exact_scalar(r,value)

    def test_string_and_nullable_unknown_facets_refuse(self):
        r=scalar('string');self.assertEqual(decode_exact_scalar(r,'雪').value,'雪')
        for value in [None,'\ud800','a\x00b']:
            with self.assertRaises(ResolutionError):decode_exact_scalar(r,value)
        invalid=[scalar('string',{'future':1}),scalar('decimal',{'precision':29,'scale':2}),
                 scalar('decimal',{'precision':3,'scale':4}),scalar('decimal',{'precision':True,'scale':0}),
                 scalar('decimal',{'precision':3,'scale':1,'future':1}),
                 scalar('integer',{'integerWidth':{'bits':64,'signed':True,'future':1}}),
                 scalar('integer',{'integerWidth':{'bits':True,'signed':True}}),
                 scalar('boolean',{'future':1}),scalar('integer',{'integerWidth':{'bits':64,'signed':True}},carrier='boolean')]
        nullable=scalar('string');nullable['logicalType']['nullable']=True;invalid.append(nullable)
        unknown=scalar('string');unknown['extra']=True;invalid.append(unknown)
        malformed=scalar('string');malformed['logicalType']['family']=[];invalid.append(malformed)
        for rep in invalid:
            with self.subTest(rep=rep),self.assertRaises(ResolutionError):decode_exact_scalar(rep,'1')
        with self.assertRaises(ResolutionError):decode_exact_scalar(scalar('decimal',{'precision':3,'scale':1}),'0'*1025)
        result=decode_exact_scalar(r,'value')
        with self.assertRaises(AttributeError):result.original='changed'


import copy,hashlib,subprocess,sys
from ashlar.weft_decode import decode_compiled_column,admit_compiled_count


class CompiledCountTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        wheel=Path('/private/tmp/ashlar-weft-python')
        if not (wheel/'weft/weft.abi3.so').exists():
            raise unittest.SkipTest('Pinned native Weft wheel unavailable; real compiler count check not executed')
        evidence=json.loads((ROOT/'docs/helix/04-build/evidence/weft-pinned-compiler-20261008.json').read_text())
        if hashlib.sha256((wheel/'weft/weft.abi3.so').read_bytes()).hexdigest()!=evidence['loaded_extension_sha256']:
            raise AssertionError('Pinned Weft extension bytes differ')
        # Preserve original request/model/binding; obtain COUNT IR from real compiler.
        q=copy.deepcopy(evidence['compiled']['request']);q['sql']='SELECT COUNT(*) AS n FROM Item i'
        code='import json,sys;sys.path.insert(0,sys.argv[1]);import weft;print(weft.compile_json(sys.stdin.read()))'
        p=subprocess.run([sys.executable,'-c',code,str(wheel)],input=json.dumps(q),text=True,capture_output=True,check=True)
        cls.artifact=json.loads(p.stdout)
        if cls.artifact['status']!='compiled':raise AssertionError(cls.artifact)
        cls.intake,cls.policy,_=fixture_inputs()

    def decode(self,value,artifact=None,column=None):
        a=self.artifact if artifact is None else artifact
        return decode_compiled_column(a['columns'][0] if column is None else column,value,a,self.policy,self.intake)

    def test_actual_compiler_count_ir_decodes_exactly(self):
        self.assertEqual(self.artifact['logicalPlan']['outputs'][0]['expression']['op'],'count')
        record=admit_compiled_count(self.artifact['columns'][0],self.artifact,self.policy,self.intake)
        self.assertEqual(dict(record),self.artifact['logicalPlan']['source']['record'])
        with self.assertRaises(TypeError):record['element']='changed'
        for value in ['0','1','9007199254740993',str(2**63-1)]:
            result=self.decode(value);self.assertEqual(result.value,int(value));self.assertEqual(result.original,value)
        for value in ['-1',str(2**63),None,1,'1.0','1e0']:
            with self.assertRaises(ResolutionError):self.decode(value)

    def test_mutated_plan_columns_pins_and_identity_refuse(self):
        for change in ['op','extra-expression','output-order','source','pin','module-pin','source-pin','record','column-identity','column-position','nullable','facets','groups','filter','joins','extra-plan','version','duplicate-column']:
            a=copy.deepcopy(self.artifact)
            if change=='op':a['logicalPlan']['outputs'][0]['expression']['op']='sum'
            if change=='extra-expression':a['logicalPlan']['outputs'][0]['expression']['argument']={}
            if change=='output-order':a['logicalPlan']['outputs'][0]['name']='different'
            if change=='source':a['logicalPlan']['source']['occurrence']='s1'
            if change=='pin':a['modelPins'][0]['sha256']='0'*64
            if change=='module-pin':a['logicalPlan']['modulePins'][0]['revision']='4'
            if change=='source-pin':a['logicalPlan']['source']['pin']['revision']='4'
            if change=='record':a['logicalPlan']['source']['record']['element']='label'
            if change=='column-identity':a['columns'][0]['sourceIdentities'][0]['element']='caption'
            if change=='column-position':a['columns'][0]['position']=True
            if change=='nullable':a['columns'][0]['nullable']=True
            if change=='facets':a['columns'][0]['representation']['logicalType']['facets']={'integerWidth':{'bits':64,'signed':True}}
            if change=='groups':a['logicalPlan']['groups']=[{}]
            if change=='filter':a['logicalPlan']['filters']=[{}]
            if change=='joins':a['logicalPlan']['joins']=[{}]
            if change=='extra-plan':a['logicalPlan']['future']=True
            if change=='version':a['interfaceVersion']='weft-compile/0.1.0'
            if change=='duplicate-column':a['columns'].append(copy.deepcopy(a['columns'][0]))
            with self.subTest(change=change),self.assertRaises(ResolutionError):self.decode('1',a)
            with self.subTest(admission=change),self.assertRaises(ResolutionError):admit_compiled_count(a['columns'][0],a,self.policy,self.intake)
        column=copy.deepcopy(self.artifact['columns'][0]);column['outputName']='changed'
        with self.assertRaises(ResolutionError):self.decode('1',column=column)

    def test_old_string_artifact_route_unchanged(self):
        a=json.loads((ROOT/'docs/helix/04-build/evidence/weft-pinned-compiler-20261008.json').read_text())['compiled']['response']
        self.assertEqual(decode_compiled_column(a['columns'][0],'updated',a,self.policy,self.intake),'updated')
        self.assertEqual(dict(decode_compiled_column(a['columns'][1],'{"state":"absent"}',a,self.policy,self.intake)),{'state':'absent'})


class CountRowCardinalityTests(unittest.TestCase):
    def test_global_count_exact_cardinality_and_carrier(self):
        from ashlar.weft_decode import admit_count_rows,ExactScalar
        admit_count_rows(((ExactScalar(0,'0'),),))
        admit_count_rows(((ExactScalar(1,'1'),),))
        for rows in [(),((),),((ExactScalar(1,'1'),ExactScalar(1,'1')),),
                     ((ExactScalar(1,'1'),),(ExactScalar(1,'1'),)),
                     ((ExactScalar(1,'2'),),),((ExactScalar(-1,'-1'),),),
                     ((ExactScalar(True,'1'),),),((1,),),[[ExactScalar(1,'1')]]]:
            with self.subTest(rows=rows),self.assertRaises(ResolutionError):admit_count_rows(rows)


class UnsupportedCountCompilerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        CompiledCountTests.setUpClass.__func__(cls)

    def test_filtered_and_grouped_metadata_refuse_before_rows(self):
        evidence=json.loads((ROOT/'docs/helix/04-build/evidence/weft-pinned-compiler-20261008.json').read_text())
        code='import json,sys;sys.path.insert(0,sys.argv[1]);import weft;print(weft.compile_json(sys.stdin.read()))'
        for sql in ["SELECT COUNT(*) AS n FROM Item i WHERE i.label = 'updated'",
                    'SELECT i.label, COUNT(*) AS n FROM Item i GROUP BY i.label ORDER BY i.label LIMIT 10']:
            q=copy.deepcopy(evidence['compiled']['request']);q['sql']=sql
            response=subprocess.run([sys.executable,'-c',code,'/private/tmp/ashlar-weft-python'],input=json.dumps(q),capture_output=True,text=True,check=True)
            a=json.loads(response.stdout);self.assertEqual(a['status'],'compiled',a.get('diagnostics'))
            count=next(c for c in a['columns'] if c['representation'].get('logicalType',{}).get('family')=='integer')
            # Admission is independent of result iteration, including zero rows.
            with self.assertRaises(ResolutionError):admit_compiled_count(count,a,self.policy,self.intake)
