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
