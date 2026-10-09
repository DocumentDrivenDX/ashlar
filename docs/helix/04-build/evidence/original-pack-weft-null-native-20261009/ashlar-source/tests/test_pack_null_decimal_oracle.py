import copy,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from run_pack_null_weft import exact_decimal_coefficient,compare_original_bag

class DecimalOracleTests(unittest.TestCase):
    def artifact(self,family='decimal'):
        identity={'documentId':'urn:authored','module':'control','element':'amount','revision':'original'}
        return {'columns':[{'representation':{'kind':'value','descriptor':identity,'nativeNull':True}}], 'logicalPlan':{'typeGraph':[{'identity':identity,'kind':'scalar','availability':'absent-allowed','type':{'family':family,'facets':{'precision':18,'scale':2}if family=='decimal'else{},'nullable':False}}]}}
    def row(self,text):return[[{'state':'value','value':text}]]
    def test_exact_scale_equivalence_preserves_both_lexicals(self):
        actual=self.row('0.10');source=self.row('0.1');before=copy.deepcopy((actual,source))
        result=compare_original_bag(self.artifact(),actual,source)
        self.assertTrue(result['exactBagEqual']);self.assertEqual((actual,source),before)
        self.assertEqual(result['actualExactCoefficientWitnesses'][0]['originalLexicalText'],'0.10')
        self.assertEqual(result['originalGraphExactCoefficientWitnesses'][0]['originalLexicalText'],'0.1')
        self.assertEqual(result['actualExactCoefficientWitnesses'][0]['exactCoefficient'],'10')
        for value in ['0','-0.00','0.000']:
            self.assertTrue(compare_original_bag(self.artifact(),self.row(value),self.row('0.00'))['exactBagEqual'])
        self.assertEqual(exact_decimal_coefficient('-1.25',{'precision':18,'scale':2}),-125)
    def test_mismatch_state_string_integer_and_multiplicity_unchanged(self):
        self.assertFalse(compare_original_bag(self.artifact(),self.row('0.11'),self.row('0.1'))['exactBagEqual'])
        self.assertFalse(compare_original_bag(self.artifact(),self.row('0'),[[{'state':'null'}]])['exactBagEqual'])
        self.assertFalse(compare_original_bag(self.artifact('boolean'),[[{'state':'value','value':False}]],[[{'state':'value','value':0}]])['exactBagEqual'])
        for family in ['string','integer']:
            self.assertFalse(compare_original_bag(self.artifact(family),self.row('0.10'),self.row('0.1'))['exactBagEqual'])
        self.assertFalse(compare_original_bag(self.artifact(),self.row('0.10')*2,self.row('0.1'))['exactBagEqual'])
    def test_no_rounding_overflow_float_or_unknown_facets(self):
        facets={'precision':18,'scale':2}
        for text in ['0.101','10000000000000000.00','1e-1','NaN','+0.1',0.1]:
            with self.assertRaises(ValueError):exact_decimal_coefficient(text,facets)
        for facets in [{'precision':0,'scale':0},{'precision':18,'scale':True},{'precision':18,'scale':2,'future':True},{'precision':1,'scale':2},{'precision':18.0,'scale':2}]:
            with self.assertRaises(ValueError):exact_decimal_coefficient('0',facets)
        with self.assertRaises(ValueError):compare_original_bag(self.artifact(),[[{'state':'unknown'}]],self.row('0'))
if __name__=='__main__':unittest.main()
