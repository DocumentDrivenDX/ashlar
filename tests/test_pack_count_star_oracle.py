"""Original full graph bags and scalar/presence distinctions, no native claims."""
import json
from pathlib import Path
from unittest import TestCase,mock
from ashlar_host.finite_pack import FinitePackDefinition
from ashlar_host.pack_count_star_oracle import pack_count_star_result_oracle,_decimal
ROOT=Path(__file__).resolve().parents[1]/'examples/domain-packs'
class Tests(TestCase):
 def inputs(self,name):
  p=ROOT/name/'upstream';return FinitePackDefinition(name),(p/'ontology.json').read_bytes(),(p/'graph/fixture.json').read_bytes()
 def oracle(self,name,case):
  d,m,g=self.inputs(name);return pack_count_star_result_oracle(d,case,m,g)
 def test_all17_complete_original_bags_opaque_ids_and_denominators(self):
  for name,count in (('archaeology',8),('ecology',9)):
   d,m,g=self.inputs(name);original=d.oracle(m,g);self.assertEqual(len(original['scenarios']),count)
   for case,rows in original['scenarios'].items():self.assertEqual(self.oracle(name,case)['witnesses']['complete_result_occurrences'],len(rows))
  self.assertEqual(self.oracle('archaeology','cycle')['rows'],[['[42,0,"ST1"]','[42,0,"ST2"]']])
  self.assertEqual(self.oracle('archaeology','media')['rows'],[['[42,0,"AS4"]','2']])
  self.assertEqual(self.oracle('archaeology','specialists')['rows'],[['5','2','3','1']])
  self.assertEqual(self.oracle('ecology','connected-measurements')['rows'],[['dissolved-oxygen','2'],['temperature','2']])
 def test_left_unmatched_absence_separate_from_matched_source_values(self):
  d,m,g=self.inputs('archaeology');original=d.oracle(m,g);self.assertEqual(original['scenarios']['evidence-links'],[['Author A','bowl',None],['Author B',None,'synthetic-ungulate']])
  result=self.oracle('archaeology','evidence-links');states=[[json.loads(cell)for cell in r[1:]]for r in result['rows']]
  self.assertEqual(states,[[{'state':'value','value':'bowl'},{'state':'absent'}],[{'state':'absent'},{'state':'value','value':'synthetic-ungulate'}]])
  self.assertTrue(result['witnesses']['matched_left_nonnull_source'])
  # An independent oracle with a matched source null cannot promote None to
  # relational absence even though its original scenario rows remain the same.
  original['objects'][next(i for i,o in enumerate(original['objects'])if o['type']['element']=='pottery_results')]['values']['pottery_results.form']=None
  with mock.patch.object(FinitePackDefinition,'oracle',return_value=original):
   with self.assertRaises(ValueError):pack_count_star_result_oracle(d,'evidence-links',m,g)
 def test_original_optional_decimals_do_not_turn_censor_into_zero(self):
  result=self.oracle('ecology','censor')['rows'];self.assertEqual(result[0][0],'[42,0,"O2"]');self.assertEqual(json.loads(result[0][1]),{'state':'value','value':'0.10'});self.assertEqual(result[0][2],'below-detection')
  d,m,g=self.inputs('ecology');source=d.oracle(m,g);self.assertEqual(source['scenarios']['censor'][0][1],'0.1')
  self.assertEqual(self.oracle('ecology','effort-event')['rows'],[]);self.assertEqual(self.oracle('ecology','effort')['rows'],[['[42,0,"OC3"]']]);self.assertEqual(self.oracle('ecology','zero')['rows'],[['[42,0,"OC2"]']])
 def test_original_optional_parent_value_keeps_authored_lexical_token(self):
  d,m,g=self.inputs('archaeology');original=d.oracle(m,g)['scenarios']['sample'];selected=self.oracle('archaeology','sample')['rows']
  self.assertEqual(json.loads(selected[0][0]),{'state':'value','value':original[0][0]});self.assertEqual(selected[0][1],original[0][1])
 def test_changed_original_or_unknown_case_refuse(self):
  d,m,g=self.inputs('ecology')
  for case,model,graph in (('invented',m,g),('zero',m+b' ',g),('zero',m,g+b' ')):
   with self.assertRaises(ValueError):pack_count_star_result_oracle(d,case,model,graph)
 def test_decimal_projection_refuses_rounding_nonfinite_float_and_negative_zero(self):
  self.assertEqual(_decimal('0.10',{'precision':18,'scale':2}),'0.10');self.assertEqual(_decimal('17',{'precision':18,'scale':2}),'17.00')
  for value in ('1.234','1e1','NaN',17.0,'-0.00'):
   with self.assertRaises(ValueError):_decimal(value,{'precision':18,'scale':2})
