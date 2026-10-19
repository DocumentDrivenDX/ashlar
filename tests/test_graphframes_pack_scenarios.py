# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure original17 correspondence and complete LEFT witnesses, no Spark claim."""
import json
from pathlib import Path
import unittest
from ashlar_host.finite_pack import FinitePackDefinition
from ashlar_host.graphframes_pack_scenarios import prepare_scenarios,validate_case,ARITY,WITNESSES,signed64_token
ROOT=Path(__file__).resolve().parents[1]
class Tests(unittest.TestCase):
 def profile(self,name):
  upstream=ROOT/'examples/domain-packs'/name/'upstream'
  raw=[(upstream/p).read_bytes()for p in ('ontology.json','graph/fixture.json','pack.json')]
  release=ROOT/'docs/helix/04-build/evidence/original17-graphframes-native-20261009/original'/('1-release.graph.json'if name=='archaeology'else'9-release.graph.json')
  return prepare_scenarios(FinitePackDefinition(name),*raw,json.loads(release.read_bytes()))
 def test_all17_original_sql_expected_and_every_field_presence(self):
  for name,count in [('archaeology',8),('ecology',9)]:
   profile=self.profile(name)
   self.assertEqual(len(profile['cases']),count)
   self.assertEqual(len(profile['nodes']),39 if name=='archaeology' else 40)
   for row in profile['nodes']:
    for field in profile['fields']:
     self.assertIn(row[field['presence_column']],('present','present-null','nonmember'))
   original=ROOT/'examples/domain-packs'/name/'upstream/pack.json'
   self.assertEqual([(c['original_scenario']['id'],c['original_scenario']['sql'])for c in profile['cases']],[(c['id'],c['sql'])for c in json.loads(original.read_bytes())['scenario_checks']])
 def test_signed64_capacity_never_rounds_original_integer(self):
  for token in ['9223372036854775808','-9223372036854775809','01','1.0','9'*1000]:
   with self.assertRaises(ValueError):signed64_token(token,{'scalarType':'integer'})
  self.assertEqual(signed64_token('-9223372036854775808',{'scalarType':'integer'}),-(2**63))
 def test_duplicate_original_vertex_and_nonlexical_value_refuse(self):
  name='ecology';upstream=ROOT/'examples/domain-packs'/name/'upstream';raw=[(upstream/p).read_bytes()for p in ('ontology.json','graph/fixture.json','pack.json')]
  release=json.loads((ROOT/'docs/helix/04-build/evidence/original17-graphframes-native-20261009/original/9-release.graph.json').read_bytes())
  release['nodes'][1]=release['nodes'][0]
  with self.assertRaises(ValueError):prepare_scenarios(FinitePackDefinition(name),*raw,release)
 def rows(self,profile,case):
  import base64
  name=case['original_scenario']['id'];rows=[list(row)for row in case['original_graph_expected']]
  if name in ('media','connected-measurements'):
   for row in rows:row[1]=int(row[1])
  if name=='evidence-links':
   objects=json.loads(base64.b64decode(profile['source_admission']['original_graph_base64']))['objects'];rows=[]
   for i in sorted((o for o in objects if o['type']['element']=='interpretations'),key=lambda o:o['values']['interpretations.id']):
    for e in objects:
     if e['type']['element']!='interpretation_evidence' or e['values']['interpretation_evidence.interpretation_id']!=i['values']['interpretations.id']:continue
     ps=[o for o in objects if o['type']['element']=='pottery_results' and o['values']['pottery_results.id']==e['values']['interpretation_evidence.pottery_result_id']]or[None]
     fs=[o for o in objects if o['type']['element']=='fauna_results' and o['values']['fauna_results.id']==e['values']['interpretation_evidence.fauna_result_id']]or[None]
     for pot in ps:
      for fauna in fs:
       pv=None if pot is None else pot['values']['pottery_results.form'];fv=None if fauna is None else fauna['values']['fauna_results.taxon']
       rows.append([i['values']['interpretations.author'],pv,fv,i['key'],e['key'],i['values']['interpretations.id'],None if pot is None else pot['key'],None if fauna is None else fauna['key'],None if pot is None else'present-null'if pv is None else'present',None if fauna is None else'present-null'if fv is None else'present'])
  return rows
 def schema(self,profile,case):
  name=case['original_scenario']['id'];names=['result_'+str(i)for i in range(ARITY[profile['pack']][name])]+(WITNESSES if name=='evidence-links'else[])
  return [{'name':n,'type':'bigint'if i==1 and name in ('media','connected-measurements')else'string'}for i,n in enumerate(names)]
 def test_complete17_original_bags_and_left_witnesses(self):
  for pack in ('archaeology','ecology'):
   profile=self.profile(pack)
   for case in profile['cases']:
    name=case['original_scenario']['id'];schema=self.schema(profile,case);raw=self.rows(profile,case)
    self.assertEqual(sorted(map(str,validate_case(profile,name,schema,raw))),sorted(map(str,case['original_graph_expected'])))
    if raw:
     with self.assertRaises(ValueError):validate_case(profile,name,schema,raw+raw[:1])
 def test_left_matching_identity_cannot_be_replaced_by_absence(self):
  profile=self.profile('archaeology');case=next(c for c in profile['cases']if c['original_scenario']['id']=='evidence-links');rows=self.rows(profile,case)
  selected=next(row for row in rows if row[6]is not None);selected[6]=None;selected[8]=None;selected[1]=None
  with self.assertRaises(ValueError):validate_case(profile,'evidence-links',self.schema(profile,case),rows)
 def test_original17_dispatch_observation_copy_and_refusal_stop(self):
  from unittest import mock
  from types import SimpleNamespace
  from ashlar_host.graphframes_pack_scenarios import execute_scenarios
  for pack in ('archaeology','ecology'):
   profile=self.profile(pack);collected=[];observed=[];owner=self
   class Frame:
    def __init__(self,case):
     self.case=case;self.schema=SimpleNamespace(fields=[SimpleNamespace(name=f['name'],dataType=SimpleNamespace(simpleString=lambda t=f['type']:t))for f in owner.schema(profile,case)])
    def limit(self,count):return self
    def collect(self):collected.append(self.case['original_scenario']['id']);return owner.rows(profile,self.case)
   plans={c['original_scenario']['id']:Frame(c)for c in profile['cases']}
   def observe(value):
    observed.append(value['case']);value['rows'].clear();value['schema'].clear()
   with mock.patch('ashlar_host.graphframes_pack_scenarios.expressions',return_value=plans):
    actual=execute_scenarios(object(),profile,observe)
    self.assertEqual(collected,[c['original_scenario']['id']for c in profile['cases']]);self.assertEqual(observed,collected);self.assertEqual(len(actual),len(profile['cases']))
    collected.clear();cancel=KeyboardInterrupt('sink')
    def refuse(value):raise cancel
    with self.assertRaises(KeyboardInterrupt)as raised:execute_scenarios(object(),profile,refuse)
    self.assertIs(raised.exception,cancel);self.assertEqual(collected,[profile['cases'][0]['original_scenario']['id']])
 def test_complete_native_capacity_refuses_before_observation(self):
  from types import SimpleNamespace
  from ashlar_host.graphframes_pack_scenarios import bounded_frame_rows
  class Frame:
   def limit(self,n):self.limit_value=n;return self
   def collect(self):return [[str(i)]for i in range(4)]
  frame=Frame()
  with self.assertRaises(ValueError):bounded_frame_rows(frame,3)
  self.assertEqual(frame.limit_value,4)
if __name__=='__main__':unittest.main()
