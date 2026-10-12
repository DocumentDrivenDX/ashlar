"""Original two authored STRING selectors; no native engine or source grant."""
import json,hashlib,unittest
from pathlib import Path
from ashlar_host.finite_pack import FinitePackDefinition
from ashlar_host.puppy_finite_pack import PuppyFinitePackConfiguration,project_finite_pack_strings
ROOT=Path(__file__).resolve().parents[1]
class Tests(unittest.TestCase):
 def selected(self,name):
  root=ROOT/'examples/domain-packs'/name/'upstream';release=(ROOT/'docs/helix/04-build/evidence/original17-graphframes-native-20261009/original'/('1-release.graph.json'if name=='archaeology'else'9-release.graph.json')).read_bytes()
  return PuppyFinitePackConfiguration(FinitePackDefinition(name),(root/'ontology.json').read_bytes(),(root/'graph/fixture.json').read_bytes(),(root/'pack.json').read_bytes(),release,hashlib.sha256(release).hexdigest())
 def test_original_two_cases_and_network_links_are_records(self):
  for name,case in (('archaeology','missing-media'),('ecology','network')):
   config=self.selected(name);projected=project_finite_pack_strings(config)
   self.assertEqual(projected['case'],case);self.assertEqual(projected['expected'],config.definition.oracle(config.model,config.graph)['scenarios'][case])
   self.assertEqual(len(projected['nodes']),39 if name=='archaeology' else 40)
   if name=='ecology':
    self.assertIn("n.original_record = 'network_links'",projected['query'])
    self.assertNotIn('-[',projected['query'])
   self.assertTrue(all(type(row['selected_0'])in (str,type(None))for row in projected['nodes']))
 def test_missing_or_duplicate_canonical_row_and_wrong_pack_refuse(self):
  from dataclasses import replace
  config=self.selected('ecology');value=json.loads(config.release)
  value['nodes'][1]=value['nodes'][0];raw=json.dumps(value).encode()
  with self.assertRaises(ValueError):project_finite_pack_strings(replace(config,release=raw,release_sha256=hashlib.sha256(raw).hexdigest()))
  with self.assertRaises(ValueError):project_finite_pack_strings(replace(config,definition=FinitePackDefinition('supply-chain')))
 def test_inert_public_execution_exact_width_and_native_incidence(self):
  from contextlib import contextmanager
  from types import SimpleNamespace
  from unittest.mock import patch
  from ashlar_host.finite_dataset import FinitePackSource
  from ashlar_host.puppy_finite_pack import execute_finite_pack_strings
  # Nominal source with explicitly inert lower semantic renewal; this tests
  # execution wiring, not producer construction or any source/native grant.
  for name in ('archaeology','ecology'):
   config=self.selected(name);projection=project_finite_pack_strings(config)
   source=object.__new__(FinitePackSource);source.definition=config.definition;source.model=config.model;source.graph=config.graph
   events=[]
   class Policy:
    @contextmanager
    def hold(self,context):
     events.append('hold')
     try:yield
     finally:events.append('close')
    def admit_original_release(self,release,context):events.append('admit')
   class Transport:
    bad=False
    drift_after_case=False
    def query(self,language,script,context):
     self.assertion=(language,script)
     if 'id(n) AS native_id'in script:
      role='edges'if 'native_src'in script else'nodes';label='FinitePackEdge'if role=='edges'else'FinitePackNode'
      rows=[]
      for original in projection[role]:
       row=dict(original,native_id=label+'['+original['graph_id']+']')
       if role=='edges':row.update(native_src='FinitePackNode['+original['src']+']',native_dst='FinitePackNode['+original['dst']+']')
       rows.append(row)
      if self.bad and role=='edges':rows[0]['native_src']='FinitePackNode[foreign]'
      return rows
     if self.drift_after_case:self.bad=True
     return [dict(zip(projection['result_columns'],row))for row in projection['expected']]
   transport=Transport();policy=Policy()
   with patch.object(FinitePackSource,'metadata',return_value={'inert':'original'}):
    result=execute_finite_pack_strings(config,source=source,transport=transport,policy=policy,context=object(),observe=lambda record:events.append(record['case']))
    self.assertEqual(result['rows'],projection['expected']);self.assertEqual(events[-1],'close')
    transport.bad=True
    with self.assertRaises(ValueError):execute_finite_pack_strings(config,source=source,transport=transport,policy=policy,context=object(),observe=lambda record:None)
    self.assertEqual(events[-1],'close')

    transport.bad=False;transport.drift_after_case=True
    with self.assertRaises(ValueError):execute_finite_pack_strings(config,source=source,transport=transport,policy=policy,context=object(),observe=lambda record:None)
    self.assertEqual(events[-1],'close')
