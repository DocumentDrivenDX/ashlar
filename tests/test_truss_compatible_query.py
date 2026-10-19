# SPDX-License-Identifier: MIT OR Apache-2.0
"""Focused inert transport coverage; no native engine qualification."""
import copy,json,unittest
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import patch
import test_truss_value_projection as values
from ashlar_host.driver import NativeDriver
from ashlar_host.truss_feed_sessions import TrussFeedSessions
from ashlar_host.truss_weft_query import execute_truss_compatible_query
from ashlar_host.truss_compatible_binding import BACKEND
from ashlar_host.truss_value_projection import admit_truss_value_projection

class Tests(unittest.TestCase):
 def run_query(self,*,unknown=False,drift=False):
  from ashlar_host.weft_compatible_cli import CompatibleCompiler
  projection,columns,admission,artifact,request,manifest,expected=values.Tests().fixture()
  manifest={**manifest,'publication_id':'publication','schema_revisions_json':'{"installation":"2"}'}
  binding=json.loads(request['target']['bindingJson'])
  artifact.update(status='compiled',backend=BACKEND)
  artifact['obligations']=[{'id':name,'owner':'host','failureCode':'WFT-OBLIGATION','parameters':parameters}for name,parameters in [
   ('ashlar.candidate.publication',{**binding,'layoutRevision':None,'layoutSha256':None}),
   ('ashlar.candidate.scalarIntegrity',{'phase':'before-user-query','samePublicationRequired':True,'noPartialPublication':True,'checks':[]}),
   ('ashlar.compatibleSchemaRevisions',{})]]
  binding.update(layoutRevision=None,layoutSha256=None)
  request['target']['bindingJson']=json.dumps(binding)
  if unknown:artifact['obligations'][2]['id']='unknown'
  driver=object.__new__(NativeDriver);driver.source_sessions=object.__new__(TrussFeedSessions);driver.ack_sessions=driver.source_sessions
  driver.source_admission=admission;driver.context=object();driver.policy=SimpleNamespace(active={'manifest':manifest,'request':{'stream':'stream'},'expected':expected})
  driver.tables={'manifest':'manifest'}
  driver.transport=SimpleNamespace(targets={name:SimpleNamespace(uuid='uuid')for name in list(json.loads(manifest['table_versions_json']))+['manifest']},spark=object())
  driver.writer=lambda *a:nullcontext();driver.hold=lambda *a,**kw:nullcontext();closing=[];driver.validate_manifest=lambda *a:closing.append(True)
  rows=[{'label':'Beta','tag':{'state':'value','value':'tagged'}},{'label':'Delta','tag':{'state':'absent'}}]
  if drift:rows.pop()
  with patch('ashlar_host.truss_weft_query.manifest_pin_vector',return_value={}),patch('ashlar_host.truss_weft_query.resolve_publication',return_value=SimpleNamespace(descriptor=SimpleNamespace(raw=manifest))),patch('ashlar_host.truss_weft_query._query_aliases',return_value=nullcontext({})),patch('ashlar_host.truss_compatible_binding.truss_compatible_compile_request',return_value=request),patch('ashlar_host.weft_compatible_cli.compile_compatible',return_value=b'original-response'),patch('ashlar_host.truss_weft_query.decode_compile_artifact',return_value=artifact),patch('ashlar_host.truss_compatible_binding.admit_truss_compatible_revisions'),patch('ashlar_host.truss_value_projection.admit_truss_value_projection',return_value=projection),patch('ashlar_host.truss_weft_query._execute_string_artifact',return_value=(rows,[])):
   result=execute_truss_compatible_query(driver,object.__new__(CompatibleCompiler),request['sql'],revision='2',document_id='cohort',physical_revisions=('1','2'))
  self.assertEqual(len(closing),2);self.assertEqual(result['response_bytes'],b'original-response');return result
 def test_transport_keeps_original_response_and_complete_bag(self):self.assertEqual(len(self.run_query()['rows']),2)
 def test_unknown_obligation_refuses(self):
  with self.assertRaisesRegex(ValueError,'obligation inventory'):self.run_query(unknown=True)
 def test_incomplete_bag_refuses(self):
  with self.assertRaisesRegex(ValueError,'complete original'):self.run_query(drift=True)
 def test_required_only_projection_and_explicit_revision_bag(self):
  _,_,admission,artifact,request,manifest,expected=values.Tests().fixture()
  artifact=copy.deepcopy(artifact);artifact['columns']=artifact['columns'][:1];artifact['logicalPlan']['outputs']=artifact['logicalPlan']['outputs'][:1];artifact['logicalPlan']['typeGraph']=artifact['logicalPlan']['typeGraph'][:1]
  artifact['logicalPlan']['requiredCapabilities']=['project','scan','type.string']
  with patch('ashlar_host.truss_compatible_binding.truss_compatible_compile_request',return_value=request):
   projection=admit_truss_value_projection(admission,artifact,request,manifest,expected,revision='2',document_id='cohort',physical_revisions=('1','2'),require_optional=False)
   projection.require_complete_bag([{'label':'Delta'},{'label':'Beta'}])
   with self.assertRaisesRegex(ValueError,'physical revisions'):
    admit_truss_value_projection(admission,artifact,request,manifest,expected,revision='2',document_id='cohort',physical_revisions=('2',),require_optional=False)
