"""Small typed RELATED_KEYS held transport ports; no engine qualification."""
import json,unittest
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import patch
import test_truss_value_projection as values
from ashlar_host.driver import NativeDriver
from ashlar_host.truss_feed_sessions import TrussFeedSessions
from ashlar_host.truss_weft_query import execute_truss_typed_related_keys_query,_execute_string_artifact
from ashlar_host.weft_compatible_cli import CompatibleCompiler
from ashlar_host.truss_compatible_binding import BACKEND

class Tests(unittest.TestCase):
 def run_query(self,*,drift=False,no_related=False):
  _,columns,admission,artifact,request,manifest,expected=values.Tests().fixture();manifest={**manifest,'publication_id':'original','schema_revisions_json':'{"installation":"2"}'}
  binding=json.loads(request['target']['bindingJson']);binding.update(layoutRevision=None,layoutSha256=None);request['target']['bindingJson']=json.dumps(binding)
  artifact.update(status='compiled',backend=BACKEND);artifact['columns']=artifact['columns'][:1]
  if not no_related:artifact['columns'].append({'outputName':'targets','position':2,'representation':{'kind':'relatedKeys'}})
  artifact['obligations']=[{'id':name,'owner':'host','failureCode':'WFT-OBLIGATION','parameters':params}for name,params in [('ashlar.candidate.publication',binding),('ashlar.candidate.scalarIntegrity',{'phase':'before-user-query','samePublicationRequired':True,'noPartialPublication':True,'checks':[]}),('ashlar.candidate.relationshipIntegrity',{'phase':'before-user-query','samePublicationRequired':True,'checks':[{'original':True}]})]]
  driver=object.__new__(NativeDriver);driver.source_sessions=object.__new__(TrussFeedSessions);driver.ack_sessions=driver.source_sessions;driver.source_admission=admission;driver.context=object();driver.policy=SimpleNamespace(active={'manifest':manifest,'request':{'stream':'stream'},'expected':expected});driver.tables={'manifest':'manifest'};driver.transport=SimpleNamespace(targets={name:SimpleNamespace(uuid='uuid')for name in list(json.loads(manifest['table_versions_json']))+['manifest']},spark=object());driver.writer=lambda *a:nullcontext();driver.hold=lambda *a,**kw:nullcontext();closing=[];driver.validate_manifest=lambda *a:closing.append(True)
  wanted=[{'label':'Beta','targets':{'items':[['Delta']],'truncated':False}}];rows=[]if drift else wanted
  with patch('ashlar_host.truss_weft_query.manifest_pin_vector',return_value={}),patch('ashlar_host.truss_weft_query.resolve_publication',return_value=SimpleNamespace(descriptor=SimpleNamespace(raw=manifest))),patch('ashlar_host.truss_weft_query._query_aliases',return_value=nullcontext({})),patch('ashlar_host.truss_weft_query.truss_scalar_compile_request',return_value=request),patch('ashlar_host.weft_compatible_cli.compile_compatible',return_value=b'original-response'),patch('ashlar_host.truss_weft_query.decode_compile_artifact',return_value=artifact),patch('ashlar_host.truss_weft_query.admit_truss_related_keys',return_value=wanted),patch('ashlar_host.truss_weft_query._execute_string_artifact',return_value=(rows,[])),patch('ashlar_host.truss_compatible_binding.admit_truss_compatible_revisions')as incompatible:
   result=execute_truss_typed_related_keys_query(driver,object.__new__(CompatibleCompiler),request['sql'],revision='2',document_id='cohort')
  incompatible.assert_not_called();self.assertEqual(len(closing),2);self.assertEqual(result['response_bytes'],b'original-response');return result
 def test_original_typed_transport_retains_related_bag_and_current_only(self):self.assertEqual(len(self.run_query()['rows']),1)
 def test_incomplete_related_bag_and_operator_free_query_refuse(self):
  with self.assertRaisesRegex(ValueError,'related-key occurrence'):self.run_query(drift=True)
  with self.assertRaisesRegex(ValueError,'Only direct current'):self.run_query(no_related=True)
 def test_original_guard_failure_prevents_result_sql(self):
  columns=[{'outputName':'targets','position':1,'nullable':False,'representation':{'kind':'relatedKeys'}}];artifact={'columns':columns,'parameters':[],'sql':'SELECT original_result','obligations':[{'id':'ashlar.candidate.scalarIntegrity','parameters':{'checks':[{'sql':'SELECT original_guard','failureCode':'WFT-NUMERIC-DOMAIN','record':{}}]}},{'id':'ashlar.candidate.relationshipIntegrity','parameters':{'checks':[]}}]};calls=[]
  def capture(frame,*,config):calls.append(frame);return {'schema':[['violations','STRING']],'rows':[['1']]}
  with patch('ashlar_host.path_capture.capture_string_frame',side_effect=capture),self.assertRaisesRegex(ValueError,'integrity check refused'):_execute_string_artifact(SimpleNamespace(sql=lambda sql,args:sql),artifact,1,related=True)
  self.assertEqual(calls,['SELECT original_guard'])

 def test_explicit_execution_modes_require_bounded_integer_budget(self):
  from ashlar_host.truss_weft_query import execute_truss_compatible_query,execute_truss_scalar_query,execute_truss_scalar_join_query,execute_truss_scalar_join_count_query,execute_truss_numeric_grouped_count_query
  for operation in (execute_truss_typed_related_keys_query,execute_truss_compatible_query,execute_truss_scalar_query,execute_truss_scalar_join_query,execute_truss_scalar_join_count_query,execute_truss_numeric_grouped_count_query):
   for budget in (None,False,0,10001):
    kwargs={}if operation is execute_truss_typed_related_keys_query else{'physical_revisions':('1',)}
    with self.subTest(operation=operation.__name__,budget=budget),self.assertRaisesRegex(ValueError,'execution row budget'):operation(object(),object(),'SELECT original',revision='1',document_id='original',maximum_rows=budget,**kwargs)

 def test_legacy_compile_none_budget_remains_compile_only(self):
  from ashlar_host.truss_weft_query import compile_truss_string_query
  with patch('ashlar_host.truss_weft_query._truss_string_query',return_value={'original':'compile'})as compile_query:
   self.assertEqual(compile_truss_string_query(object(),object(),'SELECT original',revision='1',document_id='original'),{'original':'compile'})
  self.assertEqual(compile_query.call_args.kwargs,{'revision':'1','document_id':'original'})
