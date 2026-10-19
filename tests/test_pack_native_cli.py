# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure installed invocation/custody controls; no producer, compiler or Spark runs."""
import contextlib,copy,io,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace
from ashlar_host import pack_native_cli as selected
from test_count_star_admission import SCHEMAS
from ashlar_host.count_star_admission import CountStarSchemaValidation

class NativeCliTests(unittest.TestCase):
 def configuration(self,root,mode='query'):
  value={'profile':selected.PROFILE,'mode':mode,'pack':'archaeology','publication':str(root/'publication'),'jars':str(root/'jars'),'model':str(root/'model.json'),'graph':str(root/'graph.json'),'pack_file':str(root/'pack.json'),'producer':{'source':str(root/'umf'),'bun':str(root/'bun'),'git':str(root/'git'),'timeout_seconds':30,'maximum_output_bytes':1048576,'maximum_receipt_bytes':33554432},'operation_capacity':None,'output':str(root/'observations')}
  if mode=='query':value['query']={'index':str(root/'index.json'),'installation':str(root/'compiler'),'maximum_artifact_bytes':1048576,'maximum_rows':100,'maximum_cell_bytes':10000,'maximum_total_cell_bytes':100000,'maximum_observation_bytes':1048576}
  path=root/'configuration.json';path.write_text(json.dumps(value));return path,value
 def test_closed_config_and_original_selection(self):
  with tempfile.TemporaryDirectory()as tmp:
   root=Path(tmp).resolve();path,value=self.configuration(root)
   parsed=selected.parse_finite_pack_invocation(path.read_bytes(),'query');self.assertEqual(parsed.native.producer.definition.name,'archaeology');self.assertEqual(parsed.native.output,root/'publication')
   for change in ({'pack':'medical'},{'extra':'unsupported'},{'output':str(root/'publication'/'nested')}):
    invalid={**value,**change}
    with self.assertRaises(ValueError):selected.parse_finite_pack_invocation(json.dumps(invalid).encode(),'query')
 def test_oversized_duplicate_float_and_bool_bounds_before_owner(self):
  with tempfile.TemporaryDirectory()as tmp:
   path,value=self.configuration(Path(tmp).resolve());raw=path.read_bytes()
   for invalid in (raw[:-1]+b',"mode":"query"}',raw.replace(b'"maximum_rows": 100',b'"maximum_rows": '+b'9'*60000),raw.replace(b'"maximum_rows": 100',b'"maximum_rows": true'),raw.replace(b'"maximum_rows": 100',b'"maximum_rows": 1.1')):
    path.write_bytes(invalid)
    with patch.object(selected,'_open')as opened,patch.object(selected,'_publish')as publish:
     with self.assertRaises(ValueError):selected.run_finite_pack_command(str(path),'query')
     opened.assert_not_called();publish.assert_not_called()
 def test_publish_final_after_owner_cleanup_and_configuration_custody(self):
  with tempfile.TemporaryDirectory()as tmp:
   root=Path(tmp).resolve();path,value=self.configuration(root,'publish')
   with patch.object(selected,'_publish',return_value={'source':{'original':True},'manifest':{'publication_id':'original'},'qualification':'inert owner control'}):result=selected.run_finite_pack_command(str(path),'publish')
   report=json.loads(Path(result['report']).read_bytes());self.assertTrue(report['outerCleanupComplete']);self.assertTrue(report['invocationCustodyComplete'])
 def test_mutated_configuration_and_cancel_identity_withhold_report(self):
  for primary in (ValueError('body'),KeyboardInterrupt(),SystemExit()):
   with tempfile.TemporaryDirectory()as tmp:
    root=Path(tmp).resolve();path,_=self.configuration(root,'publish')
    def publish(config):path.write_bytes(b'changed');raise primary
    with patch.object(selected,'_publish',side_effect=publish):
     try:selected.run_finite_pack_command(str(path),'publish')
     except BaseException as error:self.assertIs(error,primary)
     else:self.fail('failure lost')
    self.assertFalse((root/'observations/report.json').exists())
 def test_raw_observations_retained_before_failure_and_native_cleanup(self):
  for primary,closing,expect_closing in ((ValueError('comparison'),KeyboardInterrupt(),True),(SystemExit(),ValueError('cleanup'),False)):
   with tempfile.TemporaryDirectory()as tmp:
    root=Path(tmp).resolve();path,_=self.configuration(root);events=[]
    @contextlib.contextmanager
    def opened(config,receipt):
     events.append('open')
     try:yield SimpleNamespace(source=object())
     finally:events.append('close');raise closing
    def query(driver,pack,aliases,compile_config,execution_config,*,observe,observe_native):
     observe('cycle',0,b'original request',b'original response');observe_native('cycle',{'schema':[['original','STRING']],'rows':[['original']]});raise primary
    schema=CountStarSchemaValidation(SCHEMAS,lambda *args:None)
    with patch.object(selected,'_open',opened),patch.object(selected,'_aliases',return_value={}),patch('ashlar.weft_count_star_distribution.installed_count_star_schema_bundle',return_value=tuple(SCHEMAS.items())),patch('ashlar_host.count_star_schema.make_offline_count_star_schema_validation',return_value=schema),patch('ashlar_host.evolution_producer.snapshot',return_value=b'original pack'),patch('ashlar_host.pack_count_star_query.finite_pack_public_source',return_value=lambda *args:None),patch('ashlar_host.pack_count_star_query.query_pack_count_star_cases',side_effect=query):
     try:selected.run_finite_pack_command(str(path),'query')
     except BaseException as error:self.assertIs(error,closing if expect_closing else primary)
     else:self.fail('failure lost')
    self.assertEqual(events,['open','close']);self.assertFalse((root/'observations/report.json').exists());observed=[json.loads(r)for r in (root/'observations/observations.jsonl').read_bytes().splitlines()]
    self.assertEqual(bytes.fromhex(observed[0]['requestHex']),b'original request');self.assertEqual(observed[1]['capture']['rows'],[['original']])
 def test_output_bound_refuses_before_allocating_report(self):
  with tempfile.TemporaryDirectory()as tmp:
   path=Path(tmp).resolve()/'bounded.json'
   with self.assertRaises(ValueError):selected._write(path,{'x':'large'},4)
   self.assertFalse(path.exists())
 def test_cli_dispatch_and_payload_free_refusal(self):
  from ashlar.cli import main
  with patch('sys.argv',['ashlar','query-finite-pack','--configuration','/sensitive/path']),patch.object(selected,'run_finite_pack_command',side_effect=ValueError('secret contents')),contextlib.redirect_stderr(io.StringIO())as stderr:
   with self.assertRaises(SystemExit)as caught:main()
  self.assertEqual(caught.exception.code,2);self.assertNotIn('sensitive',stderr.getvalue());self.assertNotIn('secret',stderr.getvalue())
 def installed_schemas(self):
  import zlib,hashlib
  path=Path(__file__).parent/'fixtures/weft_count_star_installed_schema_bundle.json.zlib';raw=path.read_bytes();self.assertLess(len(raw),20000)
  expander=zlib.decompressobj();body=expander.decompress(raw,300001)
  self.assertTrue(expander.eof);self.assertFalse(expander.unused_data or expander.unconsumed_tail);self.assertLess(len(body),300001);self.assertEqual(hashlib.sha256(body).hexdigest(),'9efcd78751313f2c80d5fa5f617d6b8ca21d897876f553ad9c6536a1fb5f857b')
  schemas={k:v.encode()for k,v in json.loads(body).items()}
  self.assertEqual(set(schemas),{'application-result-v0.2.schema.json','application-result-v0.4.schema.json','backend-manifest-v0.3.schema.json','compile-request-v0.4.1.schema.json','compile-request-v0.4.schema.json','compile-response-v0.4.1.schema.json','compile-response-v0.4.schema.json','logical-plan-v0.4.1.schema.json','logical-plan-v0.4.schema.json'})
  self.assertEqual({k:hashlib.sha256(v).hexdigest()for k,v in schemas.items()},{'application-result-v0.2.schema.json': '0e9fc7aa16d14a4e78027a70514d75476d1576f9625747e1b5f56b76147f8014', 'application-result-v0.4.schema.json': 'ee87e4e845d670601a301701dc6109c7a37dadb96ccb1e7f75c8144c5bd8bc2a', 'backend-manifest-v0.3.schema.json': '655f2dea7b459b354af1f66ae1684f1ba2bce907a5c7355d34c9f9c461f9ea7a', 'compile-request-v0.4.1.schema.json': '6da5cd0b5a0cd8e92ce6e179f8e092ecc3c3c134b2d32d25d0dbe2c87b39a1ef', 'compile-request-v0.4.schema.json': '51f9793fb7ca32fdde778e2ff8cacd1018e4142fb04dea610005895aacec3932', 'compile-response-v0.4.1.schema.json': '04dd09885d93fa0aa53152a1b686a01c8303549318b12555364f7b6c564ee8cb', 'compile-response-v0.4.schema.json': 'c177b9bbb9fff3d69a833aa5bf05dbe752a1f76e65833cae8ea35fd4b1373140', 'logical-plan-v0.4.1.schema.json': '3e371f1ce3814c9328802c3981483c1ed97f97f34fd3b6767c54ebfec8c083e4', 'logical-plan-v0.4.schema.json': 'a6f28858646e4795019037233ef0e8d6b0aeb45e7440cbcf4e7a477891a1b3d5'})
  return schemas
 def decoded(self):
  from ashlar.weft_decode import decode_exact_scalar
  from test_weft_path_decode import PathDecodeTests,row
  from test_weft_related_keys_decode import RelatedKeysDecodeTests
  def rep(family,facets,decoder):return {'kind':'scalar','logicalType':{'family':family,'facets':facets,'nullable':False},'carrier':'text','decoder':decoder}
  return [decode_exact_scalar(rep('decimal',{'precision':18,'scale':2},'exact-decimal'),'-0.00'),decode_exact_scalar(rep('integer',{'integerWidth':{'bits':64,'signed':True}},'exact-integer'),'09007199254740993'),PathDecodeTests().decode({'items':[row(),row()],'truncated':False}),RelatedKeysDecodeTests().decode(b' {"items":[["a"],["a"]],"truncated":false}\n')]
 def test_successful_query_exact_nine_schema_selection_and_decoded_report(self):
  schemas=self.installed_schemas();decoded=self.decoded();events=[];selections=[]
  with tempfile.TemporaryDirectory()as tmp:
   root=Path(tmp).resolve();path,value=self.configuration(root)
   value['profile']=selected.SOURCE_PROFILE;value['query']['maximum_public_source_bytes']=2097152;path.write_text(json.dumps(value))
   @contextlib.contextmanager
   def opened(config,receipt):
    events.append('open')
    try:yield SimpleNamespace(source=object())
    finally:events.append('closed')
   def validation(bundle):
    from ashlar_host.count_star_admission import SCHEMA_SHA256
    self.assertEqual(set(bundle),set(SCHEMA_SHA256));self.assertEqual(bundle,{k:schemas[k]for k in SCHEMA_SHA256});selections.append(bundle)
    # Actual owning constructor enforces exact three pins, even with a claim-only
    # callback. This pure control makes no offline SDK validation claim.
    return CountStarSchemaValidation(bundle,lambda *args:None)
   def query(*args,**kwargs):
    self.assertEqual(args[4].maximum_public_source_bytes,2097152);self.assertEqual(args[4].admission.maximum_artifact_bytes,1048576)
    kwargs['observe']('cycle',0,b'original request',b'original response');kwargs['observe_native']('cycle',{'schema':[['raw','STRING']],'rows':[['-0.00']]})
    return {'cases':[{'id':'cycle','evidence':{'decoded':[decoded],'native_result':{'schema':[['raw','STRING']],'rows':[['-0.00']]}}}],'qualification':'inert complete query control'}
   with patch.object(selected,'_open',opened),patch.object(selected,'_aliases',return_value={}),patch('ashlar.weft_count_star_distribution.installed_count_star_schema_bundle',return_value=tuple(schemas.items())),patch('ashlar_host.count_star_schema.make_offline_count_star_schema_validation',side_effect=validation),patch('ashlar_host.evolution_producer.snapshot',return_value=b'original pack'),patch('ashlar_host.pack_count_star_query.finite_pack_public_source',return_value=lambda *args:None)as source_port,patch('ashlar_host.pack_count_star_query.query_pack_count_star_cases',side_effect=query):answer=selected.run_finite_pack_command(str(path),'query')
   self.assertEqual(source_port.call_args.args[1],2097152)
   self.assertEqual(events,['open','closed']);self.assertEqual(len(selections),1);report=json.loads(Path(answer['report']).read_bytes());self.assertTrue(report['outerCleanupComplete'])
   stored=report['cases'][0]['evidence']['decoded'][0]
   self.assertEqual(stored[0]['value'],{'$type':'Decimal','text':'-0.00'});self.assertEqual(stored[0]['original'],'-0.00')
   self.assertEqual(stored[1]['value'],{'$type':'integer','text':'9007199254740993'});self.assertEqual(stored[1]['original'],'09007199254740993')
   self.assertEqual(bytes.fromhex(stored[2]['original']['hex']),decoded[2].original);self.assertEqual(stored[2]['items'][0]['$type'],'PathItem');self.assertEqual(len(stored[2]['items']),2)
   self.assertEqual(bytes.fromhex(stored[3]['original']['hex']),decoded[3].original);self.assertEqual(stored[3]['items'],[['a'],['a']]);self.assertEqual(decoded[0].original,'-0.00');self.assertEqual(len(decoded[2].items),2)
 def test_report_unknown_carriers_refused_without_string_coercion(self):
  from dataclasses import dataclass
  from decimal import Decimal
  @dataclass
  class Unknown:value:str
  class Executable:
   def __str__(self):raise AssertionError('must not execute')
  for value in (Unknown('source'),Executable(),1.5,Decimal('NaN'),2**1000):
   with self.assertRaises(ValueError):selected._body({'decoded':value},1048576)
 def test_nested_decoded_key_integer_decimal_and_original_bytes_exact(self):
  from decimal import Decimal
  from ashlar.weft_path_decode import PathItem,DecodedPaths
  from ashlar.weft_related_keys_decode import DecodedRelatedKeys
  original=b'original carrier bytes including whitespace\n';item=PathItem((2**53-1,2**53,2**53+1),(Decimal('17.00'),),('1','2'))
  value={'metadataOrdinal':2**53+1,'paths':DecodedPaths(original,'value',(item,),False),'keys':DecodedRelatedKeys(original,((2**53+1,Decimal('-0.00')),),False)}
  report=json.loads(selected._body(value,1048576))
  self.assertEqual(report['metadataOrdinal'],2**53+1)
  self.assertEqual(report['paths']['items'][0]['intermediate'],[{'$type':'integer','text':str(i)}for i in (2**53-1,2**53,2**53+1)])
  self.assertEqual(report['paths']['items'][0]['terminal'],[{'$type':'Decimal','text':'17.00'}]);self.assertEqual(report['keys']['items'][0],[{'$type':'integer','text':str(2**53+1)},{'$type':'Decimal','text':'-0.00'}])
  self.assertEqual(bytes.fromhex(report['paths']['original']['hex']),original);self.assertEqual(item.intermediate,(2**53-1,2**53,2**53+1))
  class Unknown:pass
  with self.assertRaises(ValueError):selected._body(DecodedPaths(original,'value',(PathItem((Unknown(),),(),()),),False),1048576)
 def test_independent_source_budget_requires_explicit_new_profile_and_closed_fields(self):
  with tempfile.TemporaryDirectory()as tmp:
   root=Path(tmp).resolve();_,value=self.configuration(root)
   legacy=selected.parse_finite_pack_invocation(json.dumps(value).encode(),'query');self.assertEqual(legacy.profile,selected.PROFILE)
   value['query']['maximum_public_source_bytes']=2097152
   with self.assertRaises(ValueError):selected.parse_finite_pack_invocation(json.dumps(value).encode(),'query')
   value['profile']=selected.SOURCE_PROFILE
   admitted=selected.parse_finite_pack_invocation(json.dumps(value).encode(),'query')
   self.assertEqual(admitted.query['maximum_public_source_bytes'],2097152);self.assertEqual(admitted.query['maximum_artifact_bytes'],1048576)
   for invalid in (True,False,0,-1,16777217,2.0,'2097152'):
    changed=copy.deepcopy(value);changed['query']['maximum_public_source_bytes']=invalid
    with self.assertRaises(ValueError):selected.parse_finite_pack_invocation(json.dumps(changed).encode(),'query')
   missing=copy.deepcopy(value);del missing['query']['maximum_public_source_bytes']
   with self.assertRaises(ValueError):selected.parse_finite_pack_invocation(json.dumps(missing).encode(),'query')

 def test_new_source_profile_is_query_only_before_publish_owner(self):
  with tempfile.TemporaryDirectory()as tmp:
   path,value=self.configuration(Path(tmp).resolve(),'publish');value['profile']=selected.SOURCE_PROFILE;path.write_text(json.dumps(value))
   with patch.object(selected,'_publish')as publication:
    with self.assertRaises(ValueError):selected.run_finite_pack_command(str(path),'publish')
    publication.assert_not_called()
   value['profile']=selected.PROFILE
   original=selected.parse_finite_pack_invocation(json.dumps(value).encode(),'publish')
   with self.assertRaises(ValueError):selected.FinitePackInvocation(original.mode,original.native,original.output,original.query,selected.SOURCE_PROFILE)
