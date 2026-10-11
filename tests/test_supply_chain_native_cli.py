"""Pure installed supply-chain config/report/lifecycle controls; no native SDK."""
import contextlib,io,json,tempfile,unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar.weft_decode import ExactScalar
from ashlar_host import supply_chain_native_cli as cli,supply_chain_native as native
from ashlar_host.exact_report import report_body

class SupplyChainNativeCliTests(unittest.TestCase):
 def configuration(self,root):
  value={'profile':cli.PROFILE,'output':str(root/'publication'),'jars':str(root/'jars'),'pack':str(root/'pack.json'),'model':str(root/'model.json'),'graph':str(root/'graph.json'),'producer':{'source':str(root/'umf'),'bun':str(root/'bun'),'git':str(root/'git'),'timeout_seconds':30,'maximum_output_bytes':1048576,'maximum_receipt_bytes':8388608},'compiler':{'index':str(root/'index.json'),'installation':str(root/'compiler')},'operation_capacity':None}
  path=root/'invocation.json';path.write_text(json.dumps(value));return path,value
 def test_config_rejects_unknown_duplicate_nonfinite_and_large_integer_before_effects(self):
  with tempfile.TemporaryDirectory()as tmp:
   path,value=self.configuration(Path(tmp).resolve());raw=path.read_bytes()
   cases=[raw[:-1]+b',"profile":"duplicate"}',raw.replace(b'"timeout_seconds": 30',b'"timeout_seconds": '+b'9'*60000),raw.replace(b'"timeout_seconds": 30',b'"timeout_seconds": true'),raw.replace(b'"operation_capacity": null',b'"operation_capacity": NaN'),json.dumps({**value,'extra':True}).encode(),json.dumps({**value,'output':value['compiler']['installation']}).encode()]
   with patch.object(cli,'publish_query_supply_chain_native')as called:
    for invalid in cases:
     path.write_bytes(invalid)
     with self.assertRaises(ValueError):cli.run_supply_chain_native_command(str(path))
    called.assert_not_called()
 def test_typed_config_custody_and_native_cleanup_before_report(self):
  with tempfile.TemporaryDirectory()as tmp:
   root=Path(tmp).resolve();path,_=self.configuration(root);events=[]
   def run(config,*,config_custody):
    self.assertEqual(config.producer.maximum_receipt_bytes,8388608);config.output.mkdir(mode=0o700)
    events.append('native cleanup');config_custody.renew(cli.CONFIG_LIMIT)
    events.append('config closing');return native._finish_report(config,{'cases':[{'decoded':[[ExactScalar(Decimal('17.00'),'17.00')]]}],'qualification':'inert serialization control'})
   with patch.object(cli,'publish_query_supply_chain_native',side_effect=run):result=cli.run_supply_chain_native_command(str(path))
   self.assertEqual(events,['native cleanup','config closing']);report=json.loads(Path(result['report']).read_bytes())
   self.assertEqual(report['cases'][0]['decoded'][0][0],{'$type':'ExactScalar','value':{'$type':'Decimal','text':'17.00'},'original':'17.00'})
 def test_failure_config_closing_and_primary_cancellation_identity(self):
  for primary in (ValueError('original'),KeyboardInterrupt(),SystemExit()):
   with tempfile.TemporaryDirectory()as tmp:
    root=Path(tmp).resolve();path,_=self.configuration(root)
    def run(*args,**kwargs):path.write_bytes(b'changed');raise primary
    with patch.object(cli,'publish_query_supply_chain_native',side_effect=run):
     try:cli.run_supply_chain_native_command(str(path))
     except BaseException as error:self.assertIs(error,primary)
     else:self.fail('primary lost')
    self.assertFalse((root/'publication/report.json').exists())
 def test_closing_cancellation_outranks_ordinary_body_failure(self):
  from ashlar_host.evolution_cli import InvocationFile
  for primary,closing,expected in ((ValueError('body'),KeyboardInterrupt(),True),(SystemExit(),ValueError('close'),False)):
   with tempfile.TemporaryDirectory()as tmp:
    path,_=self.configuration(Path(tmp).resolve())
    with patch.object(cli,'publish_query_supply_chain_native',side_effect=primary),patch.object(InvocationFile,'renew',side_effect=closing):
     try:cli.run_supply_chain_native_command(str(path))
     except BaseException as error:self.assertIs(error,closing if expected else primary)
     else:self.fail('failure lost')
 def test_observer_exact_rows_and_aggregate_bound(self):
  with tempfile.TemporaryDirectory()as tmp:
   root=Path(tmp).resolve();path,_=self.configuration(root);config=cli.parse_supply_chain_native_invocation(path.read_bytes()).native;config.output.mkdir()
   capture={'schema':[['same','STRING'],['same','STRING']],'rows':[['17.00','original'],['17.00','original']]}
   def query(*args,observe,observe_native):observe_native('excursion',capture);return {'cases':[]}
   with patch.object(native,'_schemas',return_value=None),patch.object(native,'CountStarAdmissionConfig',return_value=None),patch.object(native,'CountStarExecutionConfig',return_value=None),patch.object(native,'finite_public_source',return_value=None),patch.object(native,'query_supply_chain_cases',side_effect=query):native._queries(config,SimpleNamespace(source=None),b'pack',{})
   retained=json.loads((config.output/'native-observations.jsonl').read_bytes());self.assertEqual(retained,{'id':'excursion','captured':capture})
   self.assertEqual(capture['rows'][0],['17.00','original'])
   with self.assertRaises(ValueError):report_body(retained,32)
 def test_actual_native_lifecycle_custody_precedes_final_report(self):
  from ashlar_host.evolution_cli import InvocationFile
  from ashlar_host.supply_chain_native import GRAPH_ROLES
  class Spark:
   def sql(self,sql):
    if sql.startswith('CREATE TABLE delta.'):
     Path(sql.split('`')[1]).mkdir()
    return SimpleNamespace(first=lambda:{'id':'original-uuid'})
   def stop(self):events.append('spark-close')
  class Transport:
   def __init__(self,targets):self.targets={t.table:t for t in targets}
   def original_history(self,target):return []
   def close(self):events.append('transport-close')
  class Lease:
   def __init__(self,*args,**kwargs):pass
   def renew(self):events.append('lease-renew')
   def close(self):events.append('lease-close')
  class Driver:
   def __init__(self,*args,**kwargs):self.source=args[5];self.publication_id=args[6]
   def prepare_original(self,request):pass
   def restore_committed(self,request):events.append('restored')
   def backend(self):return object()
  for mutation,primary in ((False,None),(True,None),(False,KeyboardInterrupt())):
   with tempfile.TemporaryDirectory()as tmp:
    root=Path(tmp).resolve();path,_=self.configuration(root);config=cli.parse_supply_chain_native_invocation(path.read_bytes()).native
    events=[];source=SimpleNamespace(batch=SimpleNamespace(batch_id='original'),renew=lambda:events.append('source-renew'),metadata=lambda:{'source':'original'})
    producer=SimpleNamespace(metadata=lambda:{'producer':'actual-inert-control'});targets=[];descriptor=SimpleNamespace(versions={},raw={'publication_id':'original'})
    def initialize(spark,journal,installation,selected,*args,**kwargs):targets.extend(selected);return Transport(selected)
    def queried(*args):
     events.append('queries')
     if mutation:path.write_bytes(b'changed')
     if primary is not None:raise primary
     return {'cases':[{'decoded':[[ExactScalar(17,'17')]]}]}
    def report(*args):events.append('report');return {'report':'inert'}
    patches=[patch.object(native,'_runtime',return_value=[]),patch.object(native,'_spark',return_value=Spark()),patch.object(native,'snapshot',return_value=b'original'),patch.object(native,'supply_chain_cases'),patch.object(native,'PrivateFiniteLease',Lease),patch.object(native,'HeldSupplyChainDataset',return_value=producer),patch.object(native,'FiniteSupplyChainSource',return_value=source),patch.object(native,'fixture_columns',return_value={r:(('id','STRING'),)for r in GRAPH_ROLES}),patch.object(native,'_request',return_value={'stream':'original','predecessor':'origin','schema_revisions_json':'{}'}),patch.object(native,'_write'),patch.object(native.LocalDeltaTransport,'initialize',side_effect=initialize),patch.object(native.ReadOnlyTransport,'open',side_effect=lambda *a,**k:Transport(targets)),patch.object(native,'FiniteSupplyChainDriver',Driver),patch.object(native,'publish_batch',return_value=descriptor),patch.object(native,'_queries',side_effect=queried),patch.object(native,'_finish_report',side_effect=report)]
    with contextlib.ExitStack()as stack:
     for selected in patches:stack.enter_context(selected)
     custody=InvocationFile.read(path,cli.CONFIG_LIMIT)
     if mutation or primary is not None:
      try:native.publish_query_supply_chain_native(config,config_custody=custody)
      except BaseException as error:
       if primary is not None:self.assertIs(error,primary)
      else:self.fail('failure did not withhold report')
      self.assertNotIn('report',events)
     else:
      native.publish_query_supply_chain_native(config,config_custody=custody)
      self.assertGreater(events.index('report'),events.index('spark-close'));self.assertGreater(events.index('report'),events.index('lease-close'))
    self.assertIn('source-renew',events);self.assertIn('spark-close',events);self.assertIn('lease-close',events)
 def test_aggregate_observation_budget_refuses_second_and_retains_first(self):
  with tempfile.TemporaryDirectory()as tmp:
   root=Path(tmp).resolve();path,_=self.configuration(root);config=cli.parse_supply_chain_native_invocation(path.read_bytes()).native;config.output.mkdir()
   first=report_body({'id':'first','captured':{'schema':[],'rows':[]}},10000);remaining=[]
   def body(value,maximum):
    remaining.append(maximum)
    if len(remaining)==1:return first
    self.assertEqual(maximum,64*1024*1024-len(first));raise ValueError('whole observation capacity')
   def query(*args,observe,observe_native):
    observe_native('first',{'schema':[],'rows':[]});observe_native('second',{'schema':[],'rows':[]})
   with patch.object(native,'_schemas',return_value=None),patch.object(native,'CountStarAdmissionConfig',return_value=None),patch.object(native,'CountStarExecutionConfig',return_value=None),patch.object(native,'finite_public_source',return_value=None),patch.object(native,'query_supply_chain_cases',side_effect=query),patch.object(native,'report_body',side_effect=body):
    with self.assertRaisesRegex(ValueError,'whole observation'):native._queries(config,SimpleNamespace(source=None),b'pack',{})
   self.assertEqual((config.output/'native-observations.jsonl').read_bytes(),first)
 def test_public_cli_successful_dispatch_and_help_without_sdk_imports(self):
  import subprocess,sys
  from ashlar.cli import main
  with tempfile.TemporaryDirectory()as tmp:
   root=Path(tmp).resolve();path,_=self.configuration(root)
   def owner(config,*,config_custody):
    self.assertIs(type(config),native.SupplyChainNativeConfig);self.assertEqual(config.pack,root/'pack.json');config_custody.renew(cli.CONFIG_LIMIT)
    return {'cases':5,'report':'original','qualification':'inert composition'}
   with patch('sys.argv',['ashlar','run-supply-chain-native','--configuration',str(path)]),patch.object(cli,'publish_query_supply_chain_native',side_effect=owner),contextlib.redirect_stdout(io.StringIO())as stream:main()
   self.assertEqual(json.loads(stream.getvalue())['cases'],5)
  code="import sys;from ashlar.cli import main;sys.argv=['ashlar','run-supply-chain-native','--help'];\ntry: main()\nexcept SystemExit as e: assert e.code==0\nassert 'pyspark' not in sys.modules and 'ashlar_host.supply_chain_native' not in sys.modules"
  result=subprocess.run([sys.executable,'-S','-B','-c',code],capture_output=True,timeout=10)
  self.assertEqual(result.returncode,0,result.stderr);self.assertIn(b'--configuration',result.stdout)
 def test_public_cli_payload_free_refusal(self):
  from ashlar.cli import main
  with patch('sys.argv',['ashlar','run-supply-chain-native','--configuration','/secret/path']),patch.object(cli,'run_supply_chain_native_command',side_effect=ValueError('secret value')),contextlib.redirect_stderr(io.StringIO())as stream:
   with self.assertRaises(SystemExit)as caught:main()
  self.assertEqual(caught.exception.code,2);self.assertNotIn('secret',stream.getvalue())

if __name__=='__main__':unittest.main()
