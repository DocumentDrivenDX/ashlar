import copy,json,tempfile
from pathlib import Path
from unittest import TestCase,mock
import test_supply_chain_installed_request as original_tests
from ashlar_host import supply_chain_cli as owner

class Controls(TestCase):
 def setUp(self):
  self.original=original_tests.Tests();self.original.setUp()
  self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup);self.root=Path(self.directory.name).resolve()
  for name,raw in zip(('pack','model','graph'),self.original.raw):(self.root/name).write_bytes(raw)
  (self.root/'metadata').write_text(json.dumps(dict(bindings=self.original.bindings,manifest=self.original.manifest,registry=self.original.registry,aliases=self.original.aliases)))
  self.args={k:self.root/k for k in ('pack','model','graph','metadata','index','installation','output')};self.args['maximum_artifact_bytes']=1000000
 def test_original_inputs_and_real_report_only_after_closing(self):
  def compile(*args,observe):
   observe('replay',0,b'original request',b'original response')
   return {'cases':[{'id':'replay','requestBytes':b'original request'}],'qualification':'compiler only'}
  with mock.patch.object(owner,'compile_supply_chain_cases',side_effect=compile):r=owner.compile_supply_chain_command(**self.args)
  self.assertEqual(json.loads((self.root/'output/report.json').read_bytes())['cases'][0]['requestBytes'],b'original request'.hex())
  self.assertEqual((self.root/'output/replay-0.response').read_bytes(),b'original response')
 def test_changed_closing_source_withholds_report_retains_raw(self):
  def compile(*args,observe):
   observe('replay',0,b'request',b'response');(self.root/'model').write_bytes(b'changed');return {'cases':[],'qualification':'compiler only'}
  with mock.patch.object(owner,'compile_supply_chain_cases',side_effect=compile):
   with self.assertRaises(ValueError):owner.compile_supply_chain_command(**self.args)
  self.assertFalse((self.root/'output/report.json').exists());self.assertEqual((self.root/'output/replay-0.response').read_bytes(),b'response')
 def test_invalid_original_metadata_before_compiler_and_output(self):
  metadata=json.loads((self.root/'metadata').read_bytes());metadata['bindings']['properties'][0]['property_id']='forged';(self.root/'metadata').write_text(json.dumps(metadata))
  with mock.patch.object(owner,'compile_supply_chain_cases')as compiler:
   with self.assertRaises(ValueError):owner.compile_supply_chain_command(**self.args)
   compiler.assert_not_called()
  self.assertFalse((self.root/'output').exists())
 def test_duplicate_giant_integer_and_symlink_refuse(self):
  for raw in (b'{"bindings":{},"bindings":{}}',b'{"x":'+b'9'*60000+b'}'):
   with self.assertRaises(ValueError):owner._metadata(raw)
  (self.root/'link').symlink_to(self.root/'model');self.args['model']=self.root/'link'
  with self.assertRaises(ValueError):owner.compile_supply_chain_command(**self.args)
 def test_cancellation_identity_and_closing_cancel(self):
  cancel=KeyboardInterrupt()
  def fail(*args,observe):(self.root/'model').write_bytes(b'changed');raise cancel
  with mock.patch.object(owner,'compile_supply_chain_cases',side_effect=fail):
   with self.assertRaises(KeyboardInterrupt)as caught:owner.compile_supply_chain_command(**self.args)
  self.assertIs(caught.exception,cancel)
 def test_response_bound_before_retention(self):
  def compile(*args,observe):observe('replay',0,b'r',b'x'*1000001)
  with mock.patch.object(owner,'compile_supply_chain_cases',side_effect=compile):
   with self.assertRaises(ValueError):owner.compile_supply_chain_command(**self.args)
  self.assertFalse((self.root/'output/replay-0.response').exists())
