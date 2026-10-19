# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fixed worker composition controls; native/producer ports are injected."""
import hashlib,json,os,tempfile,unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import ashlar_host.puppy_scenario_worker as worker
import test_puppy_scenario_configuration as config_fixture

class Tests(unittest.TestCase):
 def test_outer_closing_before_report_and_primary_failure_withheld(self):
  for fail in (False,True):
   with self.subTest(fail=fail),tempfile.TemporaryDirectory()as directory:
    root=Path(directory).resolve();root.chmod(0o700);v=config_fixture.Tests().value();v['output']=str(root/'output')
    for pack in v['packs']:
     for key in ('model','graph','pack'):
      p=root/(pack['name']+'-'+key);p.write_bytes(b'original');pack[key]=str(p)
    raw=json.dumps(v).encode();config=root/'configuration.json';config.write_bytes(raw);config.chmod(0o600);events=[]
    @contextmanager
    def export(selection):
     try:yield SimpleNamespace()
     finally:events.append('export-close')
    @contextmanager
    def source(*args,**kwargs):
     try:yield SimpleNamespace(source=object(),pack_bytes=b'original')
     finally:
      events.append('source-close')
      if fail:raise RuntimeError('outer closing failed')
    def execute(*args,**kwargs):
     kwargs['creation_observer'](worker.PuppyScenarioContainerReceipt('d'*64,v['runtime']['image'],v['runtime']['name'],kwargs['invocation_nonce'],root/'output/prepared/scenarios.duckdb','/tmp/scenarios.duckdb',v['runtime']['http_port'],v['runtime']['bolt_port'],None,2,3*1024**3))
     events.append('native-close');return {'profile':'native-result','native_model_blobs':{'sha':b'original'}}
    read,write=os.pipe();os.set_blocking(write,False)
    try:
     with patch.object(worker,'open_local_graph_export',side_effect=export),patch.object(worker,'open_puppy_scenario_source',side_effect=source),patch.object(worker,'prepare_puppy_scenarios',return_value={}),patch.object(worker,'execute_owned_puppy_scenarios',side_effect=execute),patch.object(worker,'PuppyFiniteLoopbackConfig',return_value=object()):
      args=dict(control_fd=write,invocation_nonce='a'*32,configuration_sha256=hashlib.sha256(raw).hexdigest())
      if fail:
       with self.assertRaises(RuntimeError):worker.run_puppy_scenario_worker(config,**args)
       self.assertFalse((root/'output/result.json').exists())
      else:
       result=worker.run_puppy_scenario_worker(config,**args);self.assertTrue(Path(result['output']).exists())
       self.assertEqual(events,['native-close','source-close','export-close','source-close','export-close'])
       report=json.loads(Path(result['output']).read_bytes());self.assertTrue(report['all_outer_closing_completed']);self.assertEqual(report['result']['native_model_blobs']['sha']['carrier'],'original-bytes-base64')
       wire=json.loads(os.read(read,4096));self.assertEqual(wire['type'],'created');self.assertEqual(wire['receipt']['invocation_nonce'],'a'*32);self.assertNotIn('private-password',json.dumps(wire))
    finally:os.close(read);os.close(write)
 def test_report_codec_preserves_raw_bytes_and_refuses_float_or_unknown_integer(self):
  class Unknown(int):pass
  value=json.loads(worker.scenario_report_bytes({'original':b'\0raw','integer':9007199254740993}))
  self.assertEqual(value['integer'],9007199254740993);self.assertEqual(value['original']['bytes_base64'],'AHJhdw==')
  for value in (1.25,Unknown(1)):
   with self.assertRaises(ValueError):worker.scenario_report_bytes({'value':value})
