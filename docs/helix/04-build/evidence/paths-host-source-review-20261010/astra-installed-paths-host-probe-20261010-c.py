import ast,base64,copy,hashlib,json,sys,unittest,zlib
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from test_host_paths_query import LifecycleTests, schemas, module
from ashlar_host.path_schema import make_offline_path_schema_validation
from ashlar_host.path_admission import PathPlanError

class Additional(unittest.TestCase):
 def fixture(self):
  f=LifecycleTests('test_complete_ten_results_persist_only_after_reader_and_stop');f.setUp();self.addCleanup(f.doCleanups);return f
 def test_alias_cancel_preserved_and_stop(self):
  f=self.fixture();primary=KeyboardInterrupt('private-payload')
  @contextmanager
  def bad_interval(context):
   try:yield
   finally:raise SystemExit('close')
  f.provider.interval=bad_interval
  f.spark.sql.side_effect=primary
  with self.assertRaises(KeyboardInterrupt) as caught:f.compose()
  self.assertIs(caught.exception,primary);self.assertTrue(primary.cleanup_failed)
  f.spark.stop.assert_called_once();self.assertFalse((f.config.output/'report.json').exists())
 def test_stop_only_failure_withholds(self):
  f=self.fixture();f.spark.stop.side_effect=OSError('secret')
  with self.assertRaises(module.HostError) as caught:f.compose()
  self.assertEqual(str(caught.exception),'paths-query-refused')
  self.assertFalse((f.config.output/'report.json').exists())
 def test_closing_schema_drift_withholds(self):
  f=self.fixture();calls=[]
  def bundle(*args):
   calls.append(1);value=schemas()
   if len(calls)>1:value[next(iter(value))]+=b' '
   return tuple(value.items())
  with self.assertRaises(module.HostError):f.compose(installed_paths_schema_bundle=bundle)
  self.assertFalse((f.config.output/'report.json').exists());f.spark.stop.assert_called_once()
 def test_final_report_write_cancel_close_failure_identity(self):
  f=self.fixture();primary=KeyboardInterrupt('write-primary');original=Path.open
  class Stream:
   def __enter__(self):return self
   def write(self,data):raise primary
   def __exit__(self,*args):raise OSError('close-replacement')
  def opening(path,*args,**kwargs):
   if path==f.config.output/'report.json' and args==('xb',):return Stream()
   return original(path,*args,**kwargs)
  with patch.object(Path,'open',opening):
   try:f.compose()
   except BaseException as caught:self.assertIs(caught,primary)
   else:self.fail('primary swallowed')
 def test_all_three_real_offline_schema_roots(self):
  path=Path('/Users/erik/Projects/ashlar/tests/test_weft_path_plan.py')
  tree=ast.parse(path.read_text());raw=next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='FIXTURE_B64')
  cases=[json.loads(line) for line in zlib.decompress(base64.b64decode(raw)).splitlines()]
  port=make_offline_path_schema_validation(schemas())
  positive=negative=0
  for case in cases:
   request=case['request'];response=case['response']
   port.validate(port.schemas,'compile-request-v0.4.schema.json',request);positive+=1
   port.validate(port.schemas,'compile-response-v0.4.schema.json',response);positive+=1
   if response['status']=='compiled':
    value=response['logicalPlan'];name='logical-plan-v0.4.schema.json';port.validate(port.schemas,name,value);positive+=1
    changed=copy.deepcopy(value);changed['unknown']=True
    with self.assertRaises(PathPlanError):port.validate(port.schemas,name,changed)
    negative+=1
   changed=copy.deepcopy(response);changed['unknown']=True
   with self.assertRaises(PathPlanError):port.validate(port.schemas,'compile-response-v0.4.schema.json',changed)
   negative+=1
  self.assertEqual((positive,negative),(59,38))
  with self.assertRaises(PathPlanError):port.validate(port.schemas,'https://invalid.invalid/schema',{})
  changed=dict(port.schemas);changed[next(iter(changed))]+=b' '
  with self.assertRaises(PathPlanError):port.validate(changed,'compile-request-v0.4.schema.json',{})

if __name__=='__main__':unittest.main(verbosity=2)
