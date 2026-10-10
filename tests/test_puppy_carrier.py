"""Inert carrier controls; no DuckDB/Puppy/native execution qualification."""
import copy,hashlib,json,sys,tempfile,types,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar.graph_release import release_columns
from ashlar_host.puppy_carrier import prepare_carrier
from ashlar_host.puppy_release_document import load_graph_release
from ashlar_host.puppy_native import original_model
from ashlar_host.puppy_carrier_cli import run_puppy_preparation,PROFILE

ROOT=Path(__file__).resolve().parents[1]
FILES=sorted((ROOT/'docs/helix/04-build/evidence/puppygraph-releases-20261009').glob('*.graph.json'))
def encoded(v):return (json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False)+'\n').encode()
class Policy:
 def __init__(self):self.calls=0
 def admit_original_release(self,release,context):self.calls+=1;load_graph_release(release.payload,release.sha256)
class Connection:
 def __init__(self,path):self.path=Path(path);self.rows={};self.actions=[];self.selected=None;self.closed=False
 def execute(self,sql):
  self.actions.append(sql)
  if sql.startswith('SELECT '):self.selected='nodes'if'carrier.nodes'in sql else'edges'
  return self
 def executemany(self,sql,rows):self.rows['nodes'if'carrier.nodes'in sql else'edges']=copy.deepcopy(rows)
 def fetchall(self):return [r[:-2] for r in self.rows.get(self.selected,[])]
 def close(self):self.closed=True;self.path.write_bytes(b'inert fixture database')
class PuppyCarrierTests(unittest.TestCase):
 def setup(self):
  raw=FILES[0].read_bytes();return raw,hashlib.sha256(raw).hexdigest()
 def sdk(self,connections):
  def connect(path):c=Connection(path);connections.append(c);return c
  return types.SimpleNamespace(connect=connect,__version__='inert-control')
 def test_complete_original_r1_r2_values_and_model_endpoints(self):
  for source in FILES:
   raw=source.read_bytes();sha=hashlib.sha256(raw).hexdigest();value=load_graph_release(raw,sha);connections=[];policy=Policy()
   with tempfile.TemporaryDirectory()as temp,patch.dict(sys.modules,duckdb=self.sdk(connections)):
    out=Path(temp).resolve()/'carrier';receipt=prepare_carrier(raw,sha,out,policy=policy,context=None)
    model=json.loads((out/'model.json').read_bytes());self.assertEqual(model['catalog'][0]['name'],'ashlar_'+sha[:24]);self.assertEqual(len(model['catalog'][0]['name']),31)
    for kind,role in [('node','nodes'),('edge','edges')]:
     expected=[[r[n]for n in release_columns(kind)]+[r['graph_id'],r['id']]for r in value[role]]
     self.assertEqual(connections[0].rows[role],expected)
    self.assertEqual(model['edge'][0]['fromKey'],[{'name':'src','type':'STRING'}]);self.assertEqual(model['edge'][0]['toKey'],[{'name':'dst','type':'STRING'}]);self.assertTrue(connections[0].closed);self.assertIn('COMMIT',connections[0].actions)
    self.assertEqual(receipt['release_sha256'],sha);self.assertFalse((out/'receipt.json').exists());self.assertGreaterEqual(policy.calls,4)
 def test_duplicate_and_endpoint_refuse_before_destination(self):
  raw,sha=self.setup();v=json.loads(raw);cases=[raw.replace(b'"format":',b'"format":"duplicate","format":',1)]
  v['edges'][0]['src']='unknown';cases.append(encoded(v))
  for body in cases:
   with tempfile.TemporaryDirectory()as temp,patch.dict(sys.modules,duckdb=self.sdk([])):
    out=Path(temp).resolve()/'carrier'
    with self.assertRaises(ValueError):prepare_carrier(body,hashlib.sha256(body).hexdigest(),out,policy=Policy(),context=None)
    self.assertFalse(out.exists())
 def test_required_authority_and_existing_private_destination(self):
  raw,sha=self.setup()
  with tempfile.TemporaryDirectory()as temp,patch.dict(sys.modules,duckdb=self.sdk([])):
   out=Path(temp).resolve()/'carrier'
   with self.assertRaises(ValueError):prepare_carrier(raw,sha,out,policy=None,context=None)
   out.mkdir()
   with self.assertRaises(FileExistsError):prepare_carrier(raw,sha,out,policy=Policy(),context=None)
 def test_prefix_collision_preserves_original_full_model_refusal(self):
  sha='a'*64;prior={'catalog':[{'name':'ashlar_'+sha[:24],'jdbc':{'jdbcUri':'original'}}],'node':[],'edge':[]};changed=copy.deepcopy(prior);changed['catalog'][0]['jdbc']['jdbcUri']='different full release'
  with self.assertRaises(ValueError):original_model(prior,changed)
  self.assertEqual(original_model(prior,prior),prior)
 def test_policy_failure_rolls_back_and_closes_preserving_cancellation(self):
  raw,sha=self.setup()
  for primary in (ValueError('current authority changed'),KeyboardInterrupt(),SystemExit(),GeneratorExit()):
   class Refuse(Policy):
    def admit_original_release(self,release,context):
     super().admit_original_release(release,context)
     if self.calls==2:raise primary
   connections=[]
   with tempfile.TemporaryDirectory()as temp,patch.dict(sys.modules,duckdb=self.sdk(connections)):
    out=Path(temp).resolve()/'carrier'
    with self.assertRaises(type(primary))as caught:prepare_carrier(raw,sha,out,policy=Refuse(),context=None)
    self.assertIs(caught.exception,primary);self.assertIn('ROLLBACK',connections[0].actions);self.assertTrue(connections[0].closed);self.assertFalse((out/'receipt.json').exists())
 def test_initial_policy_refusal_precedes_sdk_import(self):
  import builtins
  raw,sha=self.setup();original=builtins.__import__;failure=ValueError('original authority denied');seen=[]
  def guarded(name,*args,**kwargs):
   if name=='duckdb':seen.append(name);raise AssertionError('SDK imported before admission')
   return original(name,*args,**kwargs)
  class Refuse:
   def admit_original_release(self,release,context):raise failure
  with tempfile.TemporaryDirectory()as temp,patch('builtins.__import__',side_effect=guarded):
   with self.assertRaises(ValueError)as caught:prepare_carrier(raw,sha,Path(temp).resolve()/'carrier',policy=Refuse(),context=None)
   self.assertIs(caught.exception,failure);self.assertEqual(seen,[])
 def test_explicit_cli_success_and_borrowed_config_is_readonly(self):
  raw,sha=self.setup();connections=[]
  with tempfile.TemporaryDirectory()as temp,patch.dict(sys.modules,duckdb=self.sdk(connections)):
   base=Path(temp).resolve();source=base/'release.json';source.write_bytes(raw);provider=base/'provider.py'
   provider.write_text('from contextlib import contextmanager\nfrom ashlar_host.puppy_carrier_cli import PuppyPreparationSession\nclass Policy:\n def admit_original_release(self,release,context):pass\n@contextmanager\ndef open_puppy_preparation(invocation,release):\n try:invocation["release_sha256"]="forged"\n except TypeError:pass\n else:raise AssertionError("borrowed mutable configuration")\n yield PuppyPreparationSession(Policy(),None)\n')
   out=base/'carrier';config=base/'config.json';config.write_bytes(encoded({'profile':PROFILE,'release_path':str(source),'release_sha256':sha,'output_directory':str(out)}))
   result=run_puppy_preparation(config,provider,hashlib.sha256(provider.read_bytes()).hexdigest())
   self.assertEqual(result['release_sha256'],sha);self.assertEqual(json.loads((out/'receipt.json').read_bytes()),result)
 def test_installed_provider_cleanup_failure_withholds_receipt(self):
  raw,sha=self.setup();connections=[]
  with tempfile.TemporaryDirectory()as temp,patch.dict(sys.modules,duckdb=self.sdk(connections)):
   base=Path(temp).resolve();source=base/'release.json';source.write_bytes(raw);provider=base/'provider.py'
   provider.write_text('from contextlib import contextmanager\nfrom ashlar_host.puppy_carrier_cli import PuppyPreparationSession\nclass Policy:\n def admit_original_release(self,release,context):pass\n@contextmanager\ndef open_puppy_preparation(invocation,release):\n try:yield PuppyPreparationSession(Policy(),None)\n finally:raise ValueError("closing policy failure")\n')
   out=base/'carrier';config=base/'config.json';config.write_bytes(encoded({'profile':PROFILE,'release_path':str(source),'release_sha256':sha,'output_directory':str(out)}))
   with self.assertRaisesRegex(ValueError,'closing policy failure'):run_puppy_preparation(config,provider,hashlib.sha256(provider.read_bytes()).hexdigest())
   self.assertFalse((out/'receipt.json').exists());self.assertTrue(connections[0].closed)
if __name__=='__main__':unittest.main()
