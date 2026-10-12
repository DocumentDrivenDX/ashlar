"""Basic finite release writer tests with inert Spark ports, no SDK/native run."""
from pathlib import Path
from contextlib import contextmanager
import test_private_graph_custody as custody_fixture
from types import SimpleNamespace
import hashlib,unittest
from ashlar.graph_release import release_columns
from ashlar_host.puppy_release_document import load_graph_release
from ashlar_host.graph_release_delta import materialize_graph_release_delta,materialize_private_graph_release_delta
from ashlar_host.private_graph_custody import load_private_graph_release

ROOT=Path(__file__).resolve().parents[1]
class Frame:
 def __init__(self,spark,rows,schema):
  self.spark=spark;self.rows=list(rows);self.names=[field.split()[0]for field in schema.split(',')];self.schema=SimpleNamespace(fields=[SimpleNamespace(name=name,dataType=SimpleNamespace(typeName=lambda:'string'))for name in self.names])
 @property
 def write(self):return Writer(self)
 def select(self,*names):assert tuple(names)==tuple(self.names);return self
 def limit(self,n):self.spark.limits.append(n);self.limit_rows=n;return self
 def collect(self):return self.rows[:self.limit_rows]
class Writer:
 def __init__(self,frame):self.frame=frame
 def format(self,value):assert value=='delta';return self
 def mode(self,value):assert value=='errorifexists';return self
 def save(self,path):
  spark=self.frame.spark
  if path in spark.tables:raise FileExistsError(path)
  if spark.fail_edge and path.endswith('edges'):raise RuntimeError('inert edge write failed')
  spark.tables[path]=self.frame
class Spark:
 def __init__(self):self.tables={};self.limits=[];self.fail_edge=False;self.drift=False
 def createDataFrame(self,rows,schema):return Frame(self,rows,schema)
 @property
 def read(self):return self
 def format(self,value):assert value=='delta';return self
 def option(self,name,value):assert(name,value)==('versionAsOf',0);return self
 def load(self,path):
  frame=self.tables[path]
  if self.drift and path.endswith('edges'):frame.rows.append(frame.rows[0])
  return frame
 def sql(self,sql):
  path=sql.split('`')[1];role=1 if path.endswith('nodes')else 2
  values={'version':0}if sql.startswith('DESCRIBE HISTORY')else {'id':'00000000-0000-0000-0000-00000000000'+str(role),'location':'file:'+path,'format':'delta'}
  return SimpleNamespace(select=lambda *names:SimpleNamespace(first=lambda:values))
class Policy:
 def __init__(self):self.calls=0
 def admit_original_release(self,release,context):
  self.calls+=1;load_graph_release(release.payload,release.sha256)

class Tests(unittest.TestCase):
 def setUp(self):
  file=sorted((ROOT/'docs/helix/04-build/evidence/puppygraph-releases-20261009').glob('*.graph.json'))[0]
  self.raw=file.read_bytes();self.sha=hashlib.sha256(self.raw).hexdigest();self.policy=Policy();self.spark=Spark()
 def run_writer(self):return materialize_graph_release_delta(self.spark,self.raw,self.sha,node_path='/private/tmp/release-test/nodes',edge_path='/private/tmp/release-test/edges',policy=self.policy,context=None)
 def test_full_original_string_bags_and_metadata(self):
  value=load_graph_release(self.raw,self.sha);receipt=self.run_writer()
  for kind,role in (('node','nodes'),('edge','edges')):
   expected=[tuple(row[name]for name in release_columns(kind))for row in value[role]]
   self.assertEqual(self.spark.tables['/private/tmp/release-test/'+role].rows,expected)
   self.assertIn(len(expected)+1,self.spark.limits);self.assertEqual(receipt['targets'][role]['version'],0)
  self.assertEqual(self.policy.calls,2);self.assertNotEqual(receipt['targets']['nodes']['uuid'],receipt['targets']['edges']['uuid'])
 def test_changed_readback_refuses_and_closes_original_policy(self):
  self.spark.drift=True
  with self.assertRaises(ValueError):self.run_writer()
  self.assertEqual(self.policy.calls,2);self.assertEqual(len(self.spark.tables),2)
 def test_partial_write_preserved_and_overlapping_paths_refuse(self):
  self.spark.fail_edge=True
  with self.assertRaises(RuntimeError):self.run_writer()
  self.assertEqual(self.policy.calls,2);self.assertEqual(set(self.spark.tables),{'/private/tmp/release-test/nodes'})
  with self.assertRaises(ValueError):materialize_graph_release_delta(self.spark,self.raw,self.sha,node_path='/private/tmp/release-test',edge_path='/private/tmp/release-test/edges',policy=self.policy,context=None)


class PrivateTests(unittest.TestCase):
 def setUp(self):
  self.release,self.custody=custody_fixture.Tests().fixture();self.sha=hashlib.sha256(self.custody).hexdigest();self.source=object();self.context=object();self.events=[];self.spark=Spark();self.refuse=False
  test=self
  class ExportPolicy:
   @contextmanager
   def writer(self,context):
    assert context is test.context;test.events.append('writer-open')
    try:yield
    finally:test.events.append('writer-close')
   def admit_original_export(self,release,custody,source,context):
    assert release==test.release and custody==test.custody and source is test.source and context is test.context
    load_private_graph_release(release.payload,release.sha256,custody,test.sha);test.events.append('admit')
    if test.refuse and test.events.count('admit')==2:return False
  self.policy=ExportPolicy()
 def run_writer(self,custody=None):
  return materialize_private_graph_release_delta(self.spark,self.release.payload,self.release.sha256,custody_payload=self.custody if custody is None else custody,trusted_custody_sha256=self.sha,source=self.source,node_path='/private/tmp/private-release-test/nodes',edge_path='/private/tmp/private-release-test/edges',policy=self.policy,context=self.context)
 def test_explicit_private_pair_fullbags_and_closed_writer(self):
  result=self.run_writer();value=load_private_graph_release(self.release.payload,self.release.sha256,self.custody,self.sha)
  for kind,role in (('node','nodes'),('edge','edges')):
   self.assertEqual(self.spark.tables['/private/tmp/private-release-test/'+role].rows,[tuple(row[name]for name in release_columns(kind))for row in value[role]])
  self.assertEqual(result['custody_profile'],'ashlar-private-local-graph-custody/0.1');self.assertEqual(self.events,['writer-open','admit','admit','writer-close'])
 def test_changed_receipt_and_public_entry_never_fall_back(self):
  with self.assertRaises(ValueError):self.run_writer(self.custody+b' ')
  with self.assertRaises(ValueError):materialize_graph_release_delta(self.spark,self.release.payload,self.release.sha256,node_path='/private/tmp/private-release-test/nodes',edge_path='/private/tmp/private-release-test/edges',policy=Policy(),context=None)
  self.assertEqual(self.events,[]);self.assertEqual(self.spark.tables,{})
 def test_closing_export_refusal_withholds_success_and_closes_writer(self):
  self.refuse=True
  with self.assertRaises(PermissionError):self.run_writer()
  self.assertEqual(self.events[-1],'writer-close');self.assertEqual(len(self.spark.tables),2)
