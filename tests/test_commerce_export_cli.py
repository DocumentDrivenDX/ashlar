"""Basic concrete CLI composition; public Spark/reader ports inert, no DB/SDK."""
from pathlib import Path
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
import hashlib,json,tempfile,unittest
import test_private_graph_custody as fixture
from ashlar_host import commerce_export_cli as cli
from ashlar_host.ordinary_query_connection import OrdinaryQueryConnectionFactory
from ashlar_host.commerce_query_sources import CommerceQuerySourceFactory

class Tests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name).resolve();self.publication=self.root/'publication';self.publication.mkdir();self.output=self.root/'export';self.jars=self.root/'jars';self.jars.mkdir();self.config=self.root/'publish.json';self.events=[];self.fail_close=False
  scope={'service_schema':'ashlar_ack_test','scope_id':'00000000-0000-0000-0000-000000000001','installation_id':'00000000-0000-0000-0000-000000000002','consumer':'consumer','feed':'private-original-commerce-fixture','epoch':'original-commerce-v1'}
  value={'profile':'ashlar-original-commerce-local-publication/0.1','output':str(self.publication),'model':str(self.root/'model.json'),'graph':str(self.root/'graph.json'),'jars':str(self.jars),'producer':{'source':str(self.root/'umf'),'bun':str(self.root/'bun'),'git':str(self.root/'git'),'timeout_seconds':30,'maximum_output_bytes':1024,'maximum_receipt_bytes':4194304},'runtime':{'python':str(self.root/'python'),'python_sha256':'a'*64,'java_home':str(self.root/'java'),'java_sha256':'b'*64,'jvm_config_sha256':'c'*64},'installation':str(self.root/'installation.json'),'installation_sha256':'d'*64,'source':{'host':'127.0.0.1','port':25435,'database':'truss_e2e','authentication':'isolated-loopback-trust','scope':scope,'source_schema':'original_source','source_signature_sha256':'e'*64,'writer_role':'original_writer','reader_role':'original_reader','ack_role':'original_ack'}}
  self.config.write_text(json.dumps(value));self.release,self.custody=fixture.Tests().fixture()
  self.native=self.root/'native';(self.native/'_delta_log').mkdir(parents=True)
 @contextmanager
 def spark(self,*args):
  self.events.append('spark-open')
  def sql(statement):self.events.append('alias');return SimpleNamespace(collect=lambda:[])
  try:yield SimpleNamespace(sql=sql)
  finally:
   self.events.append('spark-close');self.assertFalse(self.output.exists())
   if self.fail_close:raise LookupError('inert Spark close failure')
 @contextmanager
 def reader(self,spark,publication,*,ordinary_session,source_factory):
  self.assertIs(type(ordinary_session),OrdinaryQueryConnectionFactory);self.assertIs(type(source_factory),CommerceQuerySourceFactory);self.assertEqual(publication,self.publication);self.events.append('reader-open')
  @contextmanager
  def interval(context):
   self.events.append('interval-open')
   try:yield
   finally:self.events.append('interval-close')
  try:yield SimpleNamespace(context=object(),provider=SimpleNamespace(interval=interval),aliases={'local.c.object_current':'spark_catalog.c.object_current'},driver=SimpleNamespace(transport=SimpleNamespace(targets={'local.c.object_current':SimpleNamespace(path=self.native)})))
  finally:self.events.append('reader-close')
 def export(self,factory):
  with factory():self.events.append('export-read')
  return self.release,self.custody,{}
 def ports(self):
  from contextlib import ExitStack
  stack=ExitStack();stack.enter_context(patch.object(cli,'runtime_paths',return_value=[]));stack.enter_context(patch.object(cli,'open_commerce_export_spark',self.spark));stack.enter_context(patch.object(cli,'open_commerce_reader',self.reader));stack.enter_context(patch.object(cli,'read_commerce_export',self.export));return stack
 def run_command(self):return cli.run_export_original_commerce_command(self.config,self.publication,self.output,self.jars)
 def test_real_config_and_factories_pair_only_after_cleanup(self):
  with self.ports():result=self.run_command()
  self.assertEqual((self.output/'release.graph.json').read_bytes(),self.release.payload);self.assertEqual((self.output/'custody.json').read_bytes(),self.custody)
  self.assertEqual(result['custody_sha256'],hashlib.sha256(self.custody).hexdigest());self.assertLess(self.events.index('reader-close'),self.events.index('spark-close'));self.assertIn('interval-close',self.events)
 def test_spark_close_refusal_withholds_output(self):
  self.fail_close=True
  with self.ports(),self.assertRaises(LookupError):self.run_command()
  self.assertFalse(self.output.exists());self.assertIn('reader-close',self.events)
 def test_output_overlap_refuses_before_runtime(self):
  with patch.object(cli,'runtime_paths')as runtime,self.assertRaises(ValueError):cli.run_export_original_commerce_command(self.config,self.publication,self.publication/'export',self.jars)
  runtime.assert_not_called();self.assertFalse((self.publication/'export').exists())
