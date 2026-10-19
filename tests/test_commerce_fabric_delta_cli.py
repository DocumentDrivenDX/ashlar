# SPDX-License-Identifier: MIT OR Apache-2.0
"""Installed command composition: inert native boundaries, actual ordinary factories."""
from contextlib import contextmanager,ExitStack
from pathlib import Path
from unittest.mock import patch
import json,stat,unittest
import test_commerce_export_cli as fixture
from ashlar_host import commerce_fabric_delta_cli as cli

class Tests(unittest.TestCase):
 spark=fixture.Tests.spark
 reader=fixture.Tests.reader
 def setUp(self):
  fixture.Tests.setUp(self);original=json.loads(self.config.read_bytes())
  self.config.write_text(json.dumps({'profile':'ashlar-original-commerce-export-configuration/0.1','publication':str(self.publication),'source':original['source']}))
  self.nodes=self.root/'nodes';self.edges=self.root/'edges'
 def ports(self):
  stack=ExitStack();stack.enter_context(patch.object(cli,'commerce_fabric_runtime',return_value=[]));stack.enter_context(patch.object(cli,'open_commerce_export_spark',self.spark));stack.enter_context(patch('ashlar_host.commerce_export_cli.open_commerce_reader',self.reader));stack.enter_context(patch.object(cli,'materialize_commerce_fabric_delta',self.materialize));return stack
 def materialize(self,spark,opened,*,node_path,edge_path):
  self.events.append('materialize');Path(node_path).mkdir();Path(edge_path).mkdir()
  return {'release_sha256':'a'*64,'targets':{'nodes':{},'edges':{}}}
 def run_command(self):return cli.run_materialize_commerce_fabric_command(self.config,self.jars,self.nodes,self.edges,self.output)
 # Deliberately select only this class's composition tests; export tests run separately.
 def test_report_after_all_cleanup(self):
  with self.ports():answer=self.run_command()
  self.assertEqual(json.loads(self.output.read_bytes())['release_sha256'],'a'*64)
  self.assertEqual(stat.S_IMODE(self.output.stat().st_mode),0o600)
  self.assertEqual(answer['report'],str(self.output));self.assertLess(self.events.index('reader-close'),self.events.index('spark-close'))
 def test_close_refusal_preserves_partial_tables_without_report(self):
  self.fail_close=True
  with self.ports(),self.assertRaises(LookupError):self.run_command()
  self.assertTrue(self.nodes.is_dir());self.assertTrue(self.edges.is_dir());self.assertFalse(self.output.exists())
 def test_configuration_close_refusal_withholds_report(self):
  materialize=self.materialize
  def changed(*args,**kwargs):
   result=materialize(*args,**kwargs);self.config.write_bytes(b'{}');return result
  with self.ports(),patch.object(cli,'materialize_commerce_fabric_delta',changed),self.assertRaises(Exception):self.run_command()
  self.assertFalse(self.output.exists());self.assertIn('spark-close',self.events)
 def test_overlap_refuses_before_runtime(self):
  self.nodes=self.publication/'nodes'
  with patch.object(cli,'commerce_fabric_runtime')as runtime,self.assertRaises(ValueError):self.run_command()
  runtime.assert_not_called();self.assertFalse(self.output.exists())
 def test_cancellation_preserves_primary_and_partial_tables(self):
  materialize=self.materialize;self.fail_close=True
  def cancelled(*args,**kwargs):
   materialize(*args,**kwargs);raise KeyboardInterrupt('inert native cancellation')
  with self.ports(),patch.object(cli,'materialize_commerce_fabric_delta',cancelled),self.assertRaises(KeyboardInterrupt):self.run_command()
  self.assertTrue(self.nodes.is_dir());self.assertFalse(self.output.exists());self.assertIn('reader-close',self.events)
 def test_onelake_destinations_pass_unchanged(self):
  self.nodes='abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/release_nodes'
  self.edges='abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/release_edges'
  def materialize(spark,opened,*,node_path,edge_path):
   self.assertEqual(node_path,self.nodes);self.assertEqual(edge_path,self.edges)
   return {'release_sha256':'a'*64}
  with self.ports(),patch.object(cli,'materialize_commerce_fabric_delta',materialize):self.run_command()
  self.assertTrue(self.output.exists())
 def test_complete_large_passive_metadata_is_preserved(self):
  materialize=self.materialize;opaque='x'*70000
  def large(*args,**kwargs):
   return {**materialize(*args,**kwargs),'opaque_metadata':opaque}
  with self.ports(),patch.object(cli,'materialize_commerce_fabric_delta',large):self.run_command()
  self.assertGreater(self.output.stat().st_size,65536)
  self.assertEqual(json.loads(self.output.read_bytes())['opaque_metadata'],opaque)
  self.assertIn('reader-close',self.events);self.assertIn('spark-close',self.events)
 def test_report_above_one_mib_refuses_without_truncation_or_cleanup_of_tables(self):
  materialize=self.materialize
  def oversized(*args,**kwargs):
   return {**materialize(*args,**kwargs),'opaque_metadata':'x'*1048576}
  with self.ports(),patch.object(cli,'materialize_commerce_fabric_delta',oversized),self.assertRaises(ValueError):self.run_command()
  self.assertFalse(self.output.exists());self.assertTrue(self.nodes.is_dir());self.assertTrue(self.edges.is_dir())
  self.assertIn('reader-close',self.events);self.assertIn('spark-close',self.events)
 def test_dispatch(self):
  import sys
  from ashlar.cli import main
  args=['ashlar','materialize-commerce-fabric','--configuration',str(self.config),'--jars',str(self.jars),'--nodes',str(self.nodes),'--edges',str(self.edges),'--output',str(self.output)]
  with patch.object(sys,'argv',args),patch.object(cli,'run_materialize_commerce_fabric_command',return_value={'report':'inert'})as run:main()
  run.assert_called_once_with(str(self.config),str(self.jars),str(self.nodes),str(self.edges),str(self.output))

