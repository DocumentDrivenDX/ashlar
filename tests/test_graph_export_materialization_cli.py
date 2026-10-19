# SPDX-License-Identifier: MIT OR Apache-2.0
"""Basic CLI composition: actual temporary export files, inert Spark, no SDK/PG."""
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import patch
import hashlib,json,tempfile,unittest
import test_private_graph_custody as fixture
import test_graph_release_delta as delta_fixture
from ashlar_host import graph_export_materialization_cli as cli

class Tests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name).resolve();release,custody=fixture.Tests().fixture();self.release=self.root/'release.json';self.custody=self.root/'custody.json';self.release.write_bytes(release.payload);self.custody.write_bytes(custody);self.sha=release.sha256;self.csha=hashlib.sha256(custody).hexdigest();self.jars=self.root/'jars';self.jars.mkdir();self.output=self.root/'report.json';self.nodes=self.root/'nodes';self.edges=self.root/'edges';self.events=[];self.fail_close=False
 @contextmanager
 def spark(self,*args):
  self.events.append('spark-open')
  try:yield delta_fixture.Spark()
  finally:
   self.events.append('spark-close');self.assertFalse(self.output.exists())
   if self.fail_close:raise RuntimeError('inert Spark close refusal')
 def run_command(self):return cli.run_materialize_graph_export_command(self.release,self.custody,self.sha,self.csha,self.nodes,self.edges,self.jars,self.output)
 def test_actual_local_owner_and_private_materializer_report_after_cleanup(self):
  with patch.object(cli,'finite_runtime',return_value=[]),patch.object(cli,'open_commerce_export_spark',self.spark):result=self.run_command()
  report=json.loads(self.output.read_bytes());self.assertEqual(report['export_use_profile'],'ashlar-local-development-private-export-use/0.1');self.assertEqual(report['targets']['nodes']['version'],0)
  self.assertEqual(result['report'],str(self.output));self.assertEqual(self.events,['spark-open','spark-close'])
 def test_spark_cleanup_failure_withholds_report(self):
  self.fail_close=True
  with patch.object(cli,'finite_runtime',return_value=[]),patch.object(cli,'open_commerce_export_spark',self.spark),self.assertRaises(RuntimeError):self.run_command()
  self.assertFalse(self.output.exists());self.assertEqual(self.events[-1],'spark-close')
 def test_overlapping_inputs_or_wrong_digest_refuse_before_spark(self):
  with patch.object(cli,'finite_runtime',return_value=[]),patch.object(cli,'open_commerce_export_spark')as spark:
   with self.assertRaises(ValueError):cli.run_materialize_graph_export_command(self.release,self.custody,self.sha,self.csha,self.root,self.edges,self.jars,self.output)
   self.sha='0'*64
   with self.assertRaises(ValueError):self.run_command()
   spark.assert_not_called();self.assertFalse(self.output.exists())
