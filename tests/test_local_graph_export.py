"""Basic real local-file export-use tests; Spark ports inert, no live authority."""
from pathlib import Path
import hashlib,tempfile,unittest
import test_private_graph_custody as fixture
import test_graph_release_delta as delta_fixture
from ashlar_host.local_graph_export import LocalGraphExportSelection,open_local_graph_export
from ashlar_host.graph_release_delta import materialize_private_graph_release_delta

class Tests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name).resolve()
  self.release,self.custody=fixture.Tests().fixture();self.release_path=self.root/'release.graph.json';self.custody_path=self.root/'custody.json';self.release_path.write_bytes(self.release.payload);self.custody_path.write_bytes(self.custody)
  self.selection=LocalGraphExportSelection(self.release_path,self.custody_path,self.release.sha256,hashlib.sha256(self.custody).hexdigest())
 def test_real_owned_pair_to_private_materializer_without_live_pg(self):
  spark=delta_fixture.Spark()
  with open_local_graph_export(self.selection)as session:
   with self.assertRaises(PermissionError):session.policy.admit_original_export(session.release,session.custody_bytes,session.source,session.context)
   result=materialize_private_graph_release_delta(spark,session.release.payload,session.release.sha256,custody_payload=session.custody_bytes,trusted_custody_sha256=session.custody_sha256,source=session.source,node_path='/private/tmp/local-use/nodes',edge_path='/private/tmp/local-use/edges',policy=session.policy,context=session.context)
   self.assertEqual(result['custody_profile'],'ashlar-private-local-graph-custody/0.1');self.assertEqual(len(spark.tables),2)
  with self.assertRaises(PermissionError):session.renew()
 def test_changed_selected_file_refuses_closing(self):
  with self.assertRaises(PermissionError):
   with open_local_graph_export(self.selection):self.custody_path.write_bytes(self.custody+b' ')
 def test_current_read_permission_and_context_required(self):
  with open_local_graph_export(self.selection)as session:
   with self.assertRaises(PermissionError):
    with session.writer(object()):self.fail('wrong context yielded')
   with session.writer(session.context):
    with self.assertRaises(PermissionError):
     with session.writer(session.context):self.fail('nested yielded')
  with self.assertRaises((PermissionError,OSError)):
   with open_local_graph_export(self.selection):self.release_path.chmod(0)
