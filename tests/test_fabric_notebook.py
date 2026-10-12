"""Small installed-command composition tests; no Spark or cloud execution."""
import base64,contextlib,hashlib,io,json,stat,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar import cli
from ashlar_host.fabric_notebook import prepare_fabric_notebook,SOURCE_COMMIT
from ashlar_host.local_graph_export import LocalGraphExportSelection
import test_private_graph_custody as fixture
ROOT='abfss://11111111-1111-1111-1111-111111111111@onelake.dfs.fabric.microsoft.com/22222222-2222-2222-2222-222222222222/Tables/'
class Tests(unittest.TestCase):
 def pair(self,root):
  release,custody=fixture.Tests().fixture();r=root/'release.json';c=root/'custody.json';r.write_bytes(release.payload);c.write_bytes(custody)
  return LocalGraphExportSelection(r,c,release.sha256,hashlib.sha256(custody).hexdigest())
 def test_public_command_complete_private_notebook_preserves_selected_bytes(self):
  with tempfile.TemporaryDirectory()as tmp:
   root=Path(tmp).resolve();selection=self.pair(root);output=root/'fabric.ipynb'
   argv=['ashlar','prepare-fabric-notebook','--release',str(selection.release_path),'--custody',str(selection.custody_path),'--release-sha256',selection.release_sha256,'--custody-sha256',selection.custody_sha256,'--nodes',ROOT+'release_nodes','--edges',ROOT+'release_edges','--output',str(output),'--display-name','Example release']
   with patch('sys.argv',argv),contextlib.redirect_stdout(io.StringIO())as stdout:cli.main()
   self.assertEqual(json.loads(stdout.getvalue())['notebook'],str(output));self.assertEqual(stat.S_IMODE(output.stat().st_mode),0o600)
   notebook=json.loads(output.read_bytes());self.assertEqual(notebook['nbformat'],4)
   self.assertIn(SOURCE_COMMIT,''.join(notebook['cells'][1]['source']))
   code=''.join(notebook['cells'][2]['source']);compile(code,'generated-cell','exec')
   self.assertIn(selection.release_sha256,code);self.assertIn(selection.custody_sha256,code)
   self.assertIn(base64.b64encode(selection.release_path.read_bytes()).decode(),code)
   self.assertIn(base64.b64encode(selection.custody_path.read_bytes()).decode(),code)
   self.assertIn('materialize_private_graph_release_delta',code);self.assertFalse(list(root.glob('.ashlar-notebook-*')))
   original=output.read_bytes()
   with self.assertRaises(FileExistsError):prepare_fabric_notebook(selection,ROOT+'release_nodes',ROOT+'release_edges',output,'Example release')
   self.assertEqual(output.read_bytes(),original)
 def test_bad_mapping_or_selected_hash_withholds_notebook(self):
  with tempfile.TemporaryDirectory()as tmp:
   root=Path(tmp).resolve();selection=self.pair(root);output=root/'fabric.ipynb'
   with self.assertRaises(ValueError):prepare_fabric_notebook(selection,'not-OneLake',ROOT+'release_edges',output,'Example')
   self.assertFalse(output.exists())
   bad=LocalGraphExportSelection(selection.release_path,selection.custody_path,'0'*64,selection.custody_sha256)
   with self.assertRaises(ValueError):prepare_fabric_notebook(bad,ROOT+'release_nodes',ROOT+'release_edges',output,'Example')
   self.assertFalse(output.exists())
