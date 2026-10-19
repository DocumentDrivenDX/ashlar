# SPDX-License-Identifier: MIT OR Apache-2.0
"""Finite pair notebook preparation only; no install, Spark, cloud or execution."""
import base64,hashlib,json,tempfile,unittest
from pathlib import Path
import test_finite_graph_export
from ashlar_host.fabric_notebook import prepare_fabric_notebook
from ashlar_host.finite_graph_custody import PROFILE
from ashlar_host.local_graph_export import LocalGraphExportSelection

class Tests(unittest.TestCase):
    def test_finite_pair_uses_compatible_code_and_original_digest_metadata(self):
        release,custody=test_finite_graph_export.Tests().pair()
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary).resolve();r=root/'release.graph.json';c=root/'custody.json';r.write_bytes(release.payload);c.write_bytes(custody)
            selection=LocalGraphExportSelection(r,c,release.sha256,hashlib.sha256(custody).hexdigest())
            one_lake='abfss://11111111-1111-1111-1111-111111111111@onelake.dfs.fabric.microsoft.com/22222222-2222-2222-2222-222222222222/Tables/'
            output=prepare_fabric_notebook(selection,one_lake+'archaeology_r1_nodes',one_lake+'archaeology_r1_edges',root/'finite.ipynb','Archaeology release')
            notebook=json.loads(output.read_bytes());pin='12f26f03638b4d603d5aaea4444e26cbeb036e92'
            self.assertIn(pin,''.join(notebook['cells'][1]['source']))
            self.assertEqual(notebook['metadata']['ashlar'],{'source_commit':pin,'release_sha256':release.sha256,'custody_sha256':selection.custody_sha256,'custody_profile':PROFILE})
            code=''.join(notebook['cells'][2]['source']);compile(code,'prepared-finite-cell','exec')
            self.assertIn(base64.b64encode(release.payload).decode(),code);self.assertIn(base64.b64encode(custody).decode(),code)
            self.assertIn(release.sha256,code);self.assertIn(selection.custody_sha256,code)
            self.assertIn("prefix='ashlar-graph-'",code)
            self.assertTrue(''.join(notebook['cells'][0]['source']).startswith('# Graph export'))
