# SPDX-License-Identifier: MIT OR Apache-2.0
"""Small real file-admission controls; no SDK or Spark runtime loading."""
import hashlib,json,tempfile,unittest
from pathlib import Path
import test_finite_graph_export as fixtures
from ashlar_host.graphframes_export_setup import run_configure_graphframes_export_command

class Tests(unittest.TestCase):
    def fixture(self,directory):
        root=Path(directory).resolve();release,custody=fixtures.Tests().pair()
        release_path=root/'release.json';release_path.write_bytes(release.payload);custody_path=root/'custody.json';custody_path.write_bytes(custody)
        report=root/'export-result.json';report.write_text(json.dumps({'release':{'path':str(release_path),'sha256':release.sha256},'custody':{'path':str(custody_path),'sha256':hashlib.sha256(custody).hexdigest()}}))
        java=root/'java';java.mkdir();jars=[]
        for name in ('io.graphframes_graphframes-spark3_2.12-0.12.3.jar','io.graphframes_graphframes-graphx-spark3_2.12-0.12.3.jar'):
            jar=root/name;jar.write_bytes(b'fixture only');jars.append(str(jar))
        return root,report,hashlib.sha256(report.read_bytes()).hexdigest(),jars,str(java)
    def test_actual_pair_config_preserves_selectors_private_output(self):
        with tempfile.TemporaryDirectory()as directory:
            root,report,digest,jars,java=self.fixture(directory);output=root/'configuration.json'
            result=run_configure_graphframes_export_command(str(report),digest,str(output),jars=jars,java_home=java)
            self.assertEqual(result,{'output':str(output)});self.assertEqual(output.stat().st_mode&0o777,0o600)
            selected=json.loads(output.read_bytes());original=json.loads(report.read_bytes())
            self.assertEqual(set(selected),{'release_path','custody_path','release_sha256','custody_sha256','jars','java_home'})
            self.assertEqual(selected['release_sha256'],original['release']['sha256']);self.assertEqual(selected['custody_sha256'],original['custody']['sha256'])
            self.assertEqual(selected['jars'],jars);self.assertEqual(selected['java_home'],java)
    def test_digest_and_overwrite_refuse_without_output_effects(self):
        with tempfile.TemporaryDirectory()as directory:
            root,report,digest,jars,java=self.fixture(directory);output=root/'configuration.json'
            with self.assertRaisesRegex(ValueError,'report digest differs'):
                run_configure_graphframes_export_command(str(report),'a'*64,str(output),jars=jars,java_home=java)
            self.assertFalse(output.exists());output.write_bytes(b'original')
            with self.assertRaisesRegex(ValueError,'Fresh canonical'):
                run_configure_graphframes_export_command(str(report),digest,str(output),jars=jars,java_home=java)
            self.assertEqual(output.read_bytes(),b'original')
