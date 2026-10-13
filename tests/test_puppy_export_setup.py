"""Dependency-free setup against an original retained immutable release pair."""
import hashlib,json,stat,tarfile,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar_host import puppy_export_setup as setup
from ashlar_host.puppy_export_cli import PROFILE,GREMLIN_PROFILE,parse_puppy_export_configuration

class Tests(unittest.TestCase):
    def inputs(self,root,gremlin=False):
        archive=Path(__file__).resolve().parents[1]/'docs/helix/04-build/evidence/truss15-parallel-export-20261013/actual.tar.gz'
        with tarfile.open(archive)as original:
            raw=original.extractfile('export.json-release.json').read();custody=original.extractfile('export.json-custody.json').read()
        release=root/'release.json';release.write_bytes(raw);paired=root/'custody.json';paired.write_bytes(custody)
        report=root/'report.json';report.write_text(json.dumps({'release':{'path':str(release),'sha256':hashlib.sha256(raw).hexdigest()},'custody':{'path':str(paired),'sha256':hashlib.sha256(custody).hexdigest()}}))
        value={'profile':GREMLIN_PROFILE if gremlin else PROFILE,'output':str(root/'runtime-output'),'docker':'/usr/bin/true','docker_host':'unix:///private/tmp/inert.sock','image':'sha256:'+'a'*64,'name':'ashlar-setup-test','username':'private-user','password':'private-password','http_port':25480,'bolt_port':25481,'deadline_seconds':120}
        if gremlin:value.update(language='Gremlin',gremlin_port=25482)
        operator=root/'operator.json';operator.write_text(json.dumps(value));operator.chmod(0o600)
        return report,hashlib.sha256(report.read_bytes()).hexdigest(),operator,release,paired

    def test_original_pair_and_both_profiles_generate_private_runnable_selection(self):
        for gremlin in (False,True):
            with self.subTest(gremlin=gremlin),tempfile.TemporaryDirectory()as directory:
                root=Path(directory).resolve();report,digest,operator,release,custody=self.inputs(root,gremlin);destination=root/'configuration.json'
                answer=setup.run_configure_puppy_export_command(str(report),digest,str(operator),str(destination))
                self.assertEqual(answer,{'output':str(destination)});self.assertEqual(stat.S_IMODE(destination.stat().st_mode),0o600)
                selection,runtime,_=parse_puppy_export_configuration(destination.read_bytes());self.assertEqual(selection.release_path,release);self.assertEqual(selection.custody_path,custody)
                original=json.loads(report.read_bytes());self.assertEqual(selection.release_sha256,original['release']['sha256']);self.assertEqual(selection.custody_sha256,original['custody']['sha256'])
                generated=json.loads(destination.read_bytes());self.assertEqual({k:v for k,v in generated.items()if k not in ('release_path','custody_path','release_sha256','custody_sha256')},json.loads(operator.read_bytes()))
                self.assertFalse(runtime.exists())

    def test_bad_report_mixed_pair_profile_and_closing_drift_leave_no_output(self):
        for failure in ('digest','mixed','profile','drift','overlap','public-credentials'):
            with self.subTest(failure=failure),tempfile.TemporaryDirectory()as directory:
                root=Path(directory).resolve();report,digest,operator,_,custody=self.inputs(root);destination=root/'configuration.json'
                if failure=='digest':digest='0'*64
                if failure=='mixed':
                    custody.write_bytes(b'{}');value=json.loads(report.read_bytes());value['custody']['sha256']=hashlib.sha256(b'{}').hexdigest();report.write_text(json.dumps(value));digest=hashlib.sha256(report.read_bytes()).hexdigest()
                if failure=='profile':
                    value=json.loads(operator.read_bytes());value['language']='Gremlin';operator.write_text(json.dumps(value))
                if failure=='overlap':destination=root/'runtime-output'/'configuration.json'
                if failure=='public-credentials':operator.chmod(0o644)
                original=setup.GraphExportReportFile.renew
                def renew(owner):
                    original(owner)
                    if failure=='drift':operator.write_bytes(operator.read_bytes()+b' ')
                with patch.object(setup.GraphExportReportFile,'renew',renew):
                    with self.assertRaises(Exception):setup.run_configure_puppy_export_command(str(report),digest,str(operator),str(destination))
                self.assertFalse(destination.exists());self.assertFalse((root/'runtime-output').exists())
