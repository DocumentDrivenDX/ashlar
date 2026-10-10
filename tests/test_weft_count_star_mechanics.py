"""Meaningful cancellation and availability controls for the additive layout."""
import os,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar import _weft_installation_mechanics as m
import test_weft_installation_mechanics as fixtures

class CountStarControls(unittest.TestCase):
    setUp=fixtures.MechanicsTests.setUp
    fixture=fixtures.MechanicsTests.fixture
    publish=fixtures.MechanicsTests.publish
    script=fixtures.MechanicsTests.script
    transport=fixtures.MechanicsTests.transport
    def test_closed_additive_layout_old_bytes_retained(self):
        self.assertEqual(len(m.installation_layout('paths').schemas),5)
        self.assertEqual(len(m.installation_layout('paths-keys').schemas),6)
        current=m.installation_layout('count-star')
        self.assertEqual(current.executable,'weft-paths-keys')
        self.assertEqual(current.ready_format,'ashlar-weft-count-star-ready/0.1')
        self.assertEqual(len(current.schemas),9)
        installed,config,layout=self.publish('count-star')
        self.assertTrue((config.output/'ready.json').exists())
        self.assertFalse(installed.cleanup_pending)
        for path in config.output.rglob('*'):
            if path.is_file():self.assertEqual(path.stat().st_mode&0o777,0o555 if path.name==layout.executable else 0o444)
    def test_finish_closing_cancel_outranks_error_attempts_remaining(self):
        for kind in (KeyboardInterrupt,SystemExit,GeneratorExit):
            cancellation=kind();visited=[]
            def closing():visited.append('closing');raise cancellation
            with self.assertRaises(kind)as caught:m.finish(ValueError('body'),(closing,lambda:visited.append('remaining')))
            self.assertIs(caught.exception,cancellation);self.assertEqual(visited,['closing','remaining'])
    def test_finish_body_cancel_retains_identity(self):
        primary=KeyboardInterrupt();closing=GeneratorExit()
        def cleanup():raise closing
        with self.assertRaises(KeyboardInterrupt)as caught:m.finish(primary,(cleanup,))
        self.assertIs(caught.exception,primary)
    def test_failed_staging_closing_cancel_remains_unavailable(self):
        cancellation=KeyboardInterrupt();original=m.shutil.rmtree
        def cleanup(path):original(path);raise cancellation
        with patch.object(m.shutil,'rmtree',side_effect=cleanup):
            with self.assertRaises(KeyboardInterrupt)as caught:
                self.publish('count-star',write=lambda *args:(_ for _ in ()).throw(ValueError('body')))
        self.assertIs(caught.exception,cancellation)
        self.assertFalse((self.root/'count-star').exists())
    def test_failed_ready_link_rollback_cancel_remains_unavailable(self):
        cancellation=GeneratorExit();original=m.shutil.rmtree
        def cleanup(path):
            original(path)
            if Path(path)==self.root/'count-star':raise cancellation
        with patch.object(m.os,'link',side_effect=OSError('link')),patch.object(m.shutil,'rmtree',side_effect=cleanup):
            with self.assertRaises(GeneratorExit)as caught:self.publish('count-star')
        self.assertIs(caught.exception,cancellation)
        self.assertFalse((self.root/'count-star').exists())
    def test_transport_cleanup_cancel_outranks_validation_error(self):
        script=self.script("import sys\nsys.stdout.write('{}\\n')\n")
        cancellation=SystemExit();original=m.os.killpg
        def terminate(pid,sig):
            try:original(pid,sig)
            except ProcessLookupError:pass
            raise cancellation
        with patch.object(m.os,'killpg',side_effect=terminate):
            with self.assertRaises(SystemExit)as caught:
                self.transport(script,validate=lambda raw:(_ for _ in ()).throw(ValueError('response')))
        self.assertIs(caught.exception,cancellation)
if __name__=='__main__':unittest.main()
