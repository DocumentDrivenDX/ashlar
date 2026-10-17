"""Original local config and filesystem controls; producer transport is inert."""
import hashlib,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar_host.config import ProducerConfig
from ashlar_host.configured_source_admission import ConfiguredSourceAdmissionConfig,validate_configured_source
ROOT=Path(__file__).resolve().parents[1]

class Tests(unittest.TestCase):
    def fixture(self,root):
        original=ROOT/'examples/end-to-end/configured-source.json';document=json.loads(original.read_bytes())
        for name in ('source','schema','intake','interpretation'):
            raw=(original.parent/document[name]['path']).resolve().read_bytes()
            path=root/(name+'.input');path.write_bytes(raw)
            document[name]={'path':str(path),'sha256':hashlib.sha256(raw).hexdigest()}
        path=root/'configuration.json';path.write_text(json.dumps(document))
        return ConfiguredSourceAdmissionConfig(path,ProducerConfig(Path('/producer'),Path('/bun'),Path('/git'),30,1048576,4194304),root/'output',4194304)
    def test_original_receipts_complete_report_after_admission(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700);config=self.fixture(root);receipt=b'{"original":"opaque public producer receipt"}\n'
            with patch('ashlar_host.configured_source_admission.admit_managed_records',return_value=(receipt,))as producer:
                result=validate_configured_source(config)
            self.assertTrue(result['admitted']);self.assertFalse(result['published']);self.assertFalse(result['acknowledged'])
            self.assertEqual((config.output/'record-1.json').read_bytes(),receipt)
            self.assertEqual(json.loads((config.output/'report.json').read_bytes()),result)
            self.assertEqual(result['receipts'][0]['sha256'],hashlib.sha256(receipt).hexdigest())
            self.assertEqual(producer.call_count,1)
            self.assertEqual(config.output.stat().st_mode&0o777,0o700)
            self.assertEqual((config.output/'report.json').stat().st_mode&0o777,0o600)
    def test_source_drift_or_producer_refusal_releases_no_report(self):
        for mode in ('drift','refusal','cancellation'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory()as directory:
                root=Path(directory).resolve();root.chmod(0o700);config=self.fixture(root);cancel=KeyboardInterrupt()
                def producer(*args):
                    if mode=='drift':(root/'source.input').write_bytes(b'changed');return (b'{}',)
                    if mode=='cancellation':raise cancel
                    raise ValueError('inert producer refused')
                with patch('ashlar_host.configured_source_admission.admit_managed_records',side_effect=producer):
                    with self.assertRaises(KeyboardInterrupt if mode=='cancellation'else ValueError)as raised:validate_configured_source(config)
                if mode=='cancellation':self.assertIs(raised.exception,cancel)
                self.assertFalse(config.output.exists())
