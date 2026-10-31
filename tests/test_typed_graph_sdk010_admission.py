import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ashlar.typed_graph_source import GRAPH_CHECK_PIN, build_typed_graph_request, verify_typed_graph_receipt
from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration
from ashlar_host.typed_graph_sdk010_admission import (
    SDK010_ADMISSION_PROFILE, SDK010_PRODUCER_REVISION,
    TypedGraphSdk010AdmissionConfig, _custody,
    admit_typed_graph_sdk010, admit_typed_graph_sdk010_policy,
)
from ashlar_host.configured_source_local import _Sdk010TypedGraphConfiguration, open_configured_local_source
from ashlar_host.configured_source_admission_config import ConfiguredSourceAdmissionConfig
from ashlar_host.config import ProducerConfig

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / 'docs/helix/04-build/evidence/current-upstream-release-20261029/g03-sdk058-delta4-realization-candidate-20261030'
ADMISSION_MANIFEST = ROOT / 'src/ashlar_host/resources/typed_graph_sdk010_source_admission.json'
NODE = Path('/opt/homebrew/Cellar/node/26.12.1/bin/node')
SDK_ROOT = Path('/private/tmp/ashlar-umf010-core104/sdk010/package')
SDK_ARCHIVE = Path('/private/tmp/ashlar-umf010-core104/umf-sdk-0.10.0.tgz')


class TypedGraphSdk010AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.configuration = load_typed_graph_jsonl_configuration(
            BASE / 'inputs/typed-graph/configured-typed-graph-source.json')
        self.model = (BASE / 'inputs/typed-graph/typed-graph-model.umf.json').read_bytes()
        self.request = build_typed_graph_request(self.configuration)
        self.config = TypedGraphSdk010AdmissionConfig(NODE, SDK_ROOT, SDK_ARCHIVE)
        self.manifest = json.loads(ADMISSION_MANIFEST.read_bytes())
        historical = json.loads((BASE / 'outputs/old-2ede-receipt.json').read_bytes())
        self.receipt = json.dumps(dict(historical, producerRevision=SDK010_PRODUCER_REVISION),
                                  separators=(',', ':')).encode()

    def test_real_official_sdk010_archive_and_public_api_admit_frozen_g03_request(self):
        # _custody proves the installed ESM entry and package metadata byte-match the official archive.
        baseline, manifest = _custody(self.config)
        self.assertEqual(manifest['profile'], SDK010_ADMISSION_PROFILE)
        self.assertEqual(manifest['sdkVersion'], '0.10.0')
        self.assertTrue(baseline)
        receipt = admit_typed_graph_sdk010(self.config, self.configuration, self.model, self.request)
        self.assertNotEqual(receipt, self.receipt,
            'SDK 0.10 admission must execute the selected package, not relabel the retained historical receipt')
        self.assertIsNone(verify_typed_graph_receipt(
            self.model, self.request, receipt, producer_revision=SDK010_PRODUCER_REVISION))
        policy = admit_typed_graph_sdk010_policy(self.config, self.configuration, self.model, self.request)
        self.assertEqual(policy.producer_revision, SDK010_PRODUCER_REVISION)
        self.assertEqual(policy.request_bytes, self.request)
        self.assertEqual(self.configuration.producer_revision, GRAPH_CHECK_PIN)

    def test_manifest_source_mismatch_refuses_before_checker(self):
        altered = dict(self.manifest, sourceSha256='0' * 64)
        with patch('ashlar_host.typed_graph_sdk010_admission._custody', return_value=((), altered)), \
             patch('ashlar_host.typed_graph_sdk010_admission.capture') as capture:
            with self.assertRaises(Exception):
                admit_typed_graph_sdk010(self.config, self.configuration, self.model, self.request)
        capture.assert_not_called()

    def _configured_local_inputs(self, root, receipt):
        output = root / 'admission'
        selection = ConfiguredSourceAdmissionConfig(
            self.configuration.path,
            ProducerConfig(root / 'producer', root / 'bun', root / 'git', 30, 1048576, 4194304),
            output, 4194304)
        report = {'source_sha256': hashlib.sha256(self.configuration.source_bytes).hexdigest(),
                  'receipts': [{'file': 'graph-1.json', 'sha256': hashlib.sha256(receipt).hexdigest(),
                                'bytes': len(receipt)}]}
        def admit_source(_selection):
            output.mkdir(mode=0o700)
            (output / 'graph-1.json').write_bytes(receipt)
            return report
        return selection, admit_source

    def test_local_owner_runs_full_sdk010_check_at_open_and_close_only(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(); root.chmod(0o700)
            selection, admit_source = self._configured_local_inputs(root, self.receipt)
            context = object(); custody = (('exact-sdk010-file-and-archive-bytes',), self.manifest)
            with patch('ashlar_host.configured_source_local.validate_configured_source', side_effect=admit_source), \
                 patch('ashlar_host.typed_graph_sdk010_admission._custody', return_value=custody), \
                 patch('ashlar_host.typed_graph_sdk010_admission.admit_typed_graph_sdk010', return_value=self.receipt) as full:
                with open_configured_local_source(selection, stream='sdk010-test', context=context,
                                                  sdk010_admission=self.config) as source:
                    self.assertEqual(full.call_count, 1)
                    for _ in range(3):
                        source.renew(context)
                        source.spec(1, context)
                        source.metadata(context)
                    self.assertEqual(full.call_count, 1)
                self.assertEqual(full.call_count, 2)

    def test_inner_renewal_refuses_changed_archive_custody_without_checker_replay(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(); root.chmod(0o700)
            selection, admit_source = self._configured_local_inputs(root, self.receipt)
            context = object(); initial = (('exact-sdk010-file-and-archive-bytes',), self.manifest)
            changed = (('changed-sdk010-archive-identity',), self.manifest)
            state = {'custody': initial}
            with patch('ashlar_host.configured_source_local.validate_configured_source', side_effect=admit_source), \
                 patch('ashlar_host.typed_graph_sdk010_admission._custody', side_effect=lambda _config: state['custody']), \
                 patch('ashlar_host.typed_graph_sdk010_admission.admit_typed_graph_sdk010', return_value=self.receipt) as full:
                with self.assertRaises(Exception):
                    with open_configured_local_source(selection, stream='sdk010-test', context=context,
                                                      sdk010_admission=self.config) as source:
                        self.assertEqual(full.call_count, 1)
                        state['custody'] = changed
                        source.spec(1, context)
                self.assertEqual(full.call_count, 1)

    def test_final_full_sdk010_check_failure_propagates_from_owner_close(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(); root.chmod(0o700)
            selection, admit_source = self._configured_local_inputs(root, self.receipt)
            context = object(); custody = (('exact-sdk010-file-and-archive-bytes',), self.manifest)
            with patch('ashlar_host.configured_source_local.validate_configured_source', side_effect=admit_source), \
                 patch('ashlar_host.typed_graph_sdk010_admission._custody', return_value=custody), \
                 patch('ashlar_host.typed_graph_sdk010_admission.admit_typed_graph_sdk010',
                       side_effect=[self.receipt, ValueError('closing SDK010 check refused')]) as full:
                with self.assertRaisesRegex(ValueError, 'closing SDK010 check refused'):
                    with open_configured_local_source(selection, stream='sdk010-test', context=context,
                                                      sdk010_admission=self.config) as source:
                        self.assertIsNotNone(source)
                        self.assertEqual(full.call_count, 1)
                self.assertEqual(full.call_count, 2)

    def test_sdk010_profile_is_disjoint_from_sdk058_admission_option(self):
        with self.assertRaises(ValueError):
            with open_configured_local_source(None, stream='bad', context=object(),
                sdk058_admission=object(), sdk010_admission=object()):
                pass

    def test_runtime_opt_in_is_v04_only_and_initialization_only(self):
        from ashlar_host.configured_local_runtime import ConfiguredLocalRuntimeConfig
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(); root.chmod(0o700)
            output = root / 'output'
            admission = ConfiguredSourceAdmissionConfig(
                self.configuration.path,
                ProducerConfig(root / 'producer', root / 'bun', root / 'git', 30, 1048576, 4194304),
                output / 'admission', 4194304)
            args = dict(installation=root / 'installation', output=output, jars=root / 'jars',
                java_home=root / 'java', admission=admission, stream='sdk010-test',
                materialization_clock_us='1791547200000000', operation_capacity=None,
                installation_id='sdk010-test', installation_sha256=None,
                jar_inventory={'delta-spark_4.2_2.13-4.4.1.jar': 'a' * 64,
                               'delta-storage-4.4.1.jar': 'b' * 64},
                carrier_profile='ashlar-delta/0.4-xxhash64', sdk010_admission=self.config)
            configured = ConfiguredLocalRuntimeConfig(**args)
            self.assertEqual(configured.sdk010_admission, self.config)
            with self.assertRaises(ValueError):
                ConfiguredLocalRuntimeConfig(**dict(args, carrier_profile='ashlar-delta/0.3'))


if __name__ == '__main__':
    unittest.main()
