import json
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ashlar.typed_graph_source import GRAPH_CHECK_PIN, build_typed_graph_request, verify_typed_graph_receipt
from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration
from ashlar_host.typed_graph_sdk058_admission import (
    SDK058_ADMISSION_PROFILE,
    SDK058_PRODUCER_REVISION,
    TypedGraphSdk058AdmissionConfig,
    admit_typed_graph_sdk058,
    admit_typed_graph_sdk058_policy,
)
from ashlar_host.configured_source_local import _Sdk058TypedGraphConfiguration
from ashlar_host.configured_source_local import open_configured_local_source
from ashlar_host.configured_source_admission_config import ConfiguredSourceAdmissionConfig
from ashlar_host.config import ProducerConfig

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / 'docs/helix/04-build/evidence/current-upstream-release-20261029/g03-sdk058-delta4-realization-candidate-20261030'
ADDENDUM = ROOT / 'docs/helix/04-build/evidence/current-upstream-release-20261029/g03-sdk058-source-correspondence-20261030'


class TypedGraphSdk058AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.configuration = load_typed_graph_jsonl_configuration(
            BASE / 'inputs/typed-graph/configured-typed-graph-source.json')
        self.model = (BASE / 'inputs/typed-graph/typed-graph-model.umf.json').read_bytes()
        self.historical = json.loads((BASE / 'outputs/old-2ede-receipt.json').read_bytes())
        self.release_receipt = dict(self.historical, producerRevision=SDK058_PRODUCER_REVISION)
        self.release_bytes = json.dumps(self.release_receipt, separators=(',', ':')).encode()
        self.manifest = json.loads((ADDENDUM / 'inputs/typed-graph/sdk058-source-admission.json').read_bytes())
        self.config = TypedGraphSdk058AdmissionConfig(
            node=Path('/runtime/node'), sdk_package_root=Path('/runtime/@umf/core'),
            sdk_archive=Path('/runtime/sdk.tgz'), producer_closure=Path('/runtime/closure.json'),
            producer_receipt=Path('/runtime/producer.json'), install_custody=Path('/runtime/install.json'),
            source_admission_manifest=ADDENDUM / 'inputs/typed-graph/sdk058-source-admission.json')

    def test_release_revision_is_explicit_and_old_default_stays_pinned(self):
        with self.assertRaises(ValueError):
            verify_typed_graph_receipt(self.model, build_typed_graph_request(self.configuration),
                self.release_bytes)
        verify_typed_graph_receipt(
            self.model,
            build_typed_graph_request(self.configuration),
            self.release_bytes, producer_revision=SDK058_PRODUCER_REVISION)
        self.assertEqual(self.configuration.producer_revision, GRAPH_CHECK_PIN)

    def test_admission_uses_release_receipt_and_can_mint_explicit_policy(self):
        baseline = (('pinned-custody',), self.manifest)
        with patch('ashlar_host.typed_graph_sdk058_admission._custody', return_value=baseline), \
             patch('ashlar_host.typed_graph_sdk058_admission.capture', return_value=(self.release_bytes, b'')):
            receipt = admit_typed_graph_sdk058(self.config, self.configuration, self.model)
            self.assertEqual(receipt, self.release_bytes)
        with patch('ashlar_host.typed_graph_sdk058_admission._custody', return_value=baseline), \
             patch('ashlar_host.typed_graph_sdk058_admission.capture', return_value=(self.release_bytes, b'')):
            policy = admit_typed_graph_sdk058_policy(self.config, self.configuration, self.model)
        self.assertEqual(policy.producer_revision, SDK058_PRODUCER_REVISION)
        self.assertEqual(policy.request_bytes, build_typed_graph_request(self.configuration))
        self.assertEqual(self.manifest['profile'], SDK058_ADMISSION_PROFILE)

    def test_local_cutover_wraps_source_config_without_changing_historical_default(self):
        wrapped=_Sdk058TypedGraphConfiguration.admitted(
            self.configuration,None,self.release_bytes)
        self.assertIsNone(self.configuration.policy)
        self.assertIsNone(self.configuration.original_receipt)
        self.assertEqual(self.configuration.producer_revision,GRAPH_CHECK_PIN)
        self.assertEqual(wrapped.policy.producer_revision,SDK058_PRODUCER_REVISION)
        self.assertEqual(wrapped.policy_revision,SDK058_PRODUCER_REVISION)
        wrapped.verify()

    def test_manifest_mismatch_refuses_before_execution(self):
        altered = dict(self.manifest, sourceSha256='0' * 64)
        with patch('ashlar_host.typed_graph_sdk058_admission._custody', return_value=((), altered)), \
             patch('ashlar_host.typed_graph_sdk058_admission.capture') as capture:
            with self.assertRaises(Exception):
                admit_typed_graph_sdk058(self.config, self.configuration, self.model)
        capture.assert_not_called()

    def _configured_local_inputs(self, root):
        output=root/'admission'
        selection=ConfiguredSourceAdmissionConfig(
            self.configuration.path,
            ProducerConfig(root/'producer',root/'bun',root/'git',30,1048576,4194304),
            output,4194304)
        report={'source_sha256':hashlib.sha256(self.configuration.source_bytes).hexdigest(),
                'receipts':[{'file':'graph-1.json','sha256':hashlib.sha256(self.release_bytes).hexdigest(),
                             'bytes':len(self.release_bytes)}]}
        def admit_source(_selection):
            output.mkdir(mode=0o700)
            (output/'graph-1.json').write_bytes(self.release_bytes)
            return report
        return selection,admit_source

    def test_inner_renewals_reuse_exact_receipt_and_final_close_rechecks_node(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary).resolve();root.chmod(0o700)
            selection,admit_source=self._configured_local_inputs(root);context=object()
            custody=(('exact-sdk058-file-bytes-and-identities',),self.manifest)
            with patch('ashlar_host.configured_source_local.validate_configured_source',side_effect=admit_source), \
                 patch('ashlar_host.typed_graph_sdk058_admission._custody',return_value=custody), \
                 patch('ashlar_host.typed_graph_sdk058_admission.admit_typed_graph_sdk058',
                       return_value=self.release_bytes) as full_node:
                with open_configured_local_source(selection,stream='sdk058-test',context=context,
                                                  sdk058_admission=self.config) as source:
                    self.assertEqual(full_node.call_count,1)
                    for _ in range(3):
                        source.renew(context)
                        request=source.spec(1,context).request()
                        source.admit_request(request,context)
                        source.metadata(context)
                    self.assertEqual(full_node.call_count,1)
                self.assertEqual(full_node.call_count,2)

    def test_inner_renewal_refuses_changed_sdk_file_identity_without_node_replay(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary).resolve();root.chmod(0o700)
            selection,admit_source=self._configured_local_inputs(root);context=object()
            original=(('exact-sdk058-file-bytes-and-identities',),self.manifest)
            changed=((('same-digest-replaced-path-identity',),),self.manifest)
            custody_state={'value':original}
            with patch('ashlar_host.configured_source_local.validate_configured_source',side_effect=admit_source), \
                 patch('ashlar_host.typed_graph_sdk058_admission._custody',
                       side_effect=lambda _config:custody_state['value']), \
                 patch('ashlar_host.typed_graph_sdk058_admission.admit_typed_graph_sdk058',
                       return_value=self.release_bytes) as full_node:
                with self.assertRaises(ValueError):
                    with open_configured_local_source(selection,stream='sdk058-test',context=context,
                                                      sdk058_admission=self.config) as source:
                        self.assertEqual(full_node.call_count,1)
                        custody_state['value']=changed
                        source.spec(1,context)
                self.assertEqual(full_node.call_count,1)


if __name__ == '__main__':
    unittest.main()
