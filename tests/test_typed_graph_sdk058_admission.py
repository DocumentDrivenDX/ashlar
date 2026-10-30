import json
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


if __name__ == '__main__':
    unittest.main()
