import hashlib
import json
import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

from ashlar.typed_graph_source_sdk0101 import GRAPH_CHECK_PIN
from ashlar_host.local_current_delta4_umf_sdk0101 import (
    UMF_REALIZATION_PROFILE, pinned_sdk0101_resources,
)

ROOT = Path(__file__).resolve().parents[1]


class UmfSdk0101PinTests(unittest.TestCase):
    def test_sdk0101_delta_profile_custody_and_six_role_subset(self):
        manifest, bridge, sources, semantics = pinned_sdk0101_resources()
        self.assertEqual(UMF_REALIZATION_PROFILE,
                         'ashlar-local-current-delta4-umf-source-sdk0101/0.1')
        self.assertEqual(manifest['selectedRelease']['version'], '0.10.1')
        self.assertEqual(manifest['selectedRelease']['semanticCore'], '0.10.0')
        self.assertEqual(hashlib.sha256(bridge).hexdigest(), manifest['bridge']['assetSha256'])
        self.assertEqual(set(sources), {
            'object_current.umf.json', 'edge_current.umf.json', 'tombstone.umf.json',
            'whole_source_history.umf.json', 'publication_manifest.umf.json',
            'publication_attempt_phase.umf.json', 'index.json',
        })
        self.assertEqual(set(semantics), set(manifest['roles']))

    def test_sdk0101_typed_graph_source_is_explicitly_pinned_to_current_release(self):
        self.assertEqual(GRAPH_CHECK_PIN, '90cb61eb63aec3f5a77b5991a56e3dfc3d10bd27')
        from ashlar.typed_graph_source import GRAPH_CHECK_PIN as HISTORICAL_PIN
        self.assertEqual(HISTORICAL_PIN, '2ede4c879bea5db13794a6fa30e67c821306c17f')
        checker = (ROOT / 'src/ashlar_host/resources/typed_graph_values_sdk0101.ts').read_text()
        self.assertIn("const PIN='90cb61eb63aec3f5a77b5991a56e3dfc3d10bd27';", checker)
        admission = (ROOT / 'src/ashlar_host/typed_graph_admission_sdk0101.py').read_text()
        self.assertIn("UMF_REVISION='90cb61eb63aec3f5a77b5991a56e3dfc3d10bd27'", admission)


    def test_sdk0101_runtime_profile_is_separate_and_versioned(self):
        from tempfile import TemporaryDirectory
        from types import SimpleNamespace
        from ashlar_host.config import ProducerConfig
        from ashlar_host.typed_graph_admission_sdk0101 import TypedGraphSdk0101AdmissionConfig
        from ashlar_host.configured_local_runtime import ConfiguredLocalRuntimeConfig, _scope
        from ashlar_host.configured_source_admission_config import ConfiguredSourceAdmissionConfig
        from ashlar_host.configured_local_delta4_publication import _valid_installation_profile_identity
        from ashlar_host.local_current_delta4_umf_sdk0101 import (
            BRIDGE_SHA256, BUILD_INPUTS_SHA256, LOCAL_TARGET_PROFILE,
            MANIFEST_SHA256, SOURCE_POLICY_SHA256, UMF_REALIZATION_PROFILE,
        )
        with TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            producer = ProducerConfig(root / 'umf', root / 'bun', root / 'git', 60, 1048576, 4194304)
            admission = ConfiguredSourceAdmissionConfig(root / 'config.json', producer,
                                                        root / 'output' / 'admission', 4194304)
            config = ConfiguredLocalRuntimeConfig(
                root / 'installation', root / 'output', root / 'jars', root / 'java', admission,
                'sdk0101-test', '1791547200000000', None, 'sdk0101-test', None,
                {'delta-spark_4.2_2.13-4.4.1.jar': 'a' * 64,
                 'delta-storage-4.4.1.jar': 'b' * 64},
                'ashlar-delta/0.4-xxhash64',
                sdk0101_admission=TypedGraphSdk0101AdmissionConfig(producer))
            scope = _scope(config, SimpleNamespace(
                configuration_file=SimpleNamespace(raw=b'config'), original_source=b'source',
                configuration=SimpleNamespace(model_sha256='c' * 64)))
            self.assertEqual(scope['typed_graph_policy_revision'], GRAPH_CHECK_PIN)
            profile = {
                'profile': 'ashlar-local-current-installation-profile/0.4',
                'physical_profile': 'ashlar-delta/0.4-xxhash64',
                'lookup_hash_profile': 'spark-xxhash64-typed-key/0.1',
                'generated_carriers_sha256': 'cb6c75c3cb3805011245ca460a763f15509a7d9627009301cc815e6983413faa',
                'umf_revision': '0.10.1', 'local_realization': LOCAL_TARGET_PROFILE,
                'umf_realization_profile': UMF_REALIZATION_PROFILE,
                'umf_candidate_commit': GRAPH_CHECK_PIN, 'umf_selected_tag_commit': GRAPH_CHECK_PIN,
                'umf_archive_sha256': '3d95fff822f4f44b3d1a8e56185c3697e94ee66f5315cda7389f5301f3d16d26',
                'umf_source_policy_sha256': SOURCE_POLICY_SHA256,
                'umf_realization_manifest_sha256': MANIFEST_SHA256,
                'umf_bridge_sha256': BRIDGE_SHA256, 'umf_build_inputs_sha256': BUILD_INPUTS_SHA256,
                'source_policy_revision': 'ashlar-local-current-delta4-sdk0101-source-policy/0.1',
                'typed_graph_source_revision': GRAPH_CHECK_PIN,
                'writer_settings': [{'key': 'spark.sql.parquet.compression.codec', 'value': 'zstd'}],
                'roles': {},
            }
            self.assertTrue(_valid_installation_profile_identity(profile))

    def test_sdk0101_source_binds_one_open_receipt_and_rechecks_on_close(self):
        from tempfile import TemporaryDirectory
        from types import SimpleNamespace
        from ashlar.typed_graph_source_sdk0101 import _build
        from ashlar_host.config import ProducerConfig
        from ashlar_host.configured_source_admission_config import ConfiguredSourceAdmissionConfig
        from ashlar_host.configured_source_local import open_configured_local_source
        from ashlar_host.typed_graph_admission_sdk0101 import TypedGraphSdk0101AdmissionConfig

        class Policy:
            def __init__(self, configuration, receipt, *, producer_revision=GRAPH_CHECK_PIN):
                self.producer_revision = producer_revision
                self.request_bytes = _build(configuration)[0]
                self.receipt = receipt
                self.source = configuration.model_source
                self.bindings = configuration.bindings
                self.relationships = configuration.relationships
                self.source_system = configuration.source_system
                self.schema_revision = configuration.document_revision
            def __call__(self, change):
                return None

        with TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            (root / 'inputs').mkdir(mode=0o700)
            (root / 'out').mkdir(mode=0o700)
            fixture = ROOT / 'docs/helix/04-build/evidence/current-upstream-release-20261029/g03-sdk058-delta4-realization-candidate-20261030/inputs/typed-graph'
            for name in ('typed-graph-source.jsonl', 'typed-graph-model.umf.json',
                         'typed-graph-binding.json'):
                shutil.copyfile(fixture / name, root / 'inputs' / name)
            config_value = json.loads((fixture / 'configured-typed-graph-source.json').read_bytes())
            config_value['producerRevision'] = GRAPH_CHECK_PIN
            configuration_path = root / 'inputs' / 'configured.json'
            configuration_path.write_text(json.dumps(config_value, separators=(',', ':')))
            raw_receipt = b'one retained SDK0101 receipt'
            producer = ProducerConfig(root / 'umf', root / 'bun', root / 'git',
                                      60, 1048576, 4194304)
            admission = ConfiguredSourceAdmissionConfig(
                configuration_path, producer, root / 'out' / 'admission', 4194304)
            sdk_admission = TypedGraphSdk0101AdmissionConfig(producer)
            custody = ((('source-checker-and-git-identity',),),)

            def run_open_admission(selected, *, sdk0101_admission):
                self.assertIs(selected, admission)
                self.assertIs(sdk0101_admission, sdk_admission)
                from ashlar_host.configured_typed_graph_source_admission_sdk0101 import (
                    validate_configured_typed_graph_source,
                )
                return validate_configured_typed_graph_source(selected,
                    sdk0101_admission=sdk0101_admission)

            with patch('ashlar.typed_graph_source_sdk0101.TypedGraphSourcePolicy', Policy), \
                 patch('ashlar.typed_graph_source_sdk0101.verify_typed_graph_receipt'), \
                 patch('ashlar_host.typed_graph_admission_sdk0101._custody', return_value=custody), \
                 patch('ashlar_host.configured_typed_graph_source_admission_sdk0101.admit_typed_graph',
                       return_value=raw_receipt) as open_check, \
                 patch('ashlar_host.typed_graph_admission_sdk0101.admit_typed_graph',
                       return_value=raw_receipt) as close_check, \
                 patch('ashlar_host.configured_source_local.validate_configured_source',
                       side_effect=run_open_admission):
                with open_configured_local_source(admission, stream='sdk0101-test',
                        context=SimpleNamespace(), sdk0101_admission=sdk_admission) as source:
                    self.assertEqual(open_check.call_count, 1)
                    self.assertEqual(close_check.call_count, 0)
                    self.assertEqual(source.receipt_files[0][1], raw_receipt)
                    self.assertEqual(source.sdk0101_receipt, raw_receipt)
                    self.assertEqual(source.metadata(source.context)['typed_graph_sdk0101_receipt_sha256'],
                                     hashlib.sha256(raw_receipt).hexdigest())
                self.assertEqual(open_check.call_count, 1)
                self.assertEqual(close_check.call_count, 1)


if __name__ == '__main__':
    unittest.main()
