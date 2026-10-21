# SPDX-License-Identifier: MIT OR Apache-2.0
import base64
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

from ashlar.truss_native_feed import native_feed_batch
from ashlar_host.managed_current_core import (
    ManagedCurrentCoreConfig, ManagedCurrentCoreObservation, ROLES,
    bind_current_core, bind_observed_managed_installation,restore_managed_current_admission,
)
from ashlar_host.truss_current_catalog import CurrentCatalogEvidence
from ashlar_host.truss_graph_admission import TrussGraphAdmission


def _config(backend03=None, backend02=None):
    return ManagedCurrentCoreConfig('client_dev', 'offline_test_schema',
                                    backend03, backend02, 10, 100)


def _current_admission():
    evidence_dir = (Path(__file__).parents[1] / 'docs/helix/04-build/evidence'
                    / 'truss-current-catalog-0.10-20261021')
    report_raw = (evidence_dir / 'accepted-report.json').read_bytes()
    view_raw = (evidence_dir / 'catalog-view.json').read_bytes()
    source_raw = (evidence_dir / 'accepted-source.umf.json').read_bytes()
    report = json.loads(report_raw)
    accepted = report['acceptedInput']['documents'][0]
    source = SimpleNamespace(document_id=accepted['documentId'],
        document_revision=accepted['documentRevision'], artifact_identity=accepted['artifact']['identity'],
        registration_id=report['registrationId'], sha256=accepted['artifact']['sha256'],
        content=source_raw, catalog_revision=1)
    sources = {('1', accepted['documentId']): source}
    installation = report['installationId']
    registration = report['registrationId']
    prior = {'profile': 'truss-caller-asserted-feed/0.1', 'installationId': installation,
             'sourceEpoch': 'offline-development-epoch', 'consumerId': 'offline-development-consumer',
             'registrationId': registration, 'nextXid': '0'}
    following = {**prior, 'nextXid': '2'}
    page = {'interfaceVersion': 'truss-native-feed/0.1', 'prior': prior, 'next': following,
            'writerXid': '1', 'catalogPrerequisites': [{'revision': '1',
                'reportBytesBase64': base64.b64encode(report_raw).decode(),
                'reportSha256': hashlib.sha256(report_raw).hexdigest()}],
            'revisions': [], 'changes': [], 'sources': [], 'reservations': [], 'configuration': {}}
    raw = json.dumps(page, separators=(',', ':')).encode()
    batch = native_feed_batch(raw, sha256=hashlib.sha256(raw).hexdigest(),
        prior=json.dumps(prior, separators=(',', ':')).encode(),
        next=json.dumps(following, separators=(',', ':')).encode(), feed='offline-development-feed')
    evidence = CurrentCatalogEvidence('0.0.44', {'1': report_raw}, {'1': view_raw}, sources)
    return TrussGraphAdmission(batch, evidence)


def _inventory(schema='offline_test_schema'):
    return {role: {'name': ('client_dev', schema, role),
                   'uuid': f'00000000-0000-0000-0000-{index:012d}', 'version': 0}
            for index, role in enumerate(ROLES, 1)}


class ManagedCurrentCoreTests(unittest.TestCase):
    def test_current_umf010_binding_retains_originals_and_complete_installation_inventory(self):
        admission = _current_admission()
        config = _config()
        prepared = bind_current_core(admission, config)
        self.assertEqual(prepared['status'], 'preinstallation-source-bundle')
        self.assertIsNone(prepared['destination']['tables'])
        binding = bind_observed_managed_installation(prepared, config, _inventory())
        self.assertEqual(binding['status'], 'postinstallation-installation-inventory-bound-offline')
        source = binding['source']
        self.assertEqual(source['authority'], 'development-only-until-production-owner-observed')
        self.assertEqual(source['umfVersion'], '0.10.0')
        self.assertEqual(source['trussVersion'], '0.0.44')
        self.assertNotIn('catalogEvidenceProfile', source)
        self.assertNotIn('runtimeProvenance', source)
        self.assertEqual(base64.b64decode(source['nativePageBase64'], validate=True), admission.batch.begin)
        document = source['acceptedDocuments']['1:typed-graph-example']
        original = base64.b64decode(document['sourceBytesBase64'], validate=True)
        self.assertEqual(original, (Path(__file__).parents[1] / 'docs/helix/04-build/evidence'
                                    / 'truss-current-catalog-0.10-20261021/accepted-source.umf.json').read_bytes())
        graph = source['assignedGraphByRevision']['1']
        self.assertEqual(set(graph['types']), {'1', '2'})
        self.assertEqual(set(graph['relationships']), {'1', '2'})
        self.assertEqual({prop['family'] for record in graph['types'].values()
                          for prop in record['properties']}, {'string', 'integer'})
        self.assertEqual(set(binding['destination']['tables']), set(ROLES))
        self.assertTrue(all(len(table['name']) == 3 and table['name'][0] == 'client_dev'
                            for table in binding['destination']['tables'].values()))
        self.assertEqual(binding['bounds'], {'preferredCostUsd': 10, 'hardCostUsd': 100})

    def test_managed_task_reconstructs_original_current_page_and_assignments(self):
        config=_config();admission=_current_admission();bundle=bind_current_core(admission,config)
        restored=restore_managed_current_admission(bundle,config)
        self.assertEqual(restored.batch,admission.batch)
        self.assertEqual(restored.metadata(),admission.metadata())

    def test_xxhash64_bundle_is_versioned_and_cannot_alias_legacy_profile(self):
        from ashlar_host.managed_current_core import CARRIER_PROFILE_XXHASH64
        from ashlar_host.managed_current_carriers import load_current_managed_carriers
        from ashlar_host.managed_current_weft_binding import (
            XXHASH64_BACKEND_ID, XXHASH64_BACKEND02_VERSION,
            XXHASH64_BACKEND03_VERSION, XXHASH64_TARGET_PROFILE,
        )

        legacy = _config()
        config = ManagedCurrentCoreConfig(legacy.catalog, legacy.schema,
            {'surface':'Backend03','backendId':XXHASH64_BACKEND_ID,
             'backendVersion':XXHASH64_BACKEND03_VERSION,'targetProfile':XXHASH64_TARGET_PROFILE,
             'weftInterface':'weft-compile/0.4.1','weftDialect':'weft-sql/0.4.1'},
            {'surface':'Backend02','backendId':XXHASH64_BACKEND_ID,
             'backendVersion':XXHASH64_BACKEND02_VERSION,'targetProfile':XXHASH64_TARGET_PROFILE,
             'weftInterface':'weft-compile/0.2.7','weftDialect':'weft-sql/0.2.0'},
            legacy.preferred_cost_usd,
            legacy.hard_cost_usd, carrier_profile=CARRIER_PROFILE_XXHASH64)
        admission = _current_admission()
        bundle = bind_current_core(admission, config)
        self.assertEqual(bundle["profile"], "ashlar-managed-current-core-binding/0.3")
        self.assertEqual(bundle["destination"]["physicalProfile"], CARRIER_PROFILE_XXHASH64)
        restored = restore_managed_current_admission(bundle, config)
        self.assertEqual(restored.batch, admission.batch)
        carriers = load_current_managed_carriers(CARRIER_PROFILE_XXHASH64)
        self.assertEqual(carriers.physical_profile, CARRIER_PROFILE_XXHASH64)
        self.assertIn(("lookup_hash", "BIGINT", False), carriers.ordered_fields("object_current"))
        with self.assertRaisesRegex(ValueError, "physical carrier profile"):
            restore_managed_current_admission(bundle, legacy)

        forged = json.loads(json.dumps(bundle))
        forged["destination"]["physicalProfile"] = "ashlar-delta/0.3"
        forged.pop("bindingSha256")
        forged["bindingSha256"] = hashlib.sha256(json.dumps(forged,
            sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
        with self.assertRaises(ValueError):
            restore_managed_current_admission(forged, config)

    def test_current_045_bundle_requires_distinct_version_profile_and_runtime_provenance(self):
        config=_config();bundle=bind_current_core(_current_admission(),config)
        self.assertEqual(bundle['profile'],'ashlar-managed-current-core-binding/0.1')
        forged=json.loads(json.dumps(bundle))
        forged['source']['trussVersion']='0.0.45'
        forged['profile']='ashlar-managed-current-core-binding/0.2'
        forged.pop('bindingSha256')
        forged['bindingSha256']=hashlib.sha256(json.dumps(forged,sort_keys=True,
            separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
        with self.assertRaisesRegex(ValueError,'wheel/runtime provenance'):
            restore_managed_current_admission(forged,config)
        forged['source']['catalogEvidenceProfile']='ashlar-truss-current-catalog-evidence/0.2'
        forged['source']['runtimeProvenance']={
            'trussWheelSha256':'0'*64,'runtimeManifestSha256':'1'*64}
        forged.pop('bindingSha256')
        forged['bindingSha256']=hashlib.sha256(json.dumps(forged,sort_keys=True,
            separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
        with self.assertRaisesRegex(ValueError,'wheel/runtime provenance'):
            restore_managed_current_admission(forged,config)
        from ashlar_host.truss_current_catalog import CurrentCatalogEvidence
        with self.assertRaisesRegex(ValueError,'runtime-attested catalog evidence'):
            CurrentCatalogEvidence('0.0.45',{}, {}, {}, runtime_wheel_sha256='0'*64,
                                   runtime_manifest_sha256='1'*64)

    def test_known_old_and_local_weft_targets_cannot_claim_current_managed_identity(self):
        old = {'surface': 'Backend03', 'backendId': 'ashlar.databricks',
               'backendVersion': '0.1.0-qualified', 'targetProfile': 'dbsql2026.39-qualified',
               'weftInterface': 'weft-compile/0.2.7', 'weftDialect': 'weft-sql/0.2.0'}
        with self.assertRaisesRegex(ValueError, 'historical or local'):
            _config(old, None)
        candidate = {'surface': 'Backend03', 'backendId': 'ashlar.databricks.paths-keys-having',
            'backendVersion': '0.4.1-count-star-having-spark4.2-candidate',
            'targetProfile': 'spark4.2.0-delta4.4.1-paths-keys-having-candidate',
            'weftInterface': 'weft-compile/0.4.1', 'weftDialect': 'weft-sql/0.4.1'}
        with self.assertRaisesRegex(ValueError, 'historical or local'):
            _config(candidate, None)

    def test_historical_umf070_admission_cannot_enter_current_binding(self):
        from test_truss_graph_admission import Tests as HistoricalGraphFixture
        batch, catalogs = HistoricalGraphFixture().original()
        historical = TrussGraphAdmission(batch, catalogs)
        with self.assertRaisesRegex(ValueError, 'versioned current Truss / UMF 0.10'):
            bind_current_core(historical, _config())

    def test_cost_ceiling_is_closed(self):
        with self.assertRaisesRegex(ValueError, 'cost bounds'):
            ManagedCurrentCoreConfig('client_dev', 'offline_test_schema', None, None, 11, 10)

    def test_runtime_and_production_source_observations_require_every_field(self):
        with self.assertRaisesRegex(ValueError, 'observations required'):
            ManagedCurrentCoreObservation().require_observed()
        placeholder = ManagedCurrentCoreObservation('install', 'Databricks', 'not observed',
            'build-id', 'protocol-observed', 'ordinary-principal', 'a' * 64, 'protected-ack-owner')
        with self.assertRaisesRegex(ValueError, 'observations required'):
            placeholder.require_observed()

    def test_incomplete_installation_inventory_and_wrong_namespace_refuse(self):
        admission = _current_admission()
        config = _config()
        prepared = bind_current_core(admission, config)
        with self.assertRaisesRegex(ValueError, 'six-role'):
            bind_observed_managed_installation(prepared, config, {})
        with self.assertRaisesRegex(ValueError, 'three-part Unity Catalog'):
            bind_observed_managed_installation(prepared, config, _inventory('other_schema'))


if __name__ == '__main__':
    unittest.main()
