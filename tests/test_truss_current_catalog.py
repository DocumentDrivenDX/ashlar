import json
import base64
import hashlib
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ashlar_host.truss_current_catalog import (
    CURRENT_045_EVIDENCE_PROFILE, CURRENT_TRUSS_VERSION, CURRENT_TRUSS_045_VERSION,
    CURRENT_TRUSS_045_RUNTIME_MANIFEST_SHA256, CURRENT_TRUSS_045_WHEEL_SHA256,
    CURRENT_048_EVIDENCE_PROFILE, CURRENT_TRUSS_048_VERSION,
    CURRENT_TRUSS_048_RUNTIME_MANIFEST_SHA256, CURRENT_TRUSS_048_WHEEL_SHA256,
    CURRENT_049_EVIDENCE_PROFILE, CURRENT_TRUSS_049_VERSION,
    CURRENT_TRUSS_049_RUNTIME_MANIFEST_SHA256, CURRENT_TRUSS_049_WHEEL_SHA256,
    CurrentCatalogEvidence, interpret_current_catalog, require_current_truss_runtime,
    require_installed_current_catalog_evidence,
)


EVIDENCE = (Path(__file__).parents[1] / 'docs/helix/04-build/evidence'
            / 'truss-current-catalog-0.10-20261021')


def receipt_inputs():
    report_raw = (EVIDENCE / 'accepted-report.json').read_bytes()
    view_raw = (EVIDENCE / 'catalog-view.json').read_bytes()
    source_raw = (EVIDENCE / 'accepted-source.umf.json').read_bytes()
    report = json.loads(report_raw)
    accepted = report['acceptedInput']['documents'][0]
    source = SimpleNamespace(
        document_id=accepted['documentId'],
        document_revision=accepted['documentRevision'],
        artifact_identity=accepted['artifact']['identity'],
        registration_id=report['registrationId'],
        sha256=accepted['artifact']['sha256'],
        content=source_raw,
        catalog_revision=1,
    )
    sources = {('1', accepted['documentId']): source}
    return report_raw, view_raw, sources, source


class TrussCurrentCatalogTests(unittest.TestCase):
    def test_graph_admission_profiles_are_version_specific(self):
        from ashlar_host.truss_graph_admission import current_admission_profile
        self.assertEqual(current_admission_profile(CURRENT_TRUSS_VERSION),
                         'ashlar-truss-current-umf010-graph-semantic-admission/0.1')
        self.assertEqual(current_admission_profile(CURRENT_TRUSS_045_VERSION),
                         'ashlar-truss-current-umf010-graph-semantic-admission/0.2')
        self.assertEqual(current_admission_profile(CURRENT_TRUSS_048_VERSION),
                         'ashlar-truss-current-umf010-graph-semantic-admission/0.3')
        self.assertEqual(current_admission_profile(CURRENT_TRUSS_049_VERSION),
                         'ashlar-truss-current-umf010-graph-semantic-admission/0.4')
        with self.assertRaisesRegex(ValueError, 'versioned current Truss'):
            current_admission_profile('0.0.46')

    def test_versioned_current_runtime_profiles_preserve_044_and_pin_045(self):
        self.assertEqual(require_current_truss_runtime(CURRENT_TRUSS_VERSION),
                         'ashlar-truss-current-catalog-evidence/0.1')
        manifest = b'fixture runtime manifest'
        wheel = CURRENT_TRUSS_045_WHEEL_SHA256
        direct_url = json.dumps({'archive_info': {'hashes': {'sha256': wheel}}})
        with patch('ashlar_host.truss_current_catalog.CURRENT_TRUSS_045_RUNTIME_MANIFEST_SHA256',
                   hashlib.sha256(manifest).hexdigest()):
            self.assertEqual(require_current_truss_runtime(CURRENT_TRUSS_045_VERSION,
                                                           runtime_manifest=manifest,
                                                           direct_url=direct_url),
                             CURRENT_045_EVIDENCE_PROFILE)
            with self.assertRaisesRegex(ValueError, 'manifest differs'):
                require_current_truss_runtime(CURRENT_TRUSS_045_VERSION,
                                              runtime_manifest=b'wrong',
                                              direct_url=direct_url)
        with self.assertRaisesRegex(ValueError, 'runtime-attested'):
            CurrentCatalogEvidence(CURRENT_TRUSS_045_VERSION, {}, {}, {})
        self.assertEqual(CurrentCatalogEvidence(CURRENT_TRUSS_045_VERSION, {}, {}, {},
                                                CURRENT_TRUSS_045_WHEEL_SHA256,
                                                CURRENT_TRUSS_045_RUNTIME_MANIFEST_SHA256).profile,
                         CURRENT_045_EVIDENCE_PROFILE)
        with self.assertRaisesRegex(ValueError, 'public wheel provenance'):
            with patch('ashlar_host.truss_current_catalog.CURRENT_TRUSS_045_RUNTIME_MANIFEST_SHA256',
                       hashlib.sha256(manifest).hexdigest()):
                require_current_truss_runtime(CURRENT_TRUSS_045_VERSION,
                                              runtime_manifest=manifest, direct_url='{}')

    def test_fake_045_evidence_refuses_when_another_runtime_is_installed(self):
        evidence = CurrentCatalogEvidence(CURRENT_TRUSS_045_VERSION, {}, {}, {},
                                           CURRENT_TRUSS_045_WHEEL_SHA256,
                                           CURRENT_TRUSS_045_RUNTIME_MANIFEST_SHA256)
        with patch('ashlar_host.truss_current_catalog.installed_current_truss_profile',
                   return_value='ashlar-truss-current-catalog-evidence/0.1'):
            with self.assertRaisesRegex(ValueError, 'Installed Truss 0.0.45 runtime'):
                require_installed_current_catalog_evidence(evidence)
        historical = CurrentCatalogEvidence(CURRENT_TRUSS_VERSION, {}, {}, {})
        with patch('ashlar_host.truss_current_catalog.installed_current_truss_profile',
                   side_effect=AssertionError('historical evidence must remain unchanged')):
            self.assertEqual(require_installed_current_catalog_evidence(historical),
                             'ashlar-truss-current-catalog-evidence/0.1')

    def test_048_public_release_tuple_is_exact_and_separate_from_045(self):
        manifest = b'public 0.0.48 runtime manifest fixture'
        direct_url = json.dumps({'archive_info': {'hashes': {
            'sha256': CURRENT_TRUSS_048_WHEEL_SHA256}}})
        with patch('ashlar_host.truss_current_catalog.CURRENT_TRUSS_048_RUNTIME_MANIFEST_SHA256',
                   hashlib.sha256(manifest).hexdigest()):
            self.assertEqual(require_current_truss_runtime(CURRENT_TRUSS_048_VERSION,
                runtime_manifest=manifest, direct_url=direct_url), CURRENT_048_EVIDENCE_PROFILE)
            with self.assertRaisesRegex(ValueError, '0.0.48 installed wheel'):
                require_current_truss_runtime(CURRENT_TRUSS_048_VERSION,
                    runtime_manifest=manifest,
                    direct_url=json.dumps({'archive_info': {'hashes': {'sha256': '0' * 64}}}))
        evidence = CurrentCatalogEvidence(CURRENT_TRUSS_048_VERSION, {}, {}, {},
            CURRENT_TRUSS_048_WHEEL_SHA256, CURRENT_TRUSS_048_RUNTIME_MANIFEST_SHA256)
        self.assertEqual(evidence.profile, CURRENT_048_EVIDENCE_PROFILE)
        with self.assertRaisesRegex(ValueError, '0.0.48 runtime-attested'):
            CurrentCatalogEvidence(CURRENT_TRUSS_048_VERSION, {}, {}, {},
                                   '0' * 64, CURRENT_TRUSS_048_RUNTIME_MANIFEST_SHA256)

    def test_049_public_release_tuple_is_exact_and_separate_from_048(self):
        self.assertEqual(CURRENT_TRUSS_049_WHEEL_SHA256,
                         '7ea14b2787f02cbeec9ad99d835adbcc30c684fd5be8e0765a1d8198b50e7182')
        self.assertEqual(CURRENT_TRUSS_049_RUNTIME_MANIFEST_SHA256,
                         '288801f7177338328441b5d3ce9328af31740ebf513f04ea45e30ae316a9f3a4')
        manifest = b'public 0.0.49 runtime manifest fixture'
        direct_url = json.dumps({'archive_info': {'hashes': {
            'sha256': CURRENT_TRUSS_049_WHEEL_SHA256}}})
        with patch('ashlar_host.truss_current_catalog.CURRENT_TRUSS_049_RUNTIME_MANIFEST_SHA256',
                   hashlib.sha256(manifest).hexdigest()):
            self.assertEqual(require_current_truss_runtime(CURRENT_TRUSS_049_VERSION,
                runtime_manifest=manifest, direct_url=direct_url), CURRENT_049_EVIDENCE_PROFILE)
            with self.assertRaisesRegex(ValueError, 'manifest differs'):
                require_current_truss_runtime(CURRENT_TRUSS_049_VERSION,
                    runtime_manifest=b'wrong', direct_url=direct_url)
            with self.assertRaisesRegex(ValueError, 'installed wheel differs'):
                require_current_truss_runtime(CURRENT_TRUSS_049_VERSION,
                    runtime_manifest=manifest,
                    direct_url=json.dumps({'archive_info': {'hashes': {'sha256': '0' * 64}}}))
        evidence = CurrentCatalogEvidence(CURRENT_TRUSS_049_VERSION, {}, {}, {},
            CURRENT_TRUSS_049_WHEEL_SHA256, CURRENT_TRUSS_049_RUNTIME_MANIFEST_SHA256)
        self.assertEqual(evidence.profile, CURRENT_049_EVIDENCE_PROFILE)
        with self.assertRaisesRegex(ValueError, '0.0.49 runtime-attested'):
            CurrentCatalogEvidence(CURRENT_TRUSS_049_VERSION, {}, {}, {})

    def test_actual_010_acceptance_receipt_interprets_assigned_catalog(self):
        report, view, sources, _ = receipt_inputs()
        admitted = interpret_current_catalog('1', report, view, sources)
        self.assertEqual(set(admitted['types']), {'1', '2'})
        self.assertEqual(set(admitted['edges']), {'1', '2'})
        self.assertEqual(admitted['models']['typed-graph-example'][1],
                         (EVIDENCE / 'accepted-source.umf.json').read_bytes())
        self.assertEqual(len(json.loads(view)['definitions']), 11)
        admitted_048 = interpret_current_catalog('1', report, view, sources,
                                                  truss_version=CURRENT_TRUSS_048_VERSION)
        self.assertEqual(admitted_048['models']['typed-graph-example'][1],
                         (EVIDENCE / 'accepted-source.umf.json').read_bytes())
        admitted_049 = interpret_current_catalog('1', report, view, sources,
                                                  truss_version=CURRENT_TRUSS_049_VERSION)
        self.assertEqual(admitted_049['models']['typed-graph-example'][1],
                         (EVIDENCE / 'accepted-source.umf.json').read_bytes())

    def test_actual_receipt_refuses_tampered_source_digest_and_revision(self):
        report_raw, view, sources, source = receipt_inputs()
        report = json.loads(report_raw)
        report['acceptedInput']['documents'][0]['artifact']['sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'differs from report'):
            interpret_current_catalog('1', json.dumps(report).encode(), view, sources)

        _, view, sources, source = receipt_inputs()
        sources[('1', source.document_id)] = SimpleNamespace(
            document_id=source.document_id,
            document_revision='tampered-revision',
            artifact_identity=source.artifact_identity,
            registration_id=source.registration_id,
            sha256=source.sha256,
            content=source.content,
            catalog_revision=1,
        )
        with self.assertRaisesRegex(ValueError, 'identity, revision or digest'):
            interpret_current_catalog('1', report_raw, view, sources)

    def test_artifact_identity_is_distinct_from_document_id_and_registration_is_bound(self):
        report_raw, view, sources, source = receipt_inputs()
        report = json.loads(report_raw)
        report['acceptedInput']['documents'][0]['artifact']['identity'] = 'accepted-artifact-alias'
        source.artifact_identity = 'accepted-artifact-alias'
        aliased_report = json.dumps(report).encode()
        admitted = interpret_current_catalog('1', aliased_report, view, sources)
        self.assertEqual(set(admitted['types']), {'1', '2'})

        source.artifact_identity = 'typed-graph-example'
        with self.assertRaisesRegex(ValueError, 'identity, revision or digest'):
            interpret_current_catalog('1', aliased_report, view, sources)
        source.artifact_identity = 'accepted-artifact-alias'
        source.registration_id = 'different-registration'
        with self.assertRaisesRegex(ValueError, 'identity, revision or digest'):
            interpret_current_catalog('1', aliased_report, view, sources)

    def test_catalog_reader_selects_live_current_custody_only_for_report_06(self):
        from ashlar.truss_native_feed import native_feed_batch
        from ashlar_host.truss_catalog_reader import read_graph_catalog_evidence
        from tests.test_truss_graph_sql import Tests as GraphTests
        original,_=GraphTests().batch()
        payload=json.loads(original.begin)
        report=(EVIDENCE/'accepted-report.json').read_bytes()
        payload['catalogPrerequisites']=[{'revision':'1','reportBytesBase64':base64.b64encode(report).decode(),
            'reportSha256':hashlib.sha256(report).hexdigest()}]
        raw=json.dumps(payload).encode()
        current=native_feed_batch(raw,sha256=hashlib.sha256(raw).hexdigest(),
            prior=base64.b64decode(original.cursor_before),next=base64.b64decode(original.cursor_after),feed=original.feed)
        with patch('ashlar_host.truss_catalog_reader.read_current_catalog_evidence',return_value='current') as live, \
             patch('ashlar_host.truss_catalog_reader.read_historical_catalog_views',return_value='historical') as archived:
            self.assertEqual(read_graph_catalog_evidence(lambda:None,current),'current')
            live.assert_called_once()
            archived.assert_not_called()

        with patch('ashlar_host.truss_catalog_reader.read_current_catalog_evidence',return_value='current') as live, \
             patch('ashlar_host.truss_catalog_reader.read_historical_catalog_views',return_value='historical') as archived:
            self.assertEqual(read_graph_catalog_evidence(lambda:None,original),'historical')
            live.assert_not_called()
            archived.assert_called_once()

if __name__ == '__main__':
    unittest.main()
